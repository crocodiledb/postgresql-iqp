/*-------------------------------------------------------------------------
 *
 * incTupleQueue.c
 *	  Use shared memory to send/receive tuples between processes
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/incTupleQueue.c
 *
 *-------------------------------------------------------------------------
 */


#include "postgres.h"

#include "access/htup_details.h"
#include "executor/incTupleQueue.h"
#include "utils/rel.h"

#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/shm.h>

static HeapTuple DecomposeIncTuple(int nbytes, HeapTupleHeader data);

static int byteArrayToInt(char data[], int index); 
static void intToByteArray(char data[], int index, int value); 

/*
 * IncTupQueueReader
 *      Create a tuple reader -- this should be executed in a query-span memory context
 */

IncTupQueueReader *
CreateIncTupQueueReader(Relation r, TupleDesc tupledesc)
{
    IncTupQueueReader *tq_reader = (IncTupQueueReader *)palloc(sizeof(IncTupQueueReader)); 
    tq_reader->tq_key = GEN_TQ_KEY(r);
    tq_reader->tupledesc = tupledesc; 

    return tq_reader; 
}

/*
 * OpenIncTupQueueReader
 *      Open a tuple reader -- build shared memory
 *
 * */
void  
OpenIncTupQueueReader(IncTupQueueReader * tq_reader)
{
    shm_tq *tq; 

    tq_reader->tq_id = shmget(tq_reader->tq_key,sizeof(shm_tq),IPC_CREAT|IPC_EXCL|0666);
    if (tq_reader->tq_id < 0)
    {
        elog(ERROR, "Open Shared Memory Error");
        return; 
    }

    tq = (shm_tq *)shmat(tq_reader->tq_id,NULL,0); 
    if (tq_reader->tq < 0)
    {
        elog(ERROR, "Attach Shared Memory Error"); 
        return; 
    }

    tq->tuple_num = 0;
    tq->head = 0; 
    tq->tail = 0;
    tq->complete = false; 
    tq_reader->tq = tq; 

    tq_reader->ss_head = 0;
    tq_reader->ss_cur_num = 0; 
    tq_reader->ss_total_num = 0; 
}

int 
GetIncTupQueueSize(IncTupQueueReader *tq_reader)
{
    return tq_reader->tq->tuple_num; 
}

bool GetIncTQComplete(IncTupQueueReader *tq_reader)
{
    return tq_reader->tq->complete;
}

IncTupQueueReader *
GetIncTupQueueSnapShot(IncTupQueueReader *tq_reader, IncTupQueueReader *ss_reader)
{
    if (ss_reader == NULL) {
        ss_reader = (IncTupQueueReader *)palloc(sizeof(IncTupQueueReader)); 

        ss_reader->tq_id = tq_reader->tq_id; 
        ss_reader->tq_key = tq_reader->tq_key;
        ss_reader->tupledesc = tq_reader->tupledesc; 
        ss_reader->tq = tq_reader->tq; 
    }

    ss_reader->ss_cur_num = tq_reader->tq->tuple_num; 
    ss_reader->ss_total_num = tq_reader->tq->tuple_num; 
    ss_reader->ss_head = tq_reader->tq->head; 

    return ss_reader; 
}

/*
 * Read works on the Snapshot
 * */
HeapTuple 
ReadIncTupQueue(IncTupQueueReader *tq_reader, bool *done)
{
    HeapTuple ht;

    shm_tq * tq = tq_reader->tq; 
    int head = tq_reader->ss_head; 

    if (tq_reader->ss_cur_num == 0)
    {
        *done = true; 
        return NULL; 
    }

    *done = false; 

    int nbytes = byteArrayToInt(tq->data, head);
    head = head + 4; 
    ht = DecomposeIncTuple(nbytes, &tq->data[head]); 
    tq_reader->ss_head = head + nbytes; 

    tq_reader->ss_cur_num--; 

	Assert(ht);

    return ht; 
}

static HeapTuple
DecomposeIncTuple(int nbytes, HeapTupleHeader data)
{
	HeapTupleData htup;

	/*
	 * Set up a dummy HeapTupleData pointing to the data from the shm_mq
	 * (which had better be sufficiently aligned).
	 */
	ItemPointerSetInvalid(&htup.t_self);
	htup.t_tableOid = InvalidOid;
	htup.t_len = nbytes;
	htup.t_data = data;

    return heap_copytuple(&htup); 
}

void 
DrainIncTupQueue(IncTupQueueReader *tq_reader, IncTupQueueReader *ss_reader)
{
    if (ss_reader == NULL || tq_reader->tq->tuple_num == 0) /* Just Initialized or Reset already */
        return;

    tq_reader->tq->head = 0;
    tq_reader->tq->tail = 0;
    tq_reader->tq->tuple_num = 0; 

    Assert(tq_reader->tq->tuple_num == ss_reader->ss_total_num); 
}

void  
CloseIncTupQueueReader(IncTupQueueReader * tq_reader)
{
    shmdt((const void *)tq_reader->tq);
    shmctl(tq_reader->tq_id,IPC_RMID,0);
}

void 
DestroyIncTupQueueReader(IncTupQueueReader * tq_reader)
{
    pfree(tq_reader); 
}

/*
 * CreateIncTupQueueWriter
 *      Create a tuple writer -- this should be executed in a query-span memory context
 */

IncTupQueueWriter *
CreateIncTupQueueWriter(Relation r, TupleDesc tupledesc)
{
    IncTupQueueWriter *tq_writer = (IncTupQueueWriter *)palloc(sizeof(IncTupQueueWriter)); 
    tq_writer->tq_key = GEN_TQ_KEY(r);
    tq_writer->tupledesc = tupledesc; 

    return tq_writer;  
}

/*
 * OpenIncTupQueueWriter
 *      Open a tuple writer -- build shared memory
 *
 * */
bool  
OpenIncTupQueueWriter(IncTupQueueWriter * tq_writer)
{
    shm_tq *tq; 

    tq_writer->tq_id = shmget(tq_writer->tq_key, sizeof(shm_tq), 0 );
    if (tq_writer->tq_id < 0)
    {
        if (errno == ENOENT) /* shm not exist, return */
            return false; 

        elog(ERROR, "Open Shared Memory Error");
        return false; 
    }

    tq = (shm_tq *)shmat(tq_writer->tq_id, NULL, 0); 
    if (tq_writer->tq < 0)
    {
        elog(ERROR, "Attach Shared Memory Error"); 
        return false; 
    }

    tq_writer->tq = tq; 

    return true; 
}

void  
WriteIncTupQueue(IncTupQueueWriter *tq_writer,  HeapTuple tup)
{
    shm_tq * tq = tq_writer->tq; 
    if (tq->tail + tup->t_len + 4 >= SHM_SIZE)
    {
        elog(ERROR, "Shared Memory Out of Bound"); 
        return; 
    }

    intToByteArray(tq->data, tq->tail, tup->t_len); 
    tq->tail += 4; 
    memcpy(&tq->data[tq->tail], tup->t_data, tup->t_len); 
    tq->tail += tup->t_len; 

    tq->tuple_num++; 

    return; 
}

void
MarkCompleteIncTQWriter(IncTupQueueWriter * tq_writer)
{
    tq_writer->tq->complete = true; 
}

void  
CloseIncTupQueueWriter(IncTupQueueWriter * tq_writer)
{
    shmdt((const void *)tq_writer->tq);

    return; 
}

void 
DestroyIncTupQueueWriter(IncTupQueueWriter * tq_writer)
{
    pfree(tq_writer); 
}


static 
int byteArrayToInt(char data[], int index) {
    return (((int)data[index] << 24)
            + (((int)data[index + 1] & 0xFF) << 16)
            + (((int)data[index + 2] & 0xFF) << 8)
            + ((int)data[index + 3] & 0xFF));
}

static 
void intToByteArray(char data[], int index, int value) {
    data[index] = (char)(value >> 24); 
    data[index + 1] = (char)(value >> 16); 
    data[index + 2] = (char)(value >> 8); 
    data[index + 3] = (char)(value); 
}


