/*-------------------------------------------------------------------------
*
* incTupleQueue.h
*	  Message queue for sending tuples across processes
*
*
* src/include/executor/incTupleQueue.h
*
*-------------------------------------------------------------------------
*/

#ifndef INCTUPLEQUEUE_H
#define INCTUPLEQUEUE_H

#include "access/tupdesc.h"
#include "executor/tuptable.h"
#include "utils/relcache.h"

#define SHM_SIZE 100*1024*1024

#define GEN_TQ_KEY(r) \
    ((key_t)(r->rd_id))


typedef struct shm_tq {
    int tuple_num;
    int head; 
    int tail; 
    char data[SHM_SIZE]
} shm_tq; 

typedef struct IncTupQueueReader
{
    int         tq_id;
    key_t       tq_key; 
    int         ss_head;
    int         ss_total_num; 
    int         ss_cur_num; 
    TupleDesc	tupledesc;
    shm_tq     *tq; 
} IncTupQueueReader;

typedef struct IncTupQueueWriter
{
    int         tq_id;
    key_t       tq_key;
    TupleDesc   tupledesc; 
    shm_tq     *tq; 
} IncTupQueueWriter;

/* Use these to receive tuples */
extern IncTupQueueReader *CreateIncTupQueueReader(Relation r, TupleDesc tupledesc);

extern void OpenIncTupQueueReader(IncTupQueueReader * tq_reader);

extern int GetIncTupQueueSize(IncTupQueueReader *tq_reader); 

extern IncTupQueueReader *GetIncTupQueueSnapShot(IncTupQueueReader *tq_reader, IncTupQueueReader *ss_reader); 

extern HeapTuple ReadIncTupQueue(IncTupQueueReader *tq_reader, bool *done);

extern void DrainIncTupQueue(IncTupQueueReader *tq_reader, IncTupQueueReader *ss_reader); 

extern void CloseIncTupQueueReader(IncTupQueueReader * tq_reader); 

extern void DestroyIncTupQueueReader(IncTupQueueReader * tq_reader); 

/* Use these to write tuples */
extern IncTupQueueWriter *CreateIncTupQueueWriter(Relation r, TupleDesc tupledesc);

extern bool OpenIncTupQueueWriter(IncTupQueueWriter * tq_writer); 

extern void WriteIncTupQueue(IncTupQueueWriter *tq_writer, HeapTuple tup); 

extern void CloseIncTupQueueWriter(IncTupQueueWriter * tq_writer); 

extern void DestroyIncTupQueueWriter(IncTupQueueWriter * tq_writer); 

#endif 

