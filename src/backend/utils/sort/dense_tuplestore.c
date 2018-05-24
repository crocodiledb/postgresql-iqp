#include "postgres.h"

#include <limits.h>

#include "access/htup_details.h"
#include "commands/tablespace.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "storage/buffile.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/dense_tuplestore.h"


#define DENSE_CHUNK_SIZE	    (32 * 1024L)
#define DENSE_CHUNK_THRESHOLD   (DENSE_CHUNK_SIZE / 4)

#define USEMEM(state,amt)	((state)->availMem -= (amt))
#define FREEMEM(state,amt)	((state)->availMem += (amt))

typedef struct DenseMemoryChunkData
{
    int			ntuples;		/* number of tuples stored in this chunk */
	size_t		maxlen;			/* size of the buffer holding the tuples */
	size_t		used;			/* number of buffer bytes already used */

	struct DenseMemoryChunkData *next;	/* pointer to the next chunk (linked
										 * list) */

	char		data[FLEXIBLE_ARRAY_MEMBER];	/* buffer allocated at the end */

} DenseMemoryChunkData; 

typedef struct DenseMemoryChunkData *DenseMemoryChunk;

struct Densestorestate 
{
    int             memtupindex;    /* index of the current scan */
	int			    memtupcount;	/* number of tuples currently present */
    int             memtupsize;     /* max number of tuples */
    int             chunkcount;     
    int64		    availMem;		/* remaining memory available, in bytes */
	int64		    allowedMem;		/* total memory allowed, in bytes */
    double          insertTime;     /* debug: time of inserting */
    double          allocTime;      /* debug: time of allocating */
	void	      **memtuples;		/* array of pointers to palloc'd tuples */
    MemoryContext   context;
    DenseMemoryChunk  chunks;
}; 

int densestore_getusedmem(Densestorestate *state); 

Densestorestate *densestore_begin_heap(int maxKBytes)
{	
    Densestorestate *state;
    MemoryContext  context, oldcxt; 

	context = AllocSetContextCreate(CurrentMemoryContext,
									"DenseStoreContext",
								    ALLOCSET_DEFAULT_SIZES);
	
    oldcxt = MemoryContextSwitchTo(context);

	state = (Densestorestate *) palloc(sizeof(Densestorestate));

    state->context = context; 

    state->memtupindex = 0; 
    state->memtupcount = 0;
    state->chunkcount  = 0; 
    state->availMem = maxKBytes * 1024L; 
    state->allowedMem = state->availMem; 

    state->insertTime = 0;
    state->allocTime = 0; 

    state->memtupsize = Max(16384 / sizeof(void *),
							ALLOCSET_SEPARATE_THRESHOLD / sizeof(void *) + 1);
    state->memtuples = (void **) palloc(state->memtupsize * sizeof(void *));
    USEMEM(state, GetMemoryChunkSpace(state->memtuples)); 

    state->chunks = NULL; 

    MemoryContextSwitchTo(oldcxt); 

    return state; 
}

bool densestore_ateof(Densestorestate *state)
{
    return (state->memtupindex >= state->memtupcount); 
}

void densestore_puttupleslot(Densestorestate *state, TupleTableSlot *slot)
{
    MinimalTuple tuple = ExecFetchSlotMinimalTuple(slot);
    Size size = tuple->t_len; 
    int tupcount  = state->memtupcount; 
	
	if ((state->chunks == NULL) ||
		(state->chunks->maxlen - state->chunks->used) < size)
	{

        DenseMemoryChunk newChunk;
		/* allocate new chunk and put it at the beginning of the list */
		newChunk = (DenseMemoryChunk) MemoryContextAlloc(state->context,
														offsetof(DenseMemoryChunkData, data) + DENSE_CHUNK_SIZE);

		newChunk->maxlen = DENSE_CHUNK_SIZE;
		newChunk->used = 0;
		newChunk->ntuples = 0;

		newChunk->next = state->chunks;
		state->chunks = newChunk;
        state->chunkcount++; 
        
        USEMEM(state, GetMemoryChunkSpace(newChunk)); 

        Assert(state->availMem >= 0); 
	}

    if (state->memtupcount >= state->memtupsize)
        densestore_growmemtup(state); 

    /* There is enough space in the current chunk, let's add the tuple */
	state->memtuples[tupcount] = state->chunks->data + state->chunks->used;
    memcpy(state->memtuples[tupcount], tuple, size);

	state->chunks->used += size;
	state->chunks->ntuples += 1;
    state->memtupcount++;
    state->memtupindex = state->memtupcount;  
}

bool densestore_gettupleslot(Densestorestate *state, TupleTableSlot *slot)
{
	MinimalTuple tuple;
    bool should_free = false; 

    if (densestore_ateof(state))
        return false; 

    tuple = state->memtuples[state->memtupindex]; 
    state->memtupindex++; 
 
    ExecStoreMinimalTuple(tuple, slot, should_free);
    return true; 
}

void densestore_rescan(Densestorestate *state)
{
    state->memtupindex = 0; 
}

void densestore_end(Densestorestate *state)
{
    MemoryContextDelete(state->context); 
}

int densestore_getusedmem(Densestorestate *state)
{
    return state->allowedMem - state->availMem; 
}
        
void densestore_growmemtup(Densestorestate *state)
{
	int			newmemtupsize;
    int			memtupsize = state->memtupsize;

	if (memtupsize < INT_MAX / 2)
    {
        newmemtupsize = memtupsize * 2;
    }
    else
    {
        elog(ERROR, "Mem Tuple Size Too Large"); 
    }
    
    FREEMEM(state, GetMemoryChunkSpace(state->memtuples));
	state->memtupsize = newmemtupsize;

	state->memtuples = (void **)
		repalloc_huge(state->memtuples,
					  state->memtupsize * sizeof(void *));

	USEMEM(state, GetMemoryChunkSpace(state->memtuples));
    Assert(state->availMem >= 0); 
}

