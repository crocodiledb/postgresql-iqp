/*-------------------------------------------------------------------------
*
* incTQPool.h
*	  Poll for tuple queues
*
*
* src/include/executor/incTQPool.h
*
*-------------------------------------------------------------------------
*/

#ifndef INCTQPOOL_H
#define INCTQPOOL_H

#include "executor/incTupleQueue.h"
#include "utils/palloc.h"

/* Opaque struct, only known incTQPool.c */
typedef struct IncTQPool IncTQPool; 

extern IncTQPool * CreateIncTQPool(MemoryContext mc, int maxTQ); 

extern bool AddIncTQReader(IncTQPool * tq_pool, Relation r, TupleDesc tupledesc); 

extern bool HasTQUpdate(IncTQPool *tq_pool, Relation r);

extern int GetTQUpdate(IncTQPool *tq_pool); 

extern bool IsTQComplete(IncTQPool *tq_pool, Relation r);

extern IncTupQueueReader * GetTQReader(IncTQPool *tq_pool, Relation r, IncTupQueueReader *tq_reader);

extern void DrainTQReader(IncTQPool *tq_pool, IncTupQueueReader *tq_reader); 

extern void DestroyIncTQPool(IncTQPool *tq_pool);

#endif

