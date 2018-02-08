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

/* Opaque struct, only known incTupleQueue.c */
typedef struct IncTupQueueReader IncTupQueueReader;
typedef struct IncTupQueueWriter IncTupQueueWriter; 

/* Use these to receive tuples */
extern IncTupQueueReader *CreateIncTupQueueReader(Relation r, TupleDesc tupledesc);

extern void OpenIncTupQueueReader(IncTupQueueReader * tq_reader);

extern int GetIncTupQueueSize(IncTupQueueReader *tq_reader); 

extern HeapTuple ReadIncTupQueue(IncTupQueueReader *tq_reader, bool *done);

extern void CloseIncTupQueueReader(IncTupQueueReader * tq_reader); 

extern void DestroyIncTupQueueReader(IncTupQueueReader * tq_reader); 

/* Use these to write tuples */
extern IncTupQueueWriter *CreateIncTupQueueWriter(Relation r, TupleDesc tupledesc);

extern bool OpenIncTupQueueWriter(IncTupQueueWriter * tq_writer); 

extern void WriteIncTupQueue(IncTupQueueWriter *tq_writer, HeapTuple tup); 

extern void CloseIncTupQueueWriter(IncTupQueueWriter * tq_writer); 

extern void DestroyIncTupQueueWriter(IncTupQueueWriter * tq_writer); 

#endif 

