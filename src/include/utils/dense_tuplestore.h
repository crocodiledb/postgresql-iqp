/*-------------------------------------------------------------------------
 *
 * dense_tuplestore.h
 *	  Generalized routines for temporary tuple storage with dense allocation 
 *
 * src/include/utils/dense_tuplestore.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DENSE_TUPLESTORE_H
#define DENSE_TUPLESTORE_H

#include "executor/tuptable.h"


/* Tuplestorestate is an opaque type whose details are not known outside
 * dense_tuplestore.c.
 */
typedef struct Densestorestate Densestorestate;

/*
 * Currently we only need to store MinimalTuples, but it would be easy
 * to support the same behavior for IndexTuples and/or bare Datums.
 */

extern Densestorestate *densestore_begin_heap(int maxKBytes);

extern bool densestore_ateof(Densestorestate *state);

extern void densestore_puttupleslot(Densestorestate *state, TupleTableSlot *slot);

extern bool densestore_gettupleslot(Densestorestate *state, TupleTableSlot *slot);

extern void densestore_rescan(Densestorestate *state);

extern void densestore_clear(Densestorestate *state);

extern void densestore_end(Densestorestate *state);

extern int densestore_getusedmem(Densestorestate *state); 


#endif							/* DENSE_TUPLESTORE_H */
