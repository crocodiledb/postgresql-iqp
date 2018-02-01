/*-------------------------------------------------------------------------
 *
 * nodeHashjoinInc.c
 *	  Incremental version of nodeHashjoin
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeHashjoinInc.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "executor/executor.h"
#include "executor/hashjoin.h"
#include "executor/nodeHash.h"
#include "executor/nodeHashjoin.h"
#include "miscadmin.h"
#include "utils/memutils.h"

#include "executor/incmeta.h"
#include "executor/incinfo.h"

#define HJ_BUILD_HASHTABLE		1
#define HJ_NEED_NEW_OUTER		2
#define HJ_SCAN_BUCKET			3
#define HJ_FILL_OUTER_TUPLE		4
#define HJ_FILL_INNER_TUPLES	5
#define HJ_TRIGGER_COMPUTE		6
#define HJ_END_JOIN     		7

/* Returns true if doing null-fill on outer relation */
#define HJ_FILL_OUTER(hjstate)	((hjstate)->hj_NullInnerTupleSlot != NULL)
/* Returns true if doing null-fill on inner relation */
#define HJ_FILL_INNER(hjstate)	((hjstate)->hj_NullOuterTupleSlot != NULL)

static TupleTableSlot *ExecHashJoinOuterGetTupleInc(PlanState *outerNode,
						  HashJoinState *hjstate,
						  uint32 *hashvalue);

static TupleTableSlot *ExecHashJoinReal(PlanState *pstate);

/* ----------------------------------------------------------------
 *		ExecHashJoinReal
 *
 *		This function implements the Hybrid Hashjoin algorithm.
 *
 *		Note: the relation we build hash table on is the "inner"
 *			  the other one is "outer".
 * ----------------------------------------------------------------
 */
static TupleTableSlot *			/* return: a tuple or NULL */
ExecHashJoinReal(PlanState *pstate)
{
	HashJoinState *node = castNode(HashJoinState, pstate);
	PlanState  *outerNode;
	HashState  *hashNode;
	ExprState  *joinqual;
	ExprState  *otherqual;
	ExprContext *econtext;
	HashJoinTable hashtable;
	TupleTableSlot *outerTupleSlot;
	uint32		hashvalue;
	int			batchno;

    TupleTableSlot * slot; 

	/*
	 * get information from HashJoin node
	 */
	joinqual = node->js.joinqual;
	otherqual = node->js.ps.qual;
	hashNode = (HashState *) innerPlanState(node);
	outerNode = outerPlanState(node);
	hashtable = node->hj_HashTable;
	econtext = node->js.ps.ps_ExprContext;

	/*
	 * Reset per-tuple memory context to free any expression evaluation
	 * storage allocated in the previous tuple cycle.
	 */
	ResetExprContext(econtext);

	/*
	 * run the hash join state machine
	 */
	for (;;)
	{
		/*
		 * It's possible to iterate this loop many times before returning a
		 * tuple, in some pathological cases such as needing to move much of
		 * the current batch to a later batch.  So let's check for interrupts
		 * each time through.
		 */
		CHECK_FOR_INTERRUPTS();

		switch (node->hj_JoinState)
		{
			case HJ_BUILD_HASHTABLE:

				/*
				 * First time through: build hash table for inner relation.
				 */
				Assert(hashtable == NULL);

				/*
				 * If the outer relation is completely empty, and it's not
				 * right/full join, we can quit without building the hash
				 * table.  However, for an inner join it is only a win to
				 * check this when the outer relation's startup cost is less
				 * than the projected cost of building the hash table.
				 * Otherwise it's best to build the hash table first and see
				 * if the inner relation is empty.  (When it's a left join, we
				 * should always make this check, since we aren't going to be
				 * able to skip the join on the strength of an empty inner
				 * relation anyway.)
				 *
				 * If we are rescanning the join, we make use of information
				 * gained on the previous scan: don't bother to try the
				 * prefetch if the previous scan found the outer relation
				 * nonempty. This is not 100% reliable since with new
				 * parameters the outer relation might yield different
				 * results, but it's a good heuristic.
				 *
				 * The only way to make the check is to try to fetch a tuple
				 * from the outer plan node.  If we succeed, we have to stash
				 * it away for later consumption by ExecHashJoinOuterGetTuple.
				 */
				if (HJ_FILL_INNER(node))
				{
					/* no chance to not build the hash table */
					node->hj_FirstOuterTupleSlot = NULL;
				}
				else if (HJ_FILL_OUTER(node) ||
						 (outerNode->plan->startup_cost < hashNode->ps.plan->total_cost &&
						  !node->hj_OuterNotEmpty))
				{
					node->hj_FirstOuterTupleSlot = ExecProcNode(outerNode);
					if (TupIsNull(node->hj_FirstOuterTupleSlot))
					{
						node->hj_OuterNotEmpty = false;
						return NULL;
					}
					else
						node->hj_OuterNotEmpty = true;
				}
				else
					node->hj_FirstOuterTupleSlot = NULL;

				/*
				 * create the hash table
				 */
				hashtable = ExecHashTableCreate((Hash *) hashNode->ps.plan,
												node->hj_HashOperators,
												HJ_FILL_INNER(node));
				node->hj_HashTable = hashtable;

				/*
				 * execute the Hash node, to build the hash table
				 */
				hashNode->hashtable = hashtable;
				(void) MultiExecProcNode((PlanState *) hashNode);

				/*
				 * If the inner relation is completely empty, and we're not
				 * doing a left outer join, we can quit without scanning the
				 * outer relation.
				 */
				if (hashtable->totalTuples == 0 && !HJ_FILL_OUTER(node))
					return NULL;

				/*
				 * need to remember whether nbatch has increased since we
				 * began scanning the outer relation
				 */
				hashtable->nbatch_outstart = hashtable->nbatch;

				/*
				 * Reset OuterNotEmpty for scan.  (It's OK if we fetched a
				 * tuple above, because ExecHashJoinOuterGetTuple will
				 * immediately set it again.)
				 */
				node->hj_OuterNotEmpty = false;

				node->hj_JoinState = HJ_NEED_NEW_OUTER;

				/* FALL THRU */

			case HJ_NEED_NEW_OUTER:

				/*
				 * We don't have an outer tuple, try to get the next one
				 */
				outerTupleSlot = ExecHashJoinOuterGetTupleInc(outerNode,
														   node,
														   &hashvalue);
				if (TupIsNull(outerTupleSlot))
				{
                    if (TupIsComplete(outerTupleSlot)) /* end of the whole join */
                    {  
                        node->hj_isComplete = true;

					    if (HJ_FILL_INNER(node) && !node->hj_isDelta) /* only scan unmatched inner tuples in batch processing */
				    	{
					    	/* set up to scan for unmatched inner tuples */
						    ExecPrepHashTableForUnmatched(node);
						    node->hj_JoinState = HJ_FILL_INNER_TUPLES;
					    }
					    else 
                        {
						    node->hj_JoinState = HJ_END_JOIN; 
                        }
                    } 
                    else /* Not complete, just trigger a computation */
                    {                         
                        if (HJ_FILL_INNER(node) && !node->hj_isDelta ) /* make sure we only scan unmatched inner tuples once */
                        {
						    ExecPrepHashTableForUnmatched(node);
						    node->hj_JoinState = HJ_FILL_INNER_TUPLES;
                        } 
                        else
                        {
                            node->hj_JoinState = HJ_TRIGGER_COMPUTE;
                        }

                        node->hj_isDelta = true; /* set the delta processing state */
                    }
					continue;
				}

				econtext->ecxt_outertuple = outerTupleSlot;
				node->hj_MatchedOuter = false;

				/*
				 * Find the corresponding bucket for this tuple in the main
				 * hash table or skew hash table.
				 */
				node->hj_CurHashValue = hashvalue;
				ExecHashGetBucketAndBatch(hashtable, hashvalue,
										  &node->hj_CurBucketNo, &batchno);
				node->hj_CurSkewBucketNo = ExecHashGetSkewBucket(hashtable,
																 hashvalue);
				node->hj_CurTuple = NULL; 
                
                /* We assume only one batch, which means inner hash table is stored in memory */
                Assert(batchno == 0); 

				/*
				 * The tuple might not belong to the current batch (where
				 * "current batch" includes the skew buckets if any).
				 */
				//if (batchno != hashtable->curbatch &&
				//	node->hj_CurSkewBucketNo == INVALID_SKEW_BUCKET_NO)
				//{
				//	/*
				//	 * Need to postpone this outer tuple to a later batch.
				//	 * Save it in the corresponding outer-batch file.
				//	 */
				//	Assert(batchno > hashtable->curbatch);
				//	ExecHashJoinSaveTuple(ExecFetchSlotMinimalTuple(outerTupleSlot),
				//						  hashvalue,
				//						  &hashtable->outerBatchFile[batchno]);
				//	/* Loop around, staying in HJ_NEED_NEW_OUTER state */
				//	continue;
				//}

				/* OK, let's scan the bucket for matches */
				node->hj_JoinState = HJ_SCAN_BUCKET;

				/* FALL THRU */

			case HJ_SCAN_BUCKET:

				/*
				 * Scan the selected hash bucket for matches to current outer
				 */
				if (!ExecScanHashBucket(node, econtext))
				{
					/* out of matches; check for possible outer-join fill */
					node->hj_JoinState = HJ_FILL_OUTER_TUPLE;
					continue;
				}

				/*
				 * We've got a match, but still need to test non-hashed quals.
				 * ExecScanHashBucket already set up all the state needed to
				 * call ExecQual.
				 *
				 * If we pass the qual, then save state for next call and have
				 * ExecProject form the projection, store it in the tuple
				 * table, and return the slot.
				 *
				 * Only the joinquals determine tuple match status, but all
				 * quals must pass to actually return the tuple.
				 */
				if (joinqual == NULL || ExecQual(joinqual, econtext))
				{
					node->hj_MatchedOuter = true;
					HeapTupleHeaderSetMatch(HJTUPLE_MINTUPLE(node->hj_CurTuple));

					/* In an antijoin, we never return a matched tuple */
					if (node->js.jointype == JOIN_ANTI)
					{
						node->hj_JoinState = HJ_NEED_NEW_OUTER;
						continue;
					}

					/*
					 * If we only need to join to the first matching inner
					 * tuple, then consider returning this one, but after that
					 * continue with next outer tuple.
					 */
					if (node->js.single_match)
						node->hj_JoinState = HJ_NEED_NEW_OUTER;

					if (otherqual == NULL || ExecQual(otherqual, econtext))
						return ExecProject(node->js.ps.ps_ProjInfo);
					else
						InstrCountFiltered2(node, 1);
				}
				else
					InstrCountFiltered1(node, 1);
				break;

			case HJ_FILL_OUTER_TUPLE:

				/*
				 * The current outer tuple has run out of matches, so check
				 * whether to emit a dummy outer-join tuple.  Whether we emit
				 * one or not, the next state is NEED_NEW_OUTER.
				 */
				node->hj_JoinState = HJ_NEED_NEW_OUTER;

				if (!node->hj_MatchedOuter &&
					HJ_FILL_OUTER(node))
				{
					/*
					 * Generate a fake join tuple with nulls for the inner
					 * tuple, and return it if it passes the non-join quals.
					 */
					econtext->ecxt_innertuple = node->hj_NullInnerTupleSlot;

					if (otherqual == NULL || ExecQual(otherqual, econtext))
						return ExecProject(node->js.ps.ps_ProjInfo);
					else
						InstrCountFiltered2(node, 1);
				}
				break;

			case HJ_FILL_INNER_TUPLES:

				/*
				 * We have finished a batch, but we are doing right/full join,
				 * so any unmatched inner tuples in the hashtable have to be
				 * emitted before we continue to the next batch.
				 */
				if (!ExecScanHashTableForUnmatched(node, econtext))
				{
					/*
                     * no more unmatched tuples
                     * check wether we complete the join or trigger a computation
                     */
                    if (node->hj_isComplete) 
                        node->hj_JoinState = HJ_END_JOIN;
                    else 
                        node->hj_JoinState = HJ_TRIGGER_COMPUTE;
					continue;
				}

				/*
				 * Generate a fake join tuple with nulls for the outer tuple,
				 * and return it if it passes the non-join quals.
				 */
				econtext->ecxt_outertuple = node->hj_NullOuterTupleSlot;

				if (otherqual == NULL || ExecQual(otherqual, econtext))
					return ExecProject(node->js.ps.ps_ProjInfo);
				else
					InstrCountFiltered2(node, 1);
				break;

            case HJ_TRIGGER_COMPUTE:
				/*
                node->hj_JoinState = HJ_NEED_NEW_OUTER; 

                slot = ExecClearTuple(node->js.ps.ps_ProjInfo->pi_state.resultslot); 
                MarkTupComplete(slot, false);

                return slot; 
                */ 

                return NULL;

				break;

            case HJ_END_JOIN:
                /*
                slot = ExecClearTuple(node->js.ps.ps_ProjInfo->pi_state.resultslot); 
                MarkTupComplete(slot, true); 

                return slot;
                */

                return NULL; 

                break;

			default:
				elog(ERROR, "unrecognized hashjoin state: %d",
					 (int) node->hj_JoinState);
		}
	}
}


/*
 * ExecHashJoinOuterGetTupleInc
 *
 *		get the next outer tuple for hashjoin: 
 *		executing the outer plan node in the first pass
 *
 * Returns a null slot if no more outer tuples (within the current batch).
 *
 * On success, the tuple's hash value is stored at *hashvalue --- this is
 * either originally computed, or re-read from the temp file.
 */
static TupleTableSlot *
ExecHashJoinOuterGetTupleInc(PlanState *outerNode,
						  HashJoinState *hjstate,
						  uint32 *hashvalue)
{
	HashJoinTable hashtable = hjstate->hj_HashTable;
	int			curbatch = hashtable->curbatch;
	TupleTableSlot *slot;

	/*
	 * Check to see if first outer tuple was already fetched by
	 * ExecHashJoin() and not used yet.
	 */
	slot = hjstate->hj_FirstOuterTupleSlot;
	if (!TupIsNull(slot))
		hjstate->hj_FirstOuterTupleSlot = NULL;
	else
		slot = ExecProcNode(outerNode);

	while (!TupIsNull(slot))
	{
		/*
		 * We have to compute the tuple's hash value.
		 */
		ExprContext *econtext = hjstate->js.ps.ps_ExprContext;

		econtext->ecxt_outertuple = slot;
		if (ExecHashGetHashValue(hashtable, econtext,
								 hjstate->hj_OuterHashKeys,
								 true,	/* outer tuple */
								 HJ_FILL_OUTER(hjstate),
								 hashvalue))
		{
			/* remember outer relation is not empty for possible rescan */
			hjstate->hj_OuterNotEmpty = true;

			return slot;
		}

		/*
		 * That tuple couldn't match because of a NULL, so discard it and
		 * continue with the next one.
		 */
		slot = ExecProcNode(outerNode);
	}

	/* End of this batch */
	return slot;
} 

TupleTableSlot * 
ExecHashJoinInc(PlanState *pstate) 
{
	HashJoinState *node = castNode(HashJoinState, pstate);
    TupleTableSlot * result_slot = ExecHashJoinReal(pstate); 
    if (!TupIsNull(result_slot)) 
    {
        return result_slot; 
    }
    else
    {
        result_slot = node->js.ps.ps_ProjInfo->pi_state.resultslot; 
        ExecClearTuple(result_slot);
        if (node->hj_isComplete) 
        {
            MarkTupComplete(result_slot, true);
        }
        else 
        {
            /* reset the states of hashjoin */
			node->hj_OuterNotEmpty = false;
			node->hj_JoinState = HJ_NEED_NEW_OUTER;

            MarkTupComplete(result_slot, false);
        }
        return result_slot; 
    }
}


/* ----------------------------------------------------------------
 *		ExecInitHashJoinInc
 *
 *		Init routines for incremental hash join
 * ----------------------------------------------------------------
 */

void 
ExecInitHashJoinInc(HashJoinState *node)
{
    node->hj_isComplete = false;
    node->hj_isDelta = false;
}


/* ----------------------------------------------------------------
 *		ExecResetHashJoinState
 *
 *		Reset hash join state 
 * ----------------------------------------------------------------
 */

void
ExecResetHashJoinState(HashJoinState * node)
{
    IncInfo *incInfo = node->js.ps.ps_IncInfo;
    if (incInfo->rightIncState == STATE_DROP) 
    {
        ExecHashTableDestroy(node->hj_HashTable);
        node->hj_HashTable = NULL;
        node->hj_JoinState = HJ_BUILD_HASHTABLE;
    } 
    else if (incInfo->rightIncState == STATE_KEEPDISK)
    {
        Assert(false); /* not implemented yet; will do it soon */
    } 
    else /* keep in main memory */
    {
		node->hj_OuterNotEmpty = false;
		node->hj_JoinState = HJ_NEED_NEW_OUTER;
    }

    PlanState *innerPlan; 
    PlanState *outerPlan; 
           
    innerPlan = outerPlanState(innerPlanState(node)); /* Skip Hash node */ 
    outerPlan = outerPlanState(node); 

    ExecResetState(innerPlan); 
    ExecResetState(outerPlan); 

    return; 
}

/* ----------------------------------------------------------------
 *		ExecInitHashJoinDelta
 *
 *		Reset hash join state 
 * ----------------------------------------------------------------
 */

void
ExecInitHashJoinDelta(HashJoinState * node)
{

    PlanState *innerPlan; 
    PlanState *outerPlan; 
           
    innerPlan = outerPlanState(innerPlanState(node)); /* Skip Hash node */ 
    outerPlan = outerPlanState(node); 

    ExecInitDelta(innerPlan); 
    ExecInitDelta(outerPlan); 

    return; 
}

/* ----------------------------------------------------------------
 *		ExecHashJoinMemoryCost
 *
 *		Get hash join memory cost
 * ----------------------------------------------------------------
 */

int 
ExecHashJoinMemoryCost(HashJoinState * node)
{
    return (int)((node->hj_HashTable->spaceUsed + 1023) / 1024); 
}


