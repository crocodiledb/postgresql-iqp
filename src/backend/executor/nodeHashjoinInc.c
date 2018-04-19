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
#define HJ_NEED_NEW_INNER       2
#define HJ_NEED_NEW_OUTER		3
#define HJ_SCAN_BUCKET_OUTER    4
#define HJ_SCAN_BUCKET_INNER	5

/* Returns true if doing null-fill on outer relation */
#define HJ_FILL_OUTER(hjstate)	((hjstate)->hj_NullInnerTupleSlot != NULL)
/* Returns true if doing null-fill on inner relation */
#define HJ_FILL_INNER(hjstate)	((hjstate)->hj_NullOuterTupleSlot != NULL)

static TupleTableSlot *ExecHashJoinOuterGetTupleInc(PlanState *outerNode,
						  HashJoinState *hjstate,
						  uint32 *hashvalue);

static TupleTableSlot *ExecHashJoinReal(PlanState *pstate);

void BuildOuterHashNode(HashJoinState *hjstate, EState *estate, int eflags); 

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

    TupleTableSlot *retTupleSlot; 
    TupleTableSlot *innerTupleSlot; 

    IncInfo *incInfo = pstate->ps_IncInfo;  

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
     * prepare outer hash node  
     * */
    HashJoinTable  outerHashTable = node->hj_OuterHashTable; 
    HashState     *outerHashNode  = node->hj_OuterHashNode;

    /*
     * information for inner hash table
     * */ 
    PlanState  *hashOuterNode;
    List	   *hashkeys;
    ExprContext *hashecontext;
	/*
	 * get state info from hashNode
	 */
	hashOuterNode = outerPlanState(hashNode);

	/*
	 * set expression context
	 */
	hashkeys = hashNode->hashkeys;

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
				//(void) MultiExecProcNode((PlanState *) hashNode);
                //
				node->hj_JoinState = HJ_NEED_NEW_INNER;

                break; 

            case HJ_NEED_NEW_INNER:

                Assert(hashtable != NULL); 

                innerTupleSlot = ExecProcNode(hashOuterNode);

		        if (TupIsNull(innerTupleSlot))
                {	
                    
                    /* resize the hash table if needed (NTUP_PER_BUCKET exceeded) */
            	    if (hashtable->nbuckets != hashtable->nbuckets_optimal)
		                ExecHashIncreaseNumBuckets(hashtable);

            	    /* Account for the buckets in spaceUsed (reported in EXPLAIN ANALYZE) */
                	hashtable->spaceUsed += hashtable->nbuckets * sizeof(HashJoinTuple);
                	if (hashtable->spaceUsed > hashtable->spacePeak)
		                hashtable->spacePeak = hashtable->spaceUsed;

                    node->hj_JoinState = HJ_NEED_NEW_OUTER; 

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

                    continue;
                }

                /* We have to compute the hash value and insert the tuple */
                econtext->ecxt_innertuple = innerTupleSlot;
                if (ExecHashGetHashValue(hashtable, econtext, hashkeys,
                						 false, hashtable->keepNulls,
            							 &hashvalue))
            	{
            	    /* No skew optimization, so insert normally */
            	    ExecHashTableInsert(hashtable, innerTupleSlot, hashvalue);
            	    hashtable->totalTuples += 1;
            	}

                /*
                 * Now probe the outer hash table if possible
                 */

                if (outerHashTable != NULL && TupIsDelta(innerTupleSlot))
                {
		    		/*
		    		 * Find the corresponding bucket for this tuple in the main
		    		 * hash table or skew hash table.
		    		 */
		    		node->hj_CurHashValue = hashvalue;
		    		ExecHashGetBucketAndBatch(outerHashTable, hashvalue,
		    								  &node->hj_CurBucketNo, &batchno);
		    		node->hj_CurSkewBucketNo = ExecHashGetSkewBucket(outerHashTable,
		    														 hashvalue);
		    		node->hj_CurTuple = NULL; 
                    
                    /* We assume only one batch, which means inner/outer hash table is stored in memory */
                    Assert(batchno == 0); 

		    		/* OK, let's scan the bucket for matches */
		    		node->hj_JoinState = HJ_SCAN_BUCKET_INNER;
                }

                break; 

			case HJ_SCAN_BUCKET_INNER:
				/*
				 * Scan the selected hash bucket for matches to current outer
				 */
				if (!ExecScanHashBucketInc(node, econtext, true))
				{
					node->hj_JoinState = HJ_NEED_NEW_INNER;
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

					if (otherqual == NULL || ExecQual(otherqual, econtext))
                    {
						retTupleSlot = ExecProject(node->js.ps.ps_ProjInfo);
                        MarkTupDelta(retTupleSlot, TupIsDelta(econtext->ecxt_innertuple) || TupIsDelta(econtext->ecxt_outertuple)); 
                        return retTupleSlot; 
                    }
					else
						InstrCountFiltered2(node, 1);
				}
				else
					InstrCountFiltered1(node, 1);
				break;

			case HJ_NEED_NEW_OUTER:

                if (incInfo->leftAction == PULL_NOTHING)
                    return NULL; 

				/*
				 * We don't have an outer tuple, try to get the next one
				 */
				outerTupleSlot = ExecHashJoinOuterGetTupleInc(outerNode,
														   node,
														   &hashvalue);
				if (TupIsNull(outerTupleSlot))
                {
                    if (node->hj_keep && outerHashTable != NULL)
                    {
                        /* resize the hash table if needed (NTUP_PER_BUCKET exceeded) */
                	    if (outerHashTable->nbuckets != outerHashTable->nbuckets_optimal)
    		                ExecHashIncreaseNumBuckets(outerHashTable);
    
                	    /* Account for the buckets in spaceUsed (reported in EXPLAIN ANALYZE) */
                    	outerHashTable->spaceUsed += outerHashTable->nbuckets * sizeof(HashJoinTuple);
                    	if (outerHashTable->spaceUsed > outerHashTable->spacePeak)
    		                outerHashTable->spacePeak = outerHashTable->spaceUsed;
                    }
                    return NULL;
                }

                /*
                 * Do we keep this tuple?
                 * */
                if (node->hj_keep)
                {
                    if (outerHashTable == NULL)
                    {
                        outerHashTable = ExecHashTableCreate((Hash *) outerHashNode->ps.plan, 
											                    node->hj_HashOperators,
												                HJ_FILL_OUTER(node));

                        outerHashNode->hashtable = outerHashTable; 

                        node->hj_OuterHashTable = outerHashTable; 
                    }

                    /* Insert into the hash table */
                    ExecHashTableInsert(outerHashTable, outerTupleSlot, hashvalue);
                	outerHashTable->totalTuples += 1;
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

				/* OK, let's scan the bucket for matches */
				node->hj_JoinState = HJ_SCAN_BUCKET_OUTER;

                break; 

			case HJ_SCAN_BUCKET_OUTER:

				/*
				 * Scan the selected hash bucket for matches to current outer
				 */

				if (!ExecScanHashBucketInc(node, econtext, false))
				{
					node->hj_JoinState = HJ_NEED_NEW_OUTER; 
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

					if (otherqual == NULL || ExecQual(otherqual, econtext)) 
                    {
						retTupleSlot =  ExecProject(node->js.ps.ps_ProjInfo);
                        MarkTupDelta(retTupleSlot, TupIsDelta(econtext->ecxt_innertuple) || TupIsDelta(econtext->ecxt_outertuple)); 
                        return retTupleSlot; 
                    }
					else
						InstrCountFiltered2(node, 1);
				}
				else
					InstrCountFiltered1(node, 1);


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

bool
ExecScanHashBucketInc(HashJoinState *hjstate,
                      ExprContext *econtext, 
                      bool outer)
{
	ExprState  *hjclauses = hjstate->hashclauses;
	HashJoinTuple hashTuple = hjstate->hj_CurTuple;
	uint32		hashvalue = hjstate->hj_CurHashValue;
    
    HashJoinTable hashtable;
    if (outer) 
        hashtable = hjstate->hj_OuterHashTable;
    else
        hashtable = hjstate->hj_HashTable; 

	/*
	 * hj_CurTuple is the address of the tuple last returned from the current
	 * bucket, or NULL if it's time to start scanning a new bucket.
	 *
	 * If the tuple hashed to a skew bucket then scan the skew bucket
	 * otherwise scan the standard hashtable bucket.
	 */
	if (hashTuple != NULL)
		hashTuple = hashTuple->next;
	else if (hjstate->hj_CurSkewBucketNo != INVALID_SKEW_BUCKET_NO)
		hashTuple = hashtable->skewBucket[hjstate->hj_CurSkewBucketNo]->tuples;
	else
		hashTuple = hashtable->buckets[hjstate->hj_CurBucketNo];

	while (hashTuple != NULL)
	{
		if (hashTuple->hashvalue == hashvalue)
		{
			TupleTableSlot *temptuple;

		    /* insert hashtable's tuple into exec slot so ExecQual sees it */
            if (outer)
            {
		    	temptuple = ExecStoreMinimalTuple(HJTUPLE_MINTUPLE(hashTuple),
		    									 hjstate->hj_OuterTupleSlot,
		    									 false);	/* do not pfree */
		    	econtext->ecxt_outertuple = temptuple;
            }
            else
            {
                if (!CheckMatch(TupIsDelta(econtext->ecxt_outertuple), hashTuple->delta, hjstate->hj_PullEncoding)) 
                {
                    hashTuple = hashTuple->next;
                    continue; 
                }
 		    	temptuple = ExecStoreMinimalTuple(HJTUPLE_MINTUPLE(hashTuple),
		    									 hjstate->hj_HashTupleSlot,
		    									 false);	/* do not pfree */
		    	econtext->ecxt_innertuple = temptuple;               
            }

			/* reset temp memory each time to avoid leaks from qual expr */
			ResetExprContext(econtext);

			if (ExecQual(hjclauses, econtext))
			{
                MarkTupDelta(temptuple, hashTuple->delta); 
				hjstate->hj_CurTuple = hashTuple;
				return true;
			}
		}

		hashTuple = hashTuple->next;
	}

	/*
	 * no match
	 */
	return false;

}

void 
BuildOuterHashNode(HashJoinState *hjstate, EState *estate, int eflags)
{
    hjstate->hj_OuterHashTable = NULL; 

    /* Build Hash Plan */
	Plan	 *hj_plan = &hjstate->js.ps.plan;

    Hash     *hash = makeNode(Hash); 
    Plan     *hash_plan = &hash->plan; 
    Plan     *outer_plan = outerPlan(hj_plan); 

    hash_plan->targetlist = outer_plan->targetlist; 
    hash_plan->qual = NIL;
	hash_plan->lefttree = NULL;  
	hash_plan->righttree = NULL;

	hash->skewTable = InvalidOid; 
	hash->skewColumn = 0; 
	hash->skewInherit = true;
    
    outerPlan(hash) = NULL; 
    hjstate->hj_OuterHashNode = ExecInitHash(hash, estate, eflags);
    hjstate->hj_OuterHashNode->hashkeys = NULL; 
    hjstate->hj_OuterHashNode->hashtable = NULL;  

    /* Link outer plan(state) of HashJoin to hash plan(state) */
    outerPlan(hash) = outer_plan; 
    outerPlanState(hjstate->hj_OuterHashNode) = outerPlanState(hjstate);
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
ExecInitHashJoinInc(HashJoinState *node, EState *estate, int eflags)
{
    node->hj_PullEncoding = EncodePullAction(PULL_BATCH);
    node->hj_isComplete = false;
    node->hj_isDelta = false;
    node->hj_keep = true;
    node->hj_buildtime = 0;
    node->hj_innertime = 0;  
    node->hj_outertime = 0;  
    node->hj_scantime = 0;  

    BuildOuterHashNode(node, estate, eflags); 
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
    if (incInfo->incState[RIGHT_STATE] == STATE_DROP) 
    {
        ExecHashTableDestroy(node->hj_HashTable);
        node->hj_HashTable = NULL;
        node->hj_JoinState = HJ_BUILD_HASHTABLE;
    } 
    else if (incInfo->incState[RIGHT_STATE] == STATE_KEEPDISK)
    {
        Assert(false); /* not implemented yet; will do it soon */
    } 
    else /* keep in main memory */
    {
		node->hj_OuterNotEmpty = false;
        if (incInfo->rightAction == PULL_DELTA)
            node->hj_JoinState = HJ_NEED_NEW_INNER; 
        else if (incInfo->rightAction == PULL_NOTHING)
            node->hj_JoinState = HJ_NEED_NEW_OUTER;
        else
            elog(ERROR, "Pull Action %d Not Possible", incInfo->rightAction); 
    }

    if (incInfo->incState[LEFT_STATE] == STATE_DROP)
    {
        if (node->hj_OuterHashTable != NULL)
            ExecHashTableDestroy(node->hj_OuterHashTable);
        node->hj_OuterHashTable = NULL;
        node->hj_OuterHashNode->hashtable = NULL; 
    }
    else if (incInfo->incState[LEFT_STATE] == STATE_KEEPDISK)
    {
        Assert(false); 
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
    IncInfo * incInfo = node->js.ps.ps_IncInfo; 
    IncInfo * parent = incInfo->parenttree; 
    if (parent->lefttree == incInfo)
        node->hj_PullEncoding = EncodePullAction(parent->leftAction);
    else
        node->hj_PullEncoding = EncodePullAction(parent->rightAction); 

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
ExecHashJoinMemoryCost(HashJoinState * node, bool estimate, bool right)
{
    if (right)
    {
        if (estimate)
        {
            Plan *plan = innerPlan(node->js.ps.plan); 
            return ((ExecEstimateHashTableSize(plan->plan_rows, plan->plan_width) + 1023) / 1024); 
        }
        else
        {
            return (int)((node->hj_HashTable->spaceUsed + 1023) / 1024); 
        }
    }
    else
    {
        if (estimate || node->hj_OuterHashTable == NULL )
        {
            Plan *plan = outerPlan(node->js.ps.plan);
            return ((ExecEstimateHashTableSize(plan->plan_rows, plan->plan_width) + 1023) / 1024); 
        }
        else
        {
            return (int)((node->hj_OuterHashTable->spaceUsed + 1023) / 1024); 
        }
    }
}

void 
ExecHashJoinIncMarkKeep(HashJoinState *hjs, IncState state)
{
    hjs->hj_keep = (state != STATE_DROP); 
}

