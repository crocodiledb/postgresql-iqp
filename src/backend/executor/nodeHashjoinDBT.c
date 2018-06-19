/*-------------------------------------------------------------------------
 *
 * nodeHashjoinDBT.c
 *	  Routines to handle hash join nodes for DBToaster
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeHashjoinDBT.c
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

/*
 * States of the ExecHashJoin state machine
 */
#define HJ_BUILD_HASHTABLE		1
#define HJ_NEED_NEW_INNER       2
#define HJ_NEED_NEW_OUTER		3
#define HJ_SCAN_BUCKET_OUTER    4
#define HJ_SCAN_BUCKET_INNER	5

/* Returns true if doing null-fill on outer relation */
#define HJ_FILL_OUTER(hjstate)	((hjstate)->hj_NullInnerTupleSlot != NULL)
/* Returns true if doing null-fill on inner relation */
#define HJ_FILL_INNER(hjstate)	((hjstate)->hj_NullOuterTupleSlot != NULL)

static bool ExecScanHashBucketDBT(HashJoinState *hjstate,
                                  ExprContext *econtext, 
                                  bool outer);
static TupleTableSlot * ExecHashJoin_NewInner(PlanState *pstate);
static TupleTableSlot * ExecHashJoin_NewOuter(PlanState *pstate);


static TupleTableSlot *			/* return: a tuple or NULL */
ExecHashJoin_NewInner(PlanState *pstate)
{
    HashJoinState *node = castNode(HashJoinState, pstate);
	HashState  *hashNode;
	ExprState  *joinqual;
	ExprState  *otherqual;
	ExprContext *econtext;
	uint32		hashvalue;
	int			batchno; 

    if (node->hj_OuterHashTable->totalTuples == 0)
        return NULL; 

	/*
	 * get information from HashJoin node
	 */
	hashNode = (HashState *) innerPlanState(node);
	joinqual = node->js.joinqual;
	otherqual = node->js.ps.qual;
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
    List	   *hashkeys;
	hashkeys = hashNode->hashkeys; 
    
    TupleTableSlot *innerTupleSlot = pstate->ps_WorkingTupleSlot;  

    if (node->hj_JoinState == HJ_NEED_NEW_INNER)
    {
        /* We have to compute the hash value and insert the tuple */
        econtext->ecxt_innertuple = innerTupleSlot;
        (void) ExecHashGetHashValue(outerHashTable, econtext, hashkeys,
        						 false, HJ_FILL_INNER(node),
        						 &hashvalue);

        econtext->ecxt_innertuple = innerTupleSlot;
		node->hj_MatchedOuter = false;

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

    for (;;) 
    {
        if (!ExecScanHashBucketDBT(node, econtext, true))
    	{
    		node->hj_JoinState = HJ_NEED_NEW_INNER;
            return NULL; 
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
                return ExecProject(node->js.ps.ps_ProjInfo);
            }
    		else
    			InstrCountFiltered2(node, 1);
    	}
    	else
    		InstrCountFiltered1(node, 1);     
    }
}

static TupleTableSlot *			/* return: a tuple or NULL */
ExecHashJoin_NewOuter(PlanState *pstate)
{
    HashJoinState *node = castNode(HashJoinState, pstate);
	HashState  *hashNode;
	ExprState  *joinqual;
	ExprState  *otherqual;
	ExprContext *econtext;
    List        *hashkeys; 
	HashJoinTable hashtable;
	uint32		hashvalue;
	int			batchno;

    if (node->hj_HashTable->totalTuples == 0)
        return NULL; 

	/*
	 * get information from HashJoin node
	 */
	joinqual = node->js.joinqual;
	otherqual = node->js.ps.qual;
	hashNode = (HashState *) innerPlanState(node);
	hashtable = node->hj_HashTable;
	econtext = node->js.ps.ps_ExprContext;

	/*
	 * Reset per-tuple memory context to free any expression evaluation
	 * storage allocated in the previous tuple cycle.
	 */
	ResetExprContext(econtext);

    if (node->hj_JoinState == HJ_NEED_NEW_OUTER)
    {
        TupleTableSlot *outerTupleSlot = pstate->ps_WorkingTupleSlot;  

        econtext->ecxt_outertuple = outerTupleSlot;
        (void) ExecHashGetHashValue(hashtable, econtext, node->hj_OuterHashKeys,
        						 true, HJ_FILL_OUTER(node),
        						 &hashvalue); 

        node->hj_OuterNotEmpty = true; 
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
    }

    /* We have already found the bucket; now scan it */
    for (;;)
    {
        /*
    	 * Scan the selected hash bucket for matches to current outer
    	 */
    	if (!ExecScanHashBucketDBT(node, econtext, false))
        {
            node->hj_JoinState = HJ_NEED_NEW_OUTER;
            return NULL; 
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
                return ExecProject(node->js.ps.ps_ProjInfo);
            }
    		else
    			InstrCountFiltered2(node, 1);
    	}
    	else
    		InstrCountFiltered1(node, 1);    
    }
}

/* ----------------------------------------------------------------
 *		ExecInitHashJoinDBT
 *
 *		Init routines for dbtoaster hash join
 * ----------------------------------------------------------------
 */

void 
ExecInitHashJoinDBT(HashJoinState *node, EState *estate, int eflags)
{
    BuildOuterHashNode(node, estate, eflags); 

    // Choose ExecFunction
    ScanState *innerSS = outerPlanState(innerPlanState(node)); 
    ScanState *outerSS = outerPlanState(node); 

    Oid innerOid = innerSS->ss_currentRelation->rd_id; 
    Oid outerOid = outerSS->ss_currentRelation->rd_id; 

    if ((innerOid == estate->rd_id && estate->isBuild) || (outerOid == estate->rd_id && !estate->isBuild))
        node->hj_hasInnerHash = true; 
    else
        node->hj_hasInnerHash = false; 

    if (node->hj_hasInnerHash)
    {
        node->hj_JoinState = HJ_NEED_NEW_OUTER;
        node->js.ps.ExecProcNode = ExecHashJoin_NewOuter;
    }
    else
    {
        node->hj_JoinState = HJ_NEED_NEW_INNER;
        node->js.ps.ExecProcNode = ExecHashJoin_NewInner;
    }

    HashState *innerHashNode = (HashState *) innerPlanState(node);
    HashState *outerHashNode = node->hj_OuterHashNode; 
    HashJoinTable hashtable;  

    // Build Corresponding Hash Table 
    if (node->hj_hasInnerHash)
    {
		hashtable = ExecHashTableCreate((Hash *) innerHashNode->ps.plan,
										node->hj_HashOperators,
										HJ_FILL_INNER(node));

		innerHashNode->hashtable = hashtable;

		node->hj_HashTable = hashtable;
    }
    else
    {
        hashtable = ExecHashTableCreate((Hash *) outerHashNode->ps.plan, 
							                    node->hj_HashOperators,
								                HJ_FILL_OUTER(node));

        outerHashNode->hashtable = hashtable; 

        node->hj_OuterHashTable = hashtable; 
    }
}

void 
ExecResetHashJoinDBT(HashJoinState *node)
{

}


static bool
ExecScanHashBucketDBT(HashJoinState *hjstate,
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
 		    	temptuple = ExecStoreMinimalTuple(HJTUPLE_MINTUPLE(hashTuple),
		    									 hjstate->hj_HashTupleSlot,
		    									 false);	/* do not pfree */
		    	econtext->ecxt_innertuple = temptuple;               
            }

			/* reset temp memory each time to avoid leaks from qual expr */
			ResetExprContext(econtext);

			if (ExecQual(hjclauses, econtext))
			{
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

