/*-------------------------------------------------------------------------
 *
 * nodeNestloopInc.c
 *	  routines to support incremental nest-loop joins
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeNestloopInc.c
 *
 *-------------------------------------------------------------------------
 */

/*
 * Interface Routines
 *
 *      ExecNestLoopInc
 *      ExecInitNestLoopInc
 *      ExecResetNestLoopState
 *      ExecInitNestLoopDelta
 *
 * */

#include "postgres.h"

#include "access/htup_details.h"
#include "executor/execdebug.h"
#include "executor/nodeNestloop.h"
#include "executor/hashjoin.h"
#include "executor/nodeHash.h"
#include "executor/nodeHashjoin.h"
#include "miscadmin.h"
#include "utils/memutils.h"

#include "executor/incmeta.h"
#include "executor/incinfo.h"

#define NL_NEEDOUTER 1
#define NL_RESCANINNER 2
#define NL_NEEDINNER 3
#define NL_HASHJOIN 4
#define NL_NLJOIN 5

static HashJoin *BuildHJFromNL(NestLoop *nl);
static Hash *BuildHashFromNL(NestLoop *nl);
static void LinkHJToNL(NestLoopState *nl_state, HashJoinState *hj_state); 
static List* ExtractHashClauses(List *joinclauses); 
static List* ExtractNonHashClauses(List *joinclauses); 

static void BuildHashTable(HashJoinState *hjstate, TupleTableSlot *slot);
static void FinalizeHashTable(HashJoinState *node); 
static TupleTableSlot *ProbeHashTable(HashJoinState *hjstate, TupleTableSlot *outerSlot); 


/* ----------------------------------------------------------------
 *		ExecNestLoopIncReal(node)
 *
 * old comments
 *		Returns the tuple joined from inner and outer tuples which
 *		satisfies the qualification clause.
 *
 *		It scans the inner relation to join with current outer tuple.
 *
 *		If none is found, next tuple from the outer relation is retrieved
 *		and the inner relation is scanned from the beginning again to join
 *		with the outer tuple.
 *
 *		NULL is returned if all the remaining outer tuples are tried and
 *		all fail to join with the inner tuples.
 *
 *		NULL is also returned if there is no tuple from inner relation.
 *
 *		Conditions:
 *		  -- outerTuple contains current tuple from outer relation and
 *			 the right son(inner relation) maintains "cursor" at the tuple
 *			 returned previously.
 *				This is achieved by maintaining a scan position on the outer
 *				relation.
 *
 *		Initial States:
 *		  -- the outer child and the inner child
 *			   are prepared to return the first tuple.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecNestLoopIncReal(PlanState *pstate)
{
	NestLoopState *node = castNode(NestLoopState, pstate);
    HashJoinState *hjstate = node->nl_hj; 
    IncInfo       *incInfo = pstate->ps_IncInfo; 
	NestLoop   *nl;
	PlanState  *innerPlan;
	PlanState  *outerPlan;
	TupleTableSlot *outerTupleSlot;
	TupleTableSlot *innerTupleSlot;
    TupleTableSlot *retTupleSlot; 
	ExprState  *joinqual;
	ExprState  *otherqual;
	ExprContext *econtext;
	ListCell   *lc;

	CHECK_FOR_INTERRUPTS();

	/*
	 * get information from the node
	 */
	ENL1_printf("getting info from node");

	nl = (NestLoop *) node->js.ps.plan;
	joinqual = node->js.joinqual;
	otherqual = node->js.ps.qual;
	outerPlan = outerPlanState(node);
	innerPlan = innerPlanState(node);
	econtext = node->js.ps.ps_ExprContext;

	/*
	 * Reset per-tuple memory context to free any expression evaluation
	 * storage allocated in the previous tuple cycle.
	 */
	ResetExprContext(econtext);

	/*
	 * Ok, everything is setup for the join so now loop until we return a
	 * qualifying join tuple.
	 */
	ENL1_printf("entering main loop"); 

    for (;;)
    {
	    CHECK_FOR_INTERRUPTS();

        switch (node->nl_JoinState)
        {
            case NL_NEEDOUTER:
                // get outer tuple
    			ENL1_printf("getting new outer tuple");
    			outerTupleSlot = ExecProcNode(outerPlan);
    
    			/*
    			 * if there are no more outer tuples, then the join is complete..
    			 */
    			if (TupIsNull(outerTupleSlot))
    			{
    				ENL1_printf("no outer tuple, ending join");
                    //node->nl_isComplete = TupIsComplete(outerTupleSlot); 
    				return NULL;
    			}
    
    			ENL1_printf("saving new outer tuple information");
    			econtext->ecxt_outertuple = outerTupleSlot;

                if (node->nl_hashBuild && incInfo->rightAction == PULL_DELTA)
                    node->nl_JoinState = NL_HASHJOIN; 
                else
                    node->nl_JoinState = NL_RESCANINNER; 
                break; 
            case NL_RESCANINNER:
                // do rescanning
    			/*
    			 * fetch the values of any outer Vars that must be passed to the
    			 * inner scan, and store them in the appropriate PARAM_EXEC slots.
    			 */
    			foreach(lc, nl->nestParams)
    			{
    				NestLoopParam *nlp = (NestLoopParam *) lfirst(lc);
    				int			paramno = nlp->paramno;
    				ParamExecData *prm;
    
    				prm = &(econtext->ecxt_param_exec_vals[paramno]);
    				/* Param value should be an OUTER_VAR var */
    				Assert(IsA(nlp->paramval, Var));
    				Assert(nlp->paramval->varno == OUTER_VAR);
    				Assert(nlp->paramval->varattno > 0);
    				prm->value = slot_getattr(outerTupleSlot,
    										  nlp->paramval->varattno,
    										  &(prm->isnull));
    				/* Flag parameter value as changed */
    				innerPlan->chgParam = bms_add_member(innerPlan->chgParam,
    													 paramno);
    			}
    
                /* totem: assign inner PlanState rescan state */
                if (pstate->isDelta && node->nl_useHash && node->nl_hashBuild && incInfo->rightAction == PULL_BATCH_DELTA) 
                    innerPlanState(pstate)->chgAction = PULL_BATCH; 

    			/*
    			 * now rescan the inner plan
    			 */
    			ENL1_printf("rescanning inner plan");
    			ExecReScan(innerPlan);

                node->nl_JoinState = NL_NEEDINNER; 
                break; 
            case NL_NEEDINNER:
         		/*
        		 * we have an outerTuple, try to get the next inner tuple.
        		 */
        		ENL1_printf("getting new inner tuple");
        
        		innerTupleSlot = ExecProcNode(innerPlan);
        		econtext->ecxt_innertuple = innerTupleSlot;


                if (TupIsNull(innerTupleSlot))
                {
                    if (node->nl_hashBuild)
                        node->nl_JoinState = NL_HASHJOIN;
                    else
                        node->nl_JoinState = NL_NEEDOUTER; 

                    if (pstate->isDelta && node->nl_useHash)
                    {
                        node->nl_hashBuild = true; 
                        FinalizeHashTable(hjstate); 
                    }
                }
                else
                {
                    node->nl_JoinState = NL_NLJOIN; 
                    if (pstate->isDelta && node->nl_useHash && !node->nl_hashBuild && TupIsDelta(innerTupleSlot))
                        BuildHashTable(hjstate, innerTupleSlot); 
                }
                break; 
            case NL_HASHJOIN:
                retTupleSlot = ProbeHashTable(hjstate, outerTupleSlot); 
                if (retTupleSlot)
                    return retTupleSlot; 
                node->nl_JoinState = NL_NEEDOUTER; 
                break; 
            case NL_NLJOIN:
                node->nl_JoinState = NL_NEEDINNER; 
        		/*
        		 * at this point we have a new pair of inner and outer tuples so we
        		 * test the inner and outer tuples to see if they satisfy the node's
        		 * qualification.
        		 *
        		 * Only the joinquals determine MatchedOuter status, but all quals
        		 * must pass to actually return the tuple.
        		 */
        		ENL1_printf("testing qualification"); 
        
        		if (ExecQual(joinqual, econtext))
        		{
        			if (otherqual == NULL || ExecQual(otherqual, econtext))
        			{
        				/*
        				 * qualification was satisfied so we project and return the
        				 * slot containing the result tuple using ExecProject().
        				 */
        				ENL1_printf("qualification succeeded, projecting tuple");
        
        				return ExecProject(node->js.ps.ps_ProjInfo);
        			}
        			else
        				InstrCountFiltered2(node, 1);
        		}
        		else
        			InstrCountFiltered1(node, 1);
        
        		/*
        		 * Tuple fails qual, so free per-tuple memory and try again.
        		 */
        		ResetExprContext(econtext);
        
        		ENL1_printf("qualification failed, looping");

                break;
            default: 
                elog(ERROR, "Unknown State"); 
                break; 
        }

    }
}

TupleTableSlot * 
ExecNestLoopInc(PlanState *pstate) 
{
	NestLoopState *node = castNode(NestLoopState, pstate);
    TupleTableSlot * result_slot = ExecNestLoopIncReal(pstate); 
    if (!TupIsNull(result_slot)) 
    {
        return result_slot; 
    }
    else
    {
        result_slot = node->js.ps.ps_ProjInfo->pi_state.resultslot; 
        ExecClearTuple(result_slot);
        /*if (node->nl_isComplete) 
        {
            MarkTupComplete(result_slot, true);
        }
        else 
        {
            // reset the states of nest loop
        	node->nl_NeedNewOuter = true;
	        node->nl_MatchedOuter = false;

            MarkTupComplete(result_slot, false);
        }*/
        return result_slot; 
    }
}


/* ----------------------------------------------------------------
 *		ExecInitNestLoopInc
 *
 *		Init routines for incremental nest loop 
 * ----------------------------------------------------------------
 */

void 
ExecInitNestLoopInc(NestLoopState *node, int eflags)
{
    node->js.ps.isDelta = false; 
    node->nl_useHash = true;
    node->nl_hashBuild = false;
    node->nl_JoinState = NL_NEEDOUTER; 
    innerPlanState(node)->chgAction = PULL_BATCH;  

    if (node->nl_useHash)
    {
        NestLoop *plan  = (NestLoop *)node->js.ps.plan;
    
        HashJoin *hj_plan = BuildHJFromNL(plan); 
        Hash     *hash    = BuildHashFromNL(plan); 
        EState   *estate  = node->js.ps.state;
    
        /* Link left plan and right plan for hj_plan */
        outerPlan(hj_plan) = outerPlan(plan);
        innerPlan(hj_plan) = hash; 
    
        /* Provide left PlanState */
        estate->leftChildExist = true;
        estate->tempLeftPS = outerPlanState(node); 
    
        HashJoinState *hj_state = ExecInitHashJoin(hj_plan, estate, eflags); 
    
        /* Link right Plan/PlanState to hash_plan/hash_state */
        HashState *hash_state = innerPlanState(hj_state);
        Plan *hash_plan = hash_state->ps.plan;
        Plan *nl_plan = node->js.ps.plan;
    
        outerPlan(hash_plan) = innerPlan(nl_plan); 
        outerPlanState(hash_state) = innerPlanState(node); 
    
        node->nl_hj = hj_state; 
    }
    else
    {
        node->nl_hj = NULL; 
    }
    
}


/* ----------------------------------------------------------------
 *		ExecResetNestLoopState
 *
 *		Reset nest loop state 
 * ----------------------------------------------------------------
 */

void
ExecResetNestLoopState(NestLoopState * node)
{
    node->nl_hashBuild = false;

    PlanState *innerPlan; 
    PlanState *outerPlan; 
           
    innerPlan = innerPlanState(node); 
    outerPlan = outerPlanState(node); 

    ExecResetState(innerPlan); 
    ExecResetState(outerPlan); 

    return; 
}

/* ----------------------------------------------------------------
 *		ExecInitNestLoopDelta
 *
 *		Reset nest loop state 
 * ----------------------------------------------------------------
 */

void
ExecInitNestLoopDelta(NestLoopState * node)
{
    IncInfo *incInfo = node->js.ps.ps_IncInfo; 
    node->js.ps.isDelta = true;
    node->nl_JoinState = NL_NEEDOUTER;
    
    innerPlanState(node)->chgAction = incInfo->rightAction; 

//    if(node->nl_useHash) /* use hash */
//    {
//        if (!node->nl_hashBuild)
//        {
//            if (incInfo->rightAction == PULL_DELTA)
//                innerPlanState(node)->chgState = PROC_INC_DELTA; 
//            else
//                innerPlanState(node)->chgState = PROC_INC_BATCH; 
//        }
//        else /* is in delta processing, hash built, and need pull batch */
//        {
//            innerPlanState(node)->chgState = PROC_NORM_BATCH; 
//        }
//    }
//    else /* nest loop */
//    {
//        if (incInfo->rightAction == PULL_DELTA )
//            innerPlanState(node)->chgState = PROC_INC_DELTA; 
//        else
//            innerPlanState(node)->chgState = PROC_INC_BATCH;
//    }

    PlanState *innerPlan; 
    PlanState *outerPlan; 
           
    innerPlan = innerPlanState(node); 
    outerPlan = outerPlanState(node); 

    ExecInitDelta(innerPlan); 
    ExecInitDelta(outerPlan); 

    return; 
}

static HashJoin *
BuildHJFromNL(NestLoop *nl)
{
	Plan	 *nl_plan = &nl->join.plan; 

    HashJoin *hj = makeNode(HashJoin);
    Plan     *hj_plan = &hj->join.plan;

	hj_plan->targetlist = nl_plan->targetlist; 
    hj_plan->qual = nl_plan->qual; 
    hj_plan->lefttree = NULL;
    hj_plan->righttree = NULL; 

    hj->join.inner_unique = nl->join.inner_unique;
    hj->join.jointype = nl->join.jointype;
    hj->join.joinqual = ExtractNonHashClauses(nl->join.joinqual); 
    hj->hashclauses   = ExtractHashClauses(nl->join.joinqual);

    return hj; 
}

static Hash*
BuildHashFromNL(NestLoop *nl)
{
	Plan	 *nl_plan = &nl->join.plan; 

    Hash     *hash = makeNode(Hash); 
    Plan     *hash_plan = &hash->plan; 
    Plan     *inner_plan = innerPlan(nl_plan); 

    hash_plan->targetlist = inner_plan->targetlist; 
    hash_plan->qual = NIL;
	hash_plan->lefttree = NULL;  
	hash_plan->righttree = NULL;

	hash->skewTable = InvalidOid; 
	hash->skewColumn = 0; 
	hash->skewInherit = true; 

    return hash; 
}

static void
LinkHJToNL(NestLoopState *nl_state, HashJoinState *hj_state)
{
    HashState *hash_state = innerPlanState(hj_state); 

    Plan *nl_plan = nl_state->js.ps.plan; 
    Plan *hj_plan = hj_state->js.ps.plan; 
    Plan *hash_plan = hash_state->ps.plan; 

    outerPlan(hj_plan) = outerPlan(nl_plan); 
    outerPlan(hash_plan) = innerPlan(nl_plan); 

    outerPlanState(hj_state) = outerPlanState(nl_state); 
    outerPlanState(hash_state) = innerPlanState(nl_state); 
}

/* TODO: right now we only copy joinclauses to hashcluases, which may not work all the time */
static List*
ExtractHashClauses(List *joinclauses)
{
    List	   *hashclauses = NIL;
    ListCell   *l;
    
    foreach(l, joinclauses)
    {
        Expr *expr = (Expr *) lfirst(l);

        hashclauses = lappend(hashclauses, expr);
    }

    return hashclauses;  
}

static List*
ExtractNonHashClauses(List *joinclauses)
{
   return NULL;  
}

#define HJ_NEED_NEW_OUTER 0
#define HJ_SCAN_BUCKET 1

static void
BuildHashTable(HashJoinState *node, TupleTableSlot *slot)
{
	HashJoinTable hashtable;
	HashState  *hashNode;

    hashtable = node->hj_HashTable;
	hashNode = (HashState *) innerPlanState(node);

    if (hashtable == NULL)
    {
        hashtable = ExecHashTableCreate((Hash *) hashNode->ps.plan,
                                                node->hj_HashOperators,
                                                false);
        node->hj_HashTable = hashtable; 
        hashNode->hashtable = hashtable;
    }

	PlanState  *outerNode;
	List	   *hashkeys;
	ExprContext *econtext;
	uint32		hashvalue;

	/*
	 * get state info from hashNode
	 */
	outerNode = outerPlanState(hashNode);

	/*
	 * set expression context
	 */
	hashkeys = hashNode->hashkeys;
	econtext = hashNode->ps.ps_ExprContext;

	/* We have to compute the hash value annd insert the tuple */
	econtext->ecxt_innertuple = slot;
	if (ExecHashGetHashValue(hashtable, econtext, hashkeys,
							 false, hashtable->keepNulls,
							 &hashvalue))
	{
		/* No skew optimization, so insert normally */
		ExecHashTableInsert(hashtable, slot, hashvalue);
		hashtable->totalTuples += 1;
	}
}

static void
FinalizeHashTable(HashJoinState *node)
{
    HashJoinTable hashtable = node->hj_HashTable;

	/* Account for the buckets in spaceUsed (reported in EXPLAIN ANALYZE) */
	hashtable->spaceUsed += hashtable->nbuckets * sizeof(HashJoinTuple);
	if (hashtable->spaceUsed > hashtable->spacePeak)
		hashtable->spacePeak = hashtable->spaceUsed;

    node->hj_JoinState = HJ_NEED_NEW_OUTER; 
}

static TupleTableSlot *
ProbeHashTable(HashJoinState *node, TupleTableSlot *outerTupleSlot)
{	
	HashState  *hashNode;
	ExprState  *joinqual;
	ExprState  *otherqual;
	ExprContext *econtext;
	HashJoinTable hashtable;
	uint32		hashvalue;
	int			batchno;

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

    for (;;)
    {	
        switch (node->hj_JoinState)
    	{
    		case HJ_NEED_NEW_OUTER:
    
    			econtext->ecxt_outertuple = outerTupleSlot; 
    
    			/*
    			 * Find the corresponding bucket for this tuple in the main
    			 * hash table or skew hash table.
    			 */
    			node->hj_CurHashValue = hashvalue;
    			ExecHashGetBucketAndBatch(hashtable, hashvalue,
    									  &node->hj_CurBucketNo, &batchno);
    
    			node->hj_CurTuple = NULL;
    
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
    				HeapTupleHeaderSetMatch(HJTUPLE_MINTUPLE(node->hj_CurTuple));
    
    				if (otherqual == NULL || ExecQual(otherqual, econtext))
    					return ExecProject(node->js.ps.ps_ProjInfo);
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

    return NULL; 
}
