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

#define NL_BUILD_HASHTABLE 0
#define NL_NEED_INNER 1
#define NL_NEED_OUTER 2
#define NL_SCAN_BUCKET_INNER 3
#define NL_RESCAN_INNER      4
#define NL_SCAN_BUCKET_OUTER 5

static HashJoin *BuildHJFromNL(NestLoop *nl);
static Hash *BuildHashFromNL(NestLoop *nl);
static void LinkHJToNL(NestLoopState *nl_state, HashJoinState *hj_state); 
static List* ExtractHashClauses(List *joinclauses); 
static List* ExtractNonHashClauses(List *joinclauses); 

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

    HashJoinState *hjstate = node->nl_hj; 

    /* inner/outer hashnode/hashtable */
    HashState     *innerHashNode = innerPlanState(hjstate);
	HashJoinTable innerHashTable = hjstate->hj_HashTable;

    HashState     *outerHashNode = hjstate->hj_OuterHashNode;
    HashJoinTable outerHashTable = hjstate->hj_OuterHashTable; 

    bool        hasMatch = false; 
	uint32		hashvalue;
    int			batchno;
    List	   *hashkeys = innerHashNode->hashkeys;

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
            case NL_BUILD_HASHTABLE:
                Assert(innerHashTable == NULL);
                				
                innerHashTable = ExecHashTableCreate((Hash *) innerHashNode->ps.plan,
												hjstate->hj_HashOperators,
												false);

                hjstate->hj_HashTable = innerHashTable; 

                /* Rescan the delta of inner plan */
                innerPlan->chgParam = NULL;  
                innerPlanState(pstate)->chgAction = PULL_DELTA; 
    			ExecReScan(innerPlan);

                node->nl_JoinState = NL_NEED_INNER; 

                break ;

            case NL_NEED_INNER:

                innerTupleSlot = ExecProcNode(innerPlan); 

                /* Insert Into Hash Table*/
		        if (TupIsNull(innerTupleSlot))
                {	
                    
                    /* resize the hash table if needed (NTUP_PER_BUCKET exceeded) */
            	    if (innerHashTable->nbuckets != innerHashTable->nbuckets_optimal)
		                ExecHashIncreaseNumBuckets(innerHashTable);

            	    /* Account for the buckets in spaceUsed (reported in EXPLAIN ANALYZE) */
                	innerHashTable->spaceUsed += innerHashTable->nbuckets * sizeof(HashJoinTuple);
                	if (innerHashTable->spaceUsed > innerHashTable->spacePeak)
		                innerHashTable->spacePeak = innerHashTable->spaceUsed;

                    node->nl_JoinState = NL_NEED_OUTER; 
    
    				/*
    				 * need to remember whether nbatch has increased since we
    				 * began scanning the outer relation
    				 */
    				innerHashTable->nbatch_outstart = innerHashTable->nbatch;

                    continue;
                }

                /* We have to compute the hash value and insert the tuple */
                econtext->ecxt_innertuple = innerTupleSlot;
                if (ExecHashGetHashValue(innerHashTable, econtext, hashkeys,
                						 false, false,
            							 &hashvalue))
            	{
            	    /* No skew optimization, so insert normally */
            	    ExecHashTableInsert(innerHashTable, innerTupleSlot, hashvalue);
            	    innerHashTable->totalTuples += 1;
            	}

                if (outerHashTable != NULL)
                {
                    hjstate->hj_CurHashValue = hashvalue;
		    		ExecHashGetBucketAndBatch(outerHashTable, hashvalue,
		    								  &hjstate->hj_CurBucketNo, &batchno);
		    		hjstate->hj_CurSkewBucketNo = ExecHashGetSkewBucket(outerHashTable,
		    														 hashvalue);
		    		hjstate->hj_CurTuple = NULL; 
                    
                    /* We assume only one batch, which means inner/outer hash table is stored in memory */
                    Assert(batchno == 0); 
                    node->nl_JoinState = NL_SCAN_BUCKET_OUTER; 
                }

                break; 

            case NL_SCAN_BUCKET_OUTER:
                
                if (!ExecScanHashBucketInc(hjstate, econtext, true))
                {
                    node->nl_JoinState = NL_NEED_INNER;
                    continue; 
                }
        
        		if (joinqual == NULL || ExecQual(joinqual, econtext))
        		{
        			if (otherqual == NULL || ExecQual(otherqual, econtext))
        				return ExecProject(node->js.ps.ps_ProjInfo);
        			else
        				InstrCountFiltered2(node, 1);
        		}
        		else
        			InstrCountFiltered1(node, 1);

                break;

            case NL_NEED_OUTER:

                if (incInfo->leftAction == PULL_NOTHING)
                    return NULL; 

    			outerTupleSlot = ExecProcNode(outerPlan);

				if (TupIsNull(outerTupleSlot))
                {
                    if (node->nl_keep && outerHashTable != NULL)
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

				econtext->ecxt_outertuple = outerTupleSlot;

                /*
                 * Do we keep this tuple?
                 * */
                if (node->nl_keep)
                {
                    if (outerHashTable == NULL)
                    {
                        outerHashTable = ExecHashTableCreate((Hash *) outerHashNode->ps.plan, 
											                    hjstate->hj_HashOperators,
												                false);

                        outerHashNode->hashtable = outerHashTable; 

                        hjstate->hj_OuterHashTable = outerHashTable; 
                    }

                    if (ExecHashGetHashValue(outerHashTable, econtext, hjstate->hj_OuterHashKeys, 
								 true,	/* outer tuple */
								 false, &hashvalue))
                    {
                        /* Insert into the hash table */
                        ExecHashTableInsert(outerHashTable, outerTupleSlot, hashvalue);
                	    outerHashTable->totalTuples += 1;
                    }
                    else
                    {
                        elog(ERROR, "Null attributes when computing hash values"); 
                    }
                }

                /* Two things
                 *      1) Get bucket for the inner hash table
                 *      2) Rescan inner
                 * */

				/*
				 * Find the corresponding bucket for this tuple in the main
				 * hash table or skew hash table.
				 */
                if (innerHashTable != NULL)
                {
                    if (!node->nl_keep) /* hashvalue has been not computed*/
                        ExecHashGetHashValue(innerHashTable, econtext, 
                                             hjstate->hj_OuterHashKeys, 
                                             true, /* outer tuple */ 
								             false, &hashvalue); 

                    hjstate->hj_CurHashValue = hashvalue;
                        
				    ExecHashGetBucketAndBatch(innerHashTable, hashvalue,
										  &hjstate->hj_CurBucketNo, &batchno);
				    hjstate->hj_CurSkewBucketNo = ExecHashGetSkewBucket(innerHashTable,
																    hashvalue);
				    hjstate->hj_CurTuple = NULL; 
                }

                /*
                 * Rescan inner if necessary
                 */
                if (node->nl_rescan_action != PULL_NOTHING)
                {
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
        				prm->value = slot_getattr(econtext->ecxt_outertuple,
        										  nlp->paramval->varattno,
        										  &(prm->isnull));
        				/* Flag parameter value as changed */
        				innerPlan->chgParam = bms_add_member(innerPlan->chgParam,
        													 paramno);
        			}

                    /* totem: assign inner PlanState rescan state */
                    innerPlanState(pstate)->chgAction = node->nl_rescan_action; 
    
        			/*
        			 * now rescan the inner plan
        			 */
        			ExecReScan(innerPlan);
                }

                if (innerHashTable != NULL)
                    node->nl_JoinState = NL_SCAN_BUCKET_INNER;  
                else
                    node->nl_JoinState = NL_RESCAN_INNER; 

                break;   

            case NL_SCAN_BUCKET_INNER:

                if (!ExecScanHashBucketInc(hjstate, econtext, false))
                {
                    if (node->nl_rescan_action != PULL_NOTHING)
                        node->nl_JoinState = NL_RESCAN_INNER; 
                    else
                        node->nl_JoinState = NL_NEED_OUTER; 
                    continue; 
                }

                /* Perform join*/
        		if (joinqual == NULL || ExecQual(joinqual, econtext))
        		{
        			if (otherqual == NULL || ExecQual(otherqual, econtext))
        				return ExecProject(node->js.ps.ps_ProjInfo);
        			else
        				InstrCountFiltered2(node, 1);
        		}
        		else
        			InstrCountFiltered1(node, 1);

                break; 

            case NL_RESCAN_INNER:
                innerTupleSlot = ExecProcNode(innerPlan);
                econtext->ecxt_innertuple = innerTupleSlot;

                if (TupIsNull(innerTupleSlot))
                {
                    node->nl_JoinState = NL_NEED_OUTER; 
                    continue; 
                }

		        ResetExprContext(econtext);

        		if (joinqual == NULL || ExecQual(joinqual, econtext))
        		{
        			if (otherqual == NULL || ExecQual(otherqual, econtext))
        				return ExecProject(node->js.ps.ps_ProjInfo);
        			else
        				InstrCountFiltered2(node, 1);
        		}
        		else
        			InstrCountFiltered1(node, 1);

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
    node->nl_useHash = true; 
    node->nl_keep = true;

    node->nl_JoinState = NL_NEED_OUTER; 
    node->nl_rescan_action = PULL_BATCH; 

    if (use_sym_hashjoin)
        node->nl_useHash = true; 

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

    BuildOuterHashNode(hj_state, estate, eflags); 
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
    IncInfo *incInfo = node->js.ps.ps_IncInfo;
    HashJoinState *hjstate = node->nl_hj; 

    if (incInfo->incState[LEFT_STATE] == STATE_DROP) 
    {
        if (hjstate->hj_OuterHashTable != NULL)
            ExecHashTableDestroy(hjstate->hj_OuterHashTable);
        hjstate->hj_OuterHashTable = NULL;
        hjstate->hj_OuterHashNode->hashtable = NULL; 
    }
    else if (incInfo->incState[LEFT_STATE] == STATE_KEEPDISK)
    {
        Assert(false); 
    }

    PlanState *innerPlan; 
    PlanState *outerPlan; 
           
    innerPlan = innerPlanState(node); 
    outerPlan = outerPlanState(node); 

    ExecResetState(innerPlan); 
    ExecResetState(outerPlan); 
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

    if (node->nl_useHash && (incInfo->rightAction == PULL_BATCH_DELTA || incInfo->rightAction == PULL_DELTA))
        node->nl_JoinState = NL_BUILD_HASHTABLE; 
    else
        node->nl_JoinState = NL_NEED_OUTER; 

    if (node->nl_useHash) 
    {
        if (incInfo->rightAction == PULL_BATCH_DELTA)
            node->nl_rescan_action = PULL_BATCH; 
        else if (incInfo->rightAction == PULL_DELTA)
            node->nl_rescan_action = PULL_NOTHING; 
        else
            node->nl_rescan_action = incInfo->rightAction; 
    }
    else
    {
        node->nl_rescan_action = incInfo->rightAction; 
    }

    PlanState *innerPlan; 
    PlanState *outerPlan; 
           
    innerPlan = innerPlanState(node); 
    outerPlan = outerPlanState(node); 

    ExecInitDelta(innerPlan); 
    ExecInitDelta(outerPlan); 
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

int 
ExecNestLoopMemoryCost(NestLoopState * node, bool estimate)
{
    if (!use_sym_hashjoin)
        return 0; 

    if (estimate || node->nl_hj->hj_OuterHashTable == NULL )
    {
        Plan *plan = outerPlan(node->js.ps.plan);
        return ((ExecEstimateHashTableSize(plan->plan_rows, plan->plan_width) + 1023) / 1024); 
    }
    else
    {
        return (int)((node->nl_hj->hj_OuterHashTable->spaceUsed + 1023) / 1024); 
    }
}

void 
ExecNestLoopIncMarkKeep(NestLoopState *nl, IncState state)
{
    nl->nl_keep = (state != STATE_DROP); 
}
