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

#include "executor/execdebug.h"
#include "executor/nodeNestloop.h"
#include "miscadmin.h"
#include "utils/memutils.h"

#include "executor/incmeta.h"
#include "executor/incinfo.h"

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
	NestLoop   *nl;
	PlanState  *innerPlan;
	PlanState  *outerPlan;
	TupleTableSlot *outerTupleSlot;
	TupleTableSlot *innerTupleSlot;
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
		/*
		 * If we don't have an outer tuple, get the next one and reset the
		 * inner scan.
		 */
		if (node->nl_NeedNewOuter)
		{
			ENL1_printf("getting new outer tuple");
			outerTupleSlot = ExecProcNode(outerPlan);

			/*
			 * if there are no more outer tuples, then the join is complete..
			 */
			if (TupIsNull(outerTupleSlot))
			{
				ENL1_printf("no outer tuple, ending join");
                node->nl_isComplete = TupIsComplete(outerTupleSlot); 
				return NULL;
			}

			ENL1_printf("saving new outer tuple information");
			econtext->ecxt_outertuple = outerTupleSlot;
			node->nl_NeedNewOuter = false;
			node->nl_MatchedOuter = false;

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

			/*
			 * now rescan the inner plan
			 */
			ENL1_printf("rescanning inner plan");
			ExecReScan(innerPlan);
		}

		/*
		 * we have an outerTuple, try to get the next inner tuple.
		 */
		ENL1_printf("getting new inner tuple");

		innerTupleSlot = ExecProcNode(innerPlan);
		econtext->ecxt_innertuple = innerTupleSlot;

		if (TupIsNull(innerTupleSlot))
		{
			ENL1_printf("no inner tuple, need new outer tuple");

			node->nl_NeedNewOuter = true;

			if (!node->nl_MatchedOuter &&
				(node->js.jointype == JOIN_LEFT ||
				 node->js.jointype == JOIN_ANTI))
			{
				/*
				 * We are doing an outer join and there were no join matches
				 * for this outer tuple.  Generate a fake join tuple with
				 * nulls for the inner tuple, and return it if it passes the
				 * non-join quals.
				 */
				econtext->ecxt_innertuple = node->nl_NullInnerTupleSlot;

				ENL1_printf("testing qualification for outer-join tuple");

				if (otherqual == NULL || ExecQual(otherqual, econtext))
				{
					/*
					 * qualification was satisfied so we project and return
					 * the slot containing the result tuple using
					 * ExecProject().
					 */
					ENL1_printf("qualification succeeded, projecting tuple");

					return ExecProject(node->js.ps.ps_ProjInfo);
				}
				else
					InstrCountFiltered2(node, 1);
			}

			/*
			 * Otherwise just return to top of loop for a new outer tuple.
			 */
			continue;
		}

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
			node->nl_MatchedOuter = true;

			/* In an antijoin, we never return a matched tuple */
			if (node->js.jointype == JOIN_ANTI)
			{
				node->nl_NeedNewOuter = true;
				continue;		/* return to top of loop */
			}

			/*
			 * If we only need to join to the first matching inner tuple, then
			 * consider returning this one, but after that continue with next
			 * outer tuple.
			 */
			if (node->js.single_match)
				node->nl_NeedNewOuter = true;

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
    node->nl_isComplete = false;

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


/* ----------------------------------------------------------------
 *		ExecResetNestLoopState
 *
 *		Reset nest loop state 
 * ----------------------------------------------------------------
 */

void
ExecResetNestLoopState(NestLoopState * node)
{
	node->nl_NeedNewOuter = true;
	node->nl_MatchedOuter = false;

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

