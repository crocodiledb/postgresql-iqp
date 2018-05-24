/*-------------------------------------------------------------------------
 *
 * nodeMaterialInc.c
 *	  Routines to handle inc materialization nodes.
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeMaterialInc.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		ExecMaterialInc			- materialize the result of a subplan
 *		ExecInitMaterialInc		- initialize node and subnodes
 *		ExecEndMaterialInc		- shutdown node and subnodes
 *
 */
#include "postgres.h"

#include "executor/executor.h"
#include "executor/nodeMaterial.h"
#include "miscadmin.h"

#include "executor/incinfo.h"
#include "executor/incmeta.h"
#include "access/htup_details.h"

/* ----------------------------------------------------------------
 *		ExecMaterialInc
 *
 *		Read the tuplestore first and then getnext from child nodes;
 *		if keep is true, put the tuple in tuplestore 		 
 *
 * ----------------------------------------------------------------
 */
static TupleTableSlot *			/* result tuple from subplan */
ExecMaterialInc(PlanState *pstate)
{
	MaterialIncState *node = castNode(MaterialIncState, pstate);
	EState	   *estate;
	Densestorestate *tuplestorestate;
	bool		eof_tuplestore;
	TupleTableSlot *slot;

	CHECK_FOR_INTERRUPTS();

	/*
	 * get state info from node
	 */
	estate = node->ss.ps.state;
	tuplestorestate = node->tuplestorestate;

	/*
	 * If first time through, and we need a tuplestore, initialize it.
	 */
	if (tuplestorestate == NULL && node->keep )
	{
		tuplestorestate = densestore_begin_heap(work_mem);
		node->tuplestorestate = tuplestorestate;
	}

	eof_tuplestore = (tuplestorestate == NULL) ||
		densestore_ateof(tuplestorestate);

	/*
	 * If we can fetch another tuple from the tuplestore, return it.
	 */
	slot = node->ss.ps.ps_ResultTupleSlot;
	if (!eof_tuplestore)
	{
		if (densestore_gettupleslot(tuplestorestate, slot))
			return slot;
		eof_tuplestore = true;
	}

	/*
	 * If necessary, try to fetch another row from the subplan.
	 */

	PlanState  *outerNode;
	TupleTableSlot *outerslot;


	/*
	 * We can only get here with forward==true, so no need to worry about
	 * which direction the subplan will go.
	 */
    if (!node->buffered || node->ss.ps.ps_IncInfo->leftUpdate)
    {
	    outerNode = outerPlanState(node);
    	outerslot = ExecProcNode(outerNode);
	    if (TupIsNull(outerslot))
            return ExecClearTuple(slot);
    }
    else
    {
        return ExecClearTuple(node->ss.ss_ScanTupleSlot);
    }

	/*
	 * Append a copy of the returned tuple to tuplestore.  NOTE: because
	 * the tuplestore is certainly in EOF state, its read position will
	 * move forward over the added tuple.  This is what we want.
	 */
	if (node->keep)
		densestore_puttupleslot(tuplestorestate, outerslot);


	/*
	 * We can just return the subplan's returned tuple, without copying.
	 */
	return outerslot;
}

/* ----------------------------------------------------------------
 *		ExecInitMaterial
 * ----------------------------------------------------------------
 */
MaterialIncState *
ExecInitMaterialInc(Material *node, EState *estate, int eflags)
{
	MaterialIncState *matstate;
	PlanState  *outerPS;
	TupleDesc	tupDesc;

	/*
	 * create state structure
	 */
	matstate = makeNode(MaterialIncState);
	matstate->ss.ps.plan = (Plan *) node;
	matstate->ss.ps.state = estate;
	matstate->ss.ps.ExecProcNode = ExecMaterialInc;


	/*
	 * We must have a tuplestore buffering the subplan output to do backward
	 * scan or mark/restore.  We also prefer to materialize the subplan output
	 * if we might be called on to rewind and replay it many times. However,
	 * if none of these cases apply, we can skip storing the data.
	 */
	matstate->eflags = (eflags & (EXEC_FLAG_REWIND |
								  EXEC_FLAG_BACKWARD |
								  EXEC_FLAG_MARK));

	/*
	 * Tuplestore's interpretation of the flag bits is subtly different from
	 * the general executor meaning: it doesn't think BACKWARD necessarily
	 * means "backwards all the way to start".  If told to support BACKWARD we
	 * must include REWIND in the tuplestore eflags, else tuplestore_trim
	 * might throw away too much.
	 */
	if (eflags & EXEC_FLAG_BACKWARD)
		matstate->eflags |= EXEC_FLAG_REWIND;

	matstate->eof_underlying = false;
	matstate->tuplestorestate = NULL;
    matstate->keep = true;
    matstate->buffered = false;  

	/*
	 * Miscellaneous initialization
	 *
	 * Materialization nodes don't need ExprContexts because they never call
	 * ExecQual or ExecProject.
	 */

	/*
	 * tuple table initialization
	 *
	 * material nodes only return tuples from their materialized relation.
	 */
	ExecInitResultTupleSlot(estate, &matstate->ss.ps);
	ExecInitScanTupleSlot(estate, &matstate->ss);

	/*
	 * initialize child nodes
	 *
	 * We shield the child node from the need to support REWIND, BACKWARD, or
	 * MARK/RESTORE.
	 */
	eflags &= ~(EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK);

    /* Not initialize subplan because it is initialized standalone */
	/*
    outerPlan = outerPlan(node);
	outerPlanState(matstate) = ExecInitNode(outerPlan, estate, eflags);
    */

	/*
	 * initialize tuple type.  no need to initialize projection info because
	 * this node doesn't do projections.
	 */
    outerPS = estate->tempLeftPS; 
    tupDesc = ExecGetResultType(outerPS);
    ExecAssignScanType(&matstate->ss, tupDesc);

	ExecAssignResultTypeFromTL(&matstate->ss.ps);
	ExecSetSlotDescriptor(matstate->ss.ps.ps_ResultTupleSlot, tupDesc);
	matstate->ss.ps.ps_ProjInfo = NULL;

	return matstate;
}

/* ----------------------------------------------------------------
 *		ExecEndMaterialInc
 * ----------------------------------------------------------------
 */
void
ExecEndMaterialInc(MaterialIncState *node)
{
	/*
	 * clean out the tuple table
	 */
	ExecClearTuple(node->ss.ss_ScanTupleSlot);

	/*
	 * Release tuplestore resources
	 */
	if (node->tuplestorestate != NULL)
		densestore_end(node->tuplestorestate);
	node->tuplestorestate = NULL;

	/*
	 * shut down the subplan
	 */
	ExecEndNode(outerPlanState(node));
}

/* ----------------------------------------------------------------
 *		ExecReScanMaterialInc
 *
 *		Rescans the materialized relation.
 * ----------------------------------------------------------------
 */
void
ExecReScanMaterialInc(MaterialIncState *node)
{
	PlanState  *outerPlan = outerPlanState(node);

	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);

	if (node->eflags != 0)
	{
		/*
		 * If we haven't materialized yet, just return. If outerplan's
		 * chgParam is not NULL then it will be re-scanned by ExecProcNode,
		 * else no reason to re-scan it at all.
		 */
		if (!node->tuplestorestate)
			return;

		/*
		 * If subnode is to be rescanned then we forget previous stored
		 * results; we have to re-read the subplan and re-store.  Also, if we
		 * told tuplestore it needn't support rescan, we lose and must
		 * re-read.  (This last should not happen in common cases; else our
		 * caller lied by not passing EXEC_FLAG_REWIND to us.)
		 *
		 * Otherwise we can just rewind and rescan the stored output. The
		 * state of the subnode does not change.
		 */
		if (outerPlan->chgParam != NULL ||
			(node->eflags & EXEC_FLAG_REWIND) == 0)
		{
			densestore_end(node->tuplestorestate);
			node->tuplestorestate = NULL;
			if (outerPlan->chgParam == NULL)
				ExecReScan(outerPlan);
			node->eof_underlying = false;
		}
		else
			densestore_rescan(node->tuplestorestate);
	}
	else
	{
		/* In this case we are just passing on the subquery's output */

		/*
		 * if chgParam of subnode is not null then plan will be re-scanned by
		 * first ExecProcNode.
		 */
		if (outerPlan->chgParam == NULL)
			ExecReScan(outerPlan);
		node->eof_underlying = false;
	}

}

/* ----------------------------------------------------------------
 *		ExecResetMaterialIncState
 *
 *		Reset hash join state 
 * ----------------------------------------------------------------
 */

void
ExecResetMaterialIncState(MaterialIncState * node)
{
    IncInfo *incInfo = node->ss.ps.ps_IncInfo;
    if (incInfo->incState[LEFT_STATE] == STATE_DROP) 
    {	
        if (node->tuplestorestate != NULL)
            densestore_end(node->tuplestorestate);
	    node->tuplestorestate = NULL;
    } 
    else if (incInfo->incState[LEFT_STATE] == STATE_KEEPDISK)
    {
        Assert(false); /* not implemented yet; will do it soon */
    } 
    else /* keep in main memory */
    {
        node->buffered = true; 
    }

    PlanState *outerPlan; 
    outerPlan = outerPlanState(node); 
    ExecResetState(outerPlan); 
}

/* ----------------------------------------------------------------
 *		ExecInitMaterialIncDelta
 *
 *		Reset MaterialInc state 
 * ----------------------------------------------------------------
 */

void
ExecInitMaterialIncDelta(MaterialIncState * node)
{
    if (node->tuplestorestate != NULL)
        densestore_rescan(node->tuplestorestate);

    PlanState *outerPlan; 
    outerPlan = outerPlanState(node); 
    ExecInitDelta(outerPlan); 

    return; 
}

/* ----------------------------------------------------------------
 *		ExecMaterialIncMemoryCost
 *
 *		Get material inc memory cost
 * ----------------------------------------------------------------
 */

int 
ExecMaterialIncMemoryCost(MaterialIncState * node, bool estimate)
{
    Plan *plan = node->ss.ps.plan; 
    int memory_cost = 0; 
    if (estimate)
    {
        double input_bytes = plan->plan_rows * (MAXALIGN(plan->plan_width) + MAXALIGN(SizeofHeapTupleHeader)); 
        return (int) ((input_bytes +1023)/1024); 
    }
    else
        return (densestore_getusedmem(node->tuplestorestate) + 1023)/1024;
}

/*
 * Build MaterialIncState  
 * */
MaterialIncState *ExecBuildMaterialInc(EState *estate)
{
    MemoryContext old = MemoryContextSwitchTo(estate->es_query_cxt); 

    MaterialIncState *ms; 
    Material *node = makeNode(Material); 
    node->plan.type = T_Material; 
    node->plan.lefttree = NULL;
    node->plan.righttree = NULL; 
    node->plan.initPlan = NULL; 

    int eflags = (EXEC_FLAG_REWIND |
				  EXEC_FLAG_BACKWARD |
				  EXEC_FLAG_MARK);

    ms = ExecInitMaterialInc(node, estate, eflags);
    MemoryContextSwitchTo(old); 

    return ms; 
}

void 
ExecMaterialIncMarkKeep(MaterialIncState *ms, IncState state)
{
    ms->keep = (state != STATE_DROP); 
}

