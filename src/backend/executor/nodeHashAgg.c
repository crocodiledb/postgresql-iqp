/*-------------------------------------------------------------------------
 *
 * nodeAgg.c
 *	  Routines to handle aggregate nodes.
 *
 *	  ExecAgg evaluates each aggregate in the following steps:
 *
 *		 transvalue = initcond
 *		 foreach input_value do
 *			transvalue = transfunc(transvalue, input_value)
 *		 result = finalfunc(transvalue)
 *
 *	  If a finalfunc is not supplied then the result is just the ending
 *	  value of transvalue.
 *
 *	  If transfunc is marked "strict" in pg_proc and initcond is NULL,
 *	  then the first non-NULL input_value is assigned directly to transvalue,
 *	  and transfunc isn't applied until the second non-NULL input_value.
 *	  The agg's input type and transtype must be the same in this case!
 *
 *	  If transfunc is marked "strict" then NULL input_values are skipped,
 *	  keeping the previous transvalue.	If transfunc is not strict then it
 *	  is called for every input tuple and must deal with NULL initcond
 *	  or NULL input_value for itself.
 *
 *	  If finalfunc is marked "strict" then it is not called when the
 *	  ending transvalue is NULL, instead a NULL result is created
 *	  automatically (this is just the usual handling of strict functions,
 *	  of course).  A non-strict finalfunc can make its own choice of
 *	  what to return for a NULL ending transvalue.
 *
 *	  When the transvalue datatype is pass-by-reference, we have to be
 *	  careful to ensure that the values survive across tuple cycles yet
 *	  are not allowed to accumulate until end of query.  We do this by
 *	  "ping-ponging" between two memory contexts; successive calls to the
 *	  transfunc are executed in alternate contexts, passing the previous
 *	  transvalue that is in the other context.	At the beginning of each
 *	  tuple cycle we can reset the current output context to avoid memory
 *	  usage growth.  Note: we must use MemoryContextContains() to check
 *	  whether the transfunc has perhaps handed us back one of its input
 *	  values rather than a freshly palloc'd value; if so, we copy the value
 *	  to the context we want it in.
 *
 *
 * Portions Copyright (c) 1996-2001, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  $Header: /project/eecs/db/cvsroot/postgres/src/backend/executor/nodeHashAgg.c,v 1.5 2005/02/09 00:16:42 phred Exp $
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/heapam.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_operator.h"
#include "executor/executor.h"
#include "executor/nodeGroup.h" /* for tuplesMatch */
#include "executor/nodeHashAgg.h"
#include "executor/aggHash.h"
#include "miscadmin.h"
#include "optimizer/clauses.h"
#include "parser/parse_coerce.h"
#include "parser/parse_expr.h"
#include "parser/parse_oper.h"
#include "parser/parse_type.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/tuplesort.h"
#include "utils/datum.h"

static void initialize_one_aggregate(HashAggState * aggstate,
						 AggStatePerAgg peraggstate,
						 AggStatePerGroup pergroupstate);
static void initialize_aggregates(HashAggState * aggstate,
					  AggStatePerAgg peragg,
					  AggStatePerGroup pergroup);
static void advance_transition_function(HashAggState * aggstate,
							AggStatePerAgg peraggstate,
							AggStatePerGroup pergroupstate,
							Datum newVal, bool isNull);
static void advance_aggregates(HashAggState * aggstate,
				   AggStatePerGroup pergroup);
static void process_sorted_aggregate(HashAggState * aggstate,
						 AggStatePerAgg peraggstate,
						 AggStatePerGroup pergroupstate);
static void finalize_one_aggregate(HashAggState * aggstate,
					   AggStatePerAgg peraggstate,
					   AggStatePerGroup pergroupstate,
					   Datum *resultVal, bool *resultIsNull);
static void finalize_aggregates(HashAggState * aggstate,
					AggStatePerAgg peragg,
					AggStatePerGroup pergroup);



static AggHashEntry lookup_hash_entry(HashAgg * node,
				  TupleTableSlot *slot);

static void agg_fill_hash_table(HashAgg * node);

static TupleTableSlot *exec_sorted_agg(HashAgg * node);
static TupleTableSlot *exec_hashed_agg(HashAgg * node);

 /* static */ void hash_write_tuple(BufFile *file, TupleTableSlot *tupleSlot);
 /* static */ TupleTableSlot *hash_read_tuple(BufFile *file, TupleTableSlot *tupleSlot);

static Datum GetAggInitVal(Datum textInitVal, Oid transtype);

/******************************************************************
 *	   Transfunc support code
 ******************************************************************/

/*
 * Initialize one aggregate for a new set of input values.
 *
 * When called, CurrentMemoryContext should be the per-query context.
 */
static void
initialize_one_aggregate(HashAggState * aggstate,
						 AggStatePerAgg peraggstate,
						 AggStatePerGroup pergroupstate)
{
	Aggref	   *aggref = peraggstate->aggref;

#ifdef AGG_DEBUG_MSGS
	elog(DEBUG, "initialize_one_aggregate");
#endif

	/*
	 * Start a fresh sort operation for each DISTINCT aggregate.
	 */
	if (aggref->aggdistinct)
	{
		/*
		 * In case of rescan, maybe there could be an uncompleted sort
		 * operation?  Clean it up if so.
		 */
		if (peraggstate->sortstate)
			tuplesort_end(peraggstate->sortstate);

		peraggstate->sortstate =
			tuplesort_begin_datum(peraggstate->inputType,
								  peraggstate->sortOperator,
								  false);
	}

#ifdef NOT_USED

	/*
	 * (Re)set transValue to the initial value.
	 *
	 * Note that when the initial value is pass-by-ref, we just reuse it
	 * without copying for each group.	Hence, transition function had
	 * better not scribble on its input, or it will fail for GROUP BY!
	 */
	pergroupstate->transValue = peraggstate->initValue;
	pergroupstate->transValueIsNull = peraggstate->initValueIsNull;
#endif

	/*
	 * (Re)set transValue to the initial value.
	 *
	 * Note that when the initial value is pass-by-ref, we must copy it (into
	 * the aggcontext) since we will pfree the transValue later.
	 */
	if (peraggstate->initValueIsNull)
		pergroupstate->transValue = peraggstate->initValue;
	else
	{
		MemoryContext oldContext;

		oldContext = MemoryContextSwitchTo(aggstate->aggcontext);
		pergroupstate->transValue = datumCopy(peraggstate->initValue,
											  peraggstate->transtypeByVal,
											  peraggstate->transtypeLen);
		MemoryContextSwitchTo(oldContext);
	}
	pergroupstate->transValueIsNull = peraggstate->initValueIsNull;

	/*
	 * If the initial value for the transition state doesn't exist in the
	 * pg_aggregate table then we will let the first non-NULL value
	 * returned from the outer procNode become the initial value. (This is
	 * useful for aggregates like max() and min().)  The noTransValue flag
	 * signals that we still need to do this.
	 */
	pergroupstate->noTransValue = peraggstate->initValueIsNull;
}


/*
 *
 */
static void
initialize_aggregates(HashAggState * aggstate,
					  AggStatePerAgg peragg,
					  AggStatePerGroup pergroup)
{
	int			aggno;

#ifdef AGG_DEBUG_MSGS
	elog(DEBUG, "initialize_aggregates");
#endif

	for (aggno = 0; aggno < aggstate->numaggs; aggno++)
	{
		AggStatePerAgg peraggstate = &peragg[aggno];
		AggStatePerGroup pergroupstate = &pergroup[aggno];

		initialize_one_aggregate(aggstate, peraggstate, pergroupstate);
	}
}


/*
 * Given a new input value, advance the transition function of an aggregate.
 *
 * When called, CurrentMemoryContext should be the context we want the
 * transition function result to be delivered into on this cycle.
 */
static void
advance_transition_function(HashAggState * aggstate,
							AggStatePerAgg peraggstate,
							AggStatePerGroup pergroupstate,
							Datum newVal, bool isNull)
{
	FunctionCallInfoData fcinfo;
	MemoryContext oldContext;

#ifdef AGG_DEBUG_MSGS
	elog(DEBUG, "advance_transition_function");
#endif

	if (peraggstate->transfn.fn_strict)
	{
		if (isNull)
		{
			/*
			 * For a strict transfn, nothing happens at a NULL input
			 * tuple; we just keep the prior transValue.
			 */
			return;
		}
		if (pergroupstate->noTransValue)
		{
			/*
			 * transValue has not been initialized. This is the first
			 * non-NULL input value. We use it as the initial value for
			 * transValue. (We already checked that the agg's input type
			 * is binary- compatible with its transtype, so straight copy
			 * here is OK.)
			 *
			 * We had better copy the datum if it is pass-by-ref, since the
			 * given pointer may be pointing into a scan tuple that will
			 * be freed on the next iteration of the scan.
			 */
			oldContext = MemoryContextSwitchTo(aggstate->aggcontext);
			pergroupstate->transValue = datumCopy(newVal,
											 peraggstate->transtypeByVal,
											  peraggstate->transtypeLen);
			pergroupstate->transValueIsNull = false;
			pergroupstate->noTransValue = false;
			MemoryContextSwitchTo(oldContext);
			return;
		}
		if (pergroupstate->transValueIsNull)
		{
			/*
			 * Don't call a strict function with NULL inputs.  Note it is
			 * possible to get here despite the above tests, if the
			 * transfn is strict *and* returned a NULL on a prior cycle.
			 * If that happens we will propagate the NULL all the way to
			 * the end.
			 */
			return;
		}
	}

	oldContext = MemoryContextSwitchTo(aggstate->tmpcontext->ecxt_per_tuple_memory);

	/* OK to call the transition function */
	MemSet(&fcinfo, 0, sizeof(fcinfo));
	fcinfo.flinfo = &peraggstate->transfn;
	fcinfo.nargs = 2;
	fcinfo.arg[0] = pergroupstate->transValue;
	fcinfo.argnull[0] = pergroupstate->transValueIsNull;
	fcinfo.arg[1] = newVal;
	fcinfo.argnull[1] = isNull;

	newVal = FunctionCallInvoke(&fcinfo);

#ifdef NOT_USED

	/*
	 * If the transition function was uncooperative, it may have given us
	 * a pass-by-ref result that points at the scan tuple or the
	 * prior-cycle working memory.	Copy it into the active context if it
	 * doesn't look right.
	 */
	if (!peraggstate->transtypeByVal && !fcinfo.isnull &&
		!MemoryContextContains(CurrentMemoryContext,
							   DatumGetPointer(newVal)))
		newVal = datumCopy(newVal,
						   peraggstate->transtypeByVal,
						   peraggstate->transtypeLen);
#endif

	/*
	 * If pass-by-ref datatype, must copy the new value into aggcontext
	 * and pfree the prior transValue.	But if transfn returned a pointer
	 * to its first input, we don't need to do anything.
	 */
	if (!peraggstate->transtypeByVal &&
	DatumGetPointer(newVal) != DatumGetPointer(pergroupstate->transValue))
	{
		if (!fcinfo.isnull)
		{
			MemoryContextSwitchTo(aggstate->aggcontext);
			newVal = datumCopy(newVal,
							   peraggstate->transtypeByVal,
							   peraggstate->transtypeLen);
		}
		if (!pergroupstate->transValueIsNull)
			pfree(DatumGetPointer(pergroupstate->transValue));
	}

	pergroupstate->transValue = newVal;
	pergroupstate->transValueIsNull = fcinfo.isnull;

	MemoryContextSwitchTo(oldContext);
}


/*
 * Advance all the aggregates for one input tuple.	The input tuple
 * has been stored in tmpcontext->ecxt_scantuple, so that it is accessible
 * to ExecEvalExpr.  pergroup is the array of per-group structs to use
 * (this might be in a hashtable entry).
 *
 * When called, CurrentMemoryContext should be the per-query context.
 */
static void
advance_aggregates(HashAggState * aggstate, AggStatePerGroup pergroup)
{
	ExprContext *econtext = aggstate->tmpcontext;
	int			aggno;

#ifdef AGG_DEBUG_MSGS
	elog(DEBUG, "advance_aggregates");
#endif

	for (aggno = 0; aggno < aggstate->numaggs; aggno++)
	{
		AggStatePerAgg peraggstate = &aggstate->peragg[aggno];
		AggStatePerGroup pergroupstate = &pergroup[aggno];

#ifdef NOT_USED
		AggrefExprState *aggrefstate = peraggstate->aggrefstate;
#endif
		Aggref	   *aggref = peraggstate->aggref;
		Datum		newVal;
		bool		isNull;

#ifdef AGG_DEBUG_MSGS
		elog(DEBUG, "ExecEvalExprSwitchContext: target = %p", aggref->target);
#endif
		newVal = ExecEvalExprSwitchContext(aggref->target, econtext,
										   &isNull, NULL);
#ifdef NOT_USED
		newVal = ExecEvalExprSwitchContext(aggrefstate->target, econtext,
										   &isNull, NULL);
#endif

		if (aggref->aggdistinct)
		{
			/* in DISTINCT mode, we may ignore nulls */
			if (isNull)
				continue;
			tuplesort_putdatum(peraggstate->sortstate, newVal, isNull);
		}
		else
		{
			advance_transition_function(aggstate, peraggstate, pergroupstate,
										newVal, isNull);
		}
	}
}


/*
 * Compute the final value of one aggregate.
 *
 * When called, CurrentMemoryContext should be the context where we want
 * final values delivered (ie, the per-output-tuple expression context).
 */
static void
finalize_one_aggregate(HashAggState * aggstate,
					   AggStatePerAgg peraggstate,
					   AggStatePerGroup pergroupstate,
					   Datum *resultVal, bool *resultIsNull)
{
	MemoryContext oldContext;

#ifdef AGG_DEBUG_MSGS
	elog(DEBUG, "finalize_one_aggregate");
#endif

	oldContext = MemoryContextSwitchTo(aggstate->csstate.cstate.cs_ExprContext->ecxt_per_tuple_memory);

	/*
	 * Apply the agg's finalfn if one is provided, else return transValue.
	 */
	if (OidIsValid(peraggstate->finalfn_oid))
	{
		FunctionCallInfoData fcinfo;

		MemSet(&fcinfo, 0, sizeof(fcinfo));
		fcinfo.flinfo = &peraggstate->finalfn;
		fcinfo.nargs = 1;
		fcinfo.arg[0] = pergroupstate->transValue;
		fcinfo.argnull[0] = pergroupstate->transValueIsNull;
		if (fcinfo.flinfo->fn_strict && pergroupstate->transValueIsNull)
		{
			/* don't call a strict function with NULL inputs */
			*resultVal = (Datum) 0;
			*resultIsNull = true;
		}
		else
		{
			*resultVal = FunctionCallInvoke(&fcinfo);
			*resultIsNull = fcinfo.isnull;
		}
	}
	else
	{
		*resultVal = pergroupstate->transValue;
		*resultIsNull = pergroupstate->transValueIsNull;
	}

	/*
	 * If result is pass-by-ref, make sure it is in the right context.
	 */
	if (!peraggstate->resulttypeByVal && !*resultIsNull &&
		!MemoryContextContains(CurrentMemoryContext,
							   DatumGetPointer(*resultVal)))
		*resultVal = datumCopy(*resultVal,
							   peraggstate->resulttypeByVal,
							   peraggstate->resulttypeLen);

	MemoryContextSwitchTo(oldContext);
}

/*
 * Finalize each aggregate calculation, and stash results in the
 * per-output-tuple context.
 */
static void
finalize_aggregates(HashAggState * aggstate,
					AggStatePerAgg peragg,
					AggStatePerGroup pergroup)
{
	ExprContext *econtext;
	Datum	   *aggvalues;
	bool	   *aggnulls;
	int			aggno;

#ifdef AGG_DEBUG_MSGS
	elog(DEBUG, "finalize_aggregates");
#endif

	econtext = aggstate->csstate.cstate.cs_ExprContext;
	aggvalues = econtext->ecxt_aggvalues;
	aggnulls = econtext->ecxt_aggnulls;

	for (aggno = 0; aggno < aggstate->numaggs; aggno++)
	{
		AggStatePerAgg peraggstate = &peragg[aggno];
		AggStatePerGroup pergroupstate = &pergroup[aggno];

		if (peraggstate->aggref->aggdistinct)
			process_sorted_aggregate(aggstate, peraggstate, pergroupstate);

		finalize_one_aggregate(aggstate, peraggstate, pergroupstate,
							   &aggvalues[aggno], &aggnulls[aggno]);
	}
}


/******************************************************************
 *	   Sorted (old-style) aggregate execution
 ******************************************************************/

/*
 * Run the transition function for a DISTINCT aggregate.  This is called
 * after we have completed entering all the input values into the sort
 * object.	We complete the sort, read out the values in sorted order,
 * and run the transition function on each non-duplicate value.
 *
 * When called, CurrentMemoryContext should be the per-query context.
 *
 * This is code for transfuncs, but it is specific to sorted
 * (old-style) aggs.
 */
static void
process_sorted_aggregate(HashAggState * aggstate,
						 AggStatePerAgg peraggstate,
						 AggStatePerGroup pergroupstate)
{
	Datum		oldVal = (Datum) 0;
	bool		haveOldVal = false;
	MemoryContext workcontext = aggstate->tmpcontext->ecxt_per_tuple_memory;
	MemoryContext oldContext;
	Datum		newVal;
	bool		isNull;

#ifdef AGG_DEBUG_MSGS
	elog(DEBUG, "process_sorted_aggregate");
#endif

	tuplesort_performsort(peraggstate->sortstate);

	/*
	 * Note: if input type is pass-by-ref, the datums returned by the sort
	 * are freshly palloc'd in the per-query context, so we must be
	 * careful to pfree them when they are no longer needed.
	 */

	while (tuplesort_getdatum(peraggstate->sortstate, true,
							  &newVal, &isNull))
	{
		/*
		 * DISTINCT always suppresses nulls, per SQL spec, regardless of
		 * the transition function's strictness.
		 */
		if (isNull)
			continue;

		/*
		 * Clear and select the current working context for evaluation of
		 * the equality function and transition function.
		 */
#ifdef NOT_USED
		MemoryContextReset(aggstate->agg_cxt[aggstate->which_cxt]);
		oldContext =
			MemoryContextSwitchTo(aggstate->agg_cxt[aggstate->which_cxt]);
#endif
		MemoryContextReset(workcontext);
		oldContext = MemoryContextSwitchTo(workcontext);

		if (haveOldVal &&
			DatumGetBool(FunctionCall2(&peraggstate->equalfn,
									   oldVal, newVal)))
		{
			/* equal to prior, so forget this one */
			if (!peraggstate->inputtypeByVal)
				pfree(DatumGetPointer(newVal));

			/*
			 * note we do NOT flip contexts in this case, so no need to
			 * copy prior transValue to other context.
			 */
		}
		else
		{
			advance_transition_function(aggstate, peraggstate, pergroupstate, newVal, false);

#ifdef NOT_USED

			/*
			 * Make the other context current so that this transition
			 * result is preserved.
			 */
			aggstate->which_cxt = 1 - aggstate->which_cxt;
#endif
			/* forget the old value, if any */
			if (haveOldVal && !peraggstate->inputtypeByVal)
				pfree(DatumGetPointer(oldVal));
			oldVal = newVal;
			haveOldVal = true;
		}

		MemoryContextSwitchTo(oldContext);
	}

	if (haveOldVal && !peraggstate->inputtypeByVal)
		pfree(DatumGetPointer(oldVal));

	tuplesort_end(peraggstate->sortstate);
	peraggstate->sortstate = NULL;
}

/*
 * Fetch the next tuple when executing a sorted (old-style) aggregate.
 *
 */
static TupleTableSlot *
exec_sorted_agg(HashAgg * node)
{
	HashAggState *aggstate;
	EState	   *estate;
	Plan	   *outerPlan;
	ExprContext *econtext;
	ExprContext *tmpcontext;
	ProjectionInfo *projInfo;
	Datum	   *aggvalues;
	bool	   *aggnulls;
	AggStatePerAgg peragg;
	AggStatePerGroup pergroup;
	TupleTableSlot *resultSlot;
	HeapTuple	inputTuple;

#ifdef AGG_DEBUG_MSGS
	elog(DEBUG, "exec_sorted_agg");
#endif

	/*
	 * get state info from node
	 */
	aggstate = node->aggstate;
	estate = node->plan.state;
	outerPlan = outerPlan(node);
	econtext = aggstate->csstate.cstate.cs_ExprContext;
	tmpcontext = aggstate->tmpcontext;
	aggvalues = econtext->ecxt_aggvalues;
	aggnulls = econtext->ecxt_aggnulls;
	projInfo = aggstate->csstate.cstate.cs_ProjInfo;
	peragg = aggstate->peragg;
	pergroup = aggstate->pergroup;

	/*
	 * We loop retrieving groups until we find one matching
	 * node->plan.qual
	 */
	do
	{
		if (aggstate->agg_done)
			return NULL;

#ifdef NOT_USED

		/*
		 * Clear the per-output-tuple context for each group
		 */
		MemoryContextReset(aggstate->tup_cxt);
#endif

		/*
		 * Initialize working state for a new input tuple group
		 */
		initialize_aggregates(aggstate, peragg, pergroup);

		inputTuple = NULL;		/* no saved input tuple yet */

		/*
		 * for each tuple from the outer plan, update all the aggregates
		 */
		for (;;)
		{
			TupleTableSlot *outerslot;

			outerslot = ExecProcNode(outerPlan, (Plan *) node);
			if (TupIsNull(outerslot))
				break;
#ifdef NOT_USED
			econtext->ecxt_scantuple = outerslot;
#endif

#ifdef NOT_USED

			/*
			 * Clear and select the current working context for evaluation
			 * of the input expressions and transition functions at this
			 * input tuple.
			 */
			econtext->ecxt_per_tuple_memory =
				aggstate->agg_cxt[aggstate->which_cxt];
			ResetExprContext(econtext);
			oldContext =
				MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
#endif

			/*
			 * Clear the per-output-tuple context.
			 */
#ifdef AGG_DEBUG_MSGS
			elog(DEBUG, "Reset econtext");
#endif
			ResetExprContext(econtext);

			/* set up for next advance_aggregates call */
			tmpcontext->ecxt_scantuple = outerslot;

			advance_aggregates(aggstate, pergroup);
#ifdef AGG_DEBUG_MSGS
			elog(DEBUG, "Reset tmpcontext");
#endif
			ResetExprContext(tmpcontext);

#ifdef NOT_USED
			for (aggno = 0; aggno < aggstate->numaggs; aggno++)
			{
				AggStatePerAgg peraggstate = &peragg[aggno];
				AggStatePerGroup pergroupstate = &pergroup[aggno];
				Aggref	   *aggref = peraggstate->aggref;
				Datum		newVal;

				newVal = ExecEvalExpr(aggref->target, econtext,
									  &isNull, NULL);

				if (aggref->aggdistinct)
				{
					/* in DISTINCT mode, we may ignore nulls */
					if (isNull)
						continue;
					/* putdatum has to be called in per-query context */
					MemoryContextSwitchTo(oldContext);
					tuplesort_putdatum(peraggstate->sortstate,
									   newVal, isNull);
					MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
				}
				else
				{
					advance_transition_function(peraggstate, pergroupstate,
												newVal, isNull);
				}
			}
#endif   /* NOT_USED */

#ifdef NOT_USED

			/*
			 * Make the other context current so that these transition
			 * results are preserved.
			 */
			aggstate->which_cxt = 1 - aggstate->which_cxt;

			MemoryContextSwitchTo(oldContext);
#endif

			/*
			 * Keep a copy of the first input tuple for the projection.
			 * (We only need one since only the GROUP BY columns in it can
			 * be referenced, and these will be the same for all tuples
			 * aggregated over.)
			 */
			if (!inputTuple)
				inputTuple = heap_copytuple(outerslot->val);
		}

		/*
		 * Done scanning input tuple group. Finalize each aggregate
		 * calculation, and stash results in the per-output-tuple context.
		 */
		finalize_aggregates(aggstate, peragg, pergroup);
#ifdef NOT_USED

		/*
		 * This is a bit tricky when there are both DISTINCT and plain
		 * aggregates: we must first finalize all the plain aggs and then
		 * all the DISTINCT ones.  This is needed because the last
		 * transition values for the plain aggs are stored in the
		 * not-current working context, and we have to evaluate those aggs
		 * (and stash the results in the output tup_cxt!) before we start
		 * flipping contexts again in process_sorted_aggregate.
		 */
		oldContext = MemoryContextSwitchTo(aggstate->tup_cxt);
		for (aggno = 0; aggno < aggstate->numaggs; aggno++)
		{
			AggStatePerAgg peraggstate = &peragg[aggno];
			AggStatePerGroup pergroupstate = &pergroup[aggno];

			if (!peraggstate->aggref->aggdistinct)
				finalize_aggregate(peraggstate, pergroupstate,
								   &aggvalues[aggno], &aggnulls[aggno]);
		}
		MemoryContextSwitchTo(oldContext);
		for (aggno = 0; aggno < aggstate->numaggs; aggno++)
		{
			AggStatePerAgg peraggstate = &peragg[aggno];
			AggStatePerGroup pergroupstate = &pergroup[aggno];

			if (peraggstate->aggref->aggdistinct)
			{
				process_sorted_aggregate(aggstate, peraggstate, pergroupstate);
				oldContext = MemoryContextSwitchTo(aggstate->tup_cxt);
				finalize_aggregate(peraggstate, pergroupstate,
								   &aggvalues[aggno], &aggnulls[aggno]);
				MemoryContextSwitchTo(oldContext);
			}
		}
#endif

		/*
		 * If the outerPlan is a Group node, we will reach here after each
		 * group.  We are not done unless the Group node is done (a little
		 * ugliness here while we reach into the Group's state to find
		 * out). Furthermore, when grouping we return nothing at all
		 * unless we had some input tuple(s).  By the nature of Group,
		 * there are no empty groups, so if we get here with no input the
		 * whole scan is empty.
		 *
		 * If the outerPlan isn't a Group, we are done when we get here, and
		 * we will emit a (single) tuple even if there were no input
		 * tuples.
		 */
		if (IsA(outerPlan, Group))
		{
			/* aggregation over groups */
			aggstate->agg_done = ((Group *) outerPlan)->grpstate->grp_done;
			/* check for no groups */
			if (inputTuple == NULL)
				return NULL;
		}
		else if (IsA(outerPlan, Window))		/* @BwndaggWH */
		{
			/* aggregation over windows */
            /*
			aggstate->agg_done = ((Window *) outerPlan)->wndstate->wnd_done;
            */
			/* check for no windows */
			if (inputTuple == NULL)
				return NULL;
		}						/* @EwndaggWH */
		else
		{
			aggstate->agg_done = true;

			/*
			 * If inputtuple==NULL (ie, the outerPlan didn't return
			 * anything), create a dummy all-nulls input tuple for use by
			 * ExecProject. 99.44% of the time this is a waste of cycles,
			 * because ordinarily the projected output tuple's targetlist
			 * cannot contain any direct (non-aggregated) references to
			 * input columns, so the dummy tuple will not be referenced.
			 * However there are special cases where this isn't so --- in
			 * particular an UPDATE involving an aggregate will have a
			 * targetlist reference to ctid.  We need to return a null for
			 * ctid in that situation, not coredump.
			 *
			 * The values returned for the aggregates will be the initial
			 * values of the transition functions.
			 */
			if (inputTuple == NULL)
			{
				TupleDesc	tupType;
				Datum	   *tupValue;
				char	   *null_array;
				AttrNumber	attnum;

				tupType = aggstate->csstate.css_ScanTupleSlot->ttc_tupleDescriptor;
				tupValue = projInfo->pi_tupValue;
				/* watch out for null input tuples, though... */
				if (tupType && tupValue)
				{
					null_array = (char *) palloc(sizeof(char) * tupType->natts);
					for (attnum = 0; attnum < tupType->natts; attnum++)
						null_array[attnum] = 'n';
					inputTuple = heap_formtuple(tupType, tupValue, null_array);
					pfree(null_array);
				}
			}
		}

		/*
		 * Store the representative input tuple in the tuple table slot
		 * reserved for it.  The tuple will be deleted when it is cleared
		 * from the slot.
		 */
		ExecStoreTuple(inputTuple,
					   aggstate->csstate.css_ScanTupleSlot,
					   InvalidBuffer,
					   true);
		econtext->ecxt_scantuple = aggstate->csstate.css_ScanTupleSlot;

#ifdef NOT_USED

		/*
		 * Do projection and qual check in the per-output-tuple context.
		 */
		econtext->ecxt_per_tuple_memory = aggstate->tup_cxt;
#endif

		/*
		 * Form a projection tuple using the aggregate results and the
		 * representative input tuple.	Store it in the result tuple slot.
		 * Note we do not support aggregates returning sets ...
		 */
		resultSlot = ExecProject(projInfo, NULL);

		/*
		 * If the completed tuple does not match the qualifications, it is
		 * ignored and we loop back to try to process another group.
		 * Otherwise, return the tuple.
		 */
	}
	while (!ExecQual(node->plan.qual, econtext, false));

	return resultSlot;
}


/******************************************************************
 *	   Hashed aggregate execution
 ******************************************************************/

/*
 * Initialize the hash table to empty.
 *
 * The hash table always lives in the aggcontext memory context.
 */
void
build_hash_table(HashAgg * node)
{
	HashAggState *aggstate = node->aggstate;
	MemoryContext tmpmem = aggstate->tmpcontext->ecxt_per_tuple_memory;
	Size		entrysize;

#ifdef AGG_DEBUG_MSGS
	elog(DEBUG, "build_hash_table");
#endif

	Assert((node->aggstrategy == AGG_HASHED) ||
		   (node->aggstrategy == AGG_HASHED_WINDOW));
	Assert(node->numGroups > 0);

	entrysize = sizeof(AggHashEntryData) +
		(aggstate->numaggs - 1) * sizeof(AggStatePerGroupData);

	aggstate->entrysize = entrysize;

	aggstate->hashtable = BuildTupleHashTable(node->numCols,
											  node->grpColIdx,
											  aggstate->eqfunctions,
											  node->numGroups,
											  entrysize,
											  aggstate->aggcontext,
											  tmpmem);
}

/*
 * Find or create a hashtable entry for the tuple group containing the
 * given tuple.
 *
 * When called, CurrentMemoryContext should be the per-query context.
 */
static AggHashEntry
lookup_hash_entry(HashAgg * node, TupleTableSlot *slot)
{
	HashAggState *aggstate = node->aggstate;
	AggHashEntry entry;
	bool		isnew;

#ifdef AGG_DEBUG_MSGS
	elog(DEBUG, "lookup_hash_entry");
#endif

	entry = (AggHashEntry) LookupTupleHashEntry(aggstate->hashtable,
												slot,
												&isnew);

	if (isnew)
	{
#ifdef AGG_DEBUG_MSGS
		elog(DEBUG, "lookup_hash_entry: new");
#endif
		/* initialize aggregates for new tuple group */
		initialize_aggregates(aggstate, aggstate->peragg, entry->pergroup);
	}

	return entry;
}

/*
 * ExecAgg for hashed case: phase 1, read input and build hash table
 */
static void
agg_fill_hash_table(HashAgg * node)
{
	HashAggState *aggstate;
	Plan	   *outerPlan;
	ExprContext *tmpcontext;
	AggHashEntry entry;
	TupleTableSlot *outerslot;

#ifdef AGG_DEBUG_MSGS
	elog(DEBUG, "agg_fill_hash_table");
#endif

	/*
	 * get state info from node
	 */
	aggstate = node->aggstate;
	outerPlan = outerPlan(node);
	/* tmpcontext is the per-input-tuple expression context */
	tmpcontext = aggstate->tmpcontext;

	/*
	 * Process each outer-plan tuple, and then fetch the next one, until
	 * we exhaust the outer plan.
	 */
	for (;;)
	{
		outerslot = ExecProcNode(outerPlan, (Plan *) node);
		if (TupIsNull(outerslot))
			break;
		/* set up for advance_aggregates call */
		tmpcontext->ecxt_scantuple = outerslot;

		/* Find or build hashtable entry for this tuple's group */
		entry = lookup_hash_entry(node, outerslot);

		/* Advance the aggregates */
		advance_aggregates(aggstate, entry->pergroup);

		/* Reset per-input-tuple context after each tuple */
		ResetExprContext(tmpcontext);
	}

	aggstate->table_filled = true;
	/* Initialize to walk the hash table */
	ResetTupleHashIterator(&aggstate->hashiter);
}

/*
 * ExecAgg for hashed case: phase 2, retrieving groups from hash table
 */
static TupleTableSlot *
exec_hashed_agg(HashAgg * node)
{
	HashAggState *aggstate;
	EState	   *estate;
	ExprContext *econtext;
	ProjectionInfo *projInfo;
	Datum	   *aggvalues;
	bool	   *aggnulls;
	AggStatePerAgg peragg;
	AggStatePerGroup pergroup;
	TupleHashTable hashtable;
	AggHashEntry entry;
	TupleTableSlot *firstSlot;
	TupleTableSlot *resultSlot;

	/*
	 * get state info from node
	 */
	aggstate = node->aggstate;
	if (!aggstate->table_filled)
		agg_fill_hash_table(node);

	estate = node->plan.state;
	/* econtext is the per-output-tuple expression context */
	econtext = aggstate->csstate.cstate.cs_ExprContext;
	aggvalues = econtext->ecxt_aggvalues;
	aggnulls = econtext->ecxt_aggnulls;
	projInfo = aggstate->csstate.cstate.cs_ProjInfo;
	peragg = aggstate->peragg;
	hashtable = aggstate->hashtable;
	firstSlot = aggstate->csstate.css_ScanTupleSlot;

	/*
	 * We loop retrieving groups until we find one satisfying
	 * node->plan.qual
	 */
	do
	{
		if (aggstate->agg_done)
			return NULL;

		/*
		 * Find the next entry in the hash table
		 */
		entry = (AggHashEntry) ScanTupleHashTable(hashtable,
												  &aggstate->hashiter);
		if (entry == NULL)
		{
			/* No more entries in hashtable, so done */
			aggstate->agg_done = TRUE;
			return NULL;
		}

		/*
		 * Clear the per-output-tuple context for each group
		 */
		ResetExprContext(econtext);

		/*
		 * Store the copied first input tuple in the tuple table slot
		 * reserved for it, so that it can be used in ExecProject.
		 */
		ExecStoreTuple(entry->shared.firstTuple,
					   firstSlot,
					   InvalidBuffer,
					   false);

		pergroup = entry->pergroup;

		/*
		 * Finalize each aggregate calculation, and stash results in the
		 * per-output-tuple context.
		 */
		finalize_aggregates(aggstate, peragg, pergroup);

		/*
		 * Form a projection tuple using the aggregate results and the
		 * representative input tuple.	Store it in the result tuple slot.
		 * Note we do not support aggregates returning sets ...
		 */
		econtext->ecxt_scantuple = firstSlot;
		resultSlot = ExecProject(projInfo, NULL);

		/*
		 * If the completed tuple does not match the qualifications, it is
		 * ignored and we loop back to try to process another group.
		 * Otherwise, return the tuple.
		 */
	}
	while (!ExecQual(node->plan.qual, econtext, false));


	return resultSlot;
}


/* ---------------------------------------
 *
 * ExecAgg -
 *
 *	  ExecAgg receives tuples from its outer subplan and aggregates over
 *	  the appropriate attribute for each aggregate function use (Aggref
 *	  node) appearing in the targetlist or qual of the node.  The number
 *	  of tuples to aggregate over depends on whether a GROUP BY clause is
 *	  present.	We can produce an aggregate result row per group, or just
 *	  one for the whole query.	The value of each aggregate is stored in
 *	  the expression context to be used when ExecProject evaluates the
 *	  result tuple.
 *
 *	  If the outer subplan is a Group node, ExecAgg returns as many tuples
 *	  as there are groups.
 *
 * ------------------------------------------
 */
TupleTableSlot *
ExecHashAgg(HashAgg * node)
{
	HashAggState *aggstate = node->aggstate;

	/* Quick check */
	if (aggstate->agg_done)
		return NULL;

	switch (node->aggstrategy)
	{

		case AGG_HASHED:
#ifdef AGG_DEBUG_MSGS
			elog(DEBUG, "exec_hashed_agg");
#endif
			return exec_hashed_agg(node);


		case AGG_SORTED:
		default:
#ifdef AGG_DEBUG_MSGS
			elog(DEBUG, "exec_sorted_agg");
#endif
			return exec_sorted_agg(node);
	}

}


/* -----------------
 * ExecInitAgg
 *
 *	Creates the run-time information for the agg node produced by the
 *	planner and initializes its outer subtree
 * -----------------
 */
bool
ExecInitHashAgg(HashAgg * node, EState *estate, Plan *parent)
{
	HashAggState *aggstate;
	AggStatePerAgg peragg;
	Plan	   *outerPlan;
	ExprContext *econtext;
	int			numaggs,
				aggno;
	List	   *alist;

#ifdef AGG_DEBUG_MSGS
	elog(DEBUG, "ExecInitAgg");
#endif

	/*
	 * assign the node's execution state
	 */
	node->plan.state = estate;

	/*
	 * create state structure
	 */
	aggstate = makeNode(HashAggState);
	node->aggstate = aggstate;
	aggstate->agg_done = false;

	/*
	 * find aggregates in targetlist and quals
	 *
	 * Note: pull_agg_clauses also checks that no aggs contain other agg
	 * calls in their arguments.  This would make no sense under SQL
	 * semantics anyway (and it's forbidden by the spec).  Because that is
	 * true, we don't need to worry about evaluating the aggs in any
	 * particular order.
	 */
	aggstate->aggs = nconc(pull_agg_clause((Node *) node->plan.targetlist),
						   pull_agg_clause((Node *) node->plan.qual));
	aggstate->numaggs = numaggs = length(aggstate->aggs);
	if (numaggs <= 0)
	{
		/*
		 * This used to be treated as an error, but we can't do that
		 * anymore because constant-expression simplification could
		 * optimize away all of the Aggrefs in the targetlist and qual.
		 * So, just make a debug note, and force numaggs positive so that
		 * palloc()s below don't choke.
		 */
		elog(DEBUG1, "ExecInitAgg: could not find any aggregate functions");
		numaggs = 1;
	}

#ifdef NOT_USED

	/*
	 * Create expression context
	 */
	ExecAssignExprContext(estate, &aggstate->csstate.cstate);

	/*
	 * We actually need three separate expression memory contexts: one for
	 * calculating per-output-tuple values (ie, the finished aggregate
	 * results), and two that we ping-pong between for per-input-tuple
	 * evaluation of input expressions and transition functions.  The
	 * context made by ExecAssignExprContext() is used as the output
	 * context.
	 */
	aggstate->tup_cxt =
		aggstate->csstate.cstate.cs_ExprContext->ecxt_per_tuple_memory;
	aggstate->agg_cxt[0] =
		AllocSetContextCreate(CurrentMemoryContext,
							  "AggExprContext1",
							  ALLOCSET_DEFAULT_MINSIZE,
							  ALLOCSET_DEFAULT_INITSIZE,
							  ALLOCSET_DEFAULT_MAXSIZE);
	aggstate->agg_cxt[1] =
		AllocSetContextCreate(CurrentMemoryContext,
							  "AggExprContext2",
							  ALLOCSET_DEFAULT_MINSIZE,
							  ALLOCSET_DEFAULT_INITSIZE,
							  ALLOCSET_DEFAULT_MAXSIZE);
	aggstate->which_cxt = 0;
#endif   /* NOT_USED */

	/*
	 * Create expression contexts.	We need two, one for per-input-tuple
	 * processing and one for per-output-tuple processing.	We cheat a
	 * little by using ExecAssignExprContext() to build both.
	 */
	ExecAssignExprContext(estate, &aggstate->csstate.cstate);
	aggstate->tmpcontext = aggstate->csstate.cstate.cs_ExprContext;
	ExecAssignExprContext(estate, &aggstate->csstate.cstate);

	/*
	 * We also need a long-lived memory context for holding hashtable data
	 * structures and transition values.  NOTE: the details of what is
	 * stored in aggcontext and what is stored in the regular per-query
	 * memory context are driven by a simple decision: we want to reset
	 * the aggcontext in ExecReScanAgg to recover no-longer-wanted space.
	 */
	aggstate->aggcontext =
		AllocSetContextCreate(CurrentMemoryContext,
							  "AggContext",
							  ALLOCSET_DEFAULT_MINSIZE,
							  ALLOCSET_DEFAULT_INITSIZE,
							  ALLOCSET_DEFAULT_MAXSIZE);


#define AGG_NSLOTS 2

	/*
	 * tuple table initialization
	 */
	ExecInitScanTupleSlot(estate, &aggstate->csstate);
	ExecInitResultTupleSlot(estate, &aggstate->csstate.cstate);

	/*
	 * aggstate->batchSlot = ExecInitExtraTupleSlot(estate); *//* Aid for
	 * Project 2
	 */

	if (!IsPushPlan(node))
	{
		/*
		 * initialize child nodes
		 */
		outerPlan = outerPlan(node);
		ExecInitNode(outerPlan, estate, (Plan *) node);
	}
	
	/*
	 * initialize source tuple type.
	 */
	ExecAssignScanTypeFromOuterPlan((Plan *) node, &aggstate->csstate);

	/*
	 * ExecSetSlotDescriptor(aggstate->batchSlot,			 *//* Aid for
	 * Project 2
	 */
	/* ExecGetTupType(outerPlan), false);  */

	/*
	 * Initialize result tuple type and projection info.
	 */
	ExecAssignResultTypeFromTL((Plan *) node, &aggstate->csstate.cstate);
	ExecAssignProjectionInfo((Plan *) node, &aggstate->csstate.cstate);

	/*
	 * If we are grouping, precompute fmgr lookup data for inner loop
	 */
	aggstate->eqfunctions = NULL;
	if (node->numCols > 0)
	{
#ifdef AGG_DEBUG_MSGS
		int			ii;

		elog(DEBUG, "execTuplesMatchPrepare: numCols = %d, grpColIdx = %p", node->numCols, node->grpColIdx);
		for (ii = 0; ii < node->numCols; ii++)
			elog(DEBUG, "idx[%d] = %d", ii, (int) node->grpColIdx[ii]);
#endif
		aggstate->eqfunctions =
			execTuplesMatchPrepare(ExecGetScanType(&aggstate->csstate),
								   node->numCols,
								   node->grpColIdx);
	}


	/*
	 * Set up aggregate-result storage in the expr context, and also
	 * allocate my private per-agg working storage
	 */
	econtext = aggstate->csstate.cstate.cs_ExprContext;
	econtext->ecxt_aggvalues = (Datum *) palloc(sizeof(Datum) * numaggs);
	MemSet(econtext->ecxt_aggvalues, 0, sizeof(Datum) * numaggs);
	econtext->ecxt_aggnulls = (bool *) palloc(sizeof(bool) * numaggs);
	MemSet(econtext->ecxt_aggnulls, 0, sizeof(bool) * numaggs);

	peragg = (AggStatePerAgg) palloc(sizeof(AggStatePerAggData) * numaggs);
	MemSet(peragg, 0, sizeof(AggStatePerAggData) * numaggs);
	aggstate->peragg = peragg;

	if ((node->aggstrategy == AGG_HASHED) ||
		(node->aggstrategy == AGG_HASHED_WINDOW))
	{
		build_hash_table(node);
		aggstate->table_filled = false;
	}
	else
	{
		AggStatePerGroup pergroup;

		pergroup = (AggStatePerGroup) palloc(sizeof(AggStatePerGroupData) * numaggs);
		MemSet(pergroup, 0, sizeof(AggStatePerGroupData) * numaggs);
		aggstate->pergroup = pergroup;
	}

	/*
	 * Perform lookups of aggregate function info, and initialize the
	 * unchanging fields of the per-agg data
	 */
#ifdef AGG_DEBUG_MSGS
	elog(DEBUG, "Looking up agg function info");
#endif
	aggno = -1;
	foreach(alist, aggstate->aggs)
	{
		Aggref	   *aggref = (Aggref *) lfirst(alist);
		AggStatePerAgg peraggstate = &peragg[++aggno];

		/* AggStatePerGroup pergroupstate = &pergroup[aggno]; */
/*		char	   *aggname = aggref->aggname; */
		HeapTuple	aggTuple;
		Form_pg_aggregate aggform;
		AclResult	aclresult;
		Oid			transfn_oid,
					finalfn_oid;
		Datum		textInitVal;

		/* Mark Aggref node with its associated index in the result array */
		aggref->aggno = aggno;

		/* Fill in the peraggstate data */
		peraggstate->aggref = aggref;

		aggTuple = SearchSysCache(AGGFNOID,
								  ObjectIdGetDatum(aggref->aggfnoid),
								  0, 0, 0);

/*		aggTuple = SearchSysCache(AGGNAME, */
/*								  PointerGetDatum(aggname), */
/*								  ObjectIdGetDatum(aggref->basetype), */
/*								  0, 0); */

		if (!HeapTupleIsValid(aggTuple))
			elog(ERROR, "ExecAgg: cache lookup failed for aggregate %u",
				 aggref->aggfnoid);

		aggform = (Form_pg_aggregate) GETSTRUCT(aggTuple);

		/* Check permission to call aggregate function */
		aclresult = pg_proc_aclcheck(aggref->aggfnoid, GetUserId(),
									 ACL_EXECUTE);

		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult, get_func_name(aggref->aggfnoid));

/*		get_typlenbyval(aggform->aggfinaltype, */
/*						&peraggstate->resulttypeLen, */
/*						&peraggstate->resulttypeByVal); */

		get_typlenbyval(aggref->aggtype,
						&peraggstate->resulttypeLen,
						&peraggstate->resulttypeByVal);

		get_typlenbyval(aggform->aggtranstype,
						&peraggstate->transtypeLen,
						&peraggstate->transtypeByVal);

		/*
		 * initval is potentially null, so don't try to access it as a
		 * struct field. Must do it the hard way with SysCacheGetAttr.
		 */
		textInitVal = SysCacheGetAttr(AGGFNOID, aggTuple,
									  Anum_pg_aggregate_agginitval,
									  &peraggstate->initValueIsNull);

		if (peraggstate->initValueIsNull)
			peraggstate->initValue = (Datum) 0;
		else
			peraggstate->initValue = GetAggInitVal(textInitVal,
												   aggform->aggtranstype);

/*		peraggstate->initValue = */
/*			AggNameGetInitVal(aggname, */
/*							  aggform->aggbasetype, */
/*							  &peraggstate->initValueIsNull); */

		peraggstate->transfn_oid = transfn_oid = aggform->aggtransfn;
		peraggstate->finalfn_oid = finalfn_oid = aggform->aggfinalfn;

		fmgr_info(transfn_oid, &peraggstate->transfn);
		if (OidIsValid(finalfn_oid))
			fmgr_info(finalfn_oid, &peraggstate->finalfn);

		/*
		 * If the transfn is strict and the initval is NULL, make sure
		 * input type and transtype are the same (or at least binary-
		 * compatible), so that it's OK to use the first input value as
		 * the initial transValue.	This should have been checked at agg
		 * definition time, but just in case...
		 */
		if (peraggstate->transfn.fn_strict && peraggstate->initValueIsNull)
		{
			/*
			 * Note: use the type from the input expression here, not
			 * aggform->aggbasetype, because the latter might be 0.
			 * (Consider COUNT(*).)
			 */
			Oid			inputType = exprType(aggref->target);

			if (!IsBinaryCoercible(inputType, aggform->aggtranstype))
				elog(ERROR, "Aggregate %u needs to have compatible input type and transition type",
					 aggref->aggfnoid);

/*			if (inputType != aggform->aggtranstype && */
/*				!IS_BINARY_COMPATIBLE(inputType, aggform->aggtranstype)) */
/*				elog(ERROR, "Aggregate %s needs to have compatible input type and transition type", */
/*					 aggname); */
		}

		if (aggref->aggdistinct)
		{
			/*
			 * Note: use the type from the input expression here, not
			 * aggform->aggbasetype, because the latter might be 0.
			 * (Consider COUNT(*).)
			 */
			Oid			inputType = exprType(aggref->target);
			Oid			eq_function;

			peraggstate->inputType = inputType;
			get_typlenbyval(inputType,
							&peraggstate->inputtypeLen,
							&peraggstate->inputtypeByVal);

			eq_function = compatible_oper_funcid(makeList1(makeString("=")),
												 inputType, inputType,
												 true);

/*			eq_function = compatible_oper_funcid("=", inputType, inputType, */
/*												 true); */

			if (!OidIsValid(eq_function))
				elog(ERROR, "Unable to identify an equality operator for type %s",
					 format_type_be(inputType));

/*			if (!OidIsValid(eq_function)) */
/*				elog(ERROR, "Unable to identify an equality operator for type '%s'", */
/*					 typeidTypeName(inputType)); */
			fmgr_info(eq_function, &(peraggstate->equalfn));
			peraggstate->sortOperator = any_ordering_op(inputType);
			peraggstate->sortstate = NULL;
		}

		ReleaseSysCache(aggTuple);
	}

	return TRUE;
}

static Datum
GetAggInitVal(Datum textInitVal, Oid transtype)
{
	char	   *strInitVal;
	HeapTuple	tup;
	Oid			typinput,
				typelem;
	Datum		initVal;

	strInitVal = DatumGetCString(DirectFunctionCall1(textout, textInitVal));

	tup = SearchSysCache(TYPEOID,
						 ObjectIdGetDatum(transtype),
						 0, 0, 0);
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "GetAggInitVal: cache lookup failed on aggregate transition function return type %u", transtype);

	typinput = ((Form_pg_type) GETSTRUCT(tup))->typinput;
	typelem = ((Form_pg_type) GETSTRUCT(tup))->typelem;
	ReleaseSysCache(tup);

	initVal = OidFunctionCall3(typinput,
							   CStringGetDatum(strInitVal),
							   ObjectIdGetDatum(typelem),
							   Int32GetDatum(-1));

	pfree(strInitVal);
	return initVal;
}

int
ExecCountSlotsHashAgg(HashAgg * node)
{
	if (IsPushPlan(node))
	{
		return AGG_NSLOTS + ExecCountSlotsNode(parentPlan(node));
	}
	
	return ExecCountSlotsNode(outerPlan(node)) +
		ExecCountSlotsNode(innerPlan(node)) +
		AGG_NSLOTS;
}

void
ExecEndHashAgg(HashAgg * node)
{
	HashAggState *aggstate = node->aggstate;
	Plan	   *outerPlan;

#ifdef AGG_DEBUG_MSGS
	elog(DEBUG, "ExecEndAgg");
#endif

	ExecFreeProjectionInfo(&aggstate->csstate.cstate);

#ifdef NOT_USED

	/*
	 * Make sure ExecFreeExprContext() frees the right expr context...
	 */
	aggstate->csstate.cstate.cs_ExprContext->ecxt_per_tuple_memory =
		aggstate->tup_cxt;
	ExecFreeExprContext(&aggstate->csstate.cstate);

	/*
	 * ... and I free the others.
	 */
	MemoryContextDelete(aggstate->agg_cxt[0]);
	MemoryContextDelete(aggstate->agg_cxt[1]);
#endif   /* NOT_USED */

	/*
	 * Free both the expr contexts.
	 */
	ExecFreeExprContext(&aggstate->csstate.cstate);
	aggstate->csstate.cstate.cs_ExprContext = aggstate->tmpcontext;
	ExecFreeExprContext(&aggstate->csstate.cstate);

/*	ExecClearTuple(aggstate->batchSlot);				 /\* Aid for Project 2 *\/ */

	MemoryContextDelete(aggstate->aggcontext);

	outerPlan = outerPlan(node);
	ExecEndNode(outerPlan, (Plan *) node);

	/* clean up tuple table */
	ExecClearTuple(aggstate->csstate.css_ScanTupleSlot);
}

void
ExecReScanHashAgg(HashAgg * node, ExprContext *exprCtxt, Plan *parent)
{
	HashAggState *aggstate = node->aggstate;
	ExprContext *econtext = aggstate->csstate.cstate.cs_ExprContext;

#ifdef AGG_DEBUG_MSGS
	elog(DEBUG, "ExecReScanAgg");
#endif

	aggstate->agg_done = false;
	MemSet(econtext->ecxt_aggvalues, 0, sizeof(Datum) * aggstate->numaggs);
	MemSet(econtext->ecxt_aggnulls, 0, sizeof(bool) * aggstate->numaggs);

	/* BEGIN: CHECK */
	MemoryContextReset(aggstate->aggcontext);

	if ((node->aggstrategy == AGG_HASHED) ||
		(node->aggstrategy == AGG_HASHED_WINDOW));
	{
		build_hash_table(node);
		aggstate->table_filled = false;
	}

/*	   ExecClearTuple(aggstate->batchSlot); */
	/* END: CHECK */

	/*
	 * if chgParam of subnode is not null then plan will be re-scanned by
	 * first ExecProcNode.
	 */
	if (((Plan *) node)->lefttree->chgParam == NULL)
		ExecReScan(((Plan *) node)->lefttree, exprCtxt, (Plan *) node);
}

 /* static */ void
hash_write_tuple(BufFile *file,
				 TupleTableSlot *tupleSlot)
{
	size_t		written;
	HeapTuple	heapTuple = tupleSlot->val;

	written = BufFileWrite(file, (void *) heapTuple, sizeof(HeapTupleData));
	if (written != sizeof(HeapTupleData))
		elog(ERROR, "Write to agg temp file failed");
	written = BufFileWrite(file, (void *) heapTuple->t_data, heapTuple->t_len);
	if (written != (size_t) heapTuple->t_len)
		elog(ERROR, "Write to agg temp file failed");
}

 /* static */ TupleTableSlot *
hash_read_tuple(BufFile *file,
				TupleTableSlot *tupleSlot)
{
	HeapTupleData htup;
	size_t		nread;
	HeapTuple	heapTuple;

	nread = BufFileRead(file, (void *) &htup, sizeof(HeapTupleData));
	if (nread == 0)
		return NULL;			/* end of file */
	if (nread != sizeof(HeapTupleData))
		elog(ERROR, "Read from agg temp file failed");
	heapTuple = palloc(HEAPTUPLESIZE + htup.t_len);
	memcpy((char *) heapTuple, (char *) &htup, sizeof(HeapTupleData));
	heapTuple->t_datamcxt = CurrentMemoryContext;
	heapTuple->t_data = (HeapTupleHeader)
		((char *) heapTuple + HEAPTUPLESIZE);
	nread = BufFileRead(file, (void *) heapTuple->t_data, htup.t_len);
	if (nread != (size_t) htup.t_len)
		elog(ERROR, "Read from agg temp file failed");
	return ExecStoreTuple(heapTuple, tupleSlot, InvalidBuffer, true);
}
