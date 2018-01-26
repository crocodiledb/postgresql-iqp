/*-------------------------------------------------------------------------
 *
 * nodeAggInc.c
 *	  Routines to handle aggregate nodes (incremental versions)
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeAggInc.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "executor/nodeAgg.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/tlist.h"
#include "parser/parse_agg.h"
#include "parser/parse_coerce.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/tuplesort.h"
#include "utils/datum.h"

#include "executor/incmeta.h"

/*
 * AggStatePerTransData - per aggregate state value information
 *
 * Working state for updating the aggregate's state value, by calling the
 * transition function with an input row. This struct does not store the
 * information needed to produce the final aggregate result from the transition
 * state, that's stored in AggStatePerAggData instead. This separation allows
 * multiple aggregate results to be produced from a single state value.
 */
typedef struct AggStatePerTransData
{
	/*
	 * These values are set up during ExecInitAgg() and do not change
	 * thereafter:
	 */

	/*
	 * Link to an Aggref expr this state value is for.
	 *
	 * There can be multiple Aggref's sharing the same state value, as long as
	 * the inputs and transition function are identical. This points to the
	 * first one of them.
	 */
	Aggref	   *aggref;

	/*
	 * Nominal number of arguments for aggregate function.  For plain aggs,
	 * this excludes any ORDER BY expressions.  For ordered-set aggs, this
	 * counts both the direct and aggregated (ORDER BY) arguments.
	 */
	int			numArguments;

	/*
	 * Number of aggregated input columns.  This includes ORDER BY expressions
	 * in both the plain-agg and ordered-set cases.  Ordered-set direct args
	 * are not counted, though.
	 */
	int			numInputs;

	/*
	 * Number of aggregated input columns to pass to the transfn.  This
	 * includes the ORDER BY columns for ordered-set aggs, but not for plain
	 * aggs.  (This doesn't count the transition state value!)
	 */
	int			numTransInputs;

	/*
	 * At each input row, we perform a single ExecProject call to evaluate all
	 * argument expressions that will certainly be needed at this row; that
	 * includes this aggregate's filter expression if it has one, or its
	 * regular argument expressions (including any ORDER BY columns) if it
	 * doesn't.  inputoff is the starting index of this aggregate's required
	 * expressions in the resulting tuple.
	 */
	int			inputoff;

	/* Oid of the state transition or combine function */
	Oid			transfn_oid;

	/* Oid of the serialization function or InvalidOid */
	Oid			serialfn_oid;

	/* Oid of the deserialization function or InvalidOid */
	Oid			deserialfn_oid;

	/* Oid of state value's datatype */
	Oid			aggtranstype;

	/* ExprStates for any direct-argument expressions */
	List	   *aggdirectargs;

	/*
	 * fmgr lookup data for transition function or combine function.  Note in
	 * particular that the fn_strict flag is kept here.
	 */
	FmgrInfo	transfn;

	/* fmgr lookup data for serialization function */
	FmgrInfo	serialfn;

	/* fmgr lookup data for deserialization function */
	FmgrInfo	deserialfn;

	/* Input collation derived for aggregate */
	Oid			aggCollation;

	/* number of sorting columns */
	int			numSortCols;

	/* number of sorting columns to consider in DISTINCT comparisons */
	/* (this is either zero or the same as numSortCols) */
	int			numDistinctCols;

	/* deconstructed sorting information (arrays of length numSortCols) */
	AttrNumber *sortColIdx;
	Oid		   *sortOperators;
	Oid		   *sortCollations;
	bool	   *sortNullsFirst;

	/*
	 * fmgr lookup data for input columns' equality operators --- only
	 * set/used when aggregate has DISTINCT flag.  Note that these are in
	 * order of sort column index, not parameter index.
	 */
	FmgrInfo   *equalfns;		/* array of length numDistinctCols */

	/*
	 * initial value from pg_aggregate entry
	 */
	Datum		initValue;
	bool		initValueIsNull;

	/*
	 * We need the len and byval info for the agg's input and transition data
	 * types in order to know how to copy/delete values.
	 *
	 * Note that the info for the input type is used only when handling
	 * DISTINCT aggs with just one argument, so there is only one input type.
	 */
	int16		inputtypeLen,
				transtypeLen;
	bool		inputtypeByVal,
				transtypeByVal;

	/*
	 * Stuff for evaluation of aggregate inputs, when they must be evaluated
	 * separately because there's a FILTER expression.  In such cases we will
	 * create a sortslot and the result will be stored there, whether or not
	 * we're actually sorting.
	 */
	ProjectionInfo *evalproj;	/* projection machinery */

	/*
	 * Slots for holding the evaluated input arguments.  These are set up
	 * during ExecInitAgg() and then used for each input row requiring either
	 * FILTER or ORDER BY/DISTINCT processing.
	 */
	TupleTableSlot *sortslot;	/* current input tuple */
	TupleTableSlot *uniqslot;	/* used for multi-column DISTINCT */
	TupleDesc	sortdesc;		/* descriptor of input tuples */

	/*
	 * These values are working state that is initialized at the start of an
	 * input tuple group and updated for each input tuple.
	 *
	 * For a simple (non DISTINCT/ORDER BY) aggregate, we just feed the input
	 * values straight to the transition function.  If it's DISTINCT or
	 * requires ORDER BY, we pass the input values into a Tuplesort object;
	 * then at completion of the input tuple group, we scan the sorted values,
	 * eliminate duplicates if needed, and run the transition function on the
	 * rest.
	 *
	 * We need a separate tuplesort for each grouping set.
	 */

	Tuplesortstate **sortstates;	/* sort objects, if DISTINCT or ORDER BY */

	/*
	 * This field is a pre-initialized FunctionCallInfo struct used for
	 * calling this aggregate's transfn.  We save a few cycles per row by not
	 * re-initializing the unchanging fields; which isn't much, but it seems
	 * worth the extra space consumption.
	 */
	FunctionCallInfoData transfn_fcinfo;

	/* Likewise for serialization and deserialization functions */
	FunctionCallInfoData serialfn_fcinfo;

	FunctionCallInfoData deserialfn_fcinfo;
}			AggStatePerTransData;

/*
 * AggStatePerAggData - per-aggregate information
 *
 * This contains the information needed to call the final function, to produce
 * a final aggregate result from the state value. If there are multiple
 * identical Aggrefs in the query, they can all share the same per-agg data.
 *
 * These values are set up during ExecInitAgg() and do not change thereafter.
 */
typedef struct AggStatePerAggData
{
	/*
	 * Link to an Aggref expr this state value is for.
	 *
	 * There can be multiple identical Aggref's sharing the same per-agg. This
	 * points to the first one of them.
	 */
	Aggref	   *aggref;

	/* index to the state value which this agg should use */
	int			transno;

	/* Optional Oid of final function (may be InvalidOid) */
	Oid			finalfn_oid;

	/*
	 * fmgr lookup data for final function --- only valid when finalfn_oid oid
	 * is not InvalidOid.
	 */
	FmgrInfo	finalfn;

	/*
	 * Number of arguments to pass to the finalfn.  This is always at least 1
	 * (the transition state value) plus any ordered-set direct args. If the
	 * finalfn wants extra args then we pass nulls corresponding to the
	 * aggregated input columns.
	 */
	int			numFinalArgs;

	/*
	 * We need the len and byval info for the agg's result data type in order
	 * to know how to copy/delete values.
	 */
	int16		resulttypeLen;
	bool		resulttypeByVal;

}			AggStatePerAggData;

/*
 * AggStatePerGroupData - per-aggregate-per-group working state
 *
 * These values are working state that is initialized at the start of
 * an input tuple group and updated for each input tuple.
 *
 * In AGG_PLAIN and AGG_SORTED modes, we have a single array of these
 * structs (pointed to by aggstate->pergroup); we re-use the array for
 * each input group, if it's AGG_SORTED mode.  In AGG_HASHED mode, the
 * hash table contains an array of these structs for each tuple group.
 *
 * Logically, the sortstate field belongs in this struct, but we do not
 * keep it here for space reasons: we don't support DISTINCT aggregates
 * in AGG_HASHED mode, so there's no reason to use up a pointer field
 * in every entry of the hashtable.
 */
typedef struct AggStatePerGroupData
{
	Datum		transValue;		/* current transition value */
	bool		transValueIsNull;

	bool		noTransValue;	/* true if transValue not set yet */

	/*
	 * Note: noTransValue initially has the same value as transValueIsNull,
	 * and if true both are cleared to false at the same time.  They are not
	 * the same though: if transfn later returns a NULL, we want to keep that
	 * NULL and not auto-replace it with a later input value. Only the first
	 * non-NULL input will be auto-substituted.
	 */
}			AggStatePerGroupData;

/*
 * AggStatePerPhaseData - per-grouping-set-phase state
 *
 * Grouping sets are divided into "phases", where a single phase can be
 * processed in one pass over the input. If there is more than one phase, then
 * at the end of input from the current phase, state is reset and another pass
 * taken over the data which has been re-sorted in the mean time.
 *
 * Accordingly, each phase specifies a list of grouping sets and group clause
 * information, plus each phase after the first also has a sort order.
 */
typedef struct AggStatePerPhaseData
{
	AggStrategy aggstrategy;	/* strategy for this phase */
	int			numsets;		/* number of grouping sets (or 0) */
	int		   *gset_lengths;	/* lengths of grouping sets */
	Bitmapset **grouped_cols;	/* column groupings for rollup */
	FmgrInfo   *eqfunctions;	/* per-grouping-field equality fns */
	Agg		   *aggnode;		/* Agg node for phase data */
	Sort	   *sortnode;		/* Sort node for input ordering for phase */
}			AggStatePerPhaseData;

/*
 * AggStatePerHashData - per-hashtable state
 *
 * When doing grouping sets with hashing, we have one of these for each
 * grouping set. (When doing hashing without grouping sets, we have just one of
 * them.)
 */
typedef struct AggStatePerHashData
{
	TupleHashTable hashtable;	/* hash table with one entry per group */
	TupleHashIterator hashiter; /* for iterating through hash table */
	TupleTableSlot *hashslot;	/* slot for loading hash table */
	FmgrInfo   *hashfunctions;	/* per-grouping-field hash fns */
	FmgrInfo   *eqfunctions;	/* per-grouping-field equality fns */
	int			numCols;		/* number of hash key columns */
	int			numhashGrpCols; /* number of columns in hash table */
	int			largestGrpColIdx;	/* largest col required for hashing */
	AttrNumber *hashGrpColIdxInput; /* hash col indices in input slot */
	AttrNumber *hashGrpColIdxHash;	/* indices in hashtbl tuples */
	Agg		   *aggnode;		/* original Agg node, for numGroups etc. */
}			AggStatePerHashData;


static void select_current_set(AggState *aggstate, int setno, bool is_hash);
static void initialize_phase(AggState *aggstate, int newphase);
static TupleTableSlot *fetch_input_tuple(AggState *aggstate);
static void initialize_aggregates(AggState *aggstate,
					  AggStatePerGroup pergroup,
					  int numReset);
static void advance_transition_function(AggState *aggstate,
							AggStatePerTrans pertrans,
							AggStatePerGroup pergroupstate);
static void advance_aggregates(AggState *aggstate, AggStatePerGroup pergroup,
				   AggStatePerGroup *pergroups);
static void advance_combine_function(AggState *aggstate,
						 AggStatePerTrans pertrans,
						 AggStatePerGroup pergroupstate);
static void combine_aggregates(AggState *aggstate, AggStatePerGroup pergroup);
static void process_ordered_aggregate_single(AggState *aggstate,
								 AggStatePerTrans pertrans,
								 AggStatePerGroup pergroupstate);
static void process_ordered_aggregate_multi(AggState *aggstate,
								AggStatePerTrans pertrans,
								AggStatePerGroup pergroupstate);
static void finalize_aggregate(AggState *aggstate,
				   AggStatePerAgg peragg,
				   AggStatePerGroup pergroupstate,
				   Datum *resultVal, bool *resultIsNull);
static void finalize_partialaggregate(AggState *aggstate,
						  AggStatePerAgg peragg,
						  AggStatePerGroup pergroupstate,
						  Datum *resultVal, bool *resultIsNull);
static void prepare_projection_slot(AggState *aggstate,
						TupleTableSlot *slot,
						int currentSet);
static void finalize_aggregates(AggState *aggstate,
					AggStatePerAgg peragg,
					AggStatePerGroup pergroup);
static TupleTableSlot *project_aggregates(AggState *aggstate);
static Bitmapset *find_unaggregated_cols(AggState *aggstate);
static bool find_unaggregated_cols_walker(Node *node, Bitmapset **colnos);
static void build_hash_table(AggState *aggstate);
static TupleHashEntryData *lookup_hash_entry(AggState *aggstate);
static AggStatePerGroup *lookup_hash_entries(AggState *aggstate);
static TupleTableSlot *agg_retrieve_direct(AggState *aggstate);
static void agg_fill_hash_table(AggState *aggstate);
static TupleTableSlot *agg_retrieve_hash_table(AggState *aggstate);


/*
 * Select the current grouping set; affects current_set and
 * curaggcontext.
 */
static void
select_current_set(AggState *aggstate, int setno, bool is_hash)
{
	if (is_hash)
		aggstate->curaggcontext = aggstate->hashcontext;
	else
		aggstate->curaggcontext = aggstate->aggcontexts[setno];

	aggstate->current_set = setno;
}

/*
 * Switch to phase "newphase", which must either be 0 or 1 (to reset) or
 * current_phase + 1. Juggle the tuplesorts accordingly.
 *
 * Phase 0 is for hashing, which we currently handle last in the AGG_MIXED
 * case, so when entering phase 0, all we need to do is drop open sorts.
 */
static void
initialize_phase(AggState *aggstate, int newphase)
{
	Assert(newphase <= 1 || newphase == aggstate->current_phase + 1);

	/*
	 * Whatever the previous state, we're now done with whatever input
	 * tuplesort was in use.
	 */
	if (aggstate->sort_in)
	{
		tuplesort_end(aggstate->sort_in);
		aggstate->sort_in = NULL;
	}

	if (newphase <= 1)
	{
		/*
		 * Discard any existing output tuplesort.
		 */
		if (aggstate->sort_out)
		{
			tuplesort_end(aggstate->sort_out);
			aggstate->sort_out = NULL;
		}
	}
	else
	{
		/*
		 * The old output tuplesort becomes the new input one, and this is the
		 * right time to actually sort it.
		 */
		aggstate->sort_in = aggstate->sort_out;
		aggstate->sort_out = NULL;
		Assert(aggstate->sort_in);
		tuplesort_performsort(aggstate->sort_in);
	}

	/*
	 * If this isn't the last phase, we need to sort appropriately for the
	 * next phase in sequence.
	 */
	if (newphase > 0 && newphase < aggstate->numphases - 1)
	{
		Sort	   *sortnode = aggstate->phases[newphase + 1].sortnode;
		PlanState  *outerNode = outerPlanState(aggstate);
		TupleDesc	tupDesc = ExecGetResultType(outerNode);

		aggstate->sort_out = tuplesort_begin_heap(tupDesc,
												  sortnode->numCols,
												  sortnode->sortColIdx,
												  sortnode->sortOperators,
												  sortnode->collations,
												  sortnode->nullsFirst,
												  work_mem,
												  false);
	}

	aggstate->current_phase = newphase;
	aggstate->phase = &aggstate->phases[newphase];
}

/*
 * Fetch a tuple from either the outer plan (for phase 1) or from the sorter
 * populated by the previous phase.  Copy it to the sorter for the next phase
 * if any.
 *
 * Callers cannot rely on memory for tuple in returned slot remaining valid
 * past any subsequently fetched tuple.
 */
static TupleTableSlot *
fetch_input_tuple(AggState *aggstate)
{
	TupleTableSlot *slot;

	if (aggstate->sort_in)
	{
		/* make sure we check for interrupts in either path through here */
		CHECK_FOR_INTERRUPTS();
		if (!tuplesort_gettupleslot(aggstate->sort_in, true, false,
									aggstate->sort_slot, NULL))
			return NULL;
		slot = aggstate->sort_slot;
	}
	else
		slot = ExecProcNode(outerPlanState(aggstate));

	if (!TupIsNull(slot) && aggstate->sort_out)
		tuplesort_puttupleslot(aggstate->sort_out, slot);

	return slot;
}

/*
 * (Re)Initialize an individual aggregate.
 *
 * This function handles only one grouping set, already set in
 * aggstate->current_set.
 *
 * When called, CurrentMemoryContext should be the per-query context.
 */
static void
initialize_aggregate(AggState *aggstate, AggStatePerTrans pertrans,
					 AggStatePerGroup pergroupstate)
{
	/*
	 * Start a fresh sort operation for each DISTINCT/ORDER BY aggregate.
	 */
	if (pertrans->numSortCols > 0)
	{
		/*
		 * In case of rescan, maybe there could be an uncompleted sort
		 * operation?  Clean it up if so.
		 */
		if (pertrans->sortstates[aggstate->current_set])
			tuplesort_end(pertrans->sortstates[aggstate->current_set]);


		/*
		 * We use a plain Datum sorter when there's a single input column;
		 * otherwise sort the full tuple.  (See comments for
		 * process_ordered_aggregate_single.)
		 */
		if (pertrans->numInputs == 1)
			pertrans->sortstates[aggstate->current_set] =
				tuplesort_begin_datum(pertrans->sortdesc->attrs[0]->atttypid,
									  pertrans->sortOperators[0],
									  pertrans->sortCollations[0],
									  pertrans->sortNullsFirst[0],
									  work_mem, false);
		else
			pertrans->sortstates[aggstate->current_set] =
				tuplesort_begin_heap(pertrans->sortdesc,
									 pertrans->numSortCols,
									 pertrans->sortColIdx,
									 pertrans->sortOperators,
									 pertrans->sortCollations,
									 pertrans->sortNullsFirst,
									 work_mem, false);
	}

	/*
	 * (Re)set transValue to the initial value.
	 *
	 * Note that when the initial value is pass-by-ref, we must copy it (into
	 * the aggcontext) since we will pfree the transValue later.
	 */
	if (pertrans->initValueIsNull)
		pergroupstate->transValue = pertrans->initValue;
	else
	{
		MemoryContext oldContext;

		oldContext = MemoryContextSwitchTo(
										   aggstate->curaggcontext->ecxt_per_tuple_memory);
		pergroupstate->transValue = datumCopy(pertrans->initValue,
											  pertrans->transtypeByVal,
											  pertrans->transtypeLen);
		MemoryContextSwitchTo(oldContext);
	}
	pergroupstate->transValueIsNull = pertrans->initValueIsNull;

	/*
	 * If the initial value for the transition state doesn't exist in the
	 * pg_aggregate table then we will let the first non-NULL value returned
	 * from the outer procNode become the initial value. (This is useful for
	 * aggregates like max() and min().) The noTransValue flag signals that we
	 * still need to do this.
	 */
	pergroupstate->noTransValue = pertrans->initValueIsNull;
}

/*
 * Initialize all aggregate transition states for a new group of input values.
 *
 * If there are multiple grouping sets, we initialize only the first numReset
 * of them (the grouping sets are ordered so that the most specific one, which
 * is reset most often, is first). As a convenience, if numReset is 0, we
 * reinitialize all sets. numReset is -1 to initialize a hashtable entry, in
 * which case the caller must have used select_current_set appropriately.
 *
 * When called, CurrentMemoryContext should be the per-query context.
 */
static void
initialize_aggregates(AggState *aggstate,
					  AggStatePerGroup pergroup,
					  int numReset)
{
	int			transno;
	int			numGroupingSets = Max(aggstate->phase->numsets, 1);
	int			setno = 0;
	int			numTrans = aggstate->numtrans;
	AggStatePerTrans transstates = aggstate->pertrans;

	if (numReset == 0)
		numReset = numGroupingSets;

	for (transno = 0; transno < numTrans; transno++)
	{
		AggStatePerTrans pertrans = &transstates[transno];

		if (numReset < 0)
		{
			AggStatePerGroup pergroupstate;

			pergroupstate = &pergroup[transno];

			initialize_aggregate(aggstate, pertrans, pergroupstate);
		}
		else
		{
			for (setno = 0; setno < numReset; setno++)
			{
				AggStatePerGroup pergroupstate;

				pergroupstate = &pergroup[transno + (setno * numTrans)];

				select_current_set(aggstate, setno, false);

				initialize_aggregate(aggstate, pertrans, pergroupstate);
			}
		}
	}
}

/*
 * Given new input value(s), advance the transition function of one aggregate
 * state within one grouping set only (already set in aggstate->current_set)
 *
 * The new values (and null flags) have been preloaded into argument positions
 * 1 and up in pertrans->transfn_fcinfo, so that we needn't copy them again to
 * pass to the transition function.  We also expect that the static fields of
 * the fcinfo are already initialized; that was done by ExecInitAgg().
 *
 * It doesn't matter which memory context this is called in.
 */
static void
advance_transition_function(AggState *aggstate,
							AggStatePerTrans pertrans,
							AggStatePerGroup pergroupstate)
{
	FunctionCallInfo fcinfo = &pertrans->transfn_fcinfo;
	MemoryContext oldContext;
	Datum		newVal;

	if (pertrans->transfn.fn_strict)
	{
		/*
		 * For a strict transfn, nothing happens when there's a NULL input; we
		 * just keep the prior transValue.
		 */
		int			numTransInputs = pertrans->numTransInputs;
		int			i;

		for (i = 1; i <= numTransInputs; i++)
		{
			if (fcinfo->argnull[i])
				return;
		}
		if (pergroupstate->noTransValue)
		{
			/*
			 * transValue has not been initialized. This is the first non-NULL
			 * input value. We use it as the initial value for transValue. (We
			 * already checked that the agg's input type is binary-compatible
			 * with its transtype, so straight copy here is OK.)
			 *
			 * We must copy the datum into aggcontext if it is pass-by-ref. We
			 * do not need to pfree the old transValue, since it's NULL.
			 */
			oldContext = MemoryContextSwitchTo(
											   aggstate->curaggcontext->ecxt_per_tuple_memory);
			pergroupstate->transValue = datumCopy(fcinfo->arg[1],
												  pertrans->transtypeByVal,
												  pertrans->transtypeLen);
			pergroupstate->transValueIsNull = false;
			pergroupstate->noTransValue = false;
			MemoryContextSwitchTo(oldContext);
			return;
		}
		if (pergroupstate->transValueIsNull)
		{
			/*
			 * Don't call a strict function with NULL inputs.  Note it is
			 * possible to get here despite the above tests, if the transfn is
			 * strict *and* returned a NULL on a prior cycle. If that happens
			 * we will propagate the NULL all the way to the end.
			 */
			return;
		}
	}

	/* We run the transition functions in per-input-tuple memory context */
	oldContext = MemoryContextSwitchTo(aggstate->tmpcontext->ecxt_per_tuple_memory);

	/* set up aggstate->curpertrans for AggGetAggref() */
	aggstate->curpertrans = pertrans;

	/*
	 * OK to call the transition function
	 */
	fcinfo->arg[0] = pergroupstate->transValue;
	fcinfo->argnull[0] = pergroupstate->transValueIsNull;
	fcinfo->isnull = false;		/* just in case transfn doesn't set it */

	newVal = FunctionCallInvoke(fcinfo);

	aggstate->curpertrans = NULL;

	/*
	 * If pass-by-ref datatype, must copy the new value into aggcontext and
	 * free the prior transValue.  But if transfn returned a pointer to its
	 * first input, we don't need to do anything.  Also, if transfn returned a
	 * pointer to a R/W expanded object that is already a child of the
	 * aggcontext, assume we can adopt that value without copying it.
	 */
	if (!pertrans->transtypeByVal &&
		DatumGetPointer(newVal) != DatumGetPointer(pergroupstate->transValue))
	{
		if (!fcinfo->isnull)
		{
			MemoryContextSwitchTo(aggstate->curaggcontext->ecxt_per_tuple_memory);
			if (DatumIsReadWriteExpandedObject(newVal,
											   false,
											   pertrans->transtypeLen) &&
				MemoryContextGetParent(DatumGetEOHP(newVal)->eoh_context) == CurrentMemoryContext)
				 /* do nothing */ ;
			else
				newVal = datumCopy(newVal,
								   pertrans->transtypeByVal,
								   pertrans->transtypeLen);
		}
		if (!pergroupstate->transValueIsNull)
		{
			if (DatumIsReadWriteExpandedObject(pergroupstate->transValue,
											   false,
											   pertrans->transtypeLen))
				DeleteExpandedObject(pergroupstate->transValue);
			else
				pfree(DatumGetPointer(pergroupstate->transValue));
		}
	}

	pergroupstate->transValue = newVal;
	pergroupstate->transValueIsNull = fcinfo->isnull;

	MemoryContextSwitchTo(oldContext);
}

/*
 * Advance each aggregate transition state for one input tuple.  The input
 * tuple has been stored in tmpcontext->ecxt_outertuple, so that it is
 * accessible to ExecEvalExpr.
 *
 * We have two sets of transition states to handle: one for sorted aggregation
 * and one for hashed; we do them both here, to avoid multiple evaluation of
 * the inputs.
 *
 * When called, CurrentMemoryContext should be the per-query context.
 */
static void
advance_aggregates(AggState *aggstate, AggStatePerGroup pergroup, AggStatePerGroup *pergroups)
{
	int			transno;
	int			setno = 0;
	int			numGroupingSets = Max(aggstate->phase->numsets, 1);
	int			numHashes = aggstate->num_hashes;
	int			numTrans = aggstate->numtrans;
	TupleTableSlot *combinedslot;

	/* compute required inputs for all aggregates */
	combinedslot = ExecProject(aggstate->combinedproj);

	for (transno = 0; transno < numTrans; transno++)
	{
		AggStatePerTrans pertrans = &aggstate->pertrans[transno];
		int			numTransInputs = pertrans->numTransInputs;
		int			inputoff = pertrans->inputoff;
		TupleTableSlot *slot;
		int			i;

		/* Skip anything FILTERed out */
		if (pertrans->aggref->aggfilter)
		{
			/* Check the result of the filter expression */
			if (combinedslot->tts_isnull[inputoff] ||
				!DatumGetBool(combinedslot->tts_values[inputoff]))
				continue;

			/* Now it's safe to evaluate this agg's arguments */
			slot = ExecProject(pertrans->evalproj);
			/* There's no offset needed in this slot, of course */
			inputoff = 0;
		}
		else
		{
			/* arguments are already evaluated into combinedslot @ inputoff */
			slot = combinedslot;
		}

		if (pertrans->numSortCols > 0)
		{
			/* DISTINCT and/or ORDER BY case */
			Assert(slot->tts_nvalid >= (pertrans->numInputs + inputoff));
			Assert(!pergroups);

			/*
			 * If the transfn is strict, we want to check for nullity before
			 * storing the row in the sorter, to save space if there are a lot
			 * of nulls.  Note that we must only check numTransInputs columns,
			 * not numInputs, since nullity in columns used only for sorting
			 * is not relevant here.
			 */
			if (pertrans->transfn.fn_strict)
			{
				for (i = 0; i < numTransInputs; i++)
				{
					if (slot->tts_isnull[i + inputoff])
						break;
				}
				if (i < numTransInputs)
					continue;
			}

			for (setno = 0; setno < numGroupingSets; setno++)
			{
				/* OK, put the tuple into the tuplesort object */
				if (pertrans->numInputs == 1)
					tuplesort_putdatum(pertrans->sortstates[setno],
									   slot->tts_values[inputoff],
									   slot->tts_isnull[inputoff]);
				else if (pertrans->aggref->aggfilter)
				{
					/*
					 * When filtering and ordering, we already have a slot
					 * containing just the argument columns.
					 */
					Assert(slot == pertrans->sortslot);
					tuplesort_puttupleslot(pertrans->sortstates[setno], slot);
				}
				else
				{
					/*
					 * Copy argument columns from combined slot, starting at
					 * inputoff, into sortslot, so that we can store just the
					 * columns we want.
					 */
					ExecClearTuple(pertrans->sortslot);
					memcpy(pertrans->sortslot->tts_values,
						   &slot->tts_values[inputoff],
						   pertrans->numInputs * sizeof(Datum));
					memcpy(pertrans->sortslot->tts_isnull,
						   &slot->tts_isnull[inputoff],
						   pertrans->numInputs * sizeof(bool));
					ExecStoreVirtualTuple(pertrans->sortslot);
					tuplesort_puttupleslot(pertrans->sortstates[setno],
										   pertrans->sortslot);
				}
			}
		}
		else
		{
			/* We can apply the transition function immediately */
			FunctionCallInfo fcinfo = &pertrans->transfn_fcinfo;

			/* Load values into fcinfo */
			/* Start from 1, since the 0th arg will be the transition value */
			Assert(slot->tts_nvalid >= (numTransInputs + inputoff));

			for (i = 0; i < numTransInputs; i++)
			{
				fcinfo->arg[i + 1] = slot->tts_values[i + inputoff];
				fcinfo->argnull[i + 1] = slot->tts_isnull[i + inputoff];
			}

			if (pergroup)
			{
				/* advance transition states for ordered grouping */

				for (setno = 0; setno < numGroupingSets; setno++)
				{
					AggStatePerGroup pergroupstate;

					select_current_set(aggstate, setno, false);

					pergroupstate = &pergroup[transno + (setno * numTrans)];

					advance_transition_function(aggstate, pertrans, pergroupstate);
				}
			}

			if (pergroups)
			{
				/* advance transition states for hashed grouping */

				for (setno = 0; setno < numHashes; setno++)
				{
					AggStatePerGroup pergroupstate;

					select_current_set(aggstate, setno, true);

					pergroupstate = &pergroups[setno][transno];

					advance_transition_function(aggstate, pertrans, pergroupstate);
				}
			}
		}
	}
}

/*
 * combine_aggregates replaces advance_aggregates in DO_AGGSPLIT_COMBINE
 * mode.  The principal difference is that here we may need to apply the
 * deserialization function before running the transfn (which, in this mode,
 * is actually the aggregate's combinefn).  Also, we know we don't need to
 * handle FILTER, DISTINCT, ORDER BY, or grouping sets.
 */
static void
combine_aggregates(AggState *aggstate, AggStatePerGroup pergroup)
{
	int			transno;
	int			numTrans = aggstate->numtrans;
	TupleTableSlot *slot;

	/* combine not supported with grouping sets */
	Assert(aggstate->phase->numsets <= 1);

	/* compute input for all aggregates */
	slot = ExecProject(aggstate->combinedproj);

	for (transno = 0; transno < numTrans; transno++)
	{
		AggStatePerTrans pertrans = &aggstate->pertrans[transno];
		AggStatePerGroup pergroupstate = &pergroup[transno];
		FunctionCallInfo fcinfo = &pertrans->transfn_fcinfo;
		int			inputoff = pertrans->inputoff;

		Assert(slot->tts_nvalid > inputoff);

		/*
		 * deserialfn_oid will be set if we must deserialize the input state
		 * before calling the combine function
		 */
		if (OidIsValid(pertrans->deserialfn_oid))
		{
			/* Don't call a strict deserialization function with NULL input */
			if (pertrans->deserialfn.fn_strict && slot->tts_isnull[inputoff])
			{
				fcinfo->arg[1] = slot->tts_values[inputoff];
				fcinfo->argnull[1] = slot->tts_isnull[inputoff];
			}
			else
			{
				FunctionCallInfo dsinfo = &pertrans->deserialfn_fcinfo;
				MemoryContext oldContext;

				dsinfo->arg[0] = slot->tts_values[inputoff];
				dsinfo->argnull[0] = slot->tts_isnull[inputoff];
				/* Dummy second argument for type-safety reasons */
				dsinfo->arg[1] = PointerGetDatum(NULL);
				dsinfo->argnull[1] = false;

				/*
				 * We run the deserialization functions in per-input-tuple
				 * memory context.
				 */
				oldContext = MemoryContextSwitchTo(aggstate->tmpcontext->ecxt_per_tuple_memory);

				fcinfo->arg[1] = FunctionCallInvoke(dsinfo);
				fcinfo->argnull[1] = dsinfo->isnull;

				MemoryContextSwitchTo(oldContext);
			}
		}
		else
		{
			fcinfo->arg[1] = slot->tts_values[inputoff];
			fcinfo->argnull[1] = slot->tts_isnull[inputoff];
		}

		advance_combine_function(aggstate, pertrans, pergroupstate);
	}
}

/*
 * Perform combination of states between 2 aggregate states. Effectively this
 * 'adds' two states together by whichever logic is defined in the aggregate
 * function's combine function.
 *
 * Note that in this case transfn is set to the combination function. This
 * perhaps should be changed to avoid confusion, but one field is ok for now
 * as they'll never be needed at the same time.
 */
static void
advance_combine_function(AggState *aggstate,
						 AggStatePerTrans pertrans,
						 AggStatePerGroup pergroupstate)
{
	FunctionCallInfo fcinfo = &pertrans->transfn_fcinfo;
	MemoryContext oldContext;
	Datum		newVal;

	if (pertrans->transfn.fn_strict)
	{
		/* if we're asked to merge to a NULL state, then do nothing */
		if (fcinfo->argnull[1])
			return;

		if (pergroupstate->noTransValue)
		{
			/*
			 * transValue has not yet been initialized.  If pass-by-ref
			 * datatype we must copy the combining state value into
			 * aggcontext.
			 */
			if (!pertrans->transtypeByVal)
			{
				oldContext = MemoryContextSwitchTo(
												   aggstate->curaggcontext->ecxt_per_tuple_memory);
				pergroupstate->transValue = datumCopy(fcinfo->arg[1],
													  pertrans->transtypeByVal,
													  pertrans->transtypeLen);
				MemoryContextSwitchTo(oldContext);
			}
			else
				pergroupstate->transValue = fcinfo->arg[1];

			pergroupstate->transValueIsNull = false;
			pergroupstate->noTransValue = false;
			return;
		}
	}

	/* We run the combine functions in per-input-tuple memory context */
	oldContext = MemoryContextSwitchTo(aggstate->tmpcontext->ecxt_per_tuple_memory);

	/* set up aggstate->curpertrans for AggGetAggref() */
	aggstate->curpertrans = pertrans;

	/*
	 * OK to call the combine function
	 */
	fcinfo->arg[0] = pergroupstate->transValue;
	fcinfo->argnull[0] = pergroupstate->transValueIsNull;
	fcinfo->isnull = false;		/* just in case combine func doesn't set it */

	newVal = FunctionCallInvoke(fcinfo);

	aggstate->curpertrans = NULL;

	/*
	 * If pass-by-ref datatype, must copy the new value into aggcontext and
	 * free the prior transValue.  But if the combine function returned a
	 * pointer to its first input, we don't need to do anything.  Also, if the
	 * combine function returned a pointer to a R/W expanded object that is
	 * already a child of the aggcontext, assume we can adopt that value
	 * without copying it.
	 */
	if (!pertrans->transtypeByVal &&
		DatumGetPointer(newVal) != DatumGetPointer(pergroupstate->transValue))
	{
		if (!fcinfo->isnull)
		{
			MemoryContextSwitchTo(aggstate->curaggcontext->ecxt_per_tuple_memory);
			if (DatumIsReadWriteExpandedObject(newVal,
											   false,
											   pertrans->transtypeLen) &&
				MemoryContextGetParent(DatumGetEOHP(newVal)->eoh_context) == CurrentMemoryContext)
				 /* do nothing */ ;
			else
				newVal = datumCopy(newVal,
								   pertrans->transtypeByVal,
								   pertrans->transtypeLen);
		}
		if (!pergroupstate->transValueIsNull)
		{
			if (DatumIsReadWriteExpandedObject(pergroupstate->transValue,
											   false,
											   pertrans->transtypeLen))
				DeleteExpandedObject(pergroupstate->transValue);
			else
				pfree(DatumGetPointer(pergroupstate->transValue));
		}
	}

	pergroupstate->transValue = newVal;
	pergroupstate->transValueIsNull = fcinfo->isnull;

	MemoryContextSwitchTo(oldContext);
}


/*
 * Run the transition function for a DISTINCT or ORDER BY aggregate
 * with only one input.  This is called after we have completed
 * entering all the input values into the sort object.  We complete the
 * sort, read out the values in sorted order, and run the transition
 * function on each value (applying DISTINCT if appropriate).
 *
 * Note that the strictness of the transition function was checked when
 * entering the values into the sort, so we don't check it again here;
 * we just apply standard SQL DISTINCT logic.
 *
 * The one-input case is handled separately from the multi-input case
 * for performance reasons: for single by-value inputs, such as the
 * common case of count(distinct id), the tuplesort_getdatum code path
 * is around 300% faster.  (The speedup for by-reference types is less
 * but still noticeable.)
 *
 * This function handles only one grouping set (already set in
 * aggstate->current_set).
 *
 * When called, CurrentMemoryContext should be the per-query context.
 */
static void
process_ordered_aggregate_single(AggState *aggstate,
								 AggStatePerTrans pertrans,
								 AggStatePerGroup pergroupstate)
{
	Datum		oldVal = (Datum) 0;
	bool		oldIsNull = true;
	bool		haveOldVal = false;
	MemoryContext workcontext = aggstate->tmpcontext->ecxt_per_tuple_memory;
	MemoryContext oldContext;
	bool		isDistinct = (pertrans->numDistinctCols > 0);
	Datum		newAbbrevVal = (Datum) 0;
	Datum		oldAbbrevVal = (Datum) 0;
	FunctionCallInfo fcinfo = &pertrans->transfn_fcinfo;
	Datum	   *newVal;
	bool	   *isNull;

	Assert(pertrans->numDistinctCols < 2);

	tuplesort_performsort(pertrans->sortstates[aggstate->current_set]);

	/* Load the column into argument 1 (arg 0 will be transition value) */
	newVal = fcinfo->arg + 1;
	isNull = fcinfo->argnull + 1;

	/*
	 * Note: if input type is pass-by-ref, the datums returned by the sort are
	 * freshly palloc'd in the per-query context, so we must be careful to
	 * pfree them when they are no longer needed.
	 */

	while (tuplesort_getdatum(pertrans->sortstates[aggstate->current_set],
							  true, newVal, isNull, &newAbbrevVal))
	{
		/*
		 * Clear and select the working context for evaluation of the equality
		 * function and transition function.
		 */
		MemoryContextReset(workcontext);
		oldContext = MemoryContextSwitchTo(workcontext);

		/*
		 * If DISTINCT mode, and not distinct from prior, skip it.
		 *
		 * Note: we assume equality functions don't care about collation.
		 */
		if (isDistinct &&
			haveOldVal &&
			((oldIsNull && *isNull) ||
			 (!oldIsNull && !*isNull &&
			  oldAbbrevVal == newAbbrevVal &&
			  DatumGetBool(FunctionCall2(&pertrans->equalfns[0],
										 oldVal, *newVal)))))
		{
			/* equal to prior, so forget this one */
			if (!pertrans->inputtypeByVal && !*isNull)
				pfree(DatumGetPointer(*newVal));
		}
		else
		{
			advance_transition_function(aggstate, pertrans, pergroupstate);
			/* forget the old value, if any */
			if (!oldIsNull && !pertrans->inputtypeByVal)
				pfree(DatumGetPointer(oldVal));
			/* and remember the new one for subsequent equality checks */
			oldVal = *newVal;
			oldAbbrevVal = newAbbrevVal;
			oldIsNull = *isNull;
			haveOldVal = true;
		}

		MemoryContextSwitchTo(oldContext);
	}

	if (!oldIsNull && !pertrans->inputtypeByVal)
		pfree(DatumGetPointer(oldVal));

	tuplesort_end(pertrans->sortstates[aggstate->current_set]);
	pertrans->sortstates[aggstate->current_set] = NULL;
}

/*
 * Run the transition function for a DISTINCT or ORDER BY aggregate
 * with more than one input.  This is called after we have completed
 * entering all the input values into the sort object.  We complete the
 * sort, read out the values in sorted order, and run the transition
 * function on each value (applying DISTINCT if appropriate).
 *
 * This function handles only one grouping set (already set in
 * aggstate->current_set).
 *
 * When called, CurrentMemoryContext should be the per-query context.
 */
static void
process_ordered_aggregate_multi(AggState *aggstate,
								AggStatePerTrans pertrans,
								AggStatePerGroup pergroupstate)
{
	MemoryContext workcontext = aggstate->tmpcontext->ecxt_per_tuple_memory;
	FunctionCallInfo fcinfo = &pertrans->transfn_fcinfo;
	TupleTableSlot *slot1 = pertrans->sortslot;
	TupleTableSlot *slot2 = pertrans->uniqslot;
	int			numTransInputs = pertrans->numTransInputs;
	int			numDistinctCols = pertrans->numDistinctCols;
	Datum		newAbbrevVal = (Datum) 0;
	Datum		oldAbbrevVal = (Datum) 0;
	bool		haveOldValue = false;
	int			i;

	tuplesort_performsort(pertrans->sortstates[aggstate->current_set]);

	ExecClearTuple(slot1);
	if (slot2)
		ExecClearTuple(slot2);

	while (tuplesort_gettupleslot(pertrans->sortstates[aggstate->current_set],
								  true, true, slot1, &newAbbrevVal))
	{
		CHECK_FOR_INTERRUPTS();

		/*
		 * Extract the first numTransInputs columns as datums to pass to the
		 * transfn.  (This will help execTuplesMatch too, so we do it
		 * immediately.)
		 */
		slot_getsomeattrs(slot1, numTransInputs);

		if (numDistinctCols == 0 ||
			!haveOldValue ||
			newAbbrevVal != oldAbbrevVal ||
			!execTuplesMatch(slot1, slot2,
							 numDistinctCols,
							 pertrans->sortColIdx,
							 pertrans->equalfns,
							 workcontext))
		{
			/* Load values into fcinfo */
			/* Start from 1, since the 0th arg will be the transition value */
			for (i = 0; i < numTransInputs; i++)
			{
				fcinfo->arg[i + 1] = slot1->tts_values[i];
				fcinfo->argnull[i + 1] = slot1->tts_isnull[i];
			}

			advance_transition_function(aggstate, pertrans, pergroupstate);

			if (numDistinctCols > 0)
			{
				/* swap the slot pointers to retain the current tuple */
				TupleTableSlot *tmpslot = slot2;

				slot2 = slot1;
				slot1 = tmpslot;
				/* avoid execTuplesMatch() calls by reusing abbreviated keys */
				oldAbbrevVal = newAbbrevVal;
				haveOldValue = true;
			}
		}

		/* Reset context each time, unless execTuplesMatch did it for us */
		if (numDistinctCols == 0)
			MemoryContextReset(workcontext);

		ExecClearTuple(slot1);
	}

	if (slot2)
		ExecClearTuple(slot2);

	tuplesort_end(pertrans->sortstates[aggstate->current_set]);
	pertrans->sortstates[aggstate->current_set] = NULL;
}

/*
 * Compute the final value of one aggregate.
 *
 * This function handles only one grouping set (already set in
 * aggstate->current_set).
 *
 * The finalfunction will be run, and the result delivered, in the
 * output-tuple context; caller's CurrentMemoryContext does not matter.
 *
 * The finalfn uses the state as set in the transno. This also might be
 * being used by another aggregate function, so it's important that we do
 * nothing destructive here.
 */
static void
finalize_aggregate(AggState *aggstate,
				   AggStatePerAgg peragg,
				   AggStatePerGroup pergroupstate,
				   Datum *resultVal, bool *resultIsNull)
{
	FunctionCallInfoData fcinfo;
	bool		anynull = false;
	MemoryContext oldContext;
	int			i;
	ListCell   *lc;
	AggStatePerTrans pertrans = &aggstate->pertrans[peragg->transno];

	oldContext = MemoryContextSwitchTo(aggstate->ss.ps.ps_ExprContext->ecxt_per_tuple_memory);

	/*
	 * Evaluate any direct arguments.  We do this even if there's no finalfn
	 * (which is unlikely anyway), so that side-effects happen as expected.
	 * The direct arguments go into arg positions 1 and up, leaving position 0
	 * for the transition state value.
	 */
	i = 1;
	foreach(lc, pertrans->aggdirectargs)
	{
		ExprState  *expr = (ExprState *) lfirst(lc);

		fcinfo.arg[i] = ExecEvalExpr(expr,
									 aggstate->ss.ps.ps_ExprContext,
									 &fcinfo.argnull[i]);
		anynull |= fcinfo.argnull[i];
		i++;
	}

	/*
	 * Apply the agg's finalfn if one is provided, else return transValue.
	 */
	if (OidIsValid(peragg->finalfn_oid))
	{
		int			numFinalArgs = peragg->numFinalArgs;

		/* set up aggstate->curperagg for AggGetAggref() */
		aggstate->curperagg = peragg;

		InitFunctionCallInfoData(fcinfo, &peragg->finalfn,
								 numFinalArgs,
								 pertrans->aggCollation,
								 (void *) aggstate, NULL);

		/* Fill in the transition state value */
		fcinfo.arg[0] = MakeExpandedObjectReadOnly(pergroupstate->transValue,
												   pergroupstate->transValueIsNull,
												   pertrans->transtypeLen);
		fcinfo.argnull[0] = pergroupstate->transValueIsNull;
		anynull |= pergroupstate->transValueIsNull;

		/* Fill any remaining argument positions with nulls */
		for (; i < numFinalArgs; i++)
		{
			fcinfo.arg[i] = (Datum) 0;
			fcinfo.argnull[i] = true;
			anynull = true;
		}

		if (fcinfo.flinfo->fn_strict && anynull)
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
		aggstate->curperagg = NULL;
	}
	else
	{
		/* Don't need MakeExpandedObjectReadOnly; datumCopy will copy it */
		*resultVal = pergroupstate->transValue;
		*resultIsNull = pergroupstate->transValueIsNull;
	}

	/*
	 * If result is pass-by-ref, make sure it is in the right context.
	 */
	if (!peragg->resulttypeByVal && !*resultIsNull &&
		!MemoryContextContains(CurrentMemoryContext,
							   DatumGetPointer(*resultVal)))
		*resultVal = datumCopy(*resultVal,
							   peragg->resulttypeByVal,
							   peragg->resulttypeLen);

	MemoryContextSwitchTo(oldContext);
}

/*
 * Compute the output value of one partial aggregate.
 *
 * The serialization function will be run, and the result delivered, in the
 * output-tuple context; caller's CurrentMemoryContext does not matter.
 */
static void
finalize_partialaggregate(AggState *aggstate,
						  AggStatePerAgg peragg,
						  AggStatePerGroup pergroupstate,
						  Datum *resultVal, bool *resultIsNull)
{
	AggStatePerTrans pertrans = &aggstate->pertrans[peragg->transno];
	MemoryContext oldContext;

	oldContext = MemoryContextSwitchTo(aggstate->ss.ps.ps_ExprContext->ecxt_per_tuple_memory);

	/*
	 * serialfn_oid will be set if we must serialize the transvalue before
	 * returning it
	 */
	if (OidIsValid(pertrans->serialfn_oid))
	{
		/* Don't call a strict serialization function with NULL input. */
		if (pertrans->serialfn.fn_strict && pergroupstate->transValueIsNull)
		{
			*resultVal = (Datum) 0;
			*resultIsNull = true;
		}
		else
		{
			FunctionCallInfo fcinfo = &pertrans->serialfn_fcinfo;

			fcinfo->arg[0] = MakeExpandedObjectReadOnly(pergroupstate->transValue,
														pergroupstate->transValueIsNull,
														pertrans->transtypeLen);
			fcinfo->argnull[0] = pergroupstate->transValueIsNull;

			*resultVal = FunctionCallInvoke(fcinfo);
			*resultIsNull = fcinfo->isnull;
		}
	}
	else
	{
		/* Don't need MakeExpandedObjectReadOnly; datumCopy will copy it */
		*resultVal = pergroupstate->transValue;
		*resultIsNull = pergroupstate->transValueIsNull;
	}

	/* If result is pass-by-ref, make sure it is in the right context. */
	if (!peragg->resulttypeByVal && !*resultIsNull &&
		!MemoryContextContains(CurrentMemoryContext,
							   DatumGetPointer(*resultVal)))
		*resultVal = datumCopy(*resultVal,
							   peragg->resulttypeByVal,
							   peragg->resulttypeLen);

	MemoryContextSwitchTo(oldContext);
}

/*
 * Prepare to finalize and project based on the specified representative tuple
 * slot and grouping set.
 *
 * In the specified tuple slot, force to null all attributes that should be
 * read as null in the context of the current grouping set.  Also stash the
 * current group bitmap where GroupingExpr can get at it.
 *
 * This relies on three conditions:
 *
 * 1) Nothing is ever going to try and extract the whole tuple from this slot,
 * only reference it in evaluations, which will only access individual
 * attributes.
 *
 * 2) No system columns are going to need to be nulled. (If a system column is
 * referenced in a group clause, it is actually projected in the outer plan
 * tlist.)
 *
 * 3) Within a given phase, we never need to recover the value of an attribute
 * once it has been set to null.
 *
 * Poking into the slot this way is a bit ugly, but the consensus is that the
 * alternative was worse.
 */
static void
prepare_projection_slot(AggState *aggstate, TupleTableSlot *slot, int currentSet)
{
	if (aggstate->phase->grouped_cols)
	{
		Bitmapset  *grouped_cols = aggstate->phase->grouped_cols[currentSet];

		aggstate->grouped_cols = grouped_cols;

		if (slot->tts_isempty)
		{
			/*
			 * Force all values to be NULL if working on an empty input tuple
			 * (i.e. an empty grouping set for which no input rows were
			 * supplied).
			 */
			ExecStoreAllNullTuple(slot);
		}
		else if (aggstate->all_grouped_cols)
		{
			ListCell   *lc;

			/* all_grouped_cols is arranged in desc order */
			slot_getsomeattrs(slot, linitial_int(aggstate->all_grouped_cols));

			foreach(lc, aggstate->all_grouped_cols)
			{
				int			attnum = lfirst_int(lc);

				if (!bms_is_member(attnum, grouped_cols))
					slot->tts_isnull[attnum - 1] = true;
			}
		}
	}
}

/*
 * Compute the final value of all aggregates for one group.
 *
 * This function handles only one grouping set at a time, which the caller must
 * have selected.  It's also the caller's responsibility to adjust the supplied
 * pergroup parameter to point to the current set's transvalues.
 *
 * Results are stored in the output econtext aggvalues/aggnulls.
 */
static void
finalize_aggregates(AggState *aggstate,
					AggStatePerAgg peraggs,
					AggStatePerGroup pergroup)
{
	ExprContext *econtext = aggstate->ss.ps.ps_ExprContext;
	Datum	   *aggvalues = econtext->ecxt_aggvalues;
	bool	   *aggnulls = econtext->ecxt_aggnulls;
	int			aggno;
	int			transno;

	/*
	 * If there were any DISTINCT and/or ORDER BY aggregates, sort their
	 * inputs and run the transition functions.
	 */
	for (transno = 0; transno < aggstate->numtrans; transno++)
	{
		AggStatePerTrans pertrans = &aggstate->pertrans[transno];
		AggStatePerGroup pergroupstate;

		pergroupstate = &pergroup[transno];

		if (pertrans->numSortCols > 0)
		{
			Assert(aggstate->aggstrategy != AGG_HASHED &&
				   aggstate->aggstrategy != AGG_MIXED);

			if (pertrans->numInputs == 1)
				process_ordered_aggregate_single(aggstate,
												 pertrans,
												 pergroupstate);
			else
				process_ordered_aggregate_multi(aggstate,
												pertrans,
												pergroupstate);
		}
	}

	/*
	 * Run the final functions.
	 */
	for (aggno = 0; aggno < aggstate->numaggs; aggno++)
	{
		AggStatePerAgg peragg = &peraggs[aggno];
		int			transno = peragg->transno;
		AggStatePerGroup pergroupstate;

		pergroupstate = &pergroup[transno];

		if (DO_AGGSPLIT_SKIPFINAL(aggstate->aggsplit))
			finalize_partialaggregate(aggstate, peragg, pergroupstate,
									  &aggvalues[aggno], &aggnulls[aggno]);
		else
			finalize_aggregate(aggstate, peragg, pergroupstate,
							   &aggvalues[aggno], &aggnulls[aggno]);
	}
}

/*
 * Project the result of a group (whose aggs have already been calculated by
 * finalize_aggregates). Returns the result slot, or NULL if no row is
 * projected (suppressed by qual).
 */
static TupleTableSlot *
project_aggregates(AggState *aggstate)
{
	ExprContext *econtext = aggstate->ss.ps.ps_ExprContext;

	/*
	 * Check the qual (HAVING clause); if the group does not match, ignore it.
	 */
	if (ExecQual(aggstate->ss.ps.qual, econtext))
	{
		/*
		 * Form and return projection tuple using the aggregate results and
		 * the representative input tuple.
		 */
		return ExecProject(aggstate->ss.ps.ps_ProjInfo);
	}
	else
		InstrCountFiltered1(aggstate, 1);

	return NULL;
}

/*
 * find_unaggregated_cols
 *	  Construct a bitmapset of the column numbers of un-aggregated Vars
 *	  appearing in our targetlist and qual (HAVING clause)
 */
static Bitmapset *
find_unaggregated_cols(AggState *aggstate)
{
	Agg		   *node = (Agg *) aggstate->ss.ps.plan;
	Bitmapset  *colnos;

	colnos = NULL;
	(void) find_unaggregated_cols_walker((Node *) node->plan.targetlist,
										 &colnos);
	(void) find_unaggregated_cols_walker((Node *) node->plan.qual,
										 &colnos);
	return colnos;
}

static bool
find_unaggregated_cols_walker(Node *node, Bitmapset **colnos)
{
	if (node == NULL)
		return false;
	if (IsA(node, Var))
	{
		Var		   *var = (Var *) node;

		/* setrefs.c should have set the varno to OUTER_VAR */
		Assert(var->varno == OUTER_VAR);
		Assert(var->varlevelsup == 0);
		*colnos = bms_add_member(*colnos, var->varattno);
		return false;
	}
	if (IsA(node, Aggref) ||IsA(node, GroupingFunc))
	{
		/* do not descend into aggregate exprs */
		return false;
	}
	return expression_tree_walker(node, find_unaggregated_cols_walker,
								  (void *) colnos);
}

/*
 * Initialize the hash table(s) to empty.
 *
 * To implement hashed aggregation, we need a hashtable that stores a
 * representative tuple and an array of AggStatePerGroup structs for each
 * distinct set of GROUP BY column values.  We compute the hash key from the
 * GROUP BY columns.  The per-group data is allocated in lookup_hash_entry(),
 * for each entry.
 *
 * We have a separate hashtable and associated perhash data structure for each
 * grouping set for which we're doing hashing.
 *
 * The hash tables always live in the hashcontext's per-tuple memory context
 * (there is only one of these for all tables together, since they are all
 * reset at the same time).
 */
static void
build_hash_table(AggState *aggstate)
{
	MemoryContext tmpmem = aggstate->tmpcontext->ecxt_per_tuple_memory;
	Size		additionalsize;
	int			i;

	Assert(aggstate->aggstrategy == AGG_HASHED || aggstate->aggstrategy == AGG_MIXED);

	additionalsize = aggstate->numtrans * sizeof(AggStatePerGroupData);

	for (i = 0; i < aggstate->num_hashes; ++i)
	{
		AggStatePerHash perhash = &aggstate->perhash[i];

		Assert(perhash->aggnode->numGroups > 0);

		perhash->hashtable = BuildTupleHashTable(perhash->numCols,
												 perhash->hashGrpColIdxHash,
												 perhash->eqfunctions,
												 perhash->hashfunctions,
												 perhash->aggnode->numGroups,
												 additionalsize,
												 aggstate->hashcontext->ecxt_per_tuple_memory,
												 tmpmem,
												 DO_AGGSPLIT_SKIPFINAL(aggstate->aggsplit));
	}
}

/*
 * Compute columns that actually need to be stored in hashtable entries.  The
 * incoming tuples from the child plan node will contain grouping columns,
 * other columns referenced in our targetlist and qual, columns used to
 * compute the aggregate functions, and perhaps just junk columns we don't use
 * at all.  Only columns of the first two types need to be stored in the
 * hashtable, and getting rid of the others can make the table entries
 * significantly smaller.  The hashtable only contains the relevant columns,
 * and is packed/unpacked in lookup_hash_entry() / agg_retrieve_hash_table()
 * into the format of the normal input descriptor.
 *
 * Additional columns, in addition to the columns grouped by, come from two
 * sources: Firstly functionally dependent columns that we don't need to group
 * by themselves, and secondly ctids for row-marks.
 *
 * To eliminate duplicates, we build a bitmapset of the needed columns, and
 * then build an array of the columns included in the hashtable.  Note that
 * the array is preserved over ExecReScanAgg, so we allocate it in the
 * per-query context (unlike the hash table itself).
 */
static void
find_hash_columns(AggState *aggstate)
{
	Bitmapset  *base_colnos;
	List	   *outerTlist = outerPlanState(aggstate)->plan->targetlist;
	int			numHashes = aggstate->num_hashes;
	int			j;

	/* Find Vars that will be needed in tlist and qual */
	base_colnos = find_unaggregated_cols(aggstate);

	for (j = 0; j < numHashes; ++j)
	{
		AggStatePerHash perhash = &aggstate->perhash[j];
		Bitmapset  *colnos = bms_copy(base_colnos);
		AttrNumber *grpColIdx = perhash->aggnode->grpColIdx;
		List	   *hashTlist = NIL;
		TupleDesc	hashDesc;
		int			i;

		perhash->largestGrpColIdx = 0;

		/*
		 * If we're doing grouping sets, then some Vars might be referenced in
		 * tlist/qual for the benefit of other grouping sets, but not needed
		 * when hashing; i.e. prepare_projection_slot will null them out, so
		 * there'd be no point storing them.  Use prepare_projection_slot's
		 * logic to determine which.
		 */
		if (aggstate->phases[0].grouped_cols)
		{
			Bitmapset  *grouped_cols = aggstate->phases[0].grouped_cols[j];
			ListCell   *lc;

			foreach(lc, aggstate->all_grouped_cols)
			{
				int			attnum = lfirst_int(lc);

				if (!bms_is_member(attnum, grouped_cols))
					colnos = bms_del_member(colnos, attnum);
			}
		}
		/* Add in all the grouping columns */
		for (i = 0; i < perhash->numCols; i++)
			colnos = bms_add_member(colnos, grpColIdx[i]);

		perhash->hashGrpColIdxInput =
			palloc(bms_num_members(colnos) * sizeof(AttrNumber));
		perhash->hashGrpColIdxHash =
			palloc(perhash->numCols * sizeof(AttrNumber));

		/*
		 * First build mapping for columns directly hashed. These are the
		 * first, because they'll be accessed when computing hash values and
		 * comparing tuples for exact matches. We also build simple mapping
		 * for execGrouping, so it knows where to find the to-be-hashed /
		 * compared columns in the input.
		 */
		for (i = 0; i < perhash->numCols; i++)
		{
			perhash->hashGrpColIdxInput[i] = grpColIdx[i];
			perhash->hashGrpColIdxHash[i] = i + 1;
			perhash->numhashGrpCols++;
			/* delete already mapped columns */
			bms_del_member(colnos, grpColIdx[i]);
		}

		/* and add the remaining columns */
		while ((i = bms_first_member(colnos)) >= 0)
		{
			perhash->hashGrpColIdxInput[perhash->numhashGrpCols] = i;
			perhash->numhashGrpCols++;
		}

		/* and build a tuple descriptor for the hashtable */
		for (i = 0; i < perhash->numhashGrpCols; i++)
		{
			int			varNumber = perhash->hashGrpColIdxInput[i] - 1;

			hashTlist = lappend(hashTlist, list_nth(outerTlist, varNumber));
			perhash->largestGrpColIdx =
				Max(varNumber + 1, perhash->largestGrpColIdx);
		}

		hashDesc = ExecTypeFromTL(hashTlist, false);
		ExecSetSlotDescriptor(perhash->hashslot, hashDesc);

		list_free(hashTlist);
		bms_free(colnos);
	}

	bms_free(base_colnos);
}

/*
 * Find or create a hashtable entry for the tuple group containing the current
 * tuple (already set in tmpcontext's outertuple slot), in the current grouping
 * set (which the caller must have selected - note that initialize_aggregate
 * depends on this).
 *
 * When called, CurrentMemoryContext should be the per-query context.
 */
static TupleHashEntryData *
lookup_hash_entry(AggState *aggstate)
{
	TupleTableSlot *inputslot = aggstate->tmpcontext->ecxt_outertuple;
	AggStatePerHash perhash = &aggstate->perhash[aggstate->current_set];
	TupleTableSlot *hashslot = perhash->hashslot;
	TupleHashEntryData *entry;
	bool		isnew;
	int			i;

	/* transfer just the needed columns into hashslot */
	slot_getsomeattrs(inputslot, perhash->largestGrpColIdx);
	ExecClearTuple(hashslot);

	for (i = 0; i < perhash->numhashGrpCols; i++)
	{
		int			varNumber = perhash->hashGrpColIdxInput[i] - 1;

		hashslot->tts_values[i] = inputslot->tts_values[varNumber];
		hashslot->tts_isnull[i] = inputslot->tts_isnull[varNumber];
	}
	ExecStoreVirtualTuple(hashslot);

	/* find or create the hashtable entry using the filtered tuple */
	entry = LookupTupleHashEntry(perhash->hashtable, hashslot, &isnew);

	if (isnew)
	{
		entry->additional = (AggStatePerGroup)
			MemoryContextAlloc(perhash->hashtable->tablecxt,
							   sizeof(AggStatePerGroupData) * aggstate->numtrans);
		/* initialize aggregates for new tuple group */
		initialize_aggregates(aggstate, (AggStatePerGroup) entry->additional,
							  -1);
	}

	return entry;
}

/*
 * Look up hash entries for the current tuple in all hashed grouping sets,
 * returning an array of pergroup pointers suitable for advance_aggregates.
 *
 * Be aware that lookup_hash_entry can reset the tmpcontext.
 */
static AggStatePerGroup *
lookup_hash_entries(AggState *aggstate)
{
	int			numHashes = aggstate->num_hashes;
	AggStatePerGroup *pergroup = aggstate->hash_pergroup;
	int			setno;

	for (setno = 0; setno < numHashes; setno++)
	{
		select_current_set(aggstate, setno, true);
		pergroup[setno] = lookup_hash_entry(aggstate)->additional;
	}

	return pergroup;
}

/*
 * ExecAgg -
 *
 *	  ExecAgg receives tuples from its outer subplan and aggregates over
 *	  the appropriate attribute for each aggregate function use (Aggref
 *	  node) appearing in the targetlist or qual of the node.  The number
 *	  of tuples to aggregate over depends on whether grouped or plain
 *	  aggregation is selected.  In grouped aggregation, we produce a result
 *	  row for each group; in plain aggregation there's a single result row
 *	  for the whole query.  In either case, the value of each aggregate is
 *	  stored in the expression context to be used when ExecProject evaluates
 *	  the result tuple.
 */
static TupleTableSlot *
ExecAgg(PlanState *pstate)
{
	AggState   *node = castNode(AggState, pstate);
	TupleTableSlot *result = NULL;

	CHECK_FOR_INTERRUPTS();

	if (!node->agg_done)
	{
		/* Dispatch based on strategy */
		switch (node->phase->aggstrategy)
		{
			case AGG_HASHED:
				if (!node->table_filled)
					agg_fill_hash_table(node);
				/* FALLTHROUGH */
			case AGG_MIXED:
				result = agg_retrieve_hash_table(node);
				break;
			case AGG_PLAIN:
			case AGG_SORTED:
				result = agg_retrieve_direct(node);
				break;
		}

		if (!TupIsNull(result))
			return result;
	}

	return NULL;
}

/*
 * ExecAgg for non-hashed case
 */
static TupleTableSlot *
agg_retrieve_direct(AggState *aggstate)
{
	Agg		   *node = aggstate->phase->aggnode;
	ExprContext *econtext;
	ExprContext *tmpcontext;
	AggStatePerAgg peragg;
	AggStatePerGroup pergroup;
	AggStatePerGroup *hash_pergroups = NULL;
	TupleTableSlot *outerslot;
	TupleTableSlot *firstSlot;
	TupleTableSlot *result;
	bool		hasGroupingSets = aggstate->phase->numsets > 0;
	int			numGroupingSets = Max(aggstate->phase->numsets, 1);
	int			currentSet;
	int			nextSetSize;
	int			numReset;
	int			i;

	/*
	 * get state info from node
	 *
	 * econtext is the per-output-tuple expression context
	 *
	 * tmpcontext is the per-input-tuple expression context
	 */
	econtext = aggstate->ss.ps.ps_ExprContext;
	tmpcontext = aggstate->tmpcontext;

	peragg = aggstate->peragg;
	pergroup = aggstate->pergroup;
	firstSlot = aggstate->ss.ss_ScanTupleSlot;

	/*
	 * We loop retrieving groups until we find one matching
	 * aggstate->ss.ps.qual
	 *
	 * For grouping sets, we have the invariant that aggstate->projected_set
	 * is either -1 (initial call) or the index (starting from 0) in
	 * gset_lengths for the group we just completed (either by projecting a
	 * row or by discarding it in the qual).
	 */
	while (!aggstate->agg_done)
	{
		/*
		 * Clear the per-output-tuple context for each group, as well as
		 * aggcontext (which contains any pass-by-ref transvalues of the old
		 * group).  Some aggregate functions store working state in child
		 * contexts; those now get reset automatically without us needing to
		 * do anything special.
		 *
		 * We use ReScanExprContext not just ResetExprContext because we want
		 * any registered shutdown callbacks to be called.  That allows
		 * aggregate functions to ensure they've cleaned up any non-memory
		 * resources.
		 */
		ReScanExprContext(econtext);

		/*
		 * Determine how many grouping sets need to be reset at this boundary.
		 */
		if (aggstate->projected_set >= 0 &&
			aggstate->projected_set < numGroupingSets)
			numReset = aggstate->projected_set + 1;
		else
			numReset = numGroupingSets;

		/*
		 * numReset can change on a phase boundary, but that's OK; we want to
		 * reset the contexts used in _this_ phase, and later, after possibly
		 * changing phase, initialize the right number of aggregates for the
		 * _new_ phase.
		 */

		for (i = 0; i < numReset; i++)
		{
			ReScanExprContext(aggstate->aggcontexts[i]);
		}

		/*
		 * Check if input is complete and there are no more groups to project
		 * in this phase; move to next phase or mark as done.
		 */
		if (aggstate->input_done == true &&
			aggstate->projected_set >= (numGroupingSets - 1))
		{
			if (aggstate->current_phase < aggstate->numphases - 1)
			{
				initialize_phase(aggstate, aggstate->current_phase + 1);
				aggstate->input_done = false;
				aggstate->projected_set = -1;
				numGroupingSets = Max(aggstate->phase->numsets, 1);
				node = aggstate->phase->aggnode;
				numReset = numGroupingSets;
			}
			else if (aggstate->aggstrategy == AGG_MIXED)
			{
				/*
				 * Mixed mode; we've output all the grouped stuff and have
				 * full hashtables, so switch to outputting those.
				 */
				initialize_phase(aggstate, 0);
				aggstate->table_filled = true;
				ResetTupleHashIterator(aggstate->perhash[0].hashtable,
									   &aggstate->perhash[0].hashiter);
				select_current_set(aggstate, 0, true);
				return agg_retrieve_hash_table(aggstate);
			}
			else
			{
				aggstate->agg_done = true;
				break;
			}
		}

		/*
		 * Get the number of columns in the next grouping set after the last
		 * projected one (if any). This is the number of columns to compare to
		 * see if we reached the boundary of that set too.
		 */
		if (aggstate->projected_set >= 0 &&
			aggstate->projected_set < (numGroupingSets - 1))
			nextSetSize = aggstate->phase->gset_lengths[aggstate->projected_set + 1];
		else
			nextSetSize = 0;

		/*----------
		 * If a subgroup for the current grouping set is present, project it.
		 *
		 * We have a new group if:
		 *	- we're out of input but haven't projected all grouping sets
		 *	  (checked above)
		 * OR
		 *	  - we already projected a row that wasn't from the last grouping
		 *		set
		 *	  AND
		 *	  - the next grouping set has at least one grouping column (since
		 *		empty grouping sets project only once input is exhausted)
		 *	  AND
		 *	  - the previous and pending rows differ on the grouping columns
		 *		of the next grouping set
		 *----------
		 */
		if (aggstate->input_done ||
			(node->aggstrategy != AGG_PLAIN &&
			 aggstate->projected_set != -1 &&
			 aggstate->projected_set < (numGroupingSets - 1) &&
			 nextSetSize > 0 &&
			 !execTuplesMatch(econtext->ecxt_outertuple,
							  tmpcontext->ecxt_outertuple,
							  nextSetSize,
							  node->grpColIdx,
							  aggstate->phase->eqfunctions,
							  tmpcontext->ecxt_per_tuple_memory)))
		{
			aggstate->projected_set += 1;

			Assert(aggstate->projected_set < numGroupingSets);
			Assert(nextSetSize > 0 || aggstate->input_done);
		}
		else
		{
			/*
			 * We no longer care what group we just projected, the next
			 * projection will always be the first (or only) grouping set
			 * (unless the input proves to be empty).
			 */
			aggstate->projected_set = 0;

			/*
			 * If we don't already have the first tuple of the new group,
			 * fetch it from the outer plan.
			 */
			if (aggstate->grp_firstTuple == NULL)
			{
				outerslot = fetch_input_tuple(aggstate);
				if (!TupIsNull(outerslot))
				{
					/*
					 * Make a copy of the first input tuple; we will use this
					 * for comparisons (in group mode) and for projection.
					 */
					aggstate->grp_firstTuple = ExecCopySlotTuple(outerslot);
				}
				else
				{
					/* outer plan produced no tuples at all */
					if (hasGroupingSets)
					{
						/*
						 * If there was no input at all, we need to project
						 * rows only if there are grouping sets of size 0.
						 * Note that this implies that there can't be any
						 * references to ungrouped Vars, which would otherwise
						 * cause issues with the empty output slot.
						 *
						 * XXX: This is no longer true, we currently deal with
						 * this in finalize_aggregates().
						 */
						aggstate->input_done = true;

						while (aggstate->phase->gset_lengths[aggstate->projected_set] > 0)
						{
							aggstate->projected_set += 1;
							if (aggstate->projected_set >= numGroupingSets)
							{
								/*
								 * We can't set agg_done here because we might
								 * have more phases to do, even though the
								 * input is empty. So we need to restart the
								 * whole outer loop.
								 */
								break;
							}
						}

						if (aggstate->projected_set >= numGroupingSets)
							continue;
					}
					else
					{
						aggstate->agg_done = true;
						/* If we are grouping, we should produce no tuples too */
						if (node->aggstrategy != AGG_PLAIN)
							return NULL;
					}
				}
			}

			/*
			 * Initialize working state for a new input tuple group.
			 */
			initialize_aggregates(aggstate, pergroup, numReset);

			if (aggstate->grp_firstTuple != NULL)
			{
				/*
				 * Store the copied first input tuple in the tuple table slot
				 * reserved for it.  The tuple will be deleted when it is
				 * cleared from the slot.
				 */
				ExecStoreTuple(aggstate->grp_firstTuple,
							   firstSlot,
							   InvalidBuffer,
							   true);
				aggstate->grp_firstTuple = NULL;	/* don't keep two pointers */

				/* set up for first advance_aggregates call */
				tmpcontext->ecxt_outertuple = firstSlot;

				/*
				 * Process each outer-plan tuple, and then fetch the next one,
				 * until we exhaust the outer plan or cross a group boundary.
				 */
				for (;;)
				{
					/*
					 * During phase 1 only of a mixed agg, we need to update
					 * hashtables as well in advance_aggregates.
					 */
					if (aggstate->aggstrategy == AGG_MIXED &&
						aggstate->current_phase == 1)
					{
						hash_pergroups = lookup_hash_entries(aggstate);
					}
					else
						hash_pergroups = NULL;

					if (DO_AGGSPLIT_COMBINE(aggstate->aggsplit))
						combine_aggregates(aggstate, pergroup);
					else
						advance_aggregates(aggstate, pergroup, hash_pergroups);

					/* Reset per-input-tuple context after each tuple */
					ResetExprContext(tmpcontext);

					outerslot = fetch_input_tuple(aggstate);
					if (TupIsNull(outerslot))
					{
						/* no more outer-plan tuples available */
						if (hasGroupingSets)
						{
							aggstate->input_done = true;
							break;
						}
						else
						{
							aggstate->agg_done = true;
							break;
						}
					}
					/* set up for next advance_aggregates call */
					tmpcontext->ecxt_outertuple = outerslot;

					/*
					 * If we are grouping, check whether we've crossed a group
					 * boundary.
					 */
					if (node->aggstrategy != AGG_PLAIN)
					{
						if (!execTuplesMatch(firstSlot,
											 outerslot,
											 node->numCols,
											 node->grpColIdx,
											 aggstate->phase->eqfunctions,
											 tmpcontext->ecxt_per_tuple_memory))
						{
							aggstate->grp_firstTuple = ExecCopySlotTuple(outerslot);
							break;
						}
					}
				}
			}

			/*
			 * Use the representative input tuple for any references to
			 * non-aggregated input columns in aggregate direct args, the node
			 * qual, and the tlist.  (If we are not grouping, and there are no
			 * input rows at all, we will come here with an empty firstSlot
			 * ... but if not grouping, there can't be any references to
			 * non-aggregated input columns, so no problem.)
			 */
			econtext->ecxt_outertuple = firstSlot;
		}

		Assert(aggstate->projected_set >= 0);

		currentSet = aggstate->projected_set;

		prepare_projection_slot(aggstate, econtext->ecxt_outertuple, currentSet);

		select_current_set(aggstate, currentSet, false);

		finalize_aggregates(aggstate,
							peragg,
							pergroup + (currentSet * aggstate->numtrans));

		/*
		 * If there's no row to project right now, we must continue rather
		 * than returning a null since there might be more groups.
		 */
		result = project_aggregates(aggstate);
		if (result)
			return result;
	}

	/* No more groups */
	return NULL;
}

/*
 * ExecAgg for hashed case: read input and build hash table
 */
static void
agg_fill_hash_table(AggState *aggstate)
{
	TupleTableSlot *outerslot;
	ExprContext *tmpcontext = aggstate->tmpcontext;

	/*
	 * Process each outer-plan tuple, and then fetch the next one, until we
	 * exhaust the outer plan.
	 */
	for (;;)
	{
		AggStatePerGroup *pergroups;

		outerslot = fetch_input_tuple(aggstate);
		if (TupIsNull(outerslot)) {
            aggstate->isComplete = TupIsComplete(outerslot);
            break;
        }

		/* set up for lookup_hash_entries and advance_aggregates */
		tmpcontext->ecxt_outertuple = outerslot;

		/* Find or build hashtable entries */
		pergroups = lookup_hash_entries(aggstate);

		/* Advance the aggregates */
		if (DO_AGGSPLIT_COMBINE(aggstate->aggsplit))
			combine_aggregates(aggstate, pergroups[0]);
		else
			advance_aggregates(aggstate, NULL, pergroups);

		/*
		 * Reset per-input-tuple context after each tuple, but note that the
		 * hash lookups do this too
		 */
		ResetExprContext(aggstate->tmpcontext);
	}

	aggstate->table_filled = true;
	/* Initialize to walk the first hash table */
	select_current_set(aggstate, 0, true);
	ResetTupleHashIterator(aggstate->perhash[0].hashtable,
						   &aggstate->perhash[0].hashiter);
}

/*
 * ExecAgg for hashed case: retrieving groups from hash table
 */
static TupleTableSlot *
agg_retrieve_hash_table(AggState *aggstate)
{
	ExprContext *econtext;
	AggStatePerAgg peragg;
	AggStatePerGroup pergroup;
	TupleHashEntryData *entry;
	TupleTableSlot *firstSlot;
	TupleTableSlot *result;
	AggStatePerHash perhash;

	/*
	 * get state info from node.
	 *
	 * econtext is the per-output-tuple expression context.
	 */
	econtext = aggstate->ss.ps.ps_ExprContext;
	peragg = aggstate->peragg;
	firstSlot = aggstate->ss.ss_ScanTupleSlot;

	/*
	 * Note that perhash (and therefore anything accessed through it) can
	 * change inside the loop, as we change between grouping sets.
	 */
	perhash = &aggstate->perhash[aggstate->current_set];

	/*
	 * We loop retrieving groups until we find one satisfying
	 * aggstate->ss.ps.qual
	 */
	while (!aggstate->agg_done)
	{
		TupleTableSlot *hashslot = perhash->hashslot;
		int			i;

		CHECK_FOR_INTERRUPTS();

		/*
		 * Find the next entry in the hash table
		 */
		entry = ScanTupleHashTable(perhash->hashtable, &perhash->hashiter);
		if (entry == NULL)
		{
			int			nextset = aggstate->current_set + 1;

			if (nextset < aggstate->num_hashes)
			{
				/*
				 * Switch to next grouping set, reinitialize, and restart the
				 * loop.
				 */
				select_current_set(aggstate, nextset, true);

				perhash = &aggstate->perhash[aggstate->current_set];

				ResetTupleHashIterator(perhash->hashtable, &perhash->hashiter);

				continue;
			}
			else
			{
				/* No more hashtables, so done */
				aggstate->agg_done = TRUE;
				return NULL;
			}
		}

		/*
		 * Clear the per-output-tuple context for each group
		 *
		 * We intentionally don't use ReScanExprContext here; if any aggs have
		 * registered shutdown callbacks, they mustn't be called yet, since we
		 * might not be done with that agg.
		 */
		ResetExprContext(econtext);

		/*
		 * Transform representative tuple back into one with the right
		 * columns.
		 */
		ExecStoreMinimalTuple(entry->firstTuple, hashslot, false);
		slot_getallattrs(hashslot);

		ExecClearTuple(firstSlot);
		memset(firstSlot->tts_isnull, true,
			   firstSlot->tts_tupleDescriptor->natts * sizeof(bool));

		for (i = 0; i < perhash->numhashGrpCols; i++)
		{
			int			varNumber = perhash->hashGrpColIdxInput[i] - 1;

			firstSlot->tts_values[varNumber] = hashslot->tts_values[i];
			firstSlot->tts_isnull[varNumber] = hashslot->tts_isnull[i];
		}
		ExecStoreVirtualTuple(firstSlot);

		pergroup = (AggStatePerGroup) entry->additional;

		/*
		 * Use the representative input tuple for any references to
		 * non-aggregated input columns in the qual and tlist.
		 */
		econtext->ecxt_outertuple = firstSlot;

		prepare_projection_slot(aggstate,
								econtext->ecxt_outertuple,
								aggstate->current_set);

		finalize_aggregates(aggstate, peragg, pergroup);

		result = project_aggregates(aggstate);
		if (result)
			return result;
	}

	/* No more groups */
	return NULL;
}

/* -----------------
 * ExecAggInc
 *
 * -----------------
 */
TupleTableSlot *
ExecAggInc(PlanState *pstate)
{
    AggState *node = castNode(AggState, pstate);
    TupleTableSlot * result_slot = ExecAgg(pstate);
    if (!TupIsNull(result_slot))
    {
        return result_slot;
    }
    else
    {
        result_slot = node->ss.ps.ps_ProjInfo->pi_state.resultslot; 
        ExecClearTuple(result_slot);
        if (node->isComplete) 
        {
            MarkTupComplete(result_slot, true);
        }
        else 
        {
            /* reset the states of Aggregation */
	        node->agg_done = false;
            
            Assert(node->aggstrategy == AGG_HASHED);

            node->table_filled = false;
		    ResetExprContext(node->tmpcontext);

            MarkTupComplete(result_slot, false);
        }
        return result_slot; 
    }

}

/* -----------------
 * ExecInitAggInc
 *
 *	Creates the run-time information for the agg node produced by the
 *	planner and initializes its outer subtree.
 *
 * -----------------
 */
void 
ExecInitAggInc(AggState *aggstate)
{
    aggstate->isComplete = false;
}

void 
ExecResetAggState(AggState * node)
{
	node->agg_done = false;
    node->table_filled = false;

    IncInfo *incInfo = node->ss.ps.ps_IncInfo;
    if (incInfo->leftIncState == STATE_DROP) 
    {		
        ReScanExprContext(node->hashcontext);
		/* Rebuild an empty hash table */
		build_hash_table(node);
		/* iterator will be reset when the table is filled */
    } 
    else if (incInfo->leftIncState == STATE_KEEPDISK)
    {
        Assert(false); /* not implemented yet; will do it soon */
    } 
    /*else  //keep in main memory 
    {
    }*/

    PlanState *outerPlan;
    outerPlan = outerPlanState(node);
    ExecResetState(outerPlan); 
}

void 
ExecInitAggDelta(AggState * node)
{
    PlanState *outerPlan;
    outerPlan = outerPlanState(node);
    ExecInitDelta(outerPlan); 
}
