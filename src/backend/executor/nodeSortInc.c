/*-------------------------------------------------------------------------
 *
 * nodeSortInc.c
 *	  Routines to handle sorting of relations with incremental semantics 
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeSortInc.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "executor/execdebug.h"
#include "executor/nodeSort.h"
#include "miscadmin.h"
#include "utils/tuplesort.h"

#include "executor/incmeta.h"

static void ExecCompactSort(SortState *node);

/* ----------------------------------------------------------------
 *		ExecSortInc
 *
 *		Sorts tuples from the outer subtree of the node using tuplesort,
 *		which saves the results in a temporary file or memory. After the
 *		initial call, returns a tuple from the file with each call.
 *
 *		Conditions:
 *		  -- none.
 *
 *		Initial States:
 *		  -- the outer child is prepared to return the first tuple.
 * ----------------------------------------------------------------
 */
TupleTableSlot *
ExecSortInc(PlanState *pstate)
{
	SortState  *node = castNode(SortState, pstate);
	EState	   *estate;
	ScanDirection dir;
	Tuplesortstate *tuplesortstate;
	TupleTableSlot *slot;

	CHECK_FOR_INTERRUPTS();

	/*
	 * get state info from node
	 */
	SO1_printf("ExecSort: %s\n",
			   "entering routine");

	estate = node->ss.ps.state;
	dir = estate->es_direction;
	tuplesortstate = (Tuplesortstate *) node->tuplesortstate;

	/*
	 * If first time through, read all tuples from outer plan and pass them to
	 * tuplesort.c. Subsequent calls just fetch tuples from tuplesort.
	 */

	if (node->isEOF)
	{
        if (node->isComplete) 
        {
	        slot = node->ss.ps.ps_ResultTupleSlot;
            ExecClearTuple(slot); 
            MarkTupComplete(slot, node->isComplete); 
            return slot; 
        }
        /*
         * we compact it to the stashed state and build a new one. 
         */
        ExecCompactSort(node); 

		Sort	   *plannode = (Sort *) node->ss.ps.plan;
		PlanState  *outerNode;
		TupleDesc	tupDesc;

		SO1_printf("ExecSort: %s\n",
				   "sorting subplan");

		/*
		 * Want to scan subplan in the forward direction while creating the
		 * sorted data.
		 */
		estate->es_direction = ForwardScanDirection;

		/*
		 * Initialize tuplesort module.
		 */
		SO1_printf("ExecSort: %s\n",
				   "calling tuplesort_begin");

		outerNode = outerPlanState(node);
		tupDesc = ExecGetResultType(outerNode);

		tuplesortstate = tuplesort_begin_heap(tupDesc,
											  plannode->numCols,
											  plannode->sortColIdx,
											  plannode->sortOperators,
											  plannode->collations,
											  plannode->nullsFirst,
											  work_mem,
											  node->randomAccess);
		if (node->bounded)
			tuplesort_set_bound(tuplesortstate, node->bound);
		node->tuplesortstate = (void *) tuplesortstate;

		/*
		 * Scan the subplan and feed all the tuples to tuplesort.
		 */

		for (;;)
		{
			slot = ExecProcNode(outerNode);

			if (TupIsNull(slot)) 
            {
                node->isComplete = TupIsComplete(slot);
				break;
            }

			tuplesort_puttupleslot(tuplesortstate, slot);
		}

		/*
		 * Complete the sort.
		 */
		tuplesort_performsort(tuplesortstate);

		/*
		 * restore to user specified direction
		 */
		estate->es_direction = dir;

		/*
		 * finally set the sorted flag to true
		 */
		node->isEOF = false;
		node->bounded_Done = node->bounded;
		node->bound_Done = node->bound;
		SO1_printf("ExecSort: %s\n", "sorting done");
	}

	SO1_printf("ExecSort: %s\n",
			   "retrieving tuple from tuplesort");

	/*
	 * Get the first or next tuple from tuplesort. Returns NULL if no more
	 * tuples.  Note that we only rely on slot tuple remaining valid until the
	 * next fetch from the tuplesort.
	 */
	slot = node->ss.ps.ps_ResultTupleSlot;
	(void) tuplesort_gettupleslot(tuplesortstate,
								  ScanDirectionIsForward(dir),
								  false, slot, NULL);

    if (TupIsNull(slot)) {
        node->isEOF = true;
        MarkTupComplete(slot, node->isComplete);
    }

	return slot;
}

/* ----------------------------------------------------------------
 *		ExecInitSortInc
 *		    
 *		    Initialize necessary states for incremental sorting
 *
 * ----------------------------------------------------------------
 */
void 
ExecInitSortInc(SortState *node)
{
    node->isComplete = false;
    node->isEOF = true;
    node->stashedstate = NULL;
}

/* ----------------------------------------------------------------
 *		ExecCompactSort
 *		    
 *		    Merge sorted tuples in the current state
 *		    with those in stashed state
 *
 * ----------------------------------------------------------------
 */
static void 
ExecCompactSort(SortState *node)
{
    if (node->tuplesortstate == NULL)
        return; 

    tuplesort_rescan((Tuplesortstate *) node->tuplesortstate);
    if (node->stashedstate == NULL) 
    {
        node->stashedstate = node->tuplesortstate; 
        node->tuplesortstate = NULL; 
        node->isEOF = true;
        return; 
    }

    /*
     * Begin compact tuplesortstate into stashedstate
     */
	TupleTableSlot *slot = node->ss.ps.ps_ResultTupleSlot;
	ScanDirection dir = node->ss.ps.state->es_direction;
    Tuplesortstate * tuplesortstate = (Tuplesortstate *) node->tuplesortstate; 
    Tuplesortstate * stashedstate = (Tuplesortstate *) node->stashedstate; 

    tuplesort_reverttoheap(stashedstate);
    while (
            tuplesort_gettupleslot(tuplesortstate,
								  ScanDirectionIsForward(dir),
								  false, slot, NULL)
          ) 
    {
        tuplesort_puttupleslot(stashedstate, slot); 
    }

    /*
     * We have merged everything into stashed state; now sort it
     */
    tuplesort_performsort(stashedstate); 

    /*
     * Finally, destroy the current tuplesortstate
     */
	tuplesort_end((Tuplesortstate *) tuplesortstate);
	node->tuplesortstate = NULL;

    node->isEOF = true;
} 

/* ----------------------------------------------------------------
 *		ExecExchangeSortState
 *		    
 *		    Exchange the current state to the stashed one
 *
 * ----------------------------------------------------------------
 */
void
ExecExchangeSortState(SortState *node) 
{
    /*
     * Perform the exchange
     */
    Tuplesortstate * temp = (Tuplesortstate *) node->tuplesortstate; 
    node->tuplesortstate = node->stashedstate; 
    node->stashedstate = temp; 

    /*
     * Reset stashed state and other meta states
     */
    tuplesort_rescan((Tuplesortstate *) node->stashedstate);
    node->isEOF = false; 
}

void 
ExecResetSortState(SortState * node)
{
    PlanState *outerPlan;
    outerPlan = outerPlanState(node);
    ExecResetState(outerPlan); 
}

void
ExecInitSortDelta(SortState * node)
{
    PlanState *outerPlan;
    outerPlan = outerPlanState(node);
    ExecInitDelta(outerPlan); 
}
