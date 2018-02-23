/*-------------------------------------------------------------------------
 *
 * incmeta.h
 *	  header for managing incremental computing states and corresponding routine prototypes 
 *
 *
 * src/include/executor/incmeta.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef INCMETA_H
#define INCMETA_H 

#include "nodes/execnodes.h"
#include "executor/executor.h"
#include "executor/incinfo.h"

extern bool enable_incremental;
extern int  memory_budget;
extern DecisionMethod decision_method; 


/*
 * MarkTupComplete -- mark a tuple complete or not
 */

#define MarkTupComplete(slot, iscomplete) \
    ((slot)->tts_iscomplete = iscomplete) 

/*
 * TupIsComplete -- does a TupleTableSlot indicate completion ? 
 * Note that a NULL slot indicates complete
 */

#define TupIsComplete(slot) \
    ((slot) == NULL || (slot)->tts_iscomplete) 


/*
 * prototypes from functions in executor/execScan.c
 */

extern void InitScanInc(ScanState *node);

extern TupleTableSlot * ExecScanInc(ScanState *node,
		 ExecScanAccessMtd accessMtd,
		 ExecScanRecheckMtd recheckMtd); 

extern void EndScanInc(ScanState *node); 

extern void ReScanScanInc(ScanState *node); 

/*
 * prototypes from functions in executor/nodeNestloopInc.c
 * */

extern void ExecInitNestLoopInc(NestLoopState *node, int eflags); 

extern TupleTableSlot * ExecNestLoopInc(PlanState *pstate); 

/*
 * prototypes from functions in executor/nodeHashjoinInc.c
 */

extern void ExecInitHashJoinInc(HashJoinState *hjstate); 

extern TupleTableSlot * ExecHashJoinInc(PlanState *pstate);

/*
 * prototypes from functions in executor/nodeSortInc.c
 */

extern TupleTableSlot * ExecSortInc(PlanState *pstate); 

extern void ExecInitSortInc(SortState *node); 

extern void ExecExchangeSortState(SortState *node); 

/*
 * prototypes from functions in executor/nodeMergejoinInc.c
 */
extern TupleTableSlot * ExecMergeJoinInc(PlanState *pstate);

extern void ExecInitMergeJoinInc(MergeJoinState *mergestate); 

/*
 * prototypes from functions in executor/nodeAggInc.c
 */
extern TupleTableSlot * ExecAggInc(PlanState *pstate);

extern void ExecInitAggInc(AggState *aggstate); 

/*
 * prototypes from functions in executor/incmeta.c
 */
extern void ExecIncStart(EState *estate, PlanState *planstate);

extern void ExecIncRun(EState *estate, PlanState *planstate);

extern void ExecIncFinish(EState *estate, PlanState *planstate); 

extern void ExecResetState(PlanState *ps); 

extern void ExecInitDelta(PlanState *ps); 

extern double GetTimeDiff(struct timeval x , struct timeval y); 

/*
 * prototypes from functions for ExecResetState 
 */
extern void ExecResetNestLoopState(NestLoopState * node); 

extern void ExecResetHashJoinState(HashJoinState * node); 

extern void ExecResetMergeJoinState(MergeJoinState * node); 

extern void ExecResetSeqScanState(SeqScanState * node); 

extern void ExecResetIndexScanState(IndexScanState * node); 

extern void ExecResetAggState(AggState * node); 

extern void ExecResetSortState(SortState * node); 

/*
 * prototypes from functions for ExecInitDelta
 */
extern void ExecInitNestLoopDelta(NestLoopState * node); 

extern void ExecInitHashJoinDelta(HashJoinState * node); 

extern void ExecInitMergeJoinDelta(MergeJoinState * node); 

extern void ExecInitSeqScanDelta(SeqScanState * node); 
            
extern void ExecInitIndexScanDelta(IndexScanState * node); 

extern void ExecInitAggDelta(AggState * node); 

extern void ExecInitSortDelta(SortState * node); 

/*
 * prototypes for getting memory cost
 */
extern int ExecHashJoinMemoryCost(HashJoinState * node); 

//extern void ExecInitMergeJoinDelta(MergeJoinState * node); 

extern int ExecAggMemoryCost(AggState * node); 

extern int ExecSortMemoryCost(SortState * node); 
 
#endif

