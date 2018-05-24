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

extern bool enable_incremental;
extern int  memory_budget;
extern bool use_sym_hashjoin; 

/*
 * We need to define several states used by TupleTableSlot
 * */
#define TTS_COMPLETE 0x1
#define TTS_DELTA 0x2

#define MarkTupComplete(slot, iscomplete) \
    ((slot)->tts_inc_state = (iscomplete ? ((slot)->tts_inc_state|TTS_COMPLETE) : ((slot)->tts_inc_state & ~TTS_COMPLETE)))

#define MarkTupDelta(slot, isdelta) \
    ((slot)->tts_inc_state = (isdelta ? ((slot)->tts_inc_state|TTS_DELTA) : ((slot)->tts_inc_state & ~TTS_DELTA)))

#define TupIsComplete(slot) \
    ((slot) == NULL || ((slot)->tts_inc_state & TTS_COMPLETE) != 0)

#define TupIsDelta(slot) \
    (((slot)->tts_inc_state & TTS_DELTA) != 0)


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

extern void ExecInitHashJoinInc(HashJoinState *hjstate, EState *estate, int eflags); 

extern TupleTableSlot * ExecHashJoinInc(PlanState *pstate);

extern bool ExecScanHashBucketInc(HashJoinState *hjstate, ExprContext *econtext, bool outer); 


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

extern bool CheckMatch(bool leftDelta, bool rightDelta, int pullEncoding);

extern int EncodePullAction(PullAction pullAction); 

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

extern void ExecResetMaterialIncState(MaterialIncState * node);

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

extern void ExecInitMaterialIncDelta(MaterialIncState *node); 

/*
 * prototypes for getting memory cost
 */
extern int ExecHashJoinMemoryCost(HashJoinState * node, bool estimate, bool right); 
extern int ExecEstimateHashTableSize(double ntuples, int tupwidth); 

//extern void ExecInitMergeJoinDelta(MergeJoinState * node); 

extern int ExecAggMemoryCost(AggState * node, bool estimate); 

extern int ExecSortMemoryCost(SortState * node, bool estimate); 

extern int ExecMaterialIncMemoryCost(MaterialIncState * node, bool estimate); 

extern MaterialIncState *ExecBuildMaterialInc(EState *estate);

extern void ExecMaterialIncMarkKeep(MaterialIncState *ms, IncState state); 

extern void ExecHashJoinIncMarkKeep(HashJoinState *hjs, IncState state); 

extern void ExecHashIncreaseNumBuckets(HashJoinTable hashtable);

extern int ExecNestLoopMemoryCost(NestLoopState * node, bool estimate); 

extern void ExecNestLoopIncMarkKeep(NestLoopState *nl, IncState state); 

extern void BuildOuterHashNode(HashJoinState *hjstate, EState *estate, int eflags); 

/* other prototypes for MaterialInc */
extern void ExecReScanMaterialInc(MaterialIncState *node); 
extern void ExecEndMaterialInc(MaterialIncState *node); 

#endif

