/*-------------------------------------------------------------------------
 *
 * incmeta.c
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/incmeta.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "nodes/execnodes.h"
#include "executor/execdesc.h"
#include "nodes/plannodes.h"

#include "utils/relcache.h"
#include "utils/rel.h"

#include "executor/incinfo.h"
#include "executor/incTupleQueue.h"
#include "executor/incTQPool.h"
#include "executor/execTPCH.h"
#include "executor/incDecideState.h"
#include "executor/incmodifyplan.h"

#include "optimizer/cost.h"
#include "access/htup_details.h"

#include "miscadmin.h"

#include <math.h>

#define DELTA_THRESHOLD 10

bool enable_incremental;
int  memory_budget;     /* kB units*/
DecisionMethod decision_method;

bool use_material = true; 
bool use_sym_hashjoin = true;  

char *incTagName[INC_TAG_NUM] = {"INVALID", "HASHJOIN", "MERGEJOIN", "NESTLOOP", "AGGHASH", "AGGSORT", 
    "SORT", "MATERIAL", "SEQSCAN", "INDEXSCAN"}; 


/* Functions for managing IncInfo */
static void ExecInitIncInfo (EState *estate, PlanState *ps); 
static IncInfo * BuildIncInfo (); 
static void ExecAssignIncInfo (IncInfo **incInfo_array, int numIncInfo, IncInfo *root);
static IncInfo * ExecInitIncInfoHelper(PlanState *ps, IncInfo *parent, int *count, int *leafCount);
static IncInfo *ExecReplicateIncInfoTree(IncInfo *incInfo, IncInfo *parent);
static void ExecCleanIncInfo(EState *estate); 

/* Functions for estimate updates and propagate updates */
static void ExecEstimateUpdate(EState *estate, bool slave); 
static bool ExecIncPropUpdate(IncInfo *incInfo); 

/* Generating, Waiting, and Collecting update */
static void ExecGenUpdate(EState *estate);
static void ExecWaitUpdate(EState *estate); 
static void ExecCollectUpdate(EState *estate); 

/* Functions for deciding intermediate state to be discarded or not */
static void ExecCollectCostInfo(IncInfo *incInfo, bool compute, bool memory, bool estimate); 
static void ExecDecideState(DPMeta *dpmeta, IncInfo **incInfoArray, int numIncInfo, int incMemory, bool isSlave); 

/* Functions for changing plan*/
static void ExecModifyPlan(EState *estate);
static void ExecFinalizePlan(EState *estate); 

/* Functions for generate pull actions */
static void ExecGenPullAction(IncInfo *incInfo, PullAction parentAction); 

/* Functions for resetting TQ readers */
static void ExecResetTQReader(EState *estate); 

/* Collect stat information */
static void ExecCollectStatInfo(EState *estate);

/* Two helper functions for recursively reset/init inc state */
void ExecResetState(PlanState *ps);
void ExecInitDelta(PlanState *ps); 

double GetTimeDiff(struct timeval x , struct timeval y); 

/* 
 * Interfaces for incremental query processing 
 */
void 
ExecIncStart(EState *estate, PlanState *ps)
{
    elog(NOTICE, "Start of Inc Start"); 
    MemoryContext old = MemoryContextSwitchTo(estate->es_query_cxt); 

    if (estate->es_isSelect)
    {
        ExecInitIncInfo(estate, ps);

        /* Collect Leaf Nodes (Scan Operators) */
        IncInfo **incInfoArray = estate->es_incInfo; 
        estate->reader_ss = palloc(sizeof(ScanState *) * estate->es_numLeaf); 
        for (int i  = 0, j = 0; i < estate->es_numIncInfo; i++) 
        {
            PlanState *ps = incInfoArray[i]->ps; 
            if (ps != NULL && ps->lefttree == NULL && ps->righttree == NULL)
            {
                estate->reader_ss[j] = (ScanState *)ps;
                j++;
            }
        }

        /* Create TQ Pool and TQ Readers */
        IncTQPool *tq_pool = CreateIncTQPool(estate->es_query_cxt, estate->es_numLeaf); 
        for (int i = 0; i < estate->es_numLeaf; i++)
        {
            Relation r = estate->reader_ss[i]->ss_currentRelation; 
            (void) AddIncTQReader(tq_pool, r, RelationGetDescr(r)); 
        }
        estate->tq_pool = tq_pool; 

        /* Potential Updates of TPC-H */
        TPCH_Update *update = BuildTPCHUpdate(tables_with_update);
        estate->tpch_update = update; 

        if (decision_method == DM_DP)
        {
            /* Round memory budget to MB to reduce DP running time */
            estate->es_incMemory = (memory_budget + 1023)/1024; 
    
            /* Create DPMeta for managing DP meta data */
            estate->dpmeta = BuildDPMeta(estate->es_numIncInfo, estate->es_incMemory); 
        }
        else
        {
            estate->es_incMemory = memory_budget; 
        }
    
        /* For Stat*/
        estate->es_statFile = fopen("statfile.txt", "a"); 
        estate->decisionTime = 0;
        estate->repairTime = 0;
        estate->es_incState = (IncState **) palloc(sizeof(IncState *) * estate->es_numIncInfo); 
        for (int i = 0; i < estate->es_numIncInfo; i++)
            estate->es_incState[i] = (IncState *)palloc(sizeof(IncState) * MAX_STATE); 

        estate->numDelta = 1; 
        estate->leftChildExist = false;
        estate->rightChildExist = false; 
        estate->tempLeftPS = NULL;
        estate->tempRightPS = NULL; 
        
        /* Decide intermediate state in the compile time to change plan 
         *
         * 1) estimate update
         * 2) propagate update
         * 3) collect cost information (Note we need to collect compute cost for incInfo_master )
         * 4) decide state 
         * 5) modify plan if necessary 
         *
         * */

        /* Note that we need to collect the compute cost here because the plan might be modified */

        /* step 1. estimate and propagate update */

        struct timeval start, end;  
        gettimeofday(&start , NULL);

        ExecEstimateUpdate(estate, false);

        (void) ExecIncPropUpdate(estate->es_incInfo[estate->es_numIncInfo - 1]); 

        ExecCollectCostInfo(estate->es_incInfo[estate->es_numIncInfo - 1], true, false, false); 

        if (use_material || use_sym_hashjoin)
        {
             ExecEstimateUpdate(estate, true); 

             (void) ExecIncPropUpdate(estate->es_incInfo_slave[estate->es_numIncInfo - 1]); 

             ExecCollectCostInfo(estate->es_incInfo_slave[estate->es_numIncInfo - 1], true, true, true); 

             ExecDecideState(estate->dpmeta, estate->es_incInfo_slave, estate->es_numIncInfo, estate->es_incMemory, true); 

        }
            
        /* Modify Plan */
        ExecModifyPlan(estate); 
        
        gettimeofday(&end , NULL);
        estate->decisionTime += GetTimeDiff(start, end);  

    }
    else /* Modification Command; Collect Writer */
    {
        if (ps->type == T_ModifyTableState)
        {
            ModifyTableState *mt = (ModifyTableState *)ps; 
            estate->writer_mt = mt; 
        }
        else
        {
            elog(ERROR, "We assume the root node is Modify Table"); 
        }
    }

    MemoryContextSwitchTo(old);
    elog(NOTICE, "End of Inc Start"); 
}

void 
ExecIncRun(EState *estate, PlanState *planstate)
{
    elog(NOTICE, "Start of Inc Run");
    MemoryContext old = MemoryContextSwitchTo(estate->es_query_cxt); 

    if (estate->es_isSelect) 
    {
        struct timeval start, end;  
        gettimeofday(&start , NULL);

        /* step 2. collect cost info, decide state, and generate pull actions */
        ExecCollectCostInfo(estate->es_incInfo[estate->es_numIncInfo - 1], false, true, false); 
        ExecDecideState(estate->dpmeta, estate->es_incInfo, estate->es_numIncInfo, estate->es_incMemory, false);
        ExecGenPullAction(estate->es_incInfo[estate->es_numIncInfo - 1], PULL_BATCH_DELTA);

        gettimeofday(&end , NULL);
        estate->decisionTime += GetTimeDiff(start, end);  

        /* step 3. reset state */
        ExecResetTQReader(estate); 
        ExecResetState(planstate); 

        /* step 4. generate, wait, collect, and propagate update */
        ExecGenUpdate(estate);
        ExecWaitUpdate(estate); 
        ExecCollectUpdate(estate); 
        (void) ExecIncPropUpdate(estate->es_incInfo[estate->es_numIncInfo - 1]); 

        /* step 5. generate pull actions */
        ExecGenPullAction(estate->es_incInfo[estate->es_numIncInfo - 1], PULL_BATCH_DELTA); 

        /* step 6. init for delta processing */
        ExecInitDelta(planstate); 

        /* TODO: only one delta right now */
        ExecFinalizePlan(estate);     
    }

    (void) MemoryContextSwitchTo(old);
    elog(NOTICE, "End of Inc Run"); 
}

void 
ExecIncFinish(EState *estate, PlanState *planstate)
{
    elog(NOTICE, "Start of Inc Finish"); 
    MemoryContext old = MemoryContextSwitchTo(estate->es_query_cxt); 

    if (estate->es_isSelect)
    {
        ExecCollectStatInfo(estate); 
        ExecCleanIncInfo(estate); 
        pfree(estate->reader_ss); 
        DestroyIncTQPool(estate->tq_pool); 
    }

    (void) MemoryContextSwitchTo(old); 
    elog(NOTICE, "End of Inc Finish"); 
}

/* 
 * Functions for managing IncInfo 
 * */

static void 
ExecInitIncInfo(EState *estate, PlanState *ps) 
{
    int count = 0; 
    int leafCount = 0; 

    ExecInitIncInfoHelper(ps, NULL, &count, &leafCount);
    estate->es_numLeaf = leafCount; 

    estate->es_incInfo = (IncInfo **) palloc(sizeof(IncInfo *) * count); 
    ExecAssignIncInfo(estate->es_incInfo, count, ps->ps_IncInfo); 
    estate->es_numIncInfo = count; 
    estate->es_totalMemCost  = 0;
    estate->es_usedMemory = 0;  

    IncInfo *root_replica = ExecReplicateIncInfoTree(ps->ps_IncInfo, NULL); 
    estate->es_incInfo_slave = (IncInfo **) palloc(sizeof(IncInfo *) * count);
    ExecAssignIncInfo(estate->es_incInfo_slave, count, root_replica); 
}

static IncInfo *
BuildIncInfo()
{
    IncInfo *incInfo = (IncInfo *) palloc(sizeof(IncInfo));

    incInfo->type = INC_INVALID; 

    incInfo->ps = NULL;
    incInfo->parenttree = NULL;
    incInfo->lefttree = NULL;
    incInfo->righttree = NULL;  

    incInfo->trigger_computation = -1; 
    incInfo->id = -1; 

    incInfo->execDPNode = NULL; 
    incInfo->compute_cost = 0;
    for (int i = 0; i < MAX_STATE; i++)
    {
        incInfo->memory_cost[i] = 0;
        incInfo->prepare_cost[i] = 0;
        incInfo->keep_cost[i] = 0;
        incInfo->delta_cost[i] = 0; 
        incInfo->incState[i] = STATE_DROP; 
    }

    incInfo->leftAction = PULL_BATCH;
    incInfo->rightAction = PULL_BATCH; 
    
    incInfo->leftUpdate = false;
    incInfo->rightUpdate = false;
}

static void 
ExecAssignIncInfo (IncInfo **incInfo_array, int numIncInfo, IncInfo *root)
{
    IncInfo *cur; 
    IncInfo **incInfo_queue = (IncInfo **)palloc(sizeof(IncInfo *) * numIncInfo); 
    int i_queue = 0, cap_queue = 0, id = 0; 

    incInfo_queue[i_queue] = root; 
    cap_queue++; 

    while (i_queue != cap_queue) 
    {
        cur = incInfo_queue[i_queue]; 
        i_queue++;

        if (cur->lefttree != NULL)
        {
            incInfo_queue[cap_queue] = cur->lefttree; 
            cap_queue++; 
        }
        if (cur->righttree != NULL)
        {
            incInfo_queue[cap_queue] = cur->righttree; 
            cap_queue++; 
        }

        incInfo_array[numIncInfo - i_queue] = cur;
        cur->id = numIncInfo - i_queue; 
    }

    pfree(incInfo_queue); 
}

static IncInfo * 
ExecInitIncInfoHelper(PlanState *ps, IncInfo *parent, int *count, int *leafCount) 
{
    Assert(ps != NULL); 

    PlanState *innerPlan; 
    PlanState *outerPlan; 

    IncInfo *incInfo = BuildIncInfo(); 
    incInfo->ps = ps;
    incInfo->parenttree = parent; 

    IncInfo *outerII = NULL;

    AggState *aggState;

    switch (ps->type) 
    {
        case T_HashJoinState:
            incInfo->type = INC_HASHJOIN;

            innerPlan = outerPlanState(innerPlanState(ps)); /* Skip Hash node */
            outerPlan = outerPlanState(ps); 
            innerIncInfo(incInfo) = ExecInitIncInfoHelper(innerPlan, incInfo, count, leafCount); 

            /* Insert Material node to the left subtree */
            outerII = BuildIncInfo(); 
            outerII->type = INC_MATERIAL; 
            outerII->parenttree = incInfo; 
            outerII->ps = NULL;  
            outerIncInfo(incInfo) = outerII;

            (*count)++;

            outerIncInfo(outerII) = ExecInitIncInfoHelper(outerPlan, outerII, count, leafCount); 
            break;

        case T_MergeJoinState:
            elog(ERROR, "Not Support MergeJoin yet"); 
            break; 

        case T_NestLoopState:
            incInfo->type = INC_NESTLOOP; 

            innerPlan = innerPlanState(ps);
            outerPlan = outerPlanState(ps); 
            innerIncInfo(incInfo) = ExecInitIncInfoHelper(innerPlan, incInfo, count, leafCount); 

            /* Insert Material node to the left subtree */
            outerII = BuildIncInfo(); 
            outerII->type = INC_MATERIAL; 
            outerII->parenttree = incInfo; 
            outerII->ps = NULL; 
            outerIncInfo(incInfo) = outerII; 

            (*count)++;

            outerIncInfo(outerII) = ExecInitIncInfoHelper(outerPlan, outerII, count, leafCount); 
            break;  

        case T_SeqScanState:
            incInfo->type = INC_SEQSCAN; 
            (*leafCount)++;
            break; 

        case T_IndexScanState:
            incInfo->type = INC_INDEXSCAN; 
            (*leafCount)++; 
            break; 

        case T_AggState:
            aggState = (AggState *)ps;
            if (aggState->aggstrategy == AGG_SORTED)
                incInfo->type = INC_AGGSORT;
            else
                incInfo->type = INC_AGGHASH; 
            outerPlan = outerPlanState(ps); 
            outerIncInfo(incInfo) = ExecInitIncInfoHelper(outerPlan, incInfo, count, leafCount);
            break;

        case T_SortState:
            incInfo->type = INC_SORT; 
            outerPlan = outerPlanState(ps); 
            outerIncInfo(incInfo) = ExecInitIncInfoHelper(outerPlan, incInfo, count, leafCount);
            break; 

        default:
            elog(ERROR, "InitIncInfoHelper unrecognized nodetype: %u", ps->type);
            return NULL; 
    }

    ps->ps_IncInfo = incInfo;
    (*count)++; 

    return incInfo; 
}


static IncInfo *
ExecReplicateIncInfoTree(IncInfo *incInfo, IncInfo *parent)
{
    if (incInfo == NULL)
        return NULL;

    IncInfo * ret = BuildIncInfo();
    ret->type = incInfo->type; 
    ret->ps = incInfo->ps;  
    ret->id = incInfo->id;

    if (ret->ps != NULL)
        ret->ps->ps_IncInfo_slave = ret; 

    ret->parenttree = parent; 
    ret->lefttree = ExecReplicateIncInfoTree(incInfo->lefttree, ret); 
    ret->righttree = ExecReplicateIncInfoTree(incInfo->righttree, ret); 

    return ret; 
}

static 
FreeIncInfo(IncInfo *root)
{
    if (root->lefttree)
        FreeIncInfo(root->lefttree);
    if (root->righttree)
        FreeIncInfo(root->righttree);
    pfree(root); 
}

static void 
ExecCleanIncInfo(EState *estate)
{
    /* For Stat*/
    if (estate->es_statFile != NULL) 
    {
        FILE *statFile = estate->es_statFile;
        fprintf(statFile, "%d\t%d\t%d\t%.2f\t%.2f\t%.2f\t", \
                estate->es_totalMemCost, estate->es_usedMemory, estate->es_incMemory, estate->batchTime, estate->repairTime, estate->decisionTime); 
        for (int i = 0; i < estate->es_numIncInfo; i++) 
        {
            int id = estate->es_incInfo[i]->id; 
            fprintf(statFile, "%s(%d,%d) ", incTagName[estate->es_incInfo[i]->type], estate->es_incState[id][LEFT_STATE], estate->es_incState[id][RIGHT_STATE]); 
        }
        fprintf(statFile, "\n"); 

        fclose(estate->es_statFile); 
    }

    /* Free IncInfo */
//    IncInfo * root = estate->es_incInfo[estate->es_numIncInfo - 1];
//    while (root->parenttree != NULL)
//        root = root->parenttree;
//    FreeIncInfo(root); 
//
//    pfree(estate->es_incInfo); 
//
//    if (decision_method == DM_DP)
//    {
//        for (int i = 0; i < estate->es_numIncInfo; i++) 
//        {
//            pfree(estate->es_deltaCost[i]); 
//            pfree(estate->es_deltaIncState[i]);
//            pfree(estate->es_deltaMemLeft[i]); 
//            pfree(estate->es_deltaLeftPull[i]);
//            pfree(estate->es_deltaRightPull[i]);
//            
//            pfree(estate->es_bdCost[i]); 
//            pfree(estate->es_bdIncState[i]);
//            pfree(estate->es_bdMemLeft[i]); 
//            pfree(estate->es_bdLeftPull[i]);
//            pfree(estate->es_bdRightPull[i]);   
//        }
//        pfree(estate->es_deltaCost); 
//        pfree(estate->es_deltaIncState);
//        pfree(estate->es_deltaMemLeft); 
//        pfree(estate->es_deltaLeftPull);
//        pfree(estate->es_deltaRightPull);
//        
//        pfree(estate->es_bdCost); 
//        pfree(estate->es_bdIncState);
//        pfree(estate->es_bdMemLeft); 
//        pfree(estate->es_bdLeftPull);
//        pfree(estate->es_bdRightPull);      
//    }
//
//    pfree(estate->es_incState);
}

/* Functions for estimate updates and propagate updates */
static void 
ExecEstimateUpdate(EState *estate, bool slave)
{
    TPCH_Update *update = estate->tpch_update; 
    ScanState **reader_ss_array = estate->reader_ss; 
    ScanState *ss; 

    for (int i = 0; i < estate->es_numLeaf; i++)
    {
        ss = reader_ss_array[i]; 
        if (CheckTPCHUpdate(update, GEN_TQ_KEY(ss->ss_currentRelation))) 
        {
            if (slave)
                ss->ps.ps_IncInfo_slave->leftUpdate = true;
            else
                ss->ps.ps_IncInfo->leftUpdate = true; 
        }
    }
}

static bool 
ExecIncPropUpdate(IncInfo *incInfo)
{
    if (incInfo->lefttree == NULL && incInfo->righttree == NULL) 
    {
        return incInfo->leftUpdate;  
    }
    else if (incInfo->righttree == NULL) 
    {
        incInfo->rightUpdate = false;
        incInfo->leftUpdate = ExecIncPropUpdate(incInfo->lefttree); 
        return incInfo->leftUpdate | incInfo->rightUpdate; 
    }
    else if (incInfo->lefttree == NULL) /* This case is impossible; outer plan is not NULL except for leaf nodes */
    {
        Assert(false);
        return false;
    }
    else
    {
        incInfo->leftUpdate = ExecIncPropUpdate(incInfo->lefttree); 
        incInfo->rightUpdate = ExecIncPropUpdate(incInfo->righttree); 
        return incInfo->leftUpdate | incInfo->rightUpdate; 
    }
}

/* Generating, Waiting, and Collecting update */
static void ExecGenUpdate(EState *estate)
{
    TPCH_Update *update = estate->tpch_update;
    GenTPCHUpdate(update); 
}

static void 
ExecWaitUpdate(EState *estate)
{
    int numDelta = 0; 
    int threshold = DELTA_THRESHOLD; 

    IncTQPool *tq_pool = estate->tq_pool; 

    for (;;) 
    {
		CHECK_FOR_INTERRUPTS();
        
        numDelta = GetTQUpdate(tq_pool); 

        if (numDelta >= threshold) 
        {
            elog(NOTICE, "delta %d", numDelta); 
            break;
        }

       sleep(1); 
    }
}

static void ExecCollectUpdate(EState *estate)
{
    Relation r; 

    IncTQPool *tq_pool = estate->tq_pool; 
    ScanState **reader_ss_array = estate->reader_ss; 

    for(int i = 0; i < estate->es_numLeaf; i++) 
    {
        r = reader_ss_array[i]->ss_currentRelation;  
        reader_ss_array[i]->ps.ps_IncInfo->leftUpdate = HasTQUpdate(tq_pool, r); 

        reader_ss_array[i]->tq_reader = GetTQReader(tq_pool, r, reader_ss_array[i]->tq_reader); 
    }
}

/* Functions for deciding intermediate state to be discarded or not */
static void
ExecCollectCostInfo(IncInfo *incInfo, bool compute, bool memory, bool estimate)
{
    PlanState *ps = incInfo->ps; 
    Plan *plan = NULL; 

    if (incInfo->ps != NULL)
        plan = ps->plan; 

    Plan *innerPlan; 
    Plan *outerPlan; 

    double plan_rows;
    int    plan_width; 

    switch(incInfo->type)
    {
        case INC_HASHJOIN:
            innerPlan = innerPlan(plan);
            outerPlan = outerPlan(plan);
            if (compute) 
            {
                incInfo->prepare_cost[RIGHT_STATE] = (int)(innerPlan->total_cost - outerPlan(innerPlan)->total_cost);
                incInfo->compute_cost = (int)(plan->total_cost - innerPlan->total_cost - outerPlan->total_cost);

                HashJoin *hj = (HashJoin *)plan; 
                double num_hashclauses = (double)length(hj->hashclauses); 
                double inner_delta = innerPlan->plan_rows * 0.01;
                double outer_delta = outerPlan->plan_rows * 0.01; 
                //incInfo->keep_cost[LEFT_STATE] = (DEFAULT_CPU_OPERATOR_COST * num_hashclauses + DEFAULT_CPU_TUPLE_COST) \
                //        * (outerPlan->plan_rows);
                incInfo->keep_cost[LEFT_STATE] = 0; 
                if (incInfo->rightUpdate && !incInfo->leftUpdate)
                {
                    incInfo->delta_cost[LEFT_STATE] = ceil(DEFAULT_CPU_OPERATOR_COST * num_hashclauses * inner_delta); 
                    incInfo->delta_cost[RIGHT_STATE] = ceil(DEFAULT_CPU_OPERATOR_COST * num_hashclauses * outerPlan->plan_rows); 
                }
                else if (incInfo->leftUpdate && !incInfo->rightUpdate)
                {
                    incInfo->delta_cost[RIGHT_STATE] = ceil(DEFAULT_CPU_OPERATOR_COST * num_hashclauses * outer_delta);
                }
                else if (incInfo->leftUpdate && incInfo->rightUpdate)
                {
                    incInfo->delta_cost[LEFT_STATE]  = ceil(DEFAULT_CPU_OPERATOR_COST * num_hashclauses * (inner_delta + outer_delta));
                    incInfo->delta_cost[RIGHT_STATE] = ceil(DEFAULT_CPU_OPERATOR_COST * num_hashclauses * (outer_delta + outerPlan->plan_rows));
                }
            }
            if (memory)
            {
                incInfo->memory_cost[RIGHT_STATE] = ExecHashJoinMemoryCost((HashJoinState *) ps, estimate, true);
                incInfo->memory_cost[LEFT_STATE]  = ExecHashJoinMemoryCost((HashJoinState *) ps, estimate, false);
            }
            ExecCollectCostInfo(incInfo->lefttree, compute, memory, estimate); 
            ExecCollectCostInfo(incInfo->righttree, compute, memory, estimate); 
            break;

        case INC_MERGEJOIN:
            elog(ERROR, "not supported MergeJoin yet"); 
            break;

        case INC_NESTLOOP:
            innerPlan = innerPlan(plan);
            outerPlan = outerPlan(plan);
            if (compute) 
            {
                incInfo->prepare_cost[RIGHT_STATE] = (int)(plan->startup_cost - innerPlan->total_cost); 
                incInfo->compute_cost = (int)(plan->total_cost- plan->startup_cost);
                NestLoop *nl = (NestLoop *)plan; 
                double num_hashclauses = (double)length(nl->join.joinqual); 
                double inner_delta = innerPlan->plan_rows * 0.01;
                double outer_delta = outerPlan->plan_rows * 0.01; 
                //incInfo->keep_cost[LEFT_STATE] = (DEFAULT_CPU_OPERATOR_COST * num_hashclauses + DEFAULT_CPU_TUPLE_COST) \
                //        * (outerPlan->plan_rows);
                incInfo->keep_cost[LEFT_STATE] = 0; 
                if (incInfo->rightUpdate && !incInfo->leftUpdate)
                {
                    incInfo->delta_cost[LEFT_STATE] = ceil(DEFAULT_CPU_OPERATOR_COST * num_hashclauses * inner_delta); 
                    incInfo->delta_cost[RIGHT_STATE] = ceil(DEFAULT_CPU_OPERATOR_COST * num_hashclauses * outerPlan->plan_rows); 
                }
                else if (incInfo->leftUpdate && !incInfo->rightUpdate)
                {
                    incInfo->delta_cost[RIGHT_STATE] = ceil(DEFAULT_CPU_OPERATOR_COST * num_hashclauses * outer_delta);
                }
                else if (incInfo->leftUpdate && incInfo->rightUpdate)
                {
                    incInfo->delta_cost[LEFT_STATE]  = ceil(DEFAULT_CPU_OPERATOR_COST * num_hashclauses * (inner_delta + outer_delta));
                    incInfo->delta_cost[RIGHT_STATE] = ceil(DEFAULT_CPU_OPERATOR_COST * num_hashclauses * (outer_delta + outerPlan->plan_rows));
                }
            }

            if (memory)
                incInfo->memory_cost[LEFT_STATE]  = ExecNestLoopMemoryCost((NestLoopState *) ps, estimate);

            ExecCollectCostInfo(incInfo->lefttree, compute, memory, estimate); 
            ExecCollectCostInfo(incInfo->righttree, compute, memory, estimate);
            break;

        case INC_SEQSCAN:
        case INC_INDEXSCAN:
            if (compute)
            {
                incInfo->prepare_cost[LEFT_STATE] = ceil(plan->startup_cost);
                incInfo->compute_cost = ceil(plan->total_cost - plan->startup_cost); 
            }
            return;  
            break;

        case INC_AGGHASH:
            outerPlan = outerPlan(plan); 
            if (compute)
            {
                incInfo->prepare_cost[LEFT_STATE] = (int)(ceil(plan->startup_cost - outerPlan->total_cost)); 
                incInfo->compute_cost = (int)(ceil(plan->total_cost - plan->startup_cost)); 
            }
            
            if (memory)
            {
                incInfo->memory_cost[LEFT_STATE] = ExecAggMemoryCost((AggState *) ps, estimate); 
            }
            ExecCollectCostInfo(incInfo->lefttree, compute, memory, estimate); 
            break;

        case INC_AGGSORT:
            outerPlan = outerPlan(plan); 
            if (compute)
            {
                incInfo->prepare_cost[LEFT_STATE] = (int)(ceil(plan->startup_cost - outerPlan->startup_cost)); 
                incInfo->compute_cost = (int)(ceil(plan->total_cost - plan->startup_cost)); 
            }
            
            if (memory)
            {
                incInfo->memory_cost[LEFT_STATE] = ExecAggMemoryCost((AggState *) ps, estimate); 
            }
            ExecCollectCostInfo(incInfo->lefttree, compute, memory, estimate); 
            break; 

        case INC_SORT:
            outerPlan = outerPlan(plan); 
            if (compute)
            {
                incInfo->prepare_cost[LEFT_STATE] = (int)(ceil(plan->startup_cost - outerPlan->total_cost)); 
                incInfo->compute_cost = (int)(ceil(plan->total_cost - plan->startup_cost)); 
            }

            if (memory)
                incInfo->memory_cost[LEFT_STATE] = ExecSortMemoryCost((SortState *) ps, estimate); 
            ExecCollectCostInfo(incInfo->lefttree, compute, memory, estimate); 
            break;

        case INC_MATERIAL:
            outerPlan = outerIncInfo(incInfo)->ps->plan; 
            plan_rows = outerPlan->plan_rows; 
            plan_width = outerPlan->plan_width; 
            if (compute)
            {
                incInfo->prepare_cost[LEFT_STATE] = 0;
                outerPlan = incInfo->parenttree->ps->lefttree->plan; 
                incInfo->compute_cost = 0;
                incInfo->keep_cost[LEFT_STATE] =  (int)(ceil(2 * DEFAULT_CPU_OPERATOR_COST * plan_rows));
            }
            if (memory)
            {
                if (plan == NULL)
                    incInfo->memory_cost[LEFT_STATE] = (plan_rows * (MAXALIGN(plan_width) + MAXALIGN(SizeofHeapTupleHeader)) + 1023) / 1024;
                else
                    incInfo->memory_cost[LEFT_STATE] = ExecMaterialIncMemoryCost((MaterialIncState *) ps, estimate); 
            }
            ExecCollectCostInfo(incInfo->lefttree, compute, memory, estimate); 
            break; 

        default:
            elog(ERROR, "CollectCost unrecognized nodetype: %u", ps->type);
            return NULL; 
    }

    if (memory && decision_method == DM_DP)
    {
        for (int i = 0; i < MAX_STATE; i++)
            incInfo->memory_cost[i] = (incInfo->memory_cost[i] + 1023) / 1024; 
    }
}

static void 
ExecDecideState(DPMeta *dpmeta, IncInfo **incInfoArray, int numIncInfo, int incMemory, bool isSlave)
{
    if (decision_method == DM_DP) 
    {
        ExecDPSolution(dpmeta, incInfoArray, numIncInfo, incMemory, isSlave);
        ExecDPAssignState(dpmeta, incInfoArray, numIncInfo - 1, incMemory, PULL_BATCH_DELTA);
    }
    else
    {
        ExecGreedySolution(incInfoArray, numIncInfo, incMemory, decision_method); 
    }
}

MaterialIncState *ExecBuildMaterialInc(EState *estate); 

static void
ExecModifyPlan(EState *estate)
{
    MaterialIncState *ms; 
    IncInfo **incInfoArray_slave = estate->es_incInfo_slave; 
    IncInfo *incInfo_slave; 

    IncInfo **incInfoArray = estate->es_incInfo;
    IncInfo *incInfo; 

    for (int i = 0; i < estate->es_numIncInfo; i++)
    {
        incInfo_slave = incInfoArray_slave[i];
        incInfo = incInfoArray[i]; 

        if (incInfo_slave->type == INC_MATERIAL)
        {
            if (incInfo_slave->ps == NULL)
            {
                if (incInfo_slave->incState[LEFT_STATE] == STATE_KEEPMEM) /* Build a Material node and insert it */
                {
                    estate->tempLeftPS = incInfo_slave->lefttree->ps; 
                    ms = ExecBuildMaterialInc(estate); 
                    estate->tempLeftPS = NULL; 
                    IncInsertNodeAfter((PlanState *)ms, incInfo->parenttree->ps, true); 
                    incInfo_slave->ps = (PlanState *)ms; 
                    incInfo->ps = (PlanState *)ms; 
                    ms->ss.ps.ps_IncInfo = incInfo; 
                    ms->ss.ps.ps_IncInfo_slave = incInfo_slave; 

                    /* copy some meta data */
                    ms->ss.ps.plan->plan_rows = ms->ss.ps.plan->lefttree->plan_rows; 
                    ms->ss.ps.plan->plan_width = ms->ss.ps.plan->lefttree->plan_width; 

                }
            }
            else /* Material node exists */
            {
                ExecMaterialIncMarkKeep((MaterialIncState *)incInfo_slave->ps, incInfo_slave->incState[LEFT_STATE]); 
                if (incInfo->incState[LEFT_STATE] == STATE_DROP && incInfo_slave->incState[LEFT_STATE] == STATE_DROP) /* Drop the Material node */
                {
                    IncDeleteNode(incInfo_slave->parenttree->ps, true); 
                    incInfo_slave->ps = NULL;
                    incInfo->ps = NULL;  
                }
            }
        }

        if (incInfo_slave->type == INC_HASHJOIN)
            ExecHashJoinIncMarkKeep((HashJoinState *)incInfo_slave->ps, incInfo_slave->incState[LEFT_STATE]);

        if (incInfo_slave->type == INC_NESTLOOP)
            ExecNestLoopIncMarkKeep((NestLoopState *)incInfo_slave->ps, incInfo_slave->incState[LEFT_STATE]); 
    }
}

static void
ExecFinalizePlan(EState *estate)
{
    IncInfo **incInfoArray = estate->es_incInfo;
    IncInfo *incInfo; 

    IncInfo **incInfoArray_slave = estate->es_incInfo_slave; 
    IncInfo *incInfo_slave; 

    for (int i = 0; i < estate->es_numIncInfo; i++)
    {
        incInfo = incInfoArray[i]; 
        incInfo_slave = incInfoArray_slave[i]; 

        if (incInfo->type == INC_MATERIAL && incInfo->ps != NULL)
        {
            ExecMaterialIncMarkKeep((MaterialIncState *)incInfo->ps, STATE_DROP); 
            if (incInfo->incState[LEFT_STATE] == STATE_DROP) /* Drop the MaterialInc node */
            {
                IncDeleteNode(incInfo->parenttree->ps, true); 
                incInfo->ps = NULL; 
                incInfo_slave->ps = NULL; 
            }

        }
    }
}

/* Generate Pull Actions */
static void
ExecGenPullAction(IncInfo *incInfo, PullAction parentAction)
{
    incInfo->leftAction = PULL_DELTA; 
    incInfo->rightAction = PULL_DELTA; 

    if (incInfo->lefttree == NULL && incInfo->righttree == NULL) /* Scan operator */
    {
        incInfo->leftAction = parentAction;
        incInfo->rightAction = parentAction; 
    }
    else if (incInfo->righttree == NULL) /* Agg, Sort, or MaterialInc operator */
    {
        switch (incInfo->type) 
        { 
            case INC_AGGHASH:
            case INC_AGGSORT:
                /*                                                                                   
                 * TODO: right now, we always return the whole aggregation results; 
                 *       we will fix it later when we support negation                  
                 */ 
                if (incInfo->incState[LEFT_STATE] == STATE_DROP)
                    incInfo->leftAction = PULL_BATCH_DELTA;

                break;

            case INC_SORT:
            case INC_MATERIAL:
                if (parentAction == PULL_BATCH_DELTA && incInfo->incState[LEFT_STATE] == STATE_DROP)
                    incInfo->leftAction = PULL_BATCH_DELTA;

                break;

            default:                                                                                 
                elog(ERROR, "unsupported node type except sort and aggregate");
                return;                                                                              
        }
    }
    else if (incInfo->lefttree == NULL)
    {
        Assert(false); 
    }
    else                                /* Join operator */
    {
        if (parentAction == PULL_BATCH_DELTA) 
        {
            if (incInfo->incState[LEFT_STATE] == STATE_DROP)
                incInfo->leftAction = PULL_BATCH_DELTA;
 
            if (incInfo->incState[RIGHT_STATE] == STATE_DROP)
                incInfo->rightAction = PULL_BATCH_DELTA;
        } 
        else if (parentAction == PULL_DELTA) /* only need delta */ 
        {
            if (incInfo->leftUpdate && incInfo->incState[RIGHT_STATE] == STATE_DROP) 
                incInfo->rightAction = PULL_BATCH_DELTA;

            if (incInfo->rightUpdate && incInfo->incState[LEFT_STATE] == STATE_DROP) 
                incInfo->leftAction = PULL_BATCH_DELTA;
        }
    }

    if ((!incInfo->leftUpdate && incInfo->leftAction == PULL_DELTA) || parentAction == PULL_NOTHING)
            incInfo->leftAction = PULL_NOTHING;
        
    if ((!incInfo->rightUpdate && incInfo->rightAction == PULL_DELTA) || parentAction == PULL_NOTHING)
            incInfo->rightAction = PULL_NOTHING; 

    if (incInfo->lefttree != NULL) 
        ExecGenPullAction(incInfo->lefttree, incInfo->leftAction); 

    if (incInfo->righttree != NULL)
        ExecGenPullAction(incInfo->righttree, incInfo->rightAction); 
}

/* Functions for resetting TQ readers */
static void 
ExecResetTQReader(EState *estate)
{
    IncTQPool *tq_pool = estate->tq_pool;
    ScanState **reader_ss_array = estate->reader_ss; 

    for (int i = 0; i < estate->es_numLeaf; i++)
        DrainTQReader(tq_pool, reader_ss_array[i]->tq_reader); 
}


static void 
ExecCollectStatInfo(EState *estate)
{
    IncInfo   **incInfoArray = estate->es_incInfo; 
    int         numIncInfo = estate->es_numIncInfo; 

    IncInfo   *incInfo;
    int        id; 

    for (int i = 0; i < numIncInfo; i++)
    {
        incInfo = incInfoArray[i]; 
        id = incInfo->id; 
        for (int j = 0; j < MAX_STATE; j++)
            estate->es_totalMemCost += incInfo->memory_cost[j]; 

        if (incInfo->incState[LEFT_STATE] == STATE_KEEPMEM)
            estate->es_usedMemory += incInfo->memory_cost[LEFT_STATE]; 

        if (incInfo->incState[RIGHT_STATE] == STATE_KEEPMEM) 
            estate->es_usedMemory += incInfo->memory_cost[RIGHT_STATE];

        switch(incInfo->type)
        {
            case INC_HASHJOIN:
                estate->es_incState[id][LEFT_STATE] = incInfo->incState[LEFT_STATE]; 
                estate->es_incState[id][RIGHT_STATE]  = incInfo->incState[RIGHT_STATE]; 
                break;
    
            case INC_MERGEJOIN:
                elog(ERROR, "not supported MergeJoin yet"); 
                break;
    
            case INC_NESTLOOP:
                estate->es_incState[id][LEFT_STATE]  = incInfo->incState[LEFT_STATE]; 
                break;
    
            case INC_SEQSCAN:
            case INC_INDEXSCAN:
                estate->es_incState[id][LEFT_STATE] = incInfo->incState[LEFT_STATE]; 
                break; 
    
            case INC_AGGSORT:
                estate->es_incState[id][LEFT_STATE]  = STATE_DROP; 
                break; 
    
            case INC_AGGHASH:
            case INC_MATERIAL:
            case INC_SORT:
                estate->es_incState[id][LEFT_STATE]  = incInfo->incState[LEFT_STATE]; 
                break; 

            default:
                elog(ERROR, "CollectStat unrecognized nodetype: %u", incInfo->type);
                return; 
        }
    }
}

/*
 * Step 3. After dropping/keeping states is done, 
 *         if there are deltas for base tables, 
 *         we take the following two steps to 
 *         prepare meta information for processing delta data
 *
 *         Step 3a. propogate delta information from base tables to parents 
 *
 *         Step 3b. combined with IncState, decide PullActions (see include/executor/incmeta.h for details). 
 */

void 
ExecResetState(PlanState *ps)
{
    switch (ps->type) 
    {
        case T_HashJoinState:
            ExecResetHashJoinState((HashJoinState *) ps); 
            break;

        case T_MergeJoinState:
            ExecResetMergeJoinState((MergeJoinState *) ps); 
            break; 

        case T_NestLoopState:
            ExecResetNestLoopState((NestLoopState *) ps); 
            break; 

        case T_SeqScanState:
            ExecResetSeqScanState((SeqScanState *) ps); 
            break; 

        case T_IndexScanState:
            ExecResetIndexScanState((IndexScanState *) ps); 
            break; 

        case T_AggState:
            ExecResetAggState((AggState *) ps); 
            break;

        case T_SortState:
            ExecResetSortState((SortState *) ps); 
            break;

        case T_MaterialIncState:
            ExecResetMaterialIncState((MaterialIncState *) ps);
            break; 
        
        default:
            elog(ERROR, "ResetState unrecognized nodetype: %u", ps->type);
            return; 
    }
}


void 
ExecInitDelta(PlanState *ps)
{
    switch (ps->type) 
    {
        case T_HashJoinState:
            ExecInitHashJoinDelta((HashJoinState *) ps); 
            break;

        case T_MergeJoinState:
            ExecInitMergeJoinDelta((MergeJoinState *) ps); 
            break;

        case T_NestLoopState:
            ExecInitNestLoopDelta((NestLoopState *) ps); 
            break; 

        case T_SeqScanState:
            ExecInitSeqScanDelta((SeqScanState *) ps); 
            break; 

        case T_IndexScanState:
            ExecInitIndexScanDelta((IndexScanState *) ps); 
            break; 

        case T_AggState:
            ExecInitAggDelta((AggState *) ps); 
            break;

        case T_MaterialIncState:
            ExecInitMaterialIncDelta((MaterialIncState *) ps); 
            break; 

        case T_SortState:
            ExecInitSortDelta((SortState *) ps); 
            break;
        
        default:
            elog(ERROR, "InitDelta unrecognized nodetype: %u", ps->type);
            return; 
    }
}

bool CheckMatch(bool leftDelta, bool rightDelta, int pullEncoding)
{
    int genEncoding = 0;
    genEncoding |= (leftDelta || rightDelta) << 1; 
    genEncoding |= (!leftDelta && !rightDelta); 

    return ((genEncoding & pullEncoding) != 0); 
}

int EncodePullAction(PullAction pullAction)
{
    int pullEncoding = 0; 
    if (pullAction == PULL_BATCH)
        pullEncoding = 0x1;
    else if (pullAction == PULL_DELTA)
        pullEncoding = 0x2;
    else if (pullAction == PULL_BATCH_DELTA)
        pullEncoding = 0x3; 

    return pullEncoding; 
}


double
GetTimeDiff(struct timeval x , struct timeval y)
{
    double x_ms , y_ms , diff;
	
	x_ms = (double)x.tv_sec*1000000 + (double)x.tv_usec;
	y_ms = (double)y.tv_sec*1000000 + (double)y.tv_usec;
	
	diff = (double)y_ms - (double)x_ms;

    return diff/1000; 
}
