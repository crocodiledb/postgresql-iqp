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
#include "executor/execExpr.h"
#include "executor/execTPCH.h"

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

#include "executor/iqpquery.h"

#include "miscadmin.h"

#include "utils/snapmgr.h"

#include <math.h>
#include <string.h>

/* For creating and destroying query online */
#include "parser/parser.h"
#include "nodes/parsenodes.h"
#include "nodes/params.h"
#include "nodes/plannodes.h"

#define CONF_DIR "iqp_conf/"

#define STAT_TIME_FILE  "iqp_stat/time.out"
#define STAT_MEM_FILE   "iqp_stat/mem.out"
#define STAT_STATE_FILE "iqp_stat/state.out"

#define STAT_MEM_SUMMARY_FILE "iqp_stat/mem_summary.out"

#define DELTA_THRESHOLD 1

char *iqp_query;

bool gen_mem_info;
bool enable_incremental;
int  memory_budget;     /* kB units*/
DecisionMethod decision_method;

bool use_material = true; 
bool use_sym_hashjoin = true;
bool use_default_tpch = false;
bool know_dist_only = false; 

bool useBruteForce = false; 

char *incTagName[INC_TAG_NUM] = {"INVALID", "HASHJOIN", "MERGEJOIN", "NESTLOOP", "AGGHASH", "AGGSORT", 
    "SORT", "MATERIAL", "SEQSCAN", "INDEXSCAN"}; 

/* Functions for replacing base ps */
static QueryDesc * ExecIQPBuildPS(EState *estate, char *sqlstr);
static iqp_base * ExecBuildIQPBase(EState *estate, char *query_conf);
static void ExecSwapinIQPBase(EState *estate, PlanState *root, iqp_base *base);
static void ExecSwapoutIQPBase(iqp_base *base);
static Cost PropagateDeltaCost(Plan *parent);

/* Functions for managing IncInfo */
static void ExecInitIncInfo (EState *estate, PlanState *ps); 
static IncInfo * BuildIncInfo (); 
static void ExecAssignIncInfo (IncInfo **incInfo_array, int numIncInfo, IncInfo *root);
static IncInfo * ExecInitIncInfoHelper(PlanState *ps, IncInfo *parent, int *count, int *leafCount);
static IncInfo *ExecReplicateIncInfoTree(IncInfo *incInfo, IncInfo *parent);
static void ExecCopyIncInfo(IncInfo **incInfo_array, IncInfo **incInfo_array_slave, int numIncInfo); 
static void ExecFinalizeStatInfo(EState *estate); 
static void ExecAssignStateExist(IncInfo **incInfo_array, IncInfo **incInfo_array_slave, int numIncInfo); 

/* Functions for estimate updates and propagate updates */
static void ExecEstimateUpdate(EState *estate, bool slave); 
static bool ExecIncPropUpdate(IncInfo *incInfo); 

/* Generating, Waiting, and Collecting update */
static void ExecGenUpdate(EState *estate, int deltaIndex);
static void ExecWaitUpdate(EState *estate); 
static void ExecCollectUpdate(EState *estate); 

/* Functions for deciding intermediate state to be discarded or not */
static void ExecReadMemInfo(IncInfo **incInfoArray, int numIncInfo, char *mem_info); 
static void ExecCollectCostInfo(IncInfo *incInfo, bool compute, bool memory); 
static void ExecDecideState(DPMeta *dpmeta, IncInfo **incInfoArray, int numIncInfo, int incMemory, bool isSlave); 

/* Functions for changing plan*/
static void ExecUpgradePlan(EState *estate);
static void ExecDegradePlan(EState *estate);
static void ExecGenFullPlan(EState *estate);

/* Functions for generate pull actions */
static void ExecGenPullAction(IncInfo *incInfo, PullAction parentAction); 

/* Functions for resetting TQ readers */
static void ExecResetTQReader(EState *estate); 

/* Collect stat information */
static void ExecCollectPerDeltaInfo(EState *estate);

/* Two helper functions for recursively reset/init inc state */
void ExecResetState(PlanState *ps);
void ExecInitDelta(PlanState *ps); 

/* Helper function for taking a new snapshot */
static void TakeNewSnapshot(EState *estate); 

double GetTimeDiff(struct timeval x , struct timeval y); 

static void PrintOutAttNum(PlanState *ps); 

/* 
 * Interfaces for incremental query processing 
 */
void 
ExecIncStart(EState *estate, PlanState *ps)
{
    elog(NOTICE, "Start of Inc Start"); 
    MemoryContext old = MemoryContextSwitchTo(estate->es_query_cxt); 

    /* Replacing base PlanState */
    if (estate->es_isSelect)
    {
        if (gen_mem_info)
            PrintOutAttNum(ps);

        char query_conf[50];
        memset(query_conf, 0, sizeof(query_conf));
        sprintf(query_conf, "%s.conf", iqp_query);
        iqp_base *base = ExecBuildIQPBase(estate, query_conf);
        ExecSwapinIQPBase(estate, ps, base);
        estate->base = base; 

        (void) PropagateDeltaCost(ps->plan);
    }

    if (estate->es_isSelect && !gen_mem_info)
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
            estate->reader_ss[i]->ps.state->tq_pool = tq_pool; 
            Relation r = estate->reader_ss[i]->ss_currentRelation; 
            (void) AddIncTQReader(tq_pool, r, RelationGetDescr(r)); 
        }
        estate->tq_pool = tq_pool; 

        /* Potential Updates of TPC-H */
        estate->tpch_update = ExecInitTPCHUpdate(); 
        estate->numDelta = estate->tpch_update->numdelta; 
        estate->deltaIndex = 0; 

        if (delta_mode == TPCH_DEFAULT)
            use_default_tpch = true;  

        /* Round memory budget to MB to reduce DP running time */
        estate->es_incMemory = (memory_budget + 1023)/1024; 
        if (decision_method == DM_DP)
        {
            /* Create DPMeta for managing DP meta data */
            estate->dpmeta = BuildDPMeta(estate->es_numIncInfo, estate->es_incMemory); 
        }
    
        /* For Stat*/
        estate->es_timeFile = fopen(STAT_TIME_FILE, "a"); 
        estate->es_memFile = fopen(STAT_MEM_FILE, "a"); 
        estate->es_stateFile = fopen(STAT_STATE_FILE, "a"); 
        estate->decisionTime = 0;
        estate->execTime = (double *)palloc(sizeof(double) * (estate->numDelta + 1));
        memset(estate->execTime, 0, sizeof(double) * (estate->numDelta + 1)); 
        estate->es_incState = (IncState **) palloc(sizeof(IncState *) * estate->es_numIncInfo); 
        for (int i = 0; i < estate->es_numIncInfo; i++)
            estate->es_incState[i] = (IncState *)palloc(sizeof(IncState) * MAX_STATE); 

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

        struct timeval start, end;  
        gettimeofday(&start , NULL);

        ExecEstimateUpdate(estate, true); 

        (void) ExecIncPropUpdate(estate->es_incInfo_slave[estate->es_numIncInfo - 1]); 

        char memInfo[50];
        memset(memInfo, 0, sizeof(memInfo));
        sprintf(memInfo, "%s.mem", iqp_query);
        ExecReadMemInfo(estate->es_incInfo_slave, estate->es_numIncInfo, memInfo);

        ExecCollectCostInfo(estate->es_incInfo_slave[estate->es_numIncInfo - 1], true, false); 

        ExecDecideState(estate->dpmeta, estate->es_incInfo_slave, estate->es_numIncInfo, estate->es_incMemory, true); 
            
        /* Modify Plan */
        ExecUpgradePlan(estate); 
        
        gettimeofday(&end , NULL);
        estate->decisionTime += GetTimeDiff(start, end);  

        TakeNewSnapshot(estate);
    }
    else if (estate->es_isSelect && gen_mem_info)
    {
        estate->numDelta = 0; 
        estate->deltaIndex = 0; 

        ExecInitIncInfo(estate, ps);

        estate->es_memStatFile = fopen(STAT_MEM_SUMMARY_FILE, "a"); 

        estate->execTime = (double *)palloc(sizeof(double) * (estate->numDelta + 1));
        memset(estate->execTime, 0, sizeof(double) * (estate->numDelta + 1)); 

        estate->leftChildExist = false;
        estate->rightChildExist = false; 
        estate->tempLeftPS = NULL;
        estate->tempRightPS = NULL; 

        ExecGenFullPlan(estate); 
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
        TakeNewSnapshot(estate); 

        /* step 1. estimate and propagate update */
        //ExecEstimateUpdate(estate, false);
        //(void) ExecIncPropUpdate(estate->es_incInfo[estate->es_numIncInfo - 1]); 

        /* step 2. collect cost info, decide state, and generate pull actions */
        //ExecCollectCostInfo(estate->es_incInfo[estate->es_numIncInfo - 1], true, true); 
        //ExecDecideState(estate->dpmeta, estate->es_incInfo, estate->es_numIncInfo, estate->es_incMemory, false);

        ExecCopyIncInfo(estate->es_incInfo, estate->es_incInfo_slave, estate->es_numIncInfo); 
        ExecCollectPerDeltaInfo(estate);
        ExecDegradePlan(estate);
        ExecGenPullAction(estate->es_incInfo[estate->es_numIncInfo - 1], PULL_BATCH_DELTA);

        /* step 3. reset state */
        ExecResetTQReader(estate); 
        ExecResetState(planstate); 

        /* step 4. let's consider the delta after the next delta */
        if (estate->deltaIndex < estate->numDelta)
        {
            struct timeval start, end;  
            gettimeofday(&start , NULL); 

            ExecAssignStateExist(estate->es_incInfo, estate->es_incInfo_slave, estate->es_numIncInfo); 

            ExecEstimateUpdate(estate, true);

            (void) ExecIncPropUpdate(estate->es_incInfo_slave[estate->es_numIncInfo - 1]); 

            ExecCollectCostInfo(estate->es_incInfo_slave[estate->es_numIncInfo - 1], true, true); 

            ExecDecideState(estate->dpmeta, estate->es_incInfo_slave, estate->es_numIncInfo, estate->es_incMemory, true); 

            ExecUpgradePlan(estate); 

            gettimeofday(&end , NULL);
            estate->decisionTime += GetTimeDiff(start, end);  
        }

        /* step 5. generate, wait, collect, and propagate update */
        ExecGenUpdate(estate, estate->deltaIndex - 1);
        ExecWaitUpdate(estate); 
        ExecCollectUpdate(estate); 
        (void) ExecIncPropUpdate(estate->es_incInfo[estate->es_numIncInfo - 1]); 

        /* step 6. generate pull actions */
        ExecGenPullAction(estate->es_incInfo[estate->es_numIncInfo - 1], PULL_DELTA); 

        /* step 7. init for delta processing */
        ExecInitDelta(planstate); 
    }

    (void) MemoryContextSwitchTo(old);
    elog(NOTICE, "End of Inc Run"); 
}

void 
ExecIncFinish(EState *estate, PlanState *planstate)
{
    elog(NOTICE, "Start of Inc Finish"); 
    MemoryContext old = MemoryContextSwitchTo(estate->es_query_cxt); 

    if (estate->es_isSelect && !gen_mem_info)
    {
        ExecCollectPerDeltaInfo(estate);
        ExecFinalizeStatInfo(estate); 
        pfree(estate->reader_ss); 
        DestroyIncTQPool(estate->tq_pool); 
    }
    else if (estate->es_isSelect && gen_mem_info)
    {
        if (estate->es_memStatFile != NULL)
        {
            FILE *memStatFile = estate->es_memStatFile;
            ExecCollectCostInfo(estate->es_incInfo[estate->es_numIncInfo - 1], true, true); 
            for (int i = 0; i < estate->es_numIncInfo; i++)
            {
                IncInfo *incInfo = estate->es_incInfo[i];
                fprintf(memStatFile, "(%d,%d) ", incInfo->memory_cost[LEFT_STATE], incInfo->memory_cost[RIGHT_STATE]); 
            }
            fprintf(memStatFile, "\n"); 
            fclose(memStatFile); 
        }
    }

    if (estate->es_isSelect)
    {
         ExecSwapoutIQPBase(estate->base); 

         for (int i = estate->base->base_num - 1; i >= 0; i--)
         {
             QueryDesc *qd = estate->base->base_qd[i]; 
             ExecutorFinish(qd);
             ExecutorEnd(qd);
             FreeQueryDesc(qd);
         }
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
        incInfo->mem_computed[i] = false;
        incInfo->stateExist[i] = true; 
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
    IncInfo *innerII = NULL; 

    AggState *aggState;

    switch (ps->type) 
    {
        case T_HashJoinState:
            incInfo->type = INC_HASHJOIN;

            innerPlan = outerPlanState(innerPlanState(ps)); /* Skip Hash node */
            outerPlan = outerPlanState(ps);

            /* Insert Material node to the left subtree */
            outerII = BuildIncInfo(); 
            outerII->type = INC_MATERIAL; 
            outerII->parenttree = incInfo; 
            outerII->ps = NULL;  
            outerIncInfo(incInfo) = outerII;

            (*count)++;
            outerIncInfo(outerII) = ExecInitIncInfoHelper(outerPlan, outerII, count, leafCount); 

            /* Insert Material node to the right subtree */
            innerII = BuildIncInfo();
            innerII->type = INC_MATERIAL;
            innerII->parenttree = incInfo; 
            innerII->ps = NULL;
            innerIncInfo(incInfo) = innerII; 

            (*count)++; 
            outerIncInfo(innerII) = ExecInitIncInfoHelper(innerPlan, innerII, count, leafCount); 

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

static void ExecCopyIncInfo(IncInfo **incInfo_array, IncInfo **incInfo_array_slave, int numIncInfo)
{
    IncInfo *incInfo, *incInfo_slave;

    for (int i = 0; i < numIncInfo; i++)
    {
        incInfo = incInfo_array[i];
        incInfo_slave = incInfo_array_slave[i];

        incInfo->compute_cost = incInfo_slave->compute_cost; 
        incInfo->leftUpdate = incInfo_slave->leftUpdate;
        incInfo->rightUpdate = incInfo_slave->rightUpdate; 

        for (int j = 0; j < MAX_STATE; j++ )
        {
            incInfo->memory_cost[j] = incInfo_slave->memory_cost[j]; 
            incInfo->prepare_cost[j] = incInfo_slave->prepare_cost[j]; 
            incInfo->delta_cost[j] = incInfo_slave->delta_cost[j]; 
            incInfo->keep_cost[j] = incInfo_slave->keep_cost[j]; 
            incInfo->mem_computed[j] = incInfo_slave->mem_computed[j]; 

            incInfo->incState[j] = incInfo_slave->incState[j]; 
        }
    }
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
ExecFinalizeStatInfo(EState *estate)
{
    if (estate->es_memFile != NULL)
    {
        fprintf(estate->es_memFile, "%d\t%d\n", estate->es_totalMemCost, estate->es_incMemory); 
        fclose(estate->es_memFile); 
    }

    if (estate->es_stateFile != NULL)
    {
        fprintf(estate->es_stateFile, "\n"); 
        fclose(estate->es_stateFile); 
    }

    if (estate->es_timeFile != NULL)
    {
        FILE *timeFile = estate->es_timeFile;
        for (int i = 0; i < estate->numDelta + 1; i++)
            fprintf(timeFile, "%.2f\t", estate->execTime[i]); 
        fprintf(timeFile, "%.2f\n", estate->decisionTime); 
        fclose(timeFile); 
    }
}

static void ExecAssignStateExist(IncInfo **incInfo_array, IncInfo **incInfo_array_slave, int numIncInfo)
{
    IncInfo *incInfo, *incInfo_slave; 
    for (int i = 0; i < numIncInfo; i++)
    {
        incInfo = incInfo_array[i];
        incInfo_slave = incInfo_array_slave[i]; 

        if (incInfo->lefttree != NULL)
        {
            if (incInfo->incState[LEFT_STATE] == STATE_DROP && (incInfo->leftAction != PULL_BATCH_DELTA && incInfo->leftAction != PULL_BATCH))
                incInfo_slave->stateExist[LEFT_STATE] = false;
            else
                incInfo_slave->stateExist[LEFT_STATE] = true;
        }

        if (incInfo->righttree != NULL)
        {
            if (incInfo->incState[RIGHT_STATE] == STATE_DROP && (incInfo->rightAction != PULL_BATCH_DELTA && incInfo->rightAction != PULL_BATCH))
                incInfo_slave->stateExist[RIGHT_STATE] = false;
            else
                incInfo_slave->stateExist[RIGHT_STATE] = true;
        }
    }
}

/* Functions for estimate updates and propagate updates */
static void 
ExecEstimateUpdate(EState *estate, bool slave)
{
    TPCH_Update *update = estate->tpch_update; 
    ScanState **reader_ss_array = estate->reader_ss; 
    ScanState *ss; 
    bool hasUpdate;

    for (int i = 0; i < estate->es_numLeaf; i++)
    {
        ss = reader_ss_array[i]; 

        if (use_default_tpch && know_dist_only)
            hasUpdate = CheckTPCHDefaultUpdate(GEN_TQ_KEY(ss->ss_currentRelation)); 
        else
            hasUpdate = CheckTPCHUpdate(update, GEN_TQ_KEY(ss->ss_currentRelation), estate->deltaIndex);
        if (slave)
            ss->ps.ps_IncInfo_slave->leftUpdate = hasUpdate;
        else
            ss->ps.ps_IncInfo->leftUpdate = hasUpdate; 
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
static void ExecGenUpdate(EState *estate, int deltaIndex)
{
    TPCH_Update *update = estate->tpch_update;
    GenTPCHUpdate(update, deltaIndex); 
}

static void 
ExecWaitUpdate(EState *estate)
{
    int deltasize = 0; 
    int threshold = DELTA_THRESHOLD; 

    IncTQPool *tq_pool = estate->tq_pool; 

    for (;;) 
    {
		CHECK_FOR_INTERRUPTS();
        
        deltasize = GetTQUpdate(tq_pool); 

        if (deltasize >= threshold) 
        {
            elog(NOTICE, "delta %d", deltasize); 
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
    bool hasUpdate; 

    for(int i = 0; i < estate->es_numLeaf; i++) 
    {
        r = reader_ss_array[i]->ss_currentRelation;  
        hasUpdate = HasTQUpdate(tq_pool, r);
        reader_ss_array[i]->ps.ps_IncInfo->leftUpdate = hasUpdate; 

        reader_ss_array[i]->tq_reader = GetTQReader(tq_pool, r, reader_ss_array[i]->tq_reader); 

        if (hasUpdate)
            fprintf(estate->es_stateFile, "%s\t", GetTableName(estate->tpch_update, GEN_TQ_KEY(r))); 
    }
    fprintf(estate->es_stateFile, "\n"); 
}


static void ExecReadMemInfo(IncInfo **incInfoArray, int numIncInfo, char *mem_info)
{
    FILE *mem_file; 
    char *mem_dir = CONF_DIR; 
    char fn[50];
    int left_mem, right_mem;
    IncInfo *incInfo;

    /* construct filename*/
    memset(fn, 0, sizeof(fn)); 
    strcpy(fn, mem_dir);
    strcat(fn, mem_info); 

    mem_file = fopen(fn, "r"); 
    if (mem_file == NULL)
        elog(ERROR, "%s not found", fn); 

    for (int i = 0; i < numIncInfo; i++)
    {
        incInfo = incInfoArray[i];

        fscanf(mem_file, "(%d,%d) ", &left_mem, &right_mem); 
        incInfo->memory_cost[LEFT_STATE] = left_mem;
        incInfo->memory_cost[RIGHT_STATE] = right_mem; 
        incInfo->mem_computed[LEFT_STATE] = true;
        incInfo->mem_computed[RIGHT_STATE] = true;
    }

    fclose(mem_file); 
}


/* Functions for deciding intermediate state to be discarded or not */
static void
ExecCollectCostInfo(IncInfo *incInfo, bool compute, bool memory)
{
    PlanState *ps = incInfo->ps; 
    Plan *plan = NULL; 

    if (incInfo->ps != NULL)
        plan = ps->plan; 

    Plan *innerPlan; 
    Plan *outerPlan; 

    double plan_rows;
    int    plan_width; 

    bool estimate; 

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
                if (incInfo->incState[LEFT_STATE] != STATE_DROP)
                    incInfo->keep_cost[LEFT_STATE] = 0; 
                else
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
                if (!incInfo->mem_computed[RIGHT_STATE]) 
                {
                    incInfo->memory_cost[RIGHT_STATE] = ExecHashJoinMemoryCost((HashJoinState *) ps, &estimate, true);
                    incInfo->mem_computed[RIGHT_STATE] = !estimate;
                    incInfo->memory_cost[RIGHT_STATE] = (incInfo->memory_cost[RIGHT_STATE] + 1023) / 1024; 
                }

                if (!incInfo->mem_computed[LEFT_STATE]) 
                {
                    incInfo->memory_cost[LEFT_STATE] = ExecHashJoinMemoryCost((HashJoinState *) ps, &estimate, false);
                    incInfo->mem_computed[LEFT_STATE] = !estimate; 
                    incInfo->memory_cost[LEFT_STATE] = (incInfo->memory_cost[LEFT_STATE] + 1023) / 1024; 
                }
            }
            ExecCollectCostInfo(incInfo->lefttree, compute, memory); 
            ExecCollectCostInfo(incInfo->righttree, compute, memory); 
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

            if (memory && !incInfo->mem_computed[LEFT_STATE])
            {
                incInfo->memory_cost[LEFT_STATE]  = ExecNestLoopMemoryCost((NestLoopState *) ps, &estimate);
                incInfo->mem_computed[LEFT_STATE] = !estimate; 
                incInfo->memory_cost[LEFT_STATE] = (incInfo->memory_cost[LEFT_STATE] + 1023) / 1024; 
            }

            ExecCollectCostInfo(incInfo->lefttree, compute, memory); 
            ExecCollectCostInfo(incInfo->righttree, compute, memory);
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
             
            if (memory && !incInfo->mem_computed[LEFT_STATE])
            {
                incInfo->memory_cost[LEFT_STATE] = ExecAggMemoryCost((AggState *) ps, &estimate); 
                incInfo->mem_computed[LEFT_STATE] = !estimate; 
                incInfo->memory_cost[LEFT_STATE] = (incInfo->memory_cost[LEFT_STATE] + 1023) / 1024; 
            }

            ExecCollectCostInfo(incInfo->lefttree, compute, memory); 
            break;

        case INC_AGGSORT:
            outerPlan = outerPlan(plan); 
            if (compute)
            {
                incInfo->prepare_cost[LEFT_STATE] = (int)(ceil(plan->startup_cost - outerPlan->startup_cost)); 
                incInfo->compute_cost = (int)(ceil(plan->total_cost - plan->startup_cost)); 
            }
            
            if (memory && !incInfo->mem_computed[LEFT_STATE])
            {
                incInfo->memory_cost[LEFT_STATE] = ExecAggMemoryCost((AggState *) ps, &estimate); 
                incInfo->mem_computed[LEFT_STATE] = !estimate; 
                incInfo->memory_cost[LEFT_STATE] = (incInfo->memory_cost[LEFT_STATE] + 1023) / 1024; 
            }
            ExecCollectCostInfo(incInfo->lefttree, compute, memory); 
            break; 

        case INC_SORT:
            outerPlan = outerPlan(plan); 
            if (compute)
            {
                incInfo->prepare_cost[LEFT_STATE] = (int)(ceil(plan->startup_cost - outerPlan->total_cost)); 
                incInfo->compute_cost = (int)(ceil(plan->total_cost - plan->startup_cost)); 
            }

            if (memory && !incInfo->mem_computed[LEFT_STATE]) 
            {
                incInfo->memory_cost[LEFT_STATE] = ExecSortMemoryCost((SortState *) ps, &estimate); 
                incInfo->mem_computed[LEFT_STATE] = !estimate; 
                incInfo->memory_cost[LEFT_STATE] = (incInfo->memory_cost[LEFT_STATE] + 1023) / 1024; 
            }
            ExecCollectCostInfo(incInfo->lefttree, compute, memory); 
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
                if (incInfo->incState[LEFT_STATE] == STATE_DROP)
                    incInfo->keep_cost[LEFT_STATE] =  (int)(ceil(2 * DEFAULT_CPU_OPERATOR_COST * plan_rows));
                else
                    incInfo->keep_cost[LEFT_STATE] = 0; 
            }
            if (memory && !incInfo->mem_computed[LEFT_STATE])
            {
                if (plan == NULL)
                {
                    incInfo->memory_cost[LEFT_STATE] = (plan_rows * (MAXALIGN(plan_width) + MAXALIGN(SizeofHeapTupleHeader)) + 1023) / 1024;
                }
                else
                {
                    incInfo->memory_cost[LEFT_STATE] = ExecMaterialIncMemoryCost((MaterialIncState *) ps); 
                    incInfo->mem_computed[LEFT_STATE] = true; 
                }
                incInfo->memory_cost[LEFT_STATE] = (incInfo->memory_cost[LEFT_STATE] + 1023) / 1024; 
            }
            ExecCollectCostInfo(incInfo->lefttree, compute, memory); 
            break; 

        default:
            elog(ERROR, "CollectCost unrecognized nodetype: %u", ps->type);
            return NULL; 
    }
}

static void 
ExecDecideState(DPMeta *dpmeta, IncInfo **incInfoArray, int numIncInfo, int incMemory, bool isSlave)
{
    if (useBruteForce)
    {
        ExecBruteForce(incInfoArray, numIncInfo, incMemory); 
    }
    else 
    {
        if (decision_method == DM_DP) 
        {
            ExecDPSolution(dpmeta, incInfoArray, numIncInfo, incMemory, isSlave);
            ExecDPAssignState(dpmeta, incInfoArray, numIncInfo - 1, incMemory, PULL_DELTA);
        }
        else
        {
            ExecGreedySolution(incInfoArray, numIncInfo, incMemory, decision_method); 
        }
    }
}

MaterialIncState *ExecBuildMaterialInc(EState *estate); 

static void
ExecUpgradePlan(EState *estate)
{
    MaterialIncState *ms; 
    IncInfo **incInfoArray_slave = estate->es_incInfo_slave; 
    IncInfo *incInfo_slave; 

    IncInfo **incInfoArray = estate->es_incInfo;
    IncInfo *incInfo; 
    bool    isLeft;

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

                    if (incInfo_slave == incInfo_slave->parenttree->lefttree)
                        isLeft = true;
                    else 
                        isLeft = false; 

                    if (!isLeft)
                        IncInsertNodeAfter((PlanState *)ms, incInfo->parenttree->ps->righttree, true); 
                    else
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
                /*if (incInfo->incState[LEFT_STATE] == STATE_DROP && incInfo_slave->incState[LEFT_STATE] == STATE_DROP)
                {
                    IncDeleteNode(incInfo_slave->parenttree->ps, true); 
                    incInfo_slave->ps = NULL;
                    incInfo->ps = NULL;  
                }*/
            }
        }

        if (incInfo_slave->type == INC_HASHJOIN)
            ExecHashJoinIncMarkKeep((HashJoinState *)incInfo_slave->ps, incInfo_slave->incState[LEFT_STATE]);

        if (incInfo_slave->type == INC_NESTLOOP)
            ExecNestLoopIncMarkKeep((NestLoopState *)incInfo_slave->ps, incInfo_slave->incState[LEFT_STATE]); 
    }
}

static void
ExecDegradePlan(EState *estate)
{
    IncInfo **incInfoArray = estate->es_incInfo;
    IncInfo *incInfo; 

    IncInfo **incInfoArray_slave = estate->es_incInfo_slave; 
    IncInfo *incInfo_slave;

    bool isLeft;  

    for (int i = 0; i < estate->es_numIncInfo; i++)
    {
        incInfo = incInfoArray[i]; 
        incInfo_slave = incInfoArray_slave[i]; 

        if (incInfo->type == INC_MATERIAL && incInfo->ps != NULL)
        {
            ExecMaterialIncMarkKeep((MaterialIncState *)incInfo->ps, STATE_DROP); 
            if (incInfo->incState[LEFT_STATE] == STATE_DROP) /* Drop the MaterialInc node */
            {
                if (incInfo == incInfo->parenttree->lefttree)
                        isLeft = true;
                    else 
                        isLeft = false; 

                if (!isLeft)
                    IncDeleteNode(incInfo->parenttree->ps->righttree, true); 
                else
                    IncDeleteNode(incInfo->parenttree->ps, true); 
                incInfo->ps = NULL; 
                incInfo_slave->ps = NULL; 
            }
        }
    }
}


static void 
ExecGenFullPlan(EState *estate)
{
    MaterialIncState *ms; 
    IncInfo **incInfoArray_slave = estate->es_incInfo_slave; 
    IncInfo *incInfo_slave; 

    IncInfo **incInfoArray = estate->es_incInfo;
    IncInfo *incInfo; 
    bool    isLeft;

    for (int i = 0; i < estate->es_numIncInfo; i++)
    {
        incInfo_slave = incInfoArray_slave[i];
        incInfo = incInfoArray[i]; 

        if (incInfo_slave->type == INC_MATERIAL)
        {
            if (incInfo_slave->ps == NULL)
            {
                estate->tempLeftPS = incInfo_slave->lefttree->ps; 
                ms = ExecBuildMaterialInc(estate); 
                estate->tempLeftPS = NULL;

                if (incInfo_slave == incInfo_slave->parenttree->lefttree)
                    isLeft = true;
                else 
                    isLeft = false; 

                if (!isLeft)
                    IncInsertNodeAfter((PlanState *)ms, incInfo->parenttree->ps->righttree, true); 
                else
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

        if (incInfo_slave->type == INC_HASHJOIN)
            ExecHashJoinIncMarkKeep((HashJoinState *)incInfo_slave->ps, STATE_KEEPMEM);

        if (incInfo_slave->type == INC_NESTLOOP)
            ExecNestLoopIncMarkKeep((NestLoopState *)incInfo_slave->ps, STATE_KEEPMEM); 
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
ExecCollectPerDeltaInfo(EState *estate)
{
    IncInfo   **incInfoArray = estate->es_incInfo; 
    int         numIncInfo = estate->es_numIncInfo; 

    PlanState *ps;  
    bool estimate; 

    int activeMem = 0;
    int idleMem = 0;
    int tmpMem;
    estate->es_totalMemCost = 0; 

    IncInfo   *incInfo;
    int        id;

    for (int i = 0; i < numIncInfo; i++)
    {
        incInfo = incInfoArray[i]; 
        ps = incInfo->ps; 
        id = incInfo->id; 
        for (int j = 0; j < MAX_STATE; j++)
            estate->es_totalMemCost += incInfo->memory_cost[j];  

        switch(incInfo->type)
        {
            case INC_HASHJOIN:
                tmpMem = ExecHashJoinMemoryCost((HashJoinState *) ps, &estimate, true);
                tmpMem = (tmpMem + 1023) / 1024;
                //tmpMem = incInfo->memory_cost[RIGHT_STATE];
                if (!estimate)
                {
                    activeMem += tmpMem; 
                    if (incInfo->incState[RIGHT_STATE] == STATE_KEEPMEM)
                        idleMem += tmpMem; 
                }
    
                tmpMem = ExecHashJoinMemoryCost((HashJoinState *) ps, &estimate, false);
                tmpMem = (tmpMem + 1023) / 1024;
                //tmpMem = incInfo->memory_cost[LEFT_STATE];
                if (!estimate)
                {
                    activeMem += tmpMem; 
                    if (incInfo->incState[LEFT_STATE] == STATE_KEEPMEM)
                        idleMem += tmpMem; 
                }
                estate->es_incState[id][LEFT_STATE] = incInfo->incState[LEFT_STATE]; 
                estate->es_incState[id][RIGHT_STATE]  = incInfo->incState[RIGHT_STATE]; 
                break;
    
            case INC_MERGEJOIN:
                elog(ERROR, "not supported MergeJoin yet"); 
                break;
    
            case INC_NESTLOOP:
                tmpMem = ExecNestLoopMemoryCost((NestLoopState *) ps, &estimate);
                tmpMem = (tmpMem + 1023) / 1024;
                //tmpMem = incInfo->memory_cost[LEFT_STATE];
                if (!estimate)
                {
                    activeMem += tmpMem; 
                    if (incInfo->incState[LEFT_STATE] == STATE_KEEPMEM)
                        idleMem += tmpMem; 
                }

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
                tmpMem = ExecAggMemoryCost((AggState *) ps, &estimate);
                tmpMem = (tmpMem + 1023) / 1024;
                //tmpMem = incInfo->memory_cost[LEFT_STATE];
                if (!estimate)
                {
                    activeMem += tmpMem; 
                    if (incInfo->incState[LEFT_STATE] == STATE_KEEPMEM)
                        idleMem += tmpMem; 
                }
                estate->es_incState[id][LEFT_STATE]  = incInfo->incState[LEFT_STATE]; 
                break;

            case INC_MATERIAL:
                if (ps != NULL)
                {
                    tmpMem = ExecMaterialIncMemoryCost((MaterialIncState *) ps); 
                    tmpMem = (tmpMem + 1023) / 1024;
                    //tmpMem = incInfo->memory_cost[LEFT_STATE];
                    activeMem += tmpMem; 
                    if (incInfo->incState[LEFT_STATE] == STATE_KEEPMEM)
                        idleMem += tmpMem; 
                }
                estate->es_incState[id][LEFT_STATE]  = incInfo->incState[LEFT_STATE]; 
                break; 


            case INC_SORT: 
                tmpMem = ExecSortMemoryCost((SortState *) ps, &estimate);  
                tmpMem = (tmpMem + 1023) / 1024; 
                if (!estimate)
                {
                    //tmpMem = incInfo->memory_cost[LEFT_STATE];
                    activeMem += tmpMem; 
                    if (incInfo->incState[LEFT_STATE] == STATE_KEEPMEM)
                        idleMem += tmpMem; 
                }
                estate->es_incState[id][LEFT_STATE]  = incInfo->incState[LEFT_STATE]; 
                break; 

            default:
                elog(ERROR, "CollectStat unrecognized nodetype: %u", incInfo->type);
                return; 
        }
    }

    if (estate->es_memFile != NULL) 
    {
        FILE *memFile = estate->es_memFile;
        fprintf(memFile, "%d\t%d\t", activeMem, idleMem); 
    }
    
    if (estate->es_stateFile != NULL)
    {
        FILE *stateFile = estate->es_stateFile;
        for (int i = 0; i < estate->es_numIncInfo; i++) 
        {
            int id = estate->es_incInfo[i]->id; 
            fprintf(stateFile, "%s(%d,%d) ", incTagName[estate->es_incInfo[i]->type], estate->es_incState[id][LEFT_STATE], \
                    estate->es_incState[id][RIGHT_STATE]); 
        }
        fprintf(stateFile, "\n"); 
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

static void TakeNewSnapshot(EState *estate)
{
    iqp_base * base = estate->base;
    for (int i = base->base_num - 1; i >= 0; i--)
    {
        UnregisterSnapshot(base->base_qd[i]->estate->es_snapshot);
        UnregisterSnapshot(base->base_qd[i]->snapshot);
    }
    UnregisterSnapshot(estate->es_snapshot);
    UnregisterSnapshot(estate->es_qd->snapshot);
	PopActiveSnapshot();
    
    PushActiveSnapshot(GetTransactionSnapshot());
    estate->es_qd->snapshot = RegisterSnapshot(GetActiveSnapshot());
    estate->es_snapshot =  RegisterSnapshot(GetActiveSnapshot());
    for (int i = 0; i < base->base_num; i++)
    {
        base->base_qd[i]->snapshot = RegisterSnapshot(GetActiveSnapshot());
        base->base_qd[i]->estate->es_snapshot =  RegisterSnapshot(GetActiveSnapshot());
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

List *raw_parser(const char *str);
List *pg_analyze_and_rewrite(RawStmt *parsetree, const char *query_string,
					   Oid *paramTypes, int numParams,
					   QueryEnvironment *queryEnv);
List *pg_plan_queries(List *querytrees, int cursorOptions, ParamListInfo boundParams); 

PlanState *ExecInitNode(Plan *node, EState *estate, int eflags); 


static QueryDesc * ExecIQPBuildPS(EState *estate, char *sqlstr)
{
    List	   *parsetree_list = raw_parser(sqlstr);
    RawStmt    *parsetree = lfirst_node(RawStmt, list_head(parsetree_list)); 

	List	   *querytree_list = pg_analyze_and_rewrite(parsetree, sqlstr,
												        NULL, 0, NULL);

	List		*plantree_list = pg_plan_queries(querytree_list, CURSOR_OPT_PARALLEL_OK, NULL);

    QueryDesc *queryDesc = CreateQueryDesc(linitial_node(PlannedStmt, plantree_list),
                                    sqlstr,
									GetActiveSnapshot(),
									InvalidSnapshot,
									None_Receiver,
									NULL,
									NULL,
									0);
    queryDesc->isFirst = false; 

    ExecutorStart(queryDesc, 0);

    return queryDesc; 
}


#define STR_BUFSIZE 500

static iqp_base * ExecBuildIQPBase(EState *estate, char *query_conf)
{
    iqp_base *base = palloc(sizeof(iqp_base)); 
    int base_num; 
    char **table_name; 
    char **sql; 
    Oid  *base_oid; 
    PlanState **base_ps;
    QueryDesc **base_qd;  

    FILE *conf_file; 
    char *conf_dir = CONF_DIR; 
    char fn[50];

    /* construct filename*/
    memset(fn, 0, sizeof(fn)); 
    strcpy(fn, conf_dir);
    strcat(fn, query_conf); 

    conf_file = fopen(fn, "r"); 
    if (conf_file == NULL)
        elog(ERROR, "%s not found", fn); 

    fscanf(conf_file, "%d\n", &base_num);

    table_name = palloc(sizeof(char *) * base_num); 
    sql        = palloc(sizeof(char *) * base_num);
    base_oid   = palloc(sizeof(Oid) * base_num);
    base_ps    = palloc(sizeof(PlanState *) * base_num); 
    base_qd    = palloc(sizeof(QueryDesc *) * base_num); 

    for (int i = 0; i < base_num; i++)
    {
        table_name[i] = palloc(sizeof(char) * STR_BUFSIZE); 
        memset(table_name[i], 0, sizeof(table_name)); 
        fscanf(conf_file, "%s\n", table_name[i]); 

        sql[i] = palloc(sizeof(char) * STR_BUFSIZE); 
        memset(sql[i], 0, sizeof(sql)); 
        fscanf(conf_file, "%[^\n]s", sql[i]);    
        fscanf(conf_file, "\n"); 

        base_oid[i] = IQP_GetOid(iqp_query, table_name[i]); 

        base_qd[i] = ExecIQPBuildPS(estate, sql[i]); 
        base_ps[i] = base_qd[i]->planstate; 
    }

    base->base_num = base_num;
    base->table_name = table_name; 
    base->sql = sql; 
    base->base_oid = base_oid; 
    base->base_ps = base_ps;
    base->base_qd = base_qd; 

    base->old_base_ps = palloc(sizeof(PlanState *) * base_num); 
    base->parent_ps = palloc(sizeof(PlanState *) * base_num); 
    base->isLeft    = palloc(sizeof(bool) * base_num);  

    return base;  
}

static void ExecSwapinIQPBase(EState *estate, PlanState *parent, iqp_base *base)
{
    int i; 
    if (parent->lefttree != NULL && parent->lefttree->type == T_SeqScanState)
    {
        SeqScanState *leftSS = (SeqScanState *) parent->lefttree; 
        for (i = 0; i < base->base_num; i++)
        {
            if (leftSS->ss.ss_currentRelation->rd_id == base->base_oid[i])
            {
                if (base->base_ps[i] == NULL)
                    elog(ERROR, "Multiple input for the same table");

                base->base_ps[i]->plan->delta_cost = base->base_ps[i]->plan->total_cost - parent->lefttree->plan->total_cost; 
                Assert(base->base_ps[i]->plan->delta_cost >= 0); 

                base->parent_ps[i] = parent; 
                base->old_base_ps[i] = parent->lefttree;
                base->isLeft[i] = true; 

                parent->lefttree = base->base_ps[i]; 
                parent->plan->lefttree = base->base_ps[i]->plan; 

                base->base_ps[i] = NULL; 
                break; 
            }
        }

        if (i == base->base_num)
            elog(ERROR, "%d not found", leftSS->ss.ss_currentRelation->rd_id); 
    }

    if (parent->righttree != NULL && parent->righttree->type == T_SeqScanState)
    {
        SeqScanState *rightSS = (SeqScanState *) parent->righttree; 
        for (i = 0; i < base->base_num; i++)
        {
            if (rightSS->ss.ss_currentRelation->rd_id == base->base_oid[i])
            {
                if (base->base_ps[i] == NULL)
                    elog(ERROR, "Multiple input for the same table");

                base->base_ps[i]->plan->delta_cost = base->base_ps[i]->plan->total_cost - parent->righttree->plan->total_cost; 
                Assert(base->base_ps[i]->plan->delta_cost >= 0); 

                base->parent_ps[i] = parent; 
                base->old_base_ps[i] = parent->righttree;
                base->isLeft[i] = false; 

                parent->righttree = base->base_ps[i]; 
                parent->plan->righttree = base->base_ps[i]->plan; 

                base->base_ps[i] = NULL; 
                break; 
            }
        }

        if (i == base->base_num)
            elog(ERROR, "%d not found", rightSS->ss.ss_currentRelation->rd_id); 
    }

    if (parent->lefttree != NULL)
        ExecSwapinIQPBase(estate, parent->lefttree, base);

    if (parent->righttree != NULL)
        ExecSwapinIQPBase(estate, parent->righttree, base);
}


static void ExecSwapoutIQPBase(iqp_base *base)
{
    for (int i = 0; i < base->base_num; i++)
    {
        if (base->isLeft[i])
        {
            base->base_ps[i] = base->parent_ps[i]->lefttree; 
            base->parent_ps[i]->lefttree = base->old_base_ps[i];
            base->parent_ps[i]->plan->lefttree = base->old_base_ps[i]->plan; 
        }
        else
        {
            base->base_ps[i] = base->parent_ps[i]->righttree; 
            base->parent_ps[i]->righttree = base->old_base_ps[i];
            base->parent_ps[i]->plan->righttree = base->old_base_ps[i]->plan; 
        }
    }
}

static Cost PropagateDeltaCost(Plan *parent)
{
    Cost leftCost = 0, rightCost = 0; 

    if (parent->lefttree == NULL && parent->righttree == NULL)
        return parent->delta_cost; 

    if (parent->lefttree != NULL)
        leftCost = PropagateDeltaCost(parent->lefttree);

    if (parent->righttree != NULL)
        rightCost = PropagateDeltaCost(parent->righttree);

    if (parent->lefttree != NULL && parent->righttree != NULL)
    {
        parent->startup_cost += rightCost; 
        parent->total_cost += (leftCost + rightCost);
    }
    else
    {
        parent->startup_cost += leftCost; 
        parent->total_cost += leftCost;
    }

    return leftCost + rightCost; 
}

static void PrintOutAttNum(PlanState *ps)
{

    if (ps->type == T_HashJoinState && ps->lefttree->lefttree == NULL )
    {
        HashJoinState *hj = (HashJoinState *)ps;
        ExprState * key = (ExprState *)lfirst(list_head(hj->hj_InnerHashKeys));
        int inner = key->steps[1].d.var.attnum; 

        key = (ExprState *)lfirst(list_head(hj->hj_OuterHashKeys));
        int outer = key->steps[1].d.var.attnum; 

        elog(NOTICE, "outer: %d", outer); 
    }

    if (ps->type == T_HashJoinState && ps->righttree->lefttree->lefttree == NULL )
    {
        HashJoinState *hj = (HashJoinState *)ps;
        ExprState * key = (ExprState *)lfirst(list_head(hj->hj_InnerHashKeys));
        int inner = key->steps[1].d.var.attnum; 

        key = (ExprState *)lfirst(list_head(hj->hj_OuterHashKeys));
        int outer = key->steps[1].d.var.attnum; 

        elog(NOTICE, "inner: %d", inner); 
    }

    if (ps->lefttree != NULL)
        PrintOutAttNum(ps->lefttree);

    if (ps->righttree != NULL)
        PrintOutAttNum(ps->righttree); 
}

