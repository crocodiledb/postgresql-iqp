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

#include "utils/relcache.h"
#include "utils/rel.h"

#include "executor/incinfo.h"
#include "executor/incTupleQueue.h"
#include "executor/incTQPool.h"

#include "miscadmin.h"

#define DELTA_COST 0

#define LINEITEM_OID 26372
#define DELTA_THRESHOLD 2
#define GEN_DELTA_CMD "/home/totemtang/IQP/postgresql/pg_scripts/tpch_delta/gen_delta.sh"

bool enable_incremental;
int  memory_budget;     /* kB units*/
DecisionMethod decision_method; 

static void ExecInitIncInfo(EState *estate, PlanState *ps); 
static void ExecAssignIncInfo (IncInfo **incInfo_array, int numIncInfo, IncInfo *root);
static IncInfo * ExecInitIncInfoHelper(PlanState *ps, IncInfo *parent, int *count, int *leafCount); 

static void ExecMakeDecision (EState *estate, IncInfo *incInfo); 
static void ExecEstimateDelta(EState *estate); 
static void ExecCollectCostInfo(IncInfo *incInfo); 
static void RunDPSolution(EState *estate); 
static void SimpleDropDP(EState *estate, int i, int j); 
static void SortDPHasUpdate (EState *estate, int i, int j); 
static void SortDPNoUpdate (EState *estate, int i, int j); 
static void AggDPHasUpdate (EState *estate, int i, int j); 
static void AggDPNoUpdate (EState *estate, int i, int j); 
static void HashJoinDPHasUpdate (EState *estate, int i, int j); 
static void HashJoinDPNoUpdate (EState *estate, int i, int j); 
static void NestLoopDPHasUpdate (EState *estate, int i, int j); 
static void NestLoopDPNoUpdate (EState *estate, int i, int j); 
static void ExecDPAssignState (EState *estate, int i, int j, PullAction parentAction); 
static void PrintStateDecision(EState *estate); 

static void ExecResetDelta(EState *estate, PlanState *ps); 
static void ExecWaitAndMarkDelta(EState *estate, PlanState *ps); 
static void ExecGenPullAction(IncInfo *incInfo); 
static void ExecGenPullActionHelper(IncInfo *incInfo, PullAction parentAction); 
static bool ExecIncPropUpdate(IncInfo *incInfo); 

static void ExecCleanIncInfo(EState *estate); 

double GetTimeDiff(struct timeval x , struct timeval y); 


/*
 * prototypes for Greedy Algorithms 
 */
static void ExecGreedyHelper(EState *estate);
static void AssignState(EState *estate, IncInfo *incInfo, IncState state); 

static int MemComparator(const void * a, const void *b)
{
    IncInfo *a_incInfo = *(IncInfo **)a;
    IncInfo *b_incInfo = *(IncInfo **)b; 

    return a_incInfo->memory_cost - b_incInfo->memory_cost; 
}

/* 
 * Interfaces for incremental query processing 
 */
void 
ExecIncStart(EState *estate, PlanState *ps)
{
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
            if (ps->lefttree == NULL && ps->righttree == NULL)
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
}

static 
void ExecGenDelta()
{
    system(GEN_DELTA_CMD); 
}

void 
ExecIncRun(EState *estate, PlanState *planstate)
{
    MemoryContext old = MemoryContextSwitchTo(estate->es_query_cxt); 

    if (estate->es_isSelect) 
    {
        struct timeval start, end;  

        gettimeofday(&start , NULL);

        /* Run the DP algorithm to decide dropping or keeping states */
        ExecMakeDecision(estate, planstate->ps_IncInfo); 
 
        /* Let's perform the drop/keep actions */
        ExecResetDelta(estate, planstate); 

        gettimeofday(&end , NULL);
        
        estate->decisionTime = GetTimeDiff(start, end);  
 
        /* An temp solution for generating delta*/
        ExecGenDelta();

        /* Wait for delta and mark delta */
        ExecWaitAndMarkDelta(estate, planstate); 

        /* According to the delta information, we generate pull actions */ 
        ExecGenPullAction(planstate->ps_IncInfo);

        /* Let's initialize meta data for delta processing */
        ExecInitDelta(planstate);  
    }

    (void) MemoryContextSwitchTo(old); 
}

void 
ExecIncFinish(EState *estate, PlanState *planstate)
{
    MemoryContext old = MemoryContextSwitchTo(estate->es_query_cxt); 

    if (estate->es_isSelect)
    {
        ExecCleanIncInfo(estate); 
        pfree(estate->reader_ss); 
        DestroyIncTQPool(estate->tq_pool); 
    }

    (void) MemoryContextSwitchTo(old); 
}

/*
 * Help functions for running our dynamic programming algorithm 
 */

/*
 * Step 1. Build IncInfo Trees
 *         This is run in the per-query memory context 
 *         
 *         We only support the following nodes:
 *         1) HashJoin
 *         2) SeqScan
 *         3) IndexScan
 *         4) MergeJoin
 *         5) Aggregation
 *         6) Sort
 *         7) NestLoop 
 *
 */

static void 
ExecInitIncInfo(EState *estate, PlanState *ps) 
{
    int count = 0; 
    int leafCount = 0; 
    int incMemory = 0; 

    ExecInitIncInfoHelper(ps, NULL, &count, &leafCount);
    estate->es_numLeaf = leafCount; 

    estate->es_incInfo = (IncInfo **) palloc(sizeof(IncInfo *) * count); 
    ExecAssignIncInfo(estate->es_incInfo, count, ps->ps_IncInfo); 
    estate->es_numIncInfo = count; 
    estate->es_totalMemCost  = 0; 

    /* Round memory budget to MB to reduce DP running time */
    if (decision_method == DM_DP)
    {
        estate->es_incMemory = (memory_budget + 1023)/1024; 
        incMemory = estate->es_incMemory; 
    
        /* Allocate necessary structures */
        estate->es_deltaCost = (int **) palloc(sizeof(int *) * count);
        estate->es_deltaIncState = (IncState **) palloc(sizeof(IncState *) * count);
        estate->es_deltaMemLeft = (int **) palloc(sizeof(int *) * count);
        estate->es_deltaLeftPull = (PullAction **) palloc(sizeof(PullAction *) * count); 
        estate->es_deltaRightPull = (PullAction **) palloc(sizeof(PullAction *) * count); 
    
        estate->es_bdCost = (int **) palloc(sizeof(int *) * count);
        estate->es_bdIncState = (IncState **) palloc(sizeof(IncState *) * count);
        estate->es_bdMemLeft = (int **) palloc(sizeof(int *) * count);
        estate->es_bdLeftPull = (PullAction **) palloc(sizeof(PullAction *) * count); 
        estate->es_bdRightPull = (PullAction **) palloc(sizeof(PullAction *) * count); 
    
        for (int i = 0; i < count; i++) 
        {
            estate->es_deltaCost[i] = (int *) palloc(sizeof(int) * (incMemory + 1)); 
            estate->es_deltaIncState[i] = (IncState *) palloc(sizeof(IncState) * (incMemory + 1)); 
            estate->es_deltaMemLeft[i] = (int *) palloc(sizeof(int) * incMemory); 
            estate->es_deltaLeftPull[i] = (PullAction *) palloc(sizeof(PullAction) * (incMemory + 1)); 
            estate->es_deltaRightPull[i] = (PullAction *) palloc(sizeof(PullAction) * (incMemory + 1)); 
    
            estate->es_bdCost[i] = (int *) palloc(sizeof(int) * (incMemory + 1));
            estate->es_bdIncState[i] = (IncState *) palloc(sizeof(IncState) * (incMemory + 1)); 
            estate->es_bdMemLeft[i] = (int *) palloc(sizeof(int) * (incMemory + 1));
            estate->es_bdLeftPull[i] = (PullAction *) palloc(sizeof(PullAction) * (incMemory + 1)); 
            estate->es_bdRightPull[i] = (PullAction *) palloc(sizeof(PullAction) * (incMemory + 1)); 
        }
    }
    else
    {
        estate->es_incMemory = memory_budget; 
    }

    /* For Stat*/
    estate->es_statFile = fopen("statfile.txt", "a"); 
    estate->decisionTime = 0;
    estate->repairTime = 0;
    estate->es_incState = (IncState *) palloc(sizeof(IncState) * count);
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

    IncInfo *incInfo = (IncInfo *) palloc(sizeof(IncInfo));
    incInfo->ps = ps;
    incInfo->parenttree = parent; 

    incInfo->trigger_computation = -1;  

    incInfo->parentAction = PULL_DELTA;
    incInfo->leftAction = PULL_DELTA;
    incInfo->rightAction = PULL_DELTA; 

    incInfo->leftIncState = STATE_DROP;
    incInfo->rightIncState = STATE_DROP;
    
    incInfo->leftUpdate = false;
    incInfo->rightUpdate = false;

    switch (ps->type) 
    {
        case T_HashJoinState:
            innerPlan = outerPlanState(innerPlanState(ps)); /* Skip Hash node */
            outerPlan = outerPlanState(ps); 
            innerIncInfo(incInfo) = ExecInitIncInfoHelper(innerPlan, incInfo, count, leafCount); 
            outerIncInfo(incInfo) = ExecInitIncInfoHelper(outerPlan, incInfo, count, leafCount); 
            break;

        case T_MergeJoinState:
            elog(ERROR, "Not Support MergeJoin yet"); 
            break; 

        case T_NestLoopState:
            innerPlan = innerPlanState(ps);
            outerPlan = outerPlanState(ps); 
            innerIncInfo(incInfo) = ExecInitIncInfoHelper(innerPlan, incInfo, count, leafCount); 
            outerIncInfo(incInfo) = ExecInitIncInfoHelper(outerPlan, incInfo, count, leafCount);
            break;  

        case T_SeqScanState:
        case T_IndexScanState:
            innerIncInfo(incInfo) = NULL;
            outerIncInfo(incInfo) = NULL;
            (*leafCount)++; 
            break; 

        case T_AggState:
        case T_SortState:
            outerPlan = outerPlanState(ps); 
            outerIncInfo(incInfo) = ExecInitIncInfoHelper(outerPlan, incInfo, count, leafCount);
            innerIncInfo(incInfo) = NULL;
            break; 

        default:
            elog(NOTICE, "unrecognized nodetype: %u", ps->type);
            return NULL; 
    }

    ps->ps_IncInfo = incInfo;
    (*count)++; 

    return incInfo; 
}

/*
 * Step 2. 
 *      2a. Estimate the potential updates and propogate them to the IncInfo tree
 *      2b. Collect memory_cost and computate_cost
 *      2c. Running DP algorithm to determine which states to keep/drop 
 *      2d. Assign states to IncInfo 
 */

static void 
ExecMakeDecision (EState *estate, IncInfo *incInfo)
{
    ExecEstimateDelta(estate); 
    ExecIncPropUpdate(incInfo); /* Step 2a */

    ExecCollectCostInfo(incInfo); /* Step 2b */ 

    if (decision_method == DM_DP) 
    {
        RunDPSolution(estate); /* Step 2c */
        ExecDPAssignState(estate, estate->es_numIncInfo - 1, estate->es_incMemory, PULL_BATCH_DELTA); /* Step 2d */
    }
    else
    {
        if (decision_method == DM_MEMSMALLFIRST || decision_method == DM_MEMBIGFIRST)
            qsort((void *)estate->es_incInfo, (size_t)estate->es_numIncInfo, sizeof(IncInfo *), MemComparator);
        ExecGreedyHelper(estate); 
    }
}

static void
PrintStateDecision(EState *estate)
{
    IncInfo **incInfoArray = estate->es_incInfo; 
    int numIncInfo = estate->es_numIncInfo; 
    elog(NOTICE, "State Decision"); 
    for (int i = 0; i < numIncInfo; i++)
    {
        elog(NOTICE, "%d %d", incInfoArray[i]->leftIncState, incInfoArray[i]->rightIncState); 
    }
}

static void 
ExecEstimateDelta(EState *estate) 
{
    ScanState **reader_ss_array = estate->reader_ss; 
    ScanState *ss; 

    for (int i = 0; i < estate->es_numLeaf; i++)
    {
        ss = reader_ss_array[i]; 
        if (ss->ss_currentRelation->rd_node.relNode == LINEITEM_OID)
            ss->ps.ps_IncInfo->leftUpdate = true; 
    }
}

static void
ExecCollectCostInfo(IncInfo *incInfo)
{
    PlanState *ps = incInfo->ps; 
    Plan *plan = ps->plan; 

    EState *estate = ps->state; 

    Plan *innerPlan; 
    Plan *outerPlan; 

    switch(incInfo->ps->type)
    {
        case T_HashJoinState:
            innerPlan = innerPlan(plan);
            outerPlan = outerPlan(plan); 
            incInfo->prepare_cost = (int)(innerPlan->total_cost - outerPlan(innerPlan)->total_cost);
            incInfo->compute_cost = (int)(plan->total_cost - innerPlan->total_cost - outerPlan->total_cost);
            incInfo->memory_cost = ExecHashJoinMemoryCost((HashJoinState *) ps);
            ExecCollectCostInfo(incInfo->lefttree); 
            ExecCollectCostInfo(incInfo->righttree); 
            break;

        case T_MergeJoinState:
            elog(ERROR, "not supported MergeJoin yet"); 
            break;

        case T_NestLoopState:
            innerPlan = innerPlan(plan);
            outerPlan = outerPlan(plan);
            incInfo->prepare_cost = (int)(plan->startup_cost - innerPlan->total_cost); 
            incInfo->compute_cost = (int)(plan->total_cost- plan->startup_cost); 
            incInfo->memory_cost = 0; 
            ExecCollectCostInfo(incInfo->lefttree); 
            ExecCollectCostInfo(incInfo->righttree);
            break;  

        case T_SeqScanState:
        case T_IndexScanState:
            incInfo->prepare_cost = plan->startup_cost;
            incInfo->compute_cost = plan->total_cost - plan->startup_cost; 
            incInfo->memory_cost = 0; 
            return;  
            break; 

        case T_AggState:
            outerPlan = outerPlan(plan); 
            AggState *aggState = (AggState *)incInfo->ps; 
            if (aggState->aggstrategy == AGG_SORTED)
                incInfo->prepare_cost = (int)(plan->startup_cost - outerPlan->startup_cost); 
            else /* AGG_HASHED */
                incInfo->prepare_cost = (int)(plan->startup_cost - outerPlan->total_cost); 
            incInfo->compute_cost = (int)(plan->total_cost - plan->startup_cost); 
            incInfo->memory_cost = ExecAggMemoryCost(aggState); 
            ExecCollectCostInfo(incInfo->lefttree); 
            break; 

        case T_SortState:
            outerPlan = outerPlan(plan); 
            incInfo->prepare_cost = (int)(plan->startup_cost - outerPlan->total_cost); 
            incInfo->compute_cost = (int)(plan->total_cost - plan->startup_cost); 
            incInfo->memory_cost = ExecSortMemoryCost((SortState *) ps); 
            ExecCollectCostInfo(incInfo->lefttree); 
            break; 

        default:
            elog(NOTICE, "unrecognized nodetype: %u", ps->type);
            return NULL; 
    }
}

static void 
RunDPSolution(EState *estate)
{
    IncInfo   **incInfoArray = estate->es_incInfo; 
    int         numIncInfo = estate->es_numIncInfo; 

    IncInfo   *incInfo;
    PlanState *ps;  
    int i, j;
    int incMemory = estate->es_incMemory;  

    /* Init info for leaf nodes */
    for (i = 0; i < numIncInfo; i++)
    {
        incInfo = incInfoArray[i]; 
        /* If not leaf nodes, break */
        if (incInfoArray[i]->lefttree != NULL || incInfoArray[i]->righttree != NULL) 
        {
            incInfo->memory_cost = (incInfo->memory_cost + 1023)/1024;
            estate->es_totalMemCost += incInfo->memory_cost; 
            AggState *aggState; 

            switch (incInfo->ps->type)
            {
                case T_SortState:
                    if (incInfo->leftUpdate)
                        incInfo->execDPNode = SortDPHasUpdate; 
                    else
                        incInfo->execDPNode = SortDPNoUpdate; 
                    break; 

                case T_AggState: 
                    aggState = (AggState *)incInfo->ps; 
                    if (aggState->aggstrategy == AGG_SORTED)
                        incInfo->execDPNode = SimpleDropDP; 
                    else if (incInfo->leftUpdate) /* AGG_HASHED */
                        incInfo->execDPNode = AggDPHasUpdate; 
                    else
                        incInfo->execDPNode = AggDPNoUpdate; 
                    break; 

                case T_HashJoinState:
                    if (incInfo->leftUpdate && !incInfo->rightUpdate)
                        incInfo->execDPNode = HashJoinDPHasUpdate; 
                    else if (!incInfo->leftUpdate && !incInfo->rightUpdate)
                        incInfo->execDPNode = HashJoinDPNoUpdate; 
                    else 
                        elog(ERROR, "We only support updates from the left most table"); 
                    break;
                case T_NestLoopState:
                    if (incInfo->leftUpdate && incInfo->rightUpdate)
                        elog(ERROR, "We only support updates from one side "); 
                    else if (!incInfo->leftUpdate && !incInfo->rightUpdate)
                        incInfo->execDPNode = NestLoopDPNoUpdate;
                    else
                        incInfo->execDPNode = NestLoopDPHasUpdate; 
                    break; 

                case T_MergeJoinState:
                    elog(ERROR, "not supported MergeJoin yet");
                    break;  
                default:
                    elog(ERROR, "unrecognized nodetype: %u", incInfo->ps->type);
                    return; 
            }
        }
        else 
        {
            incInfo->execDPNode = NULL; 
            for (j = 0; j <= incMemory; j++)
                SimpleDropDP(estate, i, j); 
        }
    }

    for (i = 0; i < numIncInfo; i++)
    {
        incInfo = incInfoArray[i]; 
        if (incInfo->execDPNode == NULL) /* leaf nodes */
            continue; 

        for (j = 0; j <= incMemory; j++)
            incInfo->execDPNode(estate, i, j); 
    }

}


/*
 * This is applied to operator that has not state with a single child 
 * */
static void 
SimpleDropDP(EState *estate, int i, int j)
{
    IncInfo   *incInfo = estate->es_incInfo[i]; 

    /* No Update */
    estate->es_deltaCost[i][j] = 0; 
    estate->es_deltaIncState[i][j] = STATE_DROP; 
    estate->es_deltaMemLeft[i][j] = j; 
    estate->es_deltaLeftPull[i][j] = PULL_DELTA; 
    estate->es_deltaRightPull[i][j] = PULL_DELTA; 
 
    estate->es_bdCost[i][j] = incInfo->prepare_cost + incInfo->compute_cost;
    estate->es_bdIncState[i][j] = STATE_DROP;
    estate->es_bdMemLeft[i][j] = j;
    estate->es_bdLeftPull[i][j] = PULL_BATCH_DELTA;
    estate->es_bdRightPull[i][j] = PULL_BATCH_DELTA; 


    /* Consider Update */
    if (incInfo->leftUpdate) 
    {
        estate->es_deltaCost[i][j] += DELTA_COST; 
        estate->es_bdCost[i][j] += DELTA_COST; 
    }

    /* Consider the cost of subtree */
    if (incInfo->lefttree != NULL) /* AGG_SORTED */
    {
        int left = incInfo->lefttree->id; 
        estate->es_bdCost[i][j] += estate->es_bdCost[left][j]; 

        if (incInfo->leftUpdate) /* TODO: we assume recomputation here for Aggregation here */
        {
            estate->es_deltaCost[i][j] += (incInfo->prepare_cost + incInfo->compute_cost + estate->es_bdCost[left][j]); 
            estate->es_deltaLeftPull[i][j] = PULL_BATCH_DELTA; 
        }

    }
}

/*
 * The following functions have the same parameters
 * i -- index of the current node
 * j -- memory budget 
 * */
static void 
SortDPHasUpdate (EState *estate, int i, int j)
{
    IncInfo   *incInfo = estate->es_incInfo[i]; 

    int       **deltaCost = estate->es_deltaCost; 
    IncState  **deltaIncState = estate->es_deltaIncState; 
    int       **deltaMemLeft = estate->es_deltaMemLeft;
    PullAction **deltaLeftPull = estate->es_deltaLeftPull; 

    int       **bdCost = estate->es_bdCost;  
    IncState  **bdIncState = estate->es_bdIncState; 
    int       **bdMemLeft = estate->es_bdMemLeft;
    PullAction **bdLeftPull = estate->es_bdLeftPull; 


    int left = incInfo->lefttree->id;
    int dropCost, keepCost; 

    /* compute deltaCost*/
    deltaCost[i][j] = DELTA_COST + deltaCost[left][j]; /* Always drop */
    deltaIncState[i][j] = STATE_DROP; 
    deltaMemLeft[i][j] = j;
    bdLeftPull[i][j] = PULL_DELTA; 
    
    /* compute bdCost*/
    dropCost = bdCost[left][j] + incInfo->prepare_cost + incInfo->compute_cost + DELTA_COST;
    if (j == 0 || j < incInfo->memory_cost) /* We can only drop this state */
    {
        bdCost[i][j] = dropCost; 
        bdIncState[i][j] = STATE_DROP; 
        bdMemLeft[i][j] = j; 
        bdLeftPull[i][j] = PULL_BATCH_DELTA; 
    }
    else                          /* Now we check whether drop or keep */ 
    {
        keepCost = deltaCost[left][j - incInfo->memory_cost] + DELTA_COST; 
        if (keepCost <= dropCost)
        {
            bdCost[i][j] = keepCost; 
            bdIncState[i][j] = STATE_KEEPMEM; 
            bdMemLeft[i][j] = j - incInfo->memory_cost; 
            bdLeftPull[i][j] = PULL_DELTA; 
        }
        else
        {
            bdCost[i][j] = dropCost; 
            bdIncState[i][j] = STATE_DROP;
            bdMemLeft[i][j] = j; 
            bdLeftPull[i][j] = PULL_BATCH_DELTA;
        }
    }
}

static void 
SortDPNoUpdate (EState *estate, int i, int j)
{
    IncInfo   *incInfo = estate->es_incInfo[i]; 

    int       **deltaCost = estate->es_deltaCost; 
    IncState  **deltaIncState = estate->es_deltaIncState; 
    int       **deltaMemLeft = estate->es_deltaMemLeft;
    PullAction **deltaLeftPull = estate->es_deltaLeftPull; 

    int       **bdCost = estate->es_bdCost;  
    IncState  **bdIncState = estate->es_bdIncState; 
    int       **bdMemLeft = estate->es_bdMemLeft;
    PullAction **bdLeftPull = estate->es_bdLeftPull; 


    int left = incInfo->lefttree->id;
    int dropCost; 

    /* compute deltaCost */
    deltaCost[i][j] = 0; 
    deltaIncState[i][j] = STATE_DROP; 
    deltaMemLeft[i][j] = 0; 
    deltaLeftPull[i][j] = PULL_DELTA; 
    
    /* compute bdCost*/
    dropCost = bdCost[left][j] + incInfo->prepare_cost + incInfo->compute_cost; 
    if (j == 0 || j < incInfo->memory_cost) 
    {
        bdCost[i][j] = dropCost; 
        bdIncState[i][j] = STATE_DROP; 
        bdMemLeft[i][j] = j; 
        bdLeftPull[i][j] = PULL_BATCH_DELTA; 
    }
    else 
    {
        bdCost[i][j] = 0; 
        bdIncState[i][j] = STATE_KEEPMEM; 
        bdMemLeft[i][j] = j - incInfo->memory_cost; 
        bdLeftPull[i][j] = PULL_DELTA; 
    }
}

static void
AggDPHasUpdate (EState *estate, int i, int j)
{ 
    IncInfo   *incInfo = estate->es_incInfo[i]; 

    int       **deltaCost = estate->es_deltaCost; 
    IncState  **deltaIncState = estate->es_deltaIncState; 
    int       **deltaMemLeft = estate->es_deltaMemLeft;
    PullAction **deltaLeftPull = estate->es_deltaLeftPull; 

    int       **bdCost = estate->es_bdCost;  
    IncState  **bdIncState = estate->es_bdIncState; 
    int       **bdMemLeft = estate->es_bdMemLeft;
    PullAction **bdLeftPull = estate->es_bdLeftPull; 


    int left = incInfo->lefttree->id;
    int dropCost, keepCost; 

    /* compute deltaCost*/
    dropCost = bdCost[left][j] + incInfo->prepare_cost + DELTA_COST; 
    if (j == 0 || j < incInfo->memory_cost)
    {
        deltaCost[i][j] = dropCost;
        deltaIncState[i][j] = STATE_DROP; 

        bdCost[i][j] = dropCost + incInfo->compute_cost; 
        bdIncState[i][j] = STATE_DROP;
    }
    else 
    {
        /* Here we do a little optimization:
         * The comparative keepCost and dropCost for delta and batch_delta are the same,
         * so we use the decision of delta for batch_delta
         */
        keepCost = deltaCost[left][j - incInfo->memory_cost] + DELTA_COST; 
        if (keepCost <= dropCost)
        {
            deltaCost[i][j] = keepCost;
            deltaIncState[i][j] = STATE_KEEPMEM;

            bdCost[i][j] = keepCost + incInfo->compute_cost; 
            bdIncState[i][j] = STATE_KEEPMEM; 
        }
        else
        {
            deltaCost[i][j] = dropCost;
            deltaIncState[i][j] = STATE_DROP; 

            bdCost[i][j] = dropCost + incInfo->compute_cost; 
            bdIncState[i][j] = STATE_DROP; 
        }
    }

    if (deltaIncState[i][j] == STATE_DROP)
    {
        deltaMemLeft[i][j] = j; 
        bdMemLeft[i][j] = j;
        deltaLeftPull[i][j] = PULL_BATCH_DELTA; 
        bdLeftPull[i][j] = PULL_BATCH_DELTA; 
    }
    else
    {
        deltaMemLeft[i][j] = j - incInfo->memory_cost; 
        bdMemLeft[i][j] = j - incInfo->memory_cost;
        deltaLeftPull[i][j] = PULL_DELTA; 
        bdLeftPull[i][j] = PULL_DELTA; 
    }
}

static void
AggDPNoUpdate (EState *estate, int i, int j)
{
    IncInfo   *incInfo = estate->es_incInfo[i]; 

    int       **deltaCost = estate->es_deltaCost; 
    IncState  **deltaIncState = estate->es_deltaIncState; 
    int       **deltaMemLeft = estate->es_deltaMemLeft;
    PullAction **deltaLeftPull = estate->es_deltaLeftPull; 

    int       **bdCost = estate->es_bdCost;  
    IncState  **bdIncState = estate->es_bdIncState; 
    int       **bdMemLeft = estate->es_bdMemLeft;
    PullAction **bdLeftPull = estate->es_bdLeftPull; 

    int left = incInfo->lefttree->id;
    int dropCost, keepCost; 

    deltaCost[i][j] = 0; 
    deltaIncState[i][j] = STATE_DROP; 
    deltaMemLeft[i][j] = 0; 
    deltaLeftPull[i][j] = PULL_DELTA; 

    /* compute bdCost*/
    dropCost = bdCost[left][j] + incInfo->prepare_cost + incInfo->compute_cost; 
    if (j == 0 || j < incInfo->memory_cost)
    {
        bdCost[i][j] = dropCost; 
        bdIncState[i][j] = STATE_DROP;
        bdMemLeft[i][j] = j;
        bdLeftPull[i][j] = PULL_BATCH_DELTA;
    }
    else 
    {
        keepCost = incInfo->compute_cost; 
        bdCost[i][j] = keepCost; 
        bdIncState[i][j] = STATE_KEEPMEM; 
        bdMemLeft[i][j] = j - incInfo->memory_cost; 
        bdLeftPull[i][j] = PULL_DELTA; 
    }
}


static void
NestLoopDPHasUpdate (EState *estate, int i, int j)
{    
    IncInfo   *incInfo = estate->es_incInfo[i]; 

    int       **deltaCost = estate->es_deltaCost; 
    IncState  **deltaIncState = estate->es_deltaIncState; 
    int       **deltaMemLeft = estate->es_deltaMemLeft;
    PullAction **deltaLeftPull = estate->es_deltaLeftPull; 
    PullAction **deltaRightPull = estate->es_deltaRightPull; 

    int       **bdCost = estate->es_bdCost;  
    IncState  **bdIncState = estate->es_bdIncState; 
    int       **bdMemLeft = estate->es_bdMemLeft;
    PullAction **bdLeftPull = estate->es_bdLeftPull; 
    PullAction **bdRightPull = estate->es_bdRightPull; 


    int left = incInfo->lefttree->id;
    int right = incInfo->righttree->id; 

    int deltaDropCost = INT_MAX, bdDropCost = INT_MAX;  

    /* Compute dropCost */
    int tempCost;
    int leftMem; 
    int rightMem;  
    int deltaDropMemLeft; 
    int bdDropMemLeft; 
    int k; 
    for (k = 0; k <= j; k++)
    {
        leftMem = k;
        rightMem = j - k; 

        if (incInfo->leftUpdate && !incInfo->rightUpdate)
            tempCost = deltaCost[left][leftMem] + bdCost[right][rightMem] + incInfo->prepare_cost + DELTA_COST; 
        else /* (!incInfo->leftUpdate && incInfo->rightUpdate) */
            tempCost = bdCost[left][leftMem] + deltaCost[right][rightMem] + incInfo->prepare_cost + DELTA_COST; 

        if (tempCost < deltaDropCost)
        {
            deltaDropCost = tempCost; 
            deltaDropMemLeft = k;    
        }

        tempCost = bdCost[left][leftMem] + bdCost[right][rightMem] + incInfo->prepare_cost + incInfo->compute_cost + DELTA_COST; 
        if (tempCost < bdDropCost)
        {
            bdDropCost = tempCost; 
            bdDropMemLeft = k; 
        }
    }

    /* We can only drop the state (since no state) for NestLoop */
    deltaIncState[i][j] = STATE_DROP;
    deltaCost[i][j] = deltaDropCost; 
    deltaMemLeft[i][j] = deltaDropMemLeft; 

    if(incInfo->leftUpdate && !incInfo->rightUpdate) 
    { 
        deltaLeftPull[i][j] = PULL_DELTA;
        deltaRightPull[i][j] = PULL_BATCH_DELTA;
    }
    else /* (!incInfo->leftUpdate && incInfo->rightUpdate) */
    {
        deltaLeftPull[i][j] = PULL_BATCH_DELTA; 
        deltaRightPull[i][j] = PULL_DELTA;
    }
       
    bdIncState[i][j] = STATE_DROP;
    bdCost[i][j] = bdDropCost; 
    bdMemLeft[i][j] = bdDropMemLeft;  
    bdLeftPull[i][j] = PULL_BATCH_DELTA; 
    bdRightPull[i][j] = PULL_BATCH_DELTA; 

}

static void
NestLoopDPNoUpdate (EState *estate, int i, int j)
{
    IncInfo   *incInfo = estate->es_incInfo[i]; 

    int       **deltaCost = estate->es_deltaCost; 
    IncState  **deltaIncState = estate->es_deltaIncState; 
    int       **deltaMemLeft = estate->es_deltaMemLeft;
    PullAction **deltaLeftPull = estate->es_deltaLeftPull; 
    PullAction **deltaRightPull = estate->es_deltaRightPull; 

    int       **bdCost = estate->es_bdCost;  
    IncState  **bdIncState = estate->es_bdIncState; 
    int       **bdMemLeft = estate->es_bdMemLeft;
    PullAction **bdLeftPull = estate->es_bdLeftPull; 
    PullAction **bdRightPull = estate->es_bdRightPull;     

    int left = incInfo->lefttree->id;
    int right = incInfo->righttree->id; 

    deltaCost[i][j] = 0; 
    deltaIncState[i][j] = STATE_DROP; 
    deltaMemLeft[i][j] = 0; 
    deltaLeftPull[i][j] = PULL_DELTA; 
    deltaRightPull[i][j] = PULL_DELTA; 

    int bdDropCost = INT_MAX; 

    /* Compute dropCost */
    int tempCost;
    int leftMem; 
    int rightMem;  
    int bdDropMemLeft;
    int k;  
    for (k = 0; k <= j; k++)
    {
        leftMem = k;
        rightMem = j - k; 

        tempCost = bdCost[left][leftMem] + bdCost[right][rightMem] + incInfo->prepare_cost + incInfo->compute_cost; 
        if (tempCost < bdDropCost)
        {
            bdDropCost = tempCost; 
            bdDropMemLeft = k; 
        }
    }
   
    /* DROP only */ 
    bdIncState[i][j] = STATE_DROP;
    
    bdCost[i][j] = bdDropCost; 
    bdMemLeft[i][j] = bdDropMemLeft; 
    bdLeftPull[i][j] = PULL_BATCH_DELTA; 
    bdRightPull[i][j] = PULL_BATCH_DELTA; 

}


static void
HashJoinDPHasUpdate (EState *estate, int i, int j)
{    
    IncInfo   *incInfo = estate->es_incInfo[i]; 

    int       **deltaCost = estate->es_deltaCost; 
    IncState  **deltaIncState = estate->es_deltaIncState; 
    int       **deltaMemLeft = estate->es_deltaMemLeft;
    PullAction **deltaLeftPull = estate->es_deltaLeftPull; 
    PullAction **deltaRightPull = estate->es_deltaRightPull; 

    int       **bdCost = estate->es_bdCost;  
    IncState  **bdIncState = estate->es_bdIncState; 
    int       **bdMemLeft = estate->es_bdMemLeft;
    PullAction **bdLeftPull = estate->es_bdLeftPull; 
    PullAction **bdRightPull = estate->es_bdRightPull; 


    int left = incInfo->lefttree->id;
    int right = incInfo->righttree->id; 

    int deltaDropCost = INT_MAX, deltaKeepCost, bdDropCost = INT_MAX, bdKeepCost; 

    /* First, compute dropCost */
    int tempCost;
    int leftMem; 
    int rightMem;  
    int deltaDropMemLeft; 
    int bdDropMemLeft; 
    int k; 
    for (k = 0; k <= j; k++)
    {
        leftMem = k;
        rightMem = j - k; 

        tempCost = deltaCost[left][leftMem] + bdCost[right][rightMem] + incInfo->prepare_cost + DELTA_COST; 
        if (tempCost < deltaDropCost )
        {
            deltaDropCost = tempCost; 
            deltaDropMemLeft = k;    
        }

        tempCost = bdCost[left][leftMem] + bdCost[right][rightMem] + incInfo->prepare_cost + incInfo->compute_cost + DELTA_COST; 
        if (tempCost < bdDropCost)
        {
            bdDropCost = tempCost; 
            bdDropMemLeft = k; 
        }
    }

    if (j == 0 || j < incInfo->memory_cost)   /* we can only drop the state */
    {
        deltaIncState[i][j] = STATE_DROP;
        bdIncState[i][j] = STATE_DROP;
    }
    else                            /* we need to considering keep it */ 
    {
        deltaKeepCost = deltaCost[left][j - incInfo->memory_cost] + DELTA_COST; 
        if (deltaKeepCost < deltaDropCost)
            deltaIncState[i][j] = STATE_KEEPMEM; 
        else
            deltaIncState[i][j] = STATE_DROP;

        bdKeepCost = bdCost[left][j - incInfo->memory_cost] + incInfo->compute_cost + DELTA_COST; 
        if (bdKeepCost < bdDropCost)
            bdIncState[i][j] = STATE_KEEPMEM; 
        else
            bdIncState[i][j] = STATE_DROP;
    }

    if (deltaIncState[i][j] == STATE_KEEPMEM)
    {
       deltaCost[i][j] = deltaKeepCost; 
       deltaMemLeft[i][j] = j - incInfo->memory_cost;
       deltaLeftPull[i][j] = PULL_DELTA; 
       deltaRightPull[i][j] = PULL_DELTA; 
    }
    else
    {
        deltaCost[i][j] = deltaDropCost; 
        deltaMemLeft[i][j] = deltaDropMemLeft;  
        deltaLeftPull[i][j] = PULL_DELTA; 
        deltaRightPull[i][j] = PULL_BATCH_DELTA; 
    }
        
    if (bdIncState[i][j] == STATE_KEEPMEM)
    {
        bdCost[i][j] = bdKeepCost; 
        bdMemLeft[i][j] = j - incInfo->memory_cost; 
        bdLeftPull[i][j] = PULL_BATCH_DELTA; 
        bdRightPull[i][j] = PULL_DELTA; 
    }
    else
    {
        bdCost[i][j] = bdDropCost; 
        bdMemLeft[i][j] = bdDropMemLeft;  
        bdLeftPull[i][j] = PULL_BATCH_DELTA; 
        bdRightPull[i][j] = PULL_BATCH_DELTA; 
    }

}

static void
HashJoinDPNoUpdate (EState *estate, int i, int j)
{
    IncInfo   *incInfo = estate->es_incInfo[i]; 

    int       **deltaCost = estate->es_deltaCost; 
    IncState  **deltaIncState = estate->es_deltaIncState; 
    int       **deltaMemLeft = estate->es_deltaMemLeft;
    PullAction **deltaLeftPull = estate->es_deltaLeftPull; 
    PullAction **deltaRightPull = estate->es_deltaRightPull; 

    int       **bdCost = estate->es_bdCost;  
    IncState  **bdIncState = estate->es_bdIncState; 
    int       **bdMemLeft = estate->es_bdMemLeft;
    PullAction **bdLeftPull = estate->es_bdLeftPull; 
    PullAction **bdRightPull = estate->es_bdRightPull;     

    int left = incInfo->lefttree->id;
    int right = incInfo->righttree->id; 

    deltaCost[i][j] = 0; 
    deltaIncState[i][j] = STATE_DROP; 
    deltaMemLeft[i][j] = 0; 
    deltaLeftPull[i][j] = PULL_DELTA; 
    deltaRightPull[i][j] = PULL_DELTA; 

    int bdDropCost = INT_MAX, bdKeepCost; 

    /* First, compute dropCost */
    int tempCost;
    int leftMem; 
    int rightMem;  
    int bdDropMemLeft;
    int k;  
    for (k = 0; k <= j; k++)
    {
        leftMem = k;
        rightMem = j - k; 

        tempCost = bdCost[left][leftMem] + bdCost[right][rightMem] + incInfo->prepare_cost + incInfo->compute_cost; 
        if (tempCost < bdDropCost)
        {
            bdDropCost = tempCost; 
            bdDropMemLeft = k; 
        }
    }

    if (j == 0 || j < incInfo->memory_cost)   /* we can only drop the state */
    {
        bdIncState[i][j] = STATE_DROP;
    }
    else                            /* we need to considering keep it */ 
    {
        bdKeepCost = bdCost[left][j - incInfo->memory_cost] + incInfo->compute_cost; 
        if (bdKeepCost < bdDropCost)
            bdIncState[i][j] = STATE_KEEPMEM; 
        else
            bdIncState[i][j] = STATE_DROP;
    }

    if (bdIncState[i][j] == STATE_DROP)
    {
        bdCost[i][j] = bdDropCost; 
        bdMemLeft[i][j] = bdDropMemLeft; 
        bdLeftPull[i][j] = PULL_BATCH_DELTA; 
        bdRightPull[i][j] = PULL_BATCH_DELTA; 
    } 
    else
    {
        bdCost[i][j] = bdKeepCost; 
        bdMemLeft[i][j] = j - incInfo->memory_cost; 
        bdLeftPull[i][j] = PULL_BATCH_DELTA; 
        bdRightPull[i][j] = PULL_DELTA; 
    }
}

static void
ExecDPAssignState (EState *estate, int i, int j, PullAction parentAction)
{
    IncInfo *incInfo = estate->es_incInfo[i]; 

    IncState  **deltaIncState = estate->es_deltaIncState; 
    int       **deltaMemLeft = estate->es_deltaMemLeft;
    PullAction **deltaLeftPull = estate->es_deltaLeftPull; 
    PullAction **deltaRightPull = estate->es_deltaRightPull; 

    IncState  **bdIncState = estate->es_bdIncState; 
    int       **bdMemLeft = estate->es_bdMemLeft;
    PullAction **bdLeftPull = estate->es_bdLeftPull; 
    PullAction **bdRightPull = estate->es_bdRightPull; 

    int left, right; 

    if (incInfo->lefttree)
        left = incInfo->lefttree->id;
    if (incInfo->righttree)
        right = incInfo->righttree->id; 

    switch (incInfo->ps->type)
    {
        case T_HashJoinState:
            if (parentAction == PULL_BATCH_DELTA)
            {
                incInfo->rightIncState = bdIncState[i][j]; 
                ExecDPAssignState(estate, left, bdMemLeft[i][j], bdLeftPull[i][j]); 
                if (bdIncState[i][j] == STATE_KEEPMEM)
                    ExecDPAssignState(estate, right, j - incInfo->memory_cost - bdMemLeft[i][j], bdRightPull[i][j]); 
                else
                    ExecDPAssignState(estate, right, j - bdMemLeft[i][j], bdRightPull[i][j]); 
            }
            else /* Pull Delta */
            {
                incInfo->rightIncState = deltaIncState[i][j]; 
                ExecDPAssignState(estate, left, deltaMemLeft[i][j], deltaLeftPull[i][j]); 
                if (deltaIncState[i][j] == STATE_KEEPMEM)
                    ExecDPAssignState(estate, right, j - incInfo->memory_cost - deltaMemLeft[i][j], deltaRightPull[i][j]); 
                else
                    ExecDPAssignState(estate, right, j - deltaMemLeft[i][j], deltaRightPull[i][j]); 
            }
            estate->es_incState[incInfo->id] = incInfo->rightIncState; 
            break;

        case T_MergeJoinState:
            elog(ERROR, "not supported MergeJoin yet"); 
            break;
        
        case T_NestLoopState:
            if (parentAction == PULL_BATCH_DELTA)
            {
                /* IncState are always STATE_DROP */
                ExecDPAssignState(estate, left, bdMemLeft[i][j], bdLeftPull[i][j]); 
                ExecDPAssignState(estate, right, j - bdMemLeft[i][j], bdRightPull[i][j]); 
            }
            else /* Pull Delta */
            {
                ExecDPAssignState(estate, left, deltaMemLeft[i][j], deltaLeftPull[i][j]); 
                ExecDPAssignState(estate, right, j - deltaMemLeft[i][j], deltaRightPull[i][j]); 
            }
            estate->es_incState[incInfo->id] = STATE_DROP;
            break;  

        case T_SeqScanState:
        case T_IndexScanState:
            if (parentAction == PULL_BATCH_DELTA)
                incInfo->leftIncState = bdIncState[i][j];
            else 
                incInfo->leftIncState = deltaIncState[i][j]; 
            estate->es_incState[incInfo->id] = incInfo->leftIncState; 
            break; 

        case T_AggState:
        case T_SortState:
            if (parentAction == PULL_BATCH_DELTA)
            {
                incInfo->leftIncState = bdIncState[i][j]; 
                ExecDPAssignState(estate, left, bdMemLeft[i][j], bdLeftPull[i][j]); 
            }
            else /* Pull Delta */
            {
                incInfo->leftIncState = deltaIncState[i][j]; 
                ExecDPAssignState(estate, left, deltaMemLeft[i][j], deltaLeftPull[i][j]); 
            }
            estate->es_incState[incInfo->id] = incInfo->leftIncState; 
            break; 

        default:
            elog(NOTICE, "unrecognized nodetype: %u", incInfo->ps->type);
            return NULL; 
    }
}

static void AssignState(EState *estate, IncInfo *incInfo, IncState state)
{
    int id = incInfo->id; 
    AggState *aggState; 
    switch(incInfo->ps->type)
    {
        case T_HashJoinState:
            incInfo->rightIncState = state;
            estate->es_incState[id]  = state; 
            break;

        case T_MergeJoinState:
            elog(ERROR, "not supported MergeJoin yet"); 
            break;

        case T_NestLoopState:
            incInfo->leftIncState = STATE_DROP; 
            estate->es_incState[id]  = STATE_DROP; 
            break;

        case T_SeqScanState:
        case T_IndexScanState:
            return; 
            break; 

        case T_AggState:
            aggState = (AggState *)incInfo->ps; 
            if (aggState->aggstrategy == AGG_SORTED)
            {
                incInfo->leftIncState = STATE_DROP; 
                estate->es_incState[id]  = STATE_DROP; 
            }
            else
            {
                incInfo->leftIncState = state; 
                estate->es_incState[id]  = state; 
            }
            break; 

        case T_SortState:
            incInfo->leftIncState = state; 
            estate->es_incState[id]  = state; 
            break; 

        default:
            elog(NOTICE, "unrecognized nodetype: %u", incInfo->ps->type);
            return; 
    }
}

static void 
ExecGreedyHelper(EState *estate)
{
    int incMemory = estate->es_incMemory; 
    IncInfo **incInfo_array = estate->es_incInfo; 

    if (decision_method == DM_TOPDOWN || decision_method == DM_MEMBIGFIRST)
    {
        for (int i = estate->es_numIncInfo - 1; i >=0; i--)
        {
            IncInfo *incInfo = incInfo_array[i];
            if (incInfo->memory_cost < incMemory)
            {
                AssignState(estate, incInfo, STATE_KEEPMEM); 
                incMemory -= incInfo->memory_cost; 
            }
        }
    }
    else
    {
        for (int i = 0; i < estate->es_numIncInfo; i++)
        {
            IncInfo *incInfo = incInfo_array[i];
            if (incInfo->memory_cost < incMemory)
            {
                AssignState(estate, incInfo, STATE_KEEPMEM); 
                incMemory -= incInfo->memory_cost; 
            }
        }
    }
}


static 
void ExecResetDelta(EState *estate, PlanState *ps)
{
    IncTQPool *tq_pool = estate->tq_pool;
    ScanState **reader_ss_array = estate->reader_ss; 

    for (int i = 0; i < estate->es_numLeaf; i++)
        DrainTQReader(tq_pool, reader_ss_array[i]->tq_reader); 

    ExecResetState(ps); 
} 

/*
 * Wait for delta and if there are delta coming, mark them 
 */

static void 
ExecWaitAndMarkDelta(EState *estate, PlanState *ps)
{
    Relation r; 
    int numDelta = 0; 
    int threshold = DELTA_THRESHOLD; 

    IncTQPool *tq_pool = estate->tq_pool; 
    ScanState **reader_ss_array = estate->reader_ss; 

    for (;;) 
    {
		CHECK_FOR_INTERRUPTS();
        
        numDelta = GetTQUpdate(tq_pool); 

        if (numDelta >= threshold)
            break;

       sleep(1); 
    }

    for(int i = 0; i < estate->es_numLeaf; i++) 
    {
        r = reader_ss_array[i]->ss_currentRelation;  
        reader_ss_array[i]->ps.ps_IncInfo->leftUpdate = HasTQUpdate(tq_pool, r); 

        reader_ss_array[i]->tq_reader = GetTQReader(tq_pool, r, reader_ss_array[i]->tq_reader); 
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

static void
ExecGenPullAction(IncInfo *incInfo) 
{
    ExecIncPropUpdate(incInfo); 

    ExecGenPullActionHelper(incInfo, PULL_BATCH_DELTA); 
}

static void
ExecGenPullActionHelper(IncInfo *incInfo, PullAction parentAction)
{
    incInfo->parentAction = parentAction; 

    if (incInfo->lefttree == NULL && incInfo->righttree == NULL) /* Scan operator */
    {
        incInfo->leftAction = parentAction;
        incInfo->rightAction = parentAction; 
    }
    else if (incInfo->righttree == NULL) /* Agg or Sort operator */
    {
        switch (incInfo->ps->type) 
        {
            case T_AggState:
                /* 
                 * TODO: right now, we always return the whole aggregation results; 
                 *       we will fix it later when we support negation 
                 */
                if (incInfo->leftIncState == STATE_DROP) 
                    incInfo->leftAction = PULL_BATCH_DELTA; 
                break; 

            case T_SortState:
                if (parentAction == PULL_BATCH_DELTA && incInfo->leftIncState == STATE_DROP) 
                    incInfo->leftAction = PULL_BATCH_DELTA; 
                break; 

            default:
                elog(ERROR, "unsupported node type except sort and aggregate");
                return; 
        }
        
        ExecGenPullActionHelper(incInfo->lefttree, incInfo->leftAction); 
    }
    else if (incInfo->lefttree == NULL)
    {
        Assert(false); 
    }
    else                                /* Join operator */
    {
        if (parentAction == PULL_BATCH_DELTA) 
        {
            if (incInfo->leftIncState == STATE_DROP)
                incInfo->leftAction = PULL_BATCH_DELTA; 
 
            if (incInfo->rightIncState == STATE_DROP)
                incInfo->rightAction = PULL_BATCH_DELTA;
        } 
        else /* only need delta */ 
        {
            if (incInfo->leftUpdate && incInfo->rightIncState == STATE_DROP) 
                incInfo->rightAction = PULL_BATCH_DELTA;
            if (incInfo->rightUpdate && incInfo->leftIncState == STATE_DROP) 
                incInfo->leftAction = PULL_BATCH_DELTA;
        }

        ExecGenPullActionHelper(incInfo->lefttree, incInfo->leftAction); 
        ExecGenPullActionHelper(incInfo->righttree, incInfo->rightAction); 
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
        
        default:
            elog(ERROR, "unrecognized nodetype: %u", ps->type);
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

        case T_SortState:
            ExecInitSortDelta((SortState *) ps); 
            break;
        
        default:
            elog(ERROR, "unrecognized nodetype: %u", ps->type);
            return; 
    }
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
        fprintf(statFile, "%d\t%d\t%.2f\t%.2f\t%.2f\t", \
                estate->es_totalMemCost, estate->es_incMemory, estate->batchTime, estate->repairTime, estate->decisionTime); 
        for (int i = 0; i < estate->es_numIncInfo; i++) 
        {
            fprintf(statFile, "%d ", estate->es_incState[i]); 
        }
        fprintf(statFile, "\n"); 

        fclose(estate->es_statFile); 
    }

    /* Free IncInfo */
    IncInfo * root = estate->es_incInfo[estate->es_numIncInfo - 1];
    while (root->parenttree != NULL)
        root = root->parenttree;
    FreeIncInfo(root); 

    pfree(estate->es_incInfo); 

    if (decision_method == DM_DP)
    {
        for (int i = 0; i < estate->es_numIncInfo; i++) 
        {
            pfree(estate->es_deltaCost[i]); 
            pfree(estate->es_deltaIncState[i]);
            pfree(estate->es_deltaMemLeft[i]); 
            pfree(estate->es_deltaLeftPull[i]);
            pfree(estate->es_deltaRightPull[i]);
            
            pfree(estate->es_bdCost[i]); 
            pfree(estate->es_bdIncState[i]);
            pfree(estate->es_bdMemLeft[i]); 
            pfree(estate->es_bdLeftPull[i]);
            pfree(estate->es_bdRightPull[i]);   
        }
        pfree(estate->es_deltaCost); 
        pfree(estate->es_deltaIncState);
        pfree(estate->es_deltaMemLeft); 
        pfree(estate->es_deltaLeftPull);
        pfree(estate->es_deltaRightPull);
        
        pfree(estate->es_bdCost); 
        pfree(estate->es_bdIncState);
        pfree(estate->es_bdMemLeft); 
        pfree(estate->es_bdLeftPull);
        pfree(estate->es_bdRightPull);      
    }

    pfree(estate->es_incState);
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
