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

#include "executor/incinfo.h"

#define DELTA_COST 0

bool enable_incremental;
int  memory_budget;     /* kB units*/
DecisionMethod decision_method; 


//static int ExecAssignIncInfo(IncInfo **incInfo_array, IncInfo *cur, int id);
static void ExecAssignIncInfo (IncInfo **incInfo_array, int numIncInfo, IncInfo *root);

static IncInfo * ExecInitIncInfoHelper(PlanState *ps, IncInfo *parent, int *count); 

static void ExecMarkDelta(IncInfo *incInfo); 
static void ExecCollectCostInfo(IncInfo *incInfo); 
static void RunDPSolution(EState *estate); 
static void SortDPHasUpdate (EState *estate, int i, int j); 
static void SortDPNoUpdate (EState *estate, int i, int j); 
static void AggDPHasUpdate (EState *estate, int i, int j); 
static void AggDPNoUpdate (EState *estate, int i, int j); 
static void HashJoinDPHasUpdate (EState *estate, int i, int j); 
static void HashJoinDPNoUpdate (EState *estate, int i, int j); 
static void ExecDPAssignState (EState *estate, int i, int j, PullAction parentAction); 
static void PrintStateDecision(EState *estate); 

static void ExecGenPullActionHelper(IncInfo *incInfo, PullAction parentAction); 
static bool ExecIncHasUpdate(IncInfo *incInfo); 

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
 *
 */

void ExecInitIncInfo(EState *estate, PlanState *ps) 
{
    int count = 0; 
    int incMemory = 0; 

    ExecInitIncInfoHelper(ps, NULL, &count);

    estate->es_incInfo = (IncInfo **) palloc(sizeof(IncInfo *) * count); 
    ExecAssignIncInfo(estate->es_incInfo, count, ps->ps_IncInfo); 
    estate->es_numIncInfo = count; 

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

    /* Mark trigger computation */
    
    IncInfo *left = ps->ps_IncInfo;

    while (left->lefttree != NULL)
        left = left->lefttree; 
    left->trigger_computation = 5940000; 

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

/*
static int 
ExecAssignIncInfo(IncInfo **incInfo_array, IncInfo *cur, int id)
{
    if (cur->lefttree != NULL)
        id = ExecAssignIncInfo(incInfo_array, cur->lefttree, id); 
    if (cur->righttree != NULL)
        id = ExecAssignIncInfo(incInfo_array, cur->righttree, id); 
        
    incInfo_array[id] = cur; 
    cur->id = id; 

    return id+1; 
}
*/

static IncInfo * 
ExecInitIncInfoHelper(PlanState *ps, IncInfo *parent, int *count) 
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
            innerIncInfo(incInfo) = ExecInitIncInfoHelper(innerPlan, incInfo, count); 
            outerIncInfo(incInfo) = ExecInitIncInfoHelper(outerPlan, incInfo, count); 
            break;

        case T_MergeJoinState:
            innerPlan = outerPlanState(outerPlanState(innerPlanState(ps))); /* Skip Material and Sort nodes */
            outerPlan = outerPlanState(outerPlanState(ps));                  /* Skip Sort node */ 
            innerIncInfo(incInfo) = ExecInitIncInfoHelper(innerPlan, incInfo, count); 
            outerIncInfo(incInfo) = ExecInitIncInfoHelper(outerPlan, incInfo, count); 
            break; 

        case T_SeqScanState:
        case T_IndexScanState:
            innerIncInfo(incInfo) = NULL;
            outerIncInfo(incInfo) = NULL;
            break; 

        case T_AggState:
        case T_SortState:
            outerPlan = outerPlanState(ps); 
            outerIncInfo(incInfo) = ExecInitIncInfoHelper(outerPlan, incInfo, count);
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
 *      2a. Estimate the potential updates and populate them to the IncInfo tree
 *      2b. Collect memory_cost and computate_cost
 *      2c. Running DP algorithm to determine which states to keep/drop 
 *      2d. Assign states to IncInfo 
 */

void 
ExecMakeDecision (EState *estate, IncInfo *incInfo)
{
    ExecMarkDelta(incInfo); /* Step 2a */
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

    //PrintStateDecision(estate); 
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
ExecMarkDelta(IncInfo *incInfo) 
{
    IncInfo *left = incInfo; 

    while (left->lefttree != NULL)
        left = left->lefttree; 
    left->leftUpdate = true;  
    
    ExecIncHasUpdate(incInfo); 
}

static void
ExecCollectCostInfo(IncInfo *incInfo)
{
    PlanState *ps = incInfo->ps; 
    Plan *plan = ps->plan; 

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

        case T_SeqScanState:
        case T_IndexScanState:
            incInfo->prepare_cost = 0;
            incInfo->compute_cost = plan->total_cost;
            incInfo->memory_cost = 0; 
            return;  
            break; 

        case T_AggState:
            outerPlan = outerPlan(plan); 
            incInfo->prepare_cost = (int)(plan->startup_cost - outerPlan->total_cost); 
            incInfo->compute_cost = (int)(plan->total_cost - plan->startup_cost); 
            incInfo->memory_cost = ExecAggMemoryCost((AggState *) ps); 
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

            switch (incInfo->ps->type)
            {
                case T_SortState:
                    if (incInfo->leftUpdate)
                        incInfo->execDPNode = SortDPHasUpdate; 
                    else
                        incInfo->execDPNode = SortDPNoUpdate; 
                    break; 

                case T_AggState: 
                    if (incInfo->leftUpdate)
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
            {
                deltaCost[i][j] = DELTA_COST; 
                deltaIncState[i][j] = STATE_DROP; 
                deltaMemLeft[i][j] = 0; 
                deltaLeftPull[i][j] = PULL_DELTA; 
                deltaRightPull[i][j] = PULL_DELTA; 

    
                bdCost[i][j] = incInfo->prepare_cost + incInfo->compute_cost + DELTA_COST;
                bdIncState[i][j] = STATE_DROP;
                bdMemLeft[i][j] = 0;
                bdLeftPull[i][j] = PULL_DELTA;
                bdRightPull[i][j] = PULL_DELTA; 
            }
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
        if (keepCost < dropCost)
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
        if (keepCost < dropCost)
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
    switch(incInfo->ps->type)
    {
        case T_HashJoinState:
            incInfo->rightIncState = state;
            estate->es_incState[id]  = state; 
            break;

        case T_MergeJoinState:
            elog(ERROR, "not supported MergeJoin yet"); 
            break; 

        case T_SeqScanState:
        case T_IndexScanState:
            return; 
            break; 

        case T_AggState:
        case T_SortState:
            incInfo->leftIncState = state; 
            estate->es_incState[id]  = state; 
            break; 

        default:
            elog(NOTICE, "unrecognized nodetype: %u", incInfo->ps->type);
            return NULL; 
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
ExecGenPullAction(IncInfo *incInfo) 
{
    ExecIncHasUpdate(incInfo); 

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
                 * todo: right now, we always return the whole aggregation results; 
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
ExecIncHasUpdate(IncInfo *incInfo)
{
    if (incInfo->lefttree == NULL && incInfo->righttree == NULL) 
    {
        return incInfo->leftUpdate;  
    }
    else if (incInfo->righttree == NULL) 
    {
        incInfo->rightUpdate = false;
        incInfo->leftUpdate = ExecIncHasUpdate(incInfo->lefttree); 
        return incInfo->leftUpdate | incInfo->rightUpdate; 
    }
    else if (incInfo->lefttree == NULL) /* This case is impossible; outer plan is not NULL except for leaf nodes */
    {
        Assert(false);
        return false;
    }
    else
    {
        incInfo->leftUpdate = ExecIncHasUpdate(incInfo->lefttree); 
        incInfo->rightUpdate = ExecIncHasUpdate(incInfo->righttree); 
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

void 
ExecCleanIncInfo(EState *estate)
{
    /* For Stat*/
    if (estate->es_statFile != NULL) 
    {
        FILE *statFile = estate->es_statFile;
        fprintf(statFile, "%d\t%.2f\t%.2f\t%.2f\t", estate->es_incMemory, estate->batchTime, estate->repairTime, estate->decisionTime); 
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
