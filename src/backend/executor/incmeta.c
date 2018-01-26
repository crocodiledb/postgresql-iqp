/*-------------------------------------------------------------------------
 *
 * repairState.c
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/repairState.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "nodes/execnodes.h"

#include "executor/incinfo.h"

bool enable_incremental;


static IncInfo * ExecInitIncInfoHelper(PlanState *ps, IncInfo *parent); 
static void ExecDPHelper (IncInfo *incInfo, IncState states[], int index);
static void ExecGenPullActionHelper(IncInfo *incInfo, PullAction parentAction); 
static bool ExecIncHasUpdate(IncInfo *incInfo); 


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

IncInfo * ExecInitIncInfo(PlanState *ps) 
{
    ExecInitIncInfoHelper(ps, NULL); 
   
    IncInfo *left = ps->ps_IncInfo;

    while (left->lefttree != NULL)
        left = left->lefttree; 
    left->trigger_computation = 6000000;  

}

static IncInfo * 
ExecInitIncInfoHelper(PlanState *ps, IncInfo *parent) 
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
            innerIncInfo(incInfo) = ExecInitIncInfoHelper(innerPlan, incInfo); 
            outerIncInfo(incInfo) = ExecInitIncInfoHelper(outerPlan, incInfo); 
            break;

        case T_MergeJoinState:
            innerPlan = outerPlanState(outerPlanState(innerPlanState(ps))); /* Skip Material and Sort nodes */
            outerPlan = outerPlanState(outerPlanState(ps));                  /* Skip Sort node */ 
            innerIncInfo(incInfo) = ExecInitIncInfoHelper(innerPlan, incInfo); 
            outerIncInfo(incInfo) = ExecInitIncInfoHelper(outerPlan, incInfo); 
            break; 

        case T_SeqScanState:
        case T_IndexScanState:
            innerIncInfo(incInfo) = NULL;
            outerIncInfo(incInfo) = NULL;
            break; 

        case T_AggState:
        case T_SortState:
            outerPlan = outerPlanState(ps); 
            outerIncInfo(incInfo) = ExecInitIncInfoHelper(outerPlan, incInfo);
            innerIncInfo(incInfo) = NULL;
            break; 

        default:
            elog(NOTICE, "unrecognized nodetype: %u", ps->type);
            return NULL; 
    }

    ps->ps_IncInfo = incInfo; 

    return incInfo; 
}


/*
 * Step 2. Running DP algorithm to determine which states to keep/drop  
 */

void 
ExecDP (IncInfo *incInfo)
{
    IncState states[] = {STATE_KEEPMEM, STATE_DROP, STATE_DROP, STATE_DROP, STATE_DROP, STATE_DROP, STATE_DROP}; 
    ExecDPHelper(incInfo, states, 0); 
}

static void
ExecDPHelper (IncInfo *incInfo, IncState states[], int index)
{

    if (incInfo->lefttree ==  NULL && incInfo->righttree == NULL) 
    {    
        incInfo->leftIncState = states[index];
        incInfo->rightIncState = states[index];
        return; 
    } 
    else if (incInfo->righttree == NULL) 
    {        
        incInfo->leftIncState = states[index];
        incInfo->rightIncState = states[index];
        ExecDPHelper(incInfo->lefttree, states, index+1);
    }
    else if (incInfo->lefttree == NULL)
    {
        Assert(false); 
    }
    else
    {        
        incInfo->leftIncState = STATE_DROP; /* todo: no left state for join right now */
        incInfo->rightIncState = states[index];
        ExecDPHelper(incInfo->lefttree, states, index + 1);
        ExecDPHelper(incInfo->righttree, states, index + 2); 
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
ExecMarkDelta(IncInfo *incInfo) 
{
    IncInfo *left = incInfo; 

    while (left->lefttree != NULL)
        left = left->lefttree; 
    left->leftUpdate = true;  
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
