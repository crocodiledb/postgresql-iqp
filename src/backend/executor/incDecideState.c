/*-------------------------------------------------------------------------
 *
 * incDecideState.c
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/incDecideState.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "executor/incDecideState.h"
#include "executor/incinfo.h"
#include "executor/incmeta.h"

#include <limits.h>

#define DELTA_COST 0

#define DELTA_DROP      0
#define DELTA_KEEP      1
#define BD_DROP         2
#define BD_KEEP         3
#define NONJOIN_OPTIONS 4

#define DELTA_DROPBOTH  0
#define DELTA_KEEPLEFT  1
#define DELTA_KEEPRIGHT 2
#define DELTA_KEEPBOTH  3
#define BD_DROPBOTH     4
#define BD_KEEPRIGHT    5
#define JOIN_OPTIONS    6

bool OptDP = true;

/* Helper functions for DP Algorithms */

static void SimpleDropDP(DPMeta *dpmeta, int i, int j, IncInfo *incInfo); 
static void SortDPHasUpdate (DPMeta *dpmeta, int i, int j, IncInfo *incInfo); 
static void SortDPNoUpdate (DPMeta *dpmeta, int i, int j, IncInfo *incInfo); 
static void AggDPHasUpdate (DPMeta *dpmeta, int i, int j, IncInfo *incInfo); 
static void AggDPNoUpdate (DPMeta *dpmeta, int i, int j, IncInfo *incInfo); 
static void HashJoinDPLeftUpdate (DPMeta *dpmeta, int i, int j, IncInfo *incInfo); 
static void HashJoinDPRightUpdate (DPMeta *dpmeta, int i, int j, IncInfo *incInfo); 
static void HashJoinDPNoUpdate (DPMeta *dpmeta, int i, int j, IncInfo *incInfo);
static void HashJoinDPBothUpdate (DPMeta *dpmeta, int i, int j, IncInfo *incInfo); 
static void NestLoopDP (DPMeta *dpmeta, int i, int j, IncInfo *incInfo); 
static void MaterialDP(DPMeta *dpmeta, int i, int j, IncInfo *incInfo);

static void ExecGreedyAssignState(IncInfo *incInfo, IncState state); 

static inline 
void SetCostInfo(int *cand_cost, int *cand_memleft, int *cand_memright, int cost, int memleft, int memright, int index)
{
    cand_cost[index] = cost;
    cand_memleft[index] = memleft;
    cand_memright[index] = memright; 
}

static inline
void SetDPMeta(DPMeta *dpmeta, int i, int j, \
        int *cand_cost, int *cand_memleft, int *cand_memright, 
        IncState *cand_leftstate, IncState *cand_rightstate, 
        PullAction *cand_leftpull, PullAction *cand_rightpull, 
        int index, bool delta)
{
    if (delta)
    {
        dpmeta->deltaCost[i][j] = cand_cost[index]; 
        dpmeta->deltaMemLeft[i][j] = cand_memleft[index]; 
        dpmeta->deltaMemRight[i][j] = cand_memright[index];
        dpmeta->deltaIncState[i][j][LEFT_STATE] = cand_leftstate[index];
        dpmeta->deltaIncState[i][j][RIGHT_STATE] = cand_rightstate[index]; 
        dpmeta->deltaLeftPull[i][j] = cand_leftpull[index];
        dpmeta->deltaRightPull[i][j] = cand_rightpull[index]; 
    }
    else
    {
        dpmeta->bdCost[i][j] = cand_cost[index]; 
        dpmeta->bdMemLeft[i][j] = cand_memleft[index]; 
        dpmeta->bdMemRight[i][j] = cand_memright[index];
        dpmeta->bdIncState[i][j][LEFT_STATE] = cand_leftstate[index];
        dpmeta->bdIncState[i][j][RIGHT_STATE] = cand_rightstate[index]; 
        dpmeta->bdLeftPull[i][j] = cand_leftpull[index];
        dpmeta->bdRightPull[i][j] = cand_rightpull[index]; 

    }
}


DPMeta *
BuildDPMeta(int numIncInfo, int incMemory)
{
    DPMeta *dpmeta = (DPMeta *) palloc(sizeof(DPMeta)); 

    /* Allocate necessary structures */
    dpmeta->deltaCost = (int **) palloc(sizeof(int *) * numIncInfo);
    dpmeta->deltaIncState = (IncState ***) palloc(sizeof(IncState **) * numIncInfo);
    dpmeta->deltaMemLeft = (int **) palloc(sizeof(int *) * numIncInfo);
    dpmeta->deltaMemRight = (int **) palloc(sizeof(int *) * numIncInfo);
    dpmeta->deltaLeftPull = (PullAction **) palloc(sizeof(PullAction *) * numIncInfo); 
    dpmeta->deltaRightPull = (PullAction **) palloc(sizeof(PullAction *) * numIncInfo); 
    
    dpmeta->bdCost = (int **) palloc(sizeof(int *) * numIncInfo);
    dpmeta->bdIncState = (IncState ***) palloc(sizeof(IncState **) * numIncInfo);
    dpmeta->bdMemLeft = (int **) palloc(sizeof(int *) * numIncInfo);
    dpmeta->bdMemRight = (int **) palloc(sizeof(int *) * numIncInfo);
    dpmeta->bdLeftPull = (PullAction **) palloc(sizeof(PullAction *) * numIncInfo); 
    dpmeta->bdRightPull = (PullAction **) palloc(sizeof(PullAction *) * numIncInfo); 
    
    for (int i = 0; i < numIncInfo; i++) 
    {
        dpmeta->deltaCost[i] = (int *) palloc(sizeof(int) * (incMemory + 1)); 
        dpmeta->deltaIncState[i] = (IncState **) palloc(sizeof(IncState *) * (incMemory + 1));
        for (int j = 0; j < incMemory + 1; j++)
            dpmeta->deltaIncState[i][j] = (IncState *) palloc(sizeof(IncState) * MAX_STATE); 
        dpmeta->deltaMemLeft[i] = (int *) palloc(sizeof(int) * (incMemory + 1)); 
        dpmeta->deltaMemRight[i] = (int *) palloc(sizeof(int) * (incMemory + 1)); 
        dpmeta->deltaLeftPull[i] = (PullAction *) palloc(sizeof(PullAction) * (incMemory + 1)); 
        dpmeta->deltaRightPull[i] = (PullAction *) palloc(sizeof(PullAction) * (incMemory + 1)); 
    
        dpmeta->bdCost[i] = (int *) palloc(sizeof(int) * (incMemory + 1));
        dpmeta->bdIncState[i] = (IncState **) palloc(sizeof(IncState *) * (incMemory + 1)); 
        for (int j = 0; j < incMemory + 1; j++)
            dpmeta->bdIncState[i][j] = (IncState *) palloc(sizeof(IncState) * MAX_STATE); 
        dpmeta->bdMemLeft[i] = (int *) palloc(sizeof(int) * (incMemory + 1));
        dpmeta->bdMemRight[i] = (int *) palloc(sizeof(int) * (incMemory + 1));
        dpmeta->bdLeftPull[i] = (PullAction *) palloc(sizeof(PullAction) * (incMemory + 1)); 
        dpmeta->bdRightPull[i] = (PullAction *) palloc(sizeof(PullAction) * (incMemory + 1)); 
    }

    return dpmeta; 
}

static 
void ExecOptDP(DPMeta *dpmeta, IncInfo *incInfo, int i, int incMemory, bool *accessed)
{
    memset(accessed, 0, sizeof(bool) * (incMemory + 1));
    int start, end; 
    int step = 1; 
    int j = 0; 

    for(;;)
    {
        if (j > incMemory)
            break; 

        start = j;
        if (!accessed[start])
        {
            accessed[start] = true;
            incInfo->execDPNode(dpmeta, i, start, incInfo); 
        }

        end = start + step; 
        if (end > incMemory)
            end = incMemory; 

        if (!accessed[end])
        {
            accessed[end] = true; 
            incInfo->execDPNode(dpmeta, i, end, incInfo); 
        }

        if (dpmeta->bdCost[i][start] == dpmeta->bdCost[i][end] && dpmeta->deltaCost[i][start] == dpmeta->deltaCost[i][end])
        {
            j = end + 1; 
            step = step * 2; 
            for (int k = start + 1; k < end; k++)
            {
                accessed[k] = true; 

                dpmeta->deltaCost[i][k] =  dpmeta->deltaCost[i][start];
                dpmeta->deltaMemLeft[i][k] =  dpmeta->deltaMemLeft[i][start];
                dpmeta->deltaMemRight[i][k] = dpmeta->deltaMemRight[i][start]; 
                dpmeta->deltaIncState[i][k][LEFT_STATE] =dpmeta->deltaIncState[i][start][LEFT_STATE];
                dpmeta->deltaIncState[i][k][RIGHT_STATE] = dpmeta->deltaIncState[i][start][RIGHT_STATE];
                dpmeta->deltaLeftPull[i][k] = dpmeta->deltaLeftPull[i][start];
                dpmeta->deltaRightPull[i][k] =  dpmeta->deltaRightPull[i][start]; 
    
                dpmeta->bdCost[i][k] = dpmeta->bdCost[i][start];
                dpmeta->bdMemLeft[i][k] =  dpmeta->bdMemLeft[i][start];
                dpmeta->bdMemRight[i][k] = dpmeta->bdMemRight[i][start];
                dpmeta->bdIncState[i][k][LEFT_STATE] = dpmeta->bdIncState[i][start][LEFT_STATE];
                dpmeta->bdIncState[i][k][RIGHT_STATE] = dpmeta->bdIncState[i][start][RIGHT_STATE]; 
                dpmeta->bdLeftPull[i][k] = dpmeta->bdLeftPull[i][start];
                dpmeta->bdRightPull[i][k] = dpmeta->bdRightPull[i][start]; 
            } 
        }
        else
        {
            j++; 
            step = 1;
        }
    }
}

void 
ExecDPSolution(DPMeta *dpmeta, IncInfo **incInfoArray, int numIncInfo, int incMemory, bool isSlave)
{
    IncInfo   *incInfo;
    int i, j;

    /* Init info for leaf nodes */
    for (i = 0; i < numIncInfo; i++)
    {
        incInfo = incInfoArray[i]; 
        /* If not leaf nodes, break */
        if (incInfoArray[i]->lefttree != NULL || incInfoArray[i]->righttree != NULL) 
        {
            switch (incInfo->type)
            {
                case INC_SORT:
                    if (incInfo->leftUpdate)
                        incInfo->execDPNode = SortDPHasUpdate; 
                    else
                        incInfo->execDPNode = SortDPNoUpdate; 
                    break; 

                case INC_AGGSORT:
                    incInfo->execDPNode = SimpleDropDP; 
                    break; 

                case INC_AGGHASH: 
                    if (incInfo->leftUpdate) 
                        incInfo->execDPNode = AggDPHasUpdate; 
                    else
                        incInfo->execDPNode = AggDPNoUpdate; 
                    break; 

                case INC_MATERIAL:
                    //if (isSlave || incInfo->ps)
                    incInfo->execDPNode = MaterialDP;
                    //else
                    //    incInfo->execDPNode = SimpleDropDP; 
                    break;  

                case INC_HASHJOIN:
                    if (incInfo->leftUpdate && !incInfo->rightUpdate)
                        incInfo->execDPNode = HashJoinDPLeftUpdate;
                    else if (!incInfo->leftUpdate && incInfo->rightUpdate)
                        incInfo->execDPNode = HashJoinDPRightUpdate;
                    else if (!incInfo->leftUpdate && !incInfo->rightUpdate)
                        incInfo->execDPNode = HashJoinDPNoUpdate; 
                    else
                        incInfo->execDPNode = HashJoinDPBothUpdate;  
                        //elog(ERROR, "We only support updates from a single table"); 
                    break;

                case INC_NESTLOOP:
                    //if (incInfo->leftUpdate && incInfo->rightUpdate)
                    //    elog(ERROR, "We only support updates from one side ");
                    //else
                    incInfo->execDPNode = NestLoopDP;
                    break; 

                case INC_MERGEJOIN:
                    elog(ERROR, "not supported MergeJoin yet");
                    break;

                default:
                    elog(ERROR, "ExecDPSolution unrecognized nodetype: %u", incInfo->type);
                    return; 
            }
        }
        else 
        {
            incInfo->execDPNode = NULL; 
            for (j = 0; j <= incMemory; j++)
                SimpleDropDP(dpmeta, i, j, incInfo); 
        }
    }

    bool *accessed = palloc(sizeof(bool) * (incMemory + 1)); 

    for (i = 0; i < numIncInfo; i++)
    {
        incInfo = incInfoArray[i]; 
        if (incInfo->execDPNode == NULL) /* leaf nodes */
            continue; 


        if (OptDP)
        {
            ExecOptDP(dpmeta, incInfo, i, incMemory, accessed);
        }
        else
        {
            for (j = 0; j <= incMemory; j++)
                incInfo->execDPNode(dpmeta, i, j, incInfo); 
        }
        
    }

    pfree(accessed); 
}

void
ExecDPAssignState (DPMeta *dpmeta, IncInfo **incInfoArray, int i, int j, PullAction parentAction)
{
    IncInfo *incInfo = incInfoArray[i];

    IncState  ***deltaIncState = dpmeta->deltaIncState; 
    int       **deltaMemLeft = dpmeta->deltaMemLeft;
    int       **deltaMemRight = dpmeta->deltaMemRight;
    PullAction **deltaLeftPull = dpmeta->deltaLeftPull; 
    PullAction **deltaRightPull = dpmeta->deltaRightPull; 

    IncState  ***bdIncState = dpmeta->bdIncState; 
    int       **bdMemLeft = dpmeta->bdMemLeft;
    int       **bdMemRight = dpmeta->bdMemRight;
    PullAction **bdLeftPull = dpmeta->bdLeftPull; 
    PullAction **bdRightPull = dpmeta->bdRightPull; 

    int left, right; 

    if (incInfo->lefttree)
        left = incInfo->lefttree->id;
    if (incInfo->righttree)
        right = incInfo->righttree->id; 

    switch (incInfo->type)
    {
        case INC_HASHJOIN:
        case INC_NESTLOOP:
            if (parentAction == PULL_BATCH_DELTA)
            {
                incInfo->incState[LEFT_STATE] = bdIncState[i][j][LEFT_STATE];
                incInfo->incState[RIGHT_STATE] = bdIncState[i][j][RIGHT_STATE];
                ExecDPAssignState(dpmeta, incInfoArray, left, bdMemLeft[i][j], bdLeftPull[i][j]); 
                ExecDPAssignState(dpmeta, incInfoArray, right, bdMemRight[i][j], bdRightPull[i][j]); 
            }
            else if (parentAction == PULL_DELTA) /* Pull Delta */
            {
                incInfo->incState[LEFT_STATE] = deltaIncState[i][j][LEFT_STATE];
                incInfo->incState[RIGHT_STATE] = deltaIncState[i][j][RIGHT_STATE];
                ExecDPAssignState(dpmeta, incInfoArray, left,  deltaMemLeft[i][j], deltaLeftPull[i][j]); 
                ExecDPAssignState(dpmeta, incInfoArray, right, deltaMemRight[i][j], deltaRightPull[i][j]); 
            }
            else
            {
                elog(ERROR, "Join: PULL_BATCH or PULL_NOTHING should not exist"); 
            }
            break;

        case INC_MERGEJOIN:
            elog(ERROR, "not supported MergeJoin yet"); 
            break;

        case INC_SEQSCAN:
        case INC_INDEXSCAN:
            if (parentAction == PULL_BATCH_DELTA)
                incInfo->incState[LEFT_STATE] = bdIncState[i][j][LEFT_STATE];
            else if (parentAction == PULL_DELTA) 
                incInfo->incState[LEFT_STATE] = deltaIncState[i][j][LEFT_STATE]; 
            else
                elog(ERROR, "Scan: PULL_BATCH should not exist"); 
            break; 

        case INC_AGGHASH:
        case INC_AGGSORT:
        case INC_SORT:
        case INC_MATERIAL:
            if (parentAction == PULL_BATCH_DELTA)
            {
                incInfo->incState[LEFT_STATE] = bdIncState[i][j][LEFT_STATE]; 
                ExecDPAssignState(dpmeta, incInfoArray, left, bdMemLeft[i][j], bdLeftPull[i][j]); 
            }
            else if (parentAction == PULL_DELTA) /* Pull Delta */
            {
                incInfo->incState[LEFT_STATE] = deltaIncState[i][j][LEFT_STATE]; 
                ExecDPAssignState(dpmeta, incInfoArray, left, deltaMemLeft[i][j], deltaLeftPull[i][j]); 
            }
            else
            {
                elog(ERROR, "AGG/Sort/Material: PULL_BATCH should not exist"); 
            }
            break; 

        default:
            elog(ERROR, "DPAssignState unrecognized nodetype: %u", incInfo->type);
            return NULL; 
    }
}


static void
MaterialDP(DPMeta *dpmeta, int i, int j, IncInfo *incInfo)
{
    int **deltaCost = dpmeta->deltaCost;
    int **bdCost    = dpmeta->bdCost;

    int state_mcost = incInfo->memory_cost[LEFT_STATE];
    int state_kcost = incInfo->keep_cost[LEFT_STATE]; 

    int left = incInfo->lefttree->id;

    /* deltaDropCost, deltaKeepCost, bdDropCost, bdKeepCost */
    int cand_cost[NONJOIN_OPTIONS] = {INT_MAX, INT_MAX, INT_MAX, INT_MAX};
    int cand_memleft[NONJOIN_OPTIONS] = {0, 0, 0, 0}; 
    int cand_memright[NONJOIN_OPTIONS] = {0, 0, 0, 0};
    IncState   cand_leftstate[NONJOIN_OPTIONS] =  {STATE_DROP, STATE_KEEPMEM, STATE_DROP, STATE_KEEPMEM};
    IncState   cand_rightstate[NONJOIN_OPTIONS] = {STATE_DROP, STATE_DROP, STATE_DROP, STATE_DROP};
    PullAction cand_leftpull[NONJOIN_OPTIONS] =  {PULL_DELTA, PULL_DELTA, PULL_BATCH_DELTA, PULL_DELTA};
    PullAction cand_rightpull[NONJOIN_OPTIONS] = {PULL_DELTA, PULL_DELTA, PULL_DELTA, PULL_DELTA}; 

    cand_cost[DELTA_DROP] = deltaCost[left][j];
    cand_memleft[DELTA_DROP] = j; 

    cand_cost[BD_DROP]    = bdCost[left][j];
    cand_memleft[BD_DROP] = j; 

    if (j >= state_mcost && incInfo->stateExist[LEFT_STATE])
    {
        cand_cost[BD_KEEP] = deltaCost[left][j - state_mcost] + state_kcost;
        cand_memleft[BD_KEEP] = j - state_mcost; 
    }

    int deltaIndex = cand_cost[DELTA_DROP] <= cand_cost[DELTA_KEEP] ? DELTA_DROP : DELTA_KEEP; 
    int bdIndex    = cand_cost[BD_DROP] <= cand_cost[BD_KEEP] ? BD_DROP : BD_KEEP; 

    SetDPMeta(dpmeta, i, j, 
            cand_cost, cand_memleft, cand_memright, 
            cand_leftstate, cand_rightstate, 
            cand_leftpull, cand_rightpull, 
            deltaIndex, true);

    SetDPMeta(dpmeta, i, j, 
            cand_cost, cand_memleft, cand_memright, 
            cand_leftstate, cand_rightstate, 
            cand_leftpull, cand_rightpull, 
            bdIndex, false);
}


/*
 * This is applied to operator that has no state with a single child 
 * */
static void 
SimpleDropDP(DPMeta *dpmeta, int i, int j, IncInfo *incInfo)
{
    /* No Update */
    dpmeta->deltaCost[i][j] = 0; 
    dpmeta->deltaIncState[i][j][LEFT_STATE] = STATE_DROP; 
    dpmeta->deltaMemLeft[i][j] = j; 
    dpmeta->deltaLeftPull[i][j] = PULL_DELTA; 
    dpmeta->deltaRightPull[i][j] = PULL_DELTA; 
 
    dpmeta->bdCost[i][j] = incInfo->prepare_cost[LEFT_STATE] + incInfo->compute_cost;
    dpmeta->bdIncState[i][j][LEFT_STATE] = STATE_DROP;
    dpmeta->bdMemLeft[i][j] = j;
    dpmeta->bdLeftPull[i][j] = PULL_BATCH_DELTA;
    dpmeta->bdRightPull[i][j] = PULL_BATCH_DELTA; 


    /* Consider Update */
    if (incInfo->leftUpdate) 
    {
        dpmeta->deltaCost[i][j] += DELTA_COST; 
        dpmeta->bdCost[i][j] += DELTA_COST; 
    }

    /* Consider the cost of subtree */
    if (incInfo->lefttree != NULL) /* INC_AGGSORT or INC_MATERIAL */
    {
        int left = incInfo->lefttree->id; 
        dpmeta->bdCost[i][j] += dpmeta->bdCost[left][j]; 

        if (incInfo->leftUpdate) /* TODO: we assume recomputation here for Aggregation here */
        {
            if (incInfo->type == INC_AGGSORT) 
            {
                dpmeta->deltaCost[i][j] += (incInfo->prepare_cost[LEFT_STATE] + incInfo->compute_cost + dpmeta->bdCost[left][j]);
                dpmeta->deltaLeftPull[i][j] = PULL_BATCH_DELTA;
            }
            else
            {
                dpmeta->deltaCost[i][j] += dpmeta->deltaCost[left][j];
                dpmeta->deltaLeftPull[i][j] = PULL_DELTA;
            } 
        }

    }
}

/*
 * The following functions have the same parameters
 * i -- index of the current node
 * j -- memory budget 
 * */
static void 
SortDPHasUpdate (DPMeta *dpmeta, int i, int j, IncInfo *incInfo)
{
    int **deltaCost = dpmeta->deltaCost;
    int **bdCost    = dpmeta->bdCost;

    int state_mcost = incInfo->memory_cost[LEFT_STATE];
    int left = incInfo->lefttree->id;

    /* deltaDropCost, deltaKeepCost, bdDropCost, bdKeepCost */
    int cand_cost[NONJOIN_OPTIONS] = {INT_MAX, INT_MAX, INT_MAX, INT_MAX};
    int cand_memleft[NONJOIN_OPTIONS] = {0, 0, 0, 0}; 
    int cand_memright[NONJOIN_OPTIONS] = {0, 0, 0, 0};
    IncState   cand_leftstate[NONJOIN_OPTIONS] =  {STATE_DROP, STATE_KEEPMEM, STATE_DROP, STATE_KEEPMEM};
    IncState   cand_rightstate[NONJOIN_OPTIONS] = {STATE_DROP, STATE_DROP, STATE_DROP, STATE_DROP};
    PullAction cand_leftpull[NONJOIN_OPTIONS] =  {PULL_DELTA, PULL_DELTA, PULL_BATCH_DELTA, PULL_DELTA};
    PullAction cand_rightpull[NONJOIN_OPTIONS] = {PULL_DELTA, PULL_DELTA, PULL_DELTA, PULL_DELTA}; 

    cand_cost[DELTA_DROP] = DELTA_COST + deltaCost[left][j]; /* Always drop */; 
    cand_memleft[DELTA_DROP] = j; 

    cand_cost[BD_DROP]    = bdCost[left][j] + incInfo->prepare_cost[LEFT_STATE] + incInfo->compute_cost + DELTA_COST;
    cand_memleft[BD_DROP] = j; 

    if (j >= state_mcost && incInfo->stateExist[LEFT_STATE])
    {
        cand_cost[BD_KEEP] = deltaCost[left][j - state_mcost] + DELTA_COST;
        cand_memleft[BD_KEEP] = j - state_mcost; 
    }

    int deltaIndex = cand_cost[DELTA_DROP] <= cand_cost[DELTA_KEEP] ? DELTA_DROP : DELTA_KEEP; 
    int bdIndex    = cand_cost[BD_DROP] <= cand_cost[BD_KEEP] ? BD_DROP : BD_KEEP; 

    SetDPMeta(dpmeta, i, j, 
            cand_cost, cand_memleft, cand_memright, 
            cand_leftstate, cand_rightstate, 
            cand_leftpull, cand_rightpull, 
            deltaIndex, true);

    SetDPMeta(dpmeta, i, j, 
            cand_cost, cand_memleft, cand_memright, 
            cand_leftstate, cand_rightstate, 
            cand_leftpull, cand_rightpull, 
            bdIndex, false);
}

static void 
SortDPNoUpdate (DPMeta *dpmeta, int i, int j, IncInfo *incInfo)
{
    int **deltaCost = dpmeta->deltaCost;
    int **bdCost    = dpmeta->bdCost;

    int state_mcost = incInfo->memory_cost[LEFT_STATE];
    int left = incInfo->lefttree->id;

    /* deltaDropCost, deltaKeepCost, bdDropCost, bdKeepCost */
    int cand_cost[NONJOIN_OPTIONS] = {INT_MAX, INT_MAX, INT_MAX, INT_MAX};
    int cand_memleft[NONJOIN_OPTIONS] = {0, 0, 0, 0}; 
    int cand_memright[NONJOIN_OPTIONS] = {0, 0, 0, 0};
    IncState   cand_leftstate[NONJOIN_OPTIONS] =  {STATE_DROP, STATE_KEEPMEM, STATE_DROP, STATE_KEEPMEM};
    IncState   cand_rightstate[NONJOIN_OPTIONS] = {STATE_DROP, STATE_DROP, STATE_DROP, STATE_DROP};
    PullAction cand_leftpull[NONJOIN_OPTIONS] =  {PULL_DELTA, PULL_DELTA, PULL_BATCH_DELTA, PULL_DELTA};
    PullAction cand_rightpull[NONJOIN_OPTIONS] = {PULL_DELTA, PULL_DELTA, PULL_DELTA, PULL_DELTA}; 

    cand_cost[DELTA_DROP] = 0; /* Always drop */; 
    cand_memleft[DELTA_DROP] = 0; 

    cand_cost[BD_DROP]    = bdCost[left][j] + incInfo->prepare_cost[LEFT_STATE] + incInfo->compute_cost; 
    cand_memleft[BD_DROP] = j; 

    if (j >= state_mcost && incInfo->stateExist[LEFT_STATE])
    {
        cand_cost[BD_KEEP] = 0; 
        cand_memleft[BD_KEEP] = j - state_mcost; 
    }

    int deltaIndex = cand_cost[DELTA_DROP] <= cand_cost[DELTA_KEEP] ? DELTA_DROP : DELTA_KEEP; 
    int bdIndex    = cand_cost[BD_DROP] <= cand_cost[BD_KEEP] ? BD_DROP : BD_KEEP; 

    SetDPMeta(dpmeta, i, j, 
            cand_cost, cand_memleft, cand_memright, 
            cand_leftstate, cand_rightstate, 
            cand_leftpull, cand_rightpull, 
            deltaIndex, true);

    SetDPMeta(dpmeta, i, j, 
            cand_cost, cand_memleft, cand_memright, 
            cand_leftstate, cand_rightstate, 
            cand_leftpull, cand_rightpull, 
            bdIndex, false);
}

static void
AggDPHasUpdate (DPMeta *dpmeta, int i, int j, IncInfo *incInfo)
{
    int **deltaCost = dpmeta->deltaCost;
    int **bdCost    = dpmeta->bdCost;

    int state_mcost = incInfo->memory_cost[LEFT_STATE];
    int state_pcost = incInfo->prepare_cost[LEFT_STATE]; 
    int left = incInfo->lefttree->id;

    /* deltaDropCost, deltaKeepCost, bdDropCost, bdKeepCost */
    int cand_cost[NONJOIN_OPTIONS] = {INT_MAX, INT_MAX, INT_MAX, INT_MAX};
    int cand_memleft[NONJOIN_OPTIONS] = {0, 0, 0, 0}; 
    int cand_memright[NONJOIN_OPTIONS] = {0, 0, 0, 0};
    IncState   cand_leftstate[NONJOIN_OPTIONS] =  {STATE_DROP, STATE_KEEPMEM, STATE_DROP, STATE_KEEPMEM};
    IncState   cand_rightstate[NONJOIN_OPTIONS] = {STATE_DROP, STATE_DROP, STATE_DROP, STATE_DROP};
    PullAction cand_leftpull[NONJOIN_OPTIONS] =  {PULL_BATCH_DELTA, PULL_DELTA, PULL_BATCH_DELTA, PULL_DELTA};
    PullAction cand_rightpull[NONJOIN_OPTIONS] = {PULL_DELTA, PULL_DELTA, PULL_DELTA, PULL_DELTA}; 

    cand_cost[DELTA_DROP] = bdCost[left][j] + state_pcost + DELTA_COST; 
    cand_memleft[DELTA_DROP] = j; 

    cand_cost[BD_DROP]    = cand_cost[DELTA_DROP] + incInfo->compute_cost; 
    cand_memleft[BD_DROP] = j; 

    if (j >= state_mcost && incInfo->stateExist[LEFT_STATE])
    {
        cand_cost[DELTA_KEEP] = deltaCost[left][j - state_mcost] + DELTA_COST; 
        cand_cost[BD_KEEP] = cand_cost[DELTA_KEEP] + incInfo->compute_cost; 
        cand_memleft[DELTA_KEEP] = j - state_mcost;
        cand_memleft[BD_KEEP]    = cand_memleft[DELTA_KEEP]; 
    }

    int deltaIndex = cand_cost[DELTA_DROP] <= cand_cost[DELTA_KEEP] ? DELTA_DROP : DELTA_KEEP; 
    int bdIndex    = cand_cost[BD_DROP] <= cand_cost[BD_KEEP] ? BD_DROP : BD_KEEP; 

    SetDPMeta(dpmeta, i, j, 
            cand_cost, cand_memleft, cand_memright, 
            cand_leftstate, cand_rightstate, 
            cand_leftpull, cand_rightpull, 
            deltaIndex, true);

    SetDPMeta(dpmeta, i, j, 
            cand_cost, cand_memleft, cand_memright, 
            cand_leftstate, cand_rightstate, 
            cand_leftpull, cand_rightpull, 
            bdIndex, false);
}

static void
AggDPNoUpdate (DPMeta *dpmeta, int i, int j, IncInfo *incInfo)
{
    int **deltaCost = dpmeta->deltaCost;
    int **bdCost    = dpmeta->bdCost;

    int state_mcost = incInfo->memory_cost[LEFT_STATE];
    int state_pcost = incInfo->prepare_cost[LEFT_STATE]; 
    int left = incInfo->lefttree->id;

    /* deltaDropCost, deltaKeepCost, bdDropCost, bdKeepCost */
    int cand_cost[NONJOIN_OPTIONS] = {INT_MAX, INT_MAX, INT_MAX, INT_MAX};
    int cand_memleft[NONJOIN_OPTIONS] = {0, 0, 0, 0}; 
    int cand_memright[NONJOIN_OPTIONS] = {0, 0, 0, 0};
    IncState   cand_leftstate[NONJOIN_OPTIONS] =  {STATE_DROP, STATE_KEEPMEM, STATE_DROP, STATE_KEEPMEM};
    IncState   cand_rightstate[NONJOIN_OPTIONS] = {STATE_DROP, STATE_DROP, STATE_DROP, STATE_DROP};
    PullAction cand_leftpull[NONJOIN_OPTIONS] =  {PULL_DELTA, PULL_DELTA, PULL_BATCH_DELTA, PULL_DELTA};
    PullAction cand_rightpull[NONJOIN_OPTIONS] = {PULL_DELTA, PULL_DELTA, PULL_DELTA, PULL_DELTA}; 

    cand_cost[DELTA_DROP] = 0; 
    cand_memleft[DELTA_DROP] = 0;  

    cand_cost[BD_DROP]    = bdCost[left][j] + state_pcost + incInfo->compute_cost; 
    cand_memleft[BD_DROP] = j; 

    if (j >= state_mcost && incInfo->stateExist[LEFT_STATE])
    {
        cand_cost[BD_KEEP]    = incInfo->compute_cost; 
        cand_memleft[BD_KEEP] = j - state_mcost; 
    }

    int deltaIndex = cand_cost[DELTA_DROP] <= cand_cost[DELTA_KEEP] ? DELTA_DROP : DELTA_KEEP; 
    int bdIndex    = cand_cost[BD_DROP] <= cand_cost[BD_KEEP] ? BD_DROP : BD_KEEP; 

    SetDPMeta(dpmeta, i, j, 
            cand_cost, cand_memleft, cand_memright, 
            cand_leftstate, cand_rightstate, 
            cand_leftpull, cand_rightpull, 
            deltaIndex, true);

    SetDPMeta(dpmeta, i, j, 
            cand_cost, cand_memleft, cand_memright, 
            cand_leftstate, cand_rightstate, 
            cand_leftpull, cand_rightpull, 
            bdIndex, false);
}


static void
NestLoopDP (DPMeta *dpmeta, int i, int j, IncInfo *incInfo)
{
    int       **deltaCost = dpmeta->deltaCost; 
    int       **bdCost = dpmeta->bdCost;  

    int left = incInfo->lefttree->id;
    int right = incInfo->righttree->id; 
    int state_pcost = incInfo->prepare_cost[RIGHT_STATE];

    int left_mcost = incInfo->memory_cost[LEFT_STATE]; 

    /* deltaDropBothCost, deltaKeepLeftCost, deltaKeepRightCost, deltaKeepBothCost, bdDropBothCost, bdKeepRightCost */
    /* We only need to consider
     *  deltaDropBothCost, 
     *  deltaKeepLeftCost, 
     *  bdDropBothCost
     * */
    int cand_cost[JOIN_OPTIONS] = {INT_MAX, INT_MAX, INT_MAX, INT_MAX, INT_MAX, INT_MAX};
    int cand_memleft[JOIN_OPTIONS] = {0, 0, 0, 0, 0, 0}; 
    int cand_memright[JOIN_OPTIONS] = {0, 0, 0, 0, 0, 0};
    IncState   cand_leftstate[JOIN_OPTIONS] =  {STATE_DROP, STATE_KEEPMEM, STATE_DROP, STATE_DROP, STATE_DROP, STATE_DROP};
    IncState   cand_rightstate[JOIN_OPTIONS] = {STATE_DROP, STATE_DROP, STATE_DROP, STATE_DROP, STATE_DROP, STATE_DROP};
    PullAction cand_leftpull[JOIN_OPTIONS] =  {PULL_DELTA, PULL_DELTA, PULL_DELTA, PULL_DELTA, PULL_BATCH_DELTA, PULL_DELTA};
    PullAction cand_rightpull[JOIN_OPTIONS] = {PULL_DELTA, PULL_DELTA, PULL_DELTA, PULL_DELTA, PULL_BATCH_DELTA, PULL_DELTA}; 
    
    /* First, compute dropCost */
    int tempCost;
    int leftMem; 
    int rightMem;  
    int k; 
    for (k = 0; k <= j; k++)
    {
        leftMem = k;
        rightMem = j - k;

        if (use_sym_hashjoin && incInfo->leftUpdate && incInfo->rightUpdate && incInfo->stateExist[LEFT_STATE])
        {
            if (j >= left_mcost && k <= j - left_mcost)
            {
                tempCost = deltaCost[left][leftMem] + incInfo->keep_cost[LEFT_STATE] + bdCost[right][j - k - left_mcost] + \
                           incInfo->delta_cost[LEFT_STATE];
                if (tempCost < cand_cost[DELTA_KEEPLEFT])
                    SetCostInfo(cand_cost, cand_memleft, cand_memright, tempCost, leftMem, j - k - left_mcost, DELTA_KEEPLEFT); 
            }
        }

        if (incInfo->leftUpdate && !incInfo->rightUpdate)
            tempCost = deltaCost[left][leftMem] + bdCost[right][rightMem] + state_pcost + incInfo->delta_cost[RIGHT_STATE]; 
        else if (!incInfo->leftUpdate && incInfo->rightUpdate) 
            tempCost = bdCost[left][leftMem] + deltaCost[right][rightMem] + state_pcost + incInfo->delta_cost[RIGHT_STATE]; 
        else if (incInfo->leftUpdate && incInfo->rightUpdate) /* both sides have updates */
            tempCost = bdCost[left][leftMem] + bdCost[right][rightMem] + state_pcost + incInfo->delta_cost[RIGHT_STATE]; 
        else /* else: no update, do not allocate memory */
        {
            tempCost = 0;
            leftMem = 0;
            rightMem = 0; 
        }

        if (tempCost < cand_cost[DELTA_DROPBOTH])
            SetCostInfo(cand_cost, cand_memleft, cand_memright, tempCost, leftMem, rightMem, DELTA_DROPBOTH); 

        leftMem = k;
        rightMem = j - k;

        if (incInfo->leftUpdate || incInfo->rightUpdate)
            tempCost = bdCost[left][leftMem] + bdCost[right][rightMem] + state_pcost + incInfo->compute_cost + incInfo->delta_cost[RIGHT_STATE];
        else
            tempCost = bdCost[left][leftMem] + bdCost[right][rightMem] + state_pcost + incInfo->compute_cost;

        if (tempCost < cand_cost[BD_DROPBOTH])
            SetCostInfo(cand_cost, cand_memleft, cand_memright, tempCost, leftMem, rightMem, BD_DROPBOTH); 
    }

    if (use_sym_hashjoin && incInfo->stateExist[LEFT_STATE] && !incInfo->leftUpdate && incInfo->rightUpdate && j >= left_mcost)
    {
        cand_cost[DELTA_KEEPLEFT] = incInfo->keep_cost[LEFT_STATE] + deltaCost[right][j - left_mcost] + incInfo->delta_cost[LEFT_STATE]; 
        cand_memright[DELTA_KEEPLEFT] = j - left_mcost; 
    }

    if(incInfo->leftUpdate && !incInfo->rightUpdate) 
        cand_rightpull[DELTA_DROPBOTH]= PULL_BATCH_DELTA;
    else if (!incInfo->leftUpdate && incInfo->rightUpdate)
        cand_leftpull[DELTA_DROPBOTH] = PULL_BATCH_DELTA; 
    else if (incInfo->leftUpdate && incInfo->rightUpdate)
    {
        cand_rightpull[DELTA_DROPBOTH] = PULL_BATCH_DELTA;
        cand_leftpull[DELTA_DROPBOTH] = PULL_BATCH_DELTA; 

        cand_rightpull[DELTA_KEEPLEFT] = PULL_BATCH_DELTA; 
    }

    int delta_index = cand_cost[DELTA_DROPBOTH] <= cand_cost[DELTA_KEEPLEFT] ? DELTA_DROPBOTH : DELTA_KEEPLEFT; 

    SetDPMeta(dpmeta, i, j, 
            cand_cost, cand_memleft, cand_memright, 
            cand_leftstate, cand_rightstate, 
            cand_leftpull, cand_rightpull, 
            delta_index, true);

    SetDPMeta(dpmeta, i, j, 
            cand_cost, cand_memleft, cand_memright, 
            cand_leftstate, cand_rightstate, 
            cand_leftpull, cand_rightpull, 
            BD_DROPBOTH, false);
}

static void
HashJoinDPLeftUpdate (DPMeta *dpmeta, int i, int j, IncInfo *incInfo)
{
    int       **deltaCost = dpmeta->deltaCost; 
    int       **bdCost = dpmeta->bdCost;  

    int left = incInfo->lefttree->id;
    int right = incInfo->righttree->id; 
    int state_mcost = incInfo->memory_cost[RIGHT_STATE]; 

    /* deltaDropBothCost, deltaKeepLeftCost, deltaKeepRightCost, deltaKeepBothCost, bdDropBothCost, bdKeepRightCost */
    int cand_cost[JOIN_OPTIONS] = {INT_MAX, INT_MAX, INT_MAX, INT_MAX, INT_MAX, INT_MAX};
    int cand_memleft[JOIN_OPTIONS] = {0, 0, 0, 0, 0, 0}; 
    int cand_memright[JOIN_OPTIONS] = {0, 0, 0, 0, 0, 0};
    IncState   cand_leftstate[JOIN_OPTIONS] =  {STATE_DROP, STATE_DROP, STATE_DROP, STATE_DROP, STATE_DROP, STATE_DROP};
    IncState   cand_rightstate[JOIN_OPTIONS] = {STATE_DROP, STATE_DROP, STATE_KEEPMEM, STATE_DROP, STATE_DROP, STATE_KEEPMEM};
    PullAction cand_leftpull[JOIN_OPTIONS] =  {PULL_DELTA, PULL_DELTA, PULL_DELTA, PULL_DELTA, PULL_BATCH_DELTA, PULL_BATCH_DELTA};
    PullAction cand_rightpull[JOIN_OPTIONS] = {PULL_BATCH_DELTA, PULL_DELTA, PULL_DELTA, PULL_DELTA, PULL_BATCH_DELTA, PULL_DELTA}; 
    
    /* First, compute dropCost */
    int tempCost;
    int leftMem; 
    int rightMem;  
    int k; 
    for (k = 0; k <= j; k++)
    {
        leftMem = k;
        rightMem = j - k; 

        /* deltaDropBothCost */
        tempCost = deltaCost[left][leftMem] + bdCost[right][rightMem] + incInfo->prepare_cost[RIGHT_STATE] + incInfo->delta_cost[RIGHT_STATE]; 
        if (tempCost < cand_cost[DELTA_DROPBOTH])
            SetCostInfo(cand_cost, cand_memleft, cand_memright, tempCost, leftMem, rightMem, DELTA_DROPBOTH); 

        /* bdDropBothCost */
        tempCost = bdCost[left][leftMem] + bdCost[right][rightMem] + incInfo->prepare_cost[RIGHT_STATE] \
            + incInfo->compute_cost + incInfo->delta_cost[RIGHT_STATE];
        if (tempCost < cand_cost[BD_DROPBOTH])
            SetCostInfo(cand_cost, cand_memleft, cand_memright, tempCost, leftMem, rightMem, BD_DROPBOTH); 
    }
   
    if (j >= state_mcost && incInfo->stateExist[RIGHT_STATE] )
    {
        cand_cost[DELTA_KEEPRIGHT] = deltaCost[left][j - state_mcost] + incInfo->delta_cost[RIGHT_STATE];
        cand_memleft[DELTA_KEEPRIGHT] = j - state_mcost; 

        cand_cost[BD_KEEPRIGHT] = bdCost[left][j - state_mcost] + incInfo->compute_cost + incInfo->delta_cost[RIGHT_STATE]; 
        cand_memleft[BD_KEEPRIGHT] = j - state_mcost; 
    }

    int deltaIndex = cand_cost[DELTA_DROPBOTH] <= cand_cost[DELTA_KEEPRIGHT] ? DELTA_DROPBOTH : DELTA_KEEPRIGHT;

    SetDPMeta(dpmeta, i, j, 
            cand_cost, cand_memleft, cand_memright, 
            cand_leftstate, cand_rightstate, 
            cand_leftpull, cand_rightpull, 
            deltaIndex, true);

    int bdIndex = cand_cost[BD_DROPBOTH] <= cand_cost[BD_KEEPRIGHT] ? BD_DROPBOTH : BD_KEEPRIGHT;
    SetDPMeta(dpmeta, i, j, 
            cand_cost, cand_memleft, cand_memright, 
            cand_leftstate, cand_rightstate, 
            cand_leftpull, cand_rightpull, 
            bdIndex, false);
}

static void
HashJoinDPRightUpdate (DPMeta *dpmeta, int i, int j, IncInfo *incInfo)
{
    int       **deltaCost = dpmeta->deltaCost; 
    int       **bdCost = dpmeta->bdCost;  

    int left = incInfo->lefttree->id;
    int right = incInfo->righttree->id; 

    int left_mcost = incInfo->memory_cost[LEFT_STATE]; 
    int right_mcost = incInfo->memory_cost[RIGHT_STATE];

    /* deltaDropBothCost, deltaKeepLeftCost, deltaKeepRightCost, deltaKeepBothCost, bdDropBothCost, bdKeepRightCost */
    /* We only need to consider 
     *  deltaDropBoth (1)
     *  deltaKeepLeft (2)
     *  bdDropBoth    (5)
     *  bdKeepRight   (6)
     * */
    int cand_cost[JOIN_OPTIONS] = {INT_MAX, INT_MAX, INT_MAX, INT_MAX, INT_MAX, INT_MAX};
    int cand_memleft[JOIN_OPTIONS] = {0, 0, 0, 0, 0, 0}; 
    int cand_memright[JOIN_OPTIONS] = {0, 0, 0, 0, 0, 0};
    IncState   cand_leftstate[JOIN_OPTIONS] =  {STATE_DROP, STATE_KEEPMEM, STATE_DROP, STATE_DROP, STATE_DROP, STATE_DROP};
    IncState   cand_rightstate[JOIN_OPTIONS] = {STATE_DROP, STATE_DROP, STATE_DROP, STATE_DROP, STATE_DROP, STATE_KEEPMEM};
    PullAction cand_leftpull[JOIN_OPTIONS] =  {PULL_BATCH_DELTA, PULL_DELTA, PULL_DELTA, PULL_DELTA, PULL_BATCH_DELTA, PULL_BATCH_DELTA};
    PullAction cand_rightpull[JOIN_OPTIONS] = {PULL_DELTA, PULL_DELTA, PULL_DELTA, PULL_DELTA, PULL_BATCH_DELTA, PULL_DELTA}; 
    
    int tempCost;
    int leftMem; 
    int rightMem;  
    int k; 
    for (k = 0; k <= j; k++)
    {
        leftMem = k;
        rightMem = j - k; 

        /* deltaDropBothCost */
        tempCost = bdCost[left][leftMem] + deltaCost[right][rightMem] + incInfo->delta_cost[RIGHT_STATE];  
        if (tempCost < cand_cost[DELTA_DROPBOTH])
            SetCostInfo(cand_cost, cand_memleft, cand_memright, tempCost, leftMem, rightMem, DELTA_DROPBOTH); 

        /* bdDropBothCost */
        tempCost = bdCost[left][leftMem] + bdCost[right][rightMem] + incInfo->prepare_cost[RIGHT_STATE] \
            + incInfo->compute_cost + incInfo->delta_cost[RIGHT_STATE];
        if (tempCost < cand_cost[BD_DROPBOTH])
            SetCostInfo(cand_cost, cand_memleft, cand_memright, tempCost, leftMem, rightMem, BD_DROPBOTH); 

        /* bdKeepRightCost */
        if (j >= right_mcost && k <= j - right_mcost && incInfo->stateExist[RIGHT_STATE])
        {
            rightMem = j - right_mcost - k; 
            tempCost = bdCost[left][leftMem] + deltaCost[right][rightMem] + incInfo->compute_cost + incInfo->delta_cost[RIGHT_STATE]; 
            if (tempCost < cand_cost[BD_KEEPRIGHT])
                SetCostInfo(cand_cost, cand_memleft, cand_memright, tempCost, leftMem, rightMem, BD_KEEPRIGHT); 
        }

    }

    /* deltaKeepLeftCost */
    if (use_sym_hashjoin && j >= left_mcost && incInfo->stateExist[LEFT_STATE])
    {
        cand_cost[DELTA_KEEPLEFT] = incInfo->keep_cost[LEFT_STATE] + deltaCost[right][j - left_mcost] + incInfo->delta_cost[LEFT_STATE]; 
        cand_memleft[DELTA_KEEPLEFT] = 0; 
        cand_memright[DELTA_KEEPLEFT] = j - left_mcost; 
    }

    int deltaIndex =  cand_cost[DELTA_DROPBOTH] <= cand_cost[DELTA_KEEPLEFT] ? DELTA_DROPBOTH : DELTA_KEEPLEFT; 
    SetDPMeta(dpmeta, i, j, 
            cand_cost, cand_memleft, cand_memright, 
            cand_leftstate, cand_rightstate, 
            cand_leftpull, cand_rightpull, 
            deltaIndex, true);

    int bdIndex = cand_cost[BD_DROPBOTH] <= cand_cost[BD_KEEPRIGHT] ? BD_DROPBOTH : BD_KEEPRIGHT;
    SetDPMeta(dpmeta, i, j, 
            cand_cost, cand_memleft, cand_memright, 
            cand_leftstate, cand_rightstate, 
            cand_leftpull, cand_rightpull, 
            bdIndex, false);
}

static void
HashJoinDPNoUpdate (DPMeta *dpmeta, int i, int j, IncInfo *incInfo)
{

    int       **deltaCost = dpmeta->deltaCost; 
    int       **bdCost = dpmeta->bdCost;  

    int left = incInfo->lefttree->id;
    int right = incInfo->righttree->id; 

    int right_mcost = incInfo->memory_cost[RIGHT_STATE]; 

    /* deltaDropBothCost, deltaKeepLeftCost, deltaKeepRightCost, deltaKeepBothCost, bdDropBothCost, bdKeepRightCost */
    /* We only need to consider 
     *  deltaDropBoth (1)
     *  bdDropBoth    (5)
     *  bdKeepRight   (6)
     * */
    int cand_cost[JOIN_OPTIONS] = {INT_MAX, INT_MAX, INT_MAX, INT_MAX, INT_MAX, INT_MAX};
    int cand_memleft[JOIN_OPTIONS] = {0, 0, 0, 0, 0, 0}; 
    int cand_memright[JOIN_OPTIONS] = {0, 0, 0, 0, 0, 0};
    IncState   cand_leftstate[JOIN_OPTIONS] =  {STATE_DROP, STATE_DROP, STATE_DROP, STATE_DROP, STATE_DROP, STATE_DROP};
    IncState   cand_rightstate[JOIN_OPTIONS] = {STATE_DROP, STATE_DROP, STATE_DROP, STATE_DROP, STATE_DROP, STATE_KEEPMEM};
    PullAction cand_leftpull[JOIN_OPTIONS] =  {PULL_DELTA, PULL_DELTA, PULL_DELTA, PULL_DELTA, PULL_BATCH_DELTA, PULL_BATCH_DELTA};
    PullAction cand_rightpull[JOIN_OPTIONS] = {PULL_DELTA, PULL_DELTA, PULL_DELTA, PULL_DELTA, PULL_BATCH_DELTA, PULL_DELTA}; 
    
    int tempCost;
    int leftMem; 
    int rightMem;  
    int k; 
    for (k = 0; k <= j; k++)
    {
        leftMem = k;
        rightMem = j - k; 

        /* bdDropBothCost */
        tempCost = bdCost[left][leftMem] + bdCost[right][rightMem] + incInfo->prepare_cost[RIGHT_STATE] + incInfo->compute_cost; 
        if (tempCost < cand_cost[BD_DROPBOTH])
            SetCostInfo(cand_cost, cand_memleft, cand_memright, tempCost, leftMem, rightMem, BD_DROPBOTH); 
    }

    /* bdKeepRightCost */
    if (j >= right_mcost && incInfo->stateExist[RIGHT_STATE])
    {
        cand_cost[BD_KEEPRIGHT] = bdCost[left][j - right_mcost] + incInfo->compute_cost; 
        cand_memleft[BD_KEEPRIGHT] = j - right_mcost;
        cand_memright[BD_KEEPRIGHT] = 0;  
    }

    /* deltaDropBothCost */
    cand_cost[DELTA_DROPBOTH] = 0;

    SetDPMeta(dpmeta, i, j, 
            cand_cost, cand_memleft, cand_memright, 
            cand_leftstate, cand_rightstate, 
            cand_leftpull, cand_rightpull, 
            DELTA_DROPBOTH, true);

    int bdIndex = cand_cost[BD_DROPBOTH] <= cand_cost[BD_KEEPRIGHT] ? BD_DROPBOTH : BD_KEEPRIGHT;
    SetDPMeta(dpmeta, i, j, 
            cand_cost, cand_memleft, cand_memright, 
            cand_leftstate, cand_rightstate, 
            cand_leftpull, cand_rightpull, 
            bdIndex, false);
}

static void
HashJoinDPBothUpdate (DPMeta *dpmeta, int i, int j, IncInfo *incInfo)
{
    int       **deltaCost = dpmeta->deltaCost; 
    int       **bdCost = dpmeta->bdCost;  

    int left = incInfo->lefttree->id;
    int right = incInfo->righttree->id; 

    int left_mcost = incInfo->memory_cost[LEFT_STATE]; 
    int right_mcost = incInfo->memory_cost[RIGHT_STATE]; 
    
    /* deltaDropBothCost, deltaKeepLeftCost, deltaKeepRightCost, deltaKeepBothCost, bdDropBothCost, bdKeepRightCost */
    int cand_cost[JOIN_OPTIONS] = {INT_MAX, INT_MAX, INT_MAX, INT_MAX, INT_MAX, INT_MAX};
    int cand_memleft[JOIN_OPTIONS] = {0, 0, 0, 0, 0, 0}; 
    int cand_memright[JOIN_OPTIONS] = {0, 0, 0, 0, 0, 0};
    IncState   cand_leftstate[JOIN_OPTIONS] =  {STATE_DROP, STATE_KEEPMEM, STATE_DROP, STATE_KEEPMEM, STATE_DROP, STATE_DROP};
    IncState   cand_rightstate[JOIN_OPTIONS] = {STATE_DROP, STATE_DROP, STATE_KEEPMEM, STATE_KEEPMEM, STATE_DROP, STATE_KEEPMEM};
    PullAction cand_leftpull[JOIN_OPTIONS] =  {PULL_BATCH_DELTA, PULL_DELTA, PULL_BATCH_DELTA, PULL_DELTA, PULL_BATCH_DELTA, PULL_BATCH_DELTA};
    PullAction cand_rightpull[JOIN_OPTIONS] = {PULL_BATCH_DELTA, PULL_BATCH_DELTA, PULL_DELTA, PULL_DELTA, PULL_BATCH_DELTA, PULL_DELTA}; 

    
    /* First, compute dropCost */
    int tempCost;
    int leftMem; 
    int rightMem;  
    int k; 
    for (k = 0; k <= j; k++)
    {
        leftMem = k;
        rightMem = j - k; 

        /* deltaDropBothCost */
        tempCost = bdCost[left][leftMem] + bdCost[right][rightMem] + incInfo->prepare_cost[RIGHT_STATE] + incInfo->delta_cost[RIGHT_STATE]; 
        if (tempCost < cand_cost[DELTA_DROPBOTH])
            SetCostInfo(cand_cost, cand_memleft, cand_memright, tempCost, leftMem, rightMem, DELTA_DROPBOTH); 

        /* bdDropBothCost */
        tempCost = bdCost[left][leftMem] + bdCost[right][rightMem] + incInfo->prepare_cost[RIGHT_STATE] \
            + incInfo->compute_cost + incInfo->delta_cost[RIGHT_STATE];
        if (tempCost < cand_cost[BD_DROPBOTH])
            SetCostInfo(cand_cost, cand_memleft, cand_memright, tempCost, leftMem, rightMem, BD_DROPBOTH); 

        /* deltaKeepLeftCost */
        if (use_sym_hashjoin && j >= left_mcost && k <= j - left_mcost && incInfo->stateExist[LEFT_STATE])
        {
            rightMem = j - left_mcost - k; 
            tempCost = deltaCost[left][leftMem] + incInfo->keep_cost[LEFT_STATE] + bdCost[right][rightMem] + \ 
                incInfo->prepare_cost[RIGHT_STATE] + incInfo->delta_cost[LEFT_STATE];
            if (tempCost < cand_cost[DELTA_KEEPLEFT])
                SetCostInfo(cand_cost, cand_memleft, cand_memright, tempCost, leftMem, rightMem, DELTA_KEEPLEFT); 
        }

        if (j >= right_mcost && k <= j - right_mcost && incInfo->stateExist[RIGHT_STATE])
        {
            /* deltaKeepRightCost */
            rightMem = j - right_mcost - k; 
            tempCost = bdCost[left][leftMem] + deltaCost[right][rightMem] + incInfo->delta_cost[RIGHT_STATE];
            if (tempCost < cand_cost[DELTA_KEEPRIGHT])
                SetCostInfo(cand_cost, cand_memleft, cand_memright, tempCost, leftMem, rightMem, DELTA_KEEPRIGHT); 
    
            /* bdKeepRightCost */
            tempCost = bdCost[left][leftMem] + deltaCost[right][rightMem] + incInfo->compute_cost + incInfo->delta_cost[RIGHT_STATE]; 
            if (tempCost < cand_cost[BD_KEEPRIGHT])
                SetCostInfo(cand_cost, cand_memleft, cand_memright, tempCost, leftMem, rightMem, BD_KEEPRIGHT); 
        }

        /* deltaKeepBothCost */
        if (incInfo->stateExist[LEFT_STATE] && incInfo->stateExist[RIGHT_STATE] && use_sym_hashjoin \
                && j >= (left_mcost + right_mcost) && k <= j - (left_mcost + right_mcost))
        {
            rightMem = j - left_mcost - right_mcost; 
            tempCost = deltaCost[left][leftMem] + incInfo->keep_cost[LEFT_STATE] + deltaCost[right][rightMem] + incInfo->delta_cost[LEFT_STATE];
            if (tempCost < cand_cost[DELTA_KEEPBOTH])
                SetCostInfo(cand_cost, cand_memleft, cand_memright, tempCost, leftMem, rightMem, DELTA_KEEPBOTH); 
        }
    }

    int deltaIndex, bdIndex;
    int minCost = INT_MAX; 
    for (k = DELTA_DROPBOTH; k <= DELTA_KEEPBOTH; k++)
    {
        if (cand_cost[k] < minCost)
        {
            minCost = cand_cost[k];
            deltaIndex = k; 
        }
    }
    SetDPMeta(dpmeta, i, j, 
            cand_cost, cand_memleft, cand_memright, 
            cand_leftstate, cand_rightstate, 
            cand_leftpull, cand_rightpull, 
            deltaIndex, true);

    bdIndex = cand_cost[BD_DROPBOTH] <= cand_cost[BD_KEEPRIGHT] ? BD_DROPBOTH : BD_KEEPRIGHT;
    SetDPMeta(dpmeta, i, j, 
            cand_cost, cand_memleft, cand_memright, 
            cand_leftstate, cand_rightstate, 
            cand_leftpull, cand_rightpull, 
            bdIndex, false);
}


/* Helper functions for Greedy Algorithms */

typedef struct GreedyMem
{
    IncInfo *incInfo;
    bool    left;
    int     memory_size;
}GreedyMem; 

static GreedyMem **BuildGreedyMem(IncInfo **incInfo_array, int numIncInfo)
{
    GreedyMem **memArray = (GreedyMem **)palloc(sizeof(GreedyMem *) * (numIncInfo * MAX_STATE)); 
    for (int i = 0; i < numIncInfo * MAX_STATE; i++)
    {
        memArray[i] = (GreedyMem *)palloc(sizeof(GreedyMem)); 

        int j = i / MAX_STATE;
        bool left = (i % MAX_STATE == 0);

        memArray[i]->incInfo = incInfo_array[j]; 
        memArray[i]->left = left;
        if (left)
            memArray[i]->memory_size = incInfo_array[j]->memory_cost[LEFT_STATE];
        else
            memArray[i]->memory_size = incInfo_array[j]->memory_cost[RIGHT_STATE];
    }

    return memArray;
}

static void DestroyGreedyMem(GreedyMem **memArray, int numGreedyMem)
{
    for (int i = 0; i < numGreedyMem; i++)
    {
        pfree(memArray[i]); 
    }

    pfree(memArray); 
}

static int MemComparator(const void * a, const void *b)
{
    GreedyMem *a_GM = *(GreedyMem **)a;
    GreedyMem *b_GM = *(GreedyMem **)b;

    int a_memory = a_GM->memory_size, b_memory = b_GM->memory_size;

    return (a_memory - b_memory); 
}

void 
ExecGreedySolution(IncInfo **incInfo_array, int numIncInfo, int incMemory, DecisionMethod dm)
{
    GreedyMem **GM_array = BuildGreedyMem(incInfo_array, numIncInfo);
    int numGM = numIncInfo * MAX_STATE; 

    if (dm == DM_MEMSMALLFIRST || dm == DM_MEMBIGFIRST)
            qsort((void *)GM_array, (size_t)numGM, sizeof(GreedyMem *), MemComparator);


    if (dm == DM_TOPDOWN || dm == DM_MEMBIGFIRST)
    {
        for (int i = numGM - 1; i >=0; i--)
        {
            GreedyMem *GM = GM_array[i];
            int tmp_memory = GM->memory_size;
            if (tmp_memory != 0 && tmp_memory < incMemory)
            {
                if (GM->left)
                    GM->incInfo->incState[LEFT_STATE] =  STATE_KEEPMEM;
                else
                    GM->incInfo->incState[RIGHT_STATE] =  STATE_KEEPMEM;

                incMemory -= tmp_memory; 
            }
        }
    }
    else
    {
        for (int i = 0; i < numGM; i++)
        {
            GreedyMem *GM = GM_array[i];
            int tmp_memory = GM->memory_size;
            if (tmp_memory != 0 && tmp_memory < incMemory)
            {
                if (GM->left)
                    GM->incInfo->incState[LEFT_STATE] =  STATE_KEEPMEM;
                else
                    GM->incInfo->incState[RIGHT_STATE] =  STATE_KEEPMEM;

                incMemory -= tmp_memory; 
            }
        }
    }

    DestroyGreedyMem(GM_array, numGM);
}

static int 
ExecBFCalCost(IncInfo *root, PullAction pullAction)
{
    int cost = 0;
    switch(root->type)
    {
        case INC_HASHJOIN:
            if (root->leftUpdate && !root->rightUpdate)
            {
                if (pullAction == PULL_DELTA)
                {
                    if (root->incState[RIGHT_STATE] == STATE_KEEPMEM)
                        cost += ExecBFCalCost(root->lefttree, PULL_DELTA) + root->delta_cost[RIGHT_STATE];
                    else
                        cost += ExecBFCalCost(root->lefttree, PULL_DELTA) + ExecBFCalCost(root->righttree, PULL_BATCH_DELTA) + \ 
                            root->prepare_cost[RIGHT_STATE] + root->delta_cost[RIGHT_STATE];
                }
                else
                {
                    if (root->incState[RIGHT_STATE] == STATE_KEEPMEM)
                        cost += ExecBFCalCost(root->lefttree, PULL_BATCH_DELTA) + \ 
                            root->compute_cost + root->delta_cost[RIGHT_STATE];
                    else
                        cost += ExecBFCalCost(root->lefttree, PULL_BATCH_DELTA) + ExecBFCalCost(root->righttree, PULL_BATCH_DELTA) + \ 
                            root->prepare_cost[RIGHT_STATE] + root->compute_cost + root->delta_cost[RIGHT_STATE];
                }

                if (root->incState[LEFT_STATE] == STATE_KEEPMEM)
                    cost += root->keep_cost[LEFT_STATE]; 
            }
            else if (!root->leftUpdate && root->rightUpdate)
            {
                if (pullAction == PULL_DELTA)
                {
                    if (root->incState[LEFT_STATE] == STATE_KEEPMEM)
                        cost += root->keep_cost[LEFT_STATE] + ExecBFCalCost(root->righttree, PULL_DELTA) + root->delta_cost[LEFT_STATE];
                    else
                        cost += ExecBFCalCost(root->lefttree, PULL_BATCH_DELTA) + ExecBFCalCost(root->righttree, PULL_DELTA) \
                                + root->delta_cost[LEFT_STATE]; 
                }
                else
                {
                    if (root->incState[RIGHT_STATE] == STATE_KEEPMEM)
                        cost += ExecBFCalCost(root->lefttree, PULL_BATCH_DELTA) + ExecBFCalCost(root->righttree, PULL_DELTA) + root->compute_cost \ 
                            + root->delta_cost[RIGHT_STATE];
                    else
                        cost += ExecBFCalCost(root->lefttree, PULL_BATCH_DELTA) + ExecBFCalCost(root->righttree, PULL_DELTA) + root->compute_cost \ 
                            + root->delta_cost[RIGHT_STATE] + root->prepare_cost[RIGHT_STATE];

                    if (root->incState[LEFT_STATE] == STATE_KEEPMEM)
                        cost += root->keep_cost[LEFT_STATE]; 
                }
            }
            else if (root->leftUpdate && root->rightUpdate)
            {
                if (pullAction == PULL_DELTA)
                {
                    if (root->incState[LEFT_STATE] == STATE_DROP && root->incState[RIGHT_STATE] == STATE_DROP)
                        cost += ExecBFCalCost(root->lefttree, PULL_BATCH_DELTA) + ExecBFCalCost(root->righttree, PULL_BATCH_DELTA) \ 
                            + root->prepare_cost[RIGHT_STATE] + root->delta_cost[RIGHT_STATE]; 
                    else if (root->incState[LEFT_STATE] == STATE_DROP && root->incState[RIGHT_STATE] == STATE_KEEPMEM)
                        cost += ExecBFCalCost(root->lefttree, PULL_BATCH_DELTA) + ExecBFCalCost(root->righttree, PULL_DELTA) \
                                + root->delta_cost[RIGHT_STATE];
                    else if (root->incState[LEFT_STATE] == STATE_KEEPMEM && root->incState[RIGHT_STATE] == STATE_DROP)
                        cost += ExecBFCalCost(root->lefttree, PULL_DELTA) + ExecBFCalCost(root->righttree, PULL_BATCH_DELTA) \
                                + root->keep_cost[LEFT_STATE] + root->prepare_cost[RIGHT_STATE] + root->delta_cost[LEFT_STATE]; 
                    else
                        cost += ExecBFCalCost(root->lefttree, PULL_DELTA) + ExecBFCalCost(root->righttree, PULL_DELTA) \ 
                            + root->keep_cost[LEFT_STATE] + root->delta_cost[LEFT_STATE]; 
                }
                else
                {
                    if (root->incState[RIGHT_STATE] == STATE_DROP)
                        cost += ExecBFCalCost(root->lefttree, PULL_BATCH_DELTA) + ExecBFCalCost(root->righttree, PULL_BATCH_DELTA) \ 
                            + root->prepare_cost[RIGHT_STATE] + root->compute_cost + root->delta_cost[RIGHT_STATE];
                    else
                        cost += ExecBFCalCost(root->lefttree, PULL_BATCH_DELTA) + ExecBFCalCost(root->righttree, PULL_DELTA) \ 
                            + root->compute_cost + root->delta_cost[RIGHT_STATE];

                    if (root->incState[LEFT_STATE] == STATE_KEEPMEM)
                        cost += root->keep_cost[LEFT_STATE]; 
                }
            }
            else
                elog(ERROR, "Brutefore does not support it"); 
            break;

        case INC_SEQSCAN:
            if (pullAction == PULL_DELTA)
                cost = 0;
            else 
                cost += root->prepare_cost[LEFT_STATE] + root->compute_cost; 
            break;

        case INC_AGGHASH:
            if (pullAction == PULL_DELTA)
            {
                if (root->incState[LEFT_STATE] == STATE_DROP)
                    cost += ExecBFCalCost(root->lefttree, PULL_BATCH_DELTA) + root->prepare_cost[LEFT_STATE] + DELTA_COST;
                else
                    cost += ExecBFCalCost(root->lefttree, PULL_DELTA) + DELTA_COST; 
            }
            else
            {
                if (root->incState[LEFT_STATE] == STATE_DROP)
                    cost += ExecBFCalCost(root->lefttree, PULL_BATCH_DELTA) + root->prepare_cost[LEFT_STATE] + DELTA_COST + root->compute_cost; 
                else
                    cost += ExecBFCalCost(root->lefttree, PULL_DELTA) + DELTA_COST + root->compute_cost;  
            }

            break; 

        case INC_MATERIAL:
            if (pullAction == PULL_DELTA)
            {
                if (root->incState[LEFT_STATE] == STATE_DROP)
                    cost += ExecBFCalCost(root->lefttree, PULL_DELTA);
                else
                    cost += root->keep_cost[LEFT_STATE];
            }
            else
            {
                if (root->incState[LEFT_STATE] == STATE_DROP)
                    cost += ExecBFCalCost(root->lefttree, PULL_BATCH_DELTA); 
                else
                    cost += ExecBFCalCost(root->lefttree, PULL_DELTA) + root->keep_cost[LEFT_STATE]; 
            }
            break; 

        default:
            elog(ERROR, "Bruteforce does not support unrecognized nodetype: %u", root->type);
            return 0; 
    }

    return cost; 
}

void 
ExecBruteForce(IncInfo **incInfo_array, int numIncInfo, int incMemory)
{
    IncInfo *incInfo;

    int numState = 0;
    for (int i = 0; i < numIncInfo; i++)
    {
        incInfo = incInfo_array[i];
        if (incInfo->type == INC_HASHJOIN)
            numState += 2;
        else if (incInfo->type == INC_AGGHASH)
            numState += 1;
        else if (incInfo->type == INC_MATERIAL)
            numState += 1; 
    }

    int minCost = INT_MAX; 
    int minCase; 
    int totalCase = 1 << numState; 
    int memoryCost = 0;

    elog(NOTICE, "totalcase: %d, totalstate: %d", totalCase, numState);
    for (int i = 0; i < totalCase; i++)
    {
        memoryCost = 0;
        for (int j = 0, k = 0; j < numState; k++)
        {
            incInfo = incInfo_array[k];
            if (incInfo->type == INC_HASHJOIN)
            {
                memoryCost += incInfo->memory_cost[LEFT_STATE];
                memoryCost += incInfo->memory_cost[RIGHT_STATE];

                if ((i & (1 << j)) != 0)
                    incInfo->incState[LEFT_STATE] = STATE_KEEPMEM;
                else
                    incInfo->incState[LEFT_STATE] = STATE_DROP;

                if ((i & (1 << (j+1))) != 0)
                    incInfo->incState[RIGHT_STATE] = STATE_KEEPMEM;
                else
                    incInfo->incState[RIGHT_STATE] = STATE_DROP;
                j += 2;
            }
            else if (incInfo->type == INC_AGGHASH || incInfo->type == INC_MATERIAL)
            {
                memoryCost += incInfo->memory_cost[LEFT_STATE];

                if ((i & (1 << j)) != 0)
                    incInfo->incState[LEFT_STATE] = STATE_KEEPMEM;
                else
                    incInfo->incState[LEFT_STATE] = STATE_DROP;

                j++; 
            }
        }

        //if (memoryCost > incMemory)
        //    continue; 

        int curCost = ExecBFCalCost(incInfo_array[numIncInfo - 1], PULL_DELTA);

        if (curCost < minCost)
        {
            minCost = curCost;
            minCase = i; 
        }
    }

    for (int j = 0, k = 0; j < numState; k++)
    {
        incInfo = incInfo_array[k];
        if (incInfo->type == INC_HASHJOIN)
        {
            if ((minCase & (1 << j)) != 0)
                incInfo->incState[LEFT_STATE] = STATE_KEEPMEM;
            else
                incInfo->incState[LEFT_STATE] = STATE_DROP;

            if ((minCase & (1 << (j+1))) != 0)
                incInfo->incState[RIGHT_STATE] = STATE_KEEPMEM;
            else
                incInfo->incState[RIGHT_STATE] = STATE_DROP;
            j += 2;
        }
        else if (incInfo->type == INC_AGGHASH || incInfo->type == INC_MATERIAL)
        {
            if ((minCase & (1 << j)) != 0)
                incInfo->incState[LEFT_STATE] = STATE_KEEPMEM;
            else
                incInfo->incState[LEFT_STATE] = STATE_DROP;

            j++; 
        }
    }

}


//static void 
//ExecGreedyAssignState(IncInfo *incInfo, IncState state)
//{
//    int id = incInfo->id; 
//    switch(incInfo->type)
//    {
//        case INC_HASHJOIN:
//            incInfo->incState[RIGHT_STATE] = state;
//            break;
//
//        case INC_MERGEJOIN:
//            elog(ERROR, "not supported MergeJoin yet"); 
//            break;
//
//        case INC_NESTLOOP:
//            incInfo->incState[RIGHT_STATE] = STATE_DROP;
//            break;
//
//        case INC_SEQSCAN:
//        case INC_INDEXSCAN:
//            return; 
//            break; 
//
//        case INC_AGGSORT:
//            incInfo->incState[LEFT_STATE] = STATE_DROP; 
//            break; 
//
//        case INC_AGGHASH:
//        case INC_MATERIAL:
//        case INC_SORT:
//            incInfo->incState[LEFT_STATE] = state; 
//            break; 
//
//
//        default:
//            elog(ERROR, "GreedyAssignState unrecognized nodetype: %u", incInfo->type);
//            break; 
//            return; 
//    }
//}
//
