/*-------------------------------------------------------------------------
 *
 * incRecycler.c
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/incRecycler.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "executor/incinfo.h"
#include "executor/incRecycler.h"

#include <float.h>

static void IncreaseIFactor(IncInfo *incInfo);
static int CalculateTrueCost(IncInfo *incInfo);
static void CalculateBenefit(Recycler *recycler);
static void DecideCacheState(Recycler *recycler);
static void UpdateState(IncInfo *incInfo, int iFactor, bool increase);


Recycler * InitializeRecycler(IncInfo **incInfo_array, int numIncInfo, int incMemory)
{
    IncInfo *incInfo;
    for (int i = 0; i < numIncInfo; i++)
    {
        incInfo = incInfo_array[i];
        incInfo->true_cost[LEFT_STATE]  = 0;
        incInfo->true_cost[RIGHT_STATE] = 0;

        incInfo->iFactor[LEFT_STATE]  = 1;
        incInfo->iFactor[RIGHT_STATE] = 1;
    }

    Recycler *recycler = (Recycler *)palloc(sizeof(Recycler));
    recycler->incInfo_array = incInfo_array;
    recycler->numIncInfo    = numIncInfo;
    recycler->memoryBudget  = incMemory;
    recycler->usedMemory    = 0;
    recycler->numStateCache = numIncInfo * MAX_STATE;

    StateCache **stateCache_array = (StateCache **)palloc(sizeof(StateCache *) * recycler->numStateCache);
    for (int i = 0; i < recycler->numStateCache; i++)
    {
        stateCache_array[i] = (StateCache *)palloc(sizeof(StateCache));

        int j = i / MAX_STATE;
        bool left = (i % MAX_STATE == 0);

        stateCache_array[i]->incInfo = incInfo_array[j];
        stateCache_array[i]->left    = left; 
        if (left)
            stateCache_array[i]->memory_size = incInfo_array[j]->memory_cost[LEFT_STATE];
        else
            stateCache_array[i]->memory_size = incInfo_array[j]->memory_cost[RIGHT_STATE];

        stateCache_array[i]->benefit = -DBL_MAX;
        stateCache_array[i]->cacheState    = STATE_DROP;
    }

    recycler->stateCache_array = stateCache_array; 

    return recycler; 
}

/* We update recycler when the query is invoked another time */
void UpdateRecycler(Recycler *recycler)
{
    StateCache  **stateCache_array = recycler->stateCache_array;
    IncInfo     **incInfo_array    = recycler->incInfo_array;
    int numStateCache = recycler->numStateCache; 

    StateCache  *stateCache;
    IncInfo     *incInfo; 

    bool        hasDelta;
    int         usedMem;

    /* 1) Update memory size of every state
     * 2) If a state has a delta, change its state to STATE_DROP; Set iFactor to 1
     * 3) If a state does not have a delta, check its state; if kept, add to usedMem
     * */
    for (int i = 0; i < numStateCache; i++)
    {
        stateCache = stateCache_array[i];
        incInfo    = stateCache->incInfo;

        stateCache->memory_size = stateCache->left ? incInfo->memory_cost[LEFT_STATE] : incInfo->memory_cost[RIGHT_STATE];

        hasDelta = stateCache->left ? incInfo->leftUpdate : incInfo->rightUpdate; 
        if (hasDelta)
        {
            stateCache->cacheState = STATE_DROP;

            if (stateCache->left)
                incInfo->iFactor[LEFT_STATE]  = 1;
            else
                incInfo->iFactor[RIGHT_STATE] = 1;
        }
        else
        {
            if (stateCache->cacheState == STATE_KEEPMEM)
                usedMem += stateCache->memory_size;
        }
    }

    recycler->usedMemory = usedMem;

    /*
     * Traverse IncInfo tree, increase iFactor for state without delta
     * */
    IncreaseIFactor(recycler->incInfo_array[recycler->numIncInfo - 1]);

    /*
     * Update state keep/drop in IncInfo using cacheState 
     * */
    for (int i = 0; i < numStateCache; i++)
    {
        stateCache = stateCache_array[i];
        if (stateCache->left)
            stateCache->incInfo->incState[LEFT_STATE]  = stateCache->cacheState;
        else
            stateCache->incInfo->incState[RIGHT_STATE] = stateCache->cacheState; 
    }
}

void ExecRecycler(Recycler *recycler)
{
    IncInfo **incInfo_array = recycler->incInfo_array;
    int numIncInfo          = recycler->numIncInfo;
    
    (void) CalculateTrueCost(incInfo_array[numIncInfo - 1]);

    CalculateBenefit(recycler);

    DecideCacheState(recycler); 
}

void DestroyRecycler(Recycler *recycler)
{

}

/* The following functions are used to support UpdateRecycler */
static void IncreaseIFactor(IncInfo *incInfo)
{
    if (incInfo == NULL)
        return;

    if (!incInfo->leftUpdate)
    {
        incInfo->iFactor[LEFT_STATE]++;

        if (incInfo->incState[LEFT_STATE] == STATE_DROP)
            IncreaseIFactor(incInfo->lefttree);
    }

    if (!incInfo->rightUpdate)
    {
        incInfo->iFactor[RIGHT_STATE]++;

        if (incInfo->incState[RIGHT_STATE] == STATE_DROP)
            IncreaseIFactor(incInfo->righttree);
    }
}

/* The following functions are used to support ExecRecycler */
static int CalculateTrueCost(IncInfo *incInfo)
{
    int true_cost = 0; 

    if (incInfo->lefttree != NULL && incInfo->righttree != NULL) /* Join */
    {
        int left_true_cost  = CalculateTrueCost(incInfo->lefttree);
        int right_true_cost = CalculateTrueCost(incInfo->righttree);
        incInfo->true_cost[LEFT_STATE]  = incInfo->keep_cost[LEFT_STATE] + left_true_cost;
        incInfo->true_cost[RIGHT_STATE] = incInfo->prepare_cost[RIGHT_STATE] + right_true_cost;

        if (incInfo->incState[LEFT_STATE] == STATE_DROP)
            true_cost += left_true_cost; 

        if (incInfo->incState[RIGHT_STATE] == STATE_DROP)
            true_cost += incInfo->true_cost[RIGHT_STATE]; 

        true_cost += incInfo->compute_cost;
    }
    else if (incInfo->lefttree != NULL && incInfo->righttree == NULL) /* Aggregate/Material/Sort */
    {
        if (incInfo->type == INC_AGGHASH || incInfo->type == INC_SORT || incInfo->type == INC_MATERIAL)
        {
            int left_true_cost = CalculateTrueCost(incInfo->lefttree);
            incInfo->true_cost[LEFT_STATE] = incInfo->prepare_cost[LEFT_STATE] + left_true_cost;

            if (incInfo->incState[LEFT_STATE] == STATE_DROP)
                true_cost += incInfo->true_cost[LEFT_STATE];

            true_cost += incInfo->compute_cost;
        }
        else if (incInfo->type == INC_AGGSORT)
        {
            true_cost += CalculateTrueCost(incInfo->lefttree) + incInfo->prepare_cost[LEFT_STATE] + incInfo->compute_cost;
        }
    }
    else /* Scan */
    {
        true_cost = incInfo->prepare_cost[LEFT_STATE] + incInfo->compute_cost; 
    }

    return true_cost;
}

static void CalculateBenefit(Recycler *recycler)
{
    StateCache **stateCache_array = recycler->stateCache_array;
    int          numStateCache    = recycler->numStateCache; 

    StateCache  *stateCache;
    IncInfo     *incInfo;
    double       benefit;
    
    for (int i = 0; i < recycler->numStateCache; i++)
    {
        stateCache = stateCache_array[i];
        incInfo    = stateCache->incInfo;

        if (stateCache->memory_size != 0)
        {
            if (stateCache->left)
                stateCache->benefit = (double)(incInfo->true_cost[LEFT_STATE] * incInfo->iFactor[LEFT_STATE])/(double)stateCache->memory_size; 
            else
                stateCache->benefit = (double)(incInfo->true_cost[RIGHT_STATE] * incInfo->iFactor[RIGHT_STATE])/(double)stateCache->memory_size;
        }
    }
}

static int BenefitComparator(const void * a, const void *b)
{
    StateCache *a_SC = *(StateCache **)a;
    StateCache *b_SC = *(StateCache **)b;

    double a_benefit = a_SC->benefit, b_benefit = b_SC->benefit; 

    if (a_benefit <= b_benefit)
        return -1;
    else
        return 1; 
}

static void DecideCacheState(Recycler *recycler)
{
    StateCache **stateCache_array = recycler->stateCache_array; 
    int numStateCache = recycler->numStateCache; 
    StateCache *stateCache;

    int memoryBudget = recycler->memoryBudget;
    int remainMem    = recycler->memoryBudget - recycler->usedMemory;

    /* Sort by benefit */
    qsort((void *)stateCache_array, (size_t)numStateCache, sizeof(StateCache *), BenefitComparator);

    for (int i = 0; i < numStateCache; i++)
    {
        stateCache = stateCache_array[i];
        if (stateCache->memory_size == 0)
            continue;

        if (stateCache->cacheState == STATE_DROP)
        {
            if (stateCache->memory_size <= remainMem)
            {
                remainMem -= stateCache->memory_size;
                stateCache->cacheState = STATE_KEEPMEM;
            }
            else
            {
                int    memory_sum  = 0;
                double benefit_sum = 0;
                int    cur_memory  = stateCache->memory_size;
                double cur_benefit = stateCache->benefit;

                double stateCount  = 0;

                for (int j = 0; j < numStateCache; j++)
                {
                    if (stateCache_array[j]->cacheState == STATE_KEEPMEM)
                    {
                        stateCount++;
                        memory_sum  += stateCache_array[j]->memory_size;
                        benefit_sum += stateCache_array[j]->benefit;
                        
                        /* Let's keep the current state and kick out some states with smaller benefits */
                        if ((memory_sum + remainMem) >= cur_memory && benefit_sum/stateCount < cur_benefit)
                        {
                            for (int k = 0; k <= j; k++)
                                stateCache_array[k]->cacheState = STATE_DROP;

                            stateCache->cacheState = STATE_KEEPMEM;
                            remainMem = remainMem + memory_sum - cur_memory; 

                            break; 
                        }
                    }
                } /* End of for loop */
            }
        }
    }

    recycler->usedMemory = memoryBudget - remainMem;

    /* Now we get a new set of state configuration, update state and iFactor in the incInfo tree */
    IncInfo *incInfo;

    for (int i = 0; i < numStateCache; i++)
    {
        stateCache = stateCache_array[i];
        incInfo    = stateCache->incInfo;

        if ((stateCache->left && stateCache->cacheState != incInfo->incState[LEFT_STATE]) || \ 
                (!stateCache->left && stateCache->cacheState != incInfo->incState[RIGHT_STATE]))
        {
            if (stateCache->left)
            {
                incInfo->incState[LEFT_STATE] = stateCache->cacheState; 
                UpdateState(incInfo->lefttree, incInfo->iFactor[LEFT_STATE], (stateCache->cacheState == STATE_DROP));
            }
            else
            {
                incInfo->incState[RIGHT_STATE] = stateCache->cacheState;
                UpdateState(incInfo->righttree, incInfo->iFactor[RIGHT_STATE], (stateCache->cacheState == STATE_DROP));
            }
        }
    }
}

static void UpdateState(IncInfo *incInfo, int iFactor, bool increase)
{
    if (incInfo == NULL)
        return; 

    if (increase)
    {
        incInfo->iFactor[LEFT_STATE]  += iFactor;
        incInfo->iFactor[RIGHT_STATE] += iFactor; 
    }
    else
    {
        incInfo->iFactor[LEFT_STATE]  -= iFactor;
        incInfo->iFactor[RIGHT_STATE] -= iFactor;
    }

    if (incInfo->incState[LEFT_STATE]  != STATE_KEEPMEM)
        UpdateState(incInfo->lefttree, iFactor, increase);

    if (incInfo->incState[RIGHT_STATE] != STATE_KEEPMEM)
        UpdateState(incInfo->righttree, iFactor, increase);
}


