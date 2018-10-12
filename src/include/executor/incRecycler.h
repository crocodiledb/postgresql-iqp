/*-------------------------------------------------------------------------
 *
 *  incRecycler
 *	  header for incRecycler
 *
 *
 * src/include/executor/incRecycler.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef INCRECYCLER_H
#define INCRECYCLER_H

#include "postgres.h"
#include "executor/incinfo.h"

typedef struct StateCache
{
    IncInfo         *incInfo;
    bool            left; 
    int             memory_size;
    double          benefit; 
    IncState        cacheState;
} StateCache; 

typedef struct Recycler
{
    StateCache  **stateCache_array;
    IncInfo     **incInfo_array;
    int numStateCache;
    int numIncInfo;
    int memoryBudget;
    int usedMemory;
} Recycler;


Recycler * InitializeRecycler(IncInfo **incInfo_array, int numIncInfo, int incMemory);
void       UpdateRecycler(Recycler *recycler);
void       ExecRecycler(Recycler *recycler);
void       DestroyRecycler(Recycler *recycler);

#endif 

