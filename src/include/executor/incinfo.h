/*-------------------------------------------------------------------------
 *
 * incinfo
 *	  header for IncInfo
 *
 *
 * src/include/executor/incinfo.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef INCINFO_H
#define INCINFO_H 

#include "nodes/execnodes.h"

/*
 * Helpful Macros
 */

#define innerIncInfo(node)		(((IncInfo *)(node))->righttree)
#define outerIncInfo(node)		(((IncInfo *)(node))->lefttree)

/*
 * Whether we drop or keep (in memory or on disk) the state of a node 
 */

typedef enum IncState 
{
    STATE_DROP,
    STATE_KEEPMEM, 
    STATE_KEEPDISK 
} IncState; 

/*
 * PullAction
 *      PULL_DELTA: only pull delta data 
 *      PULL_BATCH_DELTA: pull both batch and delta data 
 */

typedef enum PullAction 
{
    PULL_DELTA,
    PULL_BATCH_DELTA
} PullAction; 


/*
 * Info struct for incremental processing 
 */

typedef struct IncInfo {

    struct PlanState *ps; 

    /* Pointers to left/right/parent subtrees. 
     * They are initialized by ExecInitIncInfo. 
     */
    struct IncInfo *parenttree; 
    struct IncInfo *lefttree; 
    struct IncInfo *righttree; 

    int trigger_computation; 

    /* Pull actions  */
    PullAction leftAction; 
    PullAction rightAction; 
    PullAction parentAction; 

    /* IncState */
    IncState leftIncState; 
    IncState rightIncState; 

    /* Does left or right substrees have deltas */
    bool    leftUpdate; 
    bool    rightUpdate; 

} IncInfo; 
 
#endif

