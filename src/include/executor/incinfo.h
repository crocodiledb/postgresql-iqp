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

typedef enum DecisionMethod
{
    DM_DP,
    DM_TOPDOWN,
    DM_BOTTOMUP,
    DM_MEMSMALLFIRST,
    DM_MEMBIGFIRST
}DecisionMethod; 

typedef void *(*ExecDPNode) (struct EState *estate, int i, int j);

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
 *      PULL_BATCH: only pull batch data
 *      PULL_DELTA: only pull delta data 
 *      PULL_BATCH_DELTA: pull both batch and delta data 
 */

typedef enum PullAction 
{
    PULL_BATCH,
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
    int id;  

    /*
     * memory_cost is initialized when the batch processing is done
     * compute_cost is initialized as the estimated cost in plan tree 
     */
    ExecDPNode execDPNode; 
    int memory_cost; 
    int compute_cost;
    int prepare_cost;  

    /* Does left or right substrees have deltas */
    bool    leftUpdate; 
    bool    rightUpdate; 

    /* Pull actions  */
    PullAction leftAction; 
    PullAction rightAction; 

    /* IncState */
    IncState leftIncState; 
    IncState rightIncState; 

} IncInfo; 
 
#endif

