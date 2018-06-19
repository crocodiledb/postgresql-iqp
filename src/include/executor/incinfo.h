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

#include "postgres.h"

struct PlanState; 
struct DPMeta;

typedef void *(*ExecDPNode) (struct DPMeta *dpmeta, int i, int j, struct IncInfo *incInfo);

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
    STATE_KEEPDISK, 
    STATE_KEEPMIX, 
} IncState; 

/*
 * Max number of possible state 
 * */
#define MAX_STATE 2
#define LEFT_STATE 0
#define RIGHT_STATE 1

/*
 * PullAction
 *      PULL_BATCH: only pull batch data
 *      PULL_DELTA: only pull delta data 
 *      PULL_BATCH_DELTA: pull both batch and delta data 
 */

typedef enum PullAction 
{
    PULL_NOTHING, 
    PULL_BATCH,
    PULL_DELTA,
    PULL_BATCH_DELTA
} PullAction; 

/*
 * IncTag
 *
 * */

typedef enum Inc_Tag
{
    INC_INVALID, 
    INC_HASHJOIN,
    INC_MERGEJOIN,
    INC_NESTLOOP,
    INC_AGGHASH,
    INC_AGGSORT,
    INC_SORT,
    INC_MATERIAL,
    INC_SEQSCAN,
    INC_INDEXSCAN,
    INC_TAG_NUM
} Inc_Tag; 

/*
 * Info struct for incremental processing 
 */

typedef struct IncInfo 
{
    Inc_Tag type; 
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
    int compute_cost;
    int memory_cost[MAX_STATE]; 
    int prepare_cost[MAX_STATE];
    int delta_cost[MAX_STATE];
    int keep_cost[MAX_STATE]; 
    bool mem_computed[MAX_STATE];  

    /* Does left or right substrees have deltas; will only be used in the compile time */
    bool    leftUpdate; 
    bool    rightUpdate; 

    /* Pull actions: will be used in the batch/delta execution */
    PullAction leftAction; 
    PullAction rightAction; 

    /* IncState: will be used in the delta execution */
    IncState incState[MAX_STATE];
    bool     stateExist[MAX_STATE]; 

} IncInfo; 
 
#endif

