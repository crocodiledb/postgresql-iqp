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
 * Actions for rows collection
 * */
typedef enum RowAction
{
    ROW_CPU,
    ROW_MEM
} RowAction;

/*
 * Actions for Cost collection
 * */
typedef enum CostAction
{
    COST_CPU_INIT,
    COST_MEM_INIT,
    COST_CPU_UPDATE,
    COST_MEM_UPDATE
} CostAction; 

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
    INC_HASHJOIN,
    INC_MERGEJOIN,
    INC_NESTLOOP,
    INC_AGGHASH,
    INC_AGGSORT,
    INC_SORT,
    INC_MATERIAL,
    INC_SEQSCAN,
    INC_INDEXSCAN,
    INC_INVALID, 
    INC_TAG_NUM
} Inc_Tag; 

/*
 * Info struct for incremental processing 
 */

typedef struct IncInfo 
{
    Inc_Tag type; 
    struct PlanState *ps; 

    /* 
     * Pointers to left/right/parent subtrees. 
     * They are initialized by ExecInitIncInfo. 
     * */
    struct IncInfo *parenttree; 
    struct IncInfo *lefttree; 
    struct IncInfo *righttree; 

    int trigger_computation;
    int id;

    /*
     * Track these for CPU cost estimation
     * */
    double existing_rows;
    double upcoming_rows;
    double delta_rows; 

    double base_existing_rows;
    double base_upcoming_rows;
    double base_delta_rows;

    /*
     * Track these for Memory cost estimation
     * */
    double mem_existing_rows;
    double mem_upcoming_rows;
    double mem_delta_rows;

    /*
     * memory_cost is initialized when the batch processing is done
     * compute_cost is initialized as the estimated cost in plan tree 
     */
    ExecDPNode execDPNode; 
    
    /* The computation cost for original query processing */ 
    int compute_cost; 

    /* The memory cost of keeping left state or right state */
    int memory_cost[MAX_STATE];

    /* The local computation cost generated between the first tuple arrives at the operator 
     * and the first tuple is output from the operator
     *
     * For hash join and nestloop, it is stored in prepare_cost[RIGHT_STATE]; 
     * otherwise, it is stored in prepare_cost[LEFT_STATE]
     * */
    int prepare_cost[MAX_STATE]; 

    /* The computation cost of processing delta
     * delta_cost[LEFT_STATE]  represents the case where the left state is kept
     * delta_cost[RIGHT_STATE] represents the case where the right state is kept
     * */
    int delta_cost[MAX_STATE];

    /* The computation cost of keeping left state or right state 
     * */
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

