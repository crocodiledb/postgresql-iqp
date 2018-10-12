/*-------------------------------------------------------------------------
 *
 * incDPSolution.h
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/incDecideState.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef INCDECIDESTATE_H
#define INCDECIDESTATE_H

extern enum DecisionMethod decision_method; 

struct IncInfo; 
enum PullAction;
enum IncState; 

typedef struct DPMeta
{
    int             **deltaCost;                  /* totem: cost for computing delta */
    enum IncState   ***deltaIncState;             /* totem: two dimension array of inc states */
    int             **deltaMemLeft;               /* totem: memory allocated to the left subtree */
    int             **deltaMemRight;              /* totem: memory allocated to the right subtree */
    enum PullAction **deltaLeftPull; 
    enum PullAction **deltaRightPull; 

    int             **bdCost;                     /* totem: cost of computing both delta and batch */
    enum IncState   ***bdIncState;
    int             **bdMemLeft;
    int             **bdMemRight;
    enum PullAction **bdLeftPull; 
    enum PullAction **bdRightPull; 

} DPMeta;

typedef enum DecisionMethod
{
    DM_DP,
    DM_TOPDOWN,
    DM_BOTTOMUP,
    DM_MEMSMALLFIRST,
    DM_MEMBIGFIRST,
    DM_RECYCLER
} DecisionMethod; 

/*
 * prototypes for DP Algorithms 
 * */
extern DPMeta *BuildDPMeta(int numIncInfo, int incMemory); 
extern void ExecDPSolution(DPMeta *dpmeta, struct IncInfo **incInfoArray, int numIncInfo, int incMemory, bool isSlave); 
extern void ExecDPAssignState (DPMeta *dpmeta, struct IncInfo **incInfoArray, int i, int j, enum PullAction parentAction); 
extern void ExecBruteForce(struct IncInfo **incInfo_array, int numIncInfo, int incMemory);

/*
 * prototypes for Greedy Algorithms 
 */
extern void ExecGreedySolution(struct IncInfo **incInfo_array, int numIncInfo, int incMemory, DecisionMethod dm);

#endif

