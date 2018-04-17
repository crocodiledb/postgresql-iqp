/*-------------------------------------------------------------------------
 *
 * incmodifyplan.c
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/incmodifyplan.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "nodes/execnodes.h"
#include "executor/execdesc.h"

void IncInsertNodeAfter(PlanState *ps, PlanState *parent, bool isLeft)
{
    PlanState *child = NULL;
    if (isLeft) 
    {
        child = parent->lefttree; 
        parent->lefttree = ps; 
        parent->plan->lefttree = ps->plan;
    }
    else
    {
        child = parent->righttree; 
        parent->righttree = ps; 
        parent->plan->righttree = ps->plan;
    }

    ps->lefttree = child; 
    ps->plan->lefttree = child->plan; 
}

void IncDeleteNode(PlanState *parent, bool isLeft)
{
    PlanState *child; 
    if (isLeft)
    {
        child = parent->lefttree->lefttree; 
        parent->lefttree = child; 
    }
    else
    {
        child = parent->righttree->lefttree;
        parent->righttree = child;
    }

}
