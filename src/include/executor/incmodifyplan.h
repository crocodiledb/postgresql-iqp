/*-------------------------------------------------------------------------
 *
 * incmodifyplan.h
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/incmodifyplan.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef INCMODIFYPLAN_H
#define INCMODIFYPLAN_H

#include "nodes/execnodes.h"

extern void IncInsertNodeAfter(PlanState *ps, PlanState *parent, bool isLeft); 

extern void IncDeleteNode(PlanState *parent, bool isLeft);

#endif 
