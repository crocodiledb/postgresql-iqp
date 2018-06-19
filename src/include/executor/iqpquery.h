#ifndef DBTQUERY_H
#define DBTQUERY_H

#include "postgres.h"

struct PlanState;

typedef struct iqp_base
{
    int                 base_num;
    char                **table_name;
    char                **sql;
    Oid                 *base_oid;
    struct PlanState    **base_ps;
    struct QueryDesc    **base_qd;
    struct PlanState    **old_base_ps;
    struct PlanState    **parent_ps; 
    bool                *isLeft;  
} iqp_base; 

extern Oid IQP_GetOid(char *query, char *table_name); 

#endif
