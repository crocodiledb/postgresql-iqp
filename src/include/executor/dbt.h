
#ifndef DBT_H
#define DBT_H
#include "postgres.h"

#include "HashBundle.h"
#include "executor/execdesc.h"
#include "nodes/execnodes.h"
#include "executor/executor.h"

extern char * dbt_query; 
extern bool enable_dbtoaster; 

typedef struct DBTMatConf
{
    int         id;
    char        name[50];
    int         base_num;
    int         tlist_size;

    char        **mainsql;
    char        **tmptable; 
    char        **projsql;

    char        **tlist; 
    char        ***joinkey;
    int         *joinkey_num; 
    char        **left;
    char        **right;
    char        ***parent_names; 
    int         *parents_num; 
} DBTMatConf; 

typedef struct DBTConf 
{
    int         qd_num; 
    int         mat_num; 
    int         base_num; 
    int         preload_num; 
    DBTMatConf  **mat_array; 
    DBTMatConf  **base_array;
    DBTMatConf  **preload_array;  
} DBTConf; 

typedef struct DBTMaterial
{
    int                 id;
    char                name[50];
    int                 numHash;  
    HashBundle          *hb;
    QueryDesc           **local_qd;     
    PlanState           **local_ps;     /* Equal to the number of base relations */
    QueryDesc           **proj_qd; 
    PlanState           **proj_ps;
    struct DBTMaterial  **left; 
    struct DBTMaterial  **right; 
    Oid                 *base_oid;      /* For each join operator, what is oid of the joined based table */
    bool                *isBuild;       /* Do we build the hash table for the base table or not */
    PlanState           *additional_ps; 
    struct DBTMaterial  ***parents;
    int                 *parent_num; 
    bool                hasUpdate; 
} DBTMaterial; 

typedef struct DBTStat
{   
    struct timeval start; 
    struct timeval end;
    FILE    *timeFile; 
    FILE    *memFile;   
    double  *execTime;
    int      memCost; 
} DBTStat; 

typedef struct DBToaster 
{
    int         mat_num; 
    int         base_num;
    int         preload_num;  
    int         qd_num;
    int         qd_index;
    DBTStat     *stat;  
    DBTConf     *dbtConf; 
    DBTMaterial **mat_array; 
    DBTMaterial **base_array; 
    DBTMaterial **preload_array; 
    QueryDesc   **qd_array; 
    bool        executed; 
} DBToaster; 

extern void ExecInitDBToaster(EState *estate, PlanState *root);
extern TupleTableSlot * ExecDBToaster(EState *estate, PlanState *root);
extern void ExecEndDBToaster(EState *estate);

/* Prototypes for HashJoin */
extern void ExecInitHashJoinDBT(HashJoinState *node, EState *estate, int eflags);
extern void ExecResetHashJoinDBT(HashJoinState *node); 

/* Prototypes for Aggregate */
extern void ExecInitAggDBT(AggState *aggstate); 
extern void ExecFinalizeAggDBT(AggState *aggstate); 
extern void ExecResetAggDBT(AggState *aggstate); 

/* Prototypes for Sort */
extern void ExecResetSortDBT(SortState *node); 

/* Prototypes for Scan */
extern void ExecInitSeqScanDBT(SeqScanState *node);
extern void ExecInitScanDBT(ScanState *node); 
extern void ExecTakeDeltaDBT(ScanState *node); 
extern TupleTableSlot *ExecScanDBT(ScanState *node,
		                    ExecScanAccessMtd accessMtd,	/* function returning a tuple */
		                    ExecScanRecheckMtd recheckMtd);

/* Prototype for projection */
TupleTableSlot *ExecProjectDBT(ScanState *node); 

#endif
