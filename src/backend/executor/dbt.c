
#include "postgres.h"
#include "nodes/execnodes.h"
#include "executor/execdesc.h"
#include "nodes/plannodes.h"

#include "utils/relcache.h"
#include "utils/rel.h"

#include "executor/incTupleQueue.h"
#include "executor/incTQPool.h"
#include "executor/execTPCH.h"

#include "access/htup_details.h"

#include "miscadmin.h"

#include "utils/snapmgr.h"

#include "executor/dbt.h" 
#include "executor/nodeHash.h"
#include "executor/hashjoin.h"

#include "executor/incmeta.h"

/* For creating and destroying query online */
#include "parser/parser.h"
#include "nodes/parsenodes.h"
#include "nodes/params.h"

#define STAT_TIME_FILE  "dbt_stat/time.out"
#define STAT_MEM_FILE   "dbt_stat/mem.out"

#define DBT_DELTA_THRESHOLD 1

char *dbt_query;
bool  enable_dbtoaster; 

static DBTConf* ExecBuildDBTConf(); 
static DBToaster *ExecBuildDBToaster(DBTConf * dbtConf, PlanState *root);

static void ExecDBTProcNode(DBTMaterial *mat, TupleTableSlot * slot, int base_index);

static void ExecDBTResetTQReader(EState *estate); 
static void ExecDBTWaitUpdate(EState *estate);
static void ExecDBTCollectUpdate(EState *estate); 

static void ExecDBTCollectHashTable(EState *estate, DBTMaterial *mat, DBTMaterial *child, int base_index); 
static void ExecDBTInitMat(DBToaster *dbt, EState *estate, PlanState *root); 
static void ExecDBTResizeHashTable(DBToaster *dbt);
static void ExecDBTAccountBucketSize(DBToaster *dbt); 
static void ExecDBTResetMat(DBToaster *dbt, EState *estate, PlanState *root); 
static void ExecDBTPerPSHelper(EState *estate, PlanState *ps, bool reset); 

static void ExecEndQD(DBToaster *dbt); 
static void TakeNewSnapshot(EState *estate);
static void ExecDBTCollectStat(DBToaster *dbt, PlanState *root, int numdelta); 
static int ExecDBTMemCost(DBToaster *dbt, PlanState *root); 

static void ShowMemSize(EState *estate); 

static void ReAllocDeltaArray(EState *estate); 

void 
ExecInitDBToaster(EState *estate, PlanState *root)
{
    DBTConf     * dbtConf; 
    DBToaster   * dbt; 

    MemoryContext old = MemoryContextSwitchTo(estate->es_query_cxt); 

    /* Potential Updates of TPC-H */
    estate->tpch_update = ExecInitTPCHUpdate(tables_with_update, false); 
    if (!external_delta)
        estate->numDelta = estate->tpch_update->numdelta; 
    else
        estate->numDelta = 2; 
    estate->deltaIndex = 0; 

    // Build DBTConf and DBToaster
    dbtConf = ExecBuildDBTConf();
    TakeNewSnapshot(estate); 
    dbt     = ExecBuildDBToaster(dbtConf, root); 
    dbt->dbtConf = dbtConf; 
    estate->dbt  = dbt; 

    // Init DBTMaterial
    ExecDBTInitMat(dbt, estate, root); 

    // Setup tq
    IncTQPool *tq_pool = CreateIncTQPool(estate->es_query_cxt, dbt->base_num); 

    estate->reader_ss = palloc(sizeof(ScanState *) * dbt->base_num); 
    for (int i = 0; i < dbt->base_num; i++)
    {
        estate->reader_ss[i] = dbt->base_array[i]->local_ps[i]; 
        Relation r = estate->reader_ss[i]->ss_currentRelation; 
        (void) AddIncTQReader(tq_pool, r, RelationGetDescr(r)); 

        estate->reader_ss[i]->ps.state->tq_pool = tq_pool; 
    }

    estate->tq_pool = tq_pool; 

    /* Build DBToaster Stats */
    DBTStat *dbtStat = palloc(sizeof(DBTStat)); 
    dbtStat->timeFile = fopen(STAT_TIME_FILE, "a"); 
    dbtStat->memFile = fopen(STAT_MEM_FILE, "a"); 
    dbtStat->execTime = palloc(sizeof(double) * (estate->numDelta + 1)); 
    dbtStat->memCost =  palloc(sizeof(int) * (estate->numDelta + 1)); 
    dbt->stat = dbtStat; 
    
    MemoryContextSwitchTo(old);
}

TupleTableSlot * 
ExecDBToaster(EState *estate, PlanState *root)
{
    TupleTableSlot *slot; 
    DBToaster *dbt = estate->dbt;
    for (;;)
    {
        if (!dbt->executed)
        {
            gettimeofday(&(dbt->stat->start) , NULL); 

            if(estate->deltaIndex == 0) /* First time: Preload */
            {
                for (int i = 0; i < dbt->preload_num; i++)
                {
                    DBTMaterial *preload = dbt->preload_array[i]; 
                    for (;;)
                    {
                        slot = ExecProcNode(preload->local_ps[0]);
                        if (TupIsNull(slot))
                            break; 

                        HashBundleInsert(preload->hb, slot); 
                    }
                }
            }

            for (int i = 0; i < dbt->base_num; i++)
            {
                if (!dbt->base_array[i]->hasUpdate)
                    continue; 
    
                int count = 0; 
                DBTMaterial * mat = dbt->base_array[i];
                for (;;)
                {
                    slot = ExecProcNode(mat->local_ps[i]); 
                    if (TupIsNull(slot))
                        break;

                    /* Special case for one agg */
                    if (mat->additional_ps != NULL)
                    {
                        mat->additional_ps->ps_WorkingTupleSlot = slot; 
                        (void) mat->additional_ps->ExecProcNode(mat->additional_ps); // must be a HashAGG
                    }

                    count++; 
    
                    for (int j = 0; j < mat->parent_num[i]; j++)
                        ExecDBTProcNode(mat->parents[i][j], slot, i); 
    
                    if (mat->hb != NULL)
                        HashBundleInsert(mat->hb, slot);
                }
                    
                elog(NOTICE, "%d, %d", i, count); 

                if (estate->deltaIndex == 0)
                    ExecDBTResizeHashTable(dbt);    
            }

            /* 
            if (estate->deltaIndex == 0)
                ExecDBTAccountBucketSize(dbt);
            */
    
            /*Finallize it */
            AggState *aggstate = dbt->mat_array[0]->additional_ps; 
            ExecFinalizeAggDBT(aggstate); 
    
            dbt->executed = true; 
        }

        slot = ExecProcNode(root); 

        if (TupIsNull(slot))
        {
            gettimeofday(&(dbt->stat->end) , NULL); 

            dbt->stat->execTime[estate->deltaIndex] = GetTimeDiff(dbt->stat->start, dbt->stat->end); 
            dbt->stat->memCost[estate->deltaIndex] = ExecDBTMemCost(dbt, root); 

            if (estate->deltaIndex >= estate->numDelta)
            {
                ExecDBTCollectStat(dbt, root, estate->numDelta); 
                ExecEndQD(dbt);
                DestroyIncTQPool(estate->tq_pool); 
                return slot;
            }
            else
            {
                ReAllocDeltaArray(estate);

                estate->deltaIndex++;

                ExecDBTResetTQReader(estate);
                if (!external_delta)
                    GenTPCHUpdate(estate->tpch_update, estate->deltaIndex - 1); 
                ExecDBTWaitUpdate(estate); 
                ExecDBTCollectUpdate(estate); 

                // Reset
                ExecDBTResetMat(dbt, estate, root); 
                dbt->executed = false; 

                continue; 
            }
        }

        return slot; 
    }
}

void 
ExecEndDBToaster(EState *estate)
{
}


static void 
ExecDBTProcNode(DBTMaterial *mat, TupleTableSlot * slot, int base_index)
{
    TupleTableSlot * ret; 
    PlanState **proj_ps = mat->proj_ps; 
    PlanState **local_ps = mat->local_ps; 
    PlanState *additional_ps = mat->additional_ps; 

    local_ps[base_index]->ps_WorkingTupleSlot = slot; 

    for (;;)
    {
        ret = local_ps[base_index]->ExecProcNode(local_ps[base_index]); 
        if (TupIsNull(ret)) 
            return;

        proj_ps[base_index]->ps_WorkingTupleSlot = ret; 
        ret = ExecProjectDBT(proj_ps[base_index]); 

        if (additional_ps == NULL) 
        {
            for (int j = 0; j < mat->parent_num[base_index]; j++)
                ExecDBTProcNode(mat->parents[base_index][j], ret, base_index); 
        }
        else
        {
            additional_ps->ps_WorkingTupleSlot = ret; 
            (void) additional_ps->ExecProcNode(additional_ps); // must be a HashAGG
        }

        if (mat->hb != NULL)
            HashBundleInsert(mat->hb, ret); 
    }
}


static void ExecDBTResetTQReader(EState *estate)
{
    IncTQPool *tq_pool = estate->tq_pool;
    ScanState **reader_ss_array = estate->reader_ss; 

    for (int i = 0; i < estate->dbt->base_num; i++)
        DrainTQReader(tq_pool, reader_ss_array[i]->tq_reader); 
}

static void 
ExecDBTWaitUpdate(EState *estate)
{
    int cur_deltasize  = 0; 
    int prev_deltasize = 0;

    int threshold = DBT_DELTA_THRESHOLD; 

    IncTQPool *tq_pool = estate->tq_pool; 

    for (;;) 
    {
		CHECK_FOR_INTERRUPTS();
        
        cur_deltasize = GetTQUpdate(tq_pool); 

        if (cur_deltasize >= threshold && cur_deltasize == prev_deltasize) 
        {
            elog(NOTICE, "delta %d", cur_deltasize); 
            break;
        }

        prev_deltasize = cur_deltasize;

       sleep(100); 
    }
}

static void 
ExecDBTCollectUpdate(EState *estate)
{
    Relation r; 

    DBToaster *dbt = estate->dbt; 
    IncTQPool *tq_pool = estate->tq_pool; 
    ScanState **reader_ss_array = estate->reader_ss; 
    bool hasUpdate, isComplete; 

    for(int i = 0; i < dbt->base_num; i++) 
    {
        r = reader_ss_array[i]->ss_currentRelation;  
        hasUpdate = HasTQUpdate(tq_pool, r);
        isComplete = IsTQComplete(tq_pool, r);

        dbt->base_array[i]->hasUpdate = hasUpdate; 

        reader_ss_array[i]->tq_reader = GetTQReader(tq_pool, r, reader_ss_array[i]->tq_reader); 

        if (hasUpdate && isComplete)
            ExtMarkTableComplete(estate->tpch_update, GEN_TQ_KEY(r));
    }

    if (external_delta && ExtAllTableComplte(estate->tpch_update))
    {
        estate->numDelta = estate->deltaIndex;
    }
}

static void 
ExecDBTCollectHashTable(EState *estate, DBTMaterial *mat, DBTMaterial *child, int base_index)
{
    HashJoinTable hashtable; 
    List *hash_keys;
    bool  outer_tuple;

    /* Collect HashJoinTable */
    HashJoinState * hjState = (HashJoinState *) mat->local_ps[base_index];

    if (hjState->hj_hasInnerHash)
    {
        hashtable = hjState->hj_HashTable; 
        hash_keys = ((HashState *) innerPlanState(hjState))->hashkeys; 
        outer_tuple = false;
    }
    else
    {
        hashtable = hjState->hj_OuterHashTable; 
        hash_keys = hjState->hj_OuterHashKeys; 
        outer_tuple = true; 
    }

    hjState->hj_RealHashTable = HashBundleAddTable(child->hb, hashtable, hjState->js.ps.ps_ExprContext, \ 
                                                    hash_keys, outer_tuple, mat->joinkey[base_index], mat->joinkey_num[base_index]);
}


static void ExecDBTInitMat(DBToaster *dbt, EState *estate, PlanState *root)
{
    for (int i = 0; i < dbt->mat_num; i++)
    {
        DBTMaterial * mat = dbt->mat_array[i];
        for (int j = 0; j < dbt->base_num; j++)
        {
            if (mat->local_ps[j] != NULL)
            {
                estate->rd_id = mat->base_oid[j]; 
                estate->isBuild = mat->isBuild[j]; 
                ExecDBTPerPSHelper(estate, mat->local_ps[j], false);
            }
        }
    }

    for (int i = 0; i < dbt->preload_num; i++)
    {
        DBTMaterial * preload = dbt->preload_array[i];
        ExecDBTPerPSHelper(estate, preload->local_ps[0], false); 
    }

    for (int i = 0; i < dbt->mat_num; i++)
    {
        DBTMaterial * mat = dbt->mat_array[i]; 
        for (int j = 0; j < dbt->base_num; j++)
        {
            if (mat->local_ps[j] != NULL && mat->right[j] != NULL)
                ExecDBTCollectHashTable(estate, mat, mat->right[j], j); 
        }
    }

    PlanState *cur = root;
    for (;;)
    {
        if (cur->lefttree == NULL)
            break; 
        ExecDBTPerPSHelper(estate, cur, false); 
        cur = cur->lefttree; 
    }
}

static void
ExecDBTResizeHashTable(DBToaster *dbt)
{
    for (int i = 0; i < dbt->mat_num; i++)
    {
        DBTMaterial *mat = dbt->mat_array[i];
        if (mat->hb != NULL)
        {
            for (int j = 0; j < mat->hb->table_index; j++)
            {
                HashJoinTable hjTable = mat->hb->table_array[j]; 
                if (hjTable->nbuckets != hjTable->nbuckets_optimal)
                    ExecHashIncreaseNumBuckets(hjTable);
            }
        }
    }
}


static void ExecDBTAccountBucketSize(DBToaster *dbt)
{
    for (int i = 0; i < dbt->mat_num; i++)
    {
        DBTMaterial *mat = dbt->mat_array[i];
        if (mat->hb != NULL)
        {
            for (int j = 0; j < mat->hb->table_index; j++)
            {
                HashJoinTable hashtable = mat->hb->table_array[j];
            	hashtable->spaceUsed += hashtable->nbuckets * sizeof(HashJoinTuple);
	            if (hashtable->spaceUsed > hashtable->spacePeak)
		            hashtable->spacePeak = hashtable->spaceUsed;
            }
        }
    }
}

static void
ExecDBTResetMat(DBToaster *dbt, EState *estate, PlanState *root)
{
    for (int i = 0; i < dbt->mat_num; i++) 
    {
        DBTMaterial * mat = dbt->mat_array[i]; 
        for (int j = 0; j < dbt->base_num; j++)
        {
            if (mat->local_ps[j] != NULL)   
                ExecDBTPerPSHelper(estate, mat->local_ps[j], true); 
        }
    }

    PlanState *cur = root;
    for (;;)
    {
        if (cur->lefttree == NULL)
            break; 
        ExecDBTPerPSHelper(estate, cur, true); 
        cur = cur->lefttree; 
    }
}

/* Reset or Init */
static void
ExecDBTPerPSHelper(EState *estate, PlanState *ps, bool reset)
{
    HashJoinState *hjNode; 
    switch (ps->type) 
    {
        case T_HashJoinState:
            hjNode = (HashJoinState *)ps; 
            if (reset)
                ExecResetHashJoinDBT(hjNode); 
            else
                ExecInitHashJoinDBT(hjNode, estate, 0); 
            break;

        case T_AggState:
            if (reset)
                ExecResetAggDBT((AggState *)ps); 
            else
                ExecInitAggDBT((AggState *)ps); 
            break; 

        case T_SortState:
            if (reset)
                ExecResetSortDBT((SortState *)ps); 
            break;

        case T_SeqScanState:
            if (reset)
                ExecTakeDeltaDBT((ScanState *)ps);
            else
                ExecInitSeqScanDBT((SeqScanState *)ps); 
            break; 

        default:
            elog(ERROR, "PerHelper unrecognized nodetype: %u", ps->type);
            return; 
    }
}

#define JOIN_TEMPLATE_1 "select * from %s join %s on %s.%s_%s = %s.%s_%s;"
#define JOIN_TEMPLATE_2 "select * from %s join %s on %s.%s_%s = %s.%s_%s and %s.%s_%s = %s.%s_%s "
#define TEMP_TABLE_TEMPLATE "temp_%s_%d"
#define CREATE_TABLE_TEMPLATE "psql tpch -c \"create table %s as %s; \" "
#define DROP_TABLE_TEMPLATE "psql tpch -c \" drop table if exists %s ; \" "
#define STR_BUFSIZE 500


static bool 
CheckTargetList(char *target, char *table, DBTConf *dbtConf)
{
    for (int i = 0; i < dbtConf->mat_num; i++)
    {
        DBTMatConf * matConf = dbtConf->mat_array[i];
        if (strcmp(table, matConf->name) == 0)
        {
            for (int k = 0; k < matConf->tlist_size; k++)
            {
                if (strcmp(target, matConf->tlist[k]) == 0)
                    return true; 
            }
            return false; 
        }
    }

    elog(ERROR, "Not found table %s", table); 
    return false;
}


static DBTConf* 
ExecBuildDBTConf()
{
    DBTConf *dbtConf = palloc(sizeof(DBTConf)); 
    DBTMatConf **mat_array; 
    DBTMatConf **base_array;
    DBTMatConf **preload_array; 

    int mat_num, base_num, preload_num, tlist_size; 

    char file_name[100];
    memset(file_name, 0, sizeof(file_name)); 
    sprintf(file_name, "dbt_conf/%s.conf", dbt_query);

    FILE *conf_file = fopen(file_name, "r");
    if (conf_file == NULL)
    {
        elog(ERROR, "%s Not Found", file_name); 
        return; 
    }

    fscanf(conf_file, "%d,%d,%d\n", &mat_num, &base_num, &preload_num);
    dbtConf->mat_num = mat_num;
    dbtConf->base_num = base_num;
    dbtConf->preload_num = preload_num; 
    dbtConf->mat_array = palloc(sizeof(DBTMatConf *) * mat_num);
    dbtConf->base_array = palloc(sizeof(DBTMatConf *) * base_num);
    dbtConf->preload_array = palloc(sizeof(DBTMatConf *) * preload_num); 
    dbtConf->qd_num = 0; 

    mat_array = dbtConf->mat_array;
    base_array = dbtConf->base_array;
    preload_array = dbtConf->preload_array; 

    for (int i = 0; i < mat_num + preload_num; i++)
    {
        DBTMatConf *cur_mat; 

        if (i < mat_num)
        {
            mat_array[i] = (DBTMatConf *) palloc(sizeof(DBTMatConf)); 
            cur_mat = mat_array[i];

            if (i + base_num >= mat_num)
                base_array[i + base_num - mat_num] = cur_mat; 
        }
        else
        {
            preload_array[i - mat_num] = (DBTMatConf *) palloc(sizeof(DBTMatConf)); 
            cur_mat = preload_array[i - mat_num]; 
        }

        cur_mat->base_num = base_num;
        memset(cur_mat->name, 0, sizeof(cur_mat->name));
        fscanf(conf_file, "%s\n", cur_mat->name); 

        /* Read Target List */
        fscanf(conf_file, "%d\t", &tlist_size);
        cur_mat->tlist_size = tlist_size;
        cur_mat->tlist = palloc(sizeof(char *) * tlist_size); 
        for (int j = 0; j < tlist_size; j++ )
        {
            cur_mat->tlist[j] = palloc(sizeof(char) * STR_BUFSIZE); 
            memset(cur_mat->tlist[j], 0, sizeof(cur_mat->tlist[j]));
            fscanf(conf_file, "%s\t", cur_mat->tlist[j]); 
        }
        fscanf(conf_file, "\n"); 

        cur_mat->mainsql = palloc(sizeof(char *) * base_num); 
        cur_mat->tmptable = palloc(sizeof(char *) * base_num);
        cur_mat->joinkey = palloc(sizeof(char **) * base_num); 
        cur_mat->joinkey_num = palloc(sizeof(int) * base_num); 
        cur_mat->left = palloc(sizeof(char *) * base_num); 
        cur_mat->right = palloc(sizeof(char *) * base_num);  
        cur_mat->projsql = palloc(sizeof(char *) * base_num); 

        if (i + base_num < mat_num)
        {
            /* read join key */
            for (int j = 0; j < base_num; j++)
            {
                int temp_num; 
                fscanf(conf_file, "%d\t", &temp_num); 
                cur_mat->joinkey_num[j] = temp_num; 
                cur_mat->joinkey[j] = palloc(sizeof(char *) * temp_num); 
                for (int k = 0; k < temp_num; k++)
                {
                    cur_mat->joinkey[j][k] = palloc(sizeof(char) * STR_BUFSIZE); 
                    memset(cur_mat->joinkey[j][k], 0, sizeof(cur_mat->joinkey[j][k])); 
                    fscanf(conf_file, "%s\t", cur_mat->joinkey[j][k]); 
                }
                fscanf(conf_file, "\n"); 

                if (cur_mat->joinkey_num[j] != 0)
                    dbtConf->qd_num += 2; 
            }
            
            /* read left table */
            for (int j = 0; j < base_num; j++)
            {
                cur_mat->left[j] = palloc(sizeof(char) * STR_BUFSIZE); 
                memset(cur_mat->left[j], 0, sizeof(cur_mat->left[j])); 
                fscanf(conf_file, "%s\t", cur_mat->left[j]); 
            }
            fscanf(conf_file, "\n"); 
    
            /* read right table */
            for (int j = 0; j < base_num; j++)
            {
                cur_mat->right[j] = palloc(sizeof(char) * STR_BUFSIZE); 
                memset(cur_mat->right[j], 0, sizeof(cur_mat->right[j])); 
                fscanf(conf_file, "%s\t", cur_mat->right[j]); 
            }
            fscanf(conf_file, "\n"); 

            /* Generate joinsql and temp table */ 
            for (int j = 0; j < base_num; j++)
            {
                cur_mat->mainsql[j] = palloc(sizeof(char) * STR_BUFSIZE); 
                memset(cur_mat->mainsql[j], 0, sizeof(cur_mat->mainsql[j]));
                cur_mat->tmptable[j] = palloc(sizeof(char) * STR_BUFSIZE);
                memset(cur_mat->tmptable[j], 0, sizeof(cur_mat->tmptable[j]));

                if (cur_mat->joinkey_num[j] != 0)
                {
                    if (cur_mat->joinkey_num[j] == 1)
                    {
                        sprintf(cur_mat->mainsql[j], JOIN_TEMPLATE_1, cur_mat->left[j], cur_mat->right[j], \
                                cur_mat->left[j], cur_mat->left[j], cur_mat->joinkey[j][0], \ 
                                cur_mat->right[j], cur_mat->right[j], cur_mat->joinkey[j][0]);
                    }
                    else if (cur_mat->joinkey_num[j] == 2)
                    {
                        sprintf(cur_mat->mainsql[j], JOIN_TEMPLATE_2, cur_mat->left[j], cur_mat->right[j], \
                                cur_mat->left[j], cur_mat->left[j], cur_mat->joinkey[j][0], \ 
                                cur_mat->right[j], cur_mat->right[j], cur_mat->joinkey[j][0], \ 
                                cur_mat->left[j], cur_mat->left[j], cur_mat->joinkey[j][1], \ 
                                cur_mat->right[j], cur_mat->right[j], cur_mat->joinkey[j][1]); 
                    }
                    else
                    {
                        elog(ERROR, "Too many join keys"); 
                    }
                    sprintf(cur_mat->tmptable[j], TEMP_TABLE_TEMPLATE, cur_mat->name, j); 
                }
                else
                {
                    sprintf(cur_mat->mainsql[j], "null"); 
                    sprintf(cur_mat->tmptable[j], "null"); 
                }
            }
        }
        else /* base + preload */
        {
           dbtConf->qd_num++; 

           if (i < mat_num)
           {
                for (int j = 0; j < base_num; j++) 
                {
                    cur_mat->mainsql[j] = palloc(sizeof(char) * STR_BUFSIZE); 
                    memset(cur_mat->mainsql[j], 0, sizeof(cur_mat->mainsql[j])); 
                    if (j == (i + base_num - mat_num))
                        fscanf(conf_file, "%[^\n]s", cur_mat->mainsql[j]); 
                    else
                        strcpy(cur_mat->mainsql[j], "null"); 

                    cur_mat->joinkey_num[j]  = 0;
                    cur_mat->joinkey[j]  = NULL; 
                    cur_mat->left[j]     = NULL;
                    cur_mat->right[j]    = NULL;
                    cur_mat->tmptable[j] = NULL;
                    cur_mat->projsql[j]  = NULL; 
                }
           }
           else
           {
               cur_mat->mainsql[0] = palloc(sizeof(char) * STR_BUFSIZE); 
               memset(cur_mat->mainsql[0], 0, sizeof(cur_mat->mainsql[0]));
               fscanf(conf_file, "%[^\n]s", cur_mat->mainsql[0]); 
           }
        }

        if (i < mat_num)
        {
            cur_mat->parent_names = palloc(sizeof(char **) * base_num); 
            cur_mat->parents_num = palloc(sizeof(int) * base_num); 
            for (int j = 0; j < base_num; j++)
            {
                int temp_num; 
                fscanf(conf_file, "%d\t", &temp_num); 
                cur_mat->parent_names[j] = palloc(sizeof(char *) * temp_num); 
                for (int k = 0; k < temp_num; k++)
                {
                    cur_mat->parent_names[j][k] = palloc(sizeof(char) * STR_BUFSIZE); 
                    memset(cur_mat->parent_names[j][k], 0, sizeof(cur_mat->parent_names[j][k]));
                    fscanf(conf_file, "%s\t", cur_mat->parent_names[j][k]); 
                }
                cur_mat->parents_num[j] = temp_num; 
                fscanf(conf_file, "\n"); 
            }
        }
        fscanf(conf_file, "\n"); 
    }

    fclose(conf_file);

    /* Now we need to generate project sql for join operator */
    for (int i = 0; i < mat_num - base_num; i++)
    {
        DBTMatConf *cur_mat = mat_array[i];

        for (int j = 0; j < base_num; j++)
        {
            cur_mat->projsql[j] = palloc(sizeof(char) * STR_BUFSIZE); 
            memset(cur_mat->projsql[j], 0, sizeof(cur_mat->projsql[j])); 

            char *cur_proj = cur_mat->projsql[j]; 

            if (cur_mat->joinkey_num[j] == 0)
            {
                strcpy(cur_proj, "null"); 
                continue; 
            }

            /* contruct project sql */
            strcpy(cur_proj, "select "); 
            for (int k = 0; k < cur_mat->tlist_size; k++)
            {
                if (CheckTargetList(cur_mat->tlist[k], cur_mat->left[j], dbtConf)) /* from the left table */
                    strcpy(cur_proj + strlen(cur_proj), cur_mat->left[j]); 
                else
                    strcpy(cur_proj + strlen(cur_proj), cur_mat->right[j]); 

                strcpy(cur_proj + strlen(cur_proj), "_"); 
                strcpy(cur_proj + strlen(cur_proj), cur_mat->tlist[k]);
                if (k < cur_mat->tlist_size - 1) /* the last target list */
                    strcpy(cur_proj + strlen(cur_proj), ","); 
                strcpy(cur_proj + strlen(cur_proj), " "); 
            }
            strcpy(cur_proj + strlen(cur_proj), "from "); 
            strcpy(cur_proj + strlen(cur_proj), cur_mat->tmptable[j]); 
            strcpy(cur_proj + strlen(cur_proj), ";"); 
        }
    }

    /* Build temp table*/
    char *temp_str = malloc(sizeof(char) * STR_BUFSIZE); 
    for (int i = 0; i < mat_num - base_num; i++)
    {
        DBTMatConf *cur_mat = mat_array[i];
        for (int j = 0; j < base_num; j++)
        {
            if (cur_mat->joinkey_num[j] != 0)
            {
                memset(temp_str, 0, STR_BUFSIZE); 
                sprintf(temp_str, DROP_TABLE_TEMPLATE, cur_mat->tmptable[j]); 
                system(temp_str); 

                memset(temp_str, 0, STR_BUFSIZE); 
                sprintf(temp_str, CREATE_TABLE_TEMPLATE, cur_mat->tmptable[j], cur_mat->mainsql[j]); 
                system(temp_str); /* create the temp table */
            }
        }
    }

    free(temp_str); 

    return dbtConf; 
}

List *raw_parser(const char *str);
List *pg_analyze_and_rewrite(RawStmt *parsetree, const char *query_string,
					   Oid *paramTypes, int numParams,
					   QueryEnvironment *queryEnv);
List *pg_plan_queries(List *querytrees, int cursorOptions, ParamListInfo boundParams); 

static QueryDesc * BuildQDfromSQL(char *sqlstr)
{
    List	   *parsetree_list = raw_parser(sqlstr);
    RawStmt    *parsetree = lfirst_node(RawStmt, list_head(parsetree_list)); 

	List	   *querytree_list = pg_analyze_and_rewrite(parsetree, sqlstr,
												        NULL, 0, NULL);

	List		*plantree_list = pg_plan_queries(querytree_list, CURSOR_OPT_PARALLEL_OK, NULL);

	
    QueryDesc *queryDesc = CreateQueryDesc(linitial_node(PlannedStmt, plantree_list),
                                    sqlstr,
									GetActiveSnapshot(),
									InvalidSnapshot,
									None_Receiver,
									NULL,
									NULL,
									0);
    queryDesc->isFirst = false; 

    ExecutorStart(queryDesc, 0);

    return queryDesc;  
}

static void FindHashtoBuild(DBTMaterial *dbtMat, char *left, char *right, int base_index)
{
    Oid ret = DBT_GetOid(dbt_query, left); 
    if (ret != 0)
    {
        dbtMat->base_oid[base_index] = ret; 
        dbtMat->isBuild[base_index] = false; 
    }
    else
    {
        ret = DBT_GetOid(dbt_query, right); 
        if (ret != 0)
        {
            dbtMat->base_oid[base_index] = ret; 
            dbtMat->isBuild[base_index] = true; 
        }
        else
            elog(ERROR, "both %s and %s not match base tables ", left, right); 
    }
}

static DBTMaterial *FindMatByName(DBToaster *dbt, char *name)
{
    for(int i = 0; i < dbt->mat_num; i++)
    {
        if (strcmp(dbt->mat_array[i]->name, name) == 0)
            return dbt->mat_array[i]; 
    }

    for(int i = 0; i < dbt->preload_num; i++)
    {
        if (strcmp(dbt->preload_array[i]->name, name) == 0)
            return dbt->preload_array[i]; 
    }

    elog(ERROR, "Not Found %s", name); 
    return NULL;
}

static DBToaster *
ExecBuildDBToaster(DBTConf * dbtConf, PlanState *root)
{
    DBToaster *dbt; 
    DBTMaterial *dbtMat;
    DBTMaterial  *dbtPre; 
    int mat_num = dbtConf->mat_num; 
    int base_num = dbtConf->base_num; 
    int preload_num = dbtConf->preload_num; 

    dbt = palloc(sizeof(DBToaster)); 
    dbt->base_num = base_num;
    dbt->mat_num = mat_num;
    dbt->preload_num = preload_num; 
    dbt->qd_num = dbtConf->qd_num;
    dbt->qd_index = 0;  
    dbt->mat_array = palloc(sizeof(DBTMaterial *) * mat_num); 
    dbt->base_array = palloc(sizeof(DBTMaterial *) * base_num);
    dbt->preload_array = palloc(sizeof(DBTMaterial *) * preload_num); 
    dbt->qd_array = palloc(sizeof(QueryDesc *) * dbtConf->qd_num);
    dbt->executed = false; 

    DBTMatConf *matConf; 

    for (int i = 0; i < mat_num; i++)
    {
        dbtMat = palloc(sizeof(DBTMaterial)); 
        matConf = dbtConf->mat_array[i]; 

        memset(dbtMat->name, 0, sizeof(dbtMat->name)); 
        strcpy(dbtMat->name, matConf->name); 

        dbtMat->numHash = 0; 
        dbtMat->hasUpdate = true;

        dbt->mat_array[i] = dbtMat; 
        if (i + base_num >= mat_num)
            dbt->base_array[i + base_num - mat_num] = dbtMat; 
    }

    for (int i = 0; i < preload_num; i++) 
    {
        dbtPre = palloc(sizeof(DBTMaterial));
        matConf = dbtConf->preload_array[i]; 

        memset(dbtPre->name, 0, sizeof(dbtPre->name)); 
        strcpy(dbtPre->name, matConf->name); 

        dbtPre->numHash = 0; 
    
        dbtPre->local_qd = palloc(sizeof(QueryDesc *) * 1);
        dbtPre->local_ps = palloc(sizeof(PlanState *) * 1); 
        dbtPre->local_qd[0] = BuildQDfromSQL(matConf->mainsql[0]); 
        dbtPre->local_ps[0] = dbtPre->local_qd[0]->planstate; 
        dbt->qd_array[dbt->qd_index] = dbtPre->local_qd[0]; 
        dbt->qd_index++;

        dbt->preload_array[i] = dbtPre; 
    }

    for (int i = 0; i < mat_num; i++)
    {
        dbtMat = dbt->mat_array[i];
        matConf = dbtConf->mat_array[i]; 

        /* Building Local PS, base_oid, and isBuild */
        dbtMat->additional_ps = NULL;
        dbtMat->local_ps    =   palloc(sizeof(PlanState *) * base_num);
        dbtMat->local_qd    =   palloc(sizeof(QueryDesc *) * base_num);
        dbtMat->proj_ps     =   palloc(sizeof(PlanState *) * base_num);
        dbtMat->proj_qd     =   palloc(sizeof(QueryDesc *) * base_num);
        dbtMat->left        =   palloc(sizeof(DBTMaterial *) * base_num);
        dbtMat->right       =   palloc(sizeof(DBTMaterial *) * base_num);
        dbtMat->base_oid    =   palloc(sizeof(Oid) * base_num); 
        dbtMat->isBuild     =   palloc(sizeof(bool) * base_num); 
        dbtMat->parent_num  =   palloc(sizeof(int) * base_num); 
        dbtMat->parents     =   palloc(sizeof(DBTMaterial **) * base_num);
        dbtMat->joinkey     =   matConf->joinkey;
        dbtMat->joinkey_num =   matConf->joinkey_num; 
        
        int numHash = 0; 
        for (int j = 0; j < base_num; j++)
        {
            dbtMat->left[j] = NULL;
            dbtMat->right[j] = NULL;
            dbtMat->local_qd[j] = NULL;
            dbtMat->local_ps[j] = NULL;
            dbtMat->proj_qd[j] = NULL;
            dbtMat->proj_ps[j] = NULL;

            if (strcmp(matConf->mainsql[j], "null") != 0)
            {
                dbtMat->local_qd[j] = BuildQDfromSQL(matConf->mainsql[j]); 
                dbtMat->local_ps[j] = dbtMat->local_qd[j]->planstate; 
                dbt->qd_array[dbt->qd_index] = dbtMat->local_qd[j]; 
                dbt->qd_index++; 
                if (matConf->joinkey_num[j] != 0) /* Join */
                {
                    dbtMat->proj_qd[j] = BuildQDfromSQL(matConf->projsql[j]);
                    dbtMat->proj_ps[j] = dbtMat->proj_qd[j]->planstate; 
                    dbt->qd_array[dbt->qd_index] = dbtMat->proj_qd[j]; 
                    dbt->qd_index++; 

                    FindHashtoBuild(dbtMat, matConf->left[j], matConf->right[j], j);
                    dbtMat->left[j] = FindMatByName(dbt, matConf->left[j]); 
                    dbtMat->right[j] = FindMatByName(dbt, matConf->right[j]);
                    dbtMat->right[j]->numHash++; 
                }
            }

            dbtMat->parent_num[j] = matConf->parents_num[j]; 
            dbtMat->parents[j] = palloc(sizeof(DBTMaterial *) * dbtMat->parent_num[j]); 
            for (int k = 0; k < dbtMat->parent_num[j]; k++)
                dbtMat->parents[j][k] = FindMatByName(dbt, matConf->parent_names[j][k]); 
        }
    }

    for (int i = 0; i < dbt->mat_num + dbt->preload_num; i++)
    {
        if (i < dbt->mat_num)
        {
            dbtMat = dbt->mat_array[i]; 
            dbtMat->hb = BuildHashBundle(dbtMat->numHash); 
        }
        else
        {
            dbtPre = dbt->preload_array[i - mat_num];
            dbtPre->hb = BuildHashBundle(dbtPre->numHash); 
        }
    }

    /* Assign Additional PS*/
    PlanState *cur = root;
    if (cur->lefttree == NULL)
        dbt->mat_array[0]->additional_ps = NULL;
    else
    {
         for (;;)
         {
             if (cur->lefttree->lefttree == NULL)
             {
                 dbt->mat_array[0]->additional_ps = cur;
                 break;  
             }

             cur = cur->lefttree;  
         }
    }

    return dbt; 
}


static void ExecEndQD(DBToaster *dbt)
{
   // DBTMatConf ** mat_array = dbt->dbtConf->mat_array;  
   // int mat_num = dbt->mat_num, base_num = dbt->base_num; 

   // char *temp_str = malloc(sizeof(char) * STR_BUFSIZE); 
   // for (int i = 0; i < mat_num - base_num; i++)
   // {
   //     DBTMatConf *cur_mat = mat_array[i];
   //     for (int j = 0; j < base_num; j++)
   //     {
   //         if (strcmp(cur_mat->joinkey[j], "null") != 0)
   //         {
   //             memset(temp_str, 0, sizeof(temp_str)); 
   //             sprintf(temp_str, DROP_TABLE_TEMPLATE, cur_mat->tmptable[j]); 
   //             elog(NOTICE, "command %s", temp_str); 
   //             system(temp_str); /* drop the temp table */
   //         }
   //     }
   // }

    for (int i = dbt->qd_num - 1; i >= 0; i--)
    {
        QueryDesc *qd = dbt->qd_array[i]; 
	    ExecutorFinish(qd);
	    ExecutorEnd(qd);
	    FreeQueryDesc(qd);
    }
}

static void TakeNewSnapshot(EState *estate)
{
    UnregisterSnapshot(estate->es_snapshot);
    UnregisterSnapshot(estate->es_qd->snapshot);
	PopActiveSnapshot();
    
    PushActiveSnapshot(GetTransactionSnapshot());
    estate->es_qd->snapshot = RegisterSnapshot(GetActiveSnapshot());
    estate->es_snapshot =  RegisterSnapshot(GetActiveSnapshot());
}

static void ExecDBTCollectStat(DBToaster *dbt, PlanState *root, int numdelta)
{
    DBTStat *dbtStat = dbt->stat;

    for (int i = 0; i <= numdelta; i++)
        fprintf(dbtStat->timeFile, "%.2f\t", dbtStat->execTime[i]); 
    fprintf(dbtStat->timeFile, "\n"); 

    for (int i = 0; i <= numdelta; i++)
        fprintf(dbtStat->memFile, "%d\t", dbtStat->memCost[i]); 
    fprintf(dbtStat->memFile, "\n"); 

    fclose(dbtStat->memFile); 
    fclose(dbtStat->timeFile); 
}

static int ExecDBTMemCost(DBToaster *dbt, PlanState *root)
{
    int memCost = 0; 

    for (int i = 0; i < dbt->mat_num; i++)
    {
        HashBundle *hb = dbt->mat_array[i]->hb; 
        if (hb != NULL)
        {
            for (int j = 0; j < hb->table_index; j++)
            {
                HashJoinTable hashtable = hb->table_array[j]; 
                int bucketSpace = hashtable->nbuckets * sizeof(HashJoinTuple);
                memCost += (hashtable->spaceUsed + bucketSpace + 1023)/1024 ;

	            if (hashtable->spaceUsed + bucketSpace > hashtable->spacePeak)
		            hashtable->spacePeak = hashtable->spaceUsed;
            }
        }
    }

    bool estimate; 
    PlanState *cur = root; 
    for (;;)
    {
        if (cur->type == T_AggState)
            memCost += ExecAggMemoryCost(cur, &estimate); 
        else if (cur->type == T_SortState)
            memCost += ExecSortMemoryCost(cur, &estimate); 
        else
            elog(ERROR, "%d Not supported ", cur->type); 

        if (cur->lefttree->lefttree == NULL)
            break;  
        
        cur = cur->lefttree;
    }
   
    return (memCost + 1023)/1024; 
}


static void ShowMemSize(EState *estate)
{
    DBToaster *dbt = estate->dbt; 
    for (int i = 0; i < dbt->mat_num; i++)
    {
        DBTMaterial *mat = dbt->mat_array[i];
        elog(NOTICE, "table: %s", mat->name); 

        if (mat->hb != NULL)
        {
            for (int j = 0; j < mat->hb->table_index; j++)
            {
                HashJoinTable hj = mat->hb->table_array[j]; 
                int spaceUsed = hj->spaceUsed; 
                elog(NOTICE, "%d\t%d", hj->totalTuples, ((spaceUsed + 1023)/1024 + 1023)/1024); 
            }
        }
    }
}

static void ReAllocDeltaArray(EState *estate)
{
    DBToaster *dbt = estate->dbt;
    if ((estate->deltaIndex + 1) ==  estate->numDelta)
    {
        double *old_execTime = dbt->stat->execTime;
        int *old_memCost  = dbt->stat->memCost;
        dbt->stat->execTime = palloc(sizeof(double) * (estate->numDelta + 1) * 2 );
        dbt->stat->memCost  = palloc(sizeof(int) * (estate->numDelta + 1) * 2); 
        for (int i = 0; i <= estate->numDelta; i++)
        {
            dbt->stat->execTime[i] = old_execTime[i];
            dbt->stat->memCost[i]  = old_memCost[i];
        }
        estate->numDelta = (estate->numDelta + 1) * 2;
    }
}
