/*-------------------------------------------------------------------------
 *
 * incTQPool.c
 *      Tuple Queue Pool  
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/incTQPool.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "executor/incTQPool.h"
#include "utils/rel.h"

#include <sys/types.h>

struct IncTQPool
{
    MemoryContext mc; 
    IncTupQueueReader **pool_reader; 
    int maxTQ; 
}; 

IncTQPool *
CreateIncTQPool(MemoryContext mc, int maxTQ)
{
    MemoryContext old = MemoryContextSwitchTo(mc); 

    IncTQPool *pool = palloc(sizeof(IncTQPool)); 
    pool->mc = mc;
    pool->maxTQ = maxTQ;  
    pool->pool_reader = palloc(sizeof(IncTupQueueReader *) * maxTQ); 

    for (int i = 0; i < maxTQ; i++)
    {
        pool->pool_reader[i] = NULL; 
    }

    (void)MemoryContextSwitchTo(old); 

    return pool; 
}


bool 
AddIncTQReader(IncTQPool * tq_pool, Relation r, TupleDesc tupledesc)
{
    MemoryContext old; 
    int i;
    IncTupQueueReader **pool_reader = tq_pool->pool_reader; 

    for (i = 0; i < tq_pool->maxTQ; i++)
    {
        if (pool_reader[i] == NULL)
            break; 
        if (pool_reader[i]->tq_key  == GEN_TQ_KEY(r))
            return false; 
    }

    /* Not exist, Add one */
    old = MemoryContextSwitchTo(tq_pool->mc);

    pool_reader[i] = CreateIncTupQueueReader(r, tupledesc);
    OpenIncTupQueueReader(pool_reader[i]);

    (void)MemoryContextSwitchTo(old); 

    return true;  
}

int 
GetTQUpdate(IncTQPool *tq_pool)
{
    IncTupQueueReader **pool_reader = tq_pool->pool_reader; 
    int update_sum = 0;
    int update_count = 0;

    for(int i = 0; i < tq_pool->maxTQ; i++) 
    {
        if (pool_reader[i] != NULL)
        {
            update_count = GetIncTupQueueSize(pool_reader[i]);
            update_sum += update_count;
            //elog(NOTICE, "key %d: Delta %d", pool_reader[i]->tq_key, update_count);
        }
    }
    
    return update_sum; 
}

bool 
HasTQUpdate(IncTQPool *tq_pool, Relation r)
{
    IncTupQueueReader **pool_reader = tq_pool->pool_reader; 
    key_t key = GEN_TQ_KEY(r); 

    for(int i = 0; i < tq_pool->maxTQ; i++) 
    {
        if (pool_reader[i] == NULL)
        {
            elog(ERROR, "No Relation %d in TQ", key); 
            return false;
        }
        else if (pool_reader[i]->tq_key == key)
        {
            return (GetIncTupQueueSize(pool_reader[i]) > 0);  
        }
    }
            
    elog(ERROR, "No Relation %d in TQ", key); 
    return false;
}

bool
IsTQComplete(IncTQPool *tq_pool, Relation r)
{
    IncTupQueueReader **pool_reader = tq_pool->pool_reader; 
    key_t key = GEN_TQ_KEY(r); 

    for(int i = 0; i < tq_pool->maxTQ; i++) 
    {
        if (pool_reader[i] == NULL)
        {
            elog(ERROR, "No Relation %d in TQ", key); 
            return false;
        }
        else if (pool_reader[i]->tq_key == key)
        {
            return GetIncTQComplete(pool_reader[i]);
        }
    }
            
    elog(ERROR, "No Relation %d in TQ", key); 
    return false;
}

IncTupQueueReader * 
GetTQReader(IncTQPool *tq_pool, Relation r, IncTupQueueReader *ss_reader)
{
    IncTupQueueReader *ret; 
    IncTupQueueReader **pool_reader = tq_pool->pool_reader; 
    key_t key = GEN_TQ_KEY(r); 

    for (int i = 0; i < tq_pool->maxTQ; i++)
    {
        if (pool_reader[i] == NULL)
        {
            elog(ERROR, "No Relation %d in TQ", key); 
            return NULL; 
        }

        if (pool_reader[i]->tq_key == key)
        {
            MemoryContext old = MemoryContextSwitchTo(tq_pool->mc);
            ret = GetIncTupQueueSnapShot(pool_reader[i], ss_reader); 
            (void)MemoryContextSwitchTo(old); 
            return ret; 
        }
    }

    elog(ERROR, "No Relation %d in TQ", key); 
    return NULL; 
}

void 
DrainTQReader(IncTQPool *tq_pool, IncTupQueueReader *ss_reader)
{
    if (ss_reader == NULL)
        return; 

    IncTupQueueReader **pool_reader = tq_pool->pool_reader; 
    key_t key = ss_reader->tq_key; 

    for (int i = 0; i < tq_pool->maxTQ; i++)
    {
        if (pool_reader[i] == NULL)
            elog(ERROR, "No Relation %d in TQ", key); 

        if (pool_reader[i]->tq_key == key)
            return DrainIncTupQueue(pool_reader[i], ss_reader);
    }

    elog(ERROR, "No Relation %d in TQ", key); 
}


void 
DestroyIncTQPool(IncTQPool *tq_pool)
{
    IncTupQueueReader **pool_reader = tq_pool->pool_reader; 
    MemoryContext old = MemoryContextSwitchTo(tq_pool->mc); 

    for (int i = 0; i < tq_pool->maxTQ; i++) 
    {
        if (pool_reader[i] != NULL)
        {
            CloseIncTupQueueReader(pool_reader[i]); 
            DestroyIncTupQueueReader(pool_reader[i]); 
        }
    }

    (void)MemoryContextSwitchTo(old); 
}

