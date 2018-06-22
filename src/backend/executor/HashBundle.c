#include "postgres.h"

#include "access/htup_details.h"
#include "executor/executor.h"
#include "executor/hashjoin.h"
#include "executor/nodeHash.h"
#include "executor/nodeHashjoin.h"

#include "executor/HashBundle.h"

HashBundle *BuildHashBundle(int table_num)
{
    if (table_num == 0)
        return NULL;

    HashBundle *hb = palloc(sizeof(HashBundle)); 

    hb->table_index = 0; 
    hb->table_num = table_num; 

    hb->table_array = palloc(sizeof(HashJoinTable) * table_num); 
    hb->hashkeys_array = palloc(sizeof(List *) * table_num); 
    hb->outer_tuple_array = palloc(sizeof(bool) * table_num);
    hb->econtext_array = palloc(sizeof(ExprContext) * table_num); 
    hb->joinkey = palloc(sizeof(char **) * table_num);
    hb->joinkey_num = palloc(sizeof(int) * table_num);

    return hb;  
}

void HashBundleAddTable(HashBundle *hb, HashJoinTable table, ExprContext *econtext, List *hashkeys, bool outer_tuple)
{
   // for (int i = 0; i < hb->table_index; i++)
   // {
   //     if (hb->joinkey_num[i] == joinkey_num)
   //     {
   //         for (int j = 0; j < joinekey_num; j++)
   //         {
   //         }
   //     }
   // }

    hb->table_array[hb->table_index] = table;
    hb->econtext_array[hb->table_index] = econtext; 
    hb->hashkeys_array[hb->table_index] = hashkeys; 
    hb->outer_tuple_array[hb->table_index] = outer_tuple; 

    hb->table_index++; 

}

void HashBundleInsert(HashBundle *hb, TupleTableSlot *slot)
{
    HashJoinTable hashtable; 
    ExprContext *econtext; 
    List *hashkeys; 
    bool outer_tuple; 

    uint32 hashvalue; 

    for (int i = 0; i < hb->table_num; i++)
    {
        hashtable = hb->table_array[i]; 
        econtext = hb->econtext_array[i]; 
        hashkeys = hb->hashkeys_array[i]; 
        outer_tuple = hb->outer_tuple_array[i];

        if (outer_tuple)
            econtext->ecxt_outertuple = slot; 
        else
            econtext->ecxt_innertuple = slot; 
        
        ExecHashGetHashValue(hashtable, econtext, hashkeys,
                						 outer_tuple, false,
            							 &hashvalue); 
        
        ExecHashTableInsert(hashtable, slot, hashvalue);
        hashtable->totalTuples += 1;
    }
}




















