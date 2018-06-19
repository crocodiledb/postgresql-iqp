
#ifndef HASHBUNDLE_H
#define HASHBUNDLE_H

#include "postgres.h"
#include "nodes/execnodes.h"

typedef struct HashBundle
{
    int           table_index; 
    int           table_num; 
    HashJoinTable *table_array;
    List          **hashkeys_array; 
    bool          *outer_tuple_array; 
    ExprContext    **econtext_array; 
} HashBundle; 


extern HashBundle *BuildHashBundle(int table_num); 
extern void HashBundleAddTable(HashBundle *hb, HashJoinTable table, ExprContext *econtext, List *hashkeys, bool outer_tuple); 
extern void HashBundleInsert(HashBundle *hb, TupleTableSlot *slot); 

#endif
