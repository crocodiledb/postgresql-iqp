
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
    char          ***joinkey; 
    int           *joinkey_num; 
} HashBundle; 


extern HashBundle *BuildHashBundle(int table_num); 
extern HashJoinTable HashBundleAddTable(HashBundle *hb, HashJoinTable table, ExprContext *econtext, \ 
                                        List *hashkeys, bool outer_tuple, char **joinkey, int joinkey_num); 
extern void HashBundleInsert(HashBundle *hb, TupleTableSlot *slot); 

#endif
