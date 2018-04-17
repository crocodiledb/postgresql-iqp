/*-------------------------------------------------------------------------
 *
 * execTPCH.c
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/execTPCH.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "executor/execTPCH.h"

#include <string.h>

char *tables_with_update;

#define LINEITEM_OID 26372
#define ORDERS_OID   17648
#define CUSTOMER_OID 17637
#define PARTSUPP_OID 17627
#define SUPPLIER_OID 17618
#define PART_OID     17607
#define REGION_OID   17675
#define NATION_OID   17667

#define GEN_DELTA "/home/totemtang/IQP/postgresql/pg_scripts/tpch_delta/gen_delta.sh "


struct TPCH_Update 
{
    int  numUpdates;
    int  *table_oid; 
    char **update_tables;
    char **update_commands; 
}; 

TPCH_Update *
BuildTPCHUpdate(char *tablenames)
{
    char *dup_tablenames, *name; 
    dup_tablenames = (char *) malloc(strlen(tablenames) + 1); 
    dup_tablenames[strlen(tablenames)] = 0; 
    strncpy(dup_tablenames, tablenames, strlen(tablenames)); 
    
    TPCH_Update * update = (TPCH_Update *) palloc(sizeof(TPCH_Update)); 
    update->numUpdates = 0;

    /* Calculate the number of updating tables */
    name = strtok(dup_tablenames, ",");
    while(name != NULL)
    {
        update->numUpdates++; 
        name = strtok(NULL, ","); 
    }

    /* store table names into TPCH_Update */
    update->update_tables = (char **)palloc(sizeof(char *) * update->numUpdates);
    update->update_commands = (char **)palloc(sizeof(char *) * update->numUpdates);
    update->table_oid = (int *) palloc(sizeof(int) * update->numUpdates); 

    strncpy(dup_tablenames, tablenames, strlen(tablenames));
    int i = 0;
    name = strtok(dup_tablenames, ","); 
    while(name != NULL)
    {
        if (strcmp(name, "lineitem") == 0)
            update->table_oid[i] = LINEITEM_OID;
        else if (strcmp(name, "orders") == 0)
            update->table_oid[i] = ORDERS_OID;
        else if (strcmp(name, "customer") == 0)
            update->table_oid[i] = CUSTOMER_OID;
        else if (strcmp(name, "partsupp") == 0)
            update->table_oid[i] = PARTSUPP_OID;
        else if (strcmp(name, "part") == 0)
            update->table_oid[i] = PART_OID;
        else if (strcmp(name, "supplier") == 0)
            update->table_oid[i] = SUPPLIER_OID;
        else
            elog(ERROR, "Unrecognized table name %s", name); 

        update->update_tables[i] = (char *) palloc(strlen(name) + 1); 
        update->update_tables[i][strlen(name)] = 0; 
        strncpy(update->update_tables[i], name, strlen(name));

        update->update_commands[i] = (char *) palloc(strlen(GEN_DELTA) + strlen(name) + 1); 
        update->update_commands[i][strlen(GEN_DELTA) + strlen(name)] = 0; 
        strncpy(update->update_commands[i], GEN_DELTA, strlen(GEN_DELTA)); 
        strncpy(update->update_commands[i] + strlen(GEN_DELTA), name, strlen(name)); 


        i++; 
        name = strtok(NULL, ","); 
    }

    free(dup_tablenames); 

    return update; 
}

bool
CheckTPCHUpdate(TPCH_Update *update, int oid)
{
    for(int i = 0; i < update->numUpdates; i++)
    {
        if (update->table_oid[i] == oid)
            return true;
    }
    return false; 
}

void
GenTPCHUpdate(TPCH_Update *update)
{
    for(int i = 0; i < update->numUpdates; i++)
        system(update->update_commands[i]);
}
