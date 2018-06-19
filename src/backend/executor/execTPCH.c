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

#define LINEITEM_OID 35147
#define ORDERS_OID   35139
#define CUSTOMER_OID 35131
#define PARTSUPP_OID 35123
#define SUPPLIER_OID 35115
#define PART_OID     35107
#define REGION_OID   35160
#define NATION_OID   35155

#define SCALE_FACTOR 1

#define LINEITEM_MAX_ROW (6000000 * SCALE_FACTOR)
#define ORDERS_MAX_ROW   (6000000 * SCALE_FACTOR)
#define CUSTOMER_MAX_ROW (150000  * SCALE_FACTOR)
#define PARTSUPP_MAX_ROW (200000  * SCALE_FACTOR)
#define SUPPLIER_MAX_ROW (10000   * SCALE_FACTOR)
#define PART_MAX_ROW     (200000  * SCALE_FACTOR)

#define EXP_DELTA 0.01
#define WIDTH_DELTA 0.005 

#define DELETE_DELTA    "/home/totemtang/IQP/postgresql/pg_scripts/tpch_delta/delete_delta.sh "
#define GEN_DELTA       "/home/totemtang/IQP/postgresql/pg_scripts/tpch_delta/gen_delta.sh "
#define CMD_SIZE        200

/* Exist Mask for default settings */
#define DEFAULT_MASK_SIZE 3
int exist_mask[DEFAULT_MASK_SIZE] = {0x1, 0x2, 0x3};

#define CHECK_EXIST_MASK(mask, i) \
    ((mask & 1 << i) != 0)

typedef struct TPCH_Delta
{
    int max_row; 
    int *delta_array; 
} TPCH_Delta; 

struct TPCH_Update 
{
    int  numUpdates;
    int  *table_oid; 
    char **update_tables;
    char **update_commands;
    char **delete_commands;
    int numdelta;
    TPCH_Delta *tpch_delta; 
    int *exist_mask;  
}; 

TPCH_Update *
DefaultTPCHUpdate(int numDelta)
{
    TPCH_Update * update = BuildTPCHUpdate("lineitem,orders");
    update->exist_mask = palloc(sizeof(int) * numDelta);

    srand(time(0));

    for (int i = 0; i < numDelta; i++)
    {
        update->exist_mask[i] = exist_mask[rand() % DEFAULT_MASK_SIZE]; 
    }

    return update;
}

TPCH_Update *
BuildTPCHUpdate(char *tablenames)
{
    char *dup_tablenames, *name; 
    dup_tablenames = (char *) malloc(strlen(tablenames) + 1); 
    dup_tablenames[strlen(tablenames)] = 0; 
    strncpy(dup_tablenames, tablenames, strlen(tablenames)); 
    
    TPCH_Update * update = (TPCH_Update *) palloc(sizeof(TPCH_Update));
    update->exist_mask = NULL;  
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

        update->update_commands[i] = (char *) palloc(strlen(GEN_DELTA) + strlen(name) + CMD_SIZE);
        memset(update->update_commands[i], 0, sizeof(update->update_commands[i])); 
        strncpy(update->update_commands[i], GEN_DELTA, strlen(GEN_DELTA)); 
        strncpy(update->update_commands[i] + strlen(GEN_DELTA), name, strlen(name)); 


        i++; 
        name = strtok(NULL, ","); 
    }

    free(dup_tablenames); 

    return update; 
}

static 
int Uniform(int expected, int width)
{
    //int min, max;
    //min = expected - width; 
    //max = expected + width; 
    //return rand()%(max - min) + min; 
    return expected;  
}

void
PopulateUpdate(TPCH_Update *update, int numdelta)
{
    int expected, width; 
    double max_row; 
    char buffer[50];
    srand(time(0)); 

    update->numdelta = numdelta; 
    update->tpch_delta = palloc(sizeof(TPCH_Delta) * update->numUpdates);
    update->delete_commands = palloc(sizeof(char *) * update->numUpdates); 
    for (int i = 0; i < update->numUpdates; i++)
    {
        update->tpch_delta[i].delta_array = palloc(sizeof(int) * (numdelta + 1)); 

        if (strcmp(update->update_tables[i], "lineitem") == 0)
            max_row = LINEITEM_MAX_ROW;
        else if (strcmp(update->update_tables[i], "orders") == 0)
            max_row = ORDERS_MAX_ROW; 
        else if (strcmp(update->update_tables[i], "customer") == 0)
            max_row = CUSTOMER_MAX_ROW; 
        else if (strcmp(update->update_tables[i], "partsupp") == 0)
            max_row = PARTSUPP_MAX_ROW;
        else if (strcmp(update->update_tables[i], "part") == 0)
            max_row = PART_MAX_ROW;
        else if (strcmp(update->update_tables[i], "supplier") == 0)
            max_row = SUPPLIER_MAX_ROW; 
        else
            elog(ERROR, "Table %s", update->update_tables[i]);

        update->tpch_delta[i].delta_array[numdelta] = max_row + 1; 
        expected = (int)(max_row * EXP_DELTA);
        width = (int) (max_row * WIDTH_DELTA); 

        for (int j = numdelta - 1; j >= 0; j--)
        {
            int delta_size = 0;
            if (update->exist_mask != NULL)
            {
                int mask = update->exist_mask[j];
                if (CHECK_EXIST_MASK(mask, i))
                    delta_size = Uniform(expected, width);
                else
                    delta_size = 0; 
            }
            else
                delta_size = Uniform(expected, width); 
            update->tpch_delta[i].delta_array[j] = update->tpch_delta[i].delta_array[j + 1] - delta_size; 
        }

        update->delete_commands[i] = (char *) palloc(strlen(DELETE_DELTA) + strlen(update->update_tables[i]) + CMD_SIZE);
        memset(update->delete_commands[i], 0, sizeof(update->delete_commands[i])); 
        strncpy(update->delete_commands[i], DELETE_DELTA, strlen(DELETE_DELTA)); 
        strncpy(update->delete_commands[i] + strlen(DELETE_DELTA), update->update_tables[i], strlen(update->update_tables[i])); 

        memset(buffer, 0, sizeof(buffer));
        sprintf(buffer, " %d", update->tpch_delta[i].delta_array[0]); 
        strncpy(update->delete_commands[i] + strlen(update->delete_commands[i]), buffer, strlen(buffer)); 
       
        elog(NOTICE, "delete command %s", update->delete_commands[i]); 
        system(update->delete_commands[i]);
    }
}

bool
CheckTPCHUpdate(TPCH_Update *update, int oid, int delta_index)
{
    for(int i = 0; i < update->numUpdates; i++)
    {
        if (update->table_oid[i] == oid && update->tpch_delta[i].delta_array[delta_index] != 0)
            return true;
    }
    return false; 
}

bool 
CheckTPCHDefaultUpdate(int oid)
{
    if (oid == LINEITEM_OID || oid == ORDERS_OID)
        return true;
    else 
        return false; 
}

void
GenTPCHUpdate(TPCH_Update *update, int delta_index)
{
    char buffer[100]; 
    int *delta_array;

    for(int i = 0; i < update->numUpdates; i++) 
    {
        delta_array = update->tpch_delta[i].delta_array;
        if (delta_array[delta_index] == delta_array[delta_index + 1])
            continue; 

        int original_len = strlen(update->update_commands[i]); 

        memset(buffer, 0, sizeof(buffer)); 
        sprintf(buffer, " %d %d", update->tpch_delta[i].delta_array[delta_index], update->tpch_delta[i].delta_array[delta_index + 1]);
        strncpy(update->update_commands[i] + strlen(update->update_commands[i]), buffer, strlen(buffer)); 

        elog(NOTICE, "update command %s", update->update_commands[i]); 
        system(update->update_commands[i]);

        memset(update->update_commands[i] + original_len, 0, strlen(buffer)); 
    }
}

char *GetTableName(TPCH_Update *update, int oid)
{
    for(int i = 0; i < update->numUpdates; i++)
    {
        if (update->table_oid[i] == oid)
            return update->update_tables[i];
    }
}
