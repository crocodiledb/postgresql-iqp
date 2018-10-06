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
double bd_prob;

#define LINEITEM_AMP_FACTOR 1
#define ORDERS_AMP_FACTOR   0.25
#define PARTSUPP_AMP_FACTOR 4
#define PART_AMP_FACTOR     1
#define CUSTOMER_AMP_FACTOR 1
#define SUPPLIER_AMP_FACTOR 1

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
#define PARTSUPP_MAX_ROW (200000  * SCALE_FACTOR)
#define PART_MAX_ROW     (200000  * SCALE_FACTOR)
#define CUSTOMER_MAX_ROW (150000  * SCALE_FACTOR)
#define SUPPLIER_MAX_ROW (10000   * SCALE_FACTOR)

#define MAX_DELTA_NUM 10
#define MIN_DELTA_PERCENT 0.000999
#define EXP_DELTA 0.01
#define WIDTH_DELTA 0.005 

#define DELETE_DELTA    "/home/totemtang/IQP/postgresql/pg_scripts/tpch_delta/delete_delta.sh "
#define GEN_DELTA       "/home/totemtang/IQP/postgresql/pg_scripts/tpch_delta/gen_delta.sh "
#define CMD_SIZE        200
#define STR_BUFSIZE     500

/* Exist Mask for default settings */
#define DEFAULT_MASK_SIZE 3
int exist_mask[DEFAULT_MASK_SIZE] = {0x1, 0x2, 0x3};

#define CHECK_EXIST_MASK(mask, i) \
    ((mask & 1 << i) != 0)

int delta_count = 1;
tpch_delta_mode delta_mode = TPCH_UNIFORM; 

static char *newstr()
{
    char *str = palloc(sizeof(char) * STR_BUFSIZE);
    memset(str, 0, STR_BUFSIZE); 
    return str;
}

TPCH_Update *ExecInitTPCHUpdate(char *tablenames, bool wrong_prediction)
{
    TPCH_Update *update;
    if (delta_mode == TPCH_DEFAULT && !wrong_prediction) 
        update = DefaultTPCHUpdate(delta_count);
    else
        update = BuildTPCHUpdate(tablenames);

    (void) PopulateUpdate(update, delta_count, wrong_prediction); 

    return update;
}


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
    dup_tablenames = (char *) malloc(sizeof(char) * STR_BUFSIZE); 
    memset(dup_tablenames, 0, STR_BUFSIZE); 
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

    memset(dup_tablenames, 0, STR_BUFSIZE); 
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

        update->update_tables[i] = newstr(); 
        strcpy(update->update_tables[i], name);

        update->update_commands[i] = newstr();
        strcat(update->update_commands[i], GEN_DELTA); 
        strcat(update->update_commands[i], name); 

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

static double *Binomial(int *numdelta)
{
    double * delta_percent = palloc(sizeof(double) * MAX_DELTA_NUM);

    double remaining_percent = 1.0 - bd_prob;
    double cur_percent;  
    for (int i = 0; i < MAX_DELTA_NUM; i++)
    {
        cur_percent = remaining_percent * bd_prob;
        if (cur_percent < MIN_DELTA_PERCENT)
        {
            *numdelta = i; 
            return delta_percent; 
        }

        delta_percent[i] = cur_percent; 
        remaining_percent = remaining_percent - delta_percent[i]; 
    }

    *numdelta = MAX_DELTA_NUM;
    return delta_percent; 
}

int 
PopulateUpdate(TPCH_Update *update, int numdelta, bool wrong_prediction)
{
    int expected, width; 
    double max_row;
    double amp_factor; 
    char buffer[STR_BUFSIZE];
    srand(time(0)); 

    int new_numdelta = numdelta;
    double *bd_percent;

    if (delta_mode == TPCH_BINOMIAL)
        bd_percent = Binomial(&new_numdelta); 
    else if (delta_mode == TPCH_DECAY)
        new_numdelta = update->numUpdates; 
    
    update->numdelta =new_numdelta; 

    update->tpch_delta = palloc(sizeof(TPCH_Delta) * update->numUpdates);
    update->delete_commands = palloc(sizeof(char *) * update->numUpdates);
    update->table_amp_factor = palloc(sizeof(double) * update->numUpdates);
    for (int i = 0; i < update->numUpdates; i++)
    {
        update->tpch_delta[i].delta_array = palloc(sizeof(int) * (new_numdelta + 1)); 

        if (strcmp(update->update_tables[i], "lineitem") == 0)
        {
            max_row = LINEITEM_MAX_ROW;
            amp_factor = LINEITEM_AMP_FACTOR;
        }
        else if (strcmp(update->update_tables[i], "orders") == 0)
        {
            max_row = ORDERS_MAX_ROW;
            amp_factor = ORDERS_AMP_FACTOR;
        }
        else if (strcmp(update->update_tables[i], "customer") == 0)
        {
            max_row = CUSTOMER_MAX_ROW;
            amp_factor = CUSTOMER_AMP_FACTOR;
        }
        else if (strcmp(update->update_tables[i], "partsupp") == 0)
        {
            max_row = PARTSUPP_MAX_ROW;
            amp_factor = PARTSUPP_AMP_FACTOR;
        }
        else if (strcmp(update->update_tables[i], "part") == 0)
        {
            max_row = PART_MAX_ROW;
            amp_factor = PART_AMP_FACTOR;
        }
        else if (strcmp(update->update_tables[i], "supplier") == 0)
        {
            max_row = SUPPLIER_MAX_ROW; 
            amp_factor = SUPPLIER_AMP_FACTOR; 
        }
        else
            elog(ERROR, "Table %s", update->update_tables[i]);

        update->table_amp_factor[i] = amp_factor;
        update->tpch_delta[i].delta_array[new_numdelta] = max_row + 1; 
        expected = (int)(max_row * EXP_DELTA);
        width = (int) (max_row * WIDTH_DELTA); 

        for (int j = new_numdelta - 1; j >= 0; j--)
        {
            int delta_size = 0;

            if (wrong_prediction)
            {
                delta_size = expected; 
            }
            else
            {
                if (delta_mode == TPCH_DEFAULT) /* default settings */
                {
                    int mask = update->exist_mask[j];
                    if (CHECK_EXIST_MASK(mask, i))
                        delta_size = Uniform(expected, width);
                    else
                        delta_size = 0; 
                }
                else if (delta_mode == TPCH_BINOMIAL)
                {
                    delta_size = (int)(bd_percent[j] * max_row);
                }
                else if (delta_mode == TPCH_UNIFORM)
                {
                    delta_size = Uniform(expected, width); 
                }
                else if (delta_mode == TPCH_DECAY)
                {
                    if (i >= j)
                        delta_size = Uniform(expected, width);
                    else
                        delta_size = 0; 
                }
                else
                {
                    elog(ERROR, "mode %d not found", delta_mode); 
                }
            }

            update->tpch_delta[i].delta_array[j] = update->tpch_delta[i].delta_array[j + 1] - delta_size; 
        }

        update->delete_commands[i] = newstr();
        strcat(update->delete_commands[i], DELETE_DELTA); 
        strcat(update->delete_commands[i], update->update_tables[i]); 

        memset(buffer, 0, STR_BUFSIZE);
        sprintf(buffer, " %d", update->tpch_delta[i].delta_array[0]); 
        strcat(update->delete_commands[i], buffer); 
      
        if (!wrong_prediction) 
        {
            elog(NOTICE, "delete command %s", update->delete_commands[i]); 
            system(update->delete_commands[i]);
        }
    }

    return new_numdelta; 
}

int 
CheckTPCHUpdate(TPCH_Update *update, int oid, int delta_index)
{
    int begin, end;
    for(int i = 0; i < update->numUpdates; i++)
    {
        begin = update->tpch_delta[i].delta_array[delta_index];
        end = update->tpch_delta[i].delta_array[delta_index + 1]; 
        if (update->table_oid[i] == oid && begin != end)
        {
           double rows = (double)(end - begin);
           return (int)ceil(rows * update->table_amp_factor[i]); 
        }
    }
    return 0; 
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
    char buffer[STR_BUFSIZE]; 
    char *cur_update_command = malloc(sizeof(char) * STR_BUFSIZE);
    int *delta_array;

    for(int i = 0; i < update->numUpdates; i++) 
    {
        delta_array = update->tpch_delta[i].delta_array;
        if (delta_array[delta_index] == delta_array[delta_index + 1])
            continue; 

        memset(buffer, 0, STR_BUFSIZE); 
        memset(cur_update_command, 0, STR_BUFSIZE); 

        sprintf(buffer, " %d %d", update->tpch_delta[i].delta_array[delta_index], update->tpch_delta[i].delta_array[delta_index + 1]);

        strcat(cur_update_command, update->update_commands[i]); 
        strcat(cur_update_command, buffer); 

        elog(NOTICE, "update command %s", cur_update_command); 
        system(cur_update_command);
    }

    free(cur_update_command); 
}

char *GetTableName(TPCH_Update *update, int oid)
{
    for(int i = 0; i < update->numUpdates; i++)
    {
        if (update->table_oid[i] == oid)
            return update->update_tables[i];
    }
}

