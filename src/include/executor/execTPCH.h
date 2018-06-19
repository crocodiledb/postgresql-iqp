/*-------------------------------------------------------------------------
 *
 * execTPCH.h
 *	  header for generating TPC-H updates 
 *
 *
 * src/include/executor/execTPCH.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef EXECTPCH_H
#define EXECTPCH_H 

extern char *tables_with_update;

typedef struct TPCH_Update TPCH_Update; 

extern TPCH_Update *BuildTPCHUpdate(char *tablenames);

extern void PopulateUpdate(TPCH_Update *update, int numdelta); 

extern bool CheckTPCHUpdate(TPCH_Update *update, int oid, int delta_index);

extern void GenTPCHUpdate(TPCH_Update *update, int delta_index); 

extern TPCH_Update *DefaultTPCHUpdate(int numDelta); 

extern bool CheckTPCHDefaultUpdate(int oid);

extern char * GetTableName(TPCH_Update *update, int oid);
#endif 

