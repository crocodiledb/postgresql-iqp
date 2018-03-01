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

extern bool CheckTPCHUpdate(TPCH_Update *update, int oid);

extern void GenTPCHUpdate(TPCH_Update *update); 

#endif 

