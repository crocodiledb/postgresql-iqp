#include "postgres.h"

#include "executor/iqpquery.h"

#include <string.h>

#define I3_C 52279
#define I3_O 52276
#define I3_L 52282

#define I5_R 78757
#define I5_N 78754
#define I5_C 78739
#define I5_S 78742
#define I5_O 78745
#define I5_L 78748

#define I7_C  78761
#define I7_O  78767
#define I7_L  78782
#define I7_S  78764
#define I7_N1 78779
#define I7_N2 78776

#define I8_C  78837
#define I8_O  78846
#define I8_L  78849
#define I8_P  78843
#define I8_S  78840
#define I8_R_N 78855
#define I8_N2 78858

#define I9_S  78862
#define I9_P  78865
#define I9_PS  78868
#define I9_O  78874
#define I9_L  78877
#define I9_N 78883

#define I10_C  78886
#define I10_O  78892
#define I10_L  78895
#define I10_N  78901

#define I1_L   90510

#define I6_L   90516

#define I12_O  90525
#define I12_L  90522

#define I14_P  90534
#define I14_L  90528

#define I19_P  90543
#define I19_L  90537


Oid IQP_GetOid(char *query, char *table_name)
{
    if (strcmp(query, "q3") == 0)
    {
        if (strcmp(table_name, "i3_c") == 0)
            return I3_C;
        else if (strcmp(table_name, "i3_o") == 0)
            return I3_O; 
        else if (strcmp(table_name, "i3_l") == 0)
            return I3_L;
        else
            return 0;  
    }
    else if (strcmp(query, "q5") == 0)
    {
        if (strcmp(table_name, "i5_c") == 0)
            return I5_C;
        else if (strcmp(table_name, "i5_o") == 0)
            return I5_O; 
        else if (strcmp(table_name, "i5_l") == 0)
            return I5_L;
        else if (strcmp(table_name, "i5_r") == 0)
            return I5_R; 
        else if (strcmp(table_name, "i5_n") == 0)
            return I5_N;
        else if (strcmp(table_name, "i5_s") == 0)
            return I5_S;
        else
            return 0;  
    }
    else if (strcmp(query, "q7") == 0)
    {
        if (strcmp(table_name, "i7_c") == 0)
            return I7_C;
        else if (strcmp(table_name, "i7_o") == 0)
            return I7_O; 
        else if (strcmp(table_name, "i7_l") == 0)
            return I7_L;
        else if (strcmp(table_name, "i7_n1") == 0)
            return I7_N1; 
        else if (strcmp(table_name, "i7_n2") == 0)
            return I7_N2;
        else if (strcmp(table_name, "i7_s") == 0)
            return I7_S;
        else
            return 0;  
    }
    else if (strcmp(query, "q8") == 0)
    {
        if (strcmp(table_name, "i8_c") == 0)
            return I8_C;
        else if (strcmp(table_name, "i8_o") == 0)
            return I8_O; 
        else if (strcmp(table_name, "i8_l") == 0)
            return I8_L;
        else if (strcmp(table_name, "i8_r_n") == 0)
            return I8_R_N; 
        else if (strcmp(table_name, "i8_n2") == 0)
            return I8_N2;
        else if (strcmp(table_name, "i8_s") == 0)
            return I8_S;
        else if (strcmp(table_name, "i8_p") == 0)
            return I8_P;
        else
            return 0;  
    }
    else if (strcmp(query, "q9") == 0)
    {
        if (strcmp(table_name, "i9_s") == 0)
            return I9_S;
        else if (strcmp(table_name, "i9_p") == 0)
            return I9_P; 
        else if (strcmp(table_name, "i9_ps") == 0)
            return I9_PS;
        else if (strcmp(table_name, "i9_o") == 0)
            return I9_O; 
        else if (strcmp(table_name, "i9_l") == 0)
            return I9_L;
        else if (strcmp(table_name, "i9_n") == 0)
            return I9_N;
        else
            return 0;  
    }
    else if (strcmp(query, "q10") == 0)
    {
        if (strcmp(table_name, "i10_c") == 0)
            return I10_C;
        else if (strcmp(table_name, "i10_o") == 0)
            return I10_O; 
        else if (strcmp(table_name, "i10_l") == 0)
            return I10_L;
        else if (strcmp(table_name, "i10_n") == 0)
            return I10_N;
        else
            return 0;  
    }
    else if (strcmp(query, "q1") == 0)
    {
        if (strcmp(table_name, "i1_l") == 0)
            return I1_L;
        else
            return 0; 
    }
    else if (strcmp(query, "q6") == 0)
    {
        if (strcmp(table_name, "i6_l") == 0)
            return I6_L;
        else
            return 0;
    }
    else if (strcmp(query, "q12") == 0)
    {
        if (strcmp(table_name, "i12_o") == 0)
            return I12_O;
        else if (strcmp(table_name, "i12_l") == 0)
            return I12_L;
        else
            return 0;  
    }
    else if (strcmp(query, "q14") == 0)
    {
        if (strcmp(table_name, "i14_p") == 0)
            return I14_P;
        else if (strcmp(table_name, "i14_l") == 0)
            return I14_L;
        else
            return 0;  
    }
    else if (strcmp(query, "q19") == 0)
    {
        if (strcmp(table_name, "i19_p") == 0)
            return I19_P;
        else if (strcmp(table_name, "i19_l") == 0)
            return I19_L;
        else
            return 0;  
    }
    else
    {
        elog(ERROR, "Not Supporting Query %s", query); 
    }

    return 0; 
}
