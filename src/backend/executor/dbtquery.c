#include "postgres.h"

#include "executor/dbtquery.h"

#include <string.h>

#define Q3_C 52328
#define Q3_O 52325
#define Q3_L 52331

#define Q5_C        60657
#define Q5_S        52449
#define Q5_O        52452
#define Q5_L        52455
#define Q5_R_N_C    52443

#define Q7_C        77542
#define Q7_O        77548
#define Q7_L        77551
#define Q7_S        77545
#define Q7_N2_C     77536
#define Q7_N1_S     77539

#define Q8_C        78789
#define Q8_O        78907
#define Q8_S        78792
#define Q8_L        78910
#define Q8_P        78795
#define Q8_R_N_C    77784
#define Q8_N2_S     77787

#define Q9_S        78198
#define Q9_P        78201
#define Q9_PS       78204
#define Q9_O        78210
#define Q9_N_S      78195

#define Q10_C       78614
#define Q10_O       78620
#define Q10_L       78623
#define Q10_N_C     78608

#define Q1          82218

#define Q6          82269

#define Q12_O       82233
#define Q12_L       82236

#define Q14_P       82281
#define Q14_L       82284

#define Q19_P       90489
#define Q19_L       90492

Oid DBT_GetOid(char *query, char *table_name)
{
    if (strcmp(query, "q3") == 0)
    {
        if (strcmp(table_name, "q3_c") == 0)
            return Q3_C;
        else if (strcmp(table_name, "q3_o") == 0)
            return Q3_O; 
        else if (strcmp(table_name, "q3_l") == 0)
            return Q3_L;
        else
            return 0;  
    }
    else if (strcmp(query, "q5") == 0)
    {
        if (strcmp(table_name, "q5_c") == 0)
            return Q5_C;
        else if (strcmp(table_name, "q5_s") == 0)
            return Q5_S; 
        else if (strcmp(table_name, "q5_o") == 0)
            return Q5_O;
        else if (strcmp(table_name, "q5_l") == 0)
            return Q5_L; 
        else if (strcmp(table_name, "q5_r_n_c") == 0)
            return Q5_R_N_C;
        else
            return 0;  
    }
    else if (strcmp(query, "q7") == 0)
    {
        if (strcmp(table_name, "q7_c") == 0)
            return Q7_C;
        else if (strcmp(table_name, "q7_s") == 0)
            return Q7_S; 
        else if (strcmp(table_name, "q7_o") == 0)
            return Q7_O;
        else if (strcmp(table_name, "q7_l") == 0)
            return Q7_L; 
        else if (strcmp(table_name, "q7_n2_c") == 0)
            return Q7_N2_C;
        else if (strcmp(table_name, "q7_n1_s") == 0)
            return Q7_N1_S;
        else
            return 0;  
    }
    else if (strcmp(query, "q8") == 0)
    {
        if (strcmp(table_name, "q8_c") == 0)
            return Q8_C;
        else if (strcmp(table_name, "q8_s") == 0)
            return Q8_S; 
        else if (strcmp(table_name, "q8_o") == 0)
            return Q8_O;
        else if (strcmp(table_name, "q8_l") == 0)
            return Q8_L; 
        else if (strcmp(table_name, "q8_p") == 0)
            return Q8_P; 
        else if (strcmp(table_name, "q8_r_n_c") == 0)
            return Q8_R_N_C;
        else if (strcmp(table_name, "q8_n2_s") == 0)
            return Q8_N2_S;
        else
            return 0;  
    }
    else if (strcmp(query, "q9") == 0)
    {
        if (strcmp(table_name, "q9_s") == 0)
            return Q9_S;
        else if (strcmp(table_name, "q9_p") == 0)
            return Q9_P; 
        else if (strcmp(table_name, "q9_ps") == 0)
            return Q9_PS;
        else if (strcmp(table_name, "q9_o") == 0)
            return Q9_O; 
        else if (strcmp(table_name, "q9_n_s") == 0)
            return Q9_N_S; 
        else
            return 0;  
    }
    else if (strcmp(query, "q10") == 0)
    {
        if (strcmp(table_name, "q10_c") == 0)
            return Q10_C;
        else if (strcmp(table_name, "q10_o") == 0)
            return Q10_O;
        else if (strcmp(table_name, "q10_l") == 0)
            return Q10_L; 
        else if (strcmp(table_name, "q10_n_c") == 0)
            return Q10_N_C; 
        else
            return 0;  
    }
    else if (strcmp(query, "q1") == 0)
    {
        return Q1;
    }
    else if (strcmp(query, "q6") == 0)
    {
        return Q6;
    }
    else if (strcmp(query, "q12") == 0)
    {
        if (strcmp(table_name, "q12_o") == 0)
            return Q12_O;
        else if (strcmp(table_name, "q12_l") == 0)
            return Q12_L;
        else
            return 0;  
    }
    else if (strcmp(query, "q14") == 0)
    {
        if (strcmp(table_name, "q14_p") == 0)
            return Q14_P;
        else if (strcmp(table_name, "q14_l") == 0)
            return Q14_L;
        else
            return 0;  
    }
    else if (strcmp(query, "q19") == 0)
    {
        if (strcmp(table_name, "q19_p") == 0)
            return Q19_P;
        else if (strcmp(table_name, "q19_l") == 0)
            return Q19_L;
        else
            return 0;  
    }
    else
    {
        elog(ERROR, "Not Supporting Query %s", query); 
    }

    return 0; 
}
