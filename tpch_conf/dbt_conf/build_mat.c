#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#define STR_BUFSIZE 50
#define bool char
#define false 0 
#define true  1

#define R       0
#define N       10
#define C       20
#define N2_C    21
#define N_C     22
#define R_N_C   30
#define S       40
#define N1_S    41
#define N2_S    42
#define N_S     43
#define P       50
#define PS      60
#define O       70
#define L       90

#define NULL_STR "null"

char *dbt_query = "q8"; 

typedef struct Base
{
    int     id; 
    char    *name;

    int     nb_num; 
    char    **nb_name; 
    char    **nb_key;
    int     *nb_id;
    bool    *nb_visited;  

    int     target_num;
    char    **target_name;
    char    **target_type;
} Base; 

typedef struct Mat
{
    int  name_num;  
    char **name; 
    bool *isvalid; 

    int  nb_num; 
    char **nb_key;
    char **nb_name;

    int  target_num; 
    char **target_name; 
    char **target_type;

    int base_num;
    char *print_name;  
    struct Mat **left; 
    struct Mat **right; 
    int  *joinkey_num; 
    char ***joinkey;

    int  *parents_num; 
    struct Mat ***parents; 

    int  *depth; 
} Mat; 

Mat **total_mat_array; 
int total_mat_num; 

Base * FindBaseByID(Base **base_array, int base_num, int id);
void CleanVisited(Base **base_set, int set_size); 
Mat *BuildMat(Base **base_array, int base_num, bool *cur_set);
Mat **AddMat(Mat **mat_array, int num, Mat *mat); 
void GenMat(Base **base_array, int base_num, bool *cur_set, int index); 
char *newstr();
void populateID(Base **base_array, int base_num); 
bool Subsume(Mat *a, Mat *b); 

void CleanVisited(Base **base_set, int set_size)
{
    for (int i = 0; i < set_size; i++)
    {
        Base *cur_base = base_set[i];
        for (int j = 0; j < cur_base->nb_num; j++)
            cur_base->nb_visited[j] = false; 
    }
}

Mat *BuildMat(Base **base_array, int base_num, bool *cur_set)
{
    /* Generate Set */
    int set_size = 0;
    for (int i = 0; i < base_num; i++)
        if (cur_set[i])
            set_size++;

    if (set_size == 0)
        return NULL; 

    Base **base_set = malloc(sizeof(Base *) * set_size);

    for (int i = 0, j = 0; i < base_num; i++)
    {
        if (cur_set[i])
        {
            base_set[j] = base_array[i]; 
            j++; 
        }
    }

    if (set_size > 1)
    {
        for (int i = 0; i < set_size; i++)
        {
            Base *cur_base = base_set[i];
            bool found = false; 
            for (int j = 0; j < cur_base->nb_num; j++)
            {
                if (cur_base->nb_visited[j])
                    continue; 

                Base *match_base = FindBaseByID(base_set, set_size, cur_base->nb_id[j]);

                if (match_base != NULL)
                {
                    found = true; 
                    cur_base->nb_visited[j] = true; 
                }
            }
            if (!found)
            {
                /* Not a Connected Subgraph*/
                CleanVisited(base_set, set_size); 
                return NULL; 
            }
        }
    }

    /*Found a Connected Subgraph*/
    Mat *mat = malloc(sizeof(Mat)); 
    mat->name_num = set_size; 
    mat->name = malloc(sizeof(char *) * set_size);

    int nb_num = 0, target_num = 0; 
    for (int i = 0; i < set_size; i++)
    {
        printf("%s_", base_set[i]->name); 
        mat->name[i] = newstr();  
        strcpy(mat->name[i], base_set[i]->name); 

        for (int j = 0; j < base_set[i]->nb_num; j++)
        {
            if (!base_set[i]->nb_visited[j])
                nb_num++; 
        }
        target_num += base_set[i]->target_num; 
    }

    printf ("\t"); 

    mat->nb_num = nb_num;
    mat->nb_key = malloc(sizeof(char *) * nb_num); 
    mat->nb_name = malloc(sizeof(char *) * nb_num); 
    mat->target_num = target_num;
    mat->target_name = malloc(sizeof(char *) * target_num);
    mat->target_type = malloc(sizeof(char *) * target_num); 

    int nb_index = 0, target_index = 0; 
    for (int i = 0; i < set_size; i++)
    {
        for (int j = 0; j < base_set[i]->nb_num; j++)
        {
            if (!base_set[i]->nb_visited[j])
            {
                printf("%s:%s\t", base_set[i]->nb_name[j], base_set[i]->nb_key[j]); 
                mat->nb_key[nb_index] = newstr();
                mat->nb_name[nb_index] = newstr();
                strcpy(mat->nb_key[nb_index], base_set[i]->nb_key[j]); 
                strcpy(mat->nb_name[nb_index], base_set[i]->nb_name[j]); 
                nb_index++; 
            }
        }

        for (int j = 0; j < base_set[i]->target_num; j++)
        {
            printf("%s:%s\t",base_set[i]->target_name[j], base_set[i]->target_type[j]); 
            mat->target_name[target_index] = newstr();
            mat->target_type[target_index] = newstr(); 
            strcpy(mat->target_name[target_index], base_set[i]->target_name[j]); 
            strcpy(mat->target_type[target_index], base_set[i]->target_type[j]); 
            target_index++; 
        }
    }

    printf("\n"); 
    
    CleanVisited(base_set, set_size); 

    return mat; 
}

Mat **AddMat(Mat **mat_array, int num, Mat *mat)
{
    Mat **mat_new_array = malloc(sizeof(Mat *) * (num + 1));
    for (int i = 0; i < num; i++)
        mat_new_array[i] = mat_array[i];
    mat_new_array[num] = mat;

    return mat_new_array; 
}

void GenMat(Base **base_array, int base_num, bool *cur_set, int index)
{
    if (index == base_num)
    {
        Mat * mat = BuildMat(base_array, base_num, cur_set); 
        if (mat != NULL)
        {
            total_mat_array = AddMat(total_mat_array, total_mat_num, mat); 
            total_mat_num++; 
        }

        return; 
    }

    cur_set[index] = false; 
    GenMat(base_array, base_num, cur_set, index + 1); 

    cur_set[index] = true; 
    GenMat(base_array, base_num, cur_set, index + 1); 
}

char *newstr()
{
    char * str = malloc(sizeof(char) * STR_BUFSIZE); 
    memset(str, 0, STR_BUFSIZE); 
    return str; 
}

void populateID(Base **base_array, int base_num)
{
    for (int i = 0; i < base_num; i++)
    {
        Base *cur_base = base_array[i]; 

        for (int j = 0; j < cur_base->nb_num; j++)
        {
            char *nb_name = cur_base->nb_name[j]; 

            for (int k = 0; k < base_num; k++)
            {
                if (strcmp(nb_name, base_array[k]->name) == 0)
                {
                    cur_base->nb_id[j] = base_array[k]->id; 
                    break; 
                }
            }
        }
    }
}

Base * FindBaseByID(Base **base_array, int base_num, int id)
{
    for (int i = 0; i < base_num; i++)
    {
        if (base_array[i]->id == id)
            return base_array[i]; 
    }

    return NULL; 
}

int comparator(const void* p1, const void* p2)
{
    Mat *mat1 = (Mat *)p1; 
    Mat *mat2 = (Mat *)p2; 
    return (mat2->name_num - mat1->name_num); 
}

void SortByLayer(Mat **mat_array, int mat_num)
{
    qsort(mat_array, mat_num, sizeof(Mat *), comparator); 
}

int findnameval(char *name)
{
    int name_val; 

    if (strcmp(name, "r") == 0)
        name_val = R;
    else if (strcmp(name, "n") == 0)
        name_val = N; 
    else if (strcmp(name, "c") == 0)
        name_val = C;
    else if (strcmp(name, "r_n_c") == 0)
        name_val = R_N_C;
    else if (strcmp(name, "s") == 0)
        name_val = S;
    else if (strcmp(name, "p") == 0)
        name_val = P;
    else if (strcmp(name, "ps") == 0)
        name_val = PS;
    else if (strcmp(name, "o") == 0)
        name_val = O;
    else if (strcmp(name, "l") == 0)
        name_val = L;
    else if (strcmp(name, "n2_c") == 0)
        name_val = N2_C; 
    else if (strcmp(name, "n_c") == 0)
        name_val = N_C; 
    else if (strcmp(name, "n1_s") == 0)
        name_val = N1_S;
    else if (strcmp(name, "n2_s") == 0)
        name_val = N2_S; 
    else if (strcmp(name, "n_s") == 0)
        name_val = N_S; 
    else 
        printf("Error: %s\n", name); 

    return name_val; 
}

int name_comparator(const void* p1, const void* p2)
{
    char *name1 = *(char **)p1; 
    char *name2 = *(char **)p2; 
    int name1_val, name2_val; 

    name1_val = findnameval(name1);
    name2_val = findnameval(name2); 

    return (name1_val - name2_val); 
}

void SortNames(Mat *mat)
{
    qsort(mat->name, mat->name_num, sizeof(char *), name_comparator); 
}

int multiname_comparator(const void *p1, const void* p2)
{
    Mat *mat1 = *(Mat **)p1; 
    Mat *mat2 = *(Mat **)p2;

    for (int i = 0; i < mat1->name_num; i++)
    {
        int name1 = findnameval(mat1->name[i]); 
        int name2 = findnameval(mat2->name[i]); 

        if (name1 < name2)
            return -1;
        else if (name1 > name2)
            return 1; 

        continue; 
    }

    printf("ERROR: two same mats\n"); 
    return 0; 
}

void SortMatByNames(Mat **mat_array, int mat_num)
{
    qsort(mat_array, mat_num, sizeof(Mat *), multiname_comparator); 
}

bool Subsume(Mat *a, Mat *b)
{
    assert(a->name_num > b->name_num); 
    int i = 0, j = 0; 

    for (; i < a->name_num && j < b->name_num;)
    {
        if (strcmp(a->name[i], b->name[j]) == 0)
        {
            i++;
            j++; 
        }
        else
        {
            i++;  
        }
    }

    if (j == b->name_num)
        return true;
    else 
        return false; 
}

void SetupMatInfo(Mat *mat, int base_num)
{
    mat->base_num = base_num; 

    mat->left = malloc(sizeof(Mat *) * base_num); 
    mat->right = malloc(sizeof(Mat *) * base_num); 
    mat->joinkey_num = malloc(sizeof(int) * base_num); 
    mat->joinkey = malloc(sizeof(char **) * base_num); 
    mat->parents_num = malloc(sizeof(int) * base_num); 
    mat->parents = malloc(sizeof(Mat **) * base_num);
    mat->depth = malloc(sizeof(int) * base_num); 

    for (int i = 0; i < base_num; i++)
    {
        mat->depth[i] = 0; 
        mat->parents_num[i] = 0; 
    }

    mat->isvalid  =  malloc(sizeof(bool) * mat->name_num);
    for (int i = 0; i < mat->name_num; i++)
        mat->isvalid[i] = true; 
}

void AddParentMat(Mat *mat, int base_index,  Mat *parent)
{
    mat->parents[base_index] = AddMat(mat->parents[base_index], mat->parents_num[base_index], parent); 
    mat->parents_num[base_index]++; 
}

bool MatchChild(Mat *mat, Mat *child)
{
    int i = 0, j = 0; 
    for (; i < mat->name_num && j < child->name_num;)
    {
        if (!mat->isvalid[i])
        {
            i++; 
            continue; 
        }

        if (strcmp(mat->name[i], child->name[j]) != 0)
            return false; 
        i++;
        j++; 
    }

    assert(j == child->name_num); 

    return true; 
}

void CleanValid(Mat *mat)
{
    for (int i = 0; i < mat->name_num; i++)
        mat->isvalid[i] = true; 
}

void AddMatInfo(Mat *mat, Mat *left, Mat *right, char **joinkey, int joinkey_num, int base_index)
{
    mat->left[base_index] = left; 
    mat->right[base_index] = right; 
    mat->joinkey[base_index] = joinkey;
    mat->joinkey_num[base_index] = joinkey_num; 
    
    AddParentMat(left, base_index, mat); 
}

char **FindJoinKey(Mat *mat, Mat *base, int * joinkey_num)
{
    char **joinkey_array = malloc(sizeof(char *) * 2);
    int num = 0;  
    for (int i = 0; i < mat->nb_num; i++)
    {
        if (strcmp(mat->nb_name[i], base->name[0]) == 0)
        {
            joinkey_array[num] = newstr(); 
            strcpy(joinkey_array[num], mat->nb_key[i]);
            num++; 
        }
    }

    if (num  == 0)
    {
        printf("ERROR, not found joinkey\n"); 
        return NULL; 
    }
    else
    {
        *joinkey_num = num; 
        return joinkey_array; 
    }
}

Mat *FindBaseByDifference(Mat *parent, Mat *child, Mat **base_mat, int base_num)
{
    int i = 0, j = 0; 
    for (; i < parent->name_num && j < child->name_num; )
    {
        if (strcmp(parent->name[i], child->name[j]) == 0)
        {
            i++;
            j++; 
        }
        else
            break; 
    }

    char *base_name = parent->name[i]; 

    for (i = 0; i < base_num; i++)
    {
        if (strcmp(base_name, base_mat[i]->name[0]) == 0)
            return base_mat[i]; 
    }

    printf("Not found Base\n"); 
    return NULL; 
}

void FillMatInfo(Mat *mat, Mat **child_set, int child_num, Mat **base_mat, int base_num)
{
    assert(child_num != 0); 

    for (int i = 0; i < base_num; i++)
    {
        Mat *cur_base = base_mat[i]; 
        /* Does the current mat include this base */
        bool included = false; 
        for (int j = 0; j < mat->name_num; j++)
        {
            if (strcmp(mat->name[j], cur_base->name[0]) == 0)
            {
                mat->isvalid[j] = false; /* mask it */
                included = true;
                break; 
            }
        }
        if (!included)
        {
            mat->left[i] = NULL;
            mat->right[i] = NULL; 
            mat->joinkey[i] = NULL; 
            continue; 
        }

        /* Now the base is included in the mat; let's see how to process it */
        bool hasMatch = false; 
        int childindex = 0; 
        for (int j = 0; j < child_num; j++)
        {
            if (MatchChild(mat, child_set[j]))
            {
                childindex = j; 
                hasMatch = true; 
                break; 
            }
        }
        CleanValid(mat); 

        Mat *left, *right; 
        char **joinkey; 
        int joinkey_num; 
        if (hasMatch) 
        {
            left = cur_base; 
            right = child_set[childindex]; 
            joinkey = FindJoinKey(child_set[childindex], cur_base, &joinkey_num); 

            AddMatInfo(mat, left, right, joinkey, joinkey_num, i); 
            mat->depth[i] = 1; 
        }
        else
        {
            Mat *min_mat = child_set[0];
            int min_depth = child_set[0]->depth[i]; 
            for (int j = 1; j < child_num; j++)
            {
                if (child_set[j]->depth[i] < min_depth)
                {
                    min_mat = child_set[j]; 
                    min_depth = child_set[j]->depth[i]; 
                }
            }
            Mat *tempBase = FindBaseByDifference(mat, min_mat, base_mat, base_num); 
            left = min_mat; 
            right = tempBase; 
            joinkey = FindJoinKey(min_mat, tempBase, &joinkey_num); 
            AddMatInfo(mat, left, right, joinkey, joinkey_num, i); 
            mat->depth[i] = min_depth + 1; 
        }
    }
}

void GenName(Mat *mat, bool isTop)
{
    mat->print_name = newstr(); 

    strcat(mat->print_name, dbt_query); 

    if (!isTop)
    {
        for (int i = 0; i < mat->name_num; i++)
        {
            strcat(mat->print_name, "_"); 
            strcat(mat->print_name, mat->name[i]); 
        }
    }
}

void PrintTarget(Mat *mat)
{
    printf("%d\t", (mat->nb_num + mat->target_num)); 
    for (int k = 0; k < mat->nb_num; k++)
        printf("%s\t", mat->nb_key[k]);

    for (int k = 0; k < mat->target_num; k++)
        printf("%s\t", mat->target_name[k]);
    printf("\n"); 
}

void PrintJoinInfo(Mat *mat)
{
    for (int i = 0; i < mat->base_num; i++)
    {
        if (mat->joinkey[i] == NULL)
            printf("0\t\n"); 
        else
        {
            printf("%d\t", mat->joinkey_num[i]); 
            for (int j = 0; j < mat->joinkey_num[i]; j++)
                printf("%s\t", mat->joinkey[i][j]); 
            printf("\n"); 
        }
    }

    for (int i = 0; i < mat->base_num; i++)
    {
        if (mat->left[i] == NULL)
            printf("%s\t", NULL_STR); 
        else
            printf("%s\t", mat->left[i]->print_name);
    }
    printf("\n"); 

    for (int i = 0; i < mat->base_num; i++)
    {
        if (mat->right[i] == NULL)
            printf("%s\t", NULL_STR); 
        else
            printf("%s\t", mat->right[i]->print_name);
    }
    printf("\n"); 
}

void PrintParentInfo(Mat *mat)
{
    for(int i = 0; i  < mat->base_num; i++)
    {
        printf("%d\t", mat->parents_num[i]); 
        for (int j = 0; j < mat->parents_num[i]; j++)
        {
            printf("%s\t", mat->parents[i][j]->print_name); 
        }
        printf("\n"); 
    }
}

void PrintMatInfo(Mat ***layer_mat, int *layer_num, int layer_count)
{
    for (int i = layer_count - 1; i >= 0; i--)
    {
        for (int j = 0; j < layer_num[i]; j++)
        {
            Mat *mat = layer_mat[i][j]; 
            printf("%s\n", mat->print_name);
            PrintTarget(mat); 
            
            if (i != 0)
                PrintJoinInfo(mat);
            PrintParentInfo(mat); 
            printf("\n"); 
        }
    }
}

void PrintBuildMat(Mat ***layer_mat, int *layer_num, int layer_count)
{
    for (int i = layer_count - 1; i >= 0; i--)
    {
        for (int j = 0; j < layer_num[i]; j++)
        {
            Mat *mat = layer_mat[i][j]; 
            printf("DROP TABLE IF EXISTS %s CASCADE;\n", mat->print_name);
            printf("CREATE TABLE %s (\n", mat->print_name);

            for (int k = 0; k < mat->nb_num; k++)
            {
                printf("\t%s_%s\tBIGINT", mat->print_name, mat->nb_key[k]); 
                if (mat->target_num == 0 && k == mat->nb_num - 1)
                    printf("\n");
                else
                    printf(",\n"); 
            }

            for (int k = 0; k < mat->target_num; k++)
            {
                printf("\t%s_%s\t%s", mat->print_name, mat->target_name[k], mat->target_type[k]); 
                if (k == mat->target_num - 1)
                    printf("\n");
                else
                    printf(",\n"); 
            }

            printf(");\n");
            printf("\n"); 
        }
    }
}

void main()
{
    FILE *conf_file; 
    Base **base_array; 
    int base_num; 

    char filename[STR_BUFSIZE]; 
    memset(filename, 0, STR_BUFSIZE); 
    strcat(filename, dbt_query);
    strcat(filename, ".base"); 
    conf_file = fopen(filename, "r"); 
    if (conf_file == NULL)
        printf("File %s Not Found", filename); 

    fscanf(conf_file, "%d\n", &base_num);
    base_array = (Base **) malloc(sizeof(Base *) * base_num);

    for (int i = 0; i < base_num; i++)
    {
        Base *cur_base = malloc(sizeof(Base));

        cur_base->id = i; 
        cur_base->name = newstr();
        fscanf(conf_file, "%s\n", cur_base->name); 

        fscanf(conf_file, "%d\t", &cur_base->nb_num); 
        cur_base->nb_name = malloc(sizeof(char *) * cur_base->nb_num); 
        cur_base->nb_key = malloc(sizeof(char *) * cur_base->nb_num); 
        cur_base->nb_visited = malloc(sizeof(bool) * cur_base->nb_num);
        cur_base->nb_id =  malloc(sizeof(int) * cur_base->nb_num);
        for(int j = 0; j < cur_base->nb_num; j++)
        {
            cur_base->nb_name[j] = newstr();
            cur_base->nb_key[j] = newstr();
            fscanf(conf_file, "%[^:]:%s\t", cur_base->nb_name[j], cur_base->nb_key[j]); 
            cur_base->nb_visited[j] = false; 
        }
        fscanf(conf_file, "\n"); 

        fscanf(conf_file, "%d\t", &cur_base->target_num);
        cur_base->target_name = malloc(sizeof(char *) * cur_base->target_num); 
        cur_base->target_type = malloc(sizeof(char *) * cur_base->target_num); 
        for(int j = 0; j < cur_base->target_num; j++)
        {
            cur_base->target_name[j] = newstr();
            cur_base->target_type[j] = newstr();
            fscanf(conf_file, "%[^:]:%s\t", cur_base->target_name[j], cur_base->target_type[j]); 
        }
        fscanf(conf_file, "\n"); 

        base_array[i] = cur_base; 
    }

    printf("Read everything\n"); 

    populateID(base_array, base_num); 

    bool *cur_set = malloc(sizeof(bool) * base_num); 

    GenMat(base_array, base_num, cur_set, 0); 

    fclose(conf_file); 

    //SortByLayer(total_mat_array, total_mat_num); 

    Mat ***layer_mat = malloc(sizeof(Mat **) * base_num); 
    int *layer_num = malloc(sizeof(int) * base_num); 
    memset(layer_num, 0, sizeof(int) * base_num); 

    for (int i = 0; i < total_mat_num; i++)
    {
        Mat *tmp = total_mat_array[i];
        SortNames(tmp); 
        SetupMatInfo(tmp, base_num); 

        int layer_index = tmp->name_num - 1; 
        layer_mat[layer_index] = AddMat(layer_mat[layer_index], layer_num[layer_index], tmp); 
        layer_num[layer_index]++; 
    }

    for (int i = 0; i < base_num; i++)
        SortMatByNames(layer_mat[i], layer_num[i]);

    for (int i = 1; i < base_num; i++) /* iterating each layer */
    {
        for (int j = 0; j < layer_num[i]; j++) /* iterating within one layer */
        {
            Mat *cur_mat = layer_mat[i][j]; 
            Mat **child_set = NULL; 
            int child_num = 0; 

            /* Build all subsumed children */
            for (int k = 0; k < layer_num[i - 1]; k++)
            {
                Mat *child = layer_mat[i - 1][k]; 
                if (Subsume(cur_mat, child))
                {
                    child_set = AddMat(child_set, child_num, child); 
                    child_num++;
                }
            }

            FillMatInfo(cur_mat, child_set, child_num, layer_mat[0], base_num); 
        }
    }

    for (int i = 0; i < total_mat_num; i++)
        GenName(total_mat_array[i], (total_mat_array[i]->name_num == base_num)); 

    printf("%d,%d\n", total_mat_num, base_num);
    PrintMatInfo(layer_mat, layer_num, base_num); 

    PrintBuildMat(layer_mat, layer_num, base_num); 
}



