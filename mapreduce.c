#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <pthread.h>
#include <semaphore.h>
#include <assert.h>
#include "hashmap.h"
#include "mapreduce.h"


// def of struct
typedef struct kv kv;
typedef struct kv_list kv_list;
typedef struct arg_next arg_next;
typedef struct partition_data_t partition_data_t;

struct kv {
  char* key;
  char* value;
};

struct kv_list {
  struct kv** elements;
  size_t num_elements;  // current 
  size_t size; // max size
};

struct arg_next{
  int idx;
  pthread_mutex_t lock;  // each file can only be modified by one mapper
};

struct partition_data_t {
  kv_list** kv_list_list;
  pthread_mutex_t lock;
};

unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
  unsigned long hash = 5381;
  int c;
  while ((c = *key++) != '\0')
    hash = hash * 33 + c;
  return hash % num_partitions;
}

// g variable arguments
// arg_data* arg;  // 引数を表す変数
arg_next my_arg_next;
int input_count = 0;
char **input_args = NULL;

// g variable mapper
Mapper mapper;
unsigned int g_num_mapper;

// def global variables for reducer
Reducer reducer;
unsigned int g_num_reducer;

// g variable partition
Partitioner partitioner;
unsigned int g_num_partition;
partition_data_t** g_partition;


// func for threads of mapping
void map_func() {
  // each mapper take one of arguments, mutual exclusion here
  int next_idx;
  while (1) {
    pthread_mutex_lock(&my_arg_next.lock);
    next_idx = my_arg_next.idx;
    my_arg_next.idx += 1;
    if (next_idx >= input_count) {
      pthread_mutex_unlock(&my_arg_next.lock);
      break;
    }
    pthread_mutex_unlock(&my_arg_next.lock);
    printf("input_args[next_idx]:%s\n", input_args[next_idx]);
    // printf("input_args[next_idx]:%s\n", input_args[next_idx]);
    mapper(input_args[next_idx]);
    // traverse(g_partition->hash_table);

  }
  // multi-thread sorting for each partition in mapping threads
}

void MR_Emit(char *key, char *value) {
  
}


void MR_Run(int argc, char *argv[], Mapper map, int num_mappers,
        Reducer reduce, int num_reducers, Partitioner partition) {
  // 1: initialize
  // init mapper
  mapper = map;
  g_num_mapper = num_mappers;
  pthread_t mapper_threads[g_num_mapper];
  // init reduce
  reducer = reduce;
  g_num_reducer = num_reducers;
  // pthread_t reducer_threads[g_num_reducer];

  // init for for MR_Emit
  partitioner = partition;
  g_num_partition = num_reducers;
  g_partition = (partition_data_t**)
      malloc(sizeof(partition_data_t)*g_num_partition);
  for(int i = 0; i < g_num_partition; i++) {
    g_partition[i] = (partition_data_t*)malloc(sizeof(partition_data_t));
  }

  // init for partition


  // init arg
  printf("init arg\n");
  printf("mapper: %d, reducer: %d\n", g_num_mapper, g_num_reducer);
  printf("g_num_mapper: %d, g_num_reducer:%d, g_num_partition:%d, partition_data_t:%p\n"
        , g_num_mapper, g_num_reducer, g_num_partition, g_partition[g_num_partition-1]);
  input_count = argc - 1;
  input_args = argv + sizeof(char);


  // printf("g_num_partitioner: %d\n", g_num_partitioner);

  // init reducer

  // traverse(g_partition->hash_table);

  // 2: mapping using mapper
  // in: file, out: intermediate variables for MR_emit
  printf("start mapping\n");
  for (int i = 0; i < num_mappers; i++) {
    pthread_create(&mapper_threads[i], NULL, (void*)&map_func, NULL);
  }
  for (int i = 0; i < num_mappers; i++) {
    pthread_join(mapper_threads[i], NULL);
  }

  // traverse(g_partition->hash_table);

  // sort unique key node
  


  // release memory
  // free(arg);
  // pthread_mutex_destroy(&arg->arg_lock);

  // free(g_partition->hash_table);
  // pthread_mutex_destroy(&g_partition->lock);
  free(g_partition);

  // free(ans_hash->ans_map);
  // pthread_mutex_destroy(&ans_hash->ans_lock);
  // free(ans_hash);
}
