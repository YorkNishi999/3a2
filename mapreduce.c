#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <pthread.h>
#include <semaphore.h>
#include <assert.h>
#include "hashmap.h"
#include "mapreduce.h"

#define INIT_NUM_KV_LIST 5

// def of struct
typedef struct kv_t kv_t;
typedef struct args_t args_t;
typedef struct partition_data_t partition_data_t;
typedef struct cur_reduce_index_t cur_reduce_index_t;

struct kv_t {
  char* key;
  char* value;
};

struct cur_reduce_index_t {
  int cur_reduce_partition_index;
  pthread_mutex_t lock;
};

struct args_t {
  int idx;
  pthread_mutex_t lock;  // each file can only be modified by one mapper
};

struct partition_data_t {
  kv_t** kv_list;
  pthread_mutex_t lock;
  int cur_kv_list_index;
  int kv_list_size;
  int get_func_index;
  sem_t sem;
};

unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
  unsigned long hash = 5381;
  int c;
  while ((c = *key++) != '\0')
    hash = hash * 33 + c;
  return hash % num_partitions;
}

// g variable arguments
args_t* args; // 引数を表す変数
int file_num = 0;
char **files = NULL;  // ファイルのポインタの配列

// g variable mapper
Mapper mapper;
unsigned int g_num_mapper;

// g variables reducer
Reducer reducer;
unsigned int g_num_reducer;
cur_reduce_index_t* cur_reduce_index;

// g variable partition
Partitioner partitioner;
unsigned int g_num_partition;
partition_data_t** g_partition;


// func for threads of mapping
void map_func() {
  int file_index;
  // printf("index: %d\n", index);
  while (1) {
    pthread_mutex_lock(&(args->lock));
    // printf("args index:%d\n", args->idx);
    file_index = args->idx;
    args->idx += 1;
    if (file_index >= file_num) {
      pthread_mutex_unlock(&args->lock);
      break;
    }
    pthread_mutex_unlock(&(args->lock));
    mapper(files[file_index]);
  }
}

void MR_Emit(char *key, char *value) {
  int partition_index = partitioner(key, g_num_partition);
  partition_data_t* cur_partition = g_partition[partition_index];
  pthread_mutex_lock(&cur_partition->lock);
  // printf("key:%s, value:%s, partition:%p, cur_partition->kv_list_size:%d\n",
  //       key, value, cur_partition, cur_partition->kv_list_size);

  // increase size 
  if (cur_partition->cur_kv_list_index >= cur_partition->kv_list_size) {
    long cur_size = cur_partition->kv_list_size;
    cur_partition->kv_list_size = cur_size*2;

    kv_t **new_kv_list =
        (kv_t **)malloc(sizeof(kv_t *) * cur_partition->kv_list_size);
    // printf("new_kv_list:%p\n", new_kv_list);
    // copy the original values to new address
    memcpy(new_kv_list, cur_partition->kv_list,
           sizeof(kv_t*) * cur_size); // original size(データある分）をコピー
    free(cur_partition->kv_list);
    cur_partition->kv_list = new_kv_list;
  }

  kv_t* new_kv = (kv_t*)malloc(sizeof(kv_t));

  // insert new_kv
  char *tmp_key = (char*)malloc(strlen(key)+1);
  char *tmp_value = (char*)malloc(strlen(value)+1);
  snprintf(tmp_key, strlen(key)+1, "%s", key);
  snprintf(tmp_value, strlen(value)+1, "%s", value);
  new_kv->key = tmp_key;
  new_kv->value = tmp_value;

  cur_partition->kv_list[cur_partition->cur_kv_list_index] = new_kv;
  cur_partition->cur_kv_list_index++;

  // new_kv->key = (char*)malloc(sizeof(char)*strlen(key)+1);
  // new_kv->value = (char*)malloc(sizeof(char)*strlen(value)+1);
  // snprintf(new_kv->key, strlen(key)+1, "%s", key);
  // snprintf(new_kv->value, strlen(key)+1, "%s", value);
  // printf("key:%s, new_kv->key:%s, value: %s, new_kv->value:%s\n",
      // key, new_kv->key, value, new_kv->value);


  // printf("cur_partition->cur_kv_list_index:%d\n", cur_partition->cur_kv_list_index);
  // printf("cur_partition->kv_list[cur_partition->cur_kv_list_index]:%p\n"
  //     , cur_partition->kv_list);

  pthread_mutex_unlock(&cur_partition->lock);

}

char* get_func(char*key, int num_partition) {
  // printf("start getfunc\n");
  partition_data_t* partition = g_partition[num_partition];
  // printf("partition->cur_kv_list_index:%d\n", partition->cur_kv_list_index);

  // printf("ing get func: partition->get_func_index: %d\n", partition->get_func_index);
  while(partition->get_func_index < partition->cur_kv_list_index) {
    // printf("index:%d\n", partition->get_func_index);
    if(strcmp(key, partition->kv_list[partition->get_func_index]->key) == 0) {
      // printf("equal dayo\n");
      int a = partition->get_func_index;
      partition->get_func_index++;
      // printf("key:%s, partition->get_func_index:%d, partition->cur_kv_list_index:%d\n"
      //     ,key, partition->get_func_index, partition->cur_kv_list_index);
      // printf("partition->kv_list[partition->get_func_index]->key:%s\n",partition->kv_list[partition->get_func_index]->key);
      // if (partition->get_func_index > partition->cur_kv_list_index) {
      //   return partition->kv_list[partition->get_func_index]->value;
      // }
      return partition->kv_list[a]->value;
    }
    // partition->get_func_index++;
    return NULL;
  }
  return NULL;

}

// void reduce_func(partition_data_t* partition) {
void reduce_func() {
  // printf("start reduce_func\n");

  int partition_index = 0;

  while(1) {
    pthread_mutex_lock(&(cur_reduce_index->lock));
    partition_index = cur_reduce_index->cur_reduce_partition_index;
    // printf("partition_index:%d, cur_reduce_index->cur_reduce_partition_index:%d\n"
    //     , partition_index, cur_reduce_index->cur_reduce_partition_index);
    if (partition_index >= g_num_partition) {
      pthread_mutex_unlock(&(cur_reduce_index->lock));
      break;
    }
    cur_reduce_index->cur_reduce_partition_index++;
    pthread_mutex_unlock(&(cur_reduce_index->lock));

    partition_data_t* partition = g_partition[partition_index];

    // printf("partition->get_func_index:%d\n", partition->get_func_index);
    while(partition->get_func_index < partition->cur_kv_list_index) {
      pthread_mutex_lock(&partition->lock);
      sem_wait(&partition->sem);
      // printf("reduce func thread: %ld\n", pthread_self());
      // printf("reduce: partition: %p partition num:%d\n"
          // , partition, partition_index);

      reducer(partition->kv_list[partition->get_func_index]->key
          , get_func, partition_index);
      
      sem_post(&partition->sem);
      pthread_mutex_unlock(&partition->lock);
    }

  }

  // partition_data_t* partition = g_partition[(*i)-1];



  // for (int i = 0; i < partition->kv_list_size; )
  // int index = partition->get_func_index;

  // printf("partition->get_func_index:%d\n", partition->get_func_index);
  // while(partition->get_func_index < partition->cur_kv_list_index) {
  //   pthread_mutex_lock(&partition->lock);
  //   sem_wait(&partition->sem);
  //   printf("reduce func thread: %ld\n", pthread_self());
  //   printf("reduce: partition: %p partition num:%d\n", partition, (*i)-1);

  //   reducer(partition->kv_list[partition->get_func_index]->key, get_func, (*i)-1);
    
  //   sem_post(&partition->sem);
  //   pthread_mutex_unlock(&partition->lock);

  // //   char* key = partition->kv_list[index]->key;
  // //   printf("kokokamo!\n");
  // //   printf("key:%s\n", key);
  // //   printf("reducer index:%d, partition->kv_list[index]->key:%s\n"
  // //           , index, partition->kv_list[index]->key);
  // //   reducer(partition->kv_list[index]->key, get_func, (*i)-1);
  // //   // index++;
  // //   while(strcmp(key, partition->kv_list[index]->key) == 0){
  // //     index++;
  // //   }
  // }

  // sem_post(&g_partition[(*i)-1]->sem);

}


int comparator(const void *s1, const void *s2) {
  kv_t **p1 = (kv_t **)s1;
  kv_t **p2 = (kv_t **)s2;
  // printf("comparing %s, %s\n", (*p1)->unique_key, (*p2)->unique_key); // for debug
  return strcmp((*p1)->key, (*p2)->key);
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
  pthread_t reducer_threads[g_num_reducer];
  cur_reduce_index
      = (cur_reduce_index_t*)malloc(sizeof(cur_reduce_index_t));
  cur_reduce_index->cur_reduce_partition_index = 0;
  pthread_mutex_init(&(cur_reduce_index->lock), NULL);

  // init partition
  partitioner = partition;
  g_num_partition = num_reducers;
  g_partition = (partition_data_t**)
      malloc(sizeof(partition_data_t)*g_num_partition); // partition_dataの配列
  for(int i = 0; i < g_num_partition; i++) {
    g_partition[i] = (partition_data_t*)malloc(sizeof(partition_data_t));
    g_partition[i]->kv_list =
        (kv_t**)malloc(sizeof(kv_t)*INIT_NUM_KV_LIST); // kv_listの配列
      for (int j = 0; j < INIT_NUM_KV_LIST; j++) {
        g_partition[i]->kv_list[j] = 
            (kv_t*)malloc(sizeof(kv_t));
      }
    g_partition[i]->cur_kv_list_index = 0;
    g_partition[i]->get_func_index = 0;
    g_partition[i]->kv_list_size = INIT_NUM_KV_LIST;
    sem_init(&g_partition[i]->sem, 0, 1);
    pthread_mutex_init(&(g_partition[i]->lock), NULL);
  }

  // init arg
  args = (args_t*)malloc(sizeof(args_t));
  args->idx = 0;
  pthread_mutex_init(&(args->lock), NULL);
  file_num = argc - 1;
  files = argv + sizeof(char);
  // printf("init arg\n");
  // printf("mapper: %d, reducer: %d\n", g_num_mapper, g_num_reducer);
  // printf("g_num_mapper: %d, g_num_reducer:%d, g_num_partition:%d, g_partition[g_num_partition-1]:%p, g_partition[g_num_partition-1]->kv_list[0]:%p\n"
  //       , g_num_mapper, g_num_reducer, g_num_partition
  //       , g_partition[g_num_partition-1]
  //       , g_partition[g_num_partition-1]->kv_list[0]);

  // start mapping
  // printf("start mapping\n");
  for (int i = 0; i < g_num_mapper; i++) {
    pthread_create(&mapper_threads[i], NULL, (void*)&map_func, NULL);
  }
  for (int i = 0; i < g_num_mapper; i++) {
    pthread_join(mapper_threads[i], NULL);
  }


  // for debug before sort
  for (int j = 0; j < g_num_partition; j++) {
    partition_data_t* tmps = g_partition[j];
    for (int i = 0; i < tmps->cur_kv_list_index; i++) {
      // printf("tmp->kv_list[%d]->key:%s\n", i, tmps->kv_list[i]->key);
      // printf("tmp->kv_list[%d]->value:%s\n", i, tmps->kv_list[i]->value);
    }
  }
  // for debug before sort


  // sort unique key node
  // printf("start sorting\n");
  for (int i = 0; i < g_num_partition; i++) {
    partition_data_t* partition = g_partition[i];
    qsort(partition->kv_list, partition->cur_kv_list_index, 
        sizeof(kv_t*), comparator);
  }

  // for debug after sort
  for (int j = 0; j < g_num_partition; j++) {
    partition_data_t* tmps = g_partition[j];
    for (int i = 0; i < tmps->cur_kv_list_index; i++) {
      // printf("tmp->kv_list[%d]->key:%s\n", i, tmps->kv_list[i]->key);
      // printf("tmp->kv_list[%d]->value:%s\n", i, tmps->kv_list[i]->value);
    }
  }
  // for debug after sort


  // start reduce
  // printf("start reducer\n");
  for (int i = 0; i < num_reducers; i++) {
    // printf("i: %p, %d\n", &i, i); // for debug
    pthread_create(&reducer_threads[i], NULL, (void*)&reduce_func, NULL);
  }

  for (int i = 0; i < num_reducers; i++) {
    pthread_join(reducer_threads[i], NULL);
  }


  // release memory
  pthread_mutex_destroy(&(cur_reduce_index->lock));
  free(cur_reduce_index);

  for(int i = 0; i < g_num_partition; i++) {
    free(g_partition[i]->kv_list);
    sem_destroy(&(g_partition[i]->sem));
    free(g_partition[i]);
  }
  free(g_partition);

  pthread_mutex_destroy(&(args->lock));
  free(args);


}
