/*
 *  Author : WangBoJing , email : 1989wangbojing@gmail.com
 * 
 *  Copyright Statement:
 *  --------------------
 *  This software is protected by Copyright and the information contained
 *  herein is confidential. The software may not be copied and the information
 *  contained herein may not be used or disclosed except with the written
 *  permission of Author. (C) 2017
 * 
 *

****       *****                                      *****
  ***        *                                       **    ***
  ***        *         *                            *       **
  * **       *         *                           **        **
  * **       *         *                          **          *
  *  **      *        **                          **          *
  *  **      *       ***                          **
  *   **     *    ***********    *****    *****  **                   ****
  *   **     *        **           **      **    **                 **    **
  *    **    *        **           **      *     **                 *      **
  *    **    *        **            *      *     **                **      **
  *     **   *        **            **     *     **                *        **
  *     **   *        **             *    *      **               **        **
  *      **  *        **             **   *      **               **        **
  *      **  *        **             **   *      **               **        **
  *       ** *        **              *  *       **               **        **
  *       ** *        **              ** *        **          *   **        **
  *        ***        **               * *        **          *   **        **
  *        ***        **     *         **          *         *     **      **
  *         **        **     *         **          **       *      **      **
  *         **         **   *          *            **     *        **    **
*****        *          ****           *              *****           ****
                                       *
                                      *
                                  *****
                                  ****



 *
 */
 
// gcc -o kvstore kvstore.c rbtree.c hash.c -I ./NtyCo/core/ -L ./NtyCo/ -lntyco -lpthread -ldl


#include "nty_coroutine.h"

#include <arpa/inet.h>


#define ENABLE_ARRAY		1
#define ENABLE_RBTREE		1


#define MAX_CLIENT_NUM			1000000
#define TIME_SUB_MS(tv1, tv2)  ((tv1.tv_sec - tv2.tv_sec) * 1000 + (tv1.tv_usec - tv2.tv_usec) / 1000)

#define unused(x)	(x)=(x)
/*
3\r\n
3\r\n
SET\r\n
3\r\n
KEY\r\n
5\r\n
value\r\n
*/

// array
// SET key value
// GET key
// COUNT
// DELETE key
// EXIST key

// rbtree
// RSET key value
// RGET key
// RCOUNT
// RDELETE key
// REXIST key

// btree
// BSET key value
// BGET key
// BCOUNT
// BDELETE key
// BEXIST key


// hash
// HSET key value
// HGET key
// HCOUNT
// HDELETE key
// HEXIST key


// skiptable 
// ZSET key value
// ZGET key
// ZCOUNT
// ZDELETE key
// ZEXIST key


// KVS_CMD_ERROR (format error)
// KVS_CMD_QUIT

typedef enum kvs_cmd_e {
	// array
	KVS_CMD_START = 0,
	KVS_CMD_SET = KVS_CMD_START,
	KVS_CMD_GET,
	KVS_CMD_COUNT,
	KVS_CMD_DELETE,
	KVS_CMD_EXIST,
	// rbtree
	KVS_CMD_RSET,
	KVS_CMD_RGET,
	KVS_CMD_RCOUNT,
	KVS_CMD_RDELETE,
	KVS_CMD_REXIST,
	// btree
	KVS_CMD_BSET,
	KVS_CMD_BGET,
	KVS_CMD_BCOUNT,
	KVS_CMD_BDELETE,
	KVS_CMD_BEXIST,
	// hash
	KVS_CMD_HSET,
	KVS_CMD_HGET,
	KVS_CMD_HCOUNT,
	KVS_CMD_HDELETE,
	KVS_CMD_HEXIST,
	//skiptable
	KVS_CMD_ZSET,
	KVS_CMD_ZGET,
	KVS_CMD_ZCOUNT,
	KVS_CMD_ZDELETE,
	KVS_CMD_ZEXIST,

	// cmd
	KVS_CMD_ERROR,
	KVS_CMD_QUIT,
	KVS_CMD_END,
} kvs_cmd_t;

/*
struct cmd_arg {
	const char *cmd;
	int argc;
};

struct cmd_arg commands[] = {
	{"SET", 2}, 
	{"GET", 1}, 
};
*/

const char *commands[] = {
	"SET", "GET", "COUNT", "DELETE", "EXIST", 
	"RSET", "RGET", "RCOUNT", "RDELETE", "REXIST",
	"BSET", "BGET", "BCOUNT", "BDELETE", "BEXIST",
	"HSET", "HGET", "HCOUNT", "HDELETE", "HEXIST",
	"ZSET", "ZGET", "ZCOUNT", "ZDELETE", "ZEXIST",
};

typedef enum kvs_result_e {
	KVS_RESULT_SUCCESS,
	KVS_RESULT_FAILED
} kvs_result_t;


const char *result[] = {
	"SUCCESS",
	"FAILED"
};

#define MAX_TOKENS  			32
#define CLINET_MSG_LENGTH		1024


#if 1

void *kvs_malloc(size_t size) {
	return malloc(size);
}

void kvs_free(void *ptr) {
	return free(ptr);
}


#endif

#if ENABLE_MEM_POOL

typedef struct mempool_s {
	int block_size; // 
	int free_count;

	char *free_ptr;
	char *mem;
} mempool_t;

mempool_t mem;

#endif


#if ENABLE_ARRAY





typedef struct kvs_array_item_s {
#if ENABLE_MEM_POOL
	void *_;
#endif
	char *key;
	char *value;

} kvs_array_item_t;

#define KVS_ARRAY_ITEM_SIZE		1024

#if ENABLE_MEM_POOL

kvs_array_item_t *array_table; 


#else

kvs_array_item_t array_table[KVS_ARRAY_ITEM_SIZE] = {0};

#endif
int array_count = 0;

kvs_array_item_t *kvs_array_search_item(const char *key) {

	if (!key) return NULL;
#if ENABLE_MEM_POOL


#else


	int i = 0;
	for (i = 0;i < array_count;i ++) {
		if (0 == strcmp(array_table[i].key, key))
			return &array_table[i];
	}
#endif
	return NULL;
}


//KVS_CMD_EXIST
int kvs_array_exist(const char *key) {
	return (kvs_array_search_item(key) != NULL);
}


//KVS_CMD_SET
int kvs_array_set(const char *key, const char *value) {

	if (key == NULL || value == NULL || array_count == KVS_ARRAY_ITEM_SIZE-1) return -1;

	if (kvs_array_exist(key)) return -1;

	char *kcopy = kvs_malloc(strlen(key)+1);
	if (kcopy == NULL) return -1;
	strncpy(kcopy, key, strlen(key)+1);

	char *vcopy = kvs_malloc(strlen(value)+1);
	if (vcopy == NULL) {
		free(kcopy);
		return -1;
	}
	strncpy(vcopy, value, strlen(value)+1);
#if ENABLE_MEM_POOL

	kvs_array_item_t *item = memp_alloc(&mem);
	item->key = kcopy;
	item->value = vcopy;
	array_count ++;

#else
	array_table[array_count].key = kcopy;
	array_table[array_count].value = vcopy;
#endif
	array_count++;

	return 0;

}



//KVS_CMD_GET
char *kvs_array_get(const char *key) {

	kvs_array_item_t *item = kvs_array_search_item(key);
	if (item) {
		return item->value;
	}
	return NULL;
}

//KVS_CMD_COUNT
int kvs_array_count(void) {
	return array_count;
}


//KVS_CMD_DELETE
int kvs_array_delete(const char *key) {

	if (key == NULL) return -1;
	
	kvs_array_item_t *item = kvs_array_search_item(key);
	if (item == NULL) {
		return -1;
	}
	//
	if (item->key) {
		kvs_free(item->key);
		item->key = NULL;
	}
	if (item->value) {
		kvs_free(item->value);
		item->value = NULL;
	}
	return 0;
}


#endif


#if ENABLE_MEM_POOL


#define MEM_PAGE_SIZE		(KVS_ARRAY_ITEM_SIZE * sizeof(kvs_array_item_t))




int memp_init(mempool_t *m, int block_size) {

	if (!m) return -2;

	m->block_size = block_size;
	m->free_count = MEM_PAGE_SIZE / block_size;

	m->free_ptr = (char*)malloc(MEM_PAGE_SIZE);
	if (!m->free_ptr) return -1;
	m->mem = m->free_ptr;

	int i = 0;
	char *ptr = m->free_ptr;
	for (i = 0;i < m->free_count;i ++) {

		*(char **)ptr = ptr + block_size;
		ptr += block_size;

	}
	*(char **)ptr = NULL;

	return 0;
}


void* memp_alloc(mempool_t *m) {

	if (!m || m->free_count == 0) return NULL;

	void *ptr = m->free_ptr;

	m->free_ptr = *(char **)ptr;
	m->free_count --;

	return ptr;
}

void *memp_free(mempool_t *m, void *ptr) {

	*(char**)ptr = m->free_ptr;
	m->free_ptr = (char *)ptr;
	m->free_count ++;
	

}


void init_array_table(void) {

	memp_init(&mem, sizeof(kvs_array_item_t));
	

}


#endif

#if ENABLE_RBTREE


typedef struct _rbtree rbtree_t;
typedef struct _rbtree_node rbtree_node_t;


extern rbtree_t tree;

int put_kv_rbtree(rbtree_t *tree, char *key, char *value);
char* get_kv_rbtree(rbtree_t *tree, char *key);
int count_kv_rbtree(rbtree_t *tree);
int exist_kv_rbtree(rbtree_t *tree, char *key);
int delete_kv_rbtree(rbtree_t *tree, char *key);

int init_rbtree(rbtree_t *tree);
void dest_rbtree(rbtree_t *tree);









#endif





// OK
// Success
// SUCCESS


int kvs_parser_protocol(char *msg, char **tokens, int count) {

	if (tokens == NULL || tokens[0] == NULL || count == 0) return KVS_CMD_ERROR;

	int cmd = KVS_CMD_START;
	for (cmd = KVS_CMD_START; cmd <= KVS_CMD_ZEXIST; cmd ++) {
		if (0 == strcmp(tokens[0], commands[cmd])) break;
	}
/*
 * tokens[0]: cmd
 * tokens[1]: key
 * tokens[2]: value
 */
	

	switch (cmd) {
#if ENABLE_ARRAY
	case KVS_CMD_SET: {
		assert(count == 3); // 
		printf("cmd: %s\n", tokens[0]);
		printf("key: %s\n", tokens[1]);
		printf("value: %s\n", tokens[2]);

		int ret = 0;
		int res = kvs_array_set(tokens[1], tokens[2]);
		if (!res) {
			memset(msg, 0, CLINET_MSG_LENGTH);
			ret = snprintf(msg, CLINET_MSG_LENGTH, "SUCCESS\r\n");
		} else {
			memset(msg, 0, CLINET_MSG_LENGTH);
			ret = snprintf(msg, CLINET_MSG_LENGTH, "FAILED\r\n");
		}

		
		return ret;
	}
	case KVS_CMD_GET: {

		int ret = 0;
		char *value = kvs_array_get(tokens[1]);
		if (value) {
			memset(msg, 0, CLINET_MSG_LENGTH);
			ret = snprintf(msg, CLINET_MSG_LENGTH, "%s\r\n", value);
		} else {
			memset(msg, 0, CLINET_MSG_LENGTH);
			ret = snprintf(msg, CLINET_MSG_LENGTH, "FAILED: NO EXIST\r\n");
		}

		
		return ret;
	}
	case KVS_CMD_COUNT: {
		int res = kvs_array_count();
		
		memset(msg, 0, CLINET_MSG_LENGTH);
		int ret = snprintf(msg, CLINET_MSG_LENGTH, "%d\r\n", res);

		return ret;
	}
	case KVS_CMD_DELETE: {

	}
	case KVS_CMD_EXIST: {

		int res = kvs_array_exist(tokens[1]);
		
		memset(msg, 0, CLINET_MSG_LENGTH);
		int ret = snprintf(msg, CLINET_MSG_LENGTH, "%d\r\n", res);

		return ret;
	}
#endif

#if ENABLE_RBTREE
	// rbtree
	case KVS_CMD_RSET: {

		int ret;
		int res = put_kv_rbtree(&tree, tokens[1], tokens[2]);
		if (!res) {
			memset(msg, 0, CLINET_MSG_LENGTH);
			ret = snprintf(msg, CLINET_MSG_LENGTH, "SUCCESS\r\n");
		} else {
			memset(msg, 0, CLINET_MSG_LENGTH);
			ret = snprintf(msg, CLINET_MSG_LENGTH, "FAILED\r\n");
		}
		
		return ret;
	}
	case KVS_CMD_RGET: {
		
		int ret = 0;
		char *value = get_kv_rbtree(&tree, tokens[1]);
		if (value) {
			memset(msg, 0, CLINET_MSG_LENGTH);
			ret = snprintf(msg, CLINET_MSG_LENGTH, "%s\r\n", value);
		} else {
			memset(msg, 0, CLINET_MSG_LENGTH);
			ret = snprintf(msg, CLINET_MSG_LENGTH, "FAILED: NO EXIST\r\n");
		}

		return ret;

	}
	case KVS_CMD_RCOUNT: {
		int res = count_kv_rbtree(&tree);
		
		memset(msg, 0, CLINET_MSG_LENGTH);
		int ret = snprintf(msg, CLINET_MSG_LENGTH, "%d\r\n", res);
		
		return ret;
	}
	case KVS_CMD_RDELETE: {
		
	}
	case KVS_CMD_REXIST:{

		int res = kvs_array_exist(tokens[1]);
		
		memset(msg, 0, CLINET_MSG_LENGTH);
		int ret = snprintf(msg, CLINET_MSG_LENGTH, "%d\r\n", res);

		return ret;
	}
#endif
	// btree
	case KVS_CMD_BSET:
		break;
	case KVS_CMD_BGET:
		break;
	case KVS_CMD_BCOUNT:
		break;
		
	case KVS_CMD_BDELETE:
	case KVS_CMD_BEXIST:
		break;
	// hash
	case KVS_CMD_HSET:
		break;
	case KVS_CMD_HGET:
		break;
	case KVS_CMD_HCOUNT:
	case KVS_CMD_HDELETE:
	case KVS_CMD_HEXIST:
		break;
	//skiptable
	case KVS_CMD_ZSET:
	case KVS_CMD_ZGET:
	case KVS_CMD_ZCOUNT:
	case KVS_CMD_ZDELETE:
	case KVS_CMD_ZEXIST:
		break;

	}
	return 0;
}

// SET Name King
int kvs_split_tokens(char **tokens, char *msg) {

	int count = 0;
	char *token = strtok(msg, " ");   
    
    while (token != NULL) {
        tokens[count++] = token;   
        token = strtok(NULL, " ");
    }

	return count;
}

// 
int kvs_protocol(char *msg, int length) {

	char *tokens[MAX_TOKENS] = {0};
	int count = kvs_split_tokens(tokens, msg);
	int i = 0;

	unused(length);
	
	for (i = 0; i < count; i++) {   
        printf("%s\n", tokens[i]);
    }

	return kvs_parser_protocol(msg, tokens, count);
	
}



///////// network, coroutine

void server_reader(void *arg) {
	int fd = *(int *)arg;
	int ret = 0;

 
	while (1) {
		
		char buf[CLINET_MSG_LENGTH] = {0};
		ret = recv(fd, buf, CLINET_MSG_LENGTH, 0);
		if (ret > 0) {
			//if(fd > MAX_CLIENT_NUM) 
			printf("read from server: %.*s\n", ret, buf);

			ret = kvs_protocol(buf, ret); // 
			
			ret = send(fd, buf, ret, 0);
			if (ret == -1) {
				close(fd);
				break;
			}
		} else if (ret == 0) {	
			close(fd);
			break;
		}

	}
}


void server(void *arg) {

	unsigned short port = *(unsigned short *)arg;
	free(arg);

	int fd = socket(AF_INET, SOCK_STREAM, 0);
	if (fd < 0) return ;

	struct sockaddr_in local, remote;
	local.sin_family = AF_INET;
	local.sin_port = htons(port);
	local.sin_addr.s_addr = INADDR_ANY;
	bind(fd, (struct sockaddr*)&local, sizeof(struct sockaddr_in));

	listen(fd, 20);
	printf("listen port : %d\n", port);

	
	struct timeval tv_begin;
	gettimeofday(&tv_begin, NULL);

	while (1) {
		socklen_t len = sizeof(struct sockaddr_in);
		int cli_fd = accept(fd, (struct sockaddr*)&remote, &len);
		if (cli_fd % 1000 == 999) {

			struct timeval tv_cur;
			memcpy(&tv_cur, &tv_begin, sizeof(struct timeval));
			
			gettimeofday(&tv_begin, NULL);
			int time_used = TIME_SUB_MS(tv_begin, tv_cur);
			
			printf("client fd : %d, time_used: %d\n", cli_fd, time_used);
		}
		printf("new client comming\n");

		nty_coroutine *read_co;
		nty_coroutine_create(&read_co, server_reader, &cli_fd);

	}
	
}


void init_kvs_engine(void) {
	init_rbtree(&tree);
}


int main(int argc, char *argv[]) {
	nty_coroutine *co = NULL;

	init_kvs_engine();

	int i = 0;
	unsigned short base_port = 8000;
	//for (i = 0;i < 100;i ++) {
	unsigned short *port = calloc(1, sizeof(unsigned short));
	unused(argc);
	unused(argv);

	*port = base_port + i;
	nty_coroutine_create(&co, server, port); ////////no run
	//}

	nty_schedule_run(); //run

	return 0;
}



