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
 
// gcc -o kvstore kvstore.c -I ./NtyCo/core/ -L ./NtyCo/ -lntyco -lpthread -ldl


#include "nty_coroutine.h"

#include <arpa/inet.h>


#define ENABLE_ARRAY		1
#define ENABLE_RBTREE		1
#define ENABLE_HASH			1

#define ENABLE_KVENGINE		1
#define ENABLE_LOG			1

#define MAX_CLIENT_NUM			1000000
#define TIME_SUB_MS(tv1, tv2)  ((tv1.tv_sec - tv2.tv_sec) * 1000 + (tv1.tv_usec - tv2.tv_usec) / 1000)

#define unused(x)		x=x

#if ENABLE_LOG
#define LOG(_fmt, ...) fprintf(stdout, "[%s:%d]:%s"_fmt, __FILE__, __LINE__, __VAR_ARGS__)
#else
#define LOG(_fmt, ...) 

#endif
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




#if ENABLE_ARRAY

typedef struct kvs_array_item_s {

	char *key;
	char *value;

} kvs_array_item_t;

#define KVS_ARRAY_ITEM_SIZE		1024

kvs_array_item_t array_table[KVS_ARRAY_ITEM_SIZE] = {0};
int array_count = 0;


kvs_array_item_t *kvs_array_search_item(char *key) {

	if (!key) return NULL;
	
	int i = 0;
	for (i = 0;i < KVS_ARRAY_ITEM_SIZE;i ++) {
		if (array_table[i].key != NULL &&  0 == strcmp(array_table[i].key, key))
			return &array_table[i];
	}

	return NULL;
}


//KVS_CMD_EXIST
int kvs_array_exist(char *key) {
	return (kvs_array_search_item(key) != NULL);
}


//KVS_CMD_SET
int kvs_array_set(char *key, char *value) {

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
	
#if 0
	array_table[array_count].key = kcopy;
	array_table[array_count].value = vcopy;
	array_count++;
#else

	int i = 0;
	for (i = 0;i < KVS_ARRAY_ITEM_SIZE;i ++) {
		if (array_table[i].key == NULL && array_table[i].value == NULL) {
			break;
		}
	}

	array_table[i].key = kcopy;
	array_table[i].value = vcopy;
	array_count++;

#endif
	return 0;

}



//KVS_CMD_GET
char *kvs_array_get(char *key) {

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
int kvs_array_delete(char *key) {

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
	array_count--;
	
	return 0;
}


#endif


#if ENABLE_RBTREE

typedef struct _rbtree rbtree_t;
typedef struct _rbtree_node rbtree_node_t;



int init_rbtree(rbtree_t *tree);
void dest_rbtree(rbtree_t *tree);
int put_kv_rbtree(rbtree_t *tree, char *key, char *value);
char* get_kv_rbtree(rbtree_t *tree, char *key);
int count_kv_rbtree(rbtree_t *tree);
int exist_kv_rbtree(rbtree_t *tree, char *key);
int delete_kv_rbtree(rbtree_t *tree, char *key);

extern rbtree_t tree;


int kvs_rbtree_set(char *key, char *value)  {
	return put_kv_rbtree(&tree, key, value);
}

char *kvs_rbtree_get(char *key) {
	return 	get_kv_rbtree(&tree, key);
}

int kvs_rbtree_count(void) {
	return count_kv_rbtree(&tree);
}

int kvs_rbtree_exist(char *key) {
	return exist_kv_rbtree(&tree, key);
}

int kvs_rbtree_delete(char *key) {
	return delete_kv_rbtree(&tree, key);
}


#endif


#if ENABLE_HASH


typedef struct hashtable_s hashtable_t;


int init_hashtable(hashtable_t *hash);
void dest_hashtable(hashtable_t *hash);

int put_kv_hashtable(hashtable_t *hash, char *key, char *value);
char * get_kv_hashtable(hashtable_t *hash, char *key);
int count_kv_hashtable(hashtable_t *hash);
int delete_kv_hashtable(hashtable_t *hash, char *key);
int exist_kv_hashtable(hashtable_t *hash, char *key);


extern hashtable_t hash;



int kvs_hash_set(char *key, char *value)  {
	return put_kv_hashtable(&hash, key, value);
}

char *kvs_hash_get(char *key) {
	return 	get_kv_hashtable(&hash, key);
}

int kvs_hash_count(void) {
	return count_kv_hashtable(&hash);
}

int kvs_hash_exist(char *key) {
	return exist_kv_hashtable(&hash, key);
}

int kvs_hash_delete(char *key) {
	return delete_kv_hashtable(&hash, key);
}



#endif


#if ENABLE_KVENGINE

void init_kvengine(void) {
#if ENABLE_RBTREE
	init_rbtree(&tree);
#endif
#if ENABLE_HASH
	init_hashtable(&hash);
#endif
	// 
}


void dest_kvengine(void) {
#if ENABLE_RBTREE
	dest_rbtree(&tree);
#endif
#if ENABLE_HASH
	dest_hashtable(&hash);
#endif
}


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
// SET 

	switch (cmd) {
#if ENABLE_ARRAY
	case KVS_CMD_SET: {
		assert(count == 3); // 
#if 0
		printf("cmd: %s\n", tokens[0]);
		printf("key: %s\n", tokens[1]);
		printf("value: %s\n", tokens[2]);
#endif
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
		int ret = 0;
		int res = kvs_array_delete(tokens[1]);
		if (!res) {
			memset(msg, 0, CLINET_MSG_LENGTH);
			ret = snprintf(msg, CLINET_MSG_LENGTH, "SUCCESS\r\n");
		} else {
			memset(msg, 0, CLINET_MSG_LENGTH);
			ret = snprintf(msg, CLINET_MSG_LENGTH, "FAILED\r\n");
		}
		return ret;
	}
	case KVS_CMD_EXIST: {

		int res = kvs_array_exist(tokens[1]);
		
		memset(msg, 0, CLINET_MSG_LENGTH);
		int ret = snprintf(msg, CLINET_MSG_LENGTH, "%d\r\n", res);

		return ret;
	}
#endif
	// rbtree
#if ENABLE_RBTREE
	case KVS_CMD_RSET: {
		int ret = 0;
		int res = kvs_rbtree_set(tokens[1], tokens[2]);
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
		char *value = kvs_rbtree_get(tokens[1]);
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

		int res = kvs_rbtree_count();
		
		memset(msg, 0, CLINET_MSG_LENGTH);
		int ret = snprintf(msg, CLINET_MSG_LENGTH, "%d\r\n", res);

		return ret;
	
	}
	case KVS_CMD_RDELETE: {
		int ret = 0;
		int res = kvs_rbtree_delete(tokens[1]);
		if (!res) {
			memset(msg, 0, CLINET_MSG_LENGTH);
			ret = snprintf(msg, CLINET_MSG_LENGTH, "SUCCESS\r\n");
		} else {
			memset(msg, 0, CLINET_MSG_LENGTH);
			ret = snprintf(msg, CLINET_MSG_LENGTH, "FAILED\r\n");
		}
		return ret;
	}
	case KVS_CMD_REXIST:{

		int res = kvs_rbtree_exist(tokens[1]);
		
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
#if ENABLE_HASH
	case KVS_CMD_HSET: {
		int ret = 0;
		int res = kvs_hash_set(tokens[1], tokens[2]);

		// send_slave(msg);
		
		if (!res) {
			memset(msg, 0, CLINET_MSG_LENGTH);
			ret = snprintf(msg, CLINET_MSG_LENGTH, "SUCCESS\r\n");
		} else {
			memset(msg, 0, CLINET_MSG_LENGTH);
			ret = snprintf(msg, CLINET_MSG_LENGTH, "FAILED\r\n");
		}
		return ret;
	}
	case KVS_CMD_HGET: {

		int ret = 0;
		char *value = kvs_hash_get(tokens[1]);
		if (value) {
			memset(msg, 0, CLINET_MSG_LENGTH);
			ret = snprintf(msg, CLINET_MSG_LENGTH, "%s\r\n", value);
		} else {
			memset(msg, 0, CLINET_MSG_LENGTH);
			ret = snprintf(msg, CLINET_MSG_LENGTH, "FAILED: NO EXIST\r\n");
		}
		
		return ret; 
	}
	case KVS_CMD_HCOUNT: {

		int res = kvs_hash_count();
		
		memset(msg, 0, CLINET_MSG_LENGTH);
		int ret = snprintf(msg, CLINET_MSG_LENGTH, "%d\r\n", res);

		return ret;
		

	}
	case KVS_CMD_HDELETE: {
		int ret = 0;
		int res = kvs_hash_delete(tokens[1]);
		if (!res) {
			memset(msg, 0, CLINET_MSG_LENGTH);
			ret = snprintf(msg, CLINET_MSG_LENGTH, "SUCCESS\r\n");
		} else {
			memset(msg, 0, CLINET_MSG_LENGTH);
			ret = snprintf(msg, CLINET_MSG_LENGTH, "FAILED\r\n");
		}
		return ret;
	}
	case KVS_CMD_HEXIST: {

		int res = kvs_hash_exist(tokens[1]);
		
		memset(msg, 0, CLINET_MSG_LENGTH);
		int ret = snprintf(msg, CLINET_MSG_LENGTH, "%d\r\n", res);

		return ret;
	}
#endif
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
	//int i = 0;
	unused(length);
#if 0
	for (i = 0; i < count; i++) {   
        printf("%s\n", tokens[i]);
    }
#endif
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
			//printf("read from server: %.*s\n", ret, buf);

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
	//printf("listen port : %d\n", port);

	
	//struct timeval tv_begin;
	//gettimeofday(&tv_begin, NULL);

	while (1) {
		socklen_t len = sizeof(struct sockaddr_in);
		int cli_fd = accept(fd, (struct sockaddr*)&remote, &len);
#if 0
		if (cli_fd % 1000 == 999) {

			struct timeval tv_cur;
			memcpy(&tv_cur, &tv_begin, sizeof(struct timeval));
			
			//gettimeofday(&tv_begin, NULL);
			//int time_used = TIME_SUB_MS(tv_begin, tv_cur);
			
			//printf("client fd : %d, time_used: %d\n", cli_fd, time_used);
		}
		//printf("new client comming\n");
#endif
		nty_coroutine *read_co;
		nty_coroutine_create(&read_co, server_reader, &cli_fd);

	}
	
}



int main() {
	
	init_kvengine();
	
	nty_coroutine *co = NULL;

	int i = 0;
	unsigned short base_port = 8000;
	//for (i = 0;i < 100;i ++) {
	unsigned short *port = calloc(1, sizeof(unsigned short));
	*port = base_port + i;
	nty_coroutine_create(&co, server, port); ////////no run
	//}


	nty_schedule_run(); //run

	dest_kvengine();

	return 0;
}



