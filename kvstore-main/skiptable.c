
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#include "kvengine.h"


#define MAX_LEVEL 6

#define KEYTYPE_ENABLE		1

#define MAX_KEY_LEN			128
#define MAX_VALUE_LEN		512


typedef struct Node {
#if KEYTYPE_ENABLE
	char key[MAX_KEY_LEN];
	char value[MAX_VALUE_LEN];
#else
    int key;
    int value;
#endif
    struct Node** forward;
} Node;


typedef struct SkipList {
    int level; 
    Node* header; 

	int nb_elements;
    pthread_mutex_t lock;
} SkipList;

skiptable_t table;


#if KEYTYPE_ENABLE

Node* createNode(const char *key, char *value, int level) {

	if (!key || !value) return NULL;

    Node* node = (Node*)malloc(sizeof(Node));
    if (!node) return NULL;
    
    strncpy(node->key , key, MAX_KEY_LEN);
	strncpy(node->value, value, MAX_VALUE_LEN);
    node->forward = (Node**)malloc(sizeof(Node*) * level);
    if (!node->forward) {
		free(node);
		return NULL;
    }
    
    for (int i = 0; i < level; ++i) {
        node->forward[i] = NULL;
    }
    
    return node;
}


#else
Node* createNode(int key, int value, int level) {
    Node* node = (Node*)malloc(sizeof(Node));
    node->key = key;
    node->value = value;
    node->forward = (Node**)malloc(sizeof(Node*) * level);
    for (int i = 0; i < level; ++i) {
        node->forward[i] = NULL;
    }
    return node;
}
#endif

SkipList* createSkipList() {
    SkipList* list = (SkipList*)malloc(sizeof(SkipList));
    list->level = 1;
    list->header = createNode("", "", MAX_LEVEL);
	list->nb_elements = 0;
	
    return list;
}

#if KEYTYPE_ENABLE

void insert(SkipList* list, char *key, char *value) {
    int i, level;
    Node* update[MAX_LEVEL];
    Node* p;

    p = list->header;
    for (i = list->level - 1; i >= 0; --i) {
        while (p->forward[i] != NULL && strcmp(p->forward[i]->key, key) < 0) {  // strcmp
            p = p->forward[i];
        }
        update[i] = p;
    }

    if (p->forward[0] != NULL && strcmp(p->forward[0]->key, key) == 0) { // exist, 
        
        strncpy(p->forward[0]->value, value, MAX_VALUE_LEN);
        
    } else {
        
        level = 1;
        while (rand() % 2 && level < MAX_LEVEL) {
            ++level;
        }

       
        Node* node = createNode(key, value, level);
        if (level > list->level) {
            
            for (i = list->level; i < level; ++i) {
                update[i] = list->header;
            }
            list->level = level;
        }

        
        for (i = 0; i < level; ++i) {
            node->forward[i] = update[i]->forward[i];
            update[i]->forward[i] = node;
        }
    }
}



#else

void insert(SkipList* list, int key, int value) {
    int i, level;
    Node* update[MAX_LEVEL];
    Node* p;

    p = list->header;
    for (i = list->level - 1; i >= 0; --i) {
        while (p->forward[i] != NULL && p->forward[i]->key < key) {
            p = p->forward[i];
        }
        update[i] = p;
    }

    if (p->forward[0] != NULL && p->forward[0]->key == key) {
        
        p->forward[0]->value = value;
    } else {
        
        level = 1;
        while (rand() % 2 && level < MAX_LEVEL) {
            ++level;
        }

       
        Node* node = createNode(key, value, level);
        if (level > list->level) {
            
            for (i = list->level; i < level; ++i) {
                update[i] = list->header;
            }
            list->level = level;
        }

        
        for (i = 0; i < level; ++i) {
            node->forward[i] = update[i]->forward[i];
            update[i]->forward[i] = node;
        }
    }
}

#endif

#if KEYTYPE_ENABLE

char * find(SkipList* list, char * key) {
    int i;
    Node* p = list->header;
    for (i = list->level - 1; i >= 0; --i) {
        while (p->forward[i] != NULL && (strcmp(p->forward[i]->key, key) < 0)) {
            p = p->forward[i];
        }
    }
    
    if (p->forward[0] != NULL && (strcmp(p->forward[0]->key, key) == 0)) {
        return p->forward[0]->value;
    } else {
        return NULL;
    }
}


#else

int find(SkipList* list, int key) {
    int i;
    Node* p = list->header;
    for (i = list->level - 1; i >= 0; --i) {
        while (p->forward[i] != NULL && p->forward[i]->key < key) {
            p = p->forward[i];
        }
    }
    if (p->forward[0] != NULL && p->forward[0]->key == key) {
        return p->forward[0]->value;
    } else {
        return -1;
    }
}
#endif


#if KEYTYPE_ENABLE 

void delete(SkipList* list, char *key) {
    int i;
    Node* update[MAX_LEVEL];
    Node* p;

    p = list->header;
    for (i = list->level - 1; i >= 0; --i) {
        while (p->forward[i] != NULL && strcmp(p->forward[i]->key, key) < 0) {
            p = p->forward[i];
        }
        update[i] = p;
    }

    if (p->forward[0] != NULL && strcmp(p->forward[0]->key, key) == 0) {
        
        Node* node = p->forward[0];
        for (i = 0; i < list->level; ++i) {
            if (update[i]->forward[i] == node) {
                update[i]->forward[i] = node->forward[i];
            }
        }
        free(node);

    }

}


#else
void delete(SkipList* list, int key) {
    int i;
    Node* update[MAX_LEVEL];
    Node* p;

    p = list->header;
    for (i = list->level - 1; i >= 0; --i) {
        while (p->forward[i] != NULL && p->forward[i]->key < key) {
            p = p->forward[i];
        }
        update[i] = p;
    }

    if (p->forward[0] != NULL && p->forward[0]->key == key) {
        
        Node* node = p->forward[0];
        for (i = 0; i < list->level; ++i) {
            if (update[i]->forward[i] == node) {
                update[i]->forward[i] = node->forward[i];
            }
        }
        free(node);
    }
}
#endif

void print(SkipList* list) {
    int i;
    Node* p;
    for (i = list->level - 1; i >= 0; --i) {
        printf("Level %d: ", i);
        p = list->header->forward[i];
        while (p != NULL) {
        #if KEYTYPE_ENABLE
			printf("(%s, %s) ", p->key, p->value);
        #else
            printf("(%d,%d) ", p->key, p->value);
        #endif
            p = p->forward[i];
        }
        printf("\n");
    }
    printf("\n");
}



// init_skiptable
int init_skiptable(skiptable_t *table) {

	if (!table) return -1;

	table->level = 1;
    table->header = createNode("", "", MAX_LEVEL);

    pthread_mutex_init(&table->lock, NULL);
	
	return 0;
}

// dest_skiptable
void dest_skiptable(skiptable_t *table) {

	if (!table) return ;

	int i;
    skipnode_t* p;
    for (i = table->level - 1; i >= 0; --i) {

		p = table->header->forward[i];

		while (p != NULL) {
			pthread_mutex_lock(&table->lock);
			delete(table, p->key);
			pthread_mutex_unlock(&table->lock);

			p = p->forward[i];
		}
    }

    free(table->header);

}

// put_kv_skiptable
int put_kv_skiptable(skiptable_t *table, char *key, char *value) {

	if (!table || !key || !value) return -1;

	pthread_mutex_lock(&table->lock);
	insert(table, key, value);
	table->nb_elements ++;
	pthread_mutex_unlock(&table->lock);

	return 0;

}


// get_kv_skiptable
char *get_kv_skiptable(skiptable_t *table, char *key) {

	if (!table || !key) return NULL;

	return find(table, key);
}

int count_kv_skiptable(skiptable_t *table) {

	return table->nb_elements;

}


int delete_kv_skiptable(skiptable_t *table, char *key) {

	char *value = find(table, key);
	if (value == NULL) { // no exist
		return -1;
	}

	delete(table, key);
	return 0;

}

int exist_kv_skiptable(skiptable_t *table, char *key) {

	char *value = find(table, key);
	if (value == NULL) { // no exist
		return 0;
	}
	return 1;
}

