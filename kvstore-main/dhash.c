#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define INITIAL_SIZE 16  
#define GROWTH_FACTOR 2  

typedef struct {
    char *key;
    int value;
} hash_node;

typedef struct {
    int size;            
    int count;           
    hash_node **nodes;   
} hash_table;

hash_node *create_hash_node(const char *key, int value) {
    hash_node *node = (hash_node *) malloc(sizeof(hash_node));
    node->key = strdup(key);
    node->value = value;
    
    return node;
}

void destroy_hash_node(hash_node *node) {
    free(node->key);
    free(node);
}

hash_table *create_hash_table(int size) {
    hash_table *table = (hash_table *) malloc(sizeof(hash_table));
    
    table->size = size;
    table->count = 0;
    
    table->nodes = (hash_node **) calloc(size, sizeof(hash_node *));
    
    return table;
}

void destroy_hash_table(hash_table *table) {
    for (int i = 0; i < table->size; i++) {
        hash_node *node = table->nodes[i];
        
        if (node != NULL) {
            destroy_hash_node(node);
        }
    }
    
    free(table->nodes);
    free(table);
}

int hash_function(const char *key, int size) {
    unsigned long int value = 0;
    
    for (int i = 0; i < strlen(key); i++) {
        value = value * 37 + key[i];
    }
    
    value = value % size;
    
    return (int) value;
}


void insert(hash_table *table, const char *key, int value) {
  
    if ((table->count * 2) >= table->size) {
        hash_table *new_table = create_hash_table(table->size * GROWTH_FACTOR);

        for (int i = 0; i < table->size; i++) {
            hash_node *node = table->nodes[i];
            
            if (node != NULL) {
                insert(new_table, node->key, node->value);
            }
        }

        table->size *= GROWTH_FACTOR;
        table->count = new_table->count;
		
        hash_node **tmp_nodes = table->nodes;
        table->nodes = new_table->nodes;
        new_table->nodes = tmp_nodes;
        
		new_table->size /= GROWTH_FACTOR;
        destroy_hash_table(new_table);
		
    }
    

    int index = hash_function(key, table->size);
    while (table->nodes[index] != NULL) {
		if (table->nodes[index]->key != NULL && strcmp(table->nodes[index]->key, key)) {
			index++;
        
	        if (index == table->size) {
	            index = 0;
	        }
		}
        
    }

    if (table->nodes[index] != NULL) {
        destroy_hash_node(table->nodes[index]);
    } else {
        table->count++;
    }
    
    table->nodes[index] = create_hash_node(key, value);

}

int search(hash_table *table, const char *key) {
    int index = hash_function(key, table->size);
    
    while (table->nodes[index] != NULL && strcmp(table->nodes[index]->key, key)) {
        index++;
        
        if (index == table->size) {
            index = 0;
        }
    }
    
    return (table->nodes[index] == NULL) ? -1 : table->nodes[index]->value;
}

int main() {
    hash_table *table = create_hash_table(INITIAL_SIZE);
    
    insert(table, "apple", 10);
    insert(table, "banana", 20);
    insert(table, "orange", 30);
	insert(table, "king", 20);
    insert(table, "abc", 30);
	insert(table, "bcd", 10);
    insert(table, "cde", 20);
    insert(table, "edf", 30);
	insert(table, "grd", 20);
    insert(table, "dsd", 30);

	insert(table, "qaz", 10);
    insert(table, "wsx", 20);
    insert(table, "edc", 30);
	insert(table, "rfv", 20);
    insert(table, "tgb", 30);
	insert(table, "yhn", 10);
    insert(table, "ujm", 20);
    insert(table, "plk", 30);
	insert(table, "iop", 20);
    insert(table, "ujp", 30);
    
    printf("apple: %d\n", search(table, "apple"));
    printf("banana: %d\n", search(table, "banana"));
    printf("orange: %d\n", search(table, "orange"));
    
    destroy_hash_table(table);
    
   return 0;
}
