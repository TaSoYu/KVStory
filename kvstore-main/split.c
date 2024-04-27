
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_TOKENS 100   

int main() {
    char str[] = "SET Name King";
    char *tokens[MAX_TOKENS];   
    int count = 0;
    
    char *token = strtok(str, " ");   
    
    while (token != NULL) {
        tokens[count++] = token;   
        token = strtok(NULL, " ");
    }
    
    for (int i = 0; i < count; i++) {   
        printf("%s\n", tokens[i]);
    }
    
    return 0;
}

