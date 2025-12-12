#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <stdlib.h>

#define MAX_WORDS 5000
#define MAX_LEN   100

typedef struct {
    char word[MAX_LEN];
    int count;
} WordCount;

// Convert string to lowercase
void toLowerCase(char *str) {
    for (int i = 0; str[i]; i++)
        str[i] = tolower(str[i]);
}

// Count words from any given text buffer
int countWords(char *text, WordCount wc[]) {
    int wordTotal = 0;
    char *token = strtok(text, " ,.-!?;:\n\t\"()");

    while (token != NULL) {
        toLowerCase(token);

        int found = -1;
        for (int i = 0; i < wordTotal; i++) {
            if (strcmp(wc[i].word, token) == 0) {
                found = i;
                break;
            }
        }

        if (found != -1) {
            wc[found].count++;
        } else {
            strcpy(wc[wordTotal].word, token);
            wc[wordTotal].count = 1;
            wordTotal++;
        }

        token = strtok(NULL, " ,.-!?;:\n\t\"()");
    }

    return wordTotal;
}

// Read file into memory (simple approach)
char* readFile(const char *filename) {
    FILE *fp = fopen(filename, "r");
    if (!fp) {
        printf("Error: Cannot open file!\n");
        return NULL;
    }

    fseek(fp, 0, SEEK_END);
    long size = ftell(fp);
    rewind(fp);

    char *buffer = (char*)malloc(size + 1);
    if (!buffer) {
        printf("Memory error.\n");
        fclose(fp);
        return NULL;
    }

    fread(buffer, 1, size, fp);
    buffer[size] = '\0';
    fclose(fp);
    return buffer;
}

int main() {
    int choice;
    WordCount wc[MAX_WORDS];
    char inputText[5000];

    printf("===== WORD COUNT SYSTEM =====\n");
    printf("1. Type text manually\n");
    printf("2. Upload a text file\n");
    printf("Choose option: ");
    scanf("%d", &choice);
    getchar(); // clear enter key

    char *textToCount = NULL;

    if (choice == 1) {
        printf("\nType your text below:\n");
        fgets(inputText, sizeof(inputText), stdin);
        textToCount = inputText;
    } 
    else if (choice == 2) {
        char filename[260];
        printf("Enter filename: ");
        scanf("%s", filename);

        textToCount = readFile(filename);
        if (!textToCount) return 1; // failed to read file
    } 
    else {
        printf("Invalid option!\n");
        return 1;
    }

    // Count words
    int total = countWords(textToCount, wc);

    printf("\n===== WORD COUNT RESULT =====\n");
    for (int i = 0; i < total; i++) {
        printf("%s : %d\n", wc[i].word, wc[i].count);
    }

    // Free file buffer if used
    if (choice == 2)
        free(textToCount);

    return 0;
}
