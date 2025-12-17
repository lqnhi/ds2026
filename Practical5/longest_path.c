
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_LINE 4096

static void trim_newline(char *s) {
    size_t len = strlen(s);
    while (len > 0 && (s[len-1] == '\n' || s[len-1] == '\r')) {
        s[len-1] = '\0';
        len--;
    }
}

int main(int argc, char **argv) {
    char **filenames = NULL;
    int file_count = 0;
    int interactive = 0;

    if (argc > 1) {
        filenames = &argv[1];
        file_count = argc - 1;
    } else {
        /* Interactive mode */
        int n = 0;
        printf("LONGEST PATH SYSTEM\n");
        printf("Enter number of input files: ");
        if (scanf("%d", &n) != 1) {
            fprintf(stderr, "Invalid number provided.\n");
            return 1;
        }
        getchar(); // clear newline
        if (n <= 0) {
            printf("No input files. Exiting.\n");
            return 0;
        }
        file_count = n;
        interactive = 1;
        filenames = (char**)malloc(sizeof(char*) * file_count);
        if (!filenames) {
            fprintf(stderr, "Memory allocation failed.\n");
            return 1;
        }
        for (int i = 0; i < file_count; i++) {
            char buf[1024];
            printf("Enter file %d name: ", i + 1);
            if (!fgets(buf, sizeof(buf), stdin)) {
                fprintf(stderr, "No input.\n");
                filenames[i] = NULL;
                continue;
            }
            trim_newline(buf);
            filenames[i] = strdup(buf);
            if (!filenames[i]) {
                fprintf(stderr, "Memory allocation failed.\n");
                for (int j = 0; j < i; j++) free(filenames[j]);
                free(filenames);
                return 1;
            }
        }
    }

    size_t maxLength = 0;
    char **maxPaths = NULL;
    size_t maxPathsCount = 0;

    for (int i = 0; i < file_count; i++) {
        const char *fname = filenames[i];
        if (!fname) continue;

        FILE *fp = fopen(fname, "r");
        if (!fp) {
            perror(fname);
            continue;
        }

        char line[MAX_LINE];
        while (fgets(line, sizeof(line), fp)) {
            trim_newline(line);
            if (line[0] == '\0') continue; /* skip empty */

            size_t len = strlen(line);
            if (len > maxLength) {
                for (size_t k = 0; k < maxPathsCount; k++) free(maxPaths[k]);
                free(maxPaths);
                maxPaths = NULL;
                maxPathsCount = 0;

                maxPaths = (char**)malloc(sizeof(char*));
                if (!maxPaths) { fprintf(stderr, "Memory error.\n"); fclose(fp); goto cleanup; }
                maxPaths[0] = strdup(line);
                if (!maxPaths[0]) { fprintf(stderr, "Memory error.\n"); fclose(fp); goto cleanup; }
                maxPathsCount = 1;
                maxLength = len;
            } else if (len == maxLength) {
                char **tmp = (char**)realloc(maxPaths, sizeof(char*) * (maxPathsCount + 1));
                if (!tmp) { fprintf(stderr, "Memory error.\n"); fclose(fp); goto cleanup; }
                maxPaths = tmp;
                maxPaths[maxPathsCount] = strdup(line);
                if (!maxPaths[maxPathsCount]) { fprintf(stderr, "Memory error.\n"); fclose(fp); goto cleanup; }
                maxPathsCount++;
            }
        }

        fclose(fp);
    }

    if (maxPathsCount == 0) {
        printf("No paths found.\n");
    } else {
        printf("\nLONGEST PATH(S)\n");
        for (size_t i = 0; i < maxPathsCount; i++) {
            printf("%s\n", maxPaths[i]);
        }
        printf("\nLongest Length = %zu characters\n", maxLength);
    }

cleanup:
    if (interactive) {
        for (int i = 0; i < file_count; i++) if (filenames[i]) free(filenames[i]);
        free(filenames);
    }
    if (maxPaths) {
        for (size_t i = 0; i < maxPathsCount; i++) free(maxPaths[i]);
        free(maxPaths);
    }

    return 0;
}