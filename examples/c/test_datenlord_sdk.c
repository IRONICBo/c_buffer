#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <stdint.h>
#include "datenlord.h"

void print_error(datenlord_error *err) {
    if (err) {
        printf("Error code: %d, message: %.*s\n", err->code, (int)err->message.len, err->message.data);
    } else {
        printf("No errors occurred.\n");
    }
}

void demo_init_and_dir_check() {
    const char *config = "{ \"log_level\": \"info\", \"connection\": \"localhost\" }";
    datenlord_error *err = init(config);
    print_error(err);

    if (err) {
        free(err);
        return;
    }

    const char *dir_path = "/tmp/datenlord_test";
    bool exists_result = exists(dir_path);
    printf("Directory %s exists? %s\n", dir_path, exists_result ? "Yes" : "No");

    if (!exists_result) {
        err = mkdir(dir_path);
        print_error(err);
        if (err) free(err);
    }
}

void demo_write_and_read_file() {
    const char *file_path = "/tmp/datenlord_test/hello.txt";
    const char *content = "Hello, Datenlord!";

    datenlord_bytes write_bytes;
    write_bytes.data = (const uint8_t *)content;
    write_bytes.len = strlen(content);

    datenlord_error *err = write_file(file_path, write_bytes);
    print_error(err);
    if (err) {
        free(err);
        return;
    }

    datenlord_bytes read_bytes;
    err = read_file(file_path, &read_bytes);
    print_error(err);

    if (!err) {
        printf("Read file content: %.*s\n", (int)read_bytes.len, read_bytes.data);
    } else {
        free(err);
    }
}

void demo_delete_directory() {
    const char *dir_path = "/tmp/datenlord_test";

    datenlord_error *err = delete_dir(dir_path, true);
    print_error(err);

    if (err) free(err);
}

int main() {
    demo_init_and_dir_check();

    demo_write_and_read_file();

    demo_delete_directory();

    return 0;
}
