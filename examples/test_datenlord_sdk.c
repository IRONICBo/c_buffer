// #include <stdio.h>
// #include "datenlord.h"

// int main() {
//     // 初始化 SDK
//     init("log_level=INFO");

//     // 检查目录是否存在
//     if (exists("/tmp/test")) {
//         printf("Directory exists!\n");
//     } else {
//         printf("Directory does not exist, creating it.\n");
//         mkdir("/tmp/test");
//     }

//     // 创建文件并写入内容
//     write_file("/tmp/test/myfile.txt", "Hello, DatenLord!");

//     // 读取文件内容
//     char* content = read_file("/tmp/test/myfile.txt");
//     printf("File content: %s\n", content);

//     // 删除目录
//     delete("/tmp/test", 1);

//     return 0;
// }


#include <stdio.h>
#include <stdlib.h>
#include "datenlord.h"

int main() {
    const char *input = "Hello, Rust!";

    char *reversed = reverse_string(input);
    if (reversed) {
        printf("Reversed string: %s\n", reversed);
        free_string(reversed);
    }

    perform_async_task();

    return 0;
}