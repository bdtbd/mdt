#include <stdio.h>
#include <stdint.h>
#include <unistd.h>
#include <string>
#include <iostream>
#include <fstream>
#include <sys/time.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>

std::string TimeToString(struct timeval* filetime) {
#ifdef OS_LINUX
    pid_t tid = syscall(SYS_gettid);
#else
    pthread_t tid = pthread_self();
#endif
    uint64_t thread_id = 0;
    memcpy(&thread_id, &tid, std::min(sizeof(thread_id), sizeof(tid)));

    struct timeval now_tv;
    gettimeofday(&now_tv, NULL);
    const time_t seconds = now_tv.tv_sec;
    struct tm t;
    localtime_r(&seconds, &t);
    char buf[34];
    char* p = buf;
    p += snprintf(p, 34,
            "%04d-%02d-%02d-%02d:%02d:%02d.%06d.%06lu",
            t.tm_year + 1900,
            t.tm_mon + 1,
            t.tm_mday,
            t.tm_hour,
            t.tm_min,
            t.tm_sec,
            static_cast<int>(now_tv.tv_usec),
            (unsigned long)thread_id);
    std::string time_buf(buf, 33);
    *filetime = now_tv;
    return time_buf;
}

int main(int ac, char* av[]) {
    if (ac < 2) {
        exit(-1);
    }
    struct timeval null_tv;
    std::string str = "TIMING 12-07 12:08:34 GarbageMain-all 0 18146 http://m.7wenta.com/topic/1a91e6b1bb6c8a0c5b9aa4addfec15c4.html; stage info |garbage_docjoin:112.936:0;build_garbage_pre:208.852:0;trans_before_title_seg:6.178:0;title_seg:6.401:0;title_cluster:15311.1:0;trans_before_build_garbage_post:0.99:0;build_garbage_post:1.267:0;trans_before_adjust_layer:0.489:0;adjust_layer:1782.19:0;layer_set:205.573:0;dump_url_table:510.65:0;total_tm:18146.6|";
    std::string filename = std::string(av[1]) + "/" + TimeToString(&null_tv);
    FILE* file = fopen(filename.c_str(), "w");
    fclose(file);
    std::ofstream ofile(filename.c_str());
    while (1) {
        ofile << str << std::endl;
        usleep(1000);
    }

    return 0;
}
