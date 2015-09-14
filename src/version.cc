#include <iostream>
#include "version.h"
extern const char kGitInfo[] = "\
origin	https://github.com/00k/mdt (fetch)\n\
origin	https://github.com/00k/mdt (push)\n\
commit a37d8b4200a1d2780e3c2724a26b6800eeab5873\n\
Author: 00k <00k@users.noreply.github.com>\n\
Date:   Fri Sep 11 11:12:11 2015 +0800\n\
";
extern const char kBuildType[] = "debug";
extern const char kBuildTime[] = "Mon Sep 14 15:07:46 CST 2015";
extern const char kBuilderName[] = "spider";
extern const char kHostName[] = "db-spi-dns-master0.db01.baidu.com";
extern const char kCompiler[] = "gcc (GCC) 4.8.2";
void PrintSystemVersion() {
    std::cout << "=====  Git Info ===== " << std::endl
        << kGitInfo << std::endl;
    std::cout << "=====  Build Info ===== " << std::endl;
    std::cout << "Build Type: " << kBuildType << std::endl;
    std::cout << "Build Time: " << kBuildTime << std::endl;
    std::cout << "Builder Name: " << kBuilderName << std::endl;
    std::cout << "Build Host Name: " << kHostName << std::endl;
    std::cout << "Build Compiler: " << kCompiler << std::endl;
    std::cout << std::endl;
};
