#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stddef.h>
#include <syslog.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdarg.h>
#include <libpq-fe.h>

#include "copy.h"

const char* Copy::col_e  = "\033[0m";
const char* Copy::col_g  = "\033[1;32m";
const char* Copy::col_y  = "\033[1;33m";
const char* Copy::col_cy = "\033[1;36m";


Copy::Copy(int id) {
    dev_id = id;
};

void Copy::print() {
    printf("Copy dev_id=%s%d%s\n", col_y, dev_id, col_e);
}

