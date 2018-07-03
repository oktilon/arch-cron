#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stddef.h>
#include <syslog.h>
#include <sys/types.h>
#include <unistd.h>
#include <libpq-fe.h>

#include "copy.h"

Copy::Copy(int id) {
    dev_id = id;
};

void Copy::print() {
    printf("Copy dev_id=%d\n", dev_id);
}