#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stddef.h>
#include <syslog.h>
#include <sys/types.h>
#include <unistd.h>

#include "copy.h"
#include "db.h"

int main(int argc, char *argv[])
{
    char buff[4096];
    int cnt = -1;
    pid_t pid = getpid();

    printf("Test Run %d\n", pid);

    Copy cp = Copy(2);
    cp.print();

    Db::init();
    Db* pga = Db::arch();
    Db* pgf = Db::fast();

    if(!pga->valid()) {
        printf("Failed to connect to Arch DB\n");
        return 0;
    }
    if(!pgf->valid()) {
        printf("Failed to connect to Fast DB\n");
        return 0;
    }

    sprintf(
        buff,
        "SELECT COUNT(dev_id) FROM log.copy"
    );

    pga->exec(buff);

    if(pga->rows() != 0) {
        cnt = pga->intval(0, 0);
    }
    pga->free();

    printf("Found %d rows in log.copy table\n", cnt);

    for(int i = 0; i < 20; i++) {
        printf("sleep %s%d%s\n", Copy::col_y, i, Copy::col_e);
        sleep(1);
    }

    return 0;
}
