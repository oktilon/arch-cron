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

    if(!pga->valid()) {
        printf("Failed to connect to Arch DB\n");
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

    printf("Found %d rows in log.copy\n", cnt);
    return 0;
}
