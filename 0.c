#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stddef.h>
#include <syslog.h>
#include <sys/types.h>
#include <unistd.h>
#include <libpq-fe.h>

int main(int argc, char *argv[])
{

    PGconn *pqA, *pqF;
    PGresult *result;
    unsigned char buff[4096];
    int cnt = -1;
    pid_t pid = getpid();

    printf("Test Run %d\n", pid);

    pqA = PQsetdbLogin("127.0.0.1",
                        "5432",
                        NULL,
                        NULL,
                        "postgres",
                        "gpsprivatagro",
                        "SE@Wf#vm!ERtr5");
    if(PQstatus(pqA) == CONNECTION_BAD) {
        printf("Failed to connect to Arch DB\n");
        return 0;
    }

    sprintf(
        buff,
        "SELECT COUNT(dev_id) FROM log.copy"
    );

    result = PQexec(pqA, buff);

    if(PQntuples(result) != 0) {
        cnt = atoi(PQgetvalue(result, 0, 0));
    }
    PQclear(result);

    printf("Found %d rows\n", cnt);
    return 0;
}
