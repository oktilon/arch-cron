#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stddef.h>
#include <syslog.h>
#include <sys/types.h>
#include <regex>
#include <time.h>
#include <unistd.h>

#include "copy.h"
#include "config.h"

std::regex reg1 ("(\033\\[([\\d\\;]+m))");

void Info(const char *msg) {
    printf("%s\n", msg);
    std::string pmsg (std::regex_replace(msg, reg1, "", std::regex_constants::match_default));
    syslog(LOG_CRIT, "%s", pmsg.c_str());
}

char *getTime(time_t *tm, char *buf) {
    time_t t;
    time(&t);
    time_t r = t - *tm;
    *tm = t;
    sprintf(buf, " %s%ld%s sec.", Copy::col_cy, r, Copy::col_e);
    return buf;
}

int main(int argc, char *argv[])
{
    char buff[4096];
    char bufTm[30];
    int cnt = -1;
    pid_t pid = getpid();
    char *me = NULL;
    bool skip = false;
    int dev   = 0;
    time_t tm_start = time(NULL);
    time_t tm_mark = time(NULL);
    int *devices;
    int devices_count = 0;

    /*
        char ttt[] = "2018-07-05 09:00:00";
        time_t tti = Copy::strtotime(ttt);
        printf("%s = %ld\n\n", ttt, tti);

        tti = Copy::getMaxTime();
        printf("MaxTime=%ld [%s]\n\n", tti, Copy::getTime(tti, bufTm));
    */

    memset(buff, 0, 4000);
    bool dev_list = false;
    for(int i = 0; i < argc; i++) {
        char *pa = argv[i];
        if(i == 0) {
            me = pa;
            continue;
        }
        if(strcmp(pa, "-s") == 0 ||
           strcmp(pa, "skip") == 0 ||
           strcmp(pa, "--skip") == 0) {
            skip = true;
            continue;
        }

        if(strcmp(pa, "-d") == 0 ||
           strcmp(pa, "--devices") == 0) {
            dev_list = true;
            devices = (int*)malloc(sizeof(int));
            continue;
        }

        if(dev_list) {
            dev = atoi(argv[i]);
            if(dev > 0) {
                if(devices_count > 0) strcat(buff, ",");
                sprintf(bufTm, "%d", dev);
                strcat(buff, bufTm);
                devices_count++;
                devices = (int*)realloc(devices, devices_count*sizeof(int));
                devices[devices_count-1] = dev;
                continue;
            }
        }

        printf("Usage %s%s%s [%s-s%s] [%s-d%s %sdev1 dev2 ...%s]\n"
               "  where : %s-s%s, %s--skip%s, %sskip%s - skip pg_dump/pg_restore public\n"
               "          %s-d%s, %s--devices%s    - copy devices only from list\n"
               "          %s-h%s, %s--help%s, %shelp%s - show this help\n",
            Copy::col_g, me, Copy::col_e, Copy::col_y, Copy::col_e, Copy::col_y, Copy::col_e, Copy::col_m, Copy::col_e,
            Copy::col_y, Copy::col_e, Copy::col_y, Copy::col_e, Copy::col_y, Copy::col_e,
            Copy::col_y, Copy::col_e, Copy::col_y, Copy::col_e,
            Copy::col_y, Copy::col_e, Copy::col_y, Copy::col_e, Copy::col_y, Copy::col_e);
        return 0;
    }

    const char *bs = skip ? Copy::col_g : Copy::col_r;
    const char *ts = skip ? "true" : "false";
    const char *bd = devices_count > 0 ? Copy::col_g : Copy::col_r;

    if(devices_count == 0) sprintf(buff, "0");

    printf("Copy Arch pid=%d, dev=%s%s%s, skip=%s%s%s\n", pid, bd, buff, Copy::col_e, bs, ts, Copy::col_e);

    if(!Copy::pidLock(LOCK_FILE)) {
        return 0;
    }

    if(!skip) {
        time_t rawtime;
        tm * timeinfo;
        time(&rawtime);
        timeinfo=localtime(&rawtime);
        int wday=timeinfo->tm_wday;

        // copy public
        char out[30];
        sprintf(out, "~/.dump/pub%d.tar", wday);

        Info("pg_dump ...");
        sprintf(buff,
            "%s -h %s -p 5432 -U %s -d postgres -F t -n public -f %s",
            PG_DUMP,
            DB_FAST,
            DB_USER,
            out);
        system(buff);
        sprintf(buff, "%s%s", "pg_dumped", getTime(&tm_mark, bufTm));
        Info(buff);

        Info("pg_restore ...");
        sprintf(buff,
            "%s -h %s -p 5432 -U %s -d postgres --clean -F t -n public -f %s",
            PG_RESTORE,
            DB_ARCH,
            DB_USER,
            out);
        system(buff);
        sprintf(buff, "%s%s", "pg_restored", getTime(&tm_mark, bufTm));
        Info(buff);
    }

    // Connect databases
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

    // Read devices
    if(devices_count > 0) {
        sprintf(buff, "selected %d dev.%s", devices_count, getTime(&tm_mark, bufTm));
    } else {
        Info("read devices...");
        devices = Copy::readDevices(pgf, &devices_count);
        sprintf(buff, "readed %d dev.%s", devices_count, getTime(&tm_mark, bufTm));
    }

    Info(buff);

    for(int ix=0; ix < devices_count; ix++) {
        int id = devices[ix];
        try {
            // init
            Copy inf = Copy(id, buff);
            if(!inf.valid()) continue;
            printf("Copy DEV=%s%d%s [%s]", Copy::col_y, id, Copy::col_e, inf.sBeg);

            // calculations
            inf.doCalculations();
            //Info("DEV $id calculations" . getTime());

            inf.verifyEventTable();

            // Inputs list
            int inp_cnt = 0;
            Input **inputs = inf.readInputs(&inp_cnt);
            printf(", inps=%d", inp_cnt);

            // Read events
            int ev_cnt = 0;
            Event **events = inf.readEvents(&ev_cnt);
            printf(", evs=%d\n", ev_cnt);

            // Copy data
            int h = -1;
            int e = 0;
            for(e = 0; e < ev_cnt; e++) {
                int n = events[e]->save(buff);
                h = events[e]->h(h);
                if(n > 0) {
                    for(int k = 0; k < inp_cnt; k++) {
                        inputs[k]->copyInput(events[e], buff);
                    }
                } else {
                    printf("x");
                }
            }
            Event *last = e > 1 ? events[--e] : NULL;
            inf.save(last);
            sprintf(buff, " DEV %s%d%s [%s]%s",
                Copy::col_y, id, Copy::col_e, inf.sBeg,
                getTime(&tm_mark, bufTm));
            Info(buff);
        }
        catch(std::exception& e) {
            sprintf(buff, "Exception for %s%d%s (%s) : %s",
                Copy::col_b, id, Copy::col_e,
                getTime(&tm_mark, bufTm), e.what());
            Info(buff);
        }
    }

    sprintf(buff, "Finish%s", getTime(&tm_start, bufTm));
    Info(buff);
    return 0;
}