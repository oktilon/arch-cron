#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <syslog.h>
#include <time.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <libpq-fe.h>

#include "copy.h"

const char* Copy::col_e  = "\033[0m";
const char* Copy::col_gr = "\033[1;30m";
const char* Copy::col_r  = "\033[1;31m";
const char* Copy::col_g  = "\033[1;32m";
const char* Copy::col_y  = "\033[1;33m";
const char* Copy::col_b  = "\033[1;34m";
const char* Copy::col_m  = "\033[1;35m";
const char* Copy::col_cy = "\033[1;36m";
const char* Copy::col_w  = "\033[1;37m";
const char* Copy::col_dr = "\033[0;31m";
const char* Copy::col_dg = "\033[0;32m";
const char* Copy::col_dy = "\033[0;33m";
const char* Copy::col_db = "\033[0;34m";
const char* Copy::col_dm = "\033[0;35m";
const char* Copy::col_dc = "\033[0;36m";
const char* Copy::col_dw = "\033[0;37m";

char *Copy::pLockFile = NULL;

Copy::Copy(int id, char* buf) {
    buff = buf;
    pga = Db::arch();
    pgf = Db::fast();
    error = (char*)malloc(50);
    sBeg = (char*)malloc(20);
    sEnd = (char*)malloc(20);

    bool bNew = false;


    sprintf(buff, "SELECT COUNT(dev_id) FROM log.copy WHERE dev_id = %d", id);
    pga->exec(buff);
    if(pga->intval(0, 0) == 0) {
        printf("%sNew copy%s\n", Copy::col_y, Copy::col_e);

        sprintf(buff, "SELECT _id, extract(epoch from when1 at time zone 'Europe/Kiev') AS dt "
                      "FROM device_data.events_%d "
                      "ORDER BY when1 ASC LIMIT 1", id);
        if(pgf->exec(buff) != ExecStatusType::PGRES_TUPLES_OK) {
            sprintf(error, "init error");
            printf("%scan't read%s events for %s%d%s\n", Copy::col_r, Copy::col_e,
                    Copy::col_cy, id, Copy::col_e);
            return;
        } else {
            bNew = true;
            dev_id = id;
            event_id = pgf->intval(0, 0);
            dt = pgf->intval(0, 1);
            pgf->free();
        }
    } else {
        sprintf(buff, "SELECT event_id, extract(epoch from dt at time zone 'Europe/Kiev') "
                      "FROM log.copy WHERE dev_id = %d", id);
        if(pga->exec(buff) != ExecStatusType::PGRES_TUPLES_OK) {
            printf("can't read Copy(%s%d%s)\n", Copy::col_r, id, Copy::col_e);
        } else {
            dev_id = id;
            event_id = pga->intval(0, 0);
            dt = pga->intval(0, 1);
        }
        pga->free();
    }
    if(dev_id > 0) {
        time_t t = dt;
        if(bNew) t--;
        char s[50];
        Copy::getTime(t, s);
        sprintf(buff, "SELECT COUNT(_id) FROM device_data.events_{$id}"
                        "WHERE when1 > '%s'", s);
        if(pgf->exec(buff) == ExecStatusType::PGRES_TUPLES_OK) {
            int cnt = pgf->intval(0, 0);
            if(cnt == 0) {
                dev_id = 0;
                sprintf(error, "no fresh data");
            }
        }
        pgf->free();
    }
    setTimeLimits();
};

Copy::~Copy() {
    free(sBeg);
    free(sEnd);
    free(error);
}

void Copy::setTimeLimits() {
    Copy::getTime(dt, sBeg);
    dtEnd = dt + 86400; // 24 hours = 1440 minutes
    Copy::getTime(dtEnd, sEnd);
}

bool Copy::valid() {
    if(dev_id > 0) {
        printf("Copy DEV=%s%d%s\n", col_y, dev_id, col_e);
    }
    sleep(1);
    return dev_id > 0;
}

int *Copy::readDevices(Db *pgf, int *cnt) {
    int *ret;
    pgf->exec("SELECT _id FROM devices ORDER BY _id");
    if(pgf->resultStatus() == ExecStatusType::PGRES_TUPLES_OK) {
        *cnt = pgf->rows();
        ret = (int*)malloc(sizeof(int) * (*cnt));
        for(int i = 0; i < *cnt; i++) {
            ret[i] = pgf->intval(i, 0);
        }
    } else {
        ret = (int*)malloc(sizeof(int));
        ret[0] = 0;
    }
    return ret;
}

bool Copy::pidLock(const char *fName) {
    if(strlen(fName) == 0) return true;
    FILE *pf;
    struct stat st;
    time_t now = time(NULL);
    int tm = 0;
    int i = stat(fName, &st);
    int p = -1;
    int pid = 0;
    if(i == 0) { // file Exists
        struct tm *ts;
        char tmNow[50];
        tm = (int)(now - st.st_mtime);
        ts = localtime(&st.st_mtime);
        strftime(tmNow, sizeof(tmNow), "%d.%m.%Y %X", ts);

        pf = fopen(fName, "r");
        if(pf != NULL) {
            char buf[100];
            size_t r = fread(buf, 1, 99, pf);
            pid = atoi(buf);
            fclose(pf);
        }
        if(pid > 0) {
            char prc[20];
            sprintf(prc, "/proc/%d", pid);
            p = stat(prc, &st);
        }
        if(p == 0) {
            printf("Previous script (%d) works since %s (%d sec.)\n", pid, tmNow, tm);
            return false;
        }
    }
    pid_t my = getpid();
    pf = fopen(fName, "w");
    if(pf != NULL) {
        pLockFile = (char*)fName;
        char buf[50];
        sprintf(buf, "%d", my);
        fputs(buf, pf);
        fclose(pf);
        return true;
    }
    printf("%s (%d) can't create lock\n", fName, my);
    return false;
}

void Copy::pidUnLock() {
    struct stat st;
    if(pLockFile != NULL) {
        int s = stat(pLockFile, &st);
        if(s == 0) {
            remove(pLockFile);
        }
    }
}

char *Copy::getTime(time_t t, char *buf) {
    struct tm *ts;
    ts = localtime(&t);
    strftime(buf, sizeof(buf), "%Y-%m-%d %X", ts);
    return buf;
}

time_t Copy::strtotime(const char *s) {
    if(strlen(s)<19) return 0;
    char ss[20];
    strcpy(ss, s);
    struct tm tm;

    tm.tm_sec  = atoi(ss+17);
    ss[16] = 0;
    tm.tm_min  = atoi(ss+14);
    ss[13] = 0;
    tm.tm_hour = atoi(ss+11);
    ss[10] = 0;
    tm.tm_mday = atoi(ss+8);
    ss[7] = 0;
    tm.tm_mon  = atoi(ss+5)-1;
    ss[4] = 0;
    tm.tm_year = atoi(ss)-1900;

    return mktime(&tm);
}