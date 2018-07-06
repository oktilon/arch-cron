#include <exception>
#include <stdexcept>
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
#include "event.h"
#include "input.h"

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
time_t Copy::tm_max = 0;

Copy::Copy(int id, char* buf) {
    buff = buf;
    pga = Db::arch();
    pgf = Db::fast();
    error = (char*)malloc(50);
    sBeg = (char*)malloc(20);
    sEnd = (char*)malloc(20);
    dev_id = 0;
    m_inputs = NULL;
    m_events = NULL;

    bool bNew = false;


    sprintf(buff, "SELECT COUNT(dev_id) FROM log.copy WHERE dev_id = %d", id);
    pga->exec(buff);
    if(pga->intval(0, 0) == 0) {
        sprintf(buff, "SELECT _id, extract(epoch from when1 at time zone 'Europe/Kiev') AS dt "
                      "FROM device_data.events_%d "
                      "ORDER BY when1 ASC LIMIT 1", id);
        if(pgf->exec(buff) != ExecStatusType::PGRES_TUPLES_OK) {
            char *e = pgf->error();
            error = (char*)realloc(error, 10+strlen(e));
            strcpy(error, e);
            printf("Error read %s%d%s events: %s%s%s", col_cy, id, col_e,
                 col_r, error, col_e);
            return;
        } else {
            int cnt = pgf->count();
            if(cnt > 0) {
                printf("New copy %s%d%s\n", col_y, id, col_e);
                bNew = true;
                dev_id = id;
                event_id = pgf->intval(0, 0);
                dt = pgf->intval(0, 1);
            } else {
                printf("No data for %s%d%s\n", col_r, id, col_e);
            }
            pgf->free();
        }
    } else {
        sprintf(buff, "SELECT event_id, extract(epoch from dt at time zone 'Europe/Kiev') "
                      "FROM log.copy WHERE dev_id = %d", id);
        if(pga->exec(buff) != ExecStatusType::PGRES_TUPLES_OK) {
            printf("can't read Copy(%s%d%s)\n", col_r, id, col_e);
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
                printf("DEV %s%d%s %s%s%s", col_y, id, col_e, col_r, error, col_e);
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
    if(m_inputs) free(m_inputs);
    if(m_events) free(m_events);
}

void Copy::setTimeLimits() {
    Copy::getTime(dt, sBeg);
    dtEnd = dt + 86400; // 24 hours = 1440 minutes
    Copy::getTime(dtEnd, sEnd);
}

bool Copy::valid() { return dev_id > 0; }

int *Copy::readDevices(Db *pgf, int *cnt) {
    int *ret;
    pgf->exec("SELECT _id FROM devices ORDER BY _id");
    //pgf->exec("SELECT _id FROM devices WHERE _id > 3000 ORDER BY _id");
    if(pgf->resultStatus() == ExecStatusType::PGRES_TUPLES_OK) {
        *cnt = pgf->count();
        ret = (int*)malloc(sizeof(int) * (*cnt));
        for(int i = 0; i < *cnt; i++) {
            ret[i] = pgf->intval(i, 0);
        }
    } else {
        ret = (int*)malloc(sizeof(int));
        ret[0] = 0;
    }
    pgf->free();
    return ret;
}

void Copy::doCalculations() {
    // 1 - calcRunStop
    sprintf(buff, "SELECT * FROM calcRunStop(%d, '%s', '%s')", dev_id, sBeg, sEnd);
    pgf->exec(buff);
    pgf->free();
}

void Copy::verifyEventTable() {
    sprintf(buff, "SELECT COUNT(relname) FROM pg_catalog.pg_class "
                    "WHERE relname = 'events_%d' AND reltype > 0", dev_id);
    pga->exec(buff);
    int i = pga->count();
    int has = i > 0 ? pga->intval(0, 0) : 0;
    pga->free();
    if(has == 0) {
        sprintf(buff, "CREATE TABLE IF NOT EXISTS device_data.events_%d ( CHECK (device_id = %d ), PRIMARY KEY (_id), "
                "CONSTRAINT \"OUT_KEY_%d\" "
                "    FOREIGN KEY (device_id) REFERENCES devices (_id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE CASCADE DEFERRABLE INITIALLY IMMEDIATE "
                ") INHERITS (device_data.events);", dev_id, dev_id, dev_id);
        pga->exec(buff);
        pga->free();

        sprintf(buff, "GRANT SELECT ON TABLE device_data.events_%d TO userviewer;'", dev_id);
        pga->exec(buff);
        pga->free();
        sprintf(buff, "CREATE INDEX \"EVENTS__ID_idx_%d\" ON device_data.events_%d USING btree (_id );", dev_id, dev_id);
        pga->exec(buff);
        pga->free();
        sprintf(buff, "CREATE INDEX \"EVENTS__device_id_idx_%d\" ON device_data.events_%d USING btree (device_id );", dev_id, dev_id);
        pga->exec(buff);
        pga->free();
        sprintf(buff, "CREATE INDEX \"EVENTS__device_id_when1_idx_%d\" ON device_data.events_%d USING btree (device_id , when1 );", dev_id, dev_id);
        pga->exec(buff);
        pga->free();
        sprintf(buff, "CREATE INDEX \"EVENTS_time_stamp_idx_%d\" ON device_data.events_%d USING btree (time_stamp );", dev_id, dev_id);
        pga->exec(buff);
        pga->free();
        sprintf(buff, "CREATE INDEX \"EVENTS_when1_idx_%d\" ON device_data.events_%d USING btree (when1 );", dev_id, dev_id);
        pga->exec(buff);
        pga->free();
        sprintf(buff, "GRANT SELECT ON TABLE device_data.events_%d TO userviewer;'", dev_id);
        pga->exec(buff);
        pga->free();
    }
}

time_t Copy::getMaxTime() {
    if(tm_max == 0) {
        time_t now = time(NULL);
        struct tm *ts;
        ts = localtime(&now);

        ts->tm_sec  = 0;
        ts->tm_min  = 0;
        ts->tm_hour = 0;
        tm_max = mktime(ts);
    }
    return tm_max;
}

bool Copy::endOfTime() {
    return dtEnd > getMaxTime();
}

Event **Copy::skipDay(int *cnt) {
    dt = dtEnd;
    setTimeLimits();
    save(NULL);
    return readEvents(cnt);
}

Event **Copy::readEvents(int *cnt) {
    sprintf(buff, "SELECT * FROM device_data.events_%d "
                    "WHERE when1 > '%s' AND when1 <= '%s' "
                    "ORDER BY when1 ASC", dev_id, sBeg, sEnd);
    ExecStatusType r = pgf->exec(buff);
    if(r != ExecStatusType::PGRES_TUPLES_OK) {
        sprintf(buff, "%s : %s", "read events error", pgf->error());
        pgf->free();
        throw std::runtime_error(buff);
    }

    *cnt = pgf->count();
    if(*cnt == 0 && !endOfTime()) {
        pgf->free();
        return skipDay(cnt);
    }

    if(*cnt == 0) return m_events;

    m_events = (Event**)malloc(sizeof(Event*)*(*cnt));
    for(int r = 0; r < *cnt; r++) {
        m_events[r] = new Event(r);
    }
    pgf->free();
    return m_events;
}

Input **Copy::readInputs(int *cnt) {
    sprintf(buff, "SELECT relname FROM pg_catalog.pg_class "
                    "WHERE relname ~ 'inputs_%d_.*' AND reltype > 0 "
                    "ORDER BY relname", dev_id);
    ExecStatusType r = pgf->exec(buff);
    if(r != ExecStatusType::PGRES_TUPLES_OK) {
        sprintf(buff, "%s : %s", "read inputs error", pgf->error());
        throw std::runtime_error(buff);
    }

    *cnt = pgf->count();

    m_inputs = (Input**)malloc(sizeof(Input*)*(*cnt));
    for(int r = 0; r < *cnt; r++) {
        char *inp = pgf->value(r, 0);
        Input *pinp = new Input(inp);
        pinp->validate(buff);
        m_inputs[r] = pinp;
    }
    pgf->free();
    return m_inputs;
}

void Copy::save(Event *last) {
    char tm[20];
    if(last) {
        event_id = last->i;
        dt       = strtotime(last->w);
    }

    getTime(dt, tm);

    sprintf(buff, "SELECT COUNT(_id) "
                "FROM device_data.events_%d "
                "WHERE when1 > '%s'", dev_id, tm);
    pgf->exec(buff);
    int t = pgf->count() > 0 ? pgf->intval(0, 0) : 0;
    pgf->free();

    sprintf(buff, "INSERT INTO log.copy (dev_id, event_id, dt, cnt_left, dt_upd) "
                "VALUES (%d, %d, '%s', %d, NOW()) "
                "ON CONFLICT(dev_id) DO UPDATE SET "
                    "event_id = %d, "
                    "dt       = '%s', "
                    "cnt_left = %d, "
                    "dt_upd   = NOW();", dev_id, event_id, tm, t,
                    event_id, tm, t);
    ExecStatusType r = pga->exec(buff);
    if(r != ExecStatusType::PGRES_COMMAND_OK && r != ExecStatusType::PGRES_TUPLES_OK) {
        sprintf(buff, "save error %s", pga->error());
        throw std::runtime_error(buff);
    }
    pga->free();
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
    strftime(buf, 20, "%F %X", ts);
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