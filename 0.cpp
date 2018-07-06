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

    char ttt[] = "2018-07-05 09:00:00";
    time_t tti = Copy::strtotime(ttt);
    printf("%s = %ld\n\n", ttt, tti);

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
           strcmp(pa, "--device") == 0) {
            i++;
            if(i < argc) {
                dev = atoi(argv[i]);
            }
            continue;
        }
        printf("Usage %s%s%s [%s-s%s]\n"
               "  where : %s-s%s, %s--skip%s, %sskip%s - skip pg_dump/pg_restore public\n"
               "          %s-h%s, %s--help%s, %shelp%s - show this help\n",
            Copy::col_g, me, Copy::col_e, Copy::col_y, Copy::col_e,
            Copy::col_y, Copy::col_e, Copy::col_y, Copy::col_e, Copy::col_y, Copy::col_e,
            Copy::col_y, Copy::col_e, Copy::col_y, Copy::col_e, Copy::col_y, Copy::col_e);
        return 0;
    }

    const char *bs = skip ? Copy::col_g : Copy::col_r;
    const char *ts = skip ? "true" : "false";
    const char *bd = dev > 0 ? Copy::col_g : Copy::col_r;

    printf("Test Run. pid=%d, dev=%s%d%s, skip=%s%s%s\n", pid, bd, dev, Copy::col_e, bs, ts, Copy::col_e);

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
    Info("read devices...");
    devices = Copy::readDevices(pgf, &devices_count);

    sprintf(buff, "readed %d dev.%s", devices_count, getTime(&tm_mark, bufTm));
    Info(buff);

    Info("proceed devices:");
    for(int ix=0; ix < devices_count; ix++) {
        int id = devices[ix];
        printf("run dev %s%d%s\n", Copy::col_b, id, Copy::col_e);
        Copy inf = Copy(id, buff);
        printf("validate dev %s%d%s\n", Copy::col_b, id, Copy::col_e);
        if(!inf.valid()) continue;
    }

    sprintf(buff, "Finish%s", getTime(&tm_start, bufTm));
    Info(buff);
    return 0;
}


/*




    // Read devices
    Info('read devices...');
    $devices = $PGF->prepare("SELECT _id FROM devices ORDER BY _id")
                    ->execute_all();
    Info('readed ' . (count($devices)) . ' dev. ' . getTime());

    Info('proceed devices:');
    foreach($devices as $row) {
        try {
            $id = intval($row['_id']);
            $inf = new Copy($id);

            echo PHP_EOL . "DEV $id ";

            if(!$inf->valid()) {
                Info(Copy::$error);
                continue;
            }

            // calculations
            $inf->doCalculations();
            Info("DEV $id calculations" . getTime());

            $inf->verifyEventTable();

            // Inputs list
            $inputs = $inf->readInputs();
            Info("DEV $id inputs list" . getTime());

            // Copy data
            $events = $inf->readEvents();
            Info("DEV $id readed events" . getTime());
            $h = -1;
            foreach ($events as $ev) {
                $eold = intval($ev['_id']);
                $eid = $inf->insertEvent($ev);
                if($eid) {
                    $h = $inf->h($h);
                    foreach ($inputs as $inp) {
                        $inf->copyInput($eold, $eid, $inp);
                    }
                } else {
                    echo "0";
                }
            }
            $inf->save();
            echo PHP_EOL;
        } catch (Eception $ex) {
            $m = $ex->getMessage();
            Info("DEV $id Exception : $m");
        }
        Info("FIN $id" . getTime());
    }
    echo PHP_EOL;
    Info("Finish within " . (time() - $time) . " sec.");

    Copy::pidUnLock();
    */