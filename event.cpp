#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "event.h"
#include "db.h"

Event::Event(int row) {
    Db* db = Db::fast();

    iNew = 0;
    memset(w, 0, 20);
    memset(t, 0, 20);
    char c[60]; // (34.8116112,48.4165192)
    // read
    i       = db->intval(row, 0); // _id
    strcpy(w, db->value(row, 1)); // when1
    /*** cut HOUR ***/
    strcpy(c, w);
    c[13] = 0;
    hour = atoi(c+11);
    /****************/
    d       = db->intval(row, 2); // device_id
    strcpy(t, db->value(row, 3)); // time_stamp
    p       = db->intval(row, 4); // priority
    alt     = db->intval(row, 5); // altitude
    s       = db->intval(row, 6); // speed
    n       = db->intval(row, 7); // nsat
    ang     = db->intval(row, 8); // angle
    dst     = db->intval(row, 9); // distance
    strcpy(c, db->value(row, 10)); // coord
    is_chk  = db->intval(row, 11); // is_checked

    // parse
    int jm = strlen(c);
    int j2 = 0;
    for(int j=0; j<jm; j++) {
        if(c[j] == ',') {
            c[j] = 0;
            j2 = j+1;
        }
        if(c[j] == ')') {
            c[j] = 0;
        }
    }
    x = atof(c+1);
    y = atof(c+j2);
    // POINT(%.7f, %.7f)
}

int Event::h(int h) {
    if(hour != h) printf("-%02d", hour);
    return hour;
}

int Event::save(char *buff) {
    Db* pga = Db::arch();
    sprintf(buff, "EXPLAIN INSERT INTO device_data.events_%d ("
                "when1,"
                "device_id,"
                "time_stamp,"
                "priority,"
                "altitude,"
                "speed,"
                "nsat,"
                "angle,"
                "distance,"
                "coord"
                ") VALUES ("
                "'%s', %d, '%s', %d, %d, %d, %d, %d, %d, POINT(%.7f, %.7f)"
                ") RETURNING _id", d,
                w, d, t, p, alt, s, n, ang, dst, x, y);
    pga->exec(buff);
    int c = pga->count();
    //if(c > 0) iNew = pga->intval(0, 0);
    pga->free();
    iNew = 1;
    return iNew;
}