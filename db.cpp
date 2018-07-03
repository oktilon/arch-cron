#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stddef.h>
#include <syslog.h>
#include <sys/types.h>
#include <unistd.h>
#include <libpq-fe.h>

#include "db.h"

Db* Db::m_arch = NULL;
Db* Db::m_fast = NULL;

Db::Db(const char* srv) {
    pq = PQsetdbLogin(srv,
                        "5432",
                        NULL,
                        NULL,
                        "postgres",
                        "gpsprivatagro",
                        "SE@Wf#vm!ERtr5");
}

bool Db::valid() {
    return PQstatus(pq) != CONNECTION_BAD;
};

PGresult* Db::exec(char* sql) {
    result = PQexec(pq, sql);
    return result;
}

int Db::rows() {
    return PQntuples(result);
}

char* Db::value(int row, int field) {
    return PQgetvalue(result, row, field);
}
int Db::intval(int row, int field) {
    return atoi(PQgetvalue(result, row, field));
}

void Db::free() {
    PQclear(result);
}

void Db::init() {
    m_arch = new Db("10.10.106.253");
    m_fast = new Db("10.10.254.2");
}

Db* Db::arch() { if(!m_arch) { init(); } return m_arch; }
Db* Db::fast() { if(!m_fast) { init(); } return m_fast; }