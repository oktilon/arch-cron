#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stddef.h>
#include <syslog.h>
#include <sys/types.h>
#include <unistd.h>
#include <libpq-fe.h>

#include "db.h"
#include "config.h"

Db* Db::m_arch = NULL;
Db* Db::m_fast = NULL;

Db::Db(const char* srv) {
    pq = PQsetdbLogin(srv,
                        "5432",
                        NULL,
                        NULL,
                        DB_NAME,
                        DB_USER,
                        DB_PASS);
}

bool Db::valid() {
    return PQstatus(pq) != CONNECTION_BAD;
};


// ExecStatusType::PGRES_COMMAND_OK
// ExecStatusType::PGRES_TUPLES_OK
// ExecStatusType::PGRES_SINGLE_TUPLE
// ExecStatusType::PGRES_EMPTY_QUERY
// ExecStatusType::PGRES_BAD_RESPONSE
// ExecStatusType::PGRES_NONFATAL_ERROR
// ExecStatusType::PGRES_FATAL_ERROR

ExecStatusType Db::exec(const char* sql) {
    result = PQexec(pq, sql);
    return resultStatus();
}

PGresult *Db::prepare(const char *stmtName, const char *query, int nParams, const Oid *paramTypes) {
    result = PQprepare(pq,
                    stmtName,
                    query,
                    nParams,
                    paramTypes);
    return result;
}

PGresult *Db::execPrepared(char *stmtName,
                    int nParams,
                    char * const *paramValues,
                    const int *paramLengths,
                    const int *paramFormats,
                    int resultFormat) {

    result = PQexecPrepared(pq,
                stmtName,
                nParams,
                paramValues,
                paramLengths,
                paramFormats,
                resultFormat);
    return result;
}

ExecStatusType Db::resultStatus() {
    return PQresultStatus(result);
}

char *Db::resultStatusText() {
    return PQresStatus(resultStatus());
}

char *Db::error() {
    return PQresultErrorMessage(result);
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
    m_arch = new Db(DB_ARCH);
    m_fast = new Db(DB_FAST);
}

Db* Db::arch() { if(!m_arch) { init(); } return m_arch; }
Db* Db::fast() { if(!m_fast) { init(); } return m_fast; }