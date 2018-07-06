#include <libpq-fe.h>

#ifndef CF_DB_H
#define CF_DB_H 1

class Db {
    public:
        PGconn *pq;
        PGresult *result;

        Db(const char* srv);
        bool valid();
        char *error();
        char *res_error();
        int count();
        ExecStatusType exec(const char* sql);
        ExecStatusType resultStatus();
        char *resultStatusText();
        void free();
        char *value(int r, int c);
        int intval(int r, int c);

        PGresult *prepare(const char *stmtName, const char *query, int nParams, const Oid *paramTypes);
        PGresult *execPrepared(char *stmtName,
                                    int nParams,
                                    char * const *paramValues,
                                    const int *paramLengths,
                                    const int *paramFormats,
                                    int resultFormat);

        static Db* m_arch;
        static Db* m_fast;
        static void init();
        static Db* arch();
        static Db* fast();
};

#endif