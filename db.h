#include <libpq-fe.h>

class Db {
    public:
        PGconn *pq;
        PGresult *result;

        Db(const char* srv);
        bool valid();
        PGresult *exec(char* sql);
        PGresult *prepare(const char *stmtName, const char *query, int nParams, const Oid *paramTypes);
        PGresult *execPrepared(char *stmtName,
                                    int nParams,
                                    char * const *paramValues,
                                    const int *paramLengths,
                                    const int *paramFormats,
                                    int resultFormat);
        ExecStatusType resultStatus();
        char *resultStatusText();
        char *error();

        int rows();
        void free();
        char* value(int r, int c);
        int intval(int r, int c);

        static Db* m_arch;
        static Db* m_fast;
        static void init();
        static Db* arch();
        static Db* fast();
};