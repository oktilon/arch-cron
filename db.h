#include <libpq-fe.h>

class Db {
    public:
        PGconn *pq;
        PGresult *result;

        Db(const char* srv);
        bool valid();
        PGresult* exec(char* sql);
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