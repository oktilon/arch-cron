#include "db.h"

class Copy {
    public:
        int dev_id;
        int event_id;

        time_t dt;
        time_t dtEnd;

        char *sBeg;
        char *sEnd;
        char *ev_dt;
        int   ev_id;

        char *buff;
        char *error;
        Db *pga;
        Db *pgf;

        static const char* col_e;
        static const char* col_gr;
        static const char* col_r;
        static const char* col_g;
        static const char* col_y;
        static const char* col_b;
        static const char* col_m;
        static const char* col_cy;
        static const char* col_w;
        static const char* col_dr;
        static const char* col_dg;
        static const char* col_dy;
        static const char* col_db;
        static const char* col_dm;
        static const char* col_dc;
        static const char* col_dw;

        Copy(int id, char* b);
        ~Copy();
        bool valid();
        void setTimeLimits();

        static int *readDevices(Db *pgf, int *cnt);
        static char *pLockFile;
        static bool pidLock(const char *fName);
        static void pidUnLock();
        static char *getTime(time_t t, char *buf);
        static time_t strtotime(const char *s);
};