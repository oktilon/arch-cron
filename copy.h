class Copy {
    public:
        int dev_id;
        int event_id;
        int dt;

        int dtEnd;

        char* sBeg;
        char* sEnd;
        char* ev_dt;
        int   ev_id;

        static const char* col_e;
        static const char* col_g;
        static const char* col_y;
        static const char* col_cy;

        Copy(int id);
        void print();
};