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

        Copy(int id);
        void print();
};