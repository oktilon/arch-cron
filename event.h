#ifndef CF_EVENT_H
#define CF_EVENT_H 1

class Event {
    public:
        int i;
        float x;
        float y;
        int d;
        int p;
        int alt;
        int s;
        int n;
        int ang;
        int dst;
        int is_chk;
        char w[20];
        char t[20];

        int iNew;
        int hour;

        Event(int row);
        int save(char *b);
        int h(int h);
};

#endif