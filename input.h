#include <regex>
#include "event.h"

#ifndef CF_INPUT_H
#define CF_INPUT_H 1

class Input {
    public:
        int i;
        int t;
        char e[20];
        char txt[20];

        Input(char *s);
        void validate(char *b);
        void copyInput(Event *e, char *b);

        static std::regex regInp;
};

#endif