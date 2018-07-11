#include <string.h>

#include "input.h"
#include "db.h"

std::regex Input::regInp("^inputs\\_(\\d+)\\_(\\d+)(.*)$");

Input::Input(char* inp) {
    i = 0;
    t = 0;
    strcpy(e, "");
    strcpy(txt, inp);
    std::cmatch cm;
    if(std::regex_match(inp, cm, regInp)) {
        if(cm.size() > 2) {
            i = atoi(cm[1].str().c_str());
            t = atoi(cm[2].str().c_str());
        }
        if(cm.size() > 3) {
            sprintf(e, "%s", cm[3].str().c_str());
        }
    }
    //printf("inp_%d_%d%s\n", i, t, e);
}

void Input::validate(char* buff) {
    Db* pga = Db::arch();
    printf("Check arch for %s table ", txt);
    sprintf(buff, "SELECT COUNT(relname) FROM pg_catalog.pg_class "
                    "WHERE relname = '%s' AND reltype > 0", txt);
    pga->exec(buff);
    int ii = pga->count();
    printf("%d rows ", ii);
    int has = ii > 0 ? pga->intval(0, 0) : 0;
    pga->free();
    printf("has = %d\n", has);
    if(has == 0) {
        char dt[50] = "";
        if(strcmp(e, "") == 0) {
            sprintf(dt, "%d_%d", i, t);
            printf("Create %s dt=%s\n", txt, dt);
            sprintf(buff, "CREATE TABLE IF NOT EXISTS device_data.%s "
                    "( CHECK (device_id = %d AND input_type= %d ), "
                    "PRIMARY KEY (input_id), "
                    "CONSTRAINT \"ext_device_id_%s\" "
                        "FOREIGN KEY (device_id) REFERENCES devices (_id) "
                        "MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION,"
                    "CONSTRAINT \"ext_events_id_%s\" "
                        "FOREIGN KEY (event_id) REFERENCES device_data.events_%d (_id) "
                        "MATCH SIMPLE ON UPDATE NO ACTION ON DELETE CASCADE, "
                    "CONSTRAINT \"uk_%s\" "
                        "UNIQUE (event_id, device_id, input_type)) "
                    "INHERITS (device_data.inputs);", txt, i, t, dt, dt, i, txt);
            printf("SQL\n%s\n", buff);
            pga->exec(buff); pga->free();

            sprintf(buff, "GRANT SELECT ON TABLE device_data.%s TO userviewer;", txt);
            pga->exec(buff); pga->free();

            sprintf(buff, "CREATE INDEX \"INPUTS_INPUT_ID_idx_%s\" "
                        "ON device_data.%s USING btree (input_id );", dt, txt);
            pga->exec(buff); pga->free();

            sprintf(buff, "CREATE INDEX \"INPUTS_event_id_idx_%s\" "
                        "ON device_data.%s USING btree (event_id );", dt, txt);
            pga->exec(buff); pga->free();

            sprintf(buff, "CREATE INDEX \"INPUTS_type_idx_%s\" "
                        "ON device_data.%s USING btree (input_type );", dt, txt);
            pga->exec(buff); pga->free();

            sprintf(buff, "CREATE INDEX fki_ext_device_id_%s "
                        "ON device_data.%s USING btree (device_id );", dt, txt);
            pga->exec(buff); pga->free();

            sprintf(buff, "CREATE INDEX fki_ext_events_id_%s "
                        "ON device_data.%s USING btree (event_id );", dt, txt);
            pga->exec(buff); pga->free();
        } else {
            sprintf(dt, "%d_%d%s", i, t, e);
            printf("Create %s dt=%s\n", txt, dt);
            sprintf(buff, "CREATE TABLE IF NOT EXISTS device_data.%s "
                "(  CHECK (device_id = %d AND input_type= %d ), "
                    "PRIMARY KEY (input_id), "
                    "CONSTRAINT \"ext_device_id_%s\" "
                        "FOREIGN KEY (device_id) "
                        "REFERENCES devices (_id) "
                        "MATCH SIMPLE ON UPDATE NO ACTION ON DELETE CASCADE, "
                    "CONSTRAINT \"ext_events_id_%s\" "
                        "FOREIGN KEY (event_id) "
                        "REFERENCES device_data.events_%d (_id) "
                        "MATCH SIMPLE ON UPDATE NO ACTION ON DELETE CASCADE, "
                    "CONSTRAINT \"uk_%s\" "
                        "UNIQUE (event_id, device_id, input_type) "
                ") "
                "INHERITS (device_data.inputs );", txt, i, t, dt, dt, i, txt);
            printf("SQL\n%s\n", buff);
            pga->exec(buff); pga->free();

            sprintf(buff, "GRANT SELECT ON TABLE device_data.%s TO userviewer;", txt);
            pga->exec(buff); pga->free();

            sprintf(buff, "CREATE INDEX \"INPUTS_INPUT_ID_idx_%s\" "
                        "ON device_data.%s USING btree (input_id );", dt, txt);
            pga->exec(buff); pga->free();

            sprintf(buff, "CREATE INDEX \"INPUTS_event_id_idx_%s\" "
                        "ON device_data.%s USING btree (event_id );", dt, txt);
            pga->exec(buff); pga->free();

            sprintf(buff, "CREATE INDEX \"INPUTS_type_idx_%s\" "
                        "ON device_data.%s USING btree (input_type );", dt, txt);
            pga->exec(buff); pga->free();

            sprintf(buff, "CREATE INDEX fki_ext_device_id_%s "
                        "ON device_data.%s USING btree (device_id );", dt, txt);
            pga->exec(buff); pga->free();

            sprintf(buff, "CREATE INDEX fki_ext_events_id_%s "
                        "ON device_data.%s USING btree (event_id );", dt, txt);
            pga->exec(buff); pga->free();

        }
    }
}

void Input::copyInput(Event *e, char *buff) {
    Db* pga = Db::arch();
    Db* pgf = Db::fast();

    sprintf(buff, "SELECT input_val, input_type, device_id "
                "FROM device_data.%s WHERE event_id = %d", txt, e->i);
    pgf->exec(buff);
    int c = pgf->count();

    if(c > 0) {
        sprintf(buff, "INSERT INTO device_data.%s ("
                            "event_id,"
                            "input_val,"
                            "input_type,"
                            "device_id"
                        ") VALUES "
                        "(%d, %d, %d, %d)",
                        txt, e->iNew,
                        pgf->intval(0, 0),
                        pgf->intval(0, 1),
                        pgf->intval(0, 2));
        ExecStatusType r = pga->exec(buff);
        if(r != ExecStatusType::PGRES_COMMAND_OK && r != ExecStatusType::PGRES_TUPLES_OK) {
            printf("%s\n",pga->error());
        }
        pga->free();
    }
    pgf->free();
}