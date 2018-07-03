<?php
class Copy {
    public $dev_id   = 0;
    public $event_id = 0;
    public $dt       = null;

    private $sBeg = '';
    private $sEnd = '';
    private $ev   = null;

    public static $error = '';

    function  __construct($id) {
        global $PGA, $PGF;

        $dt  = date('Y-m-d H:i:s');
        $inf = $PGA->prepare("SELECT * FROM log.copy WHERE dev_id = :d")
                    ->bind('d', $id)
                    ->execute_row();
        if(!$inf) {
            $old = $PGF->prepare("SELECT _id, when1 FROM device_data.events_{$id}
                                ORDER BY when1 ASC LIMIT 1")
                    ->execute_row();
            if(!$old) {
                self::$error = "init error {$PGF->error}";
                return;
            }
            $dt = $old['when1'];
            $this->dev_id   = $id;
            $this->event_id = intval($old['_id']);
            $this->dt       = new DateTime($dt);
            $this->dt->modify('- 1 second');
        } else {
            $dt = $inf['dt'];
            $this->dev_id   = intval($inf['dev_id']);
            $this->event_id = intval($inf['event_id']);
            $this->dt       = new DateTime($dt);
        }

        $beg = new DateTime($dt);
        $end = new DateTime($dt);
        $end->modify("+ 24 hours");

        $this->sBeg = $beg->format('Y-m-d H:i:s');
        $this->sEnd = $end->format('Y-m-d H:i:s');

        $cnt = $PGF->prepare("SELECT COUNT(_id) FROM device_data.events_{$id}
                                WHERE when1 > :b")
                    ->bind('b', $this->sBeg)
                    ->execute_scalar();
        if(!$cnt){
            self::$error = 'no new data';
            $this->dev_id = 0;
        }
    }

    public function valid() { return $this->dev_id > 0; }

    public function save() {
        global $PGA;

        if($this->ev) {
            $this->event_id = intval($this->ev['_id']);
            $this->dt = new DateTime($this->ev['when1']);
        }

        $q = $PGA->prepare("INSERT INTO log.copy (dev_id, event_id, dt)
                            VALUES (:i, :e, :d)
                            ON CONFLICT(dev_id) DO UPDATE SET
                                event_id = :e,
                                dt = :d")
                ->bind('i', $this->dev_id)
                ->bind('e', $this->event_id)
                ->bind('d', $this->dt->format('Y-m-d H:i:s'))
                ->execute();
        if(!$q) throw new Exception("save error {$PGA->error}");
        return true;
    }

    public function b() { return $this->sBeg; }
    public function e() { return $this->sEnd; }

    public function verifyEventTable() {
        global $PGA;

        $dev_id = $this->dev_id;

        $has = $PGA->prepare("SELECT COUNT(relname) FROM pg_catalog.pg_class
                                WHERE relname = :i AND reltype > 0")
                    ->bind('i', "events_{$dev_id}")
                    ->execute_scalar();
        if(!$has) {
            $PGA->prepare("CREATE TABLE IF NOT EXISTS device_data.events_{$dev_id} ( CHECK (device_id = {$dev_id} ), PRIMARY KEY (_id),
                    CONSTRAINT \"OUT_KEY_{$dev_id}\"
                        FOREIGN KEY (device_id) REFERENCES devices (_id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE CASCADE DEFERRABLE INITIALLY IMMEDIATE
                    ) INHERITS (device_data.events);")->execute();

            $PGA->prepare("GRANT SELECT ON TABLE device_data.events_{$dev_id} TO userviewer;'")->execute();
            $PGA->prepare("CREATE INDEX \"EVENTS__ID_idx_{$dev_id}\" ON device_data.events_{$dev_id} USING btree (_id );")->execute();
            $PGA->prepare("CREATE INDEX \"EVENTS__device_id_idx_{$dev_id}\" ON device_data.events_{$dev_id} USING btree (device_id );")->execute();
            $PGA->prepare("CREATE INDEX \"EVENTS__device_id_when1_idx_{$dev_id}\" ON device_data.events_{$dev_id} USING btree (device_id , when1 );")->execute();
            $PGA->prepare("CREATE INDEX \"EVENTS_time_stamp_idx_{$dev_id}\" ON device_data.events_{$dev_id} USING btree (time_stamp );")->execute();
            $PGA->prepare("CREATE INDEX \"EVENTS_when1_idx_{$dev_id}\" ON device_data.events_{$dev_id} USING btree (when1 );")->execute();
            $PGA->prepare("GRANT SELECT ON TABLE device_data.events_{$dev_id} TO userviewer;'")->execute();
        }
    }

    public function verifyInputTable($inp) {
        global $PGA;

        $has = $PGA->prepare("SELECT COUNT(relname) FROM pg_catalog.pg_class
                                WHERE relname = :i AND reltype > 0")
                    ->bind('i', $inp)
                    ->execute_scalar();
        if(!$has) {
            echo "NO INPUTS TABLE $inp";
            return;
        }
    }

    public function doCalculations() {
        global $PGF;

        // 1 - runStop
        $PGF->prepare("SELECT * FROM calcRunStop(:i, :b, :e)")
            ->bind('i', $this->dev_id)
            ->bind('b', $this->sBeg)
            ->bind('e', $this->sEnd)
            ->execute();
    }

    public function readInputs() {
        global $PGF;

        $ret = [];
        $lst = $PGF->prepare("SELECT relname FROM pg_catalog.pg_class
                                WHERE relname ~ :i AND reltype > 0")
                    ->bind('i', "inputs_{$this->dev_id}_.*")
                    ->execute_all();
        if($lst === FALSE) {
            throw new Exception("read inputs error {$PGF->error}");
        }
        foreach($lst as $r) {
            $inp = $r['relname'];
            $ret[] = $inp;
            $this->verifyInputTable($inp);
        }
        return $ret;
    }

    public function readEvents() {
        global $PGF;

        $ret = $PGF->prepare("SELECT * FROM device_data.events_{$this->dev_id}
                            WHERE when1 > :b AND when1 <= :e
                            ORDER BY when1 ASC")
                ->bind('b', $this->sBeg)
                ->bind('e', $this->sEnd)
                ->execute_all();

        if($ret === FALSE) {
            throw new Exception("read events error {$PGF->error}");
        }
        return $ret;
    }

    public function insertEvent($ev) {
        global $PGA;
        $this->ev = $ev;
        $q = $PGA->prepare("INSERT INTO device_data.events_{$this->dev_id}
                        (when1,
                         device_id,
                         time_stamp,
                         priority,
                         altitude,
                         speed,
                         nsat,
                         angle,
                         distance,
                         coord,
                         is_checked)
                    VALUES (:w, :d, :t, :p, :a, :s, :n, :g, :i, :o, :h)
                    RETURNING _id")
                ->bind('w', $ev['when1'])
                ->bind('t', $ev['time_stamp'])
                ->bind('o', $ev['coord'])
                ->bind('d', intval($ev['device_id']))
                ->bind('p', intval($ev['priority']))
                ->bind('a', intval($ev['altitude']))
                ->bind('s', intval($ev['speed']))
                ->bind('n', intval($ev['nsat']))
                ->bind('g', intval($ev['angle']))
                ->bind('i', intval($ev['distance']))
                ->bind('h', intval($ev['is_checked']))
                ->execute_scalar();
        return intval($q);
    }

    public function copyInput($eold, $eid, $inp) {
        global $PGA, $PGF;

        $row = $PGF->prepare("SELECT * FROM device_data.{$inp} WHERE event_id = :i")
                ->bind('i', $eold)
                ->execute_row();
        if($row) {
            $w = $PGA->prepare("INSERT INTO device_data.{$inp}
                                (event_id,
                                 input_val,
                                 input_type,
                                 device_id)
                            VALUES
                            (:e, :v, :t, :d)")
                    ->bind('e', $eid)
                    ->bind('v', intval($row['input_val']))
                    ->bind('t', intval($row['input_type']))
                    ->bind('d', intval($row['device_id']))
                    ->execute();
            if(!$w) echo $PGA->error;
        }
    }
}