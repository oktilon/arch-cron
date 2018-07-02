<?php
    require_once dirname(__FILE__) . '/cron.php';
    InfoPrefix(__FILE__);

    function verifyEventTable($dev_id) {
        global $PGA;

        $has = $PGA->prepare("SELECT COUNT(relname) FROM pg_catalog.pg_class
                                WHERE relname = :i AND reltype > 0")
                    ->bind('i', "events_{$dev_id}")
                    ->execute_scalar();
        return $has > 0;
    }

    function verifyInputTable($inp) {
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

    $beg = new DateTime();
    $beg->modify("-1 DAY");

    $sBeg = $beg->formaT('Y-m-d') . " 00:00:00";
    $sEnd = $beg->formaT('Y-m-d') . " 23:59:59";

    // copy public
    $w = date('w');
    $out = "~/.dump/pub{$w}.tar";
    $src = FAST;
    Info('pg_dump');
    exec("/usr/pgsql-9.6/bin/pg_dump -h $src -p 5432 -U gpsprivatagro -d postgres -F t -n public -f $out");

    $dst = ARCH;
    Info('pg_restore');
    exec("/usr/pgsql-9.6/bin/pg_restore -h $dst -p 5432 -U gpsprivatagro -d postgres --clean -F t -n public $out");

    die("Stop\n");

    // Read devices
    $devices = $PGF->prepare("SELECT _id FROM devices ORDER BY _id")
                    ->execute_all();

    foreach($devices as $row) {
        $id = intval($row['_id']);
        echo PHP_EOL . "DEV $id ";

        // calculations
        // 1 - runStop
        $PGF->prepare("SELECT * FROM calcRunStop(:i, :b, :e)")
            ->bind('i', $id)
            ->bind('b', $sBeg)
            ->bind('e', $sEnd)
            ->execute();

        if(!verifyEventTable($id)) {
            echo "No event table";
            continue;
        }

        // Inputs list
        $inputs = [];
        $lst = $PGF->prepare("SELECT relname FROM pg_catalog.pg_class
                                WHERE relname ~ :i AND reltype > 0")
                    ->bind('i', "inputs_{$id}_.*")
                    ->execute_all();
        foreach($lst as $r) {
            $inp = $r['relname'];
            $inputs[] = $inp;
            verifyInputTable($inp);
        }

        // Copy data
        $events = $PGF->select("SELECT * FROM device_data.events_{$id}");
        foreach ($events as $ev) {
            $eold = intval($ev['_id']);
            $q = $PGA->prepare("INSERT INTO device_data.events_{$id}
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
            if($q) {
                echo "+";
                $eid = intval($q);
                foreach ($inputs as $inp) {
                    $i_fast = $PGF->prepare("SELECT * FROM device_data.{$inp} WHERE event_id = :i")
                            ->bind('i', $eold)
                            ->execute_row();
                    $w = $PGA->prepare("INSERT INTO device_data.{$inp}
                                        (event_id,
                                         input_val,
                                         input_type,
                                         device_id)
                                    VALUES
                                    (:e, :v, :t, :d)")
                            ->bind('e', $eid)
                            ->bind('v', intval($i_fast['input_val']))
                            ->bind('t', intval($i_fast['input_type']))
                            ->bind('d', intval($i_fast['device_id']))
                            ->execute();
                    echo $w ? '.' : 'x';
                }
            } else {
                echo "-";
            }
        }
    }

    echo "Found " . count($devices) . " devices.\n";
    echo "Error " . $PGF->error . "\n";
