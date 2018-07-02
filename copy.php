<?php
    require_once dirname(__FILE__) . '/cron.php';

    function insertEvents($id, $val, $lst) {
        global $PGA;

        $has = $PGA->prepare("SELECT COUNT(relname) FROM pg_catalog.pg_class
                                WHERE relname = :i AND reltype > 0")
                    ->bind('i', "events_{$id}")
                    ->execute_scalar();
        if(!$has) {
            echo "NO EVENTS TABLE FOR $id";
            return;
        }


        $val = implode(',', $val);
        $PGA->prepare("INSERT INTO device_data.events_{$id}
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
                    VALUES $val");
        foreach($lst as $ix => $bnd) {
            foreach($bnd as $k => $v) {
                $PGA->bind("$k{$ix}", $v);
            }
        }
        return $PGA->execute();
    }

    $beg = new DateTime();
    $beg->modify("-1 DAY");

    $sBeg = $beg->formaT('Y-m-d') . " 00:00:00";
    $sEnd = $beg->formaT('Y-m-d') . " 23:59:59";

    // copy public
    $w = date('w');
    $out = "~/.dump/pub{$w}.tar";
    $src = FAST;
    exec("pg_dump -h $src -p 5432 -d postgres -F t -n public -f $out");

    $dst = ARCH;
    exec("pg_restore -h $dst -p 5432 -d postgres -F t -n public $out");

    die("Stop\n");

    // Read devices
    $devices = $PGF->prepare("SELECT _id FROM devices ORDER BY _id")
                    ->execute_all();

    foreach($devices as $row) {
        $id = intval($row['_id']);

        // calculations
        // 1 - runStop
        $PGF->prepare("SELECT * FROM calcRunStop(:i, :b, :e)")
            ->bind('i', $id)
            ->bind('b', $sBeg)
            ->bind('e', $sEnd)
            ->execute();

        // Copy events
        $lst = [];
        $val = [];
        $events = $PGF->select("SELECT * FROM device_data.events_{$id}");
        foreach ($events as $ix=>$ev) {
            $lst[$ix] = [
                "w" => $ev['when1'],
                "t" => $ev['time_stamp'],
                "o" => $ev['coord'],
                "d" => intval($ev['device_id']),
                "p" => intval($ev['priority']),
                "a" => intval($ev['altitude']),
                "s" => intval($ev['speed']),
                "n" => intval($ev['nsat']),
                "g" => intval($ev['angle']),
                "i" => intval($ev['distance']),
                "h" => intval($ev['is_checked']),
            ];
            $val[] = "(:w$ix, :d$ix, :t$ix, :p$ix, :a$ix, :s$ix, :n$ix, :g$ix, :i$ix, :o$ix, :h$ix)";
        }

        $q = insertEvents($id, $val, $lst);


        // Inputs list
        $inputs = $PGF->prepare("SELECT relname FROM pg_catalog.pg_class
                                WHERE relname ~ :i AND reltype > 0")
                    ->bind('i', "inputs_{$id}_.*")
                    ->execute_all();
    }

    echo "Found " . count($devices) . " devices.\n";
    echo "Error " . $PGF->error . "\n";
