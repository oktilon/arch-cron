<?php
    require_once dirname(__FILE__) . '/cron.php';
    InfoPrefix(__FILE__);
    $time = time();
    $time_mark = $time;

    // colors
    $e  = "\033[0m";
    $g  = "\033[1;32m"; // green
    $y  = "\033[1;33m"; // yellow
    $cy = "\033[1;36m"; // cyan

    // ALTER SEQUENCE "EVENTS__ID_seq" RESTART WITH 1;
    // ALTER SEQUENCE "INPUTS_INPUT_ID_seq" RESTART WITH 1;

    function getTime() {
        global $time_mark, $cy, $e;
        $t = time();
        $r = $t - $time_mark;
        $time_mark = $t;
        return " {$cy}{$r}{$e} sec.";
    }

    $me  = $argv ? array_shift($argv) : __FILE__;

    $skip = false;
    $dev  = 0;


    while($argv) {
        $opt = array_shift($argv);
        switch($opt) {
            case '-s':
            case 'skip':
            case '--skip':
                $skip = true;
                break;

            case '-d':
            case '--device':
                $dev = $argv ? intval(array_shift($argv)) : 0;
                break;

            case '':
                break;

            default:
                echo "Usage {$g}$me{$e} [{$y}-s{$e}]\n" .
                     "  where : {$y}-s{$e}, {$y}--skip{$e}, {$y}skip{$e} - skip pg_dump/pg_restore public\n" .
                     "          {$y}-h{$e}, {$y}--help{$e}, {$y}help{$e} - show this help\n";
                die();
                break;
        }
    }

    if(!$skip) {
        // copy public
        $w = date('w');
        $out = "~/.dump/pub{$w}.tar";
        $usr = PG_USER;
        $src = FAST;
        Info('pg_dump ...');
        exec("/usr/pgsql-9.6/bin/pg_dump -h $src -p 5432 -U $usr -d postgres -F t -n public -f $out");
        Info('pg_dumped' . getTime());

        $dst = ARCH;
        Info('pg_restore ...');
        exec("/usr/pgsql-9.6/bin/pg_restore -h $dst -p 5432 -U $usr -d postgres --clean -F t -n public $out");
        Info('pg_restored' . getTime());
    }

    // Read devices
    Info('read devices...');
    $devices = $PGF->prepare("SELECT _id FROM devices ORDER BY _id")
                    ->execute_all();
    Info('readed' . getTime());

    Info('proceed devices:');
    foreach($devices as $row) {
        try {
            $id = intval($row['_id']);
            $inf = new Copy($id);

            echo PHP_EOL . "DEV $id ";

            if(!$inf->valid()) {
                Info(Copy::$error);
                continue;
            }

            // calculations
            $inf->doCalculations();
            Info('calculations' . getTime());

            $inf->verifyEventTable();

            // Inputs list
            $inputs = $inf->readInputs();
            Info('inputs list' . getTime());

            // Copy data
            $events = $inf->readEvents();
            Info('readed events' . getTime());
            $h = -1;
            foreach ($events as $ev) {
                $eold = intval($ev['_id']);
                $eid = $inf->insertEvent($ev);
                if($eid) {
                    $h = $inf->h($h);
                    foreach ($inputs as $inp) {
                        $inf->copyInput($eold, $eid, $inp);
                    }
                } else {
                    echo "0";
                }
            }
            $inf->save();
            echo PHP_EOL;
        } catch (Eception $ex) {
            $m = $ex->getMessage();
            Info("Exception : $m");
        }
        Info("FIN $id" . getTime());
    }
    echo PHP_EOL;
    Info("Finish within " . (time() - $time) . " sec.");