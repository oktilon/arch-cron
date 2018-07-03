#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <stdio.h>
#include <stddef.h>
#include <netdb.h>
#include <errno.h>
#include <unistd.h>
#include <syslog.h>
#include <libpq-fe.h>
#include <time.h>
#include <math.h>
#include <semaphore.h>

#define DEBUG 1
#define DB_USERS 12
#define SCRIPT_VER "11.9"

/* global variables and constants */

volatile sig_atomic_t   gGracefulShutdown=0;
volatile sig_atomic_t   gCaughtHupSignal=0;
int                         gLockFileDesc=-1;
int                         gMasterSocket=-1;
int                         conn = 0;
const char *const           gLockFilePath = "/var/run/teltond.pid";

/* prototypes */
typedef struct {
    unsigned long long int timeStamp;
    unsigned char priority, satellites;
    unsigned int longitude, latitude;
    unsigned short altitude, angle, speed;
    unsigned char idPres[256];
    long idVal[256];
} AvlData;

void error(const char *msg)
{
    perror(msg);
    exit(0);
}

void debug(char *msg) {
    if(DEBUG > 0) {
        syslog(LOG_CRIT, msg);
    }
}

void CloseSocket(int sock, int conn, PGconn *pqSql, sem_t *db_sem, int id)
{
    int clres;
    char sql[100] = "UPDATE public.devices SET pid = 0 WHERE _id=000000000\0";

    if(db_sem != NULL && id > 0) {
        sprintf(sql, "UPDATE public.devices SET pid = 0 WHERE _id=%d", id);
        sem_wait(db_sem);
        PQexec(pqSql, sql);
        sem_post(db_sem);
    }

    clres = close(conn);    //shutdown(conn, SHUT_RDWR);
    if(clres != 0)
        syslog(LOG_CRIT,"Failed to CloseSocket");

    exit(0);
}

void closeAll(int sig)
{
    if(conn > 0)
    {
        int clres;
                        //syslog(LOG_INFO,"closeAll!! %d", conn);
        clres = close(conn);    //shutdown(conn, SHUT_RDWR);
        if(clres != 0)
            syslog(LOG_CRIT,"Failed to CloseSocket");
    }

    exit(0);
}

void AddLog(sem_t *db_sem,
            PGconn *pqSql,
            long pid,
            unsigned char imei[],
            long id, char message[])
{
    unsigned char buff[2048];

    typedef long long time64_t;
    time64_t mktime64 (struct tm *t);
    struct tm *localtime64_r (const time64_t *t, struct tm *p);
    time_t now;
    struct tm *ts;
    char tmNow[50];

    now = time(NULL);
    ts = localtime(&now);
    strftime(tmNow, sizeof(tmNow), "%Y-%m-%d %X", ts);
    // tmNow можно убрать - вместо поставить в таблице
    // DEFAULT = current_timestamp
    if(DEBUG > 0) {
        memset(buff, 0, 2048);
        sprintf(buff,
                "LOG %s-%d : %s",
                imei,
                id,
                message);
        syslog(LOG_INFO, buff);
    }
    memset(buff, 0, 2048);
    sprintf(buff,
            "INSERT INTO "
            "gps_log (time_stamp, pid, imei, _id, message) "
            "VALUES ('%s', %d, '%s', %d, '%s');",
            tmNow,
            getpid(),
            imei,
            id,
            message);
    sem_wait(db_sem);
    PQexec(pqSql, buff);
    sem_post(db_sem);
}

float path_distance_func(   float bp_latitude,
                            float bp_longitude,
                            float ep_latitude,
                            float ep_longitude)
{
    const float pi_del_180=0.0000000017453293;// pi/(180*10000000)
    float L =
        (
            acos(
                sin(bp_latitude*pi_del_180)*sin(ep_latitude*pi_del_180)+
                cos(bp_latitude*pi_del_180)*cos(ep_latitude*pi_del_180)*
                cos((ep_longitude-bp_longitude)*pi_del_180)
            )*1852.0*10800.0
        )/M_PI;
    return(L);
}

int BecomeDaemonProcess(const char *const lockFileName,
                                const char *const logPrefix,
                                const int logLevel,
                                int *const lockFileDesc,
                                pid_t *const thisPID)
{
    int                 curPID,stdioFD,lockResult,killResult,lockFD,i,
                        numFiles;
    char                pidBuf[17],*lfs,pidStr[7];
    FILE                *lfp;
    unsigned long       lockPID;
    struct flock        exclusiveLock;

    /* Set our current working directory to root to avoid tying up any
     * directories. In a real server, we might later change to another
     * directory and call chroot() for security purposes
     * (especially if we are writing something that serves files */

    chdir("/");

    /* try to grab the lock file */

    lockFD=open(lockFileName,O_RDWR|O_CREAT|O_EXCL,0644);

    if(lockFD==-1)
        {
        /* Perhaps the lock file already exists. Try to open it */

        lfp=fopen(lockFileName,"r");

        if(lfp==0) /* Game over. Bail out */
            {
            perror("Can't get lockfile");
            return -1;
            }

        /* We opened the lockfile.
         * Our lockfiles store the daemon PID in them.
         * Find out what that PID is */

        lfs=fgets(pidBuf,16,lfp);

        if(lfs!=0)
        {
            if(pidBuf[strlen(pidBuf)-1]=='\n') /* strip linefeed */
                pidBuf[strlen(pidBuf)-1]=0;

            lockPID=strtoul(pidBuf,(char**)0,10);

            /* See if that process is running.
             * Signal 0 in kill(2) doesn't send a signal, but still
             * performs error checking */

            killResult=kill(lockPID,0);

            if(killResult==0)
            {
                printf(
                    "\n\nERROR\n\nA lock file %s has been detected. "
                    "It appears it is owned\nby the (active) process "
                    "with PID %d.\n\n",
                    lockFileName,
                    lockPID);
            }
            else
            {
                if(errno==ESRCH) /* non-existent process */
                {
                    printf(
                        "\n\nERROR\n\nA lock file %s has been "
                        "detected. It appears it is owned\nby the "
                        "process with PID %d, which is now defunct. "
                        "Delete the lock file\nand try again.\n\n",
                        lockFileName,
                        lockPID);
                }
                else
                {
                    perror(
                        "Could not acquire exclusive "
                        "lock on lock file");
                }
            }
        }
        else
            perror("Could not read lock file");

        fclose(lfp);

        return -1;
        }

    /* we have got this far so we have acquired access to the lock file.
        Set a lock on it */

    exclusiveLock.l_type=F_WRLCK; /* exclusive write lock */
    exclusiveLock.l_whence=SEEK_SET; /* use start and len */
    exclusiveLock.l_len=exclusiveLock.l_start=0; /* whole file */
    exclusiveLock.l_pid=0; /* don't care about this */
    lockResult=fcntl(lockFD,F_SETLK,&exclusiveLock);

    if(lockResult<0) /* can't get a lock */
        {
        close(lockFD);
        perror("Can't get lockfile");
        return -1;
        }

    /* now we move ourselves into the background and become a daemon.
     Remember that fork() inherits open file descriptors among others so
     our lock file is still valid */

    curPID=fork();

    switch(curPID)
        {
        case 0: /* we are the child process */
          break;

        case -1: /* error - bail out (fork failing is very bad) */
          fprintf(stderr,"Error: initial fork failed: %s\n",
                     strerror(errno));
          return -1;
          break;

        default: /* we are the parent, so exit */
          exit(0);
          break;
        }

    /* make the process a session and process group leader.
     * This simplifies job control if we are spawning child servers,
     * and starts work on detaching us from a controlling TTY */

    if(setsid()<0)
        return -1;

    /* Note by A.B.: we skipped another fork here */

    /* log PID to lock file */

    /* truncate just in case file already existed */

    if(ftruncate(lockFD,0)<0)
        return -1;

    /* store our PID. Then we can kill thedaemon with
        kill `cat <lockfile>` where <lockfile> is the path to our
        lockfile */

    sprintf(pidStr,"%d\n",(int)getpid());

    write(lockFD,pidStr,strlen(pidStr));

    *lockFileDesc=lockFD; /* return lock file descriptor to caller */

    /* close open file descriptors */
    /* Note by A.B.: sysconf(_SC_OPEN_MAX) does work under Linux.
       No need in ad hoc guessing */

    numFiles = sysconf(_SC_OPEN_MAX); /* how many file descriptors? */


    for(i=numFiles-1;i>=0;--i) /* close all open files except lock */
        {
        if(i!=lockFD) /* don't close the lock file! */
            close(i);
        }

    /* stdin/out/err to /dev/null */

    umask(0); /* set this to whatever is appropriate for you */

    stdioFD=open("/dev/null",O_RDWR); /* fd 0 = stdin */
    dup(stdioFD); /* fd 1 = stdout */
    dup(stdioFD); /* fd 2 = stderr */

    /* open the system log - here we are using the LOCAL0 facility */

    openlog(
        logPrefix,
        LOG_PID|LOG_CONS|LOG_NDELAY|LOG_NOWAIT,
        LOG_LOCAL0);

    (void)setlogmask(LOG_UPTO(logLevel)); /* set logging level */

    /* put server into its own process group. If this process now spawns
        child processes, a signal sent to the parent will be propagated
        to the children */

    setpgrp();

    return 0;
}

int main(int argc, char *argv[])
{
    int res, cdb;
    pid_t daemonPID;

    struct sockaddr_in serv_addr, cli_addr;
    int sock = 0;
    int addrLength;
    int iResult, kolWait, statConn;
    unsigned char buff[4096], IMEI[20], codecID, numAvlData, numIO;
    unsigned int id;
    short kolIMEI;
    AvlData elemAvl;
    time_t now;
    struct tm *ts;
    char tmNow[128];
    char tmSend[128];
    char logbuff[4096];

    char table_exists[256];

    int oldVolt = 1, newVolt=0;
    long int max_id;

    PGconn *pqSql, *pqSql_u[DB_USERS];
    PGresult *result;

    int f;
    pid_t child, old_pid;

    int i, iInpFld;
    char AlreadyHaveThis;
    float distance, mileage;
    float latold, lonold;
    float actLat, actLng;
    sem_t *db_sem, *db_sem_u[DB_USERS];;
    int clres;

    struct timespec sleeptime;
    sleeptime.tv_sec = 0;
    sleeptime.tv_nsec = 10000000; //10ms

    //получаем из процесса демона
    if((res=BecomeDaemonProcess(
                            gLockFilePath,
                            "gps_daemon",
                            LOG_DEBUG,
                            &gLockFileDesc,
                            &daemonPID))<0)
    {
        syslog(LOG_CRIT,"Failed to become daemon process");
        error("Failed to become daemon process");
    }

    syslog(LOG_INFO,"Started daemon v.%s", SCRIPT_VER);

    //Блокируем вызов завершения работы потомка
    signal(SIGCHLD,SIG_IGN);

    // Создаем семафор на доступ к подключению к БД
    sem_unlink("/teltond_db_semaphore");
    for(i = 0; i < DB_USERS; i++)
    {
        sprintf(buff, "/teltond_db_semaphore%d", i);
        sem_unlink(buff);
    }

    db_sem = sem_open("/teltond_db_semaphore", O_CREAT, 0777, 1);
    if(db_sem == SEM_FAILED)
    {
        syslog(LOG_CRIT,"Failed to create semaphore");
        error("Failed to create semaphore");
        exit(0);
    }

    for(i = 0; i < DB_USERS; i++)
    {
        sprintf(buff, "/teltond_db_semaphore%d", i);
        db_sem_u[i] = sem_open(buff, O_CREAT, 0777, 1);

        if(db_sem_u[i] == SEM_FAILED)
        {
            syslog(LOG_CRIT,"Failed to create semaphore");
            error("Failed to create semaphore");
            exit(0);
        }
    }


    //Подключаемся к базе данных
    for(i = 0; i < DB_USERS; i++)
    {
        sprintf(buff, "GPSDAEMON%d", i+1);
        syslog(LOG_INFO,buff);
        pqSql_u[i] = PQsetdbLogin(  "127.0.0.1",
                                    "5432",
                                    NULL,
                                    NULL,
                                    "postgres",
                                    buff,
                                    "NPkjadsNLKJboiBILSDFS"); // NPkjadsNLKJboiBILSDFS // rjGhRuE5LVHl1xLMSHdB
        if(PQstatus(pqSql_u[i]) == CONNECTION_BAD)
        {
            syslog(LOG_CRIT,"Failed to connect to DB");
            error("Failed to connect to DB");
        }
    }
    pqSql = pqSql_u[0];

    PQexec(pqSql, "UPDATE devices SET pid = 0 WHERE pid > 0");

    // Current Database pointer
    cdb = 0;

    //Создаем сокет
    if((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0)
    {
        AddLog(db_sem, pqSql, getpid(), "Unknown", 0, "Failed to create socket");
        syslog(LOG_CRIT,"Failed to create socket");
        error("Failed to create socket");
        exit(0);
    }

    //add SO_REUSEADDR
    int optval = 1;
    setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    serv_addr.sin_port = htons(11546); // 16432

    //Биндим сокет на определенный адресс и порт
    if(bind(sock, (struct sockaddr*) &serv_addr, sizeof(serv_addr)) < 0)
    {
        AddLog(db_sem, pqSql, getpid(), "Unknown", 0, "Failed to bind socket");
        syslog(LOG_CRIT,"Failed to bind socket");
        error("Failed to bind socket");
        exit(0);
    }

    //Делаем сокет не блокирующим
    if((f = fcntl(sock, F_GETFL, 0)) == -1 || fcntl(sock, F_SETFL, f | O_NONBLOCK) == -1)
    {
        error("Failed to fcntl");
        exit(0);
    }

    //Разрешаем сокету прослушивть порт
    if(listen(sock, 2048))
    {
        AddLog(db_sem, pqSql, getpid(), "Unknown", 0,"Failed to listen");
        syslog(LOG_CRIT,"Failed to create listen");
        error("Failed to listen");
        exit(0);
    }

    signal(SIGKILL|SIGTERM|SIGHUP|SIGSEGV|SIGSTOP, closeAll);
    //signal(SIGKILL, closeAll);
    //signal(SIGHUP, closeAll);
    //signal(SIGSEGV, closeAll);
    //
    unsigned char* cli_p;
    char cli_ip[20] = "000.000.000.000\0\0";
    addrLength = sizeof(cli_addr);

    while(1)
    {
        //Проверяем, есть ли подключения
        conn = accept(sock, (struct sockaddr*) &cli_addr, &addrLength);
        usleep(100000);
        cli_p = (unsigned char*)(void*)&(cli_addr.sin_addr.s_addr);
        sprintf(cli_ip, "%hhu.%hhu.%hhu.%hhu", cli_p[0], cli_p[1], cli_p[2], cli_p[3]);

        //Если подключение появилось создаем для него поток
        if(conn < 0)
            continue;

        kolWait = 0;
        cdb = (cdb+1) % DB_USERS;

        if((child = fork()) != 0)
        {
            // Не смогли родить потомка
            if(child == -1)
            {
                AddLog(
                    db_sem,
                    pqSql,
                    getpid(),
                    "Unknown",
                    0,
                    "Failed to fork");
                clres = close(sock);
                return 0;
            }
            else
            {
                // Если потомка родили - надо закрыть его соединение
                // чтобы оно ему осталось
                clres = close(conn);
                if(clres != 0)
                {
                    syslog(
                        LOG_CRIT,
                        "Failed to close connection "
                            "after fork in parent");
                }
            }
            continue;
        }

        pqSql = pqSql_u[cdb];

        // Закрываем родительский сокет - у нас его копия
        // В принципе не обязательно, но держать открытым тоже не надо
        clres = close(sock);
        if(clres != 0)
            syslog(LOG_CRIT,"Failed to close socket after fork");

        // Создаем семафор на доступ к подключению к БД
        sprintf(buff, "/teltond_db_semaphore%d", cdb);
        db_sem = sem_open(buff,0);
        if(db_sem == SEM_FAILED)
        {
            //printf("Failed to create semaphore copy\n");
            syslog(LOG_CRIT,"Failed to create semaphore copy");
            error("Failed to create semaphore copy");
            CloseSocket(sock, conn, pqSql, NULL, 0);
        }

        sprintf(buff, "Successfully forked with new connection %s", cli_ip);
        AddLog(
            db_sem,
            pqSql,
            getpid(),
            "Unknown",
            0,
            buff);
        memset(IMEI, 0, 20);

        statConn = 0;
        id = 0;

        while(1)
        {
            //Получаем данные от клиента
            iResult = recv(conn, buff, 256, MSG_PEEK|MSG_DONTWAIT);

            //Если соединение с клиентом потеряно
            if(iResult == 0)
            {
                sprintf(buff, "Lost connection with client %s", cli_ip);
                AddLog(
                    db_sem,
                    pqSql,
                    getpid(),
                    IMEI,
                    id,
                    buff);
                CloseSocket(sock, conn, pqSql, db_sem, id);
            }

            switch(statConn)
            {
                case 0: { // 2 IMEI LEN
                    //Если принято хотя бы 2 байта, читаем длину IMEI
                    if (iResult >= 2)
                    {
                        recv(conn, buff, 2, 0);
                        kolIMEI = buff[0];
                        kolIMEI = kolIMEI << 8;
                        kolIMEI += buff[1];
                        kolWait = 0;
                        statConn = 1;
                    }
                } break;

                case 1: { // 15 IMEI
                    //Если принято хоты бы 15 байт и длина IMEI равна 15
                    //Проверяем на наличие принятого IMEI в базе
                    if(iResult >= 15 &&  kolIMEI == 15)
                    {
                        memset(IMEI, 0, 20);
                        recv(conn, IMEI, 15, 0);
                        kolWait = 0;
                        statConn = 2;
                        memset(buff,0,2048);

                        sprintf(
                            buff,
                            "SELECT _id, pid "
                            "FROM devices "
                            "WHERE imei='%s' LIMIT 1",
                            IMEI);
                        sem_wait(db_sem);
                        result = PQexec(pqSql, buff);
                        if(PQntuples(result) != 0)
                        {
                            //Если есть в базе, получаем id устройства и последний pid
                            id = atol(PQgetvalue(result, 0, 0));
                            old_pid = (pid_t)atoi(PQgetvalue(result, 0, 1));

                            PQclear(result);


                            if( old_pid > 0 && 0 == kill(old_pid, 0) ) {
                                sem_post(db_sem);
                                sprintf(
                                    logbuff,
                                    "Device %d already connected to %d", id, old_pid);
                                syslog(LOG_CRIT, logbuff);
                                AddLog(
                                    db_sem,
                                    pqSql,
                                    getpid(),
                                    IMEI,
                                    id,
                                    logbuff);
                                CloseSocket(sock, conn, pqSql, NULL, 0);
                            }

                            buff[0] = 1;
                            //Отправляем в ответ "1"
                            send(conn, buff, 1, 0);

                            sprintf(
                                buff,
                                "UPDATE devices "
                                "SET pid = %d "
                                "WHERE _id=%d",
                                getpid(), id);

                            PQexec(pqSql, buff);
                            PQclear(result);
                            sem_post(db_sem);

                            sprintf(buff, "Authenticated device %s", cli_ip);
                            AddLog(
                                db_sem,
                                pqSql,
                                getpid(),
                                IMEI,
                                id,
                                buff);


                            //Обнуляем массив наличия таблиц с входами
                            memset(
                                table_exists,
                                0,
                                sizeof(table_exists));
                        }
                        else
                        {
                            //Если нет в базе - закрываем соединение
                            PQclear(result);
                            sem_post(db_sem);

                            strcpy(
                                logbuff,
                                "Device is not in the database");

                            AddLog(
                                db_sem,
                                pqSql,
                                getpid(),
                                IMEI,
                                0,
                                logbuff);
                            CloseSocket(sock, conn, pqSql, NULL, 0);
                        }
                    }
                } break;

                case 2: { // 10 HEADER+CodecId
                    if (iResult >= 10)
                    {
                        //Пропуск первых 8 байт из пакета
                        recv(conn, buff, 8, 0);
                        //Чтение CodecID и количество пакетов данных
                        recv(conn, buff, 2, 0);
                        codecID = buff[0];
                        numAvlData = buff[1];
                        if(codecID == 8 && numAvlData > 0)
                        {
                            statConn = 3;
                            kolWait = 0;
                            //sprintf(buff, "Got %u points, statConn=3", numAvlData);
                            //syslog(LOG_INFO, buff);
                        }
                        else
                        {
                            sprintf(buff, "Unknown device (CodecID = 0x%02hhX)", codecID);
                            AddLog(
                                db_sem,
                                pqSql,
                                getpid(),
                                IMEI,
                                id,
                                buff);
                            CloseSocket(sock, conn, pqSql, db_sem, id);
                        }
                    }
                    else
                    {
                        //syslog(
                            //LOG_CRIT,
                            //"PID %d #2 iResult: %d kolWait: %d/18000",
                            //getpid(),
                            //iResult,
                            //kolWait);
                    }
                } break;

                case 3: { // 24 AVL_DATA
                    //Если есть пакеты данных
                    if(numAvlData != 0)
                    {
                        memset(&elemAvl, 0, sizeof(AvlData));

                        // Чтение 24 байт и занесение информации о GPS
                        // в структуру с переводом во внутреннюю форму
                        // представления данных
                        if(iResult >= 24)
                        {
                            recv(conn, buff, 24, 0);

                            for(i = 0; i < 8; i++)
                                ((unsigned char *) (&elemAvl.timeStamp))[i] = buff[7-i];

                            elemAvl.timeStamp = elemAvl.timeStamp / 1000;
                            elemAvl.priority = buff[8];

                            for (i = 0; i<4; i++)
                                ((unsigned char *) (&elemAvl.longitude))[i] = buff[12-i];

                            for (i = 0; i<4; i++)
                                ((unsigned char *) (&elemAvl.latitude))[i] = buff[16-i];

                            for (i = 0; i<2; i++)
                                ((unsigned char *) (&elemAvl.altitude))[i] = buff[18-i];

                            for (i = 0; i<2; i++)
                                ((unsigned char *) (&elemAvl.angle))[i] = buff[20-i];

                            elemAvl.satellites = buff[21];

                            for (i = 0; i<2; i++)
                                ((unsigned char *) (&elemAvl.speed))[i] = buff[23-i];
                            kolWait = 0;
                            statConn = 4;

                            /*syslog(LOG_INFO,
                                "PID %d timeStamp: %lld lon=%d lat=%d angle=%d sat=%d spd=%d, statConn=4",
                                getpid(),
                                elemAvl.timeStamp,
                                elemAvl.longitude,
                                elemAvl.latitude,
                                elemAvl.angle,
                                elemAvl.satellites,
                                elemAvl.speed);*/

                        }
                    }
                    else
                    {
                        // Eсли принимаемые данные закончились
                        // Повторно читаем количество переданных
                        // пакетов данных.
                        // Формируем пакет данных и отправляем
                        // на устройство.
                        // Переходим к началу получения пакетов данных

                        if (iResult >= 5)
                        {
                            recv(conn, buff, 5, 0);
                            buff[3] = buff[0];
                            buff[2] = 0;
                            buff[1] = 0;
                            buff[0] = 0;
                            send(conn, buff, 4, 0);
                            kolWait = 0;
                            statConn = 2;

                            //sprintf(logbuff, "Fin %d points, statConn=2",
                            //    buff[3]);
                            //syslog(LOG_INFO, buff);
                        }
                    }
                } break;

                case 4: { // 2 IO id+cnt
                    // Читаем Event IO ID и общее количество
                    // данных состояния
                    if(iResult >= 2)
                    {
                        recv(conn, buff, 2, 0);
                        memset(elemAvl.idPres,0,256);
                        kolWait = 0;
                        statConn = 5;
                        /*syslog(LOG_INFO,
                                "PID %d IO: id=0x%02hhx, cnt=%hhu, statConn=5",
                                getpid(),
                                buff[0],
                                buff[1]);*/

                    }
                } break;

                case 5: { // 1 IO1 cnt
                    //Чтение количества данных состояния длиной 1 байт
                    if(iResult >= 1)
                    {
                        recv(conn, &numIO, 1, 0);
                        kolWait = 0;
                        statConn = 6;
                    }
                } break;

                case 6: { // 2 IO1 id+val
                    /* Если данные состояния длиной 1 байт есть
                    читаем их и заносим в структуру */
                    if(numIO != 0)
                    {
                        if(iResult >= 2)
                        {
                            recv(conn, buff, 2, 0);
                            elemAvl.idPres[buff[0]] = 1;
                            elemAvl.idVal[buff[0]] = buff[1];
                            kolWait = 0;
                            numIO--;
                        }
                    }
                    else
                    {
                        // Если данные состояния
                        // длиной 1 байт закончились
                        statConn = 7;
                        kolWait = 0;
                    }
                } break;

                case 7: { // 1 IO2 cnt
                    //Чтение количества данных состояния длиной 2 байта
                    if(iResult >= 1)
                    {
                        recv(conn, &numIO, 1, 0);
                        statConn = 8;
                        kolWait = 0;
                    }
                } break;

                case 8: { // 3 IO2 id+val
                    /*Если данные состояния длиной 2 байта есть
                    читаем их и заносим в структуру*/
                    if (numIO != 0)
                    {
                        if(iResult >= 3)
                        {
                            recv(conn, buff, 3, 0);
                            elemAvl.idPres[buff[0]] = 1;
                            elemAvl.idVal[buff[0]] = buff[1];
                            elemAvl.idVal[buff[0]] =
                                            elemAvl.idVal[buff[0]] << 8;
                            elemAvl.idVal[buff[0]] += buff[2];
                            numIO--;
                            kolWait = 0;
                        }
                    }
                    else
                    {
                        // Если данные состояния длиной
                        // 2 байта закончились
                        statConn = 9;
                        kolWait = 0;
                    }
                } break;

                case 9: { // 1 IO4 cnt
                    //Чтение количества данных состояния длиной 4 байта
                    if (iResult >= 1)
                    {
                        recv(conn, &numIO, 1, 0);
                        statConn = 10;
                        kolWait = 0;
                    }
                } break;

                case 10: { // 5 IO4 id+val
                    /*Если данные состояния длиной 4 байта есть
                    читаем их и заносим в структуру*/
                    if(numIO != 0)
                    {
                        if(iResult >= 5)
                        {
                            recv(conn, buff, 5, 0);
                            elemAvl.idPres[buff[0]] = 1;
                            elemAvl.idVal[buff[0]] = buff[1];
                            elemAvl.idVal[buff[0]] =
                                            elemAvl.idVal[buff[0]] << 8;
                            elemAvl.idVal[buff[0]] += buff[2];
                            elemAvl.idVal[buff[0]] =
                                            elemAvl.idVal[buff[0]] << 8;
                            elemAvl.idVal[buff[0]] += buff[3];
                            elemAvl.idVal[buff[0]] =
                                            elemAvl.idVal[buff[0]] << 8;
                            elemAvl.idVal[buff[0]] += buff[4];
                            kolWait = 0;
                            numIO--;
                        }
                    }
                    else
                    {
                        // Если данные состояния длиной
                        // 4 байта закончились
                        statConn = 11;
                        kolWait = 0;
                    }
                } break;

                case 11: { // 1 IO8 cnt
                    //Чтение количества данных состояния длиной 8 байт
                    if (iResult >= 1)
                    {
                        recv(conn, &numIO, 1, 0);
                        statConn = 12;
                        kolWait = 0;
                    }
                } break;

                case 12: { // 9 IO8 id+val
                    /*Если данные состояния длиной 8 байт есть
                    читаем их и заносим в структуру*/
                    if(numIO != 0)
                    {
                        if (iResult >= 9)
                        {
                            recv(conn, buff, 9, 0);
                            elemAvl.idPres[buff[0]] = 1;
                            elemAvl.idVal[buff[0]] = buff[1];
                            elemAvl.idVal[buff[0]] =
                                            elemAvl.idVal[buff[0]] << 8;
                            elemAvl.idVal[buff[0]] += buff[2];
                            elemAvl.idVal[buff[0]] =
                                            elemAvl.idVal[buff[0]] << 8;
                            elemAvl.idVal[buff[0]] += buff[3];
                            elemAvl.idVal[buff[0]] =
                                            elemAvl.idVal[buff[0]] << 8;
                            elemAvl.idVal[buff[0]] += buff[4];
                            elemAvl.idVal[buff[0]] =
                                            elemAvl.idVal[buff[0]] << 8;
                            elemAvl.idVal[buff[0]] += buff[5];
                            elemAvl.idVal[buff[0]] =
                                            elemAvl.idVal[buff[0]] << 8;
                            elemAvl.idVal[buff[0]] += buff[6];
                            elemAvl.idVal[buff[0]] =
                                            elemAvl.idVal[buff[0]] << 8;
                            elemAvl.idVal[buff[0]] += buff[7];
                            elemAvl.idVal[buff[0]] =
                                            elemAvl.idVal[buff[0]] << 8;
                            elemAvl.idVal[buff[0]] += buff[8];
                            kolWait = 0;
                            numIO--;
                        }
                    }
                    else
                    {
                        //Проверяем не были ли эти данные получены ранее
                        //Получение времени передачи пакета
                        now = time(NULL);

                        /*syslog(LOG_INFO,
                            "POINT time: %lld",
                            elemAvl.timeStamp);*/


                        if((time_t)elemAvl.timeStamp < now + 172800)
                        {//now + two days
                            ts = localtime((time_t*)&elemAvl.timeStamp);
                            strftime(tmSend, 128, "%Y-%m-%d %X", ts);

                            // Надо оптимизировать!!! Возможно поставить
                            // уникальное условие в БД на поле when1
                            // а при вставке анализировать успех
                            sprintf(
                                buff,
                                "SELECT 1 "
                                "FROM device_data.events_%d "
                                "WHERE when1='%s'",
                                id,
                                tmSend);

                            sem_wait(db_sem);
                            result = PQexec(pqSql, buff);
                            AlreadyHaveThis = 0;

                            if(PQntuples(result) != 0)
                                AlreadyHaveThis = 1;

                            PQclear(result);
                            sem_post(db_sem);

                            if(((unsigned int)elemAvl.latitude != 0) &&
                               ((unsigned int)elemAvl.longitude != 0) &&
                               AlreadyHaveThis == 0)
                            {
                                oldVolt = 1;
                                newVolt = 0;

                                //Получение текушего времени
                                ts = localtime(&now);
                                strftime(tmNow, 128, "%Y-%m-%d %X", ts);

                                // Вычисление расстояния от предыдущей
                                // коодринаты до текущей
                                sprintf(
                                    buff,
                                    "SELECT coord "
                                    "FROM device_data.events_%d "
                                    "ORDER BY _id DESC LIMIT 1;",
                                    id);
                                sem_wait(db_sem);
                                result = PQexec(pqSql, buff);

                                if(PQntuples(result) != 0)
                                {
                                    //syslog(LOG_INFO, "eval distance");
                                    sscanf(
                                        PQgetvalue(result, 0, 0),
                                        "(%f,%f)",
                                        &lonold,
                                        &latold);
                                    distance =
                                        path_distance_func(
                                            latold*10000000.0,
                                            lonold*10000000.0,
                                            elemAvl.latitude,
                                            elemAvl.longitude);
                                    PQclear(result);
                                    sem_post(db_sem);

                                    if( distance >= 0 &&
                                        distance < 10000)
                                    {
                                    }
                                    else
                                    {
                                        distance = 0;
                                    }

                                    if(distance > 1000)
                                    {
                                        sprintf(
                                            logbuff,
                                            "Distance is more than "
                                            "1km: OLD(%d, %d) NEW(%d, %d) "
                                            "dist=%.2f",
                                            latold,
                                            lonold,
                                            elemAvl.latitude,
                                            elemAvl.longitude,
                                            distance);
                                        AddLog(
                                            db_sem,
                                            pqSql,
                                            getpid(),
                                            IMEI,
                                            id,
                                            logbuff);
                                    }
                                }
                                else
                                {
                                    PQclear(result);
                                    sem_post(db_sem);

                                    distance = 0;
                                }


                                // Занесение данных о GPS в базу
                                if(elemAvl.altitude > 15000)
                                    elemAvl.altitude = 0;

                                // Лучше вычислить здесь,
                                // чем напрягать базу
                                actLat = elemAvl.latitude / 10000000.0;
                                actLng = elemAvl.longitude / 10000000.0;

                                sprintf(
                                    buff,
                                    "UPDATE event_max_time_stamp "
                                    "SET "
                                        "time_stamp = '%s', "
                                        "when1 = '%s', "
                                        "coord = POINT(%.7f, %.7f), "
                                        "speed = %d, "
                                        "nsat = %d "
                                    "WHERE device_id=%d; "
                                    "INSERT INTO device_data.events_%d "
                                    "("
                                        "when1, "
                                        "device_id, "
                                        "time_stamp, "
                                        "priority, "
                                        "altitude, "
                                        "speed, "
                                        "angle, "
                                        "nsat, "
                                        "distance, "
                                        "coord) "
                                    "VALUES"
                                    "("
                                        "'%s',"
                                        "%d,"
                                        "'%s',"
                                        "%d,"
                                        "%d,"
                                        "%d,"
                                        "%d,"
                                        "%d,"
                                        "%.2f,"
                                        "POINT(%.7f, %.7f)"
                                    ") "
                                    "ON CONFLICT(_id) DO UPDATE SET "
                                        "when1 = '%s', "
                                        "device_id = %d, "
                                        "time_stamp = '%s', "
                                        "priority = %d, "
                                        "altitude = %d, "
                                        "speed = %d, "
                                        "angle = %d, "
                                        "nsat = %d, "
                                        "distance = %.2f, "
                                        "coord = POINT(%.7f, %.7f) "
                                    "RETURNING _id;",
                                    tmNow,
                                    tmSend,
                                    actLng, actLat,
                                    elemAvl.speed,
                                    elemAvl.satellites,
                                    id,
                                    id,
                                    tmSend,
                                    id,
                                    tmNow,
                                    elemAvl.priority,
                                    elemAvl.altitude,
                                    elemAvl.speed,
                                    elemAvl.angle,
                                    elemAvl.satellites,
                                    distance,
                                    actLng,
                                    actLat,
                                    tmSend,
                                    id,
                                    tmNow,
                                    elemAvl.priority,
                                    elemAvl.altitude,
                                    elemAvl.speed,
                                    elemAvl.angle,
                                    elemAvl.satellites,
                                    distance,
                                    actLng,
                                    actLat);

                                sem_wait(db_sem);
                                result = PQexec(pqSql, buff);

                                if(PQntuples(result))
                                {
                                    max_id = atol(PQgetvalue(result, 0, 0));
                                    /*syslog(LOG_INFO,
                                            "EVENT %s = %d",
                                            tmSend,
                                            max_id);*/
                                }
                                else
                                {
                                    max_id = 0;
                                    /*syslog(LOG_CRIT, "EVENT_ERR: %s", PQresultErrorMessage(result));*/
                                }

                                PQclear(result);
                                sem_post(db_sem);

                                //sprintf(logbuff, " ");

                                //Занесение в базу данных состояния
                                for (i = 0; i < 255 && max_id; i++)
                                {
                                    //Проверяем наличие таблицы
                                    if(elemAvl.idPres[i] == 1)
                                    {

                                        sem_wait(db_sem);

                                        if(table_exists[i] == 0)
                                        {
                                            sprintf(
                                                buff,
                                                "SELECT 1 "
                                                "FROM pg_catalog.pg_class "
                                                "WHERE relname='inputs_%d_%d';",
                                                id,
                                                i);

                                            result =
                                                PQexec(pqSql, buff);
                                            table_exists[i] =
                                                PQntuples(result)>0?1:0;
                                            PQclear(result);
                                        }

                                        if(table_exists[i] == 0)
                                        {// create & insert
                                            sprintf(
                                                buff,
                                                "CREATE TABLE IF NOT EXISTS device_data.inputs_%d_%d "
                                                "( CHECK (device_id = %d AND input_type= %d ), "
                                                "PRIMARY KEY (input_id), "
                                                "CONSTRAINT \"ext_device_id_%d_%d\" "
                                                    "FOREIGN KEY (device_id) REFERENCES devices (_id) "
                                                    "MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION,"
                                                "CONSTRAINT \"ext_events_id_%d_%d\" "
                                                    "FOREIGN KEY (event_id) REFERENCES device_data.events_%d (_id) "
                                                    "MATCH SIMPLE ON UPDATE NO ACTION ON DELETE CASCADE, "
                                                "CONSTRAINT \"uk_inputs_%d_%d\" "
                                                    "UNIQUE (event_id, device_id, input_type)) "
                                                "INHERITS (device_data.inputs); "
                                                "GRANT SELECT ON TABLE device_data.inputs_%d_%d TO userviewer;"
                                                "CREATE INDEX \"INPUTS_INPUT_ID_idx_%d_%d\" "
                                                    "ON device_data.inputs_%d_%d USING btree (input_id ); "
                                                "CREATE INDEX \"INPUTS_event_id_idx_%d_%d\" "
                                                    "ON device_data.inputs_%d_%d USING btree (event_id ); "
                                                "CREATE INDEX \"INPUTS_type_idx_%d_%d\" "
                                                    "ON device_data.inputs_%d_%d USING btree (input_type ); "
                                                "CREATE INDEX fki_ext_device_id_%d_%d "
                                                    "ON device_data.inputs_%d_%d USING btree (device_id ); "
                                                "CREATE INDEX fki_ext_events_id_%d_%d "
                                                    "ON device_data.inputs_%d_%d USING btree (event_id ); "
                                                "INSERT INTO device_data.inputs_%d_%d "
                                                "("
                                                    "event_id,"
                                                    "input_val,"
                                                    "input_type,"
                                                    "device_id) "
                                                "VALUES "
                                                "(%ld,%ld,%d,%d);",
                                                id, i,
                                                id,
                                                i,
                                                id, i,
                                                id, i,
                                                id,
                                                id, i,
                                                id, i,
                                                id, i, id, i,
                                                id, i, id, i,
                                                id, i, id, i,
                                                id, i, id, i,
                                                id, i, id, i,
                                                id, i,
                                                max_id,
                                                elemAvl.idVal[i],
                                                i,
                                                id);
                                        }
                                        else
                                        {// insert
                                            sprintf(
                                                buff,
                                                "INSERT INTO device_data.inputs_%d_%d "
                                                "("
                                                    "event_id,"
                                                    "input_val,"
                                                    "input_type,"
                                                    "device_id) "
                                                "VALUES "
                                                "(%ld,%ld,%d,%d) "
                                                "ON CONFLICT ON CONSTRAINT uk_inputs_%d_%d DO UPDATE SET "
                                                    "input_val = %ld;",
                                                id, i,
                                                max_id,
                                                elemAvl.idVal[i],
                                                i,
                                                id,
                                                id, i,
                                                elemAvl.idVal[i]);
                                        }

                                        result = PQexec(pqSql, buff);
                                        /*syslog(LOG_INFO,
                                            "INP_%d = %s",
                                            i,
                                            PQresultErrorMessage(result));*/

                                        iInpFld = 0;
                                        switch(i) {
                                            case 21:
                                                iInpFld = sprintf(tmNow, "gsm");
                                                break;
                                        }

                                        if(iInpFld > 0) {
                                            sprintf(
                                                buff,
                                                "UPDATE event_max_time_stamp "
                                                "SET %s = %ld WHERE device_id=%d;",
                                                tmNow,
                                                elemAvl.idVal[i],
                                                id);
                                            PQexec(pqSql, buff);
                                        }

                                        sem_post(db_sem);

                                        table_exists[i] = 1;
                                    }
                                }
                            } else {
                                debug("Skip package");
                            }
                        }

                        statConn = 3;
                        kolWait = 0;
                        numAvlData--;

                    }
                } break;
            }

            //Реализация задержки между проверкой состояния принятых байт
            if(kolWait >= 5)
            {
                //Завершение работы потока по истечению времени ожидания
                if (kolWait >= 18000)
                {
                    if(id < 0)
                        id = 0;

                    AddLog(db_sem, pqSql, getpid(), IMEI, id, "Break the connection due to timeout");
                    CloseSocket(sock, conn, pqSql, db_sem, id);
                }

                nanosleep (&sleeptime, NULL);
            }

            kolWait++;
        }
    }

    PQfinish(pqSql);
    close(sock);
    return 0;
}
