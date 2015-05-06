package main

import (
    "log"
    "time"
    "flag"
    "os"
    "runtime"
    "syscall"
    "github.com/garyburd/redigo/redis"
)

func newPool(server, password string) *redis.Pool {
    return &redis.Pool {
        MaxIdle: 3,
        IdleTimeout: 240 * time.Second,
        Dial: func () (redis.Conn, error) {
            c, err := redis.Dial("tcp", server)
            if err != nil {
                log.Println(err)
                return nil, err
            }

            if len(password) > 0 {
                if _, err :=  c.Do("AUTH", password); err != nil {
                    c.Close()
                    log.Println(err)
                    return nil, err
                }
            }
            return c, err
        },
        TestOnBorrow: func (c redis.Conn, t time.Time) error {
            _, err := c.Do("PING")
            return err
        },

    }
}

var (
    pool *redis.Pool
    server = flag.String("server", ":6379", "")
    password = flag.String("password", "", "")
    thumbnail = flag.String("file", "", "")
    key = flag.String("key", "key", "")
    userid = flag.String("userid", "anan", "")
    uuid = flag.String("uuid", "123", "")
    out = flag.String("outfile", "test", "")
)

func download(conn redis.Conn, userid, uuid, out string) (n int, err error) {
    k := userid + ":" + uuid
    b := time.Now()
    content, err := conn.Do("GET", k)
    if err != nil {
        log.Fatal(err)
    }
    if content == nil {
        log.Printf("Empty key: %v", k)
        syscall.Exit(1)
    }
    f, err := os.Create(out)
    if err != nil {
        log.Fatal(err)
    }
    defer f.Close()

    n, err = f.Write(content.([]byte))
    if err != nil {
        log.Fatal(err)
    }
    e := time.Now()
    log.Printf("Spends %v seconds to download %s from redis", e.Sub(b), k)
    if n != len(content.([]byte)) {
        log.Fatal("partial write!")
    }

    return
}

func main() {

    flag.Parse()
    pool = newPool(*server, *password)
    conn := pool.Get()
    defer conn.Close()
    n, err := download(conn, *userid, *uuid, *out)
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("Download competed! %v bytes was downloaded", n)
    //daemon(0, 1)
    /*
    for {
        log.Println(syscall.Getpid(), "Starting...")
        time.Sleep(1 * time.Second)
    }
    */
}

func daemon(nochdir, noclose int) int {
    var ret, ret2 uintptr
    var err syscall.Errno

    darwin := runtime.GOOS == "darwin"

    // already a daemon
    if syscall.Getppid() == 1 {
        return 0
    }

    // fork off the parent process
    ret, ret2, err = syscall.RawSyscall(syscall.SYS_FORK, 0, 0, 0)
    if err != 0 {
        return -1
    }

    // failure
    if ret2 < 0 {
        os.Exit(-1)

    }

    // handle exception for darwin
    if darwin && ret2 == 1 {
        ret = 0

    }

    // if we got a good PID, then we call exit the parent process.
    if ret > 0 {
        os.Exit(0)
    }

    /* Change the file mode mask */
    _ = syscall.Umask(0)

    // create a new SID for the child process
    s_ret, s_errno := syscall.Setsid()
    if s_errno != nil {
        log.Printf("Error: syscall.Setsid errno: %d", s_errno)
    }
    if s_ret < 0 {
        return -1
    }

    if nochdir == 0 {
        os.Chdir("/")
    }

    if noclose == 0 {
        f, e := os.OpenFile("/dev/null", os.O_RDWR, 0)
        if e == nil {
            fd := f.Fd()
            syscall.Dup2(int(fd), int(os.Stdin.Fd()))
            syscall.Dup2(int(fd), int(os.Stdout.Fd()))
            syscall.Dup2(int(fd), int(os.Stderr.Fd()))
        }
    }

    return 0
}
 
