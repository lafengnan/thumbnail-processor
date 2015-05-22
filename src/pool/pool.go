package pool

import (
	"container/list"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/satori/go.uuid"
	"log"
	"reflect"
	"sync"
	"time"
)

type Job struct {
	Id     uuid.UUID
	Stat   int
	F      func(...interface{}) (interface{}, error)
	Args   []interface{}
	Result interface{}
	Err    error
	added  chan bool
}

func (job *Job) Info() {
	fname := reflect.ValueOf(job.F)
	log.Printf("Job Id: %s, Stat: %d task: %s(%s)", job.Id.String(), job.Stat, fname)
}

// Constants of a job status
const (
	Born = iota
	Submitted
	Running
	Succeed
	Failed
	Ghost
)

type Pool struct {
	supervisor_started bool
	workers_started    bool
	num_workers        int
	num_jobs_born      int
	num_jobs_submitted int
	num_jobs_running   int
	num_jobs_succeed   int
	num_jobs_failed    int
	num_jobs_ghost     int
	jobs_inqueue       chan *Job
	jobs_outqueue      chan *Job
	jobs_ready_to_run  *list.List
	jobs_succeed       *list.List
	jobs_failed        *list.List
	interval           time.Duration
	worker_wg          sync.WaitGroup
	supervisor_wg      sync.WaitGroup
}

func (pool *Pool) AddJob(f func(...interface{}) (interface{}, error), args ...interface{}) {
	job := new(Job)
	job.Id = uuid.NewV4()
	job.Stat = Born
	job.F = f
	job.Args = args
	job.Result = nil
	job.Err = nil
	job.added = make(chan bool)

	pool.jobs_inqueue <- job
	<-job.added
	job.Info()
}

// Start a supervisor
func (pool *Pool) startSupervisor() {
	if pool.supervisor_started {
		panic("Supervisor already started...")
	}
	log.Println("Starting supervisor...")
	pool.supervisor_wg.Add(1)
	go pool.supervisor()
	pool.supervisor_wg.Wait()
	pool.supervisor_started = true

}

// supervisor manage and monitor all jobs
func (pool *Pool) supervisor() {
	pool.supervisor_wg.Done()
}

// Start a worker
func (pool *Pool) startWorker(idx int) {
	log.Printf("Starting worker %d ...", idx)
	pool.worker_wg.Done()

}

// Run pool
func (pool *Pool) Run() {
	pool.startSupervisor()
	for i := 0; i < pool.num_workers; i++ {
		pool.worker_wg.Add(1)
		go pool.startWorker(i)
	}
	pool.worker_wg.Wait()
	pool.workers_started = true
}

func NewPool(workers int) (pool *Pool) {
	pool = new(Pool)
	pool.num_workers = workers
	pool.jobs_ready_to_run = list.New()
	pool.jobs_succeed = list.New()
	pool.jobs_failed = list.New()
	pool.interval = 1

	return
}

func working(job *Job) (err error) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("Error: Job: %s terminated with : %s", job.Id, err)
			job.Result = nil
			job.Err = fmt.Errorf(err.(string))
		}
	}()

	job.Result, err = job.F(job.Args...)
	return
}

func NewRedisPool(server, password string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				log.Println(err)
				return nil, err
			}

			if len(password) > 0 {
				if _, err := c.Do("AUTH", password); err != nil {
					c.Close()
					log.Println(err)
					return nil, err
				}
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}
