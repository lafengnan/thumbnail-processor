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

// Constants of a job status
const (
	Born = iota
	Submitted
	Running
	Succeed
	Failed
	Ghost
)

var stats = map[int]string{
	Born:      "Born",
	Submitted: "Submmited",
	Running:   "Running",
	Succeed:   "Succeed",
	Failed:    "Failed",
	Ghost:     "Ghost",
}

type Job struct {
	Id     uuid.UUID
	Stat   int
	F      func(...interface{}) (interface{}, error)
	Args   []interface{}
	Result interface{}
	Err    error
	added  chan bool
}

func NewJob(f func(...interface{}) (interface{}, error), args ...interface{}) *Job {
	job := &Job{
		Id:     uuid.NewV4(),
		Stat:   Born,
		F:      f,
		Args:   args,
		Result: nil,
		Err:    nil,
		added:  make(chan bool),
	}
	return job
}

func (job *Job) Info() {
	fname := reflect.ValueOf(job.F)
	log.Printf("Job Info: \n\tJob Id: %s\n\tStat: %v\n\ttask: %s(%v)\n", job.Id.String(), stats[job.Stat], fname, job.Args)
}

type Pool struct {
	mu                     *sync.Mutex
	supervisor_started     bool
	workers_started        bool
	num_workers            int
	num_running_workers    int
	num_jobs_born          int
	num_jobs_submitted     int
	num_jobs_running       int
	num_jobs_succeed       int
	num_jobs_failed        int
	num_jobs_ghost         int
	job_in                 chan *Job
	job_done               chan *Job
	job_to_worker_chan     chan chan *Job
	jobs_ready_to_run      *list.List
	jobs_succeed           *list.List
	jobs_failed            *list.List
	supervisor_killed_chan chan bool
	worker_killed_chan     chan bool
	interval               time.Duration
	worker_wg              sync.WaitGroup
	supervisor_wg          sync.WaitGroup
}

func NewPool(workers int) (pool *Pool) {
	pool = new(Pool)
	pool.mu = new(sync.Mutex)
	pool.num_workers = workers
	pool.jobs_ready_to_run = list.New()
	pool.jobs_succeed = list.New()
	pool.jobs_failed = list.New()
	pool.job_in = make(chan *Job)
	pool.job_done = make(chan *Job)
	pool.job_to_worker_chan = make(chan chan *Job)
	pool.interval = 1

	pool.startSupervisor()
	return
}

func (pool *Pool) AddJob(f func(...interface{}) (interface{}, error), args ...interface{}) {
	job := NewJob(f, args...)
	pool.mu.Lock()
	pool.num_jobs_born++
	pool.mu.Unlock()
	pool.job_in <- job
	<-job.added
}

// Start a supervisor
func (pool *Pool) startSupervisor() {
	if pool.supervisor_started {
		panic("Supervisor already started...")
	}
	log.Println("Starting supervisor...")
	pool.supervisor_wg.Add(1)
	go pool.supervisor()
	pool.supervisor_started = true

}

// Stop a supervisor
func (pool *Pool) stopSupervisor() {
	if !pool.supervisor_started {
		panic("Stop a stale supervisor")
	}
	pool.supervisor_killed_chan <- true
	pool.supervisor_wg.Wait()
	pool.supervisor_started = false
}

// supervisor manage and monitor all jobs
func (pool *Pool) supervisor() {
SUPER_LOOP:
	for {
		select {
		// New job
		case job := <-pool.job_in:
			pool.jobs_ready_to_run.PushBack(job)
			pool.num_jobs_submitted++
			job.added <- true
		// Send job to worker
		case job_out := <-pool.job_to_worker_chan:
			e := pool.jobs_ready_to_run.Front()
			var job *Job = nil
			if e != nil {
				job = e.Value.(*Job)
				pool.num_jobs_running++
				pool.jobs_ready_to_run.Remove(e)
			}
			job_out <- job
		case job := <-pool.job_done:
			pool.num_jobs_running--
			pool.jobs_succeed.PushBack(job)
		case <-pool.supervisor_killed_chan:
			break SUPER_LOOP
		}
	}
	pool.supervisor_wg.Done()
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

// Start a worker
func (pool *Pool) startWorker(idx int) {
	log.Printf("Starting worker %d...", idx)
	pool.mu.Lock()
	pool.num_running_workers++
	pool.mu.Unlock()
	job_out := make(chan *Job)
WORKER_LOOP:
	for {
		pool.job_to_worker_chan <- job_out
		job := <-job_out
		if job == nil {
			time.Sleep(pool.interval * time.Millisecond)
		} else {
			log.Print("In worker ", idx)
			working(job)
			pool.job_done <- job
		}
		select {
		case <-pool.worker_killed_chan:
			break WORKER_LOOP
		default:
		}
	}
	pool.worker_wg.Done()
}

// Run pool
func (pool *Pool) Run() {
	if !pool.supervisor_started {
		pool.startSupervisor()
	}
	for i := 0; i < pool.num_workers; i++ {
		pool.worker_wg.Add(1)
		go pool.startWorker(i)
	}

	for {
		if pool.num_running_workers == pool.num_workers {
			break
		}
		time.Sleep(pool.interval * time.Millisecond)
	}
	pool.workers_started = true
	log.Printf("%d workers are running successfully!", pool.num_running_workers)

}

// Stop pool
func (pool *Pool) Stop() {
	if !pool.workers_started {
		panic("The pool has stopped!")
	}
	for i := 0; i < pool.num_workers; i++ {
		pool.worker_killed_chan <- true
	}
	pool.worker_wg.Wait()
	pool.workers_started = false
	if pool.supervisor_started {
		pool.stopSupervisor()
	}
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
