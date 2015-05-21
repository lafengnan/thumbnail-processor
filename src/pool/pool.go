package pool

import (
	"container/list"
	"fmt"
	"github.com/satori/go.uuid"
	"log"
	"sync"
	"time"
)

type Job struct {
	Id     uuid.UUID
	F      func(...interface{}) interface{}
	Args   []interface{}
	Result interface{}
	Err    error
	added  chan bool
}

type stats struct {
	Submitted int
	Running   int
	Succeed   int
	Failed    int
}

type Pool struct {
	supervisor_started bool
	worker_started     bool
	num_workers        int
	num_jobs_submitted int
	num_jobs_running   int
	num_jobs_succeed   int
	num_jobs_failed    int
	jobs_ready_to_run  *list.List
	jobs_succeed       *list.List
	jobs_failed        *list.List
	interval           time.Duration
	worker_wg          sync.WaitGroup
	supervisor_wg      sync.WaitGroup
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
