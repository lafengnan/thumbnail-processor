package pool

import (
	"log"
	"reflect"
	"testing"
)

func task(args ...interface{}) (rt interface{}, err error) {
	log.Println("in task...")
	return
}

func TestNewJob(t *testing.T) {
	var args []interface{} = []interface{}{1, 2}
	job := NewJob(task, args...)
	job.Info()
	if len(job.Id.String()) == 0 {
		t.Fail()
	}
	if reflect.TypeOf(job.Id).Name() != "UUID" {
		t.Fail()
	}
	if job.Stat != Born {
		t.Fail()
	}
	if job.Result != nil {
		t.Fail()
	}
}

func TestAddJob(t *testing.T) {
	ppool := NewPool(10)
	ppool.AddJob(task, 1, 2)
	if ppool.num_jobs_born != 1 {
		t.Fail()
	}
}
