package gpool

import "sync"

type worker struct {
	workerPool chan *worker
	jobChannel chan Job
	stop       chan bool
}

func (w *worker) start() {
	go func() {
		var job Job
		for {
			w.workerPool <- w

			select {
			case job = <-w.jobChannel:
				job()
			case stop := <-w.stop:
				if stop {
					w.stop <- true
					return
				}
			}
		}
	}()
}

func newWorker(pool chan *worker) *worker {
	return &worker{
		workerPool: pool,
		jobChannel: make(chan Job),
		stop:       make(chan bool),
	}
}

type dispatcher struct {
	workerPool chan *worker
	jobQueue   chan Job
	stop       chan bool
}

func (d *dispatcher) dispatch() {
	for {
		select {
		case job := <-d.jobQueue:
			worker := <-d.workerPool
			worker.jobChannel <- job
		case stop := <-d.stop:
			if stop {
				for i := 0; i < cap(d.workerPool); i++ {
					worker := <-d.workerPool

					worker.stop <- true
					<-worker.stop
				}

				d.stop <- true
				return
			}
		}
	}
}

func newDispatcher(workerPool chan *worker, jobQueue chan Job) *dispatcher {
	d := &dispatcher{
		workerPool: workerPool,
		jobQueue:   jobQueue,
		stop:       make(chan bool),
	}

	for i := 0; i < cap(d.workerPool); i++ {
		worker := newWorker(d.workerPool)
		worker.start()
	}

	go d.dispatch()
	return d
}

type Job func()

type Pool struct {
	JobQueue   chan Job
	dispatcher *dispatcher
	wg         sync.WaitGroup
}

func NewPool(numWorkers int, jobQueueLen int) *Pool {
	jobQueue := make(chan Job, jobQueueLen)
	workerPool := make(chan *worker, numWorkers)

	pool := &Pool{
		JobQueue:   jobQueue,
		dispatcher: newDispatcher(workerPool, jobQueue),
	}

	return pool
}

func (p *Pool) JobDone() {
	p.wg.Done()
}

func (p *Pool) WaitCount(count int) {
	p.wg.Add(count)
}

func (p *Pool) WaitAll() {
	p.wg.Wait()
}

func (p *Pool) Release() {
	p.dispatcher.stop <- true
	<-p.dispatcher.stop
}
