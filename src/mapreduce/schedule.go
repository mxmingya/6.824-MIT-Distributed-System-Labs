package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)

	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	// find map task
	// keep reading workers address
	var wg sync.WaitGroup;// used to wait all the goroutine to be finished.

	for c := 0; c < ntasks; c++ { // ntasks, each task should be assigned to a worker
		var f string

		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			workerRPCAddress, ok := <-registerChan
			if !ok {
				fmt.Printf("%s\n", "error found when try to read from channel")
			} else {
				fmt.Println("assigning map work to worker in: ", workerRPCAddress)
			}

			if phase == mapPhase {
				f = mapFiles[i]
			}

			args := DoTaskArgs{
				jobName,
				f,
				phase,
				i,
				n_other,
			}
			// use workerRPCAddress to initiate a call to assign work to the worker
			success := call(workerRPCAddress, "Worker.DoTask", args, nil)
			for !success {
				newWorker := <- registerChan
				success = call(newWorker, "Worker.DoTask", args, nil)
			}
		 	go func() {
		 		registerChan <- workerRPCAddress
		 		}()
		}(c)
	}
	wg.Wait() // wait all the goroutine being executed.

	fmt.Printf("Schedule: %v done\n", phase)
}