package mapreduce

import (
	"fmt"
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
			// map phase, assign tasks to worker,

			// find map task
			// keep reading workers address
			c := 0 // file counter

			for ;c < ntasks; { // ntasks, each task should be assigned to a worker
				workerRPCAddress, ok := <- registerChan
				if !ok {
					continue
				} else {
					fmt.Println("assigning map work to worker in: ", workerRPCAddress)
				}

				c++

				args := DoTaskArgs{jobName,
				mapFiles[c],
				phase,
				c,
				n_other}
				// use workerRPCAddress to initiate a call to assign work to the worker
				go call(workerRPCAddress, "Worker.DoTask", args ,nil)
			}

		case reducePhase:
			ntasks = nReduce
			n_other = len(mapFiles)

			i := 0

			for ; i < ntasks; {
				workerRPCAddress, ok := <- registerChan
				if !ok {
					continue
					} else {
					fmt.Println("assigning reduce work to worker in: ", workerRPCAddress)
				}

				i++

				args := DoTaskArgs{jobName,
				"", // the fileName arg is not used for reduce task for worker
				phase,
				i,
				n_other}

				go call(workerRPCAddress, "Worker.DoTask", args, nil)
			}
	}




	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	fmt.Printf("Schedule: %v done\n", phase)
}
