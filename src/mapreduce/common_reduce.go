package mapreduce

import
(
	"encoding/json"
	"os"
	"sort"
	"io"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//

	//fmt.Println("starting to run reduce tasks")
	//var kvMap = make(map[string][]string) // map of key -> []values
	//for i := 0; i < nMap; i++ {
	//	// for each map task, yields a reduce file containing the data,
	//	// perform a reduce task on that and write the reduced data to the disk
	//	reduceFileName := reduceName(jobName, i, reduceTask)
	//	fmt.Println("file name in reduce task is:", reduceFileName)
	//	inputF, openErr := os.Open(reduceFileName); checkErr(openErr)

	kvMap := make(map[string][]string)
	for i := 0; i < nMap; i++ {
		inputF, err := os.Open(reduceName(jobName, i, reduceTask))
		dec := json.NewDecoder(inputF); checkErr(err)


	//NewDecoder(内容) -> dec.Decode(容器)
	//decode content in 内容 to 容器里
	for {
		var kv KeyValue
		decodeErr := dec.Decode(&kv)
		//fmt.Println("key :", kv.Key, "value: ", kv.Value)
		if decodeErr != nil && decodeErr == io.EOF {
			break
		}
		_, ok := kvMap[kv.Key]
		if !ok {
			// value doesnt exist in map
			kvMap[kv.Key] = make([]string, 0)
		}
		values := kvMap[kv.Key]
		values = append(values, kv.Value)
	}
	inputF.Close()
	}

	var ks []string
	for k := range kvMap {
		ks = append(ks, k)
	}

	sort.Strings(ks)

	outputF, openErr := os.Create(outFile); checkErr(openErr)
	enc := json.NewEncoder(outputF)

	for _, k := range ks {
		enc.Encode(KeyValue{k, reduceF(k, kvMap[k])})
	}

	outputF.Close()

}
