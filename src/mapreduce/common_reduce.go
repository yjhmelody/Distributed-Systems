package mapreduce

import (
	"log"
	"os"
	"sort"
	"encoding/json"
)

type keyValues []KeyValue

func (kvs keyValues) Len() int {
	return len(kvs)
}

func (kvs keyValues) Swap(i, j int) {
	kvs[i], kvs[j] = kvs[j], kvs[i]
}

func (kvs keyValues) Less(i, j int) bool {
	return kvs[i].Key < kvs[j].Key
}

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

	var kvs []KeyValue

	for i := 0; i < nMap; i++ {
		fileName := reduceName(jobName, i, reduceTask)
		f, err := os.Open(fileName)
		if err != nil {
			log.Fatal("open file error:", err)
		}
		defer f.Close()
		
		dec := json.NewDecoder(f)
		var kv KeyValue
		for dec.More() {
			err := dec.Decode(&kv)
			if err != nil {
				log.Fatal("decode error:", err)
			} else {
				kvs = append(kvs, kv)
			}
		}
	}
	// 当前 task 的输出
	mergeFile, err := os.Create(mergeName(jobName, reduceTask))
	if err != nil {
		log.Fatal("create merge file error:", err)
	}
	defer mergeFile.Close()
	enc := json.NewEncoder(mergeFile)

	// 按 key 排序，然后整合相同key的值，输出
	sort.Sort(keyValues(kvs))
	var values []string

	values = append(values, kvs[0].Value)
	
	last := 1
	for i := 1; i < len(kvs); i++ {
		cur := kvs[i-1]
		next := kvs[i]
		if cur.Key != next.Key {
			value := reduceF(cur.Key, values)
			enc.Encode(&KeyValue{cur.Key, value})
			values = make([]string, 0)
			last = i
		}
		values = append(values, next.Value)
	}

	value := reduceF(kvs[last].Key, values)
	enc.Encode(&KeyValue{kvs[last].Key, value})
}
