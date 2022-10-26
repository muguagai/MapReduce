package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		response := doHeartbeat()
		log.Printf("Worker: receive coordinator's heartbeat %v \n", response)
		switch response.Task.JobType {
		case 0:
			doMapTask(mapf, response)
		case 1:
			doReduceTask(reducef, response)
		case 2:
			time.Sleep(1 * time.Second)
		case 3:
			return
		default:
			panic(fmt.Sprintf("unexpected jobType"))
		}
	}
}

// send an RPC Request to the coordinator, wait for the Response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	//c, err := rpc.DialHTTP("tcp", ":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func doHeartbeat() HeartbeatResponse {
	// declare an argument structure.
	request := HeartbeatRequest{}

	// declare a reply structure.
	reply := HeartbeatResponse{}

	// send the RPC Request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.AskTask", &request, &reply)
	if ok {
		// reply.Y should be 100.
		return reply
	} else {
		fmt.Printf("call failed!\n")
	}
	return HeartbeatResponse{}
}
func CallTaskDone(request *ReportRequest) {
	reply := ExampleReply{}
	call("Coordinator.Report", &request, &reply)
}
func doMapTask(mapf func(string, string) []KeyValue, response HeartbeatResponse) {
	//intermediate := []KeyValue{}
	file, err := os.Open(response.Task.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", response.Task.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", response.Task.FileName)
	}
	file.Close()
	intermediate := mapf(response.Task.FileName, string(content))
	//intermediate = append(intermediate, kva...)
	// prepare output files and encoders
	nReduce := response.NReduce
	//outFiles := make([]*os.File, nReduce)
	//fileEncs := make([]*json.Encoder, nReduce)
	/*for outindex := 0; outindex < nReduce; outindex++ {
		//outname := outprefix + strconv.Itoa(outindex)
		//outFiles[outindex], _ = os.Create(outname)
		outFiles[outindex], _ = ioutil.TempFile("mr-tmp", "mr-tmp-*")
		fileEncs[outindex] = json.NewEncoder(outFiles[outindex])
	}
	// distribute keys among mr-fileindex-*
	for _, kv := range intermediate {
		outindex := ihash(kv.Key) % nReduce
		file = outFiles[outindex]
		enc := fileEncs[outindex]
		err := enc.Encode(&kv)
		if err != nil {
			fmt.Printf("File %v Key %v Value %v Error: %v\n", response.Task.FileName, kv.Key, kv.Value, err)
			panic("Json encode failed")
		}
	}
	// save as files
	for outindex, file := range outFiles {
		outname := fmt.Sprintf("mr-%v-%v", response.Task.Id, outindex)
		oldpath := filepath.Join(file.Name())
		//fmt.Printf("temp file oldpath %v\n", oldpath)
		os.Rename(oldpath, outname)
		file.Close()
	}*/
	kvas := make([][]KeyValue, nReduce)
	for i := 0; i < nReduce; i++ {
		kvas[i] = make([]KeyValue, 0)
	}
	for _, kv := range intermediate {
		index := keyReduceIndex(kv.Key, nReduce)
		kvas[index] = append(kvas[index], kv)
	}
	for i := 0; i < nReduce; i++ {
		tempfile, err := os.CreateTemp(".", "mrtemp")
		if err != nil {
			log.Fatal(err)
		}
		enc := json.NewEncoder(tempfile)
		for _, kv := range kvas[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatal(err)
			}
		}
		outname := fmt.Sprintf("mr-%v-%v", response.Task.Id, i)
		err = os.Rename(tempfile.Name(), outname)
		if err != nil {
			log.Printf("rename tempfile failed for $v\n", outname)
		}
	}
	// acknowledge master
	CallTaskDone(&ReportRequest{
		Task: response.Task,
	})
}

func doReduceTask(reducef func(string, []string) string, response HeartbeatResponse) {
	outname := fmt.Sprintf("mr-out-%v", response.Task.Id-response.NMap)
	innameprefix := "mr-"
	innamesuffix := "-" + strconv.Itoa(response.Task.Id-response.NMap)
	// read in all files as a kv array
	intermediate := []KeyValue{}
	for index := 0; index < response.NMap; index++ {
		inname := innameprefix + strconv.Itoa(index) + innamesuffix
		file, err := os.Open(inname)
		if err != nil {
			fmt.Printf("Open intermediate file %v failed: %v\n", inname, err)
			panic("Open file error")
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(intermediate))

	ofile, err := ioutil.TempFile(".", "mrtemp")
	if err != nil {
		fmt.Printf("Create output file %v failed: %v\n", outname, err)
		panic("Create file error")
	}
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	os.Rename(filepath.Join(ofile.Name()), outname)
	ofile.Close()
	// acknowledge master
	CallTaskDone(&ReportRequest{
		Task: response.Task,
	})
}

func keyReduceIndex(key string, nReduce int) int {
	return ihash(key) % nReduce
}
