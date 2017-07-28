

package main

import (
	"encoding/json"
	"github.com/gorilla/mux"
	"log"
	"net/http"
	"net"
	"strconv"
	"sync"
	"math"
	"fmt"
	"os/exec"
//	 "github.com/docker/docker/client"
	"time"
	"bytes"
)

type Task struct {
	TaskID     			string  `json:"taskid,omitempty"`
	TaskClass    			string  `json:"taskclass,omitempty"`
	Image				string  `json:"image,omitempty"`
	CPU           			int64   `json:"cpu,omitempty"`
	TotalResourcesUtilization 	float64 `json:"totalresources,omitempty"` //total resouces used max(cpu_utilization,memory utilization)
	Memory      			int64   `json:"memory,omitempty"`
	CPUUtilization  		float64 `json:"cpuutilization,omitempty"`
	MemoryUtilization 		float64 `json:"memoryutilization,omitempty"`
	TaskType    			string  `json:"tasktype,omitempty"`
	CutReceived 			float64 `json:"cutreceived,omitempty"`
	CutToReceive 			float64 `json:"cuttoreceive,omitempty"`
	OriginalCPU			int64   `json:"originalcpu,omitempty"`
	OriginalMemory			int64	`json:"originalmemory,omitempty"`

}

type TaskResources struct {
    	CPU             int64     `json:"cpu, omitempty"`
    	Memory          int64     `json:"memory,omitempty"`
    	PreviousClass   string    `json:"previousclass,omitempty"`
    	NewClass        string    `json:"newclass,omitempty"`
    	Update          bool      `json:"update,omitempty"`
	IP		string	  `json:"ip,omitempty"`
}

var tasks map[string]*Task
var classTasks map[string][]*Task
 
var locks map[string]*sync.Mutex

var ip string 

var MAX_CUT_CLASS2 = 0.16
var MAX_CUT_CLASS3 = 0.33
var MAX_CUT_CLASS4 = 0.5

//adapted binary search algorithm for inserting ordered by ascendingo order based on total resources utilization of a task
func Sort(classList []*Task, searchValue float64) int {
        listLength := len(classList)
        lowerBound := 0
        upperBound := listLength - 1

        if listLength == 0 { //if the list is empty there is no need for sorting
                return 0
        }
        for {
                midPoint := (upperBound + lowerBound) / 2
                if lowerBound > upperBound && classList[midPoint].TotalResourcesUtilization > searchValue {
                        return midPoint
                } else if lowerBound > upperBound {
                        return midPoint + 1
                }

                if classList[midPoint].TotalResourcesUtilization < searchValue {
                        lowerBound = midPoint + 1
                } else if classList[midPoint].TotalResourcesUtilization > searchValue {
                        upperBound = midPoint - 1
                } else if classList[midPoint].TotalResourcesUtilization == searchValue {
                        return midPoint
                }
	 }
}
//function used to remove the task once it finished
func RemoveTask(w http.ResponseWriter, req *http.Request) {
	params := mux.Vars(req)
	taskID := params["taskid"]

	//this checks if the task still exists. This is required because there are two ways a task can be deleted. Either through a kill or the task finished
	//If its a kill, then this code will be ran twice so this check is required for error handling. If it finished, this code is only ran once 
	if task, ok  := tasks[taskID]; ok { 
		taskClass := tasks[taskID].TaskClass
		locks[taskClass].Lock()		
		for i, task := range classTasks[taskClass] {
			if task.TaskID == taskID {
				classTasks[taskClass] = append(classTasks[taskClass][:i], classTasks[taskClass][i+1:]...) //eliminate from slice
				delete(tasks,taskID)
				break
			}
		}	
		//check if host class should be updated
		if len(classTasks[taskClass]) == 0 && taskClass != "4"{
			newClass := "0"
			if len(classTasks["2"]) > 0 {
				newClass = "2"

			} else if len(classTasks["3"]) > 0 {
				newClass = "3"
			} else {
				newClass = "4"
			}
			taskResources := &TaskResources{CPU : task.CPU, Memory: task.Memory, PreviousClass: taskClass , IP: ip, NewClass: newClass, Update: true}
			go sendInfoHostRegistry(taskResources)	
		}else {
			taskResources := &TaskResources{CPU : task.CPU, Memory: task.Memory, IP: ip, Update: false}		
			go sendInfoHostRegistry(taskResources)	
		}
		locks[taskClass].Unlock()
		executeDockerCommand([]string{"-H","tcp://"+ip+":2376","kill",taskID})
		go executeDockerCommand([]string{"-H", "tcp://"+ip+":2376","rm",taskID, "-f"}) //removing container, due to a docker bug, the container is not deleted after finishing
	}
}

func executeDockerCommand(args []string){
	cmd := exec.Command("docker", args...)
	var out,stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	err := cmd.Run()

	if err != nil {
    		//fmt.Println(fmt.Sprint(err) + ": " + stderr.String())
	}
}

func sendInfoHostRegistry(task *TaskResources){
	//Update Task Registry with the task that was just created
	url := "http://10.5.60.2:12345/host/killtask"
	jsonStr, _ := json.Marshal(task)

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonStr))
	req.Header.Set("X-Custom-Header", "myvalue")
	req.Header.Set("Content-Type", "application/json")
		
	client := &http.Client{}
	resp, err := client.Do(req)
		
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
}

//this function will be used to update task info, when a cut is performed on the task
func UpdateTask(w http.ResponseWriter, req *http.Request) {
	params := mux.Vars(req)
	taskClass := params["taskclass"]
	taskID := params["taskid"]
	newCPU := params["newcpu"]
	newMemory := params["newmemory"]
	cutReceived := params["cutreceived"]

	amountCutted, _ := strconv.ParseFloat(cutReceived, 64)
	cpuToUpdate, _ := strconv.ParseInt(newCPU,10, 64)
	memoryToUpdate, _ := strconv.ParseInt(newMemory,10,64)

	locks[taskClass].Lock()

	tasks[taskID].CPU = cpuToUpdate
	tasks[taskID].Memory = memoryToUpdate
	tasks[taskID].CutReceived += amountCutted

	locks[taskClass].Unlock()	
}

func GetClass4Tasks(w http.ResponseWriter, req *http.Request) {
	locks["4"].Lock()

	returnList := make([]*Task, 0)
	for _, task := range classTasks["4"] {
		if task.TaskType == "service" {
			returnList = append(returnList, task)
		}
	}
	
	json.NewEncoder(w).Encode(returnList)	
	locks["4"].Unlock()
}

//returns tasks higher than request class
func GetHigherTasksCUT(w http.ResponseWriter, req *http.Request) {
	params := mux.Vars(req)
	requestClass := params["requestclass"]

	listTasks := make([]*Task, 0)

	/*
		In the code below we send the requestClass instead of hostClass because if this request gets scheduled to this host
		the hostClass will be the request class because we are in the case of HostClass >= requestClass. For example, if
		3 (HostClass) >= 2 (requestClass) if this request is scheduled to this host this host class will become 2 instead of 3.
		By sending requestClass we simulate if cutting whatever is on the host the request fits
*/
	switch requestClass {
		case "1":
			listTasks = append(listTasks, tasksToBeCut(classTasks["4"], requestClass)...)
			listTasks = append(listTasks, tasksToBeCut(classTasks["3"], requestClass)...)
			listTasks = append(listTasks, tasksToBeCut(classTasks["2"], requestClass)...)
			break
		case "2":
			listTasks = append(listTasks, tasksToBeCut(classTasks["4"], requestClass)...)
			listTasks = append(listTasks, tasksToBeCut(classTasks["3"], requestClass)...)
			break
		case "3":
			listTasks = append(listTasks, tasksToBeCut(classTasks["4"], requestClass)...)
			break
	}
	json.NewEncoder(w).Encode(listTasks)
}

func tasksToBeCut(listTasks []*Task, hostClass string) ([]*Task) {
	returnList := make([]*Task, 0)
	for _, task := range listTasks {
		taskCanBeCut, cutToReceive := taskCanBeCut(task, hostClass)
		if taskCanBeCut {
			task.CutToReceive = cutToReceive //the request will receive a smaller cut than the maximum supported due to cut restrictions
			returnList = append(returnList, task)
		}
	}
	return returnList
}

//this func returns true if the task can be cut, false otherwise
func taskCanBeCut(task *Task, hostClass string) (bool, float64) {
	switch task.TaskClass {
		case "2":
 //if the host is class 2 and the task is class 2, we cannot cut the task because it would suffer twice the penalty. Because it is already feeling the penalty of the overbooking
			if hostClass == "2" || task.CutReceived >= MAX_CUT_CLASS2 {
				return false, 0.0		// OR cannot cut this task, it is already expericing the maximum cut it can receive
			} else {
				return true, MAX_CUT_CLASS2
			}		
		case "3":
			if hostClass == "3" || task.CutReceived >= MAX_CUT_CLASS3 {
				return false, 0.0
			} else if hostClass == "2" { //it must received a smaller cut for the reasons mentioned in the report
				cutToReceive := MAX_CUT_CLASS3 - MAX_CUT_CLASS2
				return true, cutToReceive
			} else {
				// Imaginando o caso em que está num host class 2 e este request sofreu 30% cut
				//mas depois este host passa a class 1. nao posso fazer cut full. tenho que fazer até preencher  até ficar full,
				//neste caso mais 20% ficando 50% cut
				cutToReceive := MAX_CUT_CLASS3 - task.CutReceived
				return true, cutToReceive
			}			
		case "4":
			if hostClass == "4" || task.CutReceived >= MAX_CUT_CLASS4 {
				return false, 0.0
			} else if hostClass == "2" { //it must received a smaller cut for the reasons mentioned in the report
				cutToReceive := MAX_CUT_CLASS4 - MAX_CUT_CLASS2
				return true, cutToReceive
			} else if hostClass == "3" {
				cutToReceive := MAX_CUT_CLASS4 - MAX_CUT_CLASS3
				return true, cutToReceive
			}else {
				cutToReceive := MAX_CUT_CLASS4 - task.CutReceived
				return true, cutToReceive
			}
	}
	return false, 0.0
}


//returns tasks higher than request class
func GetHigherTasks(w http.ResponseWriter, req *http.Request) {
	params := mux.Vars(req)
	requestClass := params["requestclass"]
	listTasks := make([]*Task, 0)

	if requestClass == "1" {
		locks["4"].Lock()
		listTasks = append(listTasks, classTasks["4"]...)
		locks["4"].Unlock()

		locks["3"].Lock()
		listTasks = append(listTasks, classTasks["3"]...)
		locks["3"].Unlock()

		locks["2"].Lock()
		listTasks = append(listTasks, classTasks["2"]...)
		locks["2"].Unlock()

	} else if requestClass == "2" {
		locks["4"].Lock()
		listTasks = append(listTasks, classTasks["4"]...)
		locks["4"].Unlock()

		locks["3"].Lock()
		listTasks = append(listTasks, classTasks["3"]...)
		locks["3"].Unlock()
	} else if requestClass == "3" {
		locks["4"].Lock()
		listTasks = append(listTasks, classTasks["4"]...)
		locks["4"].Unlock()
	}
	json.NewEncoder(w).Encode(listTasks)
}

//returns tasks equal and higher than request class
func GetEqualHigherTasks(w http.ResponseWriter, req *http.Request) {
	params := mux.Vars(req)
	requestClass := params["requestclass"]
	hostClass := params["hostclass"]
	
	listTasks := make([]*Task, 0)
	/*
	Here we send hostClass instead of requestClass because we are in the case of hostClass < requestClass so the class of the host 
	after the request is assigned to this host will remain the same (the value of hostClass)
*/
	switch requestClass {
		case "2":
			listTasks = append(listTasks, tasksToBeCut(classTasks["4"],hostClass)...)
			listTasks = append(listTasks, tasksToBeCut(classTasks["3"],hostClass)...)
			listTasks = append(listTasks, tasksToBeCut(classTasks["2"],hostClass)...)
			break
		case "3":
			listTasks = append(listTasks, tasksToBeCut(classTasks["4"],hostClass)...)
			listTasks = append(listTasks, tasksToBeCut(classTasks["3"],hostClass)...)
			break
		case "4":
			listTasks = append(listTasks, tasksToBeCut(classTasks["4"],hostClass)...)
			break
	}
	json.NewEncoder(w).Encode(listTasks)
}

//function used to remove the task after its makespan has elapsed
func CountMakespan(makespan string, taskID string) {
	makespanTime,_ := strconv.ParseInt(makespan, 10, 64)
	time.Sleep(time.Duration(makespanTime) * time.Second) //after this time we remove the task

 	//this checks if the task still exists. This is required because there are two ways a task can be deleted. Either through a kill or the task finished
        //If its a kill, then this code will be ran twice so this check is required for error handling. If it finished, this code is only ran once 
        if task, ok  := tasks[taskID]; ok { 
                taskClass := tasks[taskID].TaskClass
                locks[taskClass].Lock()         
                for i, task := range classTasks[taskClass] {
                        if task.TaskID == taskID {
                                classTasks[taskClass] = append(classTasks[taskClass][:i], classTasks[taskClass][i+1:]...) //eliminate from slice
                                delete(tasks,taskID)
                                break
                        }
                }       
                //check if host class should be updated
                if len(classTasks[taskClass]) == 0 && taskClass != "4"{
                        newClass := "0"
                        if len(classTasks["2"]) > 0 {
                                newClass = "2"

                        } else if len(classTasks["3"]) > 0 {
                                newClass = "3"
                        } else {
                                newClass = "4"
                        }
                        taskResources := &TaskResources{CPU : task.CPU, Memory: task.Memory, PreviousClass: taskClass , IP: ip, NewClass: newClass, Update: true}
                        go sendInfoHostRegistry(taskResources)  
                }else {
                        taskResources := &TaskResources{CPU : task.CPU, Memory: task.Memory, IP: ip, Update: false}             
                        go sendInfoHostRegistry(taskResources)  
                }
                locks[taskClass].Unlock()
                executeDockerCommand([]string{"-H", "tcp://"+ip+":2376","kill",taskID})
                go executeDockerCommand([]string{"-H", "tcp://"+ip+":2376","rm",taskID, "-f"}) //removing container, due to a docker bug, the container is not deleted after finishing
        }	
}
//function used to remove the task after its makespan has elapsed when using the other scheduling algorithms
func CountMakespanNotEnergy(makespan string, taskID string, cpu int64, memory int64) {
	makespanTime,_ := strconv.ParseInt(makespan, 10, 64)
	time.Sleep(time.Duration(makespanTime) * time.Second) //after this time we remove the task

        taskResources := &TaskResources{CPU : cpu, Memory: memory, IP: ip, Update: false}             
        go sendInfoHostRegistry(taskResources)  
        executeDockerCommand([]string{"-H", "tcp://"+ip+":2376","kill",taskID})
        go executeDockerCommand([]string{"-H", "tcp://"+ip+":2376","rm",taskID, "-f"}) //removing container, due to a docker bug, the container is not deleted after finishing
}

//function used to call java code that will fill the redis service with data
func executeRedis (portNumber string, memory int64) {
	time.Sleep(1 * time.Second) //after this time we remove the task
	memoryRequirement := strconv.FormatInt(memory, 10)
        args := []string{"-cp","./jedis2.jar:.","fillRedis", ip, portNumber, memoryRequirement}
        cmd := exec.Command("java", args...)
	var out,stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	err := cmd.Run()

	if err != nil {
    		//fmt.Println(fmt.Sprint(err) + ": " + stderr.String())
	}
}

//function used to send to host registry which image and port should be used to make requests 
func startRequests(portNumber string, image string, memoryAux int64, makespan string, ip string) {
	memory := strconv.FormatInt(memoryAux, 10)

	req, err := http.NewRequest("GET","http://10.5.60.2:8001/entrypoint?"+portNumber+"&"+image+"&"+memory+"&"+makespan+"&"+ip , nil)
  
	req.Header.Set("X-Custom-Header", "myvalue")
	req.Header.Set("Content-Type", "application/json")

    	client := &http.Client{}
    	resp, err := client.Do(req)
    	if err != nil {
        	panic(err)
    	}
defer resp.Body.Close()
}

func CreateTask(w http.ResponseWriter, req *http.Request) {
	params := mux.Vars(req)
	var task Task
	_ = json.NewDecoder(req.Body).Decode(&task)

	requestClass := params["requestclass"]
	makespan := params["makespan"] //benchmark purposes: used to remove the task once its makespan time elapsed.
	portNumber := params["port"]

	if task.TaskType == "service" {
		go startRequests(portNumber, task.Image, task.Memory, makespan, ip)
	}

	if task.Image == "redis" {
		go executeRedis(portNumber, task.Memory)
	}	

	if requestClass == "0" {
		go CountMakespanNotEnergy(makespan, task.TaskID, task.CPU, task.Memory)
		return
	}
	go CountMakespan(makespan, task.TaskID)	

	newTask := make([]*Task,0)
	
	locks[requestClass].Lock()

    	tasks[task.TaskID] = &task
    	newTask = append(newTask, tasks[task.TaskID])
	//when a task is created we put at the end of the list since we don't know how much it will consume.
	//then the monitor will send information about its resource utilization and it shall be updated on the list accordingly

   	classTasks[requestClass] = append(classTasks[requestClass], newTask...)
	locks[requestClass].Unlock()
}

//updates both memory and cpu. message received from energy monitors. 
func UpdateBoth(w http.ResponseWriter, req *http.Request) {

	params := mux.Vars(req)
	taskID := params["taskid"]
	cpuUpdate := params["newcpu"]
	memoryUpdate := params["newmemory"]

	//task no longer exists
	if _, ok := tasks[taskID]; !ok {
		return
	}
	cpuToUpdate, _ := strconv.ParseFloat(cpuUpdate, 64)
	memoryToUpdate, _ := strconv.ParseFloat(memoryUpdate, 64)

	taskClass := tasks[taskID].TaskClass

	locks[taskClass].Lock()

	tasks[taskID].CPUUtilization = cpuToUpdate
	tasks[taskID].MemoryUtilization = memoryToUpdate	
	locks[taskClass].Unlock()		

	go UpdateTotalResourcesUtilization(cpuToUpdate, memoryToUpdate, 1, taskID) 
}

//function whose job is to check whether the total resources should be updated or not.
func UpdateTotalResourcesUtilization(cpu float64, memory float64, updateType int, taskID string){
	taskClass := tasks[taskID].TaskClass

	locks[taskClass].Lock()
	previousTotalResourceUtilization := tasks[taskID].TotalResourcesUtilization
	afterTotalResourceUtilization := 0.0
	switch updateType {
		case 1:
			afterTotalResourceUtilization = math.Max(cpu, memory)
			tasks[taskID].TotalResourcesUtilization = afterTotalResourceUtilization
			break
		case 2:
			afterTotalResourceUtilization = math.Max(cpu, tasks[taskID].MemoryUtilization)
			tasks[taskID].TotalResourcesUtilization = afterTotalResourceUtilization
			break
		case 3:
			afterTotalResourceUtilization = math.Max(tasks[taskID].CPUUtilization, memory)
			tasks[taskID].TotalResourcesUtilization = afterTotalResourceUtilization
			break
	}
	locks[taskClass].Unlock()

	//now we must check if the host region should be updated or not
	if afterTotalResourceUtilization != previousTotalResourceUtilization { 
		go UpdateList(taskID) //we going to update the task position inside its list		
	}
}

func UpdateList(taskID string) {
	//this deletes
	taskClass := tasks[taskID].TaskClass	

	locks[taskClass].Lock()
	for i := 0; i < len(classTasks[taskClass]); i++ {
		if classTasks[taskClass][i].TaskID == taskID {
			classTasks[taskClass] = append(classTasks[taskClass][:i], classTasks[taskClass][i+1:]...)
			break
		}
	}
	//this inserts in the list in its new position
	index := Sort(classTasks[taskClass], tasks[taskID].TotalResourcesUtilization)		
	classTasks[taskClass] = InsertTask(classTasks[taskClass], index, tasks[taskID])

	locks[taskClass].Unlock()
}

func InsertTask(classTasks []*Task, index int, task *Task) ([]*Task) {
	tmp := make([]*Task, 0)
	if index >= len(classTasks) { //if this is true then we put at end
		tmp = append(tmp, classTasks...)
		tmp = append(tmp, task)
	} else { //the code below is to insert into the index positin
		tmp = append(tmp, classTasks[:index]...)
		tmp = append(tmp, task)
		tmp = append(tmp, classTasks[index:]...)
	}
	return tmp
}

//updates cpu. message received from energy monitors. 
func UpdateCPU(w http.ResponseWriter, req *http.Request) {
	params := mux.Vars(req)
	taskID := params["taskid"]
	cpuUpdate := params["newcpu"]

	//task no longer exists
	if _, ok := tasks[taskID]; !ok {
		return
	}
	cpuToUpdate, _ := strconv.ParseFloat(cpuUpdate,64)

	taskClass := tasks[taskID].TaskClass	

    	locks[taskClass].Lock()
    	tasks[taskID].CPUUtilization = cpuToUpdate
    	locks[taskClass].Unlock()     

    	go UpdateTotalResourcesUtilization(cpuToUpdate, 0.0, 2, taskID) 	
}
//updates memory. message received from energy monitors. 
func UpdateMemory(w http.ResponseWriter, req *http.Request) {

	params := mux.Vars(req)
	taskID := params["taskid"]
	memoryUpdate := params["newmemory"]

	//task no longer exists
	if _, ok := tasks[taskID]; !ok {
		return
	}

	memoryToUpdate, _ := strconv.ParseFloat(memoryUpdate,64)

	taskClass := tasks[taskID].TaskClass

    	locks[taskClass].Lock()
    	tasks[taskID].MemoryUtilization = memoryToUpdate  
    	locks[taskClass].Unlock()     

    	go UpdateTotalResourcesUtilization(0.0, memoryToUpdate, 3, taskID) 
}

func main() {
	tasks = make(map[string]*Task)
	locks = make(map[string]*sync.Mutex)
	classTasks = make(map[string][]*Task)
	
	ip = getIPAddress()

	locks["1"] = &sync.Mutex{}
	locks["2"] = &sync.Mutex{}
	locks["3"] = &sync.Mutex{}
	locks["4"] = &sync.Mutex{}

	ServeSchedulerRequests()
}

func ServeSchedulerRequests() {
	router := mux.NewRouter()
	router.HandleFunc("/task/{requestclass}&{makespan}&{port}", CreateTask).Methods("POST")
	router.HandleFunc("/task/highercut/{requestclass}", GetHigherTasksCUT).Methods("GET")
	router.HandleFunc("/task/higher/{requestclass}", GetHigherTasks).Methods("GET")
	router.HandleFunc("/task/equalhigher/{requestclass}&{hostclass}", GetEqualHigherTasks).Methods("GET")
	router.HandleFunc("/task/remove/{taskid}", RemoveTask).Methods("GET")
	router.HandleFunc("/task/updatetask/{taskclass}&{newcpu}&{newmemory}&{taskid}&{cutreceived}", UpdateTask).Methods("GET")
	router.HandleFunc("/task/class4", GetClass4Tasks).Methods("GET")
	router.HandleFunc("/task/updateboth/{taskid}&{newcpu}&{newmemory}", UpdateBoth).Methods("GET")
	router.HandleFunc("/task/updatecpu/{taskid}&{newcpu}", UpdateCPU).Methods("GET")
	router.HandleFunc("/task/updatememory/{taskid}&{newmemory}", UpdateMemory).Methods("GET")
	
	log.Fatal(http.ListenAndServe(ip+":1234", router))
}

func getIPAddress() (string) {
    addrs, err := net.InterfaceAddrs()
    if err != nil {
        fmt.Println(err.Error())
    }
    count := 0
    for _, a := range addrs {
        if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
            if ipnet.IP.To4() != nil && count == 1 {
		    fmt.Println(ipnet.IP.String())
                    return ipnet.IP.String()
            }
	    count++
        }
    }
    return ""
}


