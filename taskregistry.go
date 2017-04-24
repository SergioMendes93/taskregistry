
package main

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"log"
	"net/http"
	"net"
	"strconv"
	"sync"
	"math"
//	 "github.com/docker/docker/client"
)

type Task struct {
	TaskID     					string `json:"taskid,omitempty"`
	TaskClass    				string `json:"taskclass,omitempty"`
	Image						string `json:"image,omitempty"`
	CPU           				string `json:"cpu,omitempty"`
	TotalResourcesUtilization 	string `json:"totalresources,omitempty"` //total resouces used max(cpu_utilization,memory utilization)
	Memory      				string `json:"memory,omitempty"`
	CPUUtilization  			string `json:"cpuutilization,omitempty"`
	MemoryUtilization 			string `json:"memoryutilization,omitempty"`
	TaskType    				string `json:"tasktype,omitempty"`
	CutReceived 				string `json:"cutreceived,omitempty"`
	CutToReceive 				string `json:"cuttoreceive,omitempty"`
}

type TaskResources struct {
	CPU 	float64 `json:"cpu,omitempty"`
	Memory	float64 `json:"memory, omitempty"`
}

var tasks map[string]*Task
var classTasks map[string][]*Task
 
var locks map[string]*sync.Mutex

var MAX_CUT_CLASS2 = "0.16"
var MAX_CUT_CLASS3 = "0.33"
var MAX_CUT_CLASS4 = "0.5"

//adapted binary search algorithm for inserting ordered by ascendingo order based on total resources utilization of a task
func Sort(classList []*Task, searchValue string) int {
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
	taskClass := tasks[taskID].TaskClass

	locks[taskClass].Lock()
	taskCPU, _ := strconv.ParseFloat(tasks[taskID].CPU,64)
	taskMemory, _ := strconv.ParseFloat(tasks[taskID].Memory,64)

	for i, task := range classTasks[taskClass] {
		if task.TaskID == taskID {
			classTasks[taskClass] = append(classTasks[taskClass][:i], classTasks[taskClass][i+1:]...) //eliminate from slice
			delete(tasks,taskID)
			break
		}
	}
	locks[taskClass].Unlock()

	taskResources := &TaskResources{CPU : taskCPU, Memory: taskMemory}
	json.NewEncoder(w).Encode(taskResources) 
}

//this function will be used to update task info, when a cut is performed on the task
func UpdateTask(w http.ResponseWriter, req *http.Request) {
	params := mux.Vars(req)
	taskClass := params["taskclass"]
	taskID := params["taskid"]
	newCPU := params["newcpu"]
	newMemory := params["newmemory"]
	cutReceived := params["cutreceived"]

	locks[taskClass].Lock()

	fmt.Println("Cut performed at " + taskID)
	fmt.Print("Before CUT cpu: " + tasks[taskID].CPU + " memory: " + tasks[taskID].Memory + " cutReceived " + tasks[taskID].CutReceived)

	tasks[taskID].CPU = newCPU
	tasks[taskID].Memory = newMemory
	tasks[taskID].CutReceived += cutReceived

	fmt.Print("After CUT cpu: " + tasks[taskID].CPU + " memory: " + tasks[taskID].Memory + " cutReceived " + tasks[taskID].CutReceived)

	locks[taskClass].Unlock()	
}

func GetClass4Tasks(w http.ResponseWriter, req *http.Request) {
	locks["4"].Lock()
	json.NewEncoder(w).Encode(classTasks["4"])	
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
	if requestClass == "1" {
		listTasks = append(listTasks, tasksToBeCut(classTasks["2"], requestClass)...)
		listTasks = append(listTasks, tasksToBeCut(classTasks["3"], requestClass)...)
		listTasks = append(listTasks, tasksToBeCut(classTasks["4"], requestClass)...)
	} else if requestClass == "2" {
		listTasks = append(listTasks, tasksToBeCut(classTasks["3"], requestClass)...)
		listTasks = append(listTasks, tasksToBeCut(classTasks["4"], requestClass)...)
	} else if requestClass == "3" {
		listTasks = append(listTasks, tasksToBeCut(classTasks["4"], requestClass)...)
	}
	fmt.Println("Got tasks")
	fmt.Println(listTasks)
	
	json.NewEncoder(w).Encode(listTasks)
}

func tasksToBeCut(listTasks []*Task, hostClass string) ([]*Task) {
	returnList := make([]*Task, 0)
	
	for _, task := range listTasks {
		taskCanBeCut, cutToReceive := taskCanBeCut(task, hostClass)
		fmt.Println("Checking if task can be cut " + task.TaskID)
		fmt.Println(task)
		if taskCanBeCut {
			fmt.Println("Added to cut list with cut to receive: " + cutToReceive)
			task.CutToReceive = cutToReceive //the request will receive a smaller cut than the maximum supported due to cut restrictions
			returnList = append(returnList, task)
		}
	}
	return returnList
}

//this func returns true if the task can be cut, false otherwise
func taskCanBeCut(task *Task, hostClass string) (bool, string) {
	switch task.TaskClass {
		case "2":
			if task.CutReceived >= MAX_CUT_CLASS2 {
				return false, ""		//cannot cut this task, it is already expericing the maximum cut it can receive
			} else if hostClass == "2" { //if the host is class 2 and the task is class 2, we cannot cut the task because it would suffer twice the penalty. Because it is already feeling the penalty of the overbooking
				return false, ""
			} else {
				return true, MAX_CUT_CLASS2
			}		
		case "3":
			if task.CutReceived >= MAX_CUT_CLASS3 {
				return false, ""
			} else if hostClass == "3" {
				return false, ""
			} else if hostClass == "2" { //it must received a smaller cut for the reasons mentioned in the report
				maxCutClass3, _ := strconv.ParseFloat(MAX_CUT_CLASS3, 64)
				maxCutClass2, _ := strconv.ParseFloat(MAX_CUT_CLASS2, 64)
				cutToReceive := maxCutClass3 - maxCutClass2
				return true, strconv.FormatFloat(cutToReceive, 'f', -1, 64)
			} else {
				// Imaginando o caso em que está num host class 2 e este request sofreu 30% cut
				//mas depois este host passa a class 1. nao posso fazer cut full. tenho que fazer até preencher  até ficar full,
				//neste caso mais 20% ficando 50% cut
				maxCutClass3, _ := strconv.ParseFloat(MAX_CUT_CLASS3, 64)
				cutReceived, _ := strconv.ParseFloat(task.CutReceived,64)				
				cutToReceive := maxCutClass3 - cutReceived
				return true, strconv.FormatFloat(cutToReceive, 'f', -1, 64)
			}
			
		case "4":
			if task.CutReceived >= MAX_CUT_CLASS4 {
				return false, ""
			} else if hostClass == "4" {
				return false, ""
			} else if hostClass == "2" { //it must received a smaller cut for the reasons mentioned in the report
				maxCutClass4, _ := strconv.ParseFloat(MAX_CUT_CLASS4, 64)
				maxCutClass2, _ := strconv.ParseFloat(MAX_CUT_CLASS2, 64)
				cutToReceive := maxCutClass4 - maxCutClass2
				return true, strconv.FormatFloat(cutToReceive, 'f', -1, 64)
			} else if hostClass == "3" {
				maxCutClass4, _ := strconv.ParseFloat(MAX_CUT_CLASS4, 64)
				maxCutClass3, _ := strconv.ParseFloat(MAX_CUT_CLASS3, 64)
				cutToReceive := maxCutClass4 - maxCutClass3
				return true, strconv.FormatFloat(cutToReceive, 'f', -1, 64)
			}else {
				maxCutClass4, _ := strconv.ParseFloat(MAX_CUT_CLASS4, 64)
				cutReceived, _ := strconv.ParseFloat(task.CutReceived, 64)
				cutToReceive := maxCutClass4 - cutReceived
				return true, strconv.FormatFloat(cutToReceive, 'f', -1, 64)
			}
	}
	return false, ""
}


//returns tasks higher than request class
func GetHigherTasks(w http.ResponseWriter, req *http.Request) {
	params := mux.Vars(req)
	requestClass := params["requestclass"]

	listTasks := make([]*Task, 0)

	if requestClass == "1" {
		locks["2"].Lock()
		listTasks = append(listTasks, classTasks["2"]...)
		locks["2"].Unlock()

		locks["3"].Lock()
		listTasks = append(listTasks, classTasks["3"]...)
		locks["3"].Unlock()

		locks["4"].Lock()
		listTasks = append(listTasks, classTasks["4"]...)
		locks["4"].Unlock()

	} else if requestClass == "2" {
		locks["3"].Lock()
		listTasks = append(listTasks, classTasks["3"]...)
		locks["3"].Unlock()

		locks["4"].Lock()
		listTasks = append(listTasks, classTasks["4"]...)
		locks["4"].Unlock()
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

	if requestClass == "2" {
		listTasks = append(listTasks, tasksToBeCut(classTasks["2"],hostClass)...)
		listTasks = append(listTasks, tasksToBeCut(classTasks["3"],hostClass)...)
		listTasks = append(listTasks, tasksToBeCut(classTasks["4"],hostClass)...)
	} else if requestClass == "3" {
		listTasks = append(listTasks, tasksToBeCut(classTasks["3"],hostClass)...)
		listTasks = append(listTasks, tasksToBeCut(classTasks["4"],hostClass)...)

	} else if requestClass == "4" {
		listTasks = append(listTasks, tasksToBeCut(classTasks["4"],hostClass)...)
	}
	json.NewEncoder(w).Encode(listTasks)
}


func CreateTask(w http.ResponseWriter, req *http.Request) {
	params := mux.Vars(req)
	var task Task
	_ = json.NewDecoder(req.Body).Decode(&task)
	requestClass := params["requestclass"]

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
	fmt.Println("Updating both")

	params := mux.Vars(req)
	taskID := params["taskid"]
	cpuUpdate := params["newcpu"]
	memoryUpdate := params["newmemory"]

	locks[tasks[taskID].TaskClass].Lock()

	tasks[taskID].CPUUtilization = cpuUpdate
	tasks[taskID].MemoryUtilization = memoryUpdate	
	locks[tasks[taskID].TaskClass].Unlock()		

	go UpdateTotalResourcesUtilization(cpuUpdate, memoryUpdate, 1, taskID) 
}

//function whose job is to check whether the total resources should be updated or not.
func UpdateTotalResourcesUtilization(cpu string, memory string, updateType int, taskID string){
	locks[tasks[taskID].TaskClass].Lock()
	previousTotalResourceUtilization := tasks[taskID].TotalResourcesUtilization
	afterTotalResourceUtilization := ""

	fmt.Println("Updating total resources utilization of " + taskID + " previous value " + previousTotalResourceUtilization)

	switch updateType {
		case 1:
			newCPU,_ := strconv.ParseFloat(cpu,64)
			newMemory, _ := strconv.ParseFloat(memory, 64)
			afterTotalResourceUtilization = strconv.FormatFloat(math.Max(newCPU, newMemory), 'f',-1, 64)
			tasks[taskID].TotalResourcesUtilization = afterTotalResourceUtilization
			break
		case 2:
			newCPU,_ := strconv.ParseFloat(cpu,64)
			memory,_ := strconv.ParseFloat(tasks[taskID].MemoryUtilization, 64)
			afterTotalResourceUtilization = strconv.FormatFloat(math.Max(newCPU, memory), 'f',-1, 64)
			tasks[taskID].TotalResourcesUtilization = afterTotalResourceUtilization
			break
		case 3:
			newMemory, _ := strconv.ParseFloat(memory, 64)
			cpu,_ := strconv.ParseFloat(tasks[taskID].CPUUtilization, 64)
			afterTotalResourceUtilization = strconv.FormatFloat(math.Max(cpu, newMemory), 'f',-1, 64)
			tasks[taskID].TotalResourcesUtilization = afterTotalResourceUtilization
			break
	}
	fmt.Println("Updating total resources utilization of " + taskID + " new value " + afterTotalResourceUtilization)

	locks[tasks[taskID].TaskClass].Unlock()

	//now we must check if the host region should be updated or not
	if afterTotalResourceUtilization != previousTotalResourceUtilization { 
		go UpdateList(taskID) //we going to update the task position inside its list		
	}
}

func UpdateList(taskID string) {
	//this deletes
	taskClass := tasks[taskID].TaskClass	

	locks[taskClass].Lock()
	fmt.Println("Updating task list, list elements: " + taskID)

	for i := 0; i < len(classTasks[taskClass]); i++ {
		fmt.Println(classTasks[taskClass][i])
		if classTasks[taskClass][i].TaskID == taskID {
			classTasks[taskClass] = append(classTasks[taskClass][:i], classTasks[taskClass][i+1:]...)
			break
		}
	}

	fmt.Println("before new list ")
	for i := 0; i < len(classTasks[taskClass]); i++ {
		fmt.Println(classTasks[taskClass][i])
	}

	//this inserts in the list in its new position
	index := Sort(classTasks[taskClass], tasks[taskID].TotalResourcesUtilization)		
	classTasks[taskClass] = InsertTask(classTasks[taskClass], index, tasks[taskID])

	fmt.Println("after new list ")
	for i := 0; i < len(classTasks[taskClass]); i++ {
		fmt.Println(classTasks[taskClass][i])
	}

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
	fmt.Println("Updating cpu")

	params := mux.Vars(req)
	taskID := params["taskid"]
	cpuUpdate := params["newcpu"]

    locks[tasks[taskID].TaskClass].Lock()
    tasks[taskID].CPUUtilization = cpuUpdate
    locks[tasks[taskID].TaskClass].Unlock()     

    go UpdateTotalResourcesUtilization(cpuUpdate, "0", 2, taskID) 	
}
//updates memory. message received from energy monitors. 
func UpdateMemory(w http.ResponseWriter, req *http.Request) {
	fmt.Println("Updating memory")

	params := mux.Vars(req)
	taskID := params["taskid"]
	memoryUpdate := params["newmemory"]

    locks[tasks[taskID].TaskClass].Lock()
    tasks[taskID].MemoryUtilization = memoryUpdate  
    locks[tasks[taskID].TaskClass].Unlock()     

    go UpdateTotalResourcesUtilization("0", memoryUpdate, 3, taskID) 
}

func main() {
	tasks = make(map[string]*Task)
	locks = make(map[string]*sync.Mutex)
	classTasks = make(map[string][]*Task)

	locks["1"] = &sync.Mutex{}
	locks["2"] = &sync.Mutex{}
	locks["3"] = &sync.Mutex{}
	locks["4"] = &sync.Mutex{}

	ServeSchedulerRequests()
}

func ServeSchedulerRequests() {
	router := mux.NewRouter()

	/*	class1Tasks = append(class1Tasks, Task{TaskID: "1", TaskClass: "1"})
		class1Tasks = append(class1Tasks, Task{TaskID: "2", TaskClass: "1"})
		class1Tasks = append(class1Tasks, Task{TaskID: "3", TaskClass: "1"})
		class1Tasks = append(class1Tasks, Task{TaskID: "4", TaskClass: "1"})
		class2Tasks = append(class2Tasks, Task{TaskID: "5", TaskClass: "2"})
		class2Tasks = append(class2Tasks, Task{TaskID: "6", TaskClass: "2"})
		class2Tasks = append(class2Tasks, Task{TaskID: "7", TaskClass: "2"})
		class2Tasks = append(class2Tasks, Task{TaskID: "8", TaskClass: "2"})
		class3Tasks = append(class3Tasks, Task{TaskID: "9", TaskClass: "3"})
		class3Tasks = append(class3Tasks, Task{TaskID: "10", TaskClass: "3"})
		class3Tasks = append(class3Tasks, Task{TaskID: "11", TaskClass: "3"})
		class3Tasks = append(class3Tasks, Task{TaskID: "12", TaskClass: "3"})
		class4Tasks = append(class4Tasks, Task{TaskID: "13", TaskClass: "4"})
		class4Tasks = append(class4Tasks, Task{TaskID: "14", TaskClass: "4"})
		class4Tasks = append(class4Tasks, Task{TaskID: "15", TaskClass: "4"})
		class4Tasks = append(class4Tasks, Task{TaskID: "16", TaskClass: "4"})
	*/
	router.HandleFunc("/task/{requestclass}", CreateTask).Methods("POST")
	router.HandleFunc("/task/highercut/{requestclass}", GetHigherTasksCUT).Methods("GET")
	router.HandleFunc("/task/higher/{requestclass}", GetHigherTasks).Methods("GET")
	router.HandleFunc("/task/equalhigher/{requestclass}&{hostclass}", GetEqualHigherTasks).Methods("GET")
	router.HandleFunc("/task/remove/{taskid}", RemoveTask).Methods("GET")
	router.HandleFunc("/task/updatetask/{taskclass}&{newcpu}&{newmemory}&{taskid}&{cutreceived}", UpdateTask).Methods("GET")
	router.HandleFunc("/task/class4", GetClass4Tasks).Methods("GET")
	router.HandleFunc("/task/updateboth/{taskid}&{newcpu}&{newmemory}", UpdateBoth).Methods("GET")
	router.HandleFunc("/task/updatecpu/{taskid}&{newcpu}", UpdateCPU).Methods("GET")
	router.HandleFunc("/task/updatememory/{taskid}&{newmemory}", UpdateMemory).Methods("GET")
	
	log.Fatal(http.ListenAndServe(getIPAddress()+":1234", router))
}

func getIPAddress() (string) {
    addrs, err := net.InterfaceAddrs()
    if err != nil {
        fmt.Println(err.Error())
    }
    for _, a := range addrs {
        if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
            if ipnet.IP.To4() != nil {
		    fmt.Println(ipnet.IP.String())
                    return ipnet.IP.String()
            }
        }
    }
    return ""
}


