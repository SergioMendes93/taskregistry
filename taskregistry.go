
package main

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"log"
	"net/http"
	"net"
)

type Task struct {
	TaskID     		string `json:"taskid,omitempty"`
	TaskClass    	string `json:"taskclass,omitempty"`
	Image		  	string `json:"image,omitempty"`
	CPU           	string `json:"cpu,omitempty"`
	TotalResources 	string `json:"totalresources,omitempty"` //total resouces allocated
	Memory      	string `json:"memory,omitempty"`
	TaskType    	string `json:"tasktype,omitempty"`
	CutReceived 	string `json:"cutreceived,omitempty"`
	CutToReceive 	string `json:"cuttorceieve,omitempty"`
}

var class1Tasks []Task
var class2Tasks []Task
var class3Tasks []Task
var class4Tasks []Task

var MAX_CUT_CLASS2 = "0"
var MAX_CUT_CLASS3 = "0"
var MAX_CUT_CLASS4 = "0"

//adapted binary search algorithm for inserting orderly based on total resources of a task
func Sort(classList []Task, searchValue string)(index int) {
	listLength := len(classList)
    lowerBound := 0
    upperBound := listLength- 1

    for {
        midPoint := (upperBound + lowerBound)/2

        fmt.Println(midPoint)
        if lowerBound > upperBound && classList[midPoint].TotalResources > searchValue {
            return midPoint 
        } else if lowerBound > upperBound {
            return midPoint + 1
        }

        if classList[midPoint].TotalResources < searchValue {
            lowerBound = midPoint + 1
        } else if classList[midPoint].TotalResources > searchValue {
             upperBound = midPoint - 1
        } else if classList[midPoint].TotalResources == searchValue {
            return midPoint
      	}
	}
}

//function used to remove the task once it finished
func RemoveTask(w http.ResponseWriter, req *http.Request) {

	params := mux.Vars(req)
	taskID := params["taskid"]

	for i, task := range class1Tasks {
		if task.TaskID == taskID {
			class1Tasks = append(class1Tasks[:i], class1Tasks[i+1:]...)
			return
		}
	}

	for i, task := range class2Tasks {
		if task.TaskID == taskID {
			class2Tasks = append(class2Tasks[:i], class2Tasks[i+1:]...)
			return
		}
	}
	for i, task := range class3Tasks {
		if task.TaskID == taskID {
			class3Tasks = append(class3Tasks[:i], class3Tasks[i+1:]...)
			return
		}
	}
	for i, task := range class4Tasks {
		if task.TaskID == taskID {
			class4Tasks = append(class4Tasks[:i], class4Tasks[i+1:]...)
			return
		}
	}
}

//this function will be used to update task info, when a cut is performed on the task
func UpdateTask(w http.ResponseWriter, req *http.Request) {
	params := mux.Vars(req)
	taskClass := params["taskclass"]
	taskID := params["taskid"]
	newCPU := params["newcpu"]
	newMemory := params["newmemory"]
	cutReceived := params["cutreceived"]	

	switch taskClass {
		case "1":
			for index, task := range class1Tasks {
				if task.TaskID == taskID {
					class1Tasks[index].CPU = newCPU
					class1Tasks[index].Memory = newMemory
					class1Tasks[index].CutReceived = cutReceived
				}
			}
			break
	case "2":
		for index, task := range class2Tasks {
			if task.TaskID == taskID {
				class2Tasks[index].CPU = newCPU
				class2Tasks[index].Memory = newMemory
				class2Tasks[index].CutReceived = cutReceived
			}
		}
		break
	case "3":
		for index, task := range class3Tasks {
			if task.TaskID == taskID {
				class3Tasks[index].CPU = newCPU
				class3Tasks[index].Memory = newMemory
				class3Tasks[index].CutReceived = cutReceived
			}
		}
		break
	case "4":
		for index, task := range class4Tasks {
			if task.TaskID == taskID {
				class4Tasks[index].CPU = newCPU
				class4Tasks[index].Memory = newMemory
				class4Tasks[index].CutReceived = cutReceived
			}
		}
		break
	}
}

func GetClass4Tasks(w http.ResponseWriter, req *http.Request) {
	json.NewEncoder(w).Encode(class4Tasks)	
}

//returns tasks higher than request class
func GetHigherTasksCUT(w http.ResponseWriter, req *http.Request) {
	params := mux.Vars(req)
	requestClass := params["requestclass"]

	listTasks := make([]Task, 0)

	/*
		In the code below we send the requestClass instead of hostClass because if this request gets scheduled to this host
		the hostClass will be the request class because we are in the case of HostClass >= requestClass. For example, if
		3 (HostClass) >= 2 (requestClass) if this request is scheduled to this host this host class will become 2 instead of 3.
		By sending requestClass we simulate if cutting whatever is on the host the request fits
*/
	if requestClass == "1" {
		listTasks = append(listTasks, tasksToBeCut(class2Tasks, requestClass)...)
		listTasks = append(listTasks, tasksToBeCut(class3Tasks, requestClass)...)
		listTasks = append(listTasks, tasksToBeCut(class4Tasks, requestClass)...)
	} else if requestClass == "2" {
		listTasks = append(listTasks, tasksToBeCut(class3Tasks, requestClass)...)
		listTasks = append(listTasks, tasksToBeCut(class4Tasks, requestClass)...)
	} else if requestClass == "3" {
		listTasks = append(listTasks, tasksToBeCut(class4Tasks, requestClass)...)
	}
	
	json.NewEncoder(w).Encode(listTasks)
}

func tasksToBeCut(listTasks []Task, hostClass string) ([]Task) {
	returnList := make([]Task, 0)
	
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
func taskCanBeCut(task Task, hostClass string) (bool, string) {
	switch task.TaskClass {
		case "2":
			if task.CutReceived == MAX_CUT_CLASS2 {
				return false, ""		//cannot cut this task, it is already expericing the maximum cut it can receive
			} else if hostClass == "2" { //if the host is class 2 and the task is class 2, we cannot cut the task because it would suffer twice the penalty. Because it is already feeling the penalty of the overbooking
				return false, ""
			} else {
				return true, MAX_CUT_CLASS2
			}		
		case "3":
			if task.CutReceived == MAX_CUT_CLASS3 {
				return false, ""
			} else if hostClass == "3" {
				return false, ""
			} else if hostClass == "2" { //it must received a smaller cut for the reasons mentioned in the report
				cutToReceive := strconv.ParseFloat(MAX_CUT_CLASS3,64) - strconv.ParseFloat(MAX_CUT_CLASS2,64)
				return true, strconv.FormatFloat(cutToReceive, 'f', -1, 64)
			} else {
				// Imaginando o caso em que está num host class 2 e este request sofreu 30% cut
				//mas depois este host passa a class 1. nao posso fazer cut full. tenho que fazer até preencher  até ficar full,
				//neste caso mais 20% ficando 50% cut
				cutToReceive := strconv.ParseFloat(MAX_CUT_CLASS3,64) - strconv.ParseFloat(task.CutReceived,64)
				return true, strconv.FormatFloat(cutToReceive, 'f', -1, 64)
			}
			
		case "4":
			if task.CutReceived == MAX_CUT_CLASS4 {
				return false, ""
			} else if hostClass == "4" {
				return false, ""
			} else if hostClass == "2" { //it must received a smaller cut for the reasons mentioned in the report
				cutToReceive := strconv.ParseFloat(MAX_CUT_CLASS4,64) - strconv.ParseFloat(MAX_CUT_CLASS2,64)
				return true, strconv.FormatFloat(cutToReceive, 'f', -1, 64)
			} else if hostClass == "3" {
				cutToReceive := strconv.ParseFloat(MAX_CUT_CLASS4,64) - strconv.ParseFloat(MAX_CUT_CLASS3,64)
				return true, strconv.FormatFloat(cutToReceive, 'f', -1, 64)
			}else {
				cutToReceive := strconv.ParseFloat(MAX_CUT_CLASS4,64) - strconv.ParseFloat(task.CutReceived,64)
				return true, strconv.FormatFloat(cutToReceive, 'f', -1, 64)
			}
	}
	return false, ""
}


//returns tasks higher than request class
func GetHigherTasks(w http.ResponseWriter, req *http.Request) {
	params := mux.Vars(req)
	requestClass := params["requestclass"]

	listTasks := make([]Task, 0)

	if requestClass == "1" {
		listTasks = append(listTasks, class2Tasks...)
		listTasks = append(listTasks, class3Tasks...)
		listTasks = append(listTasks, class4Tasks...)
	} else if requestClass == "2" {
		listTasks = append(listTasks, class3Tasks...)
		listTasks = append(listTasks, class4Tasks...)
	} else if requestClass == "3" {
		listTasks = append(listTasks, class4Tasks...)
	}
	fmt.Println(listTasks)
	json.NewEncoder(w).Encode(listTasks)
}

//returns tasks equal and higher than request class
func GetEqualHigherTasks(w http.ResponseWriter, req *http.Request) {
	params := mux.Vars(req)
	requestClass := params["requestclass"]
	hostClass := params["hostclass"]
	
	listTasks := make([]Task, 0)

	/*
	Here we send hostClass instead of requestClass because we are in the case of hostClass < requestClass so the class of the host 
	after the request is assigned to this host will remain the same (the value of hostClass)
*/

	if requestClass == "2" {
		listTasks = append(listTasks, tasksToBeCut(class2Tasks,hostClass)...)
		listTasks = append(listTasks, tasksToBeCut(class3Tasks,hostClass)...)
		listTasks = append(listTasks, tasksToBeCut(class4Tasks,hostClass)...)
	} else if requestClass == "3" {
		listTasks = append(listTasks, tasksToBeCut(class3Tasks,hostClass)...)
		listTasks = append(listTasks, tasksToBeCut(class4Tasks,hostClass)...)

	} else if requestClass == "4" {
		listTasks = append(listTasks, tasksToBeCut(class4Tasks,hostClass)...)
	}
	json.NewEncoder(w).Encode(listTasks)
}

func InsertTask(classTask []Task, index int, task Task) ([]Task) {
	tmp := make([]Task, 0)
	 if index >= len(classTask) {
    	tmp = append(tmp, classTask...)
       	tmp = append(tmp, task)
    } else {
        tmp = append(tmp, classTask[:index]...)
        tmp = append(tmp, task)
        tmp = append(tmp, classTask[index:]...)
    }
	return tmp
}

func CreateTask(w http.ResponseWriter, req *http.Request) {
	params := mux.Vars(req)
	var task Task
	_ = json.NewDecoder(req.Body).Decode(&task)
	requestClass := params["requestclass"]

	switch requestClass {
	case "1":
		if len(class1Tasks) == 0 {
			class1Tasks = append(class1Tasks, task)
			break
		}
		index := Sort(class1Tasks, task.TotalResources)
		class1Tasks = InsertTask(class1Tasks, index, task)
		break
	case "2":
		task.CutToReceive = MAX_CUT_CLASS2
		if len(class2Tasks) == 0 {
			class2Tasks = append(class2Tasks, task)
			break
		}

		index := Sort(class2Tasks, task.TotalResources)
		class2Tasks = InsertTask(class2Tasks, index, task)
		break
	case "3":
		task.CutToReceive = MAX_CUT_CLASS3
		if len(class3Tasks) == 0 {
			class3Tasks = append(class3Tasks, task)
			break
		}

		index := Sort(class3Tasks, task.TotalResources)
		class3Tasks = InsertTask(class3Tasks, index, task)
		break
	case "4":
		task.CutToReceive = MAX_CUT_CLASS4
		if len(class4Tasks) == 0 {
			class4Tasks = append(class4Tasks, task)
			break
		}

		index := Sort(class4Tasks, task.TotalResources)
		class4Tasks = InsertTask(class4Tasks, index, task)
		break
	}
}

func main() {
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

	log.Fatal(http.ListenAndServe("192.168.1.154:1234", router))
}

func getIPAddress() (string) {
    addrs, err := net.InterfaceAddrs()
    if err != nil {
        fmt.Println(err.Error())
    }
    i := 0 
    for _, a := range addrs {
        if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
            if ipnet.IP.To4() != nil {
                if i == 1 {
                    return ipnet.IP.String()
                }
                i++
            }
        }
    }
    return ""
}


