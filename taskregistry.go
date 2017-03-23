
package main

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"log"
	"net/http"
)

type Task struct {
	TaskID      string `json:"taskid,omitempty"`
	TaskClass   string `json:"taskclass,omitempty"`
	Image		string `json:"image,omitempty"`
	CPU         string `json:"cpu,omitempty"`
	Memory      string `json:"memory,omitempty"`
	TaskType    string `json:"tasktype,omitempty"`
	CutReceived string `json:"cutreceived,omitempty"`
}

var class1Tasks []Task
var class2Tasks []Task
var class3Tasks []Task
var class4Tasks []Task

//this function will be used to update task info, when a cut is performed on the task
func UpdateTask(w http.ResponseWriter, req *http.Request) {
	params := mux.Vars(req)
	taskClass := params["taskclass"]
	taskID := params["taskid"]
	newCPU := params["newcpu"]
	newMemory := params["newmemory"]

	fmt.Println("novo update")

	switch taskClass {
	case "1":
		for index, task := range class1Tasks {
			if task.TaskID == taskID {
				class1Tasks[index].CPU = newCPU
				class1Tasks[index].Memory = newMemory
			}
		}
		break
	case "2":
		for index, task := range class2Tasks {
			if task.TaskID == taskID {
				class2Tasks[index].CPU = newCPU
				class2Tasks[index].Memory = newMemory
			}
		}
		break
	case "3":
		for index, task := range class3Tasks {
			if task.TaskID == taskID {
				class3Tasks[index].CPU = newCPU
				class3Tasks[index].Memory = newMemory
			}
		}
		break
	case "4":
		for index, task := range class4Tasks {
			if task.TaskID == taskID {
				class4Tasks[index].CPU = newCPU
				class4Tasks[index].Memory = newMemory
			}
		}
		break
	}
}

func GetClass4Tasks(w http.ResponseWriter, req *http.Request) {
	json.NewEncoder(w).Encode(class4Tasks)	
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

	listTasks := make([]Task, 0)

	if requestClass == "2" {
		listTasks = append(listTasks, class2Tasks...)
		listTasks = append(listTasks, class3Tasks...)
		listTasks = append(listTasks, class4Tasks...)
	} else if requestClass == "3" {
		listTasks = append(listTasks, class3Tasks...)
		listTasks = append(listTasks, class4Tasks...)
	} else if requestClass == "4" {
		listTasks = append(listTasks, class4Tasks...)
	}
	json.NewEncoder(w).Encode(listTasks)
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
			fmt.Println("after removing")
			fmt.Println(class4Tasks)
			return
		}
	}
}

func CreateTask(w http.ResponseWriter, req *http.Request) {
	params := mux.Vars(req)
	var task Task
	_ = json.NewDecoder(req.Body).Decode(&task)
	requestClass := params["requestclass"]

	fmt.Println("Task created")
	fmt.Println(task)

	switch requestClass {
	case "1":
		class1Tasks = append(class1Tasks, task)
		break
	case "2":
		class2Tasks = append(class2Tasks, task)
		break
	case "3":
		class3Tasks = append(class3Tasks, task)
		break
	case "4":
		class4Tasks = append(class4Tasks, task)
		break
	}
	fmt.Println(class4Tasks)
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
	router.HandleFunc("/task/higher/{requestclass}", GetHigherTasks).Methods("GET")
	router.HandleFunc("/task/equalhigher/{requestclass}", GetEqualHigherTasks).Methods("GET")
	router.HandleFunc("/task/remove/{taskid}", RemoveTask).Methods("GET")
	router.HandleFunc("/task/updatetask/{taskclass}&{newcpu}&{newmemory}&{taskid}", UpdateTask).Methods("GET")
	router.HandleFunc("/task/class4", GetClass4Tasks).Methods("GET")

	log.Fatal(http.ListenAndServe("192.168.1.154:1234", router))
}
