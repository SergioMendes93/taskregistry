package main

import (
	"encoding/json"
	"log"
	"fmt"
	"net/http"
	"github.com/gorilla/mux"
)

type Task struct {
	TaskID string				`json:"taskid,omitempty"`
	AllocatedResources string	`json:"allocatedresources,omitempty"`
	TaskType string				`json:"tasktype,omitempty"`
	CutReceived string			`json:"cutreceived,omitempty"`
}

var class1Tasks []Task
var class2Tasks []Task
var class3Tasks []Task
var class4Tasks []Task

func CreateTask(w http.ResponseWriter, req *http.Request) {
	params := mux.Vars(req)
	var task Task
	_ = json.NewDecoder(req.Body).Decode(&task)
	requestClass := params["requestclass"]

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
}

func main() {
	ServeSchedulerRequests()
}

func ServeSchedulerRequests() {
	router := mux.NewRouter()

	router.HandleFunc("/task/{requestclass}",CreateTask).Methods("POST")
	
	log.Fatal(http.ListenAndServe("192.168.1.154:1234",router))
}
