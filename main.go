package main

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/rs/xid"
)

var (
	port string = envOrDefault("PORT", "1993")
	// In minutes
	timeout time.Duration
)

type TaskType struct {
	id string
}

type Task struct {
	id            string
	taskType      TaskType
	inProgress    bool
	taskStartTime time.Time
}

type Worker struct {
	address       string // Should be unique
	listenPort    string
	taskType      TaskType
	workingOnTask *Task
}

type HeartbeatRequest struct {
	workerAddress   string
	responseChannel chan bool
}

func envOrDefault(key, def string) string {
	e := os.Getenv(key)
	if len(e) == 0 {
		return def
	}
	return e
}

func main() {
	t, err := strconv.Atoi(envOrDefault("TIMEOUT", "30"))
	if err != nil {
		timeout = 30
	} else {
		timeout = time.Duration(t)
	}
	ln, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	newTaskChan := make(chan Task)
	taskCompletedChan := make(chan string) // worker address
	newWorkerChan := make(chan Worker)
	removeWorkerChan := make(chan string) // worker address
	heartbeatRequestChannel := make(chan HeartbeatRequest)

	if err != nil {
		log.Fatalf("Failed to start server")
	}

	log.Printf("Starting queue handler routine")
	go handleQueue(
		newTaskChan,
		newWorkerChan,
		taskCompletedChan,
		removeWorkerChan,
		heartbeatRequestChannel,
	)

	log.Printf("Started listening to requests on port: %s", port)
	for {
		conn, err := ln.Accept()
		if err != nil {
			// TODO add something that makes sense here
			log.Print("Failed to accept connection")
		}
		go handleConnection(
			conn,
			newTaskChan,
			newWorkerChan,
			taskCompletedChan,
			removeWorkerChan,
			heartbeatRequestChannel,
		)
	}
}

func removeTask(
	task Task,
	queue *[]Task) {
	clone := make([]Task, len(*queue))
	copy(clone, *queue)
	index, found := findTaskIndex(task.id, clone)

	if found {
		*queue = append(clone[:index], clone[index+1:]...)
	}
}

func removeTaskAndFreeWorker(
	task Task,
	workers *[]Worker,
	queue *[]Task,
) {
	var workerIndex int
	var worker Worker
	workersClone := make([]Worker, len(*workers))
	copy(workersClone, *workers)
	found := false
	for i, w := range workersClone {
		if w.workingOnTask == nil {
			continue
		}
		if w.workingOnTask.id == task.id {
			workerIndex = i
			worker = w
			found = true
			break
		}
	}
	if found {
		worker.workingOnTask = nil
		workersClone[workerIndex] = worker
		*workers = workersClone
	}
	removeTask(task, queue)
}

func findTaskIndex(taskId string, queue []Task) (int, bool) {
	var index int
	found := false
	for i, t := range queue {
		if t.id == taskId {
			index = i
			found = true
			break
		}
	}
	return index, found
}

func findWorkerIndex(workerAddress string, workers []Worker) (int, bool) {
	var index int
	found := false
	for i, w := range workers {
		if workerAddress == w.address {
			index = i
			found = true
			break
		}
	}
	return index, found
}

func removeWorker(
	worker Worker,
	workers *[]Worker,
) {
	clone := make([]Worker, len(*workers))
	copy(clone, *workers)
	index, found := findWorkerIndex(worker.address, *workers)

	if found {
		*workers = append(clone[:index], clone[index+1:]...)
	}
}

func assignTaskToWorker(
	task Task,
	worker Worker,
	queue *[]Task,
	workers *[]Worker,
) {
	workersClone := make([]Worker, len(*workers))
	copy(workersClone, *workers)
	queueClone := make([]Task, len(*queue))
	copy(queueClone, *queue)

	workerIndex, workerFound := findWorkerIndex(worker.address, workersClone)
	taskIndex, taskFound := findTaskIndex(task.id, queueClone)

	if workerFound && taskFound {
		w := workersClone[workerIndex]
		t := queueClone[taskIndex]
		t.inProgress = true
		w.workingOnTask = &t
		queueClone[taskIndex] = t
		workersClone[workerIndex] = w
		*queue = queueClone
		*workers = workersClone
	}
}

func sendTaskToWorker(worker Worker, removeWorkerChan chan string) {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%s", worker.address, worker.listenPort))
	if err != nil {
		log.Printf("Failed to connect to worker %s, removing...", worker.address)
		// Remove worker if cant dial
		removeWorkerChan <- worker.address
		return
	}

	_, err = conn.Write([]byte("worker:start"))
	if err != nil {
		log.Printf("Failed to send message to worker: %s, removing...", worker.address)
		removeWorkerChan <- worker.address
		return
	}

	reply := make([]byte, 64)

	_, err = conn.Read(reply)

	if err != nil {
		log.Printf("Failed to receive response from worker: %s, removing...", worker.address)
		removeWorkerChan <- worker.address
		return
	}

	if string(bytes.Trim(reply, "\x00")) != "worker:ack" {
		log.Printf("Invalid response from worker: %s, removing...", worker.address)
		removeWorkerChan <- worker.address
		return
	}

}

func handleQueue(
	newTaskChan chan Task,
	newWorkerChan chan Worker,
	taskCompleteChan, removeWorkerChan chan string,
	heartbeatRequestChannel chan HeartbeatRequest,
) {
	var taskQueue []Task
	var workers []Worker

	for {
		log.Printf("Current tasks: %v", taskQueue)
		log.Printf("Current workers: %v", workers)
		select {
		case newTask := <-newTaskChan:
			log.Printf("Added new task: %s of type: %s", newTask.id, newTask.taskType.id)
			taskQueue = append(taskQueue, newTask)
		case workerAddress := <-taskCompleteChan:
			workerIndex, found := findWorkerIndex(workerAddress, workers)
			if found {
				worker := workers[workerIndex]
				task := worker.workingOnTask
				log.Printf("Completed task: %s", task.id)
				removeTaskAndFreeWorker(*task, &workers, &taskQueue)
			}

		case newWorker := <-newWorkerChan:
			_, found := findWorkerIndex(newWorker.address, workers)
			if !found {
				workers = append(workers, newWorker)
			}

		case workerAddress := <-removeWorkerChan:
			workerIndex, found := findWorkerIndex(workerAddress, workers)
			if found {
				worker := workers[workerIndex]
				removeWorker(worker, &workers)
				log.Printf("Disconnected worker: %s", worker.address)

			}
		case heartbeatRequest := <-heartbeatRequestChannel:
			_, found := findWorkerIndex(heartbeatRequest.workerAddress, workers)
			heartbeatRequest.responseChannel <- found
		}
		// Always after any channel emits, try to find workers for tasks
		for _, task := range taskQueue {
			// Check if task has timedout otherwise ignore
			if task.inProgress {
				timeoutTime := task.taskStartTime.Add(timeout * time.Minute)

				if time.Now().After(timeoutTime) {
					// Remove task and free
					removeTaskAndFreeWorker(task, &workers, &taskQueue)
				}
				continue
			}
			// Check if any workers avaiable for TaskType
			taskType := task.taskType
			for _, w := range workers {
				if w.taskType.id != taskType.id {
					continue
				}
				if w.workingOnTask != nil {
					continue
				}
				// Found free worker
				assignTaskToWorker(task, w, &taskQueue, &workers)
				// Send request to worker
				go sendTaskToWorker(w, removeWorkerChan)
			}

		}

	}
}

func stringSliceContains(slice []string, str string) bool {
	for _, item := range slice {
		if item == str {
			return true
		}
	}
	return false
}

func handleWorkerRequest(
	conn net.Conn,
	message []string,
	taskCompleteChan chan string,
	newWorkerChan chan Worker,
	removeWorkerChan chan string,
	heartbeatRequestChannel chan HeartbeatRequest,
) {
	allowedActions := []string{"join", "disconnect", "ready", "heartbeat"}

	action := message[1]
	if !stringSliceContains(allowedActions, action) {
		log.Printf("Invalid client action: %s", action)
		conn.Write([]byte("Invalid action"))
		conn.Close()
		return
	}

	if action == "join" {
		if len(message) < 4 {
			log.Printf("Invalid message")
			conn.Write([]byte("Invalid message"))
			conn.Close()
			return
		}

		taskTypeId := message[2]
		workerListenPort := message[3]

		newWorker := Worker{
			taskType: TaskType{
				id: taskTypeId,
			},
			address:       strings.Split(conn.RemoteAddr().String(), ":")[0],
			listenPort:    workerListenPort,
			workingOnTask: nil,
		}
		newWorkerChan <- newWorker

		conn.Write([]byte("success"))
		conn.Close()
		return

	}

	if action == "disconnect" {
		removeWorkerChan <- strings.Split(conn.RemoteAddr().String(), ":")[0]
		conn.Write([]byte("success"))
		conn.Close()
		return
	}

	if action == "ready" {
		taskCompleteChan <- strings.Split(conn.RemoteAddr().String(), ":")[0]
		conn.Write([]byte("success"))
		conn.Close()
		return
	}

	if action == "heartbeat" {
		responseChannel := make(chan bool)
		heartbeatRequestChannel <- HeartbeatRequest{
			workerAddress:   strings.Split(conn.RemoteAddr().String(), ":")[0],
			responseChannel: responseChannel,
		}
		found := <-responseChannel

		if found {
			conn.Write([]byte("success"))
			conn.Close()
			return
		} else {
			conn.Write([]byte("Worker is not connected"))
			conn.Close()
			return
		}
	}
}

func handleClientRequest(
	conn net.Conn,
	message []string,
	newTaskChan chan Task,
) {
	action := message[1]
	if action != "task" {
		log.Printf("Invalid client action: %s", action)
		conn.Write([]byte("Invalid action"))
		conn.Close()
		return
	}
	if len(message) < 3 {
		log.Printf("Invalid message")
		conn.Write([]byte("Invalid message"))
		conn.Close()
		return
	}
	taskTypeId := message[2]

	taskId := xid.New().String()
	newTask := Task{
		id: taskId,
		taskType: TaskType{
			id: taskTypeId,
		},
		inProgress: false,
	}

	newTaskChan <- newTask

	conn.Write([]byte("success"))
	conn.Close()

}

func handleConnection(
	conn net.Conn,
	newTaskChan chan Task,
	newWorkerChan chan Worker,
	taskCompleteChan, removeWorkerChan chan string,
	heartbeatRequestChannel chan HeartbeatRequest,
) {

	buf := make([]byte, 64)

	_, err := conn.Read(buf)
	if err != nil {
		log.Printf("Error reading: %s", err.Error())
		conn.Write([]byte("Error reading message"))
		conn.Close()
		return
	}

	str := string(bytes.Trim(buf, "\x00"))

	log.Printf("Received: %s", str)

	message := strings.Split(str, ":")

	if len(message) < 2 || len(message) > 4 {
		log.Printf("Message malformed: %s", str)
		conn.Write([]byte("Invalid message"))
		conn.Close()
		return
	}

	// Check message connection header
	header := message[0]
	if header == "worker" {
		handleWorkerRequest(
			conn,
			message,
			taskCompleteChan,
			newWorkerChan,
			removeWorkerChan,
			heartbeatRequestChannel,
		)
	} else if header == "client" {
		handleClientRequest(conn, message, newTaskChan)
	} else {
		log.Printf("Message header invalid: %s", header)
		conn.Write([]byte("Message header invalid"))
		conn.Close()

	}

}
