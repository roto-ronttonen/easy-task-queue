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
	address               string // Should be unique
	listenPort            string
	taskType              TaskType
	workingOnTask         *Task
	lastHeartbeatReceived time.Time
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
	index, found := findTaskIndex(task.id, queue)

	if found {
		*queue = append((*queue)[:index], (*queue)[index+1:]...)
	}
}

func removeTaskAndFreeWorker(
	task Task,
	workers *[]Worker,
	queue *[]Task,
) {
	var workerIndex int
	var worker Worker
	found := false
	for i, w := range *workers {
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
		(*workers)[workerIndex] = worker
	}
	removeTask(task, queue)
}

func removeTaskAndWorker(
	task Task,
	workers *[]Worker,
	queue *[]Task,
) {
	var worker Worker
	found := false
	for _, w := range *workers {
		if w.workingOnTask == nil {
			continue
		}
		if w.workingOnTask.id == task.id {
			worker = w
			found = true
			break
		}
	}
	if found {
		removeWorker(worker, workers)
	}
	removeTask(task, queue)
}

func findTaskIndex(taskId string, queue *[]Task) (int, bool) {
	var index int
	found := false
	for i, t := range *queue {
		if t.id == taskId {
			index = i
			found = true
			break
		}
	}
	return index, found
}

func findWorkerIndex(workerAddress string, workers *[]Worker) (int, bool) {
	var index int
	found := false
	for i, w := range *workers {
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
	index, found := findWorkerIndex(worker.address, workers)

	if found {
		*workers = append((*workers)[:index], (*workers)[index+1:]...)
	}
}

func assignTaskToWorker(
	task Task,
	worker Worker,
	queue *[]Task,
	inProgressTasks *[]Task,
	workers *[]Worker,
) {

	workerIndex, workerFound := findWorkerIndex(worker.address, workers)
	taskIndex, taskFound := findTaskIndex(task.id, queue)

	if workerFound && taskFound {
		w := (*workers)[workerIndex]
		t := (*queue)[taskIndex]
		t.inProgress = true
		t.taskStartTime = time.Now()
		w.workingOnTask = &t
		*inProgressTasks = append(*inProgressTasks, t)
		removeTask(t, queue)
		(*workers)[workerIndex] = w
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

func handleTimeouts(tasksInProgress *[]Task, workers *[]Worker) {
	for _, task := range *tasksInProgress {
		timeoutTime := task.taskStartTime.Add(timeout * time.Minute)

		if time.Now().After(timeoutTime) {
			log.Printf("Removing task because of timeout: %s", task.id)
			removeTaskAndWorker(task, workers, tasksInProgress)
		}
	}
}

func handleHeartbeatTimeouts(workers *[]Worker) {
	for _, worker := range *workers {
		if time.Now().After(worker.lastHeartbeatReceived.Add(1 * time.Minute)) {
			log.Printf("Removing worker because no heartbeat: %s", worker.address)
			removeWorker(worker, workers)
		}
	}
}

func handleQueue(
	newTaskChan chan Task,
	newWorkerChan chan Worker,
	taskCompleteChan, removeWorkerChan chan string,
	heartbeatRequestChannel chan HeartbeatRequest,
) {
	var taskQueue, tasksInProgress []Task
	var workers []Worker

	for {
		if len(taskQueue) > 30 {
			log.Printf("First 30 tasks in queue: %v", taskQueue[:30])
		} else {
			log.Printf("Current tasks in queue: %v", taskQueue)
		}
		log.Printf("Num tasks in queue: %d", len(taskQueue))
		if len(tasksInProgress) > 30 {
			log.Printf("First 30 tasks in progress: %v", tasksInProgress[:30])
		} else {
			log.Printf("Current tasks in progress: %v", tasksInProgress)
		}
		log.Printf("Num tasks in progress: %d", len(tasksInProgress))
		if len(workers) > 30 {
			log.Printf("First 30 workers: %v", workers[:30])
		} else {
			log.Printf("Current workers: %v", workers)
		}
		log.Printf("Num workers: %d", len(workers))

		select {
		case newTask := <-newTaskChan:
			log.Printf("Added new task: %s of type: %s", newTask.id, newTask.taskType.id)
			taskQueue = append(taskQueue, newTask)
		case workerAddress := <-taskCompleteChan:
			workerIndex, found := findWorkerIndex(workerAddress, &workers)
			if found {
				worker := workers[workerIndex]
				task := worker.workingOnTask
				if task == nil {
					log.Printf("Tried to complete non existent task on worker: %s", worker.address)
				} else {
					log.Printf("Completed task: %s", task.id)
					removeTaskAndFreeWorker(*task, &workers, &tasksInProgress)
				}

			}

		case newWorker := <-newWorkerChan:
			_, found := findWorkerIndex(newWorker.address, &workers)
			if !found {
				workers = append(workers, newWorker)
			}

		case workerAddress := <-removeWorkerChan:
			workerIndex, found := findWorkerIndex(workerAddress, &workers)
			if found {
				worker := workers[workerIndex]
				if worker.workingOnTask != nil {
					taskIndex, found := findTaskIndex(worker.workingOnTask.id, &tasksInProgress)
					if found {
						task := tasksInProgress[taskIndex]
						task.inProgress = false
						// Prepend to taskqueue
						taskQueue = append([]Task{task}, taskQueue...)
						removeTask(*worker.workingOnTask, &tasksInProgress)
					}
				}
				removeWorker(worker, &workers)
				log.Printf("Disconnected worker: %s", worker.address)

			}
		case heartbeatRequest := <-heartbeatRequestChannel:
			index, found := findWorkerIndex(heartbeatRequest.workerAddress, &workers)
			if found {
				workers[index].lastHeartbeatReceived = time.Now()
			}
			heartbeatRequest.responseChannel <- found
		}
		// Always after any channel emits, try to find workers for tasks

		// NEW VERY FAST ALGORITHM

		// Check timedout tasks
		handleTimeouts(&tasksInProgress, &workers)
		// Check timedout workers
		handleHeartbeatTimeouts(&workers)

		// Find free workers
		// This is better because in sensible cases there are more tasks than workers
		// And if not then there isn't much load anyways
		var freeWorkers []Worker
		for _, worker := range workers {
			if worker.workingOnTask == nil {
				freeWorkers = append(freeWorkers, worker)
			}
		}

		// Loop throught free workers and find a task for them
		for _, freeWorker := range freeWorkers {
			taskType := freeWorker.taskType.id
			// This part might require optimization some day
			// But thankfully it doesn't run on every request like previously
			for _, task := range taskQueue {
				if task.taskType.id == taskType {
					assignTaskToWorker(task, freeWorker, &taskQueue, &tasksInProgress, &workers)
					go sendTaskToWorker(freeWorker, removeWorkerChan)
					break
				}
			}
		}

		// OLD VERY SLOW ALGORITHM (also doesnt work anymore with addition of tasksInProgress slice)
		// for _, task := range taskQueue {
		// 	// Check if task has timedout otherwise ignore
		// 	if task.inProgress {
		// 		timeoutTime := task.taskStartTime.Add(timeout * time.Minute)

		// 		if time.Now().After(timeoutTime) {
		// 			// Remove task and free
		// 			removeTaskAndFreeWorker(task, &workers, &taskQueue)
		// 		}
		// 		continue
		// 	}
		// 	// Check if any workers avaiable for TaskType
		// 	taskType := task.taskType
		// 	for _, w := range workers {
		// 		if w.taskType.id != taskType.id {
		// 			continue
		// 		}
		// 		if w.workingOnTask != nil {
		// 			continue
		// 		}

		// 		taskIsFree := true

		// 		// Check no other worker is working on this task
		// 		for _, ww := range workers {
		// 			if ww.address == w.address {
		// 				continue
		// 			}
		// 			if ww.workingOnTask == nil {
		// 				continue
		// 			}

		// 			if ww.workingOnTask.id == task.id {
		// 				taskIsFree = false
		// 				break
		// 			}
		// 		}

		// 		if taskIsFree {
		// 			// Found free worker
		// 			assignTaskToWorker(task, w, &taskQueue, &workers)
		// 			// Send request to worker
		// 			go sendTaskToWorker(w, removeWorkerChan)
		// 		}

		// 	}

		// }

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
			address:               strings.Split(conn.RemoteAddr().String(), ":")[0],
			listenPort:            workerListenPort,
			workingOnTask:         nil,
			lastHeartbeatReceived: time.Now(),
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

	connectionStartTime := time.Now()

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

	log.Printf("Message %s, handled in time %s", str, time.Since(connectionStartTime))

}
