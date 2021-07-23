# How to start

First clone this repository with `git clone`

Then inside cloned folder run `go build`

On linux run `./easy-task-queue`

Don't know about other systems, but go builds an executable so start how you would start an executable

Also has a dockerfile you can build

Also has a docker image published at `rotoronttonen/easy-task-queue` you can pull

# Enviroment

PORT defaults to 1993
TIMEOUT (in minutes) defaults to 30

# Protocol

Suuuper easy protocol babyyyyy

Text based tcp protocol, ezpz parsing

All requests to server respond with `success` if successful, otherwise with an error message

Workers:

Worker initiated requests:

- Register as worker: send `worker:join:{tasktypeid}:{workerListenPort}`
- Disconnect as worker: send `worker:disconnect`
- Task completed: send `worker:ready`
- Heartbeat to check if worker still registered (heartbeat is required every 1 minute): send `worker:heartbeat`

Task queue iniated requests:

- Listen to message: `worker:start:{optionalData}` (on receive send ack and start your work)
- Ack task received: send `worker:ack` (this should be your reply to worker:start, if you dont respond with this the worker will be removed and a reconnect will be necessary)

Clients:

- Queue new task: send `client:task:{tasktypeid}:{optionalData}`

For clients its a fire and forget kind of deal. If you want some kind of pub sub for when tasks are complete use redis or something, that will work. Easy task queue is only for really easily creating asynchronous tasks with multiple machines (or most likely containers)

For workers it's also pretty easy. Just listen to messages and do work, the easy queue will take care of everything else and workers will receive more jobs only if they have sent a task-ready message

Optional data defaults to empty string
