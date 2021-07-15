# Protocol

Suuuper easy protocol babyyyyy

Text based tcp protocol, ezpz parsing

All requests to server respond with `success` if successful, otherwise with an error message

Workers:

Worker initiated requests:

- Register as worker: send `worker:join:{tasktypeid}:{workerListenPort}`
- Disconnect as worker: send `worker:disconnect`
- Task completed: send `worker:ready`
- Heartbeat to check if worker still registered: send `worker:heartbeat`

Task queue iniated requests:

- Listen to message: `worker:start` (on receive send ack and start your work)
- Ack task received: send `worker:ack` (this should be your reply to worker:start, if you dont respond with this the worker will be removed and a reconnect will be necessary)

Clients:

- Queue new task: send `client:task:{tasktypeid}`

For clients its a fire and forget kind of deal. If you want some kind of pub sub for when tasks are complete use redis or something, that will work. Easy task queue is only for really easily creating asynchronous tasks with multiple machines (or most likely containers)

For workers it's also pretty easy. Just listen to messages and do work, the easy queue will take care of everything else and workers will receive more jobs only if they have sent a task-ready message

# Enviroment

PORT defaults to 1993
TIMEOUT (in minutes) defaults to 30
