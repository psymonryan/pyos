# The Python Coroutine Operating System
#
# Based on Dave Beazley's pyos8 2008
# Further implemented by Simon Ryan 2013,2014

import types
import socket
import time
import sys
from Queue import Queue
import select
import heapq
import pprint
pp=pprint.PrettyPrinter()

# This function allows us to gracefully send exception details to the log
import traceback
def formatExceptionInfo(maxTBlevel=5):
     cla, exc, trbk = sys.exc_info()
     excName = cla.__name__
     try:
         excArgs = exc.__dict__["args"]
     except KeyError:
         excArgs = "<no args>"
     excTb = traceback.format_tb(trbk, maxTBlevel)
     return pp.pformat((excName, excArgs, excTb))

class Task(object):
    # warning class variables are not thread safe
    taskid = 0
    def __init__(self,target):
        # Increment the class variable so we get a unique one for each Task
        Task.taskid += 1
        self.tid     = Task.taskid   # Task ID
        self.target  = target        # Target coroutine
        self.sendval = None          # Value to send
        self.stack   = []            # Call stack

    # Run a task until it hits the next yield statement
    def run(self):
        while True:
            try:
                # run the target (either the generator we were initialised with or something we popped off the stack)
                result = self.target.send(self.sendval)
                if isinstance(result,SystemCall): return result
                if isinstance(result,types.GeneratorType):
                    self.stack.append(self.target)
                    self.sendval = None
                    self.target  = result
                else:
                    # If it not a 'subroutine' generator or a System call then its an actual value, so just return
                    if not self.stack: return
                    # there was some more generators on the stack so we want to pass the result upwards
                    # since we must be in a 'subroutine'
                    self.sendval = result
                    self.target  = self.stack.pop()
            except StopIteration:
                if not self.stack: raise
                self.sendval = None
                self.target = self.stack.pop()

class Scheduler(object):
    def __init__(self):
        # A queue of tasks ready to be run
        self.ready   = Queue()   
        # A dict to allow us to find a task by its task id
        self.taskmap = {}        

        # These dicts hold tasks that are no longer in the ready queue
        # They will be placed back under various conditions
        # Tasks waiting for other tasks to exit
        self.exit_waiting = {}
        # I/O waiting
        self.read_waiting = {}
        self.write_waiting = {}
        # Tasks sleeping
        self.sleep_waiting = []
        # Messages - each entry holds a list of tids subscribed to the message
        self.messagesubscribers = {}
        # Message Queue - each entry looks like (message, params)
        self.mqueue = Queue()
        
    def new(self,target):
        # Create a new task object (we wrap a coroutine function in the Task class to make it a schedulable task)
        newtask = Task(target)
        self.taskmap[newtask.tid] = newtask
        # Place it in the ready queue so it will be executed
        self.schedule(newtask)
        # the caller gets to know the task id
        return newtask.tid

    def exit(self,task):
        if not task.tid in self.taskmap:
            print('killing non-existant task',task.tid)
            return
        del self.taskmap[task.tid]
        # Notify other tasks waiting for exit
        # This removes the dict entry for this tid and 
        # task iterates over the list of tasks that were waiting and re-schedules them
        # the list of these tasks is removed (popped) out of the exit_waiting dict
        for task in self.exit_waiting.pop(task.tid,[]):
            self.schedule(task)

    def waitforexit(self,task,waittid):
        if waittid in self.taskmap:
            # If we are still in the taskmap then keep waiting
            self.exit_waiting.setdefault(waittid,[]).append(task)
            return True
        else:
            # indicate the task has already exited
            return False

    # I/O waiting
    def waitforread(self,task,fd):
        self.read_waiting[fd] = task

    def waitforwrite(self,task,fd):
        self.write_waiting[fd] = task

    def iopoll(self,timeout):
        if self.read_waiting or self.write_waiting:
           # Select only the file descriptors that are ready
           r,w,e = select.select(self.read_waiting,
                                 self.write_waiting,[],timeout)
           # iterate over these ready descriptors and re-schedule the tasks that were waiting
           for fd in r: self.schedule(self.read_waiting.pop(fd))
           for fd in w: self.schedule(self.write_waiting.pop(fd))
           if r or w: self.schedule(None)
        else:
            # If we have no read or write sockets to select on, then lets sleep either the timeout or our minimum time granularity
            if 0.1 < timeout:
                time.sleep(0.1)
            # If the timout is less than or equal to our minimum granularity then just sleep this amount
            elif timeout > 0:
                time.sleep(timeout)
            # If we are told to sleep for 0 then sleep for the minimum instead
            elif self.mqueue.empty():
                time.sleep(0.1)

    # Time waiting
    def waitforclock(self,task,clocktime):
        # Add task to our sleep_waiting queue. 
        # Using heappush means we auto sort on push so that the next task is ready to pop
        heapq.heappush(self.sleep_waiting,(clocktime,task))

    def timepoll(self):
        # Process all items waiting to expire
        woken = False
        # while there is something there, and the last item is ready to run
        while self.sleep_waiting and self.sleep_waiting[0][0] <= time.time():
            expiredtask = heapq.heappop(self.sleep_waiting)
            if expiredtask[1].tid in self.taskmap:
                self.schedule(expiredtask[1])
                woken = True
            else:
                print('scheduled task already exited',expiredtask[1].tid)
        if woken:
            self.schedule(None)
    
    # Go Idle
    def goidle(self,task):
        pass

    # Messaging
    def sendmessage(self,message):
        # Insert the given message into the message queue
        self.mqueue.put(message)

    def processmessage(self):
        message = self.mqueue.get(False)
        key = message[1]
        if not key in self.messagesubscribers:
            print('got unknown message:',message)
        else:
            for tid in self.messagesubscribers[key]:
                task = tid in self.taskmap and self.taskmap[tid]
                if task:
                    task.sendval = message
                    self.schedule(task)

    def subscribemessage(self,messagekey,tid):
        self.messagesubscribers.setdefault(messagekey,[]).append(tid)

    # Scheduling
    def schedule(self,task):
        # just place the task in the queue
        self.ready.put(task)

    def processready(self):
        # Process all the ready tasks or until we hit a None boundary
        while 1:
            if self.ready.empty():
                break
            task = self.ready.get(False)
            if task == None:
                # Since some tasks may generate more tasks, we need boundaries so the mainloop can continue
                break
            try:
                result = task.run()
                if isinstance(result,SystemCall):
                    result.task  = task
                    result.sched = self
                    result.handle()
                    continue
            except StopIteration:
                # If we reach the 'end' of the stream of values from a generator, we exit the task
                self.exit(task)
                continue
            except:
                # Other types of exceptions just cause the task to exit with some logging so we know why
                print('Task exiting with error: %s' % formatExceptionInfo())
                self.exit(task)
                continue
            # Re-schedule the task now that we have successfully run it
            self.schedule(task)

    # Task select polling loop
    def mainloop(self):
        while 1:

            # Process tasks if we have some
            if not self.ready.empty():
                self.processready()

            # Process io quickly or sit and select until the next sleeping task is due to wake
            if self.ready.empty():
                # if there are no tasks in the ready queue and there is nothing sleeping then set timeout to 0 to indicate 'use minumim granularity'
                if not self.sleep_waiting:
                    self.iopoll(0)
                else:
                    # We can timeout for as long as the next sleep_waiting task would sleep
                    waiting = self.sleep_waiting[0][0] - time.time()
                    if waiting >= 0:
                        self.iopoll(waiting)
                    else:
                        self.iopoll(0)
            else:
                # Otherwise since we have waiting tasks, just return asap
                self.iopoll(0)

            # Check to see if any sleeping tasks can be woken
            if self.sleep_waiting:
                self.timepoll()
                # immediately process these new readytasks so nothing can overwrite the sendval
                self.processready()

            # Check to see if any tasks have waiting messages
            # This should cover idle, running and sleeping tasks
            if not self.mqueue.empty():
                self.processmessage()
                # immediately process these new readytasks so nothing can overwrite the sendval
                self.processready()

            if not self.taskmap:
                print('no more tasks')
                break

# System Calls
class SystemCall(object):
    def handle(self):
        pass

# Return a task's ID number
class GetTid(SystemCall):
    def handle(self):
        self.task.sendval = self.task.tid
        self.sched.schedule(self.task)

# Create a new task
class NewTask(SystemCall):
    def __init__(self,target):
        self.target = target
    def handle(self):
        tid = self.sched.new(self.target)
        self.task.sendval = tid
        self.sched.schedule(self.task)

# Kill a task
class KillTask(SystemCall):
    def __init__(self,tid):
        self.tid = tid
    def handle(self):
        task = self.sched.taskmap.get(self.tid,None)
        if task:
            task.target.close() 
            self.task.sendval = True
        else:
            self.task.sendval = False
        self.sched.schedule(self.task)

# Sleep for n seconds
class Sleep(SystemCall):
    def __init__(self,sleeptime):
        self.sleeptime = sleeptime
    def handle(self):
        now = time.time()
        whendone = now + self.sleeptime
        self.sched.waitforclock(self.task,whendone)

# Become idle until we have something that brings us back to attention
class Idle(SystemCall):
    def __init__(self):
        pass
    def handle(self):
        self.sched.goidle(self.task)

# Send a message to all observers of that message
class Message(SystemCall):
    def __init__(self,key,value):
        self.message = (key,value)
    def handle(self):
        self.sched.sendmessage((self.task.tid,self.message[0],self.message[1]))
        self.sched.schedule(self.task)

# Subscribe to a message as an observer
class SubscribeMessage(SystemCall):
    def __init__(self,messagekey):
        self.messagekey = messagekey
    def handle(self):
        self.sched.subscribemessage(self.messagekey,self.task.tid)
        self.sched.schedule(self.task)

# Wait for a task to exit
class WaitTask(SystemCall):
    def __init__(self,tid):
        self.tid = tid
    def handle(self):
        result = self.sched.waitforexit(self.task,self.tid)
        self.task.sendval = result
        # If waiting for a non-existent task,
        # return immediately without waiting
        if not result:
            self.sched.schedule(self.task)

# Wait for reading
class ReadWait(SystemCall):
    def __init__(self,f):
        self.f = f
    def handle(self):
        fd = self.f.fileno()
        self.sched.waitforread(self.task,fd)

# Wait for writing
class WriteWait(SystemCall):
    def __init__(self,f):
        self.f = f
    def handle(self):
        fd = self.f.fileno()
        self.sched.waitforwrite(self.task,fd)

# IO related functions
def Accept(sock):
    yield ReadWait(sock)
    yield sock.accept()

def Send(sock,buffer):
    while buffer:
        yield WriteWait(sock)
        try:
            len = sock.send(buffer)
        except IOError:
            print('Send IOError')
            raise
        except socket.error as e:
            print('Error in Send:',e)
            raise
        buffer = buffer[len:]

def Recv(sock,maxbytes):
    yield ReadWait(sock)
    try:
        yield sock.recv(maxbytes)
    except socket.error as e:
        print('Error in Recv:',e)
        raise
        
