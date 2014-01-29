#!/usr/bin/python2
# Perf and Unit tests
#
# Test what happens with large numbers of tasks
# Simon Ryan 2014

from pyos9 import *
import sys,random,time,threading

mode = sys.argv[1:2] and sys.argv[1] or None
num = sys.argv[2:3] and sys.argv[2] or None
if not (mode and num):
    print('usage: %s [pyos|threads] numberoftasks' % (sys.argv[0]))
    sys.exit(1)

try:
    num = int(num)
except:
    num = 1000

now = time.time()

def readhellos():
    yield SubscribeMessage('hello')
    while 1:
        # We need to go idle in order to listen for the message
        yield Idle()
        message = (yield)
        if message and message[2] == 'quit':
            break

def spawninator():
    print('spawninator starting')
    for i in xrange(num):
        yield NewTask(readhellos())
    print('spawninator finishing spawning %d tasks in %2.2f seconds' % (num,time.time() - now))
    print('sending quit message')
    yield Message('hello','quit')

class globs:
    # Poor mans messaging for threads
    timetodie = False

def simpletask():
    # Something we can launch as a thread
    while 1:
        time.sleep(1)
        if globs.timetodie:
            break

if mode == 'pyos':
    sched = Scheduler()
    sched.new(spawninator())
    sched.mainloop()

else:
    # Do approximately the same but with threads
    threadlist = []
    for i in range(num):
         try:
             testthread = threading.Thread(target=simpletask)
             testthread.start()
         except:
             print('Thread create error at thread number %d' % (i+1))
             break
         threadlist.append(testthread)
    elapsed = time.time() - now

    print('spawninator finishing spawning %d tasks in %2.2f seconds' % (num,elapsed))

    globs.timetodie = True
    for task in threadlist:
        task.join()

    elapsed = time.time() - now

print('Finished in %2.2f seconds' % (time.time() - now))
