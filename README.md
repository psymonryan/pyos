pyos
====

Python Coroutine Operating System

Based on the inspirational examples provided by David Beazley in his "A Curious Course on Coroutines and Concurrency"
(http://www.dabeaz.com/coroutines/index.html)

The code implements an operating system in pure python leveraging the efficiency of co-routines in python to 
schedule tasks via a sequential execution loop where control is handed back to the scheduler using yield statements

As an exercise the code has been extended to include task Messaging and Sleep system calls.

For more info checkout my blog entry: http://timedistort.blogspot.com.au/2014/01/curiosity-killed-thread.html
