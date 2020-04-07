import os
import sys 
import psutil
from multiprocessing import Process, Queue, Pipe
def getid_current_process():
    current_process_pid = psutil.Process().pid
    print('Main pid',current_process_pid)
    pid_ = []  
    for process in psutil.process_iter():
        if process.name() == 'python':
            #print(process.pid)
            pid_.append(process.pid)
            #print(process.name)
    return pid_
def kill_all_process(process_id):
    PROCNAME = "python" 
    for proc in psutil.process_iter():
        if proc.name() == PROCNAME:# and proc.pid == process_id:
            proc.kill()
if __name__ == '__main__':
    running_process = getid_current_process()
    print(running_process)  
    print('Num process running :',len(running_process))
    ##########################
    kill_all_process(4330)
