import pickle
from configuration import address, authkey
from DAG import QueueManager
from dag_scripts import *

QueueManager.register('get_queue')
QueueManager.register('get_dict')
QueueManager.register('ready_to_execute_queue')
m = QueueManager(address=address, authkey=authkey)
m.connect()
queue = m.get_queue()
mem = m.get_dict()
task_source = m.ready_to_execute_queue()

while True:
    if queue.qsize() > 0:
        task_binary = queue.get()
        task_object = pickle.loads(task_binary)
        if task_object.readyToExecute(mem):
            print(task_object.task_instance_id + ' : queing this job, ready to execute')
            task_source.put(task_binary)
        else:
            print(task_object.task_instance_id + ' dependency is still not met')
            queue.put(task_binary)

