from DAG import QueueManager
import multiprocessing
import pickle
from datetime import datetime
from configuration import address, authkey, worker_count
from dag_scripts import *


def worker(q, mem):
    while True:
        time.sleep(0.5)
        if q.qsize() > 0:
            task_binary = q.get()
            task_object = pickle.loads(task_binary)
            print('Executing the task: '+task_object.task_instance_id+' at {}'.format(datetime.now()))
            try:
                task_object.execute()
                mem.update({task_object.task_instance_id: 1})
            except Exception as e:
                print(e)
                mem.update({task_object.task_instance_id: 2})


if __name__ == '__main__':
    QueueManager.register('get_dict')
    QueueManager.register('ready_to_execute_queue')
    m = QueueManager(address=address, authkey=authkey)
    m.connect()
    mem = m.get_dict()
    task_source = m.ready_to_execute_queue()
    workers = []
    for _ in range(worker_count):
        workers.append(multiprocessing.Process(target=worker, args=(task_source, mem)))
        workers[-1].start()

    for _ in range(worker_count):
        workers[_].join()
