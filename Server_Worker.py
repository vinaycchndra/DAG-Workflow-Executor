import pickle
from configuration import address, authkey
from DAG import QueueManager
from dag_scripts import *

if __name__ == '__main__':
    QueueManager.register('get_queue')
    m = QueueManager(address=address, authkey=authkey)
    m.connect()
    queue = m.get_queue()

    while True:
        z1 = queue.get()
        node1 = pickle.loads(z1)
        print(node1.task_instance_id)
        node1.callable(2)
