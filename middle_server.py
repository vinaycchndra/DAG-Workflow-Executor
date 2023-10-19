from configuration import address, authkey
from queue import Queue
from DAG import QueueManager


queue1 = Queue()
queue2 = Queue()
mem = dict()
QueueManager.register('get_queue', callable=lambda: queue1)
QueueManager.register('get_dict', callable=lambda: mem)
QueueManager.register('ready_to_execute_queue', callable=lambda: queue2)
m = QueueManager(address=address, authkey=authkey)
s = m.get_server()
s.serve_forever()
