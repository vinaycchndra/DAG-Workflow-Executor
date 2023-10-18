from configuration import address, authkey
from queue import Queue
from DAG import QueueManager


queue = Queue()
QueueManager.register('get_queue', callable=lambda: queue)
m = QueueManager(address=address, authkey=authkey)
s = m.get_server()
s.serve_forever()
