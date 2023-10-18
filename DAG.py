from datetime import datetime
from multiprocessing.managers import BaseManager
from configuration import address, authkey
import pickle

class Python_Operator:
    def __init__(self, task_id, python_callable, dag, kwargs=None):
        self.kwargs = kwargs
        self.task_id = dag.dag_instance_id+'-'+task_id
        self.callable = python_callable
        self.child_nodes = set()
        self.parent_nodes = set()
        self.visited = self.is_executed = self.failed = False
        dag.addNode(self)

    @property 
    def readyToExecute(self):
        for node in self.parent_nodes:
            if not node.is_executed:
                return False
        return True
    
    def execute(self):
        self.callable(self.kwargs)
        self.is_executed = True
    
    def __rshift__(self, next_node):
        self.child_nodes.add(next_node)
        next_node.parent_nodes.add(self)
        return next_node

# Base Manager class for the inter python script communication for passing task nodes by creating task que

class QueueManager(BaseManager):
    pass

# Graph class holds all the task nodes.

class DirectedAcyclicGraph:
    def __init__(self, dag_id):
        self.dag_id = dag_id
        self.dag_instance_id = self.dag_id+'-'+str(datetime.datetime.now())
        self.__nodes = []
        self.topological_order = deque()


    def __isCycle(self):
        for node in self.__nodes:
            if node.visited == False and self.dfsCycleDetect(node):
                return True

        # Restoring visited attribute of the node to the False for topological sorting implementation
        # Since after checking cycles in graph these false values are set to True for cycle detection algorithm
        for node in self.__nodes:
            node.visited = False
        return False

    def __topoLogicalSort(self):
        for node in self.__nodes:
            if not node.visited:
                self.dfsTopologicalSort(node)

    def dfsCycleDetect(self, node, stack_mem = None):
        if stack_mem is None:
            stack_mem = set()

        stack_mem.add(node)

        for child in node.child_nodes:
            if child.visited:
                continue
            if child in stack_mem or self.dfsCycleDetect(child, stack_mem):
                return True
        node.visited = True
        stack_mem.remove(node)

        return False

    def dfsTopologicalSort(self,node):
        if node.visited:
            return
        node.visited = True

        for child_node in node.child_nodes:
            self.dfsTopologicalSort(child_node)
        self.topological_order.appendleft(node)

    def addNode(self, node):
        self.__nodes.append(node)

    def pushTasks(self):


    def __enter__(self):
        return self
        
    def __exit__(self):
        if self.__isCycle():
            raise Exception('There is cycle in your Workflow')
        self.__topoLogicalSort()

        # adding parent task instance ids for every node to check for execution dependency in worker node script ranther than the node objects which caueses serialization while pushing into the manager que 
        for node in self.__nodes:
            new = []
            for node_p in node.parent_nodes:
                new.append(node_p.task_instance_id)
            node.parent_nodes = new
            
            # deleting child nodes for all the nodes as we do not want to serialize 
            del node.child_nodes
         
        m = QueueManager(address=address, authkey=authkey)
        m.connect()
        queue = m.get_queue()
        for node in self.topological_order:
            task_binary = pickle.dumps(node)
            queue.put(task_binary)
        print([node.val for node in self.topological_order])

    
