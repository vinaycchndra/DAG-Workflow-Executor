from DAG import DirectedAcyclicGraph, Python_Operator
import time


def func(**kwargs):
    a = kwargs['a']
    time.sleep(a)


def func2(**kwargs):
    a = kwargs['a']
    time.sleep(a)


if __name__ == '__main__':

    with DirectedAcyclicGraph('My Prototype') as dag:
        node_a = Python_Operator('A', func, dag, kwargs={'a': 2})
        node_b = Python_Operator('B', func2, dag, kwargs={'a': 2})
        node_d = Python_Operator('D', func2, dag, kwargs={'a': 2})
        node_e = Python_Operator('E', func, dag, kwargs={'a': 2})
        node_f = Python_Operator('F', func, dag, kwargs={'a': 2})
        node_g = Python_Operator('G', func, dag, kwargs={'a': 2})
        node_a >> node_b >> node_e >> node_g
        node_a >> node_d >> node_e
        node_d >> node_f >> node_g
        node_e >> node_f


