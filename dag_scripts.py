from DAG import DirectedAcyclicGraph, Python_Operator
import time


def func(x):
    time.sleep(2)
    print(x*x)


if __name__ == '__main__':
    with DirectedAcyclicGraph('My Prototype') as dag:
        node_a = Python_Operator('A', func, dag, 4)
        node_b = Python_Operator('B', func, dag, 5)
        node_d = Python_Operator('D', func, dag, 6)
        node_e = Python_Operator('E', func, dag, 7)
        node_f = Python_Operator('F', func, dag, 8)
        node_g = Python_Operator('G', func, dag, 9)
        node_a >> node_b >> node_e >> node_g
        node_a >> node_d >> node_e >> node_g
        node_d >> node_f >> node_g
        node_e >> node_f



