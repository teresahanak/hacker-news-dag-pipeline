from collections import deque
import csv
import itertools


class DAG:
    def __init__(self):
        self.graph = {}

    def in_degrees(self):
        degrees = {node: 0 for node, value in self.graph.items()}
        for node in degrees:
            for pointers in self.graph.values():
                if node in pointers:
                    degrees[node] += 1
        return degrees

    def sort(self):
        degrees = self.in_degrees()
        to_visit = deque([node for node, value in degrees.items() if value == 0])

        searched = []
        while to_visit:
            node = to_visit.popleft()
            for pointer in self.graph[node]:
                degrees[pointer] -= 1
                if degrees[pointer] == 0:
                    to_visit.append(pointer)
            searched.append(node)
        return degrees

    def add(self, node, to=None):
        if node not in self.graph:
            self.graph[node] = []
        if to:
            self.graph[node].append(to)
            if to not in self.graph:
                self.graph[to] = []
        if len(self.sort()) != len(self.graph):
            raise Exception("Graph contains cycle.")
            
class Pipeline:
    def __init__(self):
        self.tasks = DAG()

    def task(self, depends_on=None):
        def inner(f):
            self.tasks.add(f)
            if depends_on:
                self.tasks.add(depends_on, f)
            return f

        return inner

    def run(self):
        scheduled = self.tasks.sort()
        completed = {}
        
        for task in scheduled:
            for node, values in self.tasks.graph.items():
                if task in values:
                    completed[task] = task(completed[node])
            if task not in completed:
                completed[task] = task()
        return completed
    
def build_csv(lines, header=None, file=None):
    if header:
        lines = itertools.chain([header], lines)
    writer = csv.writer(file, delimiter=',')
    writer.writerows(lines)
    file.seek(0)
    return file