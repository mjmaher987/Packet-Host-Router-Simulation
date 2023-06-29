# Imported Libraries
from enum import Enum

import numpy as np


class FinishProgramException(Exception):
    pass


class Task:
    TASK_ID = 1

    def __init__(self, inter_arrival=None, priority=None, execute_time=None):
        self.task_id = Task.TASK_ID
        self.inter_arrival = inter_arrival
        self.arrival = None
        self.priority = priority
        self.execution_time = execute_time
        self.start_execution_time = None
        self.end_execution_time = None
        self.processor = None
        self.is_in_queue = False
        Task.TASK_ID += 1

    def __str__(self):
        return str(self.inter_arrival, self.priority)

    @staticmethod
    def get_next_arrival_task(all_tasks):  # all tasks are sorted
        for t in all_tasks:
            if t.start_execution_time is None and not t.is_in_queue:
                return t
        raise FinishProgramException()

    @staticmethod
    def get_next_free_processor_task(all_tasks):
        in_progress_tasks = []
        for t in all_tasks:
            if t.start_execution_time is not None and t.end_execution_time is None:
                in_progress_tasks.append(t)
        if in_progress_tasks:
            return min(in_progress_tasks, key=lambda x: x.start_execution_time + x.execution_time)

    @staticmethod
    def set_all_arrivals(all_tasks):
        cum_sum = 0
        for t in all_tasks:
            t.arrival = cum_sum + t.inter_arrival
            cum_sum += t.inter_arrival

    def finish(self):
        self.end_execution_time = self.start_execution_time + self.execution_time


class BaseQueue:
    def __init__(self, length_limit):
        self.length_limit = length_limit

    def get_next(self):
        raise NotImplemented

    def add_arrival_to_queue(self, task):
        raise NotImplemented


class FIFO(BaseQueue):

    def __init__(self, length_limit):
        super().__init__(length_limit)
        self.queue = []

    def get_next(self):
        try:
            return self.queue.pop()
        except IndexError:
            return None

    def add_arrival_to_queue(self, task):
        if len(self.queue) < self.length_limit:
            self.queue.append(task)


class WRR(BaseQueue):

    def __init__(self, length_limit):
        super().__init__(length_limit)
        self._priority_queues = [[] for i in range(3)]

    def get_next(self):
        for priority in range(3):
            if self._priority_queues[priority]:
                return self._priority_queues[priority].pop()
        return None

    def add_arrival_to_queue(self, task):
        if len(self._priority_queues[task.priority]) < self.length_limit:
            self._priority_queues[task.priority].append(task)


class NPPS(BaseQueue):
    def __init__(self, length_limit):
        super().__init__(length_limit)
        self.queue = []

    def get_next(self):
        try:
            return self.queue.pop()
        except IndexError:
            return None

    def add_arrival_to_queue(self, task):
        if len(self.queue) < self.length_limit:
            self.queue.append(task)
            self.queue = sorted(self.queue, key=lambda x: (x.priority, x.inter_arrival))


class EventType(Enum):
    END_TASK = 1
    NEW_TASK = 2


class Router:
    def __init__(self, processors_num, service_policy, length_limit, simulation_time, all_tasks):
        self.processors = [i for i in range(processors_num)]
        self.busy_processors = []
        self.service_policy = service_policy(length_limit)
        self.length_limit = length_limit  # TODO
        self.simulation_time = simulation_time
        self.all_tasks = all_tasks
        self.current_time = 0

    def handle_and_get_next_event(self):
        next_arrival_task = Task.get_next_arrival_task(self.all_tasks)
        next_arrival_time = next_arrival_task.arrival if next_arrival_task else None

        next_free_processor_task = Task.get_next_free_processor_task(self.all_tasks)
        if next_free_processor_task:
            next_free_processor_time = next_free_processor_task.start_execution_time + next_free_processor_task.execution_time
        else:
            next_free_processor_time = None

        next_event_time = self.get_next_event_time(next_arrival_time, next_free_processor_time)

        if next_event_time and next_event_time > self.simulation_time or next_event_time is None:
            raise FinishProgramException()

        if next_event_time == next_arrival_time:  # TODO what happens if next_arrival_time == next_free_processor_time
            return EventType.NEW_TASK.value, next_event_time
        elif next_event_time == next_free_processor_time:
            next_free_processor_task.finish()
            self.busy_processors.remove(next_free_processor_task.processor)
            return EventType.END_TASK.value, next_event_time
        else:
            raise Exception("what happened exactly?")

    def get_next_event_time(self, next_arrival_time, next_free_processor_time):
        if next_arrival_time is not None and next_free_processor_time is not None:
            return min(next_arrival_time, next_free_processor_time)
        elif next_arrival_time is not None:
            return next_arrival_time
        elif next_free_processor_time is not None:
            return next_free_processor_time
        else:
            return None

    def get_first_free_processor(self):
        for processor in self.processors:
            if processor not in self.busy_processors:
                return processor

    def execute_all_tasks(self):
        Task.set_all_arrivals(self.all_tasks)
        try:
            while self.current_time <= self.simulation_time:
                next_event, next_event_time = self.handle_and_get_next_event()
                if next_event == EventType.NEW_TASK.value:
                    free_processor = self.get_first_free_processor()
                    next_task = Task.get_next_arrival_task(self.all_tasks)
                    if free_processor is not None:
                        self.execute(next_task, free_processor, next_event_time)
                    else:
                        next_task.is_in_queue = True
                        self.service_policy.add_arrival_to_queue(next_task)
                elif next_event == EventType.END_TASK.value:
                    free_processor = self.get_first_free_processor()
                    if free_processor is not None:
                        task_in_queue = self.service_policy.get_next()
                        if task_in_queue is not None:
                            self.execute(task_in_queue, free_processor, next_event_time)
                    else:
                        raise Exception('how is it possible !?')
                else:
                    raise Exception(f'next_event is not in [FINISH_PROGRAM, NEW_TASK, END_TASK]')
                self.current_time = next_event_time
        except FinishProgramException:
            self.finish_all()

        print(f'Simulation ended at {self.current_time}')

    def finish_all(self):
        for t in self.all_tasks:
            t: Task
            if t.start_execution_time is not None and t.end_execution_time is None:
                t.finish()

    def execute(self, t, processor, next_event_time):
        t.start_execution_time = next_event_time
        t.processor = processor
        self.busy_processors.append(processor)


X = 1  # parameter of the poisson distribution (in packet generation)
Y = 1  # parameter of the exponential distribution (in router - for service time generation)
T = 5  # Total simulation time

PROCESSORS_NUM = 2  # It can vary
SERVICE_POLICY = [FIFO, WRR, NPPS][2]  # It can vary
LENGTH_LIMIT = 20  # It can vary

generator = np.random.default_rng()
packet_arrivals = generator.poisson(lam=X, size=T * X * 2)
priorities = np.random.choice([0, 1, 2], p=[0.2, 0.3, 0.5], size=T * X * 2)

generator = np.random.default_rng()
packet_times = generator.exponential(Y, size=T * X * 2)

packets = [Task(inter_arrival=arrival, priority=priority, execute_time=time)
           for (arrival, priority, time) in zip(packet_arrivals, priorities, packet_times)]

r = Router(processors_num=PROCESSORS_NUM, service_policy=SERVICE_POLICY, length_limit=LENGTH_LIMIT, simulation_time=T,
           all_tasks=packets)
r.execute_all_tasks()

import csv

# Define a list of tasks
tasks = packets

# Open a CSV file for writing
with open('tasks.csv', 'w', newline='') as csvfile:
    # Create a CSV writer object
    writer = csv.writer(csvfile)

    # Write the header row
    writer.writerow(
        ['task_id', 'inter_arrival', 'priority', 'execution_time', 'start_execution_time', 'end_execution_time',
         'processor'])

    # Write the data rows
    for task in tasks:
        writer.writerow(
            [task.task_id, task.inter_arrival, task.priority, task.execution_time, task.start_execution_time,
             task.end_execution_time, task.processor])
