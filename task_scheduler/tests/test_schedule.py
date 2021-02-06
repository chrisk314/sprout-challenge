"""Unit tests for functions/classes in schedule.py."""

import os
from unittest import TestCase

from task_scheduler.io import load_csv_data, load_json_data
from task_scheduler.schedule import schedule_uniform


class TestSchedule(TestCase):

    tasks_csv_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "data/tasks.csv")
    employees_json_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "data/employees.txt")

    def setUp(self):
        self.tasks = load_csv_data(self.tasks_csv_path)
        self.workers = load_json_data(self.employees_json_path)

    def test_schedule_uniform(self):
        tasks, workers = self.tasks, self.workers

        pre_assigned_tasks = {
            w: tasks.index[tasks.user == w] for w in workers
        }

        tasks = schedule_uniform(tasks, workers)

        # Check that there are no unassigned tasks
        self.assertEqual(len(tasks[tasks.user == 'None']), 0)

        # Check that all workers have been assigned tasks
        self.assertEqual(set(workers.keys()), set(tasks.user.unique()))

        # Check that number of tasks assigned is uniform
        tasks_per_user = [len(tasks[tasks.user == w]) for w in workers]
        self.assertLessEqual(max(tasks_per_user) - min(tasks_per_user), 1)

        # For each worker check that all tasks assigned by management are still
        # assigned to only that worker.
        for w, pre_assigned_idx in pre_assigned_tasks.items():
            now_assigned = set(tasks.loc[pre_assigned_idx, 'user'])
            self.assertEqual(len(now_assigned - {w}), 0)
