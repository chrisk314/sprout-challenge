"""Unit tests for functions/classes in schedule.py."""

import os
from unittest import TestCase

from task_scheduler.io import load_csv_data, load_json_data
from task_scheduler.schedule import schedule, schedule_uniform, schedule_uniform_by_type


class TestSchedule(TestCase):

    tasks_csv_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "data/tasks.csv")
    employees_json_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "data/employees.txt")

    def setUp(self):
        self.tasks = load_csv_data(self.tasks_csv_path)
        self.workers = load_json_data(self.employees_json_path)

    def test_schedule_uniform(self):
        """Tests schedule.schedule_uniform function."""
        tasks, workers = self.tasks, self.workers

        pre_assigned_tasks = {
            w: tasks.index[tasks.user == w] for w in workers
        }

        # tasks = schedule_uniform(tasks, workers)
        tasks = schedule(tasks, workers, quota_func=schedule_uniform, check_assignments=False)

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

    def test_schedule_uniform_by_type(self):
        """Tests schedule.schedule_uniform_by_type function."""
        tasks, workers = self.tasks, self.workers

        correctly_pre_assigned_tasks = {
            w: tasks.index[(tasks.type == t) & (tasks.user == w)] for w, t in workers.items()
        }

        # tasks = schedule_uniform(tasks, workers)
        tasks = schedule(tasks, workers, quota_func=schedule_uniform_by_type)

        # Check that there are no unassigned tasks
        self.assertEqual(len(tasks[tasks.user == 'None']), 0)

        # Check that all workers have been assigned tasks
        self.assertEqual(set(workers.keys()), set(tasks.user.unique()))

        # Check that number of tasks assigned is uniform by type
        for task_type in tasks.type.unique():
            tasks_per_user = [
                len(tasks[(tasks.type == task_type) & (tasks.user == w)])
                for w in tasks[tasks.type == task_type].user.unique()
            ]
            self.assertLessEqual(max(tasks_per_user) - min(tasks_per_user), 1)

        # For each worker check that all tasks assigned correctly by management are still
        # assigned to only that worker.
        for w, pre_assigned_idx in correctly_pre_assigned_tasks.items():
            now_assigned = set(tasks.loc[pre_assigned_idx, 'user'])
            self.assertEqual(len(now_assigned - {w}), 0)
