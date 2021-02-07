"""Functions/class for schedling worker tasks."""

from collections import defaultdict
from itertools import cycle

import pandas as pd


def schedule_uniform(tasks, workers, **kwargs):
    """Returns dict of quotas for `tasks` uniformly distributed to `workers`.

    Args:
        tasks: pd.DataFrame containing tasks. Pre assigned tasks will be respected.
        workers: dict containing info about workers e.g. {worker: type}

    Returns:
        Dict of task quotas of form {worker: {task_type: task_count, ...}, ...}.
    """
    cycle_workers = cycle(workers)
    worker_task_quota = defaultdict(dict)
    for task_type in tasks.type.unique():
        n_tasks = len(tasks[tasks.type == task_type])
        n_workers = len(workers)
        n_tasks_per_worker = n_tasks // n_workers
        n_tasks_remainder = n_tasks % n_workers

        for i, w in enumerate(workers):
            assigned_task_count = len(tasks[(tasks.type == task_type) & (tasks.user == w)])
            worker_task_quota[w][task_type] = n_tasks_per_worker - assigned_task_count

        # Assign the remaining tasks in round robin fashion
        for i in range(n_tasks_remainder):
            w = next(cycle_workers)
            worker_task_quota[w][task_type] += 1

    return worker_task_quota


def schedule_uniform_by_type(tasks, workers, **kwargs):
    """Returns dict of quotas for `tasks` uniformly distributed to `workers` by task type.

    Args:
        tasks: pd.DataFrame containing tasks. Pre assigned tasks will be respected.
        workers: dict containing info about workers e.g. {worker: type}

    Returns:
        Dict of task quotas of form {worker: {task_type: task_count, ...}, ...}.
    """
    # Construct new tasks DataFrame from sub sets by type
    worker_task_quota = defaultdict(dict)
    for task_type in tasks.type.unique():
        workers_by_type = {w: t for w, t in workers.items() if t == task_type}
        worker_task_quota_by_type = schedule_uniform(
            tasks[tasks.type == task_type],
            workers_by_type,
        )
        for w in workers_by_type:
            worker_task_quota[w].update(worker_task_quota_by_type[w])
    return worker_task_quota


def fix_incorrect_task_assignments(tasks, workers):
    """Returns pd.DataFrame of tasks with any incorrectly assigned tasks unassigned.

    Args:
        tasks: pd.DataFrame containing tasks.
        workers: dict containing info about workers e.g. {worker: type}

    Returns:
        pd.DataFrame with incorrectly assigned tasks unassigned.
    """
    for task_type in tasks.type.unique():
        workers_for_type = set([
            w for w, w_type in workers.items()
            if w_type == task_type
        ])
        pre_assigned_workers_for_type = set(tasks[tasks.type == task_type].user.unique())
        for misassigned_w in pre_assigned_workers_for_type - workers_for_type:
            tasks.loc[(tasks.type == task_type) & (tasks.user == misassigned_w), 'user'] = 'None'
    return tasks


def assign_tasks(tasks, worker_task_quota):
    """Returns pd.DataFrame of tasks assigned according to quota.

    Args:
        tasks: pd.DataFrame containing tasks with some unassigned tasks.
        worker_task_quota: Dict of task quotas of form
            {worker: {task_type: task_count, ...}, ...}

    Returns:
        pd.DataFrame with all tasks assigned according to quota.
    """
    for w, quota in worker_task_quota.items():
        for task_type, task_count in quota.items():
            mask = tasks[(tasks.type == task_type) & (tasks.user == 'None')]
            tasks.loc[mask.index[:task_count], 'user'] = w
    return tasks


def schedule(tasks, workers, quota_func=schedule_uniform, quota_kwargs={},
             check_assignments=True):
    """Returns pd.DataFrame of tasks assigned in accordance with `quota_func`.

    Args:
        tasks: pd.DataFrame containing tasks.
        workers: dict containing info about workers e.g. {worker: type}
        quota_func: function taking args (tasks, workers) and returning
            worker task quota dict of form
            {worker: {task_type: task_count, ...}, ...}.
        quota_kwargs: optional keyword args to pass to `quota_func`.
        check_assignments: Bool iff True unassign incorrectly assigned tasks.

    Returns:
        pd.DataFrame with all tasks assigned according to quota.
    """
    if check_assignments:
        tasks = fix_incorrect_task_assignments(tasks, workers)
    worker_task_quotas = quota_func(tasks, workers, **quota_kwargs)
    tasks = assign_tasks(tasks, worker_task_quotas)
    return tasks
