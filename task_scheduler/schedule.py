"""Functions/class for schedling worker tasks."""

import pandas as pd


def schedule_uniform(tasks, workers):
    """Returns pd.DataFrame with tasks uniformly distributed across workers.

    Args:
        tasks: pd.DataFrame containing tasks. Some tasks may be pre assigned.
            Pre assigned tasks will be respected.
        workers: dict containing info about workers e.g. {worker: type}

    Returns:
        pd.DataFrame with all tasks assigned to a worker.
    """
    n_tasks = len(tasks)
    n_workers = len(workers)
    n_tasks_per_worker = n_tasks // n_workers
    n_tasks_remainder = n_tasks % n_tasks_per_worker

    # Determine counts of new tasks to assign
    assigned_tasks_count = {w: len(tasks[tasks.user == w]) for w in workers}
    newly_assigned_tasks_count = {
        w: n_tasks_per_worker - assigned_tasks_count[w] for w in workers
    }
    for w in list(workers.keys())[:n_tasks_remainder]:
        newly_assigned_tasks_count[w] += 1

    unassigned_index = tasks.index[tasks.user == 'None']

    prev_idx = 0
    for w, n_tasks in newly_assigned_tasks_count.items():
        idx_block = unassigned_index[prev_idx:prev_idx + n_tasks]
        tasks.loc[idx_block, 'user'] = w
        prev_idx += n_tasks

    return tasks


def fix_incorrect_task_assignments(tasks, workers):
    """Returns pd.DataFrame of tasks with any incorrectly assigned tasks unassigned.

    Args:
        tasks: pd.DataFrame containing tasks.
        workers: dict containing info about workers e.g. {worker: type}

    Returns:
        pd.DataFrame with incorrectly assigned tasks unassigned.
    """
    # First unassign any tasks which are incorrectly assigned for workers of specific type
    for task_type in tasks.type.unique():
        workers_for_type = set([
            w for w, w_type in workers.items()
            if w_type == task_type
        ])
        pre_assigned_workers_for_type = set(tasks[tasks.type == task_type].user.unique())
        for misassigned_w in pre_assigned_workers_for_type - workers_for_type:
            tasks.loc[(tasks.type == task_type) & (tasks.user == misassigned_w), 'user'] = 'None'
    return tasks


def schedule_uniform_by_type(tasks, workers):
    """Returns pd.DataFrame with tasks uniformly distributed across workers by type.

    Args:
        tasks: pd.DataFrame containing tasks. Some tasks may be pre assigned.
            Pre assigned tasks will be respected where correctly assigned by type.
        workers: dict containing info about workers e.g. {worker: type}

    Returns:
        pd.DataFrame with all tasks assigned to a worker.
    """
    tasks = fix_incorrect_task_assignments(tasks, workers)

    # Construct new tasks DataFrame from sub sets by type
    tasks_by_type_all = []
    for task_type in tasks.type.unique():
        tasks_by_type = schedule_uniform(
            tasks[tasks.type == task_type].copy(),
            {w: t for w, t in workers.items() if t == task_type}
        )
        tasks_by_type_all += [tasks_by_type]
    tasks = pd.concat(tasks_by_type_all)

    tasks.sort_values(['version', 'user'], inplace=True)

    return tasks
