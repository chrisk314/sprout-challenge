"""Functions/class for schedling worker tasks."""

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
