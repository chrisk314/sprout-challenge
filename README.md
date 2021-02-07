# Solution

Tasks specified in a `tasks.csv` are scheduled to workers specified in json form as below:
```
{"Cho Chang": "engineer",... , "Susan Bones": "architect"}
```

A high level handler function `task_scheduler.schedule` takes as arguments a `pandas.DataFrame`
of `tasks` and a `dict` of `workers` data as above and manages how the tasks are assigned
to individual workers through a task quota assigning function `quota_func`. Different
quota functions can be specified with optional arguments resulting in a task quota dict as
below:

```
{"Cho Chang": {"engineer": 76}, ..., "Susan Bones": {"architect": 124}}
```

Unassigned tasks are then assigned by a simple `assign_tasks` function according to the
quota data. The `pandas.DataFrame` `tasks` is returned modified in place with all unassigned
tasks assigned.

# Install
```
virtualenv -p python3 .venv
source .venv/bin/activate
python3 -m pip install .
```

# Test
Unit tests.
```
python3 -m unittest discover -s task_scheduler/tests
```

Code style tests.
```
python3 -m pip install flake8
python3 -m flake8 task_scheduler/ --ignore E501 && echo "Code clean." || echo "Code dirty!"
```

# Usage

## As package
```python
from task_scheduler.schedule import schedule, schedule_uniform_by_type
from task_scheduler.io import load_csv_data, load_json_data

if __name__ == '__main__':
    tasks_path = 'task_scheduler/tests/data/tasks.csv'
    workers_path = 'task_scheduler/tests/data/employees.txt'
    tasks = load_csv_data(tasks_path)
    workers = load_json_data(workers_path)
    tasks = schedule(tasks, workers, quota_func=schedule_uniform_by_type)
    tasks.to_csv('./tasks-assigned.csv')
```
