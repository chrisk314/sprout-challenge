"""Input/output handler functions
"""
import ast
import json

import pandas as pd


def load_csv_data(path):
    """Returns pandas.DataFrame containing data loaded from csv file at `path`.

    This function also sanitizes column names, i.e., strips whitespace.
    """
    with open(path, 'r') as f:
        df = pd.read_csv(f)
    df.columns = [x.strip() for x in df.columns]
    return df


def load_json_data(path):
    """Returns dict containing data loaded from json file at `path`."""
    with open(path, 'r') as f:
        json_s = f.read()
        try:
            return json.loads(json_s)
        except json.JSONDecodeError:
            return ast.literal_eval(json_s)
