#!/usr/bin/env python3
import datetime
from collections import defaultdict
from constants import POOL_ALIAS
import plotly.express as px
import sys

"""
Parse tasks from JSON
"""
def parse_tasks(d):
    dates = []
    for i, ti in enumerate(d['task_instances']):
        execution_date = datetime.datetime.fromisoformat(ti['start_date'])
        end_date       = datetime.datetime.fromisoformat(ti['end_date'])

        dates.append((execution_date, ti, 'start'))
        dates.append((end_date, ti, 'stop'))

    dates.sort(key=lambda e: e[0])

    return dates

"""
Create pool_sizes: pool_name -> pool_size
"""
def compute_pool_size(dates):

    pool_sizes = defaultdict(int)
    tmp = defaultdict(int)

    for _, ti, event in dates:

        pool, pool_slots = ti['pool'], ti['pool_slots']

        if event == 'start':
            tmp[pool] += pool_slots
        else:
            tmp[pool] -= pool_slots

        # update the pool size
        pool_sizes[pool] = max(pool_sizes[pool], tmp[pool])

    return pool_sizes

"""
Create the Gantt chart data
"""
def build_gantt_data(dates):
    pool_sizes = compute_pool_size(dates)

    df = []

    # slots[slot_id] = (next_available, tasks)
    slots = {
        pool: [None] * pool_size
        for pool, pool_size in pool_sizes.items()
    }
    df = []

    for t, ti, event in dates:
        pool = ti['pool']

        # slots for the pool where the current task is running
        ss = slots[pool]

        # ignore 'stop' events
        if event != 'start': continue

        # find first available pool
        for i, next_available in enumerate(ss):
            if next_available is None or next_available <= t:
                found = True
                break

        if not found: raise ValueError('incoherent planning :(')

        t_start = t
        t_end   = t + datetime.timedelta(seconds=ti['duration'])

        # set the next availability of the current slot
        ss[i] = t_end

        # --- Transformation for displaying as a Gantt chart ---
        map_index = None if ti['map_index'] == -1 else f'{ti["map_index"]}'

        pool = POOL_ALIAS.get(pool, pool)
        df.append({
            'task': ti['task_id'], 'start': t_start, 'end': t_end, 'resource': f'{pool}.{i}', 'map_index': map_index
        })
        # --- ---

    t0 = dates[0][0]
    delta = t0 - datetime.datetime.fromtimestamp(0, tz=t0.tzinfo)

    for d in df:
        d['start'] = d['start'] - delta
        d['end']   = d['end'] - delta

    df.sort(key=lambda e: e['resource'])

    return df

def create_gantt_byresources(df):
    fig = px.timeline(df,
        x_start="start", x_end="end", y="resource", color="task", text='map_index',
        labels={
            "resource": "Resources",
            "task": "Task",
            "map_index": "Batch ID"
        },
        width=28 * 30, height=12 * 30,
        color_discrete_sequence=px.colors.qualitative.G10
    )
    fig.update_xaxes(tickformat='%s', title='Time (s)')
    fig.update_traces(textposition='inside')
    fig.write_image("figures/gantt_byresources.svg")
    print('wrote Gantt by task to "figures/gantt_byresources.svg"')

def create_gantt_bytask(df):
    fig = px.timeline(df,
        x_start="start", x_end="end", y="task", 
        labels={
            "resource": "Resources",
            "task": "Task",
            "map_index": "Batch ID",
        },
        width=28 * 30, height=12 * 30,
        color_discrete_sequence=px.colors.qualitative.G10
    )
    fig.update_xaxes(tickformat='%s', title='Time (s)')
    fig.update_traces(textposition='inside')
    fig.write_image("figures/gantt_bytask.svg")
    print('wrote Gantt by task to "figures/gantt_bytask.svg"')

def main():
    from constants import BASE_URL, SESSION_COOKIE, POOL_ALIAS
    import requests, json

    if len(sys.argv) not in {2, 3}:
        print(f'usage:', file=sys.stderr)
        print(f'  {sys.argv[0]} <dag_id> <run_id> | data from Apache Airflow API', file=sys.stderr)
        print(f'  {sys.argv[0]} <data_file>       | data from <data_file>', file=sys.stderr)
        exit(1)

    if len(sys.argv) == 3:
        dag_id = sys.argv[1]
        run_id = sys.argv[2]

        print('dag_id:', dag_id)
        print('run_id:', run_id)

        s = requests.Session()
        s.cookies.set('session', SESSION_COOKIE)
        s.headers['Content-type'] = 'application/json'

        r = s.get(f'{BASE_URL}/dags/{dag_id}/dagRuns/{run_id}/taskInstances')
        d = r.json()
    else:
        with open(sys.argv[1], 'rb') as f:
            d = json.load(f)

    dates      = parse_tasks(d)
    gantt_data = build_gantt_data(dates)
    
    create_gantt_byresources(gantt_data)
    create_gantt_bytask(gantt_data)
    return d

if __name__ == '__main__':
    d = main()
    import json
    with open('out.json', 'w') as f:
        json.dump(d, f)