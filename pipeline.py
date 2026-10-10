"""Equivalent full, streaming, and lazy reductions on the complete HIGGS CSV."""
from pathlib import Path
import csv, math, time
import numpy as np
import pandas as pd
import dask
import dask.dataframe as dd
ROOT = Path(__file__).resolve().parent
from dataset import COLUMNS, METRICS
CHUNKSIZE = 100_000

def read_options(path):
    # HIGGS has no header: explicit names preserve the first observation.
    with Path(path).open('r', encoding='utf-8', newline='') as source:
        first = next(csv.reader(source))
    if len(first) != len(COLUMNS):
        raise ValueError(f'Expected {len(COLUMNS)} numeric columns, got {len(first)}.')
    for value in first:
        if value:
            float(value)
    return {'header': None, 'names': COLUMNS, 'dtype': 'float64',
            'encoding': 'utf-8', 'engine': 'c', 'on_bad_lines': 'error'}

def partition_stats(frame):
    values = {'rows': len(frame)}
    for col in METRICS:
        values[col + '_sum'] = float(frame[col].sum(skipna=True))
        values[col + '_count'] = int(frame[col].count())
    return pd.DataFrame([values])

def finish(values):
    result = {'rows': int(values['rows']), 'metrics': {}}
    for col in METRICS:
        count = int(values[col + '_count'])
        total = float(values[col + '_sum'])
        result['metrics'][col] = {'sum': total, 'valid_count': count,
                                  'missing_count': result['rows'] - count,
                                  'mean': total / count if count else None}
    return result

def run_full(path):
    options = read_options(path)
    started = time.perf_counter()
    frame = pd.read_csv(path, **options)  # Task 1: intentionally load all rows at once.
    read_seconds = time.perf_counter() - started
    result = finish(partition_stats(frame).iloc[0].to_dict())
    return {'aggregation': result, 'read_seconds': read_seconds, 'dataframe_deep_bytes': int(frame.memory_usage(deep=True).sum())}

def run_chunks(path, chunksize=CHUNKSIZE):
    options = read_options(path)
    rows = 0
    counts = {col: 0 for col in METRICS}
    sums = {col: [] for col in METRICS}  # One scalar per chunk, never retain input chunks.
    chunk_count = 0
    max_rows = 0
    for frame in pd.read_csv(path, chunksize=chunksize, **options):
        stats = partition_stats(frame).iloc[0].to_dict()
        rows += len(frame)
        max_rows = max(max_rows, len(frame))
        chunk_count += 1
        for col in METRICS:
            sums[col].append(float(stats[col + '_sum']))
            counts[col] += int(stats[col + '_count'])
        if chunk_count % 50 == 0:
            print(f'Pandas chunks: {rows:,} rows, {chunk_count} chunks', flush=True)
        del frame
    values = {'rows': rows}
    for col in METRICS:
        values[col + '_sum'] = math.fsum(sums[col])
        values[col + '_count'] = counts[col]
    return {'aggregation': finish(values), 'chunk_count': chunk_count,
            'chunksize': chunksize, 'max_chunk_rows': max_rows}

def graph_sample(graph, maximum=25):
    keys = list(graph)[:maximum]
    nodes = [{'id': i, 'key': str(key)[:150]} for i, key in enumerate(keys)]
    index = {key: i for i, key in enumerate(keys)}
    edges = []
    from dask.core import get_dependencies
    for key in keys:
        for dependency in get_dependencies(graph, key):
            if dependency in index:
                edges.append([index[dependency], index[key]])
    return {'nodes': nodes, 'edges': edges, 'shown_tasks': len(keys), 'total_tasks': len(graph)}

def run_dask(path, blocksize='64MiB', threads=4):
    options = read_options(path)
    build_start = time.perf_counter()
    frame = dd.read_csv(str(path), blocksize=blocksize, **options)
    meta = pd.DataFrame({'rows': pd.Series(dtype='int64'), **{
        col + suffix: pd.Series(dtype=('float64' if suffix == '_sum' else 'int64'))
        for col in METRICS for suffix in ['_sum', '_count']}})
    # Opaque map_partitions reads all columns, matching the pandas runs.
    # Each partition becomes one tiny summary row, then a tree reduction combines them.
    partial = frame.map_partitions(partition_stats, meta=meta)
    query = partial.sum(split_every=8)
    before = query.__dask_graph__()
    optimized = query.optimize(fuse=True)
    after = optimized.__dask_graph__()
    build_seconds = time.perf_counter() - build_start
    graph_info = {'partitions': frame.npartitions, 'tasks_before': len(before), 'tasks_after': len(after),
                  'expression_before': str(query.expr), 'expression_after': str(optimized.expr),
                  'sample_before': graph_sample(before), 'sample_after': graph_sample(after)}
    print(f'Dask: {frame.npartitions} partitions; {len(before)} -> {len(after)} tasks; {threads} threads', flush=True)
    compute_start = time.perf_counter()
    values = optimized.compute(scheduler='threads', num_workers=threads)
    return {'aggregation': finish(values.to_dict()), 'build_seconds': build_seconds,
            'compute_seconds': time.perf_counter() - compute_start,
            'blocksize': blocksize, 'threads': threads, 'graph': graph_info}

def compare(reference, actual, rtol=1e-9, atol=1e-6):
    if reference['rows'] != actual['rows']:
        raise AssertionError('Logical CSV record counts differ.')
    for col in METRICS:
        a, b = reference['metrics'][col], actual['metrics'][col]
        if a['valid_count'] != b['valid_count'] or a['missing_count'] != b['missing_count']:
            raise AssertionError(f'Counts differ for {col}.')
        for key in ['sum', 'mean']:
            if a[key] is None or b[key] is None:
                if a[key] != b[key]:
                    raise AssertionError(f'Empty-column results differ for {col}.')
            elif not math.isclose(a[key], b[key], rel_tol=rtol, abs_tol=atol):
                raise AssertionError(f'{col} {key} differs beyond tolerance: {a[key]} vs {b[key]}')
    return True
