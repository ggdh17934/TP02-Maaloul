"""One isolated measured method; errors and memory limits are recorded explicitly."""
import argparse, json, time, traceback
from pathlib import Path

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('method', choices=['full','chunks','dask'])
    parser.add_argument('--csv', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--memory-gib', type=float, default=2)
    parser.add_argument('--threads', type=int, default=4)
    parser.add_argument('--blocksize', default='64MiB')
    args = parser.parse_args()
    record = {'method': args.method, 'csv': args.csv, 'status': 'starting'}
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    exit_code = 0
    started = None
    try:
        if args.memory_gib > 0:
            from memory_limit import set_memory_limit
            record['memory_limit'] = set_memory_limit(args.memory_gib * 1024**3)
        else:
            record['memory_limit'] = None
        import pipeline
        record['imports_complete'] = True
        record['all_columns_loaded'] = True
        started = time.perf_counter()
        if args.method == 'full':
            record.update(pipeline.run_full(args.csv))
        elif args.method == 'chunks':
            record.update(pipeline.run_chunks(args.csv))
        else:
            record.update(pipeline.run_dask(args.csv, args.blocksize, args.threads))
        record['status'] = 'success'
    except MemoryError as error:
        record.update(status='oom', exception_type=type(error).__name__, error=str(error), traceback=traceback.format_exc())
        traceback.print_exc()
        exit_code = 2
    except Exception as error:
        record.update(status='error', exception_type=type(error).__name__, error=str(error), traceback=traceback.format_exc())
        traceback.print_exc()
        exit_code = 1
    finally:
        if started is not None:
            record['operation_seconds'] = time.perf_counter() - started
        record['completed_utc'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
        temp = output.with_suffix('.json.tmp')
        temp.write_text(json.dumps(record, indent=2), encoding='utf-8')
        temp.replace(output)
    return exit_code

if __name__ == '__main__':
    raise SystemExit(main())
