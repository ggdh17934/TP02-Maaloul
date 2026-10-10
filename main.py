"""Run repeatable isolated benchmarks and profile the entire worker process tree."""
from pathlib import Path
import argparse, csv, hashlib, importlib.metadata, json, platform, subprocess, sys, time
import psutil
from download_data import ROOT, CSV_PATH, CSV_BYTES, MANIFEST, write_json

def system_info():
    memory = psutil.virtual_memory()
    packages = {}
    for name in ['pandas','dask','distributed','psutil','matplotlib','pyarrow','numpy']:
        packages[name] = importlib.metadata.version(name)
    return {'os': platform.platform(), 'python': platform.python_version(), 'python_executable': sys.executable,
            'physical_cores': psutil.cpu_count(logical=False), 'logical_processors': psutil.cpu_count(),
            'ram_total_bytes': memory.total, 'ram_available_bytes_at_start': memory.available,
            'packages': packages, 'captured_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}

def measured_run(method, run, path, destination, memory_gib, threads, blocksize):
    prefix = f'{method}_run{run}'
    result_path = destination / f'{prefix}.json'
    configuration = {'input_path': str(path.resolve()), 'input_bytes': path.stat().st_size,
                     'memory_gib': memory_gib, 'threads': threads, 'blocksize': blocksize,
                     'code_sha256': hashlib.sha256(b''.join((ROOT/name).read_bytes() for name in ['pipeline.py','worker.py','memory_limit.py','dataset.py'])).hexdigest()}
    if result_path.exists():
        existing = json.loads(result_path.read_text(encoding='utf-8'))
        if existing.get('status') in ['success','oom'] and existing.get('profile') and existing.get('run_configuration') == configuration:
            print(f'Already measured {prefix}: {existing["status"]}', flush=True)
            return existing
    cmd = [sys.executable, '-u', str(ROOT/'worker.py'), method, '--csv', str(path), '--output', str(result_path),
           '--memory-gib', str(memory_gib), '--threads', str(threads), '--blocksize', blocksize]
    start = time.perf_counter()
    samples_path = destination / f'{prefix}_memory.csv'
    peak_rss = 0
    peak_private = 0
    minimum_available = psutil.virtual_memory().available
    stopped = None
    with (destination/f'{prefix}.log').open('w', encoding='utf-8') as log, samples_path.open('w', newline='', encoding='utf-8') as samples:
        process = subprocess.Popen(cmd, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
        worker = psutil.Process(process.pid)
        writer = csv.writer(samples)
        writer.writerow(['elapsed_seconds','process_tree_rss_bytes','process_tree_private_bytes','system_available_bytes','system_percent'])
        last_report = 0
        while process.poll() is None:
            try:
                members = [worker] + worker.children(recursive=True)
                rss = private = 0
                for member in members:
                    try:
                        info = member.memory_info()
                        rss += info.rss
                        private += getattr(info, 'private', 0)
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass
                available = psutil.virtual_memory()
                elapsed = time.perf_counter() - start
                writer.writerow([round(elapsed,4),rss,private,available.available,available.percent])
                peak_rss = max(peak_rss,rss)
                peak_private = max(peak_private,private)
                minimum_available = min(minimum_available,available.available)
                if available.available < 3 * 1024**3:
                    stopped = 'system_memory_guard'
                    for member in reversed(members):
                        try: member.kill()
                        except (psutil.NoSuchProcess, psutil.AccessDenied): pass
                elif elapsed > 7200:
                    stopped = 'timeout'
                    for member in reversed(members):
                        try: member.kill()
                        except (psutil.NoSuchProcess, psutil.AccessDenied): pass
                if elapsed - last_report >= 30:
                    print(f'{prefix}: {elapsed:.0f}s; RSS={rss/1024**3:.2f} GiB; peak={peak_rss/1024**3:.2f} GiB', flush=True)
                    last_report = elapsed
            except psutil.NoSuchProcess:
                break
            time.sleep(0.2)
        code = process.wait()
    if result_path.exists():
        record = json.loads(result_path.read_text(encoding='utf-8'))
    else:
        record = {'method': method,'status':stopped or 'process_exit_without_result','csv':str(path)}
    if stopped:
        record['status'] = stopped
    record.update(run=run, exit_code=code, run_configuration=configuration, profile={
        'end_to_end_seconds':time.perf_counter()-start,'peak_process_tree_rss_bytes':peak_rss,
        'peak_process_tree_private_bytes':peak_private,'minimum_system_available_bytes':minimum_available,
        'sample_interval_seconds':0.2,'samples_file':samples_path.relative_to(ROOT).as_posix(),
        'notes':'RSS is sampled, includes imports; summed child RSS can double-count shared pages. One interpreter worker with Dask threads; Windows may add a small Python launcher process.'})
    write_json(result_path,record)
    print(f'{prefix}: {record["status"]}; operation={record.get("operation_seconds",0):.2f}s; peak RSS={peak_rss/1024**3:.2f} GiB',flush=True)
    return record

def validate_records(records, path, is_full):
    from pipeline import compare
    successes = [record for record in records if record['status']=='success']
    if not successes:
        raise AssertionError('No successful runs to validate.')
    for record in successes[1:]:
        compare(successes[0]['aggregation'],record['aggregation'])
    manifest = json.loads(MANIFEST.read_text(encoding='utf-8')) if is_full else {}
    rows = successes[0]['aggregation']['rows']
    if is_full and rows != manifest['expected_rows']:
        raise AssertionError('Parser record count differs from the expected complete HIGGS dataset.')
    validation = {'status':'passed','logical_records':rows,'successful_runs_compared':len(successes),
                  'relative_tolerance':1e-9,'absolute_tolerance':1e-6,
                  'full_input':is_full,'input_bytes':path.stat().st_size,
                  'input_above_5_gib':path.stat().st_size>5*1024**3,
                  'metrics':successes[0]['aggregation']['metrics']}
    return validation

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--csv', type=Path, default=CSV_PATH)
    parser.add_argument('--repeats', type=int, default=3)
    parser.add_argument('--memory-gib', type=float, default=2)
    parser.add_argument('--threads', type=int, default=4)
    parser.add_argument('--blocksize', default='64MiB')
    parser.add_argument('--sample',action='store_true',help='Small correctness run, explicitly excluded from full benchmark.')
    parser.add_argument('--methods', nargs='+',choices=['full','chunks','dask'],default=['full','chunks','dask'])
    args=parser.parse_args()
    if args.repeats<1 or args.threads<1 or args.memory_gib<0:
        parser.error('Invalid repeats, threads, or memory budget.')
    path=args.csv.resolve()
    if not path.is_file():
        parser.error('CSV missing. Run download_data.py prepare first.')
    is_full=not args.sample
    if is_full and (path!=CSV_PATH.resolve() or path.stat().st_size!=CSV_BYTES or not MANIFEST.exists()):
        parser.error('Full benchmark requires the prepared original CSV and manifest.')
    destination=ROOT/'results'/('sample' if args.sample else 'full')
    destination.mkdir(parents=True,exist_ok=True)
    write_json(destination/'environment.json',system_info())
    write_json(destination/'settings.json',{'memory_gib':args.memory_gib,'threads':args.threads,'blocksize':args.blocksize,
            'chunksize':100000,'repeats':args.repeats,'methods':args.methods,'full_input':is_full,
            'cache_policy':'No OS cache eviction. Method order alternates; repetitions are warm/mixed-cache runs.',
            'baseline_policy':'All methods read all 29 numeric columns and compute the same four numeric reductions.'})
    records=[]
    for repeat in range(1,args.repeats+1):
        order=list(args.methods)
        if repeat%2==0: order.reverse()
        for method in order:
            # An OOM baseline needs one real documented failure, not three redundant crashes.
            if repeat>1 and method=='full' and any(r['method']=='full' and r['status']=='oom' for r in records):
                continue
            record=measured_run(method,repeat,path,destination,args.memory_gib,args.threads,args.blocksize)
            records.append(record)
            if record['status'] not in ['success','oom']:
                raise RuntimeError(f'{method} failed: inspect {destination}/{method}_run{repeat}.log')
    validation=validate_records(records,path,is_full)
    write_json(destination/'validation.json',validation)
    write_json(destination/'summary.json',{'runs':records,'validation':validation})
    with (destination/'benchmark.csv').open('w',newline='',encoding='utf-8') as f:
        writer=csv.writer(f)
        writer.writerow(['method','run','status','operation_seconds','end_to_end_seconds','peak_rss_mib','memory_limit_gib','rows'])
        for r in records:
            writer.writerow([r['method'],r['run'],r['status'],r.get('operation_seconds'),r['profile']['end_to_end_seconds'],
                  r['profile']['peak_process_tree_rss_bytes']/1024**2,args.memory_gib,r.get('aggregation',{}).get('rows')])
    print(f'Validated {validation["logical_records"]:,} records; {validation["successful_runs_compared"]} successful runs agree.',flush=True)

if __name__=='__main__':
    main()
