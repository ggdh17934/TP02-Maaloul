"""Download and prepare the complete Kaggle mirror of UCI HIGGS, with resumable I/O."""
from pathlib import Path
import argparse, csv, hashlib, json, re, shutil, time, urllib.request, zipfile
from dataset import COLUMNS, EXPECTED_ROWS
ROOT = Path(__file__).resolve().parent
URL = 'https://www.kaggle.com/api/v1/datasets/download/erikbiswas/higgs-uci-dataset?datasetVersionNumber=1'
ZIP_BYTES = 2_902_133_066
CSV_BYTES = 8_035_497_980
ZIP_PATH = ROOT / 'data/downloads/higgs.zip'
CSV_PATH = ROOT / 'data/raw/HIGGS.csv'
MANIFEST = ROOT / 'data/manifest.json'

def write_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + '.tmp')
    temp.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding='utf-8')
    temp.replace(path)

def sha256(path):
    digest = hashlib.sha256()
    with path.open('rb') as f:
        for block in iter(lambda: f.read(8 * 1024**2), b''):
            digest.update(block)
    return digest.hexdigest()

def download():
    ZIP_PATH.parent.mkdir(parents=True, exist_ok=True)
    if ZIP_PATH.exists():
        if ZIP_PATH.stat().st_size != ZIP_BYTES or not zipfile.is_zipfile(ZIP_PATH):
            raise ValueError('Existing archive is not the expected complete ZIP. Inspect before replacing.')
        print('Archive already complete; skipping download.', flush=True)
        return
    partial = ZIP_PATH.with_suffix('.zip.part')
    started = time.perf_counter()
    initial = partial.stat().st_size if partial.exists() else 0
    # Small independent ranges avoid per-connection throttling; bounded memory.
    from concurrent.futures import ThreadPoolExecutor, as_completed
    parts_dir = ROOT / 'tmp/higgs-transfer'
    parts_dir.mkdir(parents=True, exist_ok=True)
    if initial > ZIP_BYTES:
        raise ValueError('Partial archive exceeds the expected size.')
    ranges = [(offset, min(offset + 4*1024**2, ZIP_BYTES))
              for offset in range(initial, ZIP_BYTES, 4*1024**2)]
    # Resolve Kaggle once, then use its signed storage redirect for each range.
    for attempt in range(6):
        try:
            with urllib.request.urlopen(urllib.request.Request(URL, headers={'Range':'bytes=0-63'}), timeout=30) as first:
                transfer_url = first.geturl()
            break
        except Exception:
            if attempt == 5: raise
            time.sleep(min(2**attempt,8))
    def fetch(bounds):
        offset, stop = bounds
        target = parts_dir / f'{offset:012d}.bin'
        if target.exists() and target.stat().st_size == stop-offset:
            return target, stop-offset
        for attempt in range(6):
            temp = target.with_suffix('.tmp')
            try:
                request = urllib.request.Request(transfer_url,headers={'Range':f'bytes={offset}-{stop-1}'})
                with urllib.request.urlopen(request,timeout=30) as response:
                    match = re.fullmatch(r'bytes (\d+)-(\d+)/(\d+)',response.headers.get('Content-Range',''))
                    if response.status != 206 or not match or tuple(map(int,match.groups())) != (offset,stop-1,ZIP_BYTES):
                        raise ValueError('Incorrect range returned by server.')
                    with temp.open('wb') as output:
                        remaining = stop-offset
                        while remaining:
                            block = response.read(min(1024**2,remaining))
                            if not block:raise OSError('Incomplete byte range.')
                            output.write(block);remaining -= len(block)
                temp.replace(target)
                return target,stop-offset
            except Exception:
                if attempt==5:raise
                time.sleep(min(2**attempt,8))
    completed = initial
    last_report = 0
    failures = []
    with ThreadPoolExecutor(max_workers=16) as pool:
        futures = [pool.submit(fetch,bounds) for bounds in ranges]
        bounds_by_future = dict(zip(futures, ranges))
        for future in as_completed(futures):
            try:
                _, count = future.result()
                completed += count
            except Exception as error:
                failures.append(bounds_by_future[future])
                print(f'Range retry scheduled: {type(error).__name__}',flush=True)
            now = time.perf_counter()
            if now-last_report >= 15 or completed == ZIP_BYTES:
                print(f'Download {completed/ZIP_BYTES:.1%}: {completed/1e9:.3f}/{ZIP_BYTES/1e9:.3f} GB; {(completed-initial)/max(now-started,.001)/1e6:.2f} MB/s',flush=True)
                last_report=now
    for bounds in failures:
        fetch(bounds)
    with partial.open('ab') as output:
        for offset,stop in ranges:
            part=parts_dir/f'{offset:012d}.bin'
            with part.open('rb') as source:
                shutil.copyfileobj(source,output,length=1024**2)
    if partial.stat().st_size == ZIP_BYTES:
        for offset,_ in ranges:(parts_dir/f'{offset:012d}.bin').unlink()
    if partial.stat().st_size != ZIP_BYTES or not zipfile.is_zipfile(partial):
        raise ValueError('Downloaded file failed size or ZIP signature validation.')
    partial.replace(ZIP_PATH)
    print('Download complete.', flush=True)

def prepare():
    download()
    if CSV_PATH.exists():
        if CSV_PATH.stat().st_size != CSV_BYTES:
            raise ValueError('Existing CSV size differs from published size. Inspect before replacing.')
        if MANIFEST.exists():
            old = json.loads(MANIFEST.read_text(encoding='utf-8'))
            if old.get('zip_crc_verified') and old.get('csv_bytes') == CSV_BYTES and old.get('csv_path') == CSV_PATH.relative_to(ROOT).as_posix():
                print('CSV already extracted and verified; skipping preparation.', flush=True)
                return
        raise ValueError('Existing CSV has no completed verification manifest. Use verify command.')
    if shutil.disk_usage(ROOT).free < CSV_BYTES + 5 * 1024**3:
        raise OSError('Insufficient disk space for the full CSV and working margin.')
    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    partial = CSV_PATH.with_suffix('.csv.part')
    digest = hashlib.sha256()
    line_count = 0
    total = 0
    started = time.perf_counter()
    last_report = started
    last_byte = b''
    with zipfile.ZipFile(ZIP_PATH) as archive:
        if archive.namelist() != ['HIGGS.csv']:
            raise ValueError('Unexpected archive members; no extraction performed.')
        info = archive.getinfo('HIGGS.csv')
        if info.file_size != CSV_BYTES:
            raise ValueError('ZIP member size differs from Kaggle file metadata.')
        with archive.open(info) as source, partial.open('wb') as output:
            while True:
                block = source.read(8 * 1024**2)
                if not block:
                    break
                output.write(block)
                digest.update(block)
                line_count += block.count(b'\n')
                total += len(block)
                last_byte = block[-1:]
                now = time.perf_counter()
                if now - last_report >= 15:
                    print(f'Extract {total / CSV_BYTES:.1%}: {total / 1e9:.2f}/{CSV_BYTES / 1e9:.2f} GB', flush=True)
                    last_report = now
        # ZipExtFile verifies the member CRC when it is read to EOF.
        crc = info.CRC
    if total != CSV_BYTES or total <= 5 * 1024**3:
        raise ValueError('Extracted CSV does not satisfy published size and >5 GiB input requirement.')
    partial.replace(CSV_PATH)
    with CSV_PATH.open('r', encoding='utf-8', newline='') as f:
        first_row = next(csv.reader(f))
    if len(first_row) != len(COLUMNS):
        raise ValueError('Unexpected CSV schema.')
    for value in first_row:
        float(value)
    columns = COLUMNS
    physical_lines = line_count + (bool(last_byte) and last_byte != b'\n')
    if physical_lines != EXPECTED_ROWS:
        raise ValueError('Unexpected HIGGS row count.')
    manifest = {
        'dataset': 'HIGGS', 'kaggle_ref': 'erikbiswas/higgs-uci-dataset', 'version': 1,
        'source': 'https://www.kaggle.com/datasets/erikbiswas/higgs-uci-dataset',
        'license': 'CC BY 4.0 (original UCI publisher)', 'original_source': 'https://archive.ics.uci.edu/dataset/280/higgs', 'download_url': URL,
        'archive_path': ZIP_PATH.relative_to(ROOT).as_posix(), 'archive_bytes': ZIP_BYTES,
        'archive_sha256': sha256(ZIP_PATH), 'csv_path': CSV_PATH.relative_to(ROOT).as_posix(),
        'csv_bytes': total, 'csv_sha256': digest.hexdigest(), 'zip_crc32': f'{crc:08x}',
        'zip_crc_verified': True, 'physical_data_lines': physical_lines, 'has_header': False, 'expected_rows': EXPECTED_ROWS,
        'physical_line_count_note': 'No header. HIGGS numeric records have no quoted multiline fields; parser independently counts logical records.',
        'columns': columns, 'column_count': len(columns), 'input_above_5_gib': True,
        'prepared_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'extraction_seconds': time.perf_counter() - started}
    write_json(MANIFEST, manifest)
    print(f'Prepared full CSV: {total:,} bytes, {physical_lines:,} physical data lines; CRC verified.', flush=True)

def verify():
    manifest = json.loads(MANIFEST.read_text(encoding='utf-8'))
    for path, key in [(CSV_PATH, 'csv'), (ZIP_PATH, 'archive')]:
        if path.stat().st_size != manifest[key + '_bytes'] or sha256(path) != manifest[key + '_sha256']:
            raise ValueError(f'{key} checksum or size mismatch.')
    print('Archive and CSV SHA-256 match the preparation manifest.')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', nargs='?', choices=['prepare', 'download', 'verify'], default='prepare')
    args = parser.parse_args()
    {'prepare': prepare, 'download': download, 'verify': verify}[args.action]()
