"""Prepare <=1 GiB release assets, or restore the complete archive from them."""
from pathlib import Path
import argparse, hashlib, json, shutil
from download_data import ROOT, ZIP_PATH, ZIP_BYTES, write_json, sha256
RELEASE = ROOT/'release'
PART_BYTES = 1024**3

def split():
    if not ZIP_PATH.is_file() or ZIP_PATH.stat().st_size != ZIP_BYTES:
        raise ValueError('Prepare the complete original Kaggle archive first.')
    RELEASE.mkdir(exist_ok=True)
    if shutil.disk_usage(ROOT).free < ZIP_BYTES + 1024**3:
        raise OSError('Insufficient disk space for release parts.')
    parts = []
    archive_hash = hashlib.sha256()
    with ZIP_PATH.open('rb') as source:
        index = 1
        while source.tell() < ZIP_BYTES:
            name = f'{ZIP_PATH.name}.part{index:03d}'
            partial = RELEASE/(name+'.tmp')
            digest = hashlib.sha256()
            size = 0
            with partial.open('wb') as output:
                while size < PART_BYTES:
                    block = source.read(min(8*1024**2, PART_BYTES-size))
                    if not block: break
                    output.write(block); digest.update(block); archive_hash.update(block); size += len(block)
            partial.replace(RELEASE/name)
            parts.append({'name':name,'bytes':size,'sha256':digest.hexdigest()})
            print(f'Release part {index}: {size:,} bytes',flush=True)
            index += 1
    info={'archive_name':ZIP_PATH.name,'archive_bytes':ZIP_BYTES,'archive_sha256':archive_hash.hexdigest(),
          'part_bytes_maximum':PART_BYTES,'parts':parts,'format':'Sequential byte slices: concatenate in manifest order before opening as ZIP.',
          'source':'https://www.kaggle.com/datasets/erikbiswas/higgs-uci-dataset','license':'CC BY 4.0'}
    write_json(RELEASE/'manifest.json',info)
    (RELEASE/'README.md').write_text("""# Full dataset release assets

Download every `higgs.zip.partNNN` file and `manifest.json` from the same GitHub Release into this project's `release/` folder. These are sequential binary slices, not independently extractable ZIPs.

From the project root run:

```powershell
.\\.venv\\Scripts\\python.exe release_data.py restore
.\\.venv\\Scripts\\python.exe download_data.py prepare
```

Restore checks each part and the complete archive SHA-256. Preparation checks the ZIP member CRC and extracts the full original CSV. The license is CC BY 4.0; credit Daniel Whiteson, the UCI HIGGS dataset, and the linked Kaggle mirror. Original publisher license: CC BY 4.0.

No GitHub Release has been published merely by generating these local assets.
""",encoding='utf-8')
    print(f'Prepared {len(parts)} parts and checksums.',flush=True)

def restore():
    info=json.loads((RELEASE/'manifest.json').read_text(encoding='utf-8'))
    if ZIP_PATH.exists():
        if ZIP_PATH.stat().st_size==info['archive_bytes'] and sha256(ZIP_PATH)==info['archive_sha256']:
            print('Original archive is already present and verified.')
            return
        raise ValueError('Existing archive differs; inspect before replacing.')
    if shutil.disk_usage(ROOT).free < info['archive_bytes']+1024**3:
        raise OSError('Insufficient disk space to restore archive.')
    ZIP_PATH.parent.mkdir(parents=True,exist_ok=True)
    partial=ZIP_PATH.with_suffix('.zip.part')
    digest=hashlib.sha256()
    with partial.open('wb') as output:
        for part in info['parts']:
            name=part['name']
            if Path(name).name!=name:
                raise ValueError('Unexpected part path in manifest.')
            path=RELEASE/name
            if path.stat().st_size!=part['bytes']:
                raise ValueError(f'Part size mismatch: {name}')
            part_hash=hashlib.sha256()
            with path.open('rb') as source:
                for block in iter(lambda:source.read(8*1024**2),b''):
                    output.write(block); digest.update(block); part_hash.update(block)
            if part_hash.hexdigest()!=part['sha256']:
                raise ValueError(f'Part checksum mismatch: {name}')
    if partial.stat().st_size!=info['archive_bytes'] or digest.hexdigest()!=info['archive_sha256']:
        raise ValueError('Restored archive checksum or size mismatch.')
    partial.replace(ZIP_PATH)
    print('Original full archive restored and verified.')

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=['split','restore'])
    args=parser.parse_args()
    {'split':split,'restore':restore}[args.action]()
