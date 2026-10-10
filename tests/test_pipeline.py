import csv, math, subprocess, sys, tempfile, unittest
from pathlib import Path
import pandas as pd
from pipeline import METRICS, compare, finish, run_full, run_chunks, run_dask

class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[1]/'tmp')
        self.path = Path(self.temp.name)/'fixture.csv'
        from dataset import COLUMNS
        with self.path.open('w', newline='', encoding='utf-8') as source:
            writer = csv.writer(source)
            for i in range(7):
                row = {col: i / 10 for col in COLUMNS}
                row.update(label=i % 2, lepton_pt=i+1, m_jj='' if i in [1,3] else i,
                           m_wwbb=1000 if i==6 else i+1)
                writer.writerow([row[col] for col in COLUMNS])
    def tearDown(self):
        self.temp.cleanup()
    def test_unequal_chunks_and_missing_values(self):
        result=run_chunks(self.path,chunksize=3)
        a=result['aggregation']
        self.assertEqual(result['chunk_count'],3)
        self.assertEqual(a['rows'],7)
        self.assertEqual(a['metrics']['m_wwbb']['sum'],1021)
        self.assertAlmostEqual(a['metrics']['m_wwbb']['mean'],1021/7)
        self.assertEqual(a['metrics']['m_jj']['valid_count'],5)
        self.assertEqual(a['metrics']['m_jj']['missing_count'],2)
    def test_three_methods_match_across_partitions(self):
        reference=run_full(self.path)['aggregation']
        self.assertTrue(compare(reference,run_chunks(self.path,chunksize=3)['aggregation']))
        result=run_dask(self.path,blocksize=256,threads=2)
        self.assertGreater(result['graph']['partitions'],1)
        self.assertTrue(compare(reference,result['aggregation']))
    def test_wrong_result_is_rejected(self):
        reference=run_full(self.path)['aggregation']
        changed=run_full(self.path)['aggregation']
        changed['metrics']['m_wwbb']['sum']+=10
        with self.assertRaises(AssertionError): compare(reference,changed)
    def test_missing_required_column_rejected(self):
        from dataset import COLUMNS
        frame=pd.read_csv(self.path, header=None, names=COLUMNS)
        frame.drop(columns=['m_wwbb']).to_csv(self.path,index=False,header=False)
        with self.assertRaises(ValueError): run_chunks(self.path,chunksize=3)
    def test_all_missing_column_mean_is_none(self):
        from dataset import COLUMNS
        frame=pd.read_csv(self.path, header=None, names=COLUMNS)
        frame['m_jj']=float('nan')
        frame.to_csv(self.path,index=False,header=False)
        result=run_chunks(self.path,chunksize=3)['aggregation']['metrics']['m_jj']
        self.assertEqual(result['valid_count'],0)
        self.assertIsNone(result['mean'])
    @unittest.skipUnless(sys.platform=='win32','Windows Job Object experiment')
    def test_memory_limit_causes_real_memoryerror(self):
        code='from memory_limit import set_memory_limit; set_memory_limit(64*1024**2);\ntry: x=bytearray(128*1024**2)\nexcept MemoryError: print("EXPECTED_OOM"); raise SystemExit(0)\nraise SystemExit(1)'
        result=subprocess.run([sys.executable,'-c',code],cwd=Path(__file__).resolve().parents[1],capture_output=True,text=True)
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertIn('EXPECTED_OOM',result.stdout)

if __name__=='__main__': unittest.main()
