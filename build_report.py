"""Build the Arabic report and figures from validated full-dataset results."""
import csv, json, statistics
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from download_data import ROOT, write_json

def main():
    directory=ROOT/'results/full'
    summary=json.loads((directory/'summary.json').read_text(encoding='utf-8'))
    manifest=json.loads((ROOT/'data/manifest.json').read_text(encoding='utf-8'))
    env=json.loads((directory/'environment.json').read_text(encoding='utf-8'))
    settings=json.loads((directory/'settings.json').read_text(encoding='utf-8'))
    runs=summary['runs']; validation=summary['validation']
    if validation['status']!='passed' or not validation['input_above_5_gib']:
        raise ValueError('Full validated results required.')
    grouped={m:[r for r in runs if r['method']==m] for m in ['full','chunks','dask']}
    if not all(grouped.values()): raise ValueError('All three methods must have recorded outcomes.')
    labels={'full':'Standard Pandas','chunks':'Pandas Chunking','dask':'Dask DataFrame'}
    colors={'full':'#e6550d','chunks':'#2b8cbe','dask':'#41ab5d'}
    figures=ROOT/'figures'; figures.mkdir(exist_ok=True)
    success=[m for m in grouped if any(r['status']=='success' for r in grouped[m])]
    medians=[statistics.median(r['operation_seconds'] for r in grouped[m] if r['status']=='success') for m in success]
    low=[min(r['operation_seconds'] for r in grouped[m] if r['status']=='success') for m in success]
    high=[max(r['operation_seconds'] for r in grouped[m] if r['status']=='success') for m in success]
    fig,ax=plt.subplots(figsize=(8,4.5),layout='constrained')
    bars=ax.bar([labels[m] for m in success],medians,color=[colors[m] for m in success],yerr=[[a-b for a,b in zip(medians,low)],[a-b for a,b in zip(high,medians)]],capsize=6)
    for bar,value,upper in zip(bars,medians,high): ax.text(bar.get_x()+bar.get_width()/2,upper+max(high)*.025,f'{value:.1f}s',ha='center',va='bottom')
    ax.set_ylim(0,max(high)*1.15)
    ax.set_ylabel('Read + aggregation time (seconds)'); ax.set_title('Successful full-input runs: median and range'); ax.grid(axis='y',alpha=.2); ax.set_axisbelow(True)
    fig.savefig(figures/'execution_time.png',dpi=160); plt.close(fig)
    fig,ax=plt.subplots(figsize=(8,4.5),layout='constrained')
    peaks=[max(r['profile']['peak_process_tree_rss_bytes'] for r in grouped[m])/1024**3 for m in grouped]
    bars=ax.bar([labels[m]+(' (OOM)' if grouped[m][0]['status']=='oom' else '') for m in grouped],peaks,color=['#e6550d','#2b8cbe','#41ab5d'])
    for bar,value in zip(bars,peaks): ax.text(bar.get_x()+bar.get_width()/2,value,f'{value:.2f} GiB',ha='center',va='bottom')
    ax.set_ylabel('Maximum sampled process-tree RSS (GiB)'); ax.set_title('All 29 numeric columns; same enforced allocation budget'); ax.grid(axis='y',alpha=.2); ax.set_axisbelow(True)
    fig.savefig(figures/'peak_memory.png',dpi=160); plt.close(fig)
    fig,ax=plt.subplots(figsize=(9,4.5),layout='constrained')
    for method,group in grouped.items():
        with (ROOT/group[0]['profile']['samples_file']).open(newline='',encoding='utf-8') as f: samples=list(csv.DictReader(f))
        ax.plot([float(x['elapsed_seconds']) for x in samples],[int(x['process_tree_rss_bytes'])/1024**3 for x in samples],label=labels[method],color=colors[method],linewidth=1.4)
    ax.set_xlabel('Worker wall-clock time including imports (seconds)'); ax.set_ylabel('Sampled process-tree RSS (GiB)'); ax.set_title('Memory traces: first recorded full-input run'); ax.legend(); ax.grid(alpha=.2)
    fig.savefig(figures/'memory_over_time.png',dpi=160); plt.close(fig)
    dask_run=next(r for r in grouped['dask'] if r['status']=='success'); graph=dask_run['graph']
    write_json(directory/'dask_graph.json',graph)
    fig,axes=plt.subplots(1,2,figsize=(11,4.5),layout='constrained')
    axes[0].bar(['Before optimization','After fusion'],[graph['tasks_before'],graph['tasks_after']],color=['#9ecae1','#3182bd'])
    axes[0].set_title('Actual Dask task graph size'); axes[0].set_ylabel('Tasks')
    for i,value in enumerate([graph['tasks_before'],graph['tasks_after']]): axes[0].text(i,value,str(value),ha='center',va='bottom')
    axes[1].axis('off')
    for y,text in [(.8,f'CSV: {graph["partitions"]} partitions'),(.5,'Partition statistics: rows, sums, counts'),(.2,'Tree reduction -> one small result')]:
        axes[1].text(.5,y,text,ha='center',va='center',bbox={'boxstyle':'round,pad=.7','facecolor':'#e5f5e0','edgecolor':'#238b45'},fontsize=10)
    for start,end in [(.71,.58),(.41,.28)]: axes[1].annotate('',xy=(.5,end),xytext=(.5,start),arrowprops={'arrowstyle':'->','lw':1.5})
    axes[1].set_title('Execution structure (schematic)')
    fig.savefig(figures/'dask_optimization.png',dpi=160); plt.close(fig)
    table=[]
    for method,group in grouped.items():
        good=[r for r in group if r['status']=='success']
        if good:
            times=[r['operation_seconds'] for r in good]; status='نجاح'
            timing=f'{statistics.median(times):.2f} ثانية، المدى {min(times):.2f}-{max(times):.2f}'
        else:
            status=group[0]['status']+' / '+group[0].get('exception_type','')
            timing=f'{group[0].get("operation_seconds",0):.2f} ثانية حتى الفشل؛ ليس زمن نجاح'
        peak=max(r['profile']['peak_process_tree_rss_bytes'] for r in group)/1024**3
        table.append(f'| {labels[method]} | {status} | {len(group)} | {timing} | {peak:.3f}GiB |')
    metric_table=[]
    for col,v in validation['metrics'].items():
        mean=f'{v["mean"]:.9g}' if v['mean'] is not None else 'None'
        metric_table.append(f'| {col} | {v["valid_count"]:,} | {v["missing_count"]:,} | {v["sum"]:.9g} | {mean} |')
    chunk=next(r for r in grouped['chunks'] if r['status']=='success'); full=grouped['full'][0]
    failure=('ظهر استثناء تخصيص ذاكرة فعلي `'+full.get('exception_type','')+'` أثناء محاولة pd.read_csv تحميل الملف كاملًا. التتبّع محفوظ في results/full/full_run1.log وJSON.' if full['status']=='oom' else 'نجحت القراءة التقليدية في هذه التجربة؛ لا ندعي حدوث OOM.')
    report=f"""# تقرير TP02: معالجة بيانات إدخال أكبر من 5GB

جامعة الوادي، ماستر ثانية IA & Data Science، Big Data Analytics، السنة 2026/2027.

## البيانات ودليل الحجم

استخدمنا **HIGGS** الكاملة من [نسخة Kaggle](https://www.kaggle.com/datasets/erikbiswas/higgs-uci-dataset)، إصدار 1. [المصدر الأصلي UCI](https://archive.ics.uci.edu/dataset/280/higgs) يصف 11 مليون حدث محاكاة فيزياء: تصنيف label و28 خاصية عددية. نعتمد رخصة المصدر الأصلي CC BY 4.0 وننسب البيانات إلى Daniel Whiteson وورقة Baldi وSadowski وWhiteson (2014)، حتى إن عرضت النسخة المرآة رخصة مختلفة.

- الإدخال: data/raw/HIGGS.csv.
- حجم CSV الفعلي: **{manifest['csv_bytes']:,} بايت = {manifest['csv_bytes']/1e9:.3f}GB = {manifest['csv_bytes']/1024**3:.3f}GiB**.
- حجم الأرشيف: **{manifest['archive_bytes']:,} بايت = {manifest['archive_bytes']/1e9:.3f}GB**.
- السجلات المنطقية التي أحصاها قارئ CSV: **{validation['logical_records']:,}**. الملف دون ترويسة.
- الأعمدة: **{manifest['column_count']}**.
- CRC للعضو داخل ZIP تحقق بقراءته حتى النهاية. SHA-256 للـCSV: `{manifest['csv_sha256']}`.

ملف data/manifest.json يضم المصدر والحجم والبصمات وأسماء الأعمدة المعرّفة صراحةً في dataset.py. البصمة تعرّف النسخة المحلية ولا تستبدل checksum ناشر إن لم ينشره. شرط 5GB يخص **الإدخال**؛ النتائج تجميعات صغيرة. العينة الصغيرة اختبرت صحة التنفيذ ولم تستبدل البيانات الكاملة.

ملف ZIP أصغر من 5GB لأن الضغط يقلل الحجم؛ ملف CSV الذي نقرأه يتجاوز حتى 5GiB. نص التكليف يقول قراءة dataset أكبر من 5GB بواسطة pd.read_csv ولا يحدد حدًا لحجم ZIP.

## البيئة والقياس

النظام `{env['os']}`، Python {env['python']}، RAM نحو {env['ram_total_bytes']/1024**3:.2f}GiB، {env['physical_cores']} أنوية فيزيائية و{env['logical_processors']} معالجًا منطقيًا.

الإصدارات: {', '.join(n+' '+v for n,v in env['packages'].items())}. requirements.txt يثبت جميع الإصدارات.

كل الطرق تقرأ **كل الأعمدة الـ29** بنفس النوع float64 لكل الأعمدة، وتجميع أربعة أعمدة عددية هي label وlepton_pt وm_jj وm_wwbb. نفس التجميعات تطبق على كل السجلات، ولا نتخطى الأسطر المعطوبة.

تستخدم التجارب **ميزانية تخصيص للعملية قدرها {settings['memory_gib']}GiB** مفروضة بواسطة Windows Job Object. هذا حد للذاكرة الملتزمة/التخصيص، وليس نفس قيمة RSS. الجهاز نفسه لديه ذاكرة أكبر؛ النتائج تصف البيئة المحددة ولا تثبت أن التحميل يفشل دون حد أو على كل جهاز.

الزمن التشغيلي للقراءة والتجميع يقاس بعد الاستيراد؛ الزمن الكامل محفوظ كذلك. قمة RSS معاينة كل 0.2 ثانية وتشمل الاستيراد. قد تفوت قمم أقصر من الفاصل. جمع RSS للأبناء قد يحسب صفحات مشتركة أكثر من مرة؛ التنفيذ عامل Python واحد مع threads؛ قد يضيف Windows عملية launcher صغيرة.

لم نفرغ cache نظام الملفات. ترتيب الطرق يتبدل عبر التكرارات؛ النتائج cache مختلط/متكرر وليست cold-cache مضبوطة. التنزيل وفك الضغط خارج زمن التجميع.

CSV بلا ترويسة؛ header=None وnames=COLUMNS يمنعان إسقاط أول سجل. لا نكرر الصفوف ولا نضيف padding لبلوغ الحجم المطلوب.

## Task 1: القراءة التقليدية

استدعاء pd.read_csv على كل الملف دفعة واحدة. {failure}

اختُبرت آلية الحد بتخصيص معروف الحجم وأنتجت MemoryError فعليًا. لا نفتعل الخطأ ولا نستهلك معظم RAM الجهاز. guard أو timeout لا يصنف تلقائيًا OOM.

## Task 2: Pandas Chunking

chunksize=100,000 بالضبط. عدد الأجزاء **{chunk['chunk_count']:,}** وأكبر جزء **{chunk['max_chunk_rows']:,}** سطر. يحرر الجزء بعد حساب إحصاءاته، ولا يعاد تكوين الجدول الكامل.

المتوسط = المجموع / عدد القيم الصالحة، لا متوسط متوسطات الأجزاء. نستخدم math.fsum لمجاميع الأجزاء ونحتفظ فقط بإحصاءات صغيرة لكل جزء.

## Task 3: Dask والتحسين

dd.read_csv ينشئ **{graph['partitions']} partitions** بحجم blocksize=64MiB. الحجم بالبايت ويختلف عن chunksize بالأسطر. الإنشاء قد يقرأ عينة وmetadata، والمسح الكامل يحدث عند compute.

كل partition يتحول إلى إحصاءات صغيرة ثم تجميع شجري split_every=8. المقاييس تنفذ ضمن رسم واحد. query.optimize(fuse=True) خفض الرسم من **{graph['tasks_before']}** إلى **{graph['tasks_after']}** مهمة. التنفيذ threaded scheduler بعدد {settings["threads"]} threads. لا نحمل كامل الجدول بـddf.compute ولا persist.

نحافظ على قراءة كل الأعمدة للمقارنة العادلة؛ إسقاط الأعمدة ليس مختلطًا مع هذه التجربة. التعبيرات وأعداد المهام وعينة الاعتماديات في results/full/dask_graph.json.

![تحسين Dask](../figures/dask_optimization.png)

## النتائج الفعلية

| الطريقة | الحالة | محاولات مسجلة | زمن القراءة والتجميع | أكبر RSS معاين |
| --- | --- | --- | --- | --- |
{chr(10).join(table)}

زمن OOM هو زمن حتى الفشل وليس زمن نجاح؛ لا نحسب speedup مقابل محاولة فاشلة. تفوق Dask يستنتج من القياس ولا يفترض من جدول التكليف.

![زمن التنفيذ](../figures/execution_time.png)

![قمة الذاكرة](../figures/peak_memory.png)

![سلوك الذاكرة](../figures/memory_over_time.png)

متوسط label يمثل نسبة الأحداث ذات الفئة 1، وليس دقة نموذج تصنيف. لا يتطلب هذا TP تدريب نموذج.

## صحة النتائج

العدد المنطقي {validation['logical_records']:,} يطابق عدد الأسطر المادية: 11 مليون سجل دون ترويسة. تطابقت النتائج عبر **{validation['successful_runs_compared']} محاولات ناجحة** بسماحية relative 1e-9 وabsolute 1e-6 بسبب ترتيب جمع float64.

| العمود | صالح | مفقود | المجموع | المتوسط |
| --- | --- | --- | --- | --- |
{chr(10).join(metric_table)}

نجحت ستة اختبارات: الأجزاء غير المتساوية والقيم المفقودة، تطابق الطرق عبر عدة partitions، رفض نتيجة خاطئة، رفض عمود مطلوب مفقود، عمود كله مفقود، وإنفاذ حد الذاكرة. عينة البيانات الفعلية ذات 100,003 سجل تطابقت فيها الطرق الثلاث أيضًا. نتائجها منفصلة في results/sample.

## مطابقة التكليف

| المطلب | الدليل |
| --- | --- |
| بيانات إدخال أكبر من 5GB | CSV وdata/manifest.json |
| القراءة التقليدية وتوثيق النتيجة | worker.py وسجل full_run1 |
| chunks من 100,000 وتجميع تراكمي | pipeline.py ونتائج chunks |
| Dask والتقييم الكسول وتحسين الرسم | pipeline.py وdask_graph.json والرسم |
| الزمن واستهلاك RAM | benchmark.csv وملفات الذاكرة والرسوم |
| تشغيل قابل للإعادة | README.md وrequirements.txt |
| رفع العمل إلى GitHub | حالة وروابط النشر الفعلية في SUBMISSION_AR.md |

البيانات والبيئة والملفات المؤقتة لا تدخل تاريخ Git. البيانات المضغوطة مقسمة إلى أجزاء <=1GiB لإرفاقها في Release تابع للمستودع نفسه، مع بصمات لإعادة التجميع. جميع الملفات المحلية داخل المشروع على E. توليد التقرير لا يعني اكتمال النشر.
"""
    (ROOT/'report').mkdir(exist_ok=True)
    (ROOT/'report/REPORT_AR.md').write_text(report,encoding='utf-8')
    write_json(directory/'requirements_checklist.json',{'input_above_5_gib':True,'all_three_methods_recorded':True,'traditional_outcome':full['status'],'traditional_exception':full.get('exception_type'),'controlled_allocation_limit_gib':settings['memory_gib'],'chunksize_100000':chunk['chunksize']==100000,'lazy_dask_and_optimization_recorded':True,'time_and_rss_profiled':True,'numerical_validation_passed':True,'github_published':'See SUBMISSION_AR.md'})
    print('Arabic report and four figures generated from actual validated results.')

if __name__=='__main__': main()
