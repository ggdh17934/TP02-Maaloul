# TP02 — معالجة ملف HIGGS أكبر من 5GB

مشروع Big Data Analytics، ماستر ثانية IA & Data Science، جامعة الوادي، 2026/2027.

[المستودع العام](https://github.com/ggdh17934/TP02-Maaloul) — [الشرح العربي الشامل](TP02_FULL_DESCRIPTION_AR.md) — [التقرير العربي](report/REPORT_AR.md) — [البيانات الكاملة في Release](https://github.com/ggdh17934/TP02-Maaloul/releases/tag/v1.0.0)

نقارن قراءة CSV كاملًا بواسطة Pandas، القراءة بدفعات 100,000 صف، والتنفيذ الكسول والمتوازي بواسطة Dask. الكود والنتائج والتقرير والتكليف في مستودع واحد؛ ملفات البيانات الكبيرة تُحمّل إلى مجلد المشروع ولا تدخل تاريخ Git.

## البيانات المختارة

- HIGGS كاملة: **11,000,000 صف و29 عمودًا عدديًا**، دون ترويسة.
- CSV: **8,035,497,980 بايت = 8.04GB ≈ 7.48GiB**.
- ZIP من Kaggle: **2,902,133,066 بايت = 2.90GB**.
- [نسخة Kaggle](https://www.kaggle.com/datasets/erikbiswas/higgs-uci-dataset)، الإصدار 1.
- [المصدر الأصلي UCI](https://archive.ics.uci.edu/dataset/280/higgs)، رخصة CC BY 4.0.

شرط التكليف يتعلق بالإدخال الذي نقرأه بواسطة pd.read_csv؛ الوثيقة لا تنص على حجم ZIP. ملف CSV يتجاوز 5GB وحتى 5GiB. النتائج تجميعات صغيرة. 100,000 هو حجم كل دفعة وليس عدد الصفوف الكلي.

## المتطلبات والمساحة

Windows 64-bit وPython 3.13. لا نحتاج SQL Server أو قاعدة بيانات منفصلة؛ البيانات جدول CSV. تم تثبيت المكتبات المطلوبة داخل `.venv`: pandas وdask[complete] وpsutil وmatplotlib وpyarrow؛ الإصدارات مثبتة في requirements.txt.

الحد الأدنى للبيانات المضغوطة والمفكوكة معًا 10.94GB. خصص نحو **20GB مساحة حرة على الأقل** للإعداد من الصفر، أو **25GB مساحة مريحة** مع أجزاء Release؛ يشترط البرنامج هامش 5GiB قبل فك الضغط. الحجم النهائي المقاس مع البيئة والأجزاء نحو 14.28GB. البيئة الحالية فقط تُستخدم لهذا المشروع؛ ملفات cache والتجارب داخل tmp/. لا توجد رسوم تدريب أو GPU مطلوب.

## التشغيل من PowerShell

افتح الطرفية في مجلد المشروع ثم:

```powershell
python -m venv .venv
New-Item -ItemType Directory -Force tmp\pip | Out-Null
$env:TEMP = Join-Path $PWD 'tmp\pip'
$env:TMP = $env:TEMP
.\.venv\Scripts\python.exe -m pip install --no-cache-dir -r requirements.txt
.\.venv\Scripts\python.exe download_data.py prepare
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
.\.venv\Scripts\python.exe main.py --repeats 3 --memory-gib 2 --threads 4 --blocksize 64MiB
.\.venv\Scripts\python.exe build_report.py
```

إذا كانت `.venv` موجودة والمكتبات مثبتة، يمكن تشغيل `powershell -ExecutionPolicy Bypass -File .\run.ps1` لتنفيذ تنزيل البيانات والاختبارات والتجارب وتوليد التقرير. لا ينشر هذا الأمر إلى GitHub.

لإعادة حساب بصمات ZIP وCSV: `.\.venv\Scripts\python.exe download_data.py verify`.

**تجربة OOM مضبوطة:** يفرض Windows Job Object حد تخصيص 2GiB على كل عامل قياس. هذا ليس RAM الجهاز، وليس RSS؛ التقرير يذكر الفرق. دون الحد قد ينجح Pandas على جهاز 32GB. للتجربة على RAM الجهاز يمكن استخدام `--memory-gib 0`، لكن ستتغير النتائج ولا ينبغي خلطها بالتقرير المسجل. المخططات تُبنى من القياسات الفعلية ولا تفترض أن Dask أسرع دائمًا.

## دليل الملفات للأستاذ

| الملف | الغرض |
| --- | --- |
| TP02 How to Work with Big Data Files ( 5GB).pdf | التكليف الأصلي |
| report/REPORT_AR.md | التقرير والنتائج الفعلية بالعربية |
| dataset.py | أسماء الأعمدة؛ CSV بلا ترويسة |
| pipeline.py | الطرق الثلاث والتجميعات ومقارنة النتائج |
| main.py / worker.py / memory_limit.py | القياس المعزول وحد الذاكرة وتسجيل الأخطاء |
| download_data.py | تنزيل قابل للاستئناف وفك الضغط والتحقق |
| data/manifest.json | الحجم الحقيقي والصفوف والبصمات والمصدر |
| results/full/ | الزمن وRSS والسجلات وJSON والتحقق والرسم الكسول |
| results/sample/ | تحقق صغير منفصل؛ ليس بديلًا لتجربة 8GB |
| figures/ | مخططات الزمن والذاكرة وتحسين Dask |
| tests/test_pipeline.py | صحة التجميع والتعامل مع المفقود وحد الذاكرة |
| release_data.py | تقسيم ZIP إلى أجزاء ≤1GiB والتحقق عند التجميع |
| SUBMISSION_AR.md | حالة الرفع الفعلية؛ لا نفترض أن العمل منشور |

حساب المتوسط العالمي = مجموع القيم / عدد القيم الصالحة. نقارن الأعداد بالضبط، والمجاميع والمتوسطات بسماحية float64 محددة. جميع الطرق تقرأ الأعمدة الـ29 كاملة بنفس النوع.

## البيانات وGitHub

الـCSV والـZIP والبيئة لا تدخل Git. [GitHub يمنع الملفات العادية الأكبر من 100MiB](https://docs.github.com/en/repositories/working-with-files/managing-large-files/about-large-files-on-github)، و[كل ملف Release يجب أن يكون أصغر من 2GiB](https://docs.github.com/en/repositories/releasing-projects-on-github/about-releases)، لذلك نجهز أجزاء بحجم أقصى 1GiB دون Git LFS. يستطيع الأستاذ الحصول على نفس النسخة بتشغيل download_data.py دون بيانات اعتماد Kaggle وفق endpoint العام الذي تحققنا منه. المصدر والإصدار والبصمات موثقة. قد يتغير إتاحة endpoint في المستقبل.

لإعداد ملفات بيانات في Release تابع للمستودع نفسه، شغّل `release_data.py split`. لإعادة التجميع بعد تنزيل الأجزاء وmanifest إلى release/: `release_data.py restore` ثم `download_data.py prepare`. تفاصيل المصدر والرخصة في DATASET_AR.md. نُشر الكود والتقرير والنتائج إلى المستودع العام، والبيانات المضغوطة إلى Release v1.0.0 في المستودع نفسه. تفاصيل التسليم في [SUBMISSION_AR.md](SUBMISSION_AR.md).

كل ملفات المشروع المحلية داخل هذا المجلد على E. التجربة القديمة محفوظة مؤقتًا في archive/ ومستبعدة من Git؛ لا تدخل التقرير الحالي.
