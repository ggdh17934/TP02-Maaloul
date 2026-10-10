# البيانات المعتمدة: HIGGS

اخترنا النسخة الكاملة بدل Flight Prices لأنها أقل حجمًا وصفوفًا، وتتجاوز شرط الإدخال في PDF.

| الخاصية | القيمة |
| --- | --- |
| المصدر الأصلي | https://archive.ics.uci.edu/dataset/280/higgs |
| نسخة Kaggle | https://www.kaggle.com/datasets/erikbiswas/higgs-uci-dataset |
| إصدار Kaggle | 1 |
| CSV | HIGGS.csv، 8,035,497,980 بايت |
| ZIP | 2,902,133,066 بايت |
| السجلات | 11,000,000 |
| الأعمدة | label + 28 خاصية = 29 |
| الترويسة | لا توجد؛ الأسماء مضبوطة في dataset.py |
| رخصة المصدر الأصلي | CC BY 4.0 |

بيانات محاكاة Monte Carlo لتصنيف أحداث تنتج Higgs مقابل الخلفية. لا يتطلب TP تدريب نموذج. التجميعات المختارة: مجموع ومتوسط label وlepton_pt وm_jj وm_wwbb. متوسط label يعطي نسبة الفئة 1. ترتيب الخصائص مأخوذ من وصف UCI؛ أول 21 خاصية حركية وآخر سبع خصائص مشتقة.

ملف ZIP المضغوط أقل من 5GB، لكن CSV الفعلي الذي نعالجه أكبر من 5GB وحتى 5GiB. التكليف يذكر قراءة dataset 5GB+ بـpd.read_csv ولا يذكر ZIP؛ هذا تفسير نص الوثيقة وليس نقلًا لشرط شفهي إضافي من الأستاذ. لا نكرر السجلات ولا نضيف بيانات اصطناعية لتكبير الحجم.

## النسبة والرخصة

Daniel Whiteson؛ P. Baldi, P. Sadowski, D. Whiteson (2014), Searching for exotic particles in high-energy physics with deep learning، Nature Communications، DOI: https://doi.org/10.1038/ncomms5308.

UCI dataset DOI: https://doi.org/10.24432/C5V312. الرخصة: https://creativecommons.org/licenses/by/4.0/ . يعرض مرآة Kaggle رخصة CC0، لكننا نحافظ على النسبة وشروط CC BY 4.0 المعلنة من الناشر الأصلي. CSV الأصلي محفوظ دون تغيير؛ أسماء الأعمدة تُضاف داخل قارئ Python فقط.

download_data.py يتحقق من حجم ZIP ومن اسم عضو الأرشيف وحجمه ومن CRC أثناء فك الضغط؛ يحسب SHA-256 للـCSV والـZIP ويعد الأسطر. التجارب تعيد عد السجلات بقارئ CSV وتطابقها مع 11 مليون. التفاصيل الفعلية في data/manifest.json.
