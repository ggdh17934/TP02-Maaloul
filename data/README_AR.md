# ملفات البيانات

- downloads/higgs.zip: ZIP المصدر، 2.90GB.
- raw/HIGGS.csv: CSV الكامل، 8.04GB و11 مليون صف، دون ترويسة.
- manifest.json: البيانات الوصفية والبصمات والتحقق الفعلي؛ يضاف إلى Git.
- samples/first_100003.csv: عينة للتحقق فقط، مستبعدة من Git.

التنزيل والإعداد: python download_data.py prepare. التحقق المستقل من البصمات: python download_data.py verify.

CSV وZIP والعينة لا تدخل Git. كل الملفات داخل مجلد المشروع. تقرير التجربة يستخدم الملف الكامل لا العينة.
