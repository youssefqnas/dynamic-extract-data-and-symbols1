# استخدم أحدث إصدار رسمي من بايثون كصورة أساسية
FROM python:3.11-slim

# تعيين مجلد العمل داخل الحاوية
WORKDIR /app

# نسخ ملف متطلبات بايثون أولاً للاستفادة من التخزين المؤقت لـ Docker
COPY requirements.txt .

# تثبيت المكتبات المطلوبة
RUN pip install --no-cache-dir -r requirements.txt

# نسخ باقي كود التطبيق إلى مجلد العمل
COPY . .

# الأمر الذي سيتم تشغيله عند بدء تشغيل الحاوية
CMD ["python", "main.py"]