# PDF Accounting Importer — PEAK A–U (Starter)

เว็บแอปสำหรับ “อัปโหลดเอกสารบัญชีหลายไฟล์” แล้วแปลงเป็น Template Excel/CSV (PEAK Import A–U)

✅ ทำได้ตอนนี้ (พร้อมใช้)
- Upload หลายไฟล์พร้อมกัน (PDF/รูปภาพ)
- จำแนกประเภทเอกสารแบบ rule-based (Shopee / Lazada / TikTok / Other / Ads / Unknown)
- Extract ข้อมูลพื้นฐานจาก PDF แบบ “มี text” (pdfplumber)
- สร้างผลลัพธ์ A–U ตามฟอร์ม PEAK และ export เป็น CSV/XLSX
- UI สวยแบบ Light Glass: ซ้าย = upload/queue, ขวา = ตาราง + filter + download

✅ เพิ่มแล้ว (AI OCR + LLM เติมช่อง)
- รองรับไฟล์สแกน/รูปภาพ/ PDF ที่ไม่มี text ด้วย Google Document AI OCR (เปิดด้วย env)
- ใช้ LLM เติมช่องที่ขาด/ผิดกฎ และจัดกลุ่มค่าใช้จ่ายอัตโนมัติ (เปิดด้วย env)

---

## Run ด้วย Docker (แนะนำ)
```bash
docker compose up --build
```
- Frontend: http://localhost:5173
- Backend: http://localhost:8000

ลองเช็กสุขภาพ API:
- http://localhost:8000/api/health

---

## Run แบบ Local
### Backend
```bash
cd backend
python -m venv .venv
# Windows: .venv\Scripts\activate
# mac/linux: source .venv/bin/activate
pip install -r requirements.txt
# (optional) ตั้งค่า env เพื่อเปิด OCR/LLM ตามด้านล่าง
uvicorn app.main:app --reload --port 8000
```

### Frontend
```bash
cd frontend/public
python -m http.server 5173
```

---

## จุดที่เราจะพัฒนาต่อ (เฟส 2)
- OCR: เพิ่ม provider อื่นๆ (Vision / Azure / PaddleOCR)
- LLM: เพิ่มการอธิบาย + confidence ต่อ field + human-in-the-loop review
- Dictionary กลาง (Vendor/TaxID/Branch → GL Code/Account/Group)
- Template Editor: ให้ user ปรับ mapping A–U ได้บนหน้าเว็บ
- Review UI: ไฮไลท์ช่องผิดกฎ + ให้แก้ในเว็บ แล้ว export ใหม่

---

## เปิดใช้งาน AI OCR + LLM (แนะนำสำหรับความแม่นยำ)

### 1) เปิด OCR (Google Document AI)
เตรียม Google Cloud Project + Document AI Processor (OCR Processor หรือ Invoice Parser ก็ได้) แล้วตั้งค่า env:

```bash
set ENABLE_OCR=1
set OCR_PROVIDER=document_ai
set DOC_AI_PROJECT_ID=YOUR_PROJECT
set DOC_AI_LOCATION=us
set DOC_AI_PROCESSOR_ID=YOUR_PROCESSOR_ID
set GOOGLE_APPLICATION_CREDENTIALS=C:\path\to\service-account.json
```

### 2) เปิด LLM เติมช่อง + จัดกลุ่ม (OpenAI)
```bash
set ENABLE_LLM=1
set OPENAI_API_KEY=YOUR_KEY
set OPENAI_MODEL=gpt-4o-mini
```

> ถ้าไม่ตั้ง env เหล่านี้ ระบบจะทำงานแบบ rule-based เหมือนเดิม และไฟล์สแกนจะถูกจัดเป็น NEEDS_REVIEW
