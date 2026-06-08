"""BnB uchun Google Drive va Sheets"""
import io, os, logging
import requests as http

BNB_BOT_TOKEN  = os.environ.get("BNB_BOT_TOKEN", "")
BNB_SHEETS_ID  = os.environ.get("BNB_SHEETS_ID", "1EQEFLi_fBkrT2Yn0CmtzqT-BPEJnUfcZ9zp-lkcTxf8")
FERUDIN_PDF_ID = os.environ.get("FERUDIN_PDF_ID", "1DUt5tdNSnkRrcnLkRid6mLe-GSGtnXuY")

APT_ISH = {
    "23":  "1rNIUhclrRTE_pQHbCK_vofB__Jo04CTS",
    "28":  "1vCUr1oKGBVyKTCzR5Wap7_QULP2B8ZXj",
    "68":  "1VjfWReRMGw-4G02VPe06WKgENZMGx3WK",
    "80":  "1ckAZgI36LOa6yh_gfPLTkVJLvqksBiWL",
    "84":  "1JJIBDZH4Wr__LboHoxMBj9ZZP-Yui8db",
    "88":  "1Z3g5DuQiMuUSN--qZQJJL1YoKnmfyxdq",
    "701": "",
}
APT_CAD = {
    "23":  "1IJPp4NxvyQqIzD2YPgyJ_Qrw6fwMyDZ5",
    "28":  "1P0mcUUDkYeJQtIvxTNZc8vGxUcSJzMp0",
    "68":  "1iWLhmmQkGBac8kObWlsPSg_vYNQNJMLX",
    "80":  "1ZpLPZTq9Zvbls3jHmj2KPmzFQrPJxdKB",
    "84":  "1Pn8ToycF0cuEG4qz-aLRM5Sw7LHDP-I_",
    "88":  "1i-4gZlNCbZW7B9QAW323tqdAgdC_QOQF",
    "701": "",
}

BNB_HEADERS = [
    "Pasport ID", "Ism-familya", "Fuqaroligi", "Tug'ilgan sana",
    "O'zbekistonga kirgan sana", "Apartamentga kirgan sana", "Ketish sanasi",
    "Reg. boshlangan kun", "Reg. tugagan kun", "Kvartira", "Xona",
    "To'lov miqdori", "Kim to'ladi", "Saqlangan sana",
]

def _drive_creds():
    import json
    from google.oauth2.service_account import Credentials
    creds_str = os.environ.get("BNB_CREDS_JSON", "")
    if not creds_str:
        creds_file = os.environ.get("GOOGLE_CREDS_JSON", "")
        if creds_file:
            try: info = json.loads(creds_file)
            except: info = {}
        else: info = {}
    else:
        try: info = json.loads(creds_str)
        except: info = {}
    if not info: raise ValueError("BNB_CREDS_JSON topilmadi")
    return Credentials.from_service_account_info(info, scopes=[
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/spreadsheets",
    ])

def get_drive_file(file_id):
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseDownload
    if not file_id or not file_id.strip():
        raise ValueError("file_id bo'sh")
    service = build("drive", "v3", credentials=_drive_creds(), cache_discovery=False)
    meta = service.files().get(fileId=file_id, fields="mimeType,name").execute()
    mime = meta.get("mimeType", "")
    if mime == "application/vnd.google-apps.document":
        req = service.files().export_media(
            fileId=file_id,
            mimeType="application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        )
    else:
        req = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    dl = MediaIoBaseDownload(fh, req)
    done = False
    while not done: _, done = dl.next_chunk()
    fh.seek(0)
    return fh.read()

def tg_send_file(chat_id, file_bytes, filename, caption="", token=None):
    tok = token or BNB_BOT_TOKEN or os.environ.get("BOT_TOKEN", "")
    try:
        r = http.post(
            f"https://api.telegram.org/bot{tok}/sendDocument",
            data={"chat_id": chat_id, "caption": caption},
            files={"document": (filename, io.BytesIO(file_bytes))},
            timeout=120,
        )
        return r.json()
    except Exception as e:
        logging.error(f"tg_send_file: {e}")
        return {}

def save_bnb_to_sheets(registration):
    import gspread
    from datetime import datetime
    gc = gspread.authorize(_drive_creds())
    sh = gc.open_by_key(BNB_SHEETS_ID)
    ws = sh.sheet1
    if not ws.row_values(1):
        ws.insert_row(BNB_HEADERS, 1)
    for guest in registration.get("guests", []):
        row = [
            guest.get("passportId", ""),
            guest.get("name", ""),
            guest.get("nationality", ""),
            guest.get("dob", ""),
            registration.get("uzbekEntryDate", ""),
            registration.get("aptEntryDate", ""),
            registration.get("departureDate", ""),
            registration.get("regStartDate", ""),
            registration.get("regEndDate", ""),
            registration.get("apartment", ""),
            registration.get("room", ""),
            registration.get("paymentAmount", ""),
            "AirBnB" if registration.get("paymentBy") == "airbnb" else "Mehmon",
            datetime.now().strftime("%Y-%m-%d %H:%M"),
        ]
        ws.append_row(row, value_input_option="RAW")
