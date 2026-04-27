# api.py — Family Accounting REST API
# FastAPI + Google Sheets
# Railway da main.py bilan birga ishlaydi

import os, json, logging
from datetime import datetime
from typing import Optional
import pytz
import gspread
from google.oauth2.service_account import Credentials
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SPREADSHEET_ID = os.environ['SPREADSHEET_ID']
CREDS_JSON     = os.environ['GOOGLE_CREDS_JSON']
TZ             = pytz.timezone('Asia/Tashkent')

app = FastAPI(title='Family Accounting API')

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_methods=['*'],
    allow_headers=['*'],
)

# ── GOOGLE SHEETS ──────────────────────────────────────────
def get_ss():
    info  = json.loads(CREDS_JSON)
    creds = Credentials.from_service_account_info(info, scopes=[
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive'
    ])
    return gspread.authorize(creds).open_by_key(SPREADSHEET_ID)

def fmt_num(s):
    try:
        return float(str(s).replace(' ','').replace(',','.').replace('$','').replace("so'm",''))
    except: return 0.0

def today_str():
    return datetime.now(TZ).strftime('%d.%m.%Y')

def norm_date(s):
    s = str(s).strip()
    if len(s) == 10 and s[2] == '.' and s[5] == '.': return s
    for fmt in ['%d/%m/%Y','%m/%d/%Y','%Y-%m-%d']:
        try:
            from datetime import datetime as dt2
            return dt2.strptime(s, fmt).strftime('%d.%m.%Y')
        except: pass
    try:
        from datetime import timedelta, datetime as dt2
        return (dt2(1899,12,30) + timedelta(days=int(float(s)))).strftime('%d.%m.%Y')
    except: pass
    return s

# ── MODELS ─────────────────────────────────────────────────
class Transaction(BaseModel):
    type:    str  # CHIQIM | KIRIM
    sana:    str  # DD.MM.YYYY
    egasi:   str  # FERUDIN | GULOYIM
    tur:     str  # xarajat/kirim turi
    tolov:   str  # CASH | CARD | OTHER
    valyuta: str  # USD | UZS
    summa:   float
    note:    Optional[str] = ''

class UpdateTransaction(BaseModel):
    egasi:   Optional[str] = None
    tur:     Optional[str] = None
    tolov:   Optional[str] = None
    valyuta: Optional[str] = None
    summa:   Optional[float] = None
    note:    Optional[str] = None

# ── HELPERS ────────────────────────────────────────────────
def read_sheet(sheet_name: str):
    sh   = get_ss().worksheet(sheet_name)
    rows = sh.get_all_values()
    result = []
    for i, row in enumerate(rows[2:], start=3):
        if not row[2] or not row[4]: continue
        usd = fmt_num(row[6]) if len(row) > 6 and row[6] else 0.0
        uzs = fmt_num(row[7]) if len(row) > 7 and row[7] else 0.0
        result.append({
            'row':     i,
            'type':    sheet_name,
            'sana':    norm_date(row[2]),
            'egasi':   row[3] if len(row) > 3 else '',
            'tur':     row[4] if len(row) > 4 else '',
            'tolov':   row[5] if len(row) > 5 else '',
            'usd':     usd,
            'uzs':     uzs,
            'note':    row[9] if len(row) > 9 else '',
        })
    return result

def find_next_row(sh):
    col_c = sh.col_values(3)
    last  = 2
    for i, v in enumerate(col_c):
        if i < 2: continue
        if v and v.strip(): last = i + 1
    return last + 1

# ── ENDPOINTS ──────────────────────────────────────────────

@app.get('/')
def root():
    return {'status': 'ok', 'message': 'Family Accounting API'}

# ── BALANCE ────────────────────────────────────────────────
@app.get('/balance')
def get_balance():
    try:
        ss = get_ss()
        val = ss.worksheet('DASHBOARD').acell('B2').value
        bal = fmt_num(val)
        return {'balance': bal, 'formatted': f'{int(round(bal))}$'}
    except Exception as e:
        raise HTTPException(500, str(e))

# ── BUGUNGI MA'LUMOTLAR ─────────────────────────────────────
@app.get('/today')
def get_today():
    try:
        today = today_str()
        ss    = get_ss()
        result = {'date': today, 'chiqimlar': [], 'kirimlar': [],
                  'total_ch_usd': 0.0, 'total_ch_uzs': 0.0,
                  'total_ki_usd': 0.0, 'total_ki_uzs': 0.0}
        for sname, key in [('CHIQIM','chiqimlar'),('KIRIM','kirimlar')]:
            sh    = ss.worksheet(sname)
            dates = sh.col_values(3)
            turs  = sh.col_values(5)
            egasi = sh.col_values(4)
            tolov = sh.col_values(6)
            usds  = sh.col_values(7)
            uzss  = sh.col_values(8)
            notes = sh.col_values(10)
            n = max(len(dates), len(turs))
            for i in range(2, n):
                d = str(dates[i]).strip() if i < len(dates) else ''
                if not d or norm_date(d) != today: continue
                tur = str(turs[i]).strip() if i < len(turs) else ''
                if not tur: continue
                u = fmt_num(usds[i] if i < len(usds) else '')
                z = fmt_num(uzss[i] if i < len(uzss) else '')
                result[key].append({
                    'row':   i + 1,
                    'tur':   tur,
                    'egasi': str(egasi[i]).strip() if i < len(egasi) else '',
                    'tolov': str(tolov[i]).strip() if i < len(tolov) else '',
                    'usd':   u,
                    'uzs':   z,
                    'note':  str(notes[i]).strip() if i < len(notes) else '',
                })
                if key == 'chiqimlar':
                    result['total_ch_usd'] += u; result['total_ch_uzs'] += z
                else:
                    result['total_ki_usd'] += u; result['total_ki_uzs'] += z
        return result
    except Exception as e:
        raise HTTPException(500, str(e))

# ── SANA BO'YICHA FILTER ────────────────────────────────────
@app.get('/by-date')
def get_by_date(date: str = Query(..., description='DD.MM.YYYY')):
    try:
        ss = get_ss()
        result = {'date': date, 'chiqimlar': [], 'kirimlar': []}
        for sname, key in [('CHIQIM','chiqimlar'),('KIRIM','kirimlar')]:
            sh    = ss.worksheet(sname)
            dates = sh.col_values(3)
            turs  = sh.col_values(5)
            egasi = sh.col_values(4)
            tolov = sh.col_values(6)
            usds  = sh.col_values(7)
            uzss  = sh.col_values(8)
            notes = sh.col_values(10)
            n = max(len(dates), len(turs))
            for i in range(2, n):
                d = str(dates[i]).strip() if i < len(dates) else ''
                if not d or norm_date(d) != date: continue
                tur = str(turs[i]).strip() if i < len(turs) else ''
                if not tur: continue
                u = fmt_num(usds[i] if i < len(usds) else '')
                z = fmt_num(uzss[i] if i < len(uzss) else '')
                result[key].append({
                    'row':   i + 1,
                    'tur':   tur,
                    'egasi': str(egasi[i]).strip() if i < len(egasi) else '',
                    'tolov': str(tolov[i]).strip() if i < len(tolov) else '',
                    'usd':   u,
                    'uzs':   z,
                    'note':  str(notes[i]).strip() if i < len(notes) else '',
                })
        return result
    except Exception as e:
        raise HTTPException(500, str(e))

# ── BARCHA AMALLAR (tarix) ─────────────────────────────────
@app.get('/history')
def get_history(limit: int = 50):
    try:
        ch = read_sheet('CHIQIM')
        ki = read_sheet('KIRIM')
        all_tx = ch + ki
        # Sanaga qarab tartiblash (yangi → eski)
        all_tx.sort(key=lambda x: datetime.strptime(x['sana'], '%d.%m.%Y') if x['sana'] else datetime.min, reverse=True)
        return {'transactions': all_tx[:limit], 'total': len(all_tx)}
    except Exception as e:
        raise HTTPException(500, str(e))

# ── STATISTIKA ─────────────────────────────────────────────
@app.get('/stats')
def get_stats():
    try:
        ch = read_sheet('CHIQIM')
        ki = read_sheet('KIRIM')
        # Chiqim statistika
        ch_by_tur = {}
        for t in ch:
            key = t['tur']
            val = t['usd'] + (t['uzs'] / 12000 if t['uzs'] else 0)
            ch_by_tur[key] = ch_by_tur.get(key, 0) + val
        # Kirim statistika
        ki_by_tur = {}
        for t in ki:
            key = t['tur']
            val = t['usd'] + (t['uzs'] / 12000 if t['uzs'] else 0)
            ki_by_tur[key] = ki_by_tur.get(key, 0) + val
        # Top va bottom
        ch_sorted = sorted(ch_by_tur.items(), key=lambda x: x[1], reverse=True)
        ki_sorted = sorted(ki_by_tur.items(), key=lambda x: x[1], reverse=True)
        total_ch  = sum(ch_by_tur.values())
        total_ki  = sum(ki_by_tur.values())
        return {
            'chiqim': {
                'by_tur':    ch_sorted,
                'top':       ch_sorted[0] if ch_sorted else None,
                'bottom':    ch_sorted[-1] if ch_sorted else None,
                'total_usd': round(total_ch, 2),
                'count':     len(ch),
            },
            'kirim': {
                'by_tur':    ki_sorted,
                'top':       ki_sorted[0] if ki_sorted else None,
                'bottom':    ki_sorted[-1] if ki_sorted else None,
                'total_usd': round(total_ki, 2),
                'count':     len(ki),
            },
            'net': round(total_ki - total_ch, 2),
        }
    except Exception as e:
        raise HTTPException(500, str(e))

# ── YANGI AMAL QO'SHISH ─────────────────────────────────────
@app.post('/transaction')
def add_transaction(tx: Transaction):
    try:
        ss      = get_ss()
        sh      = ss.worksheet(tx.type)
        new_row = find_next_row(sh)
        row_num = new_row - 2
        usd     = tx.summa if tx.valyuta == 'USD' else ''
        uzs     = tx.summa if tx.valyuta == 'UZS' else ''
        sh.update(f'B{new_row}:H{new_row}', [[
            row_num, tx.sana, tx.egasi, tx.tur, tx.tolov, usd, uzs
        ]])
        sh.update(f'J{new_row}', [[tx.note or '']])
        return {'success': True, 'row': new_row, 'message': 'Saqlandi'}
    except Exception as e:
        raise HTTPException(500, str(e))

# ── AMALNI TAHRIRLASH ──────────────────────────────────────
@app.put('/transaction/{sheet}/{row}')
def update_transaction(sheet: str, row: int, data: UpdateTransaction):
    try:
        if sheet not in ['CHIQIM', 'KIRIM']:
            raise HTTPException(400, 'Sheet noto\'g\'ri')
        ss = get_ss()
        sh = ss.worksheet(sheet)
        if data.egasi:   sh.update(f'D{row}', [[data.egasi]])
        if data.tur:     sh.update(f'E{row}', [[data.tur]])
        if data.tolov:   sh.update(f'F{row}', [[data.tolov]])
        if data.summa is not None:
            if data.valyuta == 'USD':
                sh.update(f'G{row}', [[data.summa]])
                sh.update(f'H{row}', [['']])
            else:
                sh.update(f'H{row}', [[data.summa]])
                sh.update(f'G{row}', [['']])
        if data.note is not None: sh.update(f'J{row}', [[data.note]])
        return {'success': True, 'message': 'Yangilandi'}
    except Exception as e:
        raise HTTPException(500, str(e))
