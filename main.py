#!/usr/bin/env python3
"""
Family Accounting Bot — Kengaytirilgan versiya
Yangi: AI (rasm+ovoz), QARZ tizimi, Admin panel (kategoriyalar)
"""
import os, json, logging, base64, asyncio, re
from datetime import datetime, timedelta, time as dtime, date
import urllib.request
from io import BytesIO
import pytz
import gspread
from google.oauth2.service_account import Credentials
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, ConversationHandler, filters, ContextTypes
)

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# ── MUHIT O'ZGARUVCHILARI ────────────────────────────────
TOKEN           = os.environ['BOT_TOKEN']
CHAT_1          = os.environ['CHAT_1']
CHAT_2          = os.environ['CHAT_2']
SPREADSHEET_ID  = os.environ['SPREADSHEET_ID']
CREDS_JSON      = os.environ['GOOGLE_CREDS_JSON']
ANTHROPIC_API_KEY = os.environ.get('ANTHROPIC_API_KEY', '')
OPENAI_API_KEY    = os.environ.get('OPENAI_API_KEY', '')
TZ      = pytz.timezone('Asia/Tashkent')
ALLOWED = {CHAT_1, CHAT_2}

# ── CONVERSATION STATES ──────────────────────────────────
TUR, EGASI, TOLOV, VALYUTA, SUMMA, NOTE = range(6)
H_TIP, H_DAVR, H_TUR, H_DATE_FROM, H_DATE_TO = range(6, 11)

# ── PENDING AI DATA (memory) ─────────────────────────────
pending_ai: dict = {}   # {user_id: {op_type, data, source}}

# ── KATEGORIYALAR KESHI ──────────────────────────────────
_cats: dict = {'chiqim': None, 'kirim': None}

DEFAULT_CHIQIM = [
    'OZIQ OVQAT','BENZIN','RASSROCHKA','KIYIM KECHAK',
    'XURSHIDGA','ISHXONAMGA','UYDAGILARGA','SHTRAFLAR',
    'SHOPPPING','ISHXONA REG','SARTAROSH','BOSHQA'
]
DEFAULT_KIRIM = ['ISHXONA','SEEDBEE','BUSINESS','UYDAGILAR','BOSHQA']

def get_chiqim_turs(): return _cats['chiqim'] or DEFAULT_CHIQIM[:]
def get_kirim_turs():  return _cats['kirim']  or DEFAULT_KIRIM[:]

# ══════════════════════════════════════════════════════════
# GOOGLE SHEETS — ASOSIY FUNKSIYALAR
# ══════════════════════════════════════════════════════════
def get_ss():
    info  = json.loads(CREDS_JSON)
    creds = Credentials.from_service_account_info(info, scopes=[
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive'
    ])
    return gspread.authorize(creds).open_by_key(SPREADSHEET_ID)

def num_clean(s):
    try:
        s = str(s)
        for ch in ['$','\xa0','\u202f','\u00a0',' ',"'",'"']:
            s = s.replace(ch,'')
        s = s.replace("so'm",'').replace('UZS','').strip()
        if ',' in s and '.' not in s: s = s.replace(',','.')
        elif ',' in s and '.' in s:   s = s.replace('.','').replace(',','.')
        return float(s) if s else 0.0
    except: return 0.0

def get_balance():
    try:
        ss = get_ss()
        for sheet, cell in [('KUNLIK_VIEW','E2'),('DASHBOARD','B2')]:
            try:
                raw = ss.worksheet(sheet).acell(cell).value
                if not raw: continue
                v = num_clean(raw)
                if v > 0: return v
            except Exception as e: logger.error(f'bal {sheet}: {e}')
        return 0.0
    except Exception as e:
        logger.error(f'get_balance: {e}')
        return 0.0

def save_row(sheet_name, st):
    sh       = get_ss().worksheet(sheet_name)
    now_dt   = datetime.now(TZ)
    today    = now_dt.strftime('%d.%m.%Y')
    now_t    = now_dt.strftime('%H:%M')
    usd_val  = float(st['summa']) if st['valyuta'] == 'USD' else ''
    uzs_val  = float(st['summa']) if st['valyuta'] == 'UZS' else ''
    col_c    = sh.col_values(3)
    last     = 2
    for i, v in enumerate(col_c):
        if i < 2: continue
        if v and str(v).strip(): last = i + 1
    new_row = last + 1
    sh.update(f'B{new_row}:I{new_row}', [[
        new_row-2, today, st['egasi'], st['tur'],
        st['tolov'], usd_val, uzs_val, now_t
    ]], value_input_option='USER_ENTERED')
    sh.update(f'J{new_row}', [[st.get('note','')]])
    logger.info(f'Saved to {sheet_name} row {new_row}')
    return new_row

def norm_date(s):
    s = str(s).strip()
    if not s: return ''
    if len(s)==10 and s[2]=='.' and s[5]=='.': return s
    for fmt in ['%d/%m/%Y','%m/%d/%Y','%Y-%m-%d','%d-%m-%Y','%d.%m.%y']:
        try: return datetime.strptime(s, fmt).strftime('%d.%m.%Y')
        except: pass
    try:
        from datetime import timedelta
        return (datetime(1899,12,30)+timedelta(days=int(float(s)))).strftime('%d.%m.%Y')
    except: pass
    return s

def today_str(): return datetime.now(TZ).strftime('%d.%m.%Y')
def fmt(n):
    try: return f"{int(round(float(n))):,}".replace(',', ' ')
    except: return '0'
def sstr(u, z):
    p = []
    if u and float(u) > 0: p.append(f"{int(round(float(u)))}$")
    if z and float(z) > 0: p.append(f"{fmt(z)} so'm")
    return ' + '.join(p) if p else '0'
def smstr(st):
    if st['valyuta'] == 'USD': return f"{int(round(float(st['summa'])))}$"
    return f"{fmt(st['summa'])} so'm"

def confirm_text(st, bal=None):
    lbl     = 'CHIQIM' if st['type'] == 'CHIQIM' else 'KIRIM'
    ico     = '📤' if st['type'] == 'CHIQIM' else '📥'
    bal_str = f"{round(float(bal),2)}$" if bal is not None else '—'
    return (
        f"{ico} <b>{today_str()}</b>\n\n"
        f"▪️ {lbl} TURI: <b>{st['tur']}</b>\n"
        f"▪️ EGASI: <b>{st['egasi']}</b>\n"
        f"▪️ TO'LOV: <b>{st['tolov']}</b>\n"
        f"▪️ VALYUTA: <b>{st['valyuta']}</b>\n"
        f"▪️ SUMMA: <b>{smstr(st)}</b>\n"
        f"▪️ NOTE: <b>{st.get('note') or '—'}</b>\n\n"
        f"💰 BALANCE: <b>{bal_str}</b>"
    )

def get_bugun():
    today = today_str()
    ss    = get_ss()
    r     = dict(ch=[], ki=[], chU=0.0, chZ=0.0, kiU=0.0, kiZ=0.0)
    for sname, target in [('CHIQIM','ch'),('KIRIM','ki')]:
        try:
            sh    = ss.worksheet(sname)
            dates = sh.col_values(3); turs = sh.col_values(5)
            usds  = sh.col_values(7); uzss = sh.col_values(8)
            n = max(len(dates), len(turs))
            for i in range(2, n):
                d = str(dates[i]).strip() if i < len(dates) else ''
                if not d or norm_date(d) != today: continue
                tur = str(turs[i]).strip() if i < len(turs) else ''
                if not tur: continue
                u = num_clean(usds[i] if i < len(usds) else '')
                z = num_clean(uzss[i] if i < len(uzss) else '')
                item = {'tur': tur, 'usd': u, 'uzs': z}
                if target == 'ch':
                    r['ch'].append(item); r['chU'] += u; r['chZ'] += z
                else:
                    r['ki'].append(item); r['kiU'] += u; r['kiZ'] += z
        except Exception as e: logger.error(f'get_bugun {sname}: {e}')
    return r

def get_filtered(tip, davr, tur, date_from=None, date_to=None):
    now = datetime.now(TZ)
    ss  = get_ss()
    sh  = ss.worksheet(tip)
    dates=sh.col_values(3); turs=sh.col_values(5); egasi_col=sh.col_values(4)
    usds=sh.col_values(7);  uzss=sh.col_values(8); notes=sh.col_values(10)
    n=max(len(dates),len(turs)); result=[]; total_usd=0.0; total_uzs=0.0
    dt_from=None; dt_to=None
    if date_from:
        try: dt_from=datetime.strptime(date_from,'%d.%m.%Y')
        except: pass
    if date_to:
        try: dt_to=datetime.strptime(date_to,'%d.%m.%Y')
        except: pass
    for i in range(2, n):
        d=str(dates[i]).strip() if i<len(dates) else ''
        if not d: continue
        nd=norm_date(d)
        if not nd: continue
        try: dt=datetime.strptime(nd,'%d.%m.%Y')
        except: continue
        if davr=='bu_oy':
            if dt.month!=now.month or dt.year!=now.year: continue
        elif davr=='otgan_oy':
            prev=now.month-1 if now.month>1 else 12
            prev_y=now.year if now.month>1 else now.year-1
            if dt.month!=prev or dt.year!=prev_y: continue
        elif davr=='bu_yil':
            if dt.year!=now.year: continue
        elif davr=='custom':
            if dt_from and dt<dt_from: continue
            if dt_to   and dt>dt_to:   continue
        t=str(turs[i]).strip() if i<len(turs) else ''
        if not t: continue
        if tur!='BARCHASI' and t!=tur: continue
        u=num_clean(usds[i] if i<len(usds) else '')
        z=num_clean(uzss[i] if i<len(uzss) else '')
        eg=str(egasi_col[i]).strip() if i<len(egasi_col) else ''
        nt=str(notes[i]).strip() if i<len(notes) else ''
        result.append({'sana':nd,'tur':t,'egasi':eg,'usd':u,'uzs':z,'note':nt})
        total_usd+=u; total_uzs+=z
    return result, total_usd, total_uzs

async def delete_messages(bot, chat_id, msg_ids):
    for mid in msg_ids:
        try: await bot.delete_message(chat_id=chat_id, message_id=mid)
        except: pass

# ══════════════════════════════════════════════════════════
# KATEGORIYALAR — SETTINGS VARAQI
# ══════════════════════════════════════════════════════════
async def load_categories():
    """SETTINGS varag'idan kategoriyalarni yuklash"""
    try:
        sh = get_ss()
        try: ws = sh.worksheet('SETTINGS')
        except Exception:
            ws = sh.add_worksheet(title='SETTINGS', rows=100, cols=10)
            ws.update('A1', [['kalit','qiymat']])
            ws.update('A2', [['chiqim_turs', json.dumps(DEFAULT_CHIQIM)]])
            ws.update('A3', [['kirim_turs',  json.dumps(DEFAULT_KIRIM)]])
            logger.info('SETTINGS varag\'i yaratildi')
        records = ws.get_all_values()
        for row in records[1:]:
            if len(row) < 2: continue
            key, val = row[0], row[1]
            if key == 'chiqim_turs':
                try: _cats['chiqim'] = json.loads(val)
                except: pass
            elif key == 'kirim_turs':
                try: _cats['kirim'] = json.loads(val)
                except: pass
    except Exception as e:
        logger.error(f'load_categories: {e}')

async def save_categories():
    try:
        sh = get_ss()
        try: ws = sh.worksheet('SETTINGS')
        except Exception:
            ws = sh.add_worksheet(title='SETTINGS', rows=100, cols=10)
            ws.update('A1', [['kalit','qiymat']])
        ws.update('A2', [['chiqim_turs', json.dumps(_cats.get('chiqim', DEFAULT_CHIQIM))]])
        ws.update('A3', [['kirim_turs',  json.dumps(_cats.get('kirim',  DEFAULT_KIRIM))]])
        return True
    except Exception as e:
        logger.error(f'save_categories: {e}')
        return False

# ══════════════════════════════════════════════════════════
# QARZ TIZIMI — GOOGLE SHEETS
# ══════════════════════════════════════════════════════════
async def ensure_qarz():
    """QARZ varag'i mavjudligini ta'minlash"""
    try:
        sh = get_ss()
        try: sh.worksheet('QARZ')
        except Exception:
            ws = sh.add_worksheet(title='QARZ', rows=500, cols=12)
            ws.update('A1:J1', [[
                'raqam','tur','kim','summa_uzs','summa_usd',
                'sana','muddat','holat','qaytarilgan_sana','note'
            ]])
            logger.info('QARZ varag\'i yaratildi')
    except Exception as e:
        logger.error(f'ensure_qarz: {e}')

def get_qarz_ws(): return get_ss().worksheet('QARZ')

def qarz_to_list(rows):
    if len(rows) < 2: return []
    headers = rows[0]
    result  = []
    for i, row in enumerate(rows[1:], start=2):
        row_p = row + [''] * (len(headers) - len(row))
        d = dict(zip(headers, row_p))
        d['_row'] = i
        result.append(d)
    return result

def qarz_aktiv(lst): return [r for r in lst if r.get('holat','').upper() == 'AKTIV']

# ══════════════════════════════════════════════════════════
# KLAVIATURALAR
# ══════════════════════════════════════════════════════════
def kb_main():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('📤 CHIQIM', callback_data='MC'),
         InlineKeyboardButton('📥 KIRIM',  callback_data='MK')],
        [InlineKeyboardButton('💰 BALANS', callback_data='MB'),
         InlineKeyboardButton('📅 BUGUN',  callback_data='MG')],
        [InlineKeyboardButton('📊 STATISTIKA', callback_data='MS'),
         InlineKeyboardButton('🔍 HISOBOT',    callback_data='MH')],
        [InlineKeyboardButton('💳 QARZ',       callback_data='QARZ_MENU'),
         InlineKeyboardButton('⚙️ ADMIN',      callback_data='ADMIN_MENU')],
    ])

def kb_chiqim():
    turs = get_chiqim_turs()
    emoji_map = {
        'OZIQ OVQAT':'🛒','BENZIN':'⛽','RASSROCHKA':'💳',
        'KIYIM KECHAK':'👗','XURSHIDGA':'👨','ISHXONAMGA':'🏢',
        'UYDAGILARGA':'🏠','SHTRAFLAR':'🚫','SHOPPPING':'🛍',
        'ISHXONA REG':'📋','SARTAROSH':'✂️','BOSHQA':'💡',
    }
    buttons = []
    row = []
    for t in turs:
        em = emoji_map.get(t, '▪️')
        row.append(InlineKeyboardButton(f'{em} {t}', callback_data=f'C|{t}'))
        if len(row) == 2:
            buttons.append(row); row = []
    if row: buttons.append(row)
    buttons.append([InlineKeyboardButton('🔙 Orqaga', callback_data='BACK')])
    return InlineKeyboardMarkup(buttons)

def kb_kirim():
    turs = get_kirim_turs()
    emoji_map = {
        'ISHXONA':'🏢','SEEDBEE':'🌱','BUSINESS':'💼',
        'UYDAGILAR':'🏠','BOSHQA':'💡',
    }
    buttons = []
    row = []
    for t in turs:
        em = emoji_map.get(t, '▪️')
        row.append(InlineKeyboardButton(f'{em} {t}', callback_data=f'K|{t}'))
        if len(row) == 2:
            buttons.append(row); row = []
    if row: buttons.append(row)
    buttons.append([InlineKeyboardButton('🔙 Orqaga', callback_data='BACK')])
    return InlineKeyboardMarkup(buttons)

kb_egasi   = lambda: InlineKeyboardMarkup([[InlineKeyboardButton('👨 Ferudin',callback_data='E|FERUDIN'),InlineKeyboardButton('👩 Guloyim',callback_data='E|GULOYIM')]])
kb_tolov   = lambda: InlineKeyboardMarkup([[InlineKeyboardButton('💵 Cash',callback_data='T|CASH'),InlineKeyboardButton('💳 Card',callback_data='T|CARD'),InlineKeyboardButton('📌 Other',callback_data='T|OTHER')]])
kb_valyuta = lambda: InlineKeyboardMarkup([[InlineKeyboardButton('💵 USD ($)',callback_data='V|USD'),InlineKeyboardButton("🇺🇿 UZS (so'm)",callback_data='V|UZS')]])
kb_note    = lambda: InlineKeyboardMarkup([[InlineKeyboardButton('✅ Done — note kerak emas',callback_data='SKIP')]])

def kb_h_tip():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('📤 Chiqimlar',callback_data='HT|CHIQIM'),
         InlineKeyboardButton('📥 Kirimlar', callback_data='HT|KIRIM')],
        [InlineKeyboardButton('🔙 Asosiy',   callback_data='BACK')]
    ])

def kb_h_davr():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('📅 Bu oy',    callback_data='HD|bu_oy'),
         InlineKeyboardButton('📅 Otgan oy', callback_data='HD|otgan_oy')],
        [InlineKeyboardButton('📅 Bu yil',   callback_data='HD|bu_yil'),
         InlineKeyboardButton('📅 Hammasi',  callback_data='HD|hammasi')],
        [InlineKeyboardButton("📆 O'z sanani kiritish", callback_data='HD|custom')],
        [InlineKeyboardButton('🔙 Orqaga',   callback_data='MH')]
    ])

def kb_h_tur(tip='CHIQIM'):
    turs = get_chiqim_turs() if tip == 'CHIQIM' else get_kirim_turs()
    buttons = [[InlineKeyboardButton('📋 Barchasi', callback_data='HU|BARCHASI')]]
    row = []
    for t in turs:
        row.append(InlineKeyboardButton(t, callback_data=f'HU|{t}'))
        if len(row) == 2: buttons.append(row); row = []
    if row: buttons.append(row)
    buttons.append([InlineKeyboardButton('🔙 Orqaga', callback_data='HTB')])
    return InlineKeyboardMarkup(buttons)

def kb_ai_confirm():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('✅ Saqlash',   callback_data='AI_SAVE'),
         InlineKeyboardButton('✏️ Tahrirlash',callback_data='AI_EDIT')],
        [InlineKeyboardButton('❌ Bekor',     callback_data='AI_CANCEL')]
    ])

# ══════════════════════════════════════════════════════════
# HISOBOT CONVERSATION
# ══════════════════════════════════════════════════════════
def ok(update): return str(update.effective_chat.id) in ALLOWED

async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ok(update): return
    ctx.user_data.clear()
    await update.message.reply_text(
        '👋 <b>FAMILY ACCOUNTING</b>\n\nNima qilmoqchisiz?',
        parse_mode='HTML', reply_markup=kb_main()
    )
    return ConversationHandler.END

async def hisobot_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if not ok(update): return ConversationHandler.END
    ctx.user_data['h'] = {}
    await q.message.reply_text('🔍 <b>HISOBOT</b>\n\nAmal turini tanlang:',
        parse_mode='HTML', reply_markup=kb_h_tip())
    return H_TIP

async def hisobot_tip(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    d = q.data
    if d == 'BACK':
        await q.message.reply_text('👋 <b>FAMILY ACCOUNTING</b>',parse_mode='HTML',reply_markup=kb_main())
        return ConversationHandler.END
    if d.startswith('HT|'):
        ctx.user_data['h']['tip'] = d[3:]
        lbl = 'Chiqimlar' if d[3:]=='CHIQIM' else 'Kirimlar'
        await q.message.reply_text(f'🔍 <b>{lbl}</b>\n\nQaysi davr?',
            parse_mode='HTML', reply_markup=kb_h_davr())
        return H_DAVR
    return H_TIP

async def hisobot_davr(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    d = q.data
    if d == 'MH':
        ctx.user_data['h'] = {}
        await q.message.reply_text('🔍 <b>HISOBOT</b>\n\nAmal turini tanlang:',
            parse_mode='HTML', reply_markup=kb_h_tip())
        return H_TIP
    if d.startswith('HD|'):
        davr = d[3:]
        ctx.user_data['h']['davr'] = davr
        if davr == 'custom':
            await q.message.reply_text(
                "📆 <b>Boshlang'ich sanani yozing:</b>\n<i>Masalan: 14.04.2026</i>",
                parse_mode='HTML')
            return H_DATE_FROM
        tip = ctx.user_data['h'].get('tip','CHIQIM')
        davr_txt = {'bu_oy':'Bu oy','otgan_oy':'Otgan oy','bu_yil':'Bu yil','hammasi':'Hammasi'}.get(davr,davr)
        await q.message.reply_text(f'🔍 Davr: <b>{davr_txt}</b>\n\nXarajat turini tanlang:',
            parse_mode='HTML', reply_markup=kb_h_tur(tip))
        return H_TUR
    return H_DAVR

async def hisobot_date_from(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ok(update): return H_DATE_FROM
    txt = update.message.text.strip()
    try: datetime.strptime(txt,'%d.%m.%Y')
    except:
        await update.message.reply_text("❌ Format noto'g'ri. Masalan: <b>14.04.2026</b>",parse_mode='HTML')
        return H_DATE_FROM
    ctx.user_data['h']['date_from'] = txt
    await update.message.reply_text(
        f"✅ Boshlanish: <b>{txt}</b>\n\n📆 <b>Tugash sanasini yozing:</b>\n<i>Masalan: 28.04.2026</i>",
        parse_mode='HTML')
    return H_DATE_TO

async def hisobot_date_to(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ok(update): return H_DATE_TO
    txt = update.message.text.strip()
    try: datetime.strptime(txt,'%d.%m.%Y')
    except:
        await update.message.reply_text("❌ Format noto'g'ri. Masalan: <b>28.04.2026</b>",parse_mode='HTML')
        return H_DATE_TO
    ctx.user_data['h']['date_to'] = txt
    tip = ctx.user_data['h'].get('tip','CHIQIM')
    df  = ctx.user_data['h'].get('date_from','')
    await update.message.reply_text(f"✅ <b>{df} — {txt}</b>\n\nXarajat turini tanlang:",
        parse_mode='HTML', reply_markup=kb_h_tur(tip))
    return H_TUR

async def hisobot_tur(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    d = q.data
    if d == 'HTB':
        await q.message.reply_text('Davr tanlang:',reply_markup=kb_h_davr())
        return H_DAVR
    if d.startswith('HU|'):
        tur  = d[3:]
        h    = ctx.user_data.get('h',{})
        tip  = h.get('tip','CHIQIM')
        davr = h.get('davr','bu_oy')
        df   = h.get('date_from')
        dt   = h.get('date_to')
        davr_txt = f'{df} — {dt}' if davr=='custom' and df and dt else \
            {'bu_oy':'Bu oy','otgan_oy':'Otgan oy','bu_yil':'Bu yil','hammasi':'Hammasi'}.get(davr,davr)
        tur_txt = 'Barchasi' if tur=='BARCHASI' else tur
        lbl = 'CHIQIM' if tip=='CHIQIM' else 'KIRIM'
        ico = '📤' if tip=='CHIQIM' else '📥'
        await q.message.reply_text('⏳ Hisobot tayyorlanmoqda...')
        rows, total_usd, total_uzs = get_filtered(tip, davr, tur, df, dt)
        if not rows:
            txt = f"🔍 <b>{ico} {lbl}</b>\nDavr: <b>{davr_txt}</b>\nTur: <b>{tur_txt}</b>\n\n📭 Ma'lumot topilmadi."
        else:
            txt = f"🔍 <b>{ico} {lbl}</b>\nDavr: <b>{davr_txt}</b>\nTur: <b>{tur_txt}</b>\nJami: <b>{len(rows)} ta</b>\n\n"
            show = rows[-15:] if len(rows) > 15 else rows
            if len(rows) > 15: txt += "(Oxirgi 15 ta)\n\n"
            for r in reversed(show):
                sum_str  = sstr(r['usd'],r['uzs'])
                note_str = f' — {r["note"]}' if r.get('note') else ''
                txt += f"▪️ <b>{r['sana']}</b> | {r['tur']} | {r['egasi']}\n   💰 {sum_str}{note_str}\n"
            txt += f"\n📊 <b>JAMI: {sstr(total_usd, total_uzs)}</b>"
        await q.message.reply_text(txt, parse_mode='HTML', reply_markup=kb_main())
        return ConversationHandler.END
    return H_TUR

# ══════════════════════════════════════════════════════════
# ASOSIY BOT — KIRIM/CHIQIM CONVERSATION
# ══════════════════════════════════════════════════════════
async def btn(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q  = update.callback_query
    await q.answer()
    if not ok(update): return
    d  = q.data
    ud = ctx.user_data

    if d == 'BACK':
        ud.clear()
        await q.message.reply_text('👋 <b>FAMILY ACCOUNTING</b>',parse_mode='HTML',reply_markup=kb_main())
        return ConversationHandler.END
    if d == 'MC':
        ud.clear(); ud['type']='CHIQIM'; ud['msgs']=[]
        m = await q.message.reply_text('📤 <b>CHIQIM</b>\n\nXarajat turini tanlang:',parse_mode='HTML',reply_markup=kb_chiqim())
        ud['msgs'].append(m.message_id); return TUR
    if d == 'MK':
        ud.clear(); ud['type']='KIRIM'; ud['msgs']=[]
        m = await q.message.reply_text('📥 <b>KIRIM</b>\n\nKirim turini tanlang:',parse_mode='HTML',reply_markup=kb_kirim())
        ud['msgs'].append(m.message_id); return TUR
    if d == 'MB':
        await q.message.reply_text('⏳ Balans tekshirilmoqda...')
        bal = get_balance()
        await q.message.reply_text(f'💰 <b>Joriy balans: {round(bal,2)}$</b>',parse_mode='HTML',reply_markup=kb_main())
        return ConversationHandler.END
    if d == 'MG':
        await q.message.reply_text("⏳ Ma'lumotlar yuklanmoqda...")
        dv  = get_bugun()
        txt = f'📅 <b>{today_str()}</b>\n\n<b>📤 Chiqimlar:</b>\n'
        txt += ('\n'.join(f'  • {c["tur"]}: {sstr(c["usd"],c["uzs"])}' for c in dv['ch'])) or "  Yo'q"
        txt += '\n\n<b>📥 Kirimlar:</b>\n'
        txt += ('\n'.join(f'  • {k["tur"]}: {sstr(k["usd"],k["uzs"])}' for k in dv['ki'])) or "  Yo'q"
        await q.message.reply_text(txt,parse_mode='HTML',reply_markup=kb_main())
        return ConversationHandler.END
    if d == 'MS':
        await q.message.reply_text('⏳ Statistika yuklanmoqda...')
        dv  = get_bugun()
        bal = get_balance()
        txt = (f'📊 <b>Statistika</b>\n\n'
               f'💰 Balans: <b>{round(bal,2)}$</b>\n'
               f'Bugungi chiqim: <b>{sstr(dv["chU"],dv["chZ"])}</b>\n'
               f'Bugungi kirim:  <b>{sstr(dv["kiU"],dv["kiZ"])}</b>')
        await q.message.reply_text(txt,parse_mode='HTML',reply_markup=kb_main())
        return ConversationHandler.END
    if d.startswith('C|'):
        ud['tur'] = d[2:]
        m = await q.message.reply_text(f'📤 <b>{ud["tur"]}</b>\n\nKim sarfladi?',parse_mode='HTML',reply_markup=kb_egasi())
        ud.setdefault('msgs',[]).append(m.message_id); return EGASI
    if d.startswith('K|'):
        ud['tur'] = d[2:]
        m = await q.message.reply_text(f'📥 <b>{ud["tur"]}</b>\n\nKimning kirimi?',parse_mode='HTML',reply_markup=kb_egasi())
        ud.setdefault('msgs',[]).append(m.message_id); return EGASI
    if d.startswith('E|'):
        ud['egasi'] = d[2:]
        m = await q.message.reply_text(f"👤 <b>{ud['egasi']}</b>\n\nTo'lov turi?",parse_mode='HTML',reply_markup=kb_tolov())
        ud.setdefault('msgs',[]).append(m.message_id); return TOLOV
    if d.startswith('T|'):
        ud['tolov'] = d[2:]
        m = await q.message.reply_text(f"💳 <b>{ud['tolov']}</b>\n\nValyuta:",parse_mode='HTML',reply_markup=kb_valyuta())
        ud.setdefault('msgs',[]).append(m.message_id); return VALYUTA
    if d.startswith('V|'):
        ud['valyuta'] = d[2:]
        hint = 'Masalan: 150' if ud['valyuta']=='USD' else 'Masalan: 350000'
        m = await q.message.reply_text(f"💱 <b>{ud['valyuta']}</b>\n\nSummani yozing:\n<i>{hint}</i>",parse_mode='HTML')
        ud.setdefault('msgs',[]).append(m.message_id); return SUMMA
    if d == 'SKIP':
        ud['note'] = ''
        ud.setdefault('msgs',[]).append(q.message.message_id)
        await _finalize(q.message, ctx)
        return ConversationHandler.END

    # ── QARZ va ADMIN callbacklari uchun redirect ──────
    if d == 'QARZ_MENU' or d.startswith('QARZ_'):
        await qarz_callback(update, ctx)
        return ConversationHandler.END
    if d == 'ADMIN_MENU' or d.startswith('ADM_'):
        await admin_callback(update, ctx)
        return ConversationHandler.END

    return ConversationHandler.END

async def get_summa(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ok(update): return
    # ── AI edit yoki outer state bo'lsa — ignore (outer handler hal qiladi) ──
    txt = update.message.text.strip().replace(' ','').replace(',','.')
    try:
        num = float(txt); assert num > 0
    except:
        await update.message.reply_text('❌ Raqam kiriting.\n<i>Masalan: 150 yoki 350000</i>',parse_mode='HTML')
        return SUMMA
    ctx.user_data['summa'] = num
    ctx.user_data.setdefault('msgs',[]).append(update.message.message_id)
    m = await update.message.reply_text(
        f"✅ Summa: <b>{smstr(ctx.user_data)}</b>\n\nNote yozing yoki o'tkazib yuboring:",
        parse_mode='HTML', reply_markup=kb_note())
    ctx.user_data['msgs'].append(m.message_id)
    return NOTE

async def get_note(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ok(update): return
    ctx.user_data.setdefault('msgs',[]).append(update.message.message_id)
    ctx.user_data['note'] = update.message.text.strip()
    await _finalize(update.message, ctx)
    return ConversationHandler.END

async def _finalize(message, ctx):
    st = dict(ctx.user_data)
    try:
        if 'type' not in st or 'tur' not in st:
            ctx.user_data.clear()
            await message.reply_text('Qaytadan boshlang:',reply_markup=kb_main())
            return
        m_wait = await message.reply_text('⏳ Saqlanmoqda...')
        save_row(st['type'], st)
        bal  = get_balance()
        txt  = confirm_text(st, bal)
        msgs = list(st.get('msgs',[]))
        msgs.append(m_wait.message_id)
        ctx.user_data.clear()
        await message.reply_text(txt, parse_mode='HTML', reply_markup=kb_main())
        try: await delete_messages(ctx.application.bot, message.chat_id, msgs)
        except Exception as de: logger.error(f'delete msgs: {de}')
    except Exception as e:
        logger.error(f'finalize: {e}')
        ctx.user_data.clear()
        await message.reply_text(f'❌ Xato: {e}',reply_markup=kb_main())

# ══════════════════════════════════════════════════════════
# OUTER TEXT HANDLER — AI edit / QARZ input / Admin input
# ══════════════════════════════════════════════════════════
async def outer_text_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """ConversationHandler faol bo'lmaganda ishga tushadi"""
    if not ok(update): return
    if ctx.user_data.get('ai_editing'):
        await ai_edit_text(update, ctx)
    elif ctx.user_data.get('qarz_new'):
        await qarz_input(update, ctx)
    elif ctx.user_data.get('admin_action'):
        await admin_text(update, ctx)
    else:
        await analyze_and_route(update, ctx)

# ══════════════════════════════════════════════════════════
# AI — RASM (PHOTO) HANDLER
# ══════════════════════════════════════════════════════════
async def handle_photo(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Chek/receipt rasmi → Claude Vision → tasdiqlash"""
    if not ok(update): return
    msg = await update.message.reply_text('📸 Rasm tahlil qilinmoqda...')
    try:
        if not ANTHROPIC_API_KEY:
            await msg.edit_text('❌ ANTHROPIC_API_KEY Railway Variables ga qo\'shilmagan.')
            return
        import anthropic
        # Rasm yuklash
        photo     = update.message.photo[-1]
        tg_file   = await ctx.bot.get_file(photo.file_id)
        fb        = await tg_file.download_as_bytearray()
        image_b64 = base64.standard_b64encode(bytes(fb)).decode('utf-8')
        media_type = 'image/png' if bytes(fb)[:8]==b'\x89PNG\r\n\x1a\n' else 'image/jpeg'

        chiqim_list = ', '.join(get_chiqim_turs())
        today       = today_str()
        client      = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

        resp = await asyncio.to_thread(
            client.messages.create,
            model='claude-opus-4-5',
            max_tokens=512,
            messages=[{
                'role': 'user',
                'content': [
                    {'type':'image','source':{'type':'base64','media_type':media_type,'data':image_b64}},
                    {'type':'text','text':(
                        f'Bu chek yoki to\'lov kvitansiyasi. '
                        f'Faqat JSON qaytargin (hech qanday matn qo\'shma):\n'
                        f'{{"summa_uzs":<UZS son yoki null>,'
                        f'"summa_usd":<USD son yoki null>,'
                        f'"sana":"<DD.MM.YYYY, topilmasa {today}>","tur":"<biri: {chiqim_list}>","tolov":"<NAQD yoki KARTA>","note":"<do\'kon nomi yoki qisqacha izoh>"}}'
                    )}
                ]
            }]
        )
        raw  = resp.content[0].text.strip()
        raw  = re.sub(r'```(?:json)?\s*','',raw).strip('`').strip()
        data = json.loads(raw)

        uid = update.effective_user.id
        pending_ai[uid] = {'op_type': 'chiqim', 'data': data, 'source': 'photo'}
        await msg.edit_text(_fmt_ai(data,'chiqim'), parse_mode='HTML', reply_markup=kb_ai_confirm())

    except json.JSONDecodeError:
        await msg.edit_text('❌ Claude JSON qaytarishda xatolik. Rasm aniq ekanligini tekshiring.')
    except Exception as e:
        import anthropic as ant
        if isinstance(e, ant.AuthenticationError):
            await msg.edit_text('❌ ANTHROPIC_API_KEY noto\'g\'ri.')
        else:
            await msg.edit_text(f'❌ Xatolik: {str(e)[:120]}')
            logger.error(f'handle_photo: {e}')

# ══════════════════════════════════════════════════════════
# AI — OVOZLI XABAR (VOICE) HANDLER
# ══════════════════════════════════════════════════════════
async def handle_voice(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Ovozli xabar → Whisper → Claude → accounting/task/memory/suhbat"""
    if not ok(update): return
    msg = await update.message.reply_text('🎙 Ovoz qayta ishlanmoqda...')
    try:
        if not OPENAI_API_KEY:
            await msg.edit_text("❌ OPENAI_API_KEY Railway Variables ga qo'shilmagan.")
            return
        if not ANTHROPIC_API_KEY:
            await msg.edit_text("❌ ANTHROPIC_API_KEY Railway Variables ga qo'shilmagan.")
            return
        import openai as oai
        import anthropic

        voice    = update.message.voice or update.message.audio
        tg_file  = await ctx.bot.get_file(voice.file_id)
        fb       = await tg_file.download_as_bytearray()
        audio_io = BytesIO(bytes(fb))
        audio_io.name = 'voice.ogg'

        oai_client = oai.OpenAI(api_key=OPENAI_API_KEY)
        transcript = await asyncio.to_thread(
            oai_client.audio.transcriptions.create,
            model='whisper-1', file=audio_io, language=None)
        voice_text = transcript.text.strip()
        await msg.edit_text(
            f'🎙 <i>{voice_text}</i>\n\n⏳ Tahlil qilinmoqda...', parse_mode='HTML')

        cid    = str(update.effective_chat.id)
        kim    = 'FERUDIN' if cid == CHAT_1 else 'GULOYIM'
        today  = today_str()
        now_hm = datetime.now(TZ).strftime('%H:%M')
        ch_lst = ', '.join(get_chiqim_turs())
        ki_lst = ', '.join(get_kirim_turs())
        claude = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

        resp = await asyncio.to_thread(
            claude.messages.create,
            model='claude-opus-4-5',
            max_tokens=512,
            messages=[{'role': 'user', 'content': (
                f'Ovozli xabar: "{voice_text}"\nBugun: {today} {now_hm} Toshkent\n\n'
                f'Faqat JSON qaytargin:\n'
                f'{{"intent":"<CHIQIM|KIRIM|TASK|MEMORY_SAVE|MEMORY_QUERY|SUHBAT>",'
                f'"operatsiya":"<CHIQIM|KIRIM|null>",'
                f'"summa_uzs":<son|null>,"summa_usd":<son|null>,'
                f'"sana":"<DD.MM.YYYY>","tur":"<{ch_lst}|{ki_lst}>",'
                f'"egasi":"<FERUDIN|GULOYIM>","tolov":"<NAQD|KARTA>","note":"<izoh>",'
                f'"task_matn":"<null|vazifa>","task_vaqt":"<null|DD.MM.YYYY HH:MM>",'
                f'"task_egasi":"<FERUDIN|GULOYIM|IKKALASI>",'
                f'"memory_kalit":"<null|kalit>","memory_qiymat":"<null|qiymat>"}}'
            )}])
        raw  = re.sub(r'```(?:json)?\s*', '', resp.content[0].text.strip()).strip('`').strip()
        data = json.loads(raw)
        intent = data.get('intent', 'SUHBAT')

        if intent in ('CHIQIM', 'KIRIM'):
            op  = intent.lower()
            uid = update.effective_user.id
            pending_ai[uid] = {'op_type': op, 'data': data, 'source': 'voice', 'voice_text': voice_text}
            await msg.edit_text(_fmt_ai(data, op), parse_mode='HTML', reply_markup=kb_ai_confirm())

        elif intent == 'TASK':
            matn  = data.get('task_matn') or voice_text
            vaqt  = data.get('task_vaqt')
            egasi = data.get('task_egasi', kim)
            if not vaqt:
                await msg.edit_text(
                    f'🎙 <i>{voice_text}</i>\n\n⚠️ Vaqt aniqlanmadi. '
                    f'"Ertaga 10:00 da bozor" deb aniqroq ayting.', parse_mode='HTML')
                return
            await save_and_schedule_task(ctx.application, matn, vaqt, egasi, cid)
            await msg.edit_text(
                f'🎙 <i>{voice_text}</i>\n\n✅ <b>Task saqlandi!</b>\n\n'
                f'📋 {matn}\n⏰ {vaqt}\n👤 {egasi}', parse_mode='HTML')

        elif intent == 'MEMORY_SAVE':
            kalit  = data.get('memory_kalit') or ''
            qiymat = data.get('memory_qiymat') or ''
            if kalit and qiymat:
                r = await memory_save(kalit, qiymat, kim)
                await msg.edit_text(
                    f'🎙 <i>{voice_text}</i>\n\n🧠 <b>Xotiraga {r}!</b>\n📌 <b>{kalit}</b>: {qiymat}',
                    parse_mode='HTML')
            else:
                await msg.edit_text(
                    f'🎙 <i>{voice_text}</i>\n\n⚠️ Kalit yoki qiymat aniqlanmadi.', parse_mode='HTML')

        elif intent == 'MEMORY_QUERY':
            kalit   = data.get('memory_kalit') or voice_text
            results = await memory_search(kalit)
            if not results:
                await msg.edit_text(f'🎙 <i>{voice_text}</i>\n\n🔍 "<b>{kalit}</b>" topilmadi.', parse_mode='HTML')
            else:
                txt2 = f'🎙 <i>{voice_text}</i>\n\n🧠 <b>{kalit}:</b>\n\n'
                for r in results[:5]:
                    txt2 += f'📌 <b>{r["kalit"]}</b>: {r["qiymat"]}\n'
                await msg.edit_text(txt2, parse_mode='HTML')

        else:
            await msg.edit_text(
                f'🎙 <i>{voice_text}</i>\n\n'
                f'💬 Accounting, task yoki memory uchun aniqroq ayting.', parse_mode='HTML')

    except json.JSONDecodeError:
        await msg.edit_text('❌ JSON parse xatolik.')
    except Exception as e:
        await msg.edit_text(f'❌ Xatolik: {str(e)[:120]}')
        logger.error(f'handle_voice: {e}')

def _fmt_ai(data: dict, op_type: str) -> str:
    uzs = data.get('summa_uzs')
    usd = data.get('summa_usd')
    parts = []
    if uzs: parts.append(f"{float(uzs):,.0f} UZS")
    if usd: parts.append(f"{float(usd):.2f} USD")
    summa_str = ' / '.join(parts) or '❓ Aniqlanmadi'
    ico = '💸' if op_type == 'chiqim' else '💰'
    return (
        f"{ico} <b>AI tahlil natijasi:</b>\n\n"
        f"📊 Operatsiya : <b>{op_type.upper()}</b>\n"
        f"💰 Summa      : {summa_str}\n"
        f"📅 Sana       : {data.get('sana','?')}\n"
        f"🏷 Tur        : {data.get('tur','?')}\n"
        f"👤 Egasi      : {data.get('egasi','FERUDIN')}\n"
        f"💳 To'lov     : {data.get('tolov','NAQD')}\n"
        f"📝 Izoh       : {data.get('note','—')}\n\n"
        f"<b>Shu ma'lumotlarni saqlaysizmi?</b>"
    )

# ══════════════════════════════════════════════════════════
# AI — CALLBACK (saqlash / tahrirlash / bekor)
# ══════════════════════════════════════════════════════════
async def ai_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q   = update.callback_query
    await q.answer()
    uid = q.from_user.id
    d   = q.data

    if d == 'AI_CANCEL':
        pending_ai.pop(uid, None)
        await q.edit_message_text('❌ Bekor qilindi.')
        return

    if d == 'AI_EDIT':
        if uid not in pending_ai:
            await q.edit_message_text('❌ Sessiya tugadi. Qayta yuboring.')
            return
        ctx.user_data['ai_editing'] = True
        await q.edit_message_text(
            '✏️ <b>Tahrirlash:</b>\n\nFaqat o\'zgartirmoqchi bo\'lgan qatorlarni yozing:\n\n'
            '<code>summa_uzs: 75000\n'
            'summa_usd: 8.5\n'
            'tur: BENZIN\n'
            'egasi: GULOYIM\n'
            'tolov: KARTA\n'
            'note: Avtomobil benzin\n'
            'sana: 25.04.2025\n'
            'operatsiya: CHIQIM</code>',
            parse_mode='HTML')
        return

    if d == 'AI_SAVE':
        if uid not in pending_ai:
            await q.edit_message_text('❌ Sessiya tugadi. Qayta yuboring.')
            return
        pending = pending_ai.pop(uid)
        await _ai_save(q, ctx, pending, uid)

async def ai_edit_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """AI natijasini matn orqali tahrirlash"""
    uid = update.effective_user.id
    if uid not in pending_ai:
        await update.message.reply_text('❌ Tahrirlash sessiyasi tugadi. Qayta yuboring.')
        ctx.user_data.pop('ai_editing', None)
        return

    text = update.message.text.strip()
    data = pending_ai[uid]['data']
    op_t = pending_ai[uid]['op_type']

    for line in text.split('\n'):
        if ':' not in line: continue
        key, _, val = line.partition(':')
        key = key.strip().lower()
        val = val.strip()
        if key in ('summa_uzs','summa_usd'):
            try: data[key] = float(val.replace(',','').replace(' ',''))
            except: pass
        elif key == 'operatsiya':
            op_t = val.strip().lower()
            pending_ai[uid]['op_type'] = op_t
        elif key in ('tur','egasi','tolov','note','sana'):
            data[key] = val

    pending_ai[uid]['data'] = data
    ctx.user_data.pop('ai_editing', None)
    await update.message.reply_text(
        _fmt_ai(data, op_t), parse_mode='HTML', reply_markup=kb_ai_confirm())

async def _ai_save(query, ctx, pending: dict, uid: int):
    data    = pending['data']
    op_type = pending['op_type']
    try:
        sheet_name = 'CHIQIM' if op_type == 'chiqim' else 'KIRIM'
        ss    = get_ss()
        ws    = ss.worksheet(sheet_name)
        col_c = ws.col_values(3)
        last  = 2
        for i, v in enumerate(col_c):
            if i < 2: continue
            if v and str(v).strip(): last = i + 1
        new_row = last + 1

        sana   = data.get('sana') or today_str()
        egasi  = data.get('egasi', 'FERUDIN')
        if str(uid) == CHAT_2 and 'egasi' not in data:
            egasi = 'GULOYIM'
        tur    = data.get('tur','BOSHQA')
        tolov  = data.get('tolov','NAQD')
        usd    = data.get('summa_usd') or ''
        uzs    = data.get('summa_uzs') or ''
        note   = data.get('note','')

        now_t = datetime.now(TZ).strftime('%H:%M')
        ws.update(f'B{new_row}:I{new_row}', [[
            new_row-2, sana, egasi, tur, tolov,
            usd if usd else '', uzs if uzs else '', now_t
        ]], value_input_option='USER_ENTERED')
        ws.update(f'J{new_row}', [[note]])

        uzs_f = f"{float(uzs):,.0f} UZS" if uzs else ''
        usd_f = f"{float(usd):.2f} USD"   if usd else ''
        summa_f = ' / '.join(filter(None,[uzs_f, usd_f])) or '?'
        ico = '💸' if op_type == 'chiqim' else '💰'

        await query.edit_message_text(
            f'✅ <b>Saqlandi!</b>\n\n{ico} {tur}: <b>{summa_f}</b>\n📅 {sana}  👤 {egasi}\n💳 {tolov}'
            + (f'\n📝 {note}' if note else ''),
            parse_mode='HTML')
    except Exception as e:
        await query.edit_message_text(f'❌ Saqlashda xatolik: {str(e)[:120]}')
        logger.error(f'_ai_save: {e}')

# ══════════════════════════════════════════════════════════
# QARZ TIZIMI — HANDLERS
# ══════════════════════════════════════════════════════════
async def qarz_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Barcha QARZ_ callbacklarini boshqarish"""
    q   = update.callback_query
    await q.answer()
    d   = q.data

    if d in ('QARZ_MENU', 'QARZ_BACK'):
        await ensure_qarz()
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton('➕ Qarz berish',  callback_data='QARZ_ADD_BER'),
             InlineKeyboardButton('➕ Qarz olish',   callback_data='QARZ_ADD_OL')],
            [InlineKeyboardButton('✅ Qaytarishlar', callback_data='QARZ_QAYT'),
             InlineKeyboardButton('📋 Ro\'yxat',    callback_data='QARZ_LIST')],
            [InlineKeyboardButton('📊 Statistika',  callback_data='QARZ_STAT')],
            [InlineKeyboardButton('🔙 Asosiy menyu',callback_data='QARZ_ASOSIY')],
        ])
        txt = '💳 <b>QARZ TIZIMI</b>\n\nQuyidagilardan birini tanlang:'
        if hasattr(q.message,'edit_text'):
            await q.edit_message_text(txt, parse_mode='HTML', reply_markup=kb)
        return

    if d == 'QARZ_ASOSIY':
        await q.edit_message_text('👋 <b>FAMILY ACCOUNTING</b>',parse_mode='HTML',reply_markup=kb_main())
        return

    if d in ('QARZ_ADD_BER','QARZ_ADD_OL'):
        tur = 'BERILGAN' if d == 'QARZ_ADD_BER' else 'OLINGAN'
        ctx.user_data['qarz_new'] = {'tur': tur, 'step': 'kim'}
        arr = '→' if tur == 'BERILGAN' else '←'
        await q.edit_message_text(
            f"💬 <b>Yangi qarz ({tur} {arr})</b>\n\n"
            f"{'Kimga berdingiz?' if tur=='BERILGAN' else 'Kimdan oldingiz?'} (ism yozing):",
            parse_mode='HTML')
        return

    if d == 'QARZ_LIST':
        await ensure_qarz()
        try:
            ws   = get_qarz_ws()
            rows = ws.get_all_values()
            lst  = qarz_aktiv(qarz_to_list(rows))
            if not lst:
                await q.edit_message_text('📭 Faol qarzlar yo\'q.',
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('🔙 Orqaga',callback_data='QARZ_BACK')]]))
                return
            lines = ['📋 <b>FAOL QARZLAR:</b>\n']
            ber = [r for r in lst if r['tur']=='BERILGAN']
            ol  = [r for r in lst if r['tur']=='OLINGAN']
            if ber:
                lines.append('💸 <b>Bergan qarzlarim:</b>')
                for r in ber:
                    s = _qarz_sum(r)
                    lines.append(f"  • {r['kim']} → {s} (muddat: {r['muddat']})")
            if ol:
                lines.append('\n💰 <b>Olgan qarzlarim:</b>')
                for r in ol:
                    s = _qarz_sum(r)
                    lines.append(f"  • {r['kim']} → {s} (muddat: {r['muddat']})")
            await q.edit_message_text('\n'.join(lines), parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('🔙 Orqaga',callback_data='QARZ_BACK')]]))
        except Exception as e:
            await q.edit_message_text(f'❌ Xatolik: {e}')
        return

    if d == 'QARZ_QAYT':
        await ensure_qarz()
        try:
            ws   = get_qarz_ws()
            rows = ws.get_all_values()
            lst  = qarz_aktiv(qarz_to_list(rows))
            if not lst:
                await q.edit_message_text('📭 Qaytaradigan faol qarzlar yo\'q.',
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('🔙 Orqaga',callback_data='QARZ_BACK')]]))
                return
            buttons = []
            for r in lst:
                s   = _qarz_sum(r)
                lbl = f"{'→' if r['tur']=='BERILGAN' else '←'} {r['kim']} ({s})"
                buttons.append([InlineKeyboardButton(lbl, callback_data=f"QARZ_DONE_{r['_row']}")])
            buttons.append([InlineKeyboardButton('🔙 Orqaga',callback_data='QARZ_BACK')])
            await q.edit_message_text('✅ <b>Qaysi qarz qaytarildi?</b>',
                parse_mode='HTML', reply_markup=InlineKeyboardMarkup(buttons))
        except Exception as e:
            await q.edit_message_text(f'❌ Xatolik: {e}')
        return

    if d.startswith('QARZ_DONE_'):
        row_idx = int(d.split('_')[-1])
        try:
            ws    = get_qarz_ws()
            today = today_str()

            # Qarz ma'lumotlarini olish
            row       = ws.row_values(row_idx)
            tur       = row[1] if len(row) > 1 else 'BERILGAN'
            kim       = row[2] if len(row) > 2 else '?'
            summa_uzs = row[3] if len(row) > 3 and row[3] else ''
            summa_usd = row[4] if len(row) > 4 and row[4] else ''

            # QARZ ni TUGADI deb belgilash
            ws.update_cell(row_idx, 8, 'TUGADI')
            ws.update_cell(row_idx, 9, today)

            egasi = 'FERUDIN'
            if tur == 'BERILGAN':
                # Bergan qarzim QAYTIB KELDI → KIRIM → balans ko'payadi
                qarz_to_sheet('KIRIM', egasi, summa_usd, summa_uzs,
                    f'Qarz qaytdi: {kim}')
                icon   = '✅💰'
                effect = "Balansga qaytdi (+)"
            else:
                # Olgan qarzimni QAYTARDIM → CHIQIM → balans kamayadi
                qarz_to_sheet('CHIQIM', egasi, summa_usd, summa_uzs,
                    f'Qarz qaytarildi: {kim}')
                icon   = '✅💸'
                effect = "Balansdan ayrildi (−)"

            bal = get_balance()
            s_uzs = f"{float(summa_uzs):,.0f} UZS" if summa_uzs else ''
            s_usd = f"{float(summa_usd):.2f} USD"   if summa_usd else ''
            summa_str = ' / '.join(filter(None, [s_uzs, s_usd])) or '?'

            await q.edit_message_text(
                f'{icon} <b>{kim}</b> bilan hisob-kitob yakunlandi!\n\n'
                f'💰 {summa_str}\n'
                f'📅 {today}\n'
                f'<i>{effect}</i>\n'
                f'💰 Joriy balans: <b>{round(bal, 2)}$</b>',
                parse_mode='HTML')
        except Exception as e:
            await q.edit_message_text(f'❌ Xatolik: {e}')
        return

    if d == 'QARZ_STAT':
        await ensure_qarz()
        try:
            ws   = get_qarz_ws()
            rows = ws.get_all_values()
            lst  = qarz_aktiv(qarz_to_list(rows))
            ber_uzs = sum(float(r['summa_uzs']) for r in lst if r['tur']=='BERILGAN' and r.get('summa_uzs'))
            ber_usd = sum(float(r['summa_usd']) for r in lst if r['tur']=='BERILGAN' and r.get('summa_usd'))
            ol_uzs  = sum(float(r['summa_uzs']) for r in lst if r['tur']=='OLINGAN'  and r.get('summa_uzs'))
            ol_usd  = sum(float(r['summa_usd']) for r in lst if r['tur']=='OLINGAN'  and r.get('summa_usd'))
            today_d = date.today()
            overdue = []
            for r in lst:
                try:
                    deadline = datetime.strptime(r['muddat'],'%d.%m.%Y').date()
                    if deadline < today_d: overdue.append(r)
                except: pass
            txt = (
                f'📊 <b>QARZ STATISTIKASI</b>\n\n'
                f'💸 <b>Bergan qarzlarim (faol):</b>\n'
                f'   {ber_uzs:,.0f} UZS / {ber_usd:.2f} USD\n'
                f'   ({sum(1 for r in lst if r["tur"]=="BERILGAN")} ta)\n\n'
                f'💰 <b>Olgan qarzlarim (faol):</b>\n'
                f'   {ol_uzs:,.0f} UZS / {ol_usd:.2f} USD\n'
                f'   ({sum(1 for r in lst if r["tur"]=="OLINGAN")} ta)\n'
            )
            if overdue:
                txt += f'\n⚠️ <b>Muddati o\'tgan:</b> {len(overdue)} ta\n'
                for r in overdue[:5]:
                    txt += f"  • {r['kim']} (muddat: {r['muddat']})\n"
            await q.edit_message_text(txt, parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('🔙 Orqaga',callback_data='QARZ_BACK')]]))
        except Exception as e:
            await q.edit_message_text(f'❌ Xatolik: {e}')
        return

def _qarz_sum(r: dict) -> str:
    uzs = f"{float(r['summa_uzs']):,.0f} UZS" if r.get('summa_uzs') else ''
    usd = f"{float(r['summa_usd']):.2f} USD"   if r.get('summa_usd') else ''
    return ' / '.join(filter(None,[uzs,usd])) or '?'

async def qarz_input(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """QARZ kiritish bosqichlari"""
    uid  = update.effective_user.id
    text = update.message.text.strip()
    q    = ctx.user_data.get('qarz_new')
    if not q: return
    step = q.get('step')

    if step == 'kim':
        q['kim'] = text; q['step'] = 'summa'
        await update.message.reply_text(
            '💰 Summa qancha?\n'
            '<i>UZS uchun: 500000\nUSD uchun: 50USD</i>',
            parse_mode='HTML')

    elif step == 'summa':
        tu = text.upper().replace(' ','')
        if 'USD' in tu:
            q['summa_usd'] = float(re.sub(r'[^\d.]','',tu))
            q['summa_uzs'] = ''
        else:
            q['summa_uzs'] = float(re.sub(r'[^\d.]','',text))
            q['summa_usd'] = ''
        q['step'] = 'muddat'
        await update.message.reply_text(
            '📅 Qaytarish muddati? (DD.MM.YYYY)\n<i>Masalan: 15.06.2025</i>',
            parse_mode='HTML')

    elif step == 'muddat':
        try: datetime.strptime(text,'%d.%m.%Y')
        except:
            await update.message.reply_text('❌ Format noto\'g\'ri. DD.MM.YYYY ko\'rinishida:')
            return
        q['muddat'] = text; q['step'] = 'note'
        await update.message.reply_text('📝 Izoh (ixtiyoriy, yo\'q bo\'lsa — yuboring):')

    elif step == 'note':
        q['note'] = text if text != '—' else ''
        q.pop('step')
        await _qarz_save(update, q)
        ctx.user_data.pop('qarz_new', None)

def qarz_to_sheet(sheet_name: str, egasi: str, usd_val, uzs_val, note: str):
    """Qarz operatsiyasini CHIQIM yoki KIRIM ga yozib balansni yangilaydi"""
    sh    = get_ss().worksheet(sheet_name)
    today = datetime.now(TZ).strftime('%d.%m.%Y')
    col_c = sh.col_values(3)
    last  = 2
    for i, v in enumerate(col_c):
        if i < 2: continue
        if v and str(v).strip(): last = i + 1
    new_row = last + 1
    now_t = datetime.now(TZ).strftime('%H:%M')
    sh.update(f'B{new_row}:I{new_row}', [[
        new_row - 2, today, egasi, 'BOSHQA', 'CASH',
        usd_val if usd_val else '',
        uzs_val if uzs_val else '', now_t
    ]], value_input_option='USER_ENTERED')
    sh.update(f'J{new_row}', [[note]])
    logger.info(f'qarz_to_sheet → {sheet_name} row {new_row}: {note}')
    return new_row

async def _qarz_save(update, q: dict):
    try:
        await ensure_qarz()
        ws    = get_qarz_ws()
        rows  = ws.get_all_values()
        num   = len(rows)
        today = today_str()
        ws.append_row([
            num, q['tur'], q['kim'],
            q.get('summa_uzs',''), q.get('summa_usd',''),
            today, q['muddat'], 'AKTIV', '', q.get('note','')
        ], value_input_option='USER_ENTERED')

        tur   = q['tur']
        egasi = 'FERUDIN'

        if tur == 'BERILGAN':
            # Men qarz BERDIM → CHIQIM → balans kamayadi
            qarz_to_sheet('CHIQIM', egasi,
                q.get('summa_usd') or '', q.get('summa_uzs') or '',
                f"Qarz berildi: {q['kim']}")
            icon   = '💸'
            effect = "Balansdan ayrildi (−)"
        else:
            # Men qarz OLDIM → KIRIM → balans ko'payadi
            qarz_to_sheet('KIRIM', egasi,
                q.get('summa_usd') or '', q.get('summa_uzs') or '',
                f"Qarz olindi: {q['kim']}")
            icon   = '💰'
            effect = "Balansga qo'shildi (+)"

        bal = get_balance()
        arr = '→' if tur == 'BERILGAN' else '←'
        s   = _qarz_sum(q)
        await update.message.reply_text(
            f'✅ <b>Qarz saqlandi!</b>\n\n'
            f'{icon} {arr} <b>{q["kim"]}</b>\n'
            f'💰 {s}\n'
            f'📅 {today}  ⏰ Muddat: {q["muddat"]}\n'
            f'<i>{effect}</i>\n'
            f'💰 Joriy balans: <b>{round(bal, 2)}$</b>'
            + (f'\n📝 {q["note"]}' if q.get('note') else ''),
            parse_mode='HTML')
    except Exception as e:
        await update.message.reply_text(f'❌ Qarz saqlashda xatolik: {str(e)[:100]}')
        logger.error(f'_qarz_save: {e}')

# ══════════════════════════════════════════════════════════
# QARZ MUDDAT NOTIFICATION — kunlik 09:00
# ══════════════════════════════════════════════════════════
async def qarz_notify_job(ctx: ContextTypes.DEFAULT_TYPE):
    try:
        await ensure_qarz()
        ws   = get_qarz_ws()
        rows = ws.get_all_values()
        lst  = qarz_aktiv(qarz_to_list(rows))
        today_d = date.today()
        for r in lst:
            try: deadline = datetime.strptime(r['muddat'],'%d.%m.%Y').date()
            except: continue
            days_left = (deadline - today_d).days
            if days_left not in (3, 1, 0, -1): continue
            s = _qarz_sum(r)
            if days_left > 0: urgency = f'⏰ {days_left} kun qoldi!'
            elif days_left == 0: urgency = '🚨 BUGUN muddati tugaydi!'
            else: urgency = f'🔴 Muddat {abs(days_left)} kun oldin o\'tdi!'
            tur_txt = '💸 Berdi' if r['tur']=='BERILGAN' else '💰 Oldi'
            txt = (
                f'⚠️ <b>QARZ ESLATMA</b>\n\n'
                f'{tur_txt}: <b>{r["kim"]}</b>\n'
                f'💰 {s}\n📅 Muddat: {r["muddat"]}\n{urgency}'
            )
            for cid in [CHAT_1, CHAT_2]:
                try: await ctx.bot.send_message(chat_id=cid, text=txt, parse_mode='HTML')
                except Exception as se: logger.error(f'qarz_notify send: {se}')
    except Exception as e:
        logger.error(f'qarz_notify_job: {e}')

# ══════════════════════════════════════════════════════════
# ADMIN PANEL — KATEGORIYALAR
# ══════════════════════════════════════════════════════════
async def admin_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q   = update.callback_query
    await q.answer()
    d   = q.data

    if d in ('ADMIN_MENU','ADM_BACK'):
        await load_categories()
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton('📂 Chiqim turlari', callback_data='ADM_CAT_CH'),
             InlineKeyboardButton('📂 Kirim turlari',  callback_data='ADM_CAT_KI')],
            [InlineKeyboardButton('🔄 Qayta yuklash',  callback_data='ADM_RELOAD')],
            [InlineKeyboardButton('🔙 Asosiy menyu',   callback_data='ADM_MAIN')],
        ])
        await q.edit_message_text('⚙️ <b>ADMIN PANEL</b>\n\nKategoriyalarni boshqarish:',
            parse_mode='HTML', reply_markup=kb)
        return

    if d == 'ADM_MAIN':
        await q.edit_message_text('👋 <b>FAMILY ACCOUNTING</b>',parse_mode='HTML',reply_markup=kb_main())
        return

    if d in ('ADM_CAT_CH','ADM_CAT_KI'):
        cat_type = 'chiqim' if d=='ADM_CAT_CH' else 'kirim'
        cats     = get_chiqim_turs() if cat_type=='chiqim' else get_kirim_turs()
        num_lst  = '\n'.join(f'{i+1}. {c}' for i,c in enumerate(cats))
        lbl = 'Chiqim' if cat_type=='chiqim' else 'Kirim'
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton('➕ Qo\'shish',       callback_data=f'ADM_ADD_{cat_type}')],
            [InlineKeyboardButton('🗑 O\'chirish',      callback_data=f'ADM_DEL_{cat_type}')],
            [InlineKeyboardButton('✏️ Nomini o\'zgartirish', callback_data=f'ADM_REN_{cat_type}')],
            [InlineKeyboardButton('🔙 Orqaga',          callback_data='ADM_BACK')],
        ])
        await q.edit_message_text(
            f'📂 <b>{lbl} kategoriyalari:</b>\n\n{num_lst}\n\nAmal tanlang:',
            parse_mode='HTML', reply_markup=kb)
        return

    if d in ('ADM_ADD_chiqim','ADM_ADD_kirim'):
        cat_type = d.split('_',2)[-1]
        ctx.user_data['admin_action'] = {'action':'add','cat_type':cat_type}
        lbl = 'chiqim' if cat_type=='chiqim' else 'kirim'
        await q.edit_message_text(
            f'✏️ Yangi <b>{lbl}</b> kategoriyasi nomini yozing\n'
            f'<i>(katta harflar bilan, masalan: DORIXONA)</i>',
            parse_mode='HTML')
        return

    if d in ('ADM_DEL_chiqim','ADM_DEL_kirim'):
        cat_type = d.split('_',2)[-1]
        cats     = get_chiqim_turs() if cat_type=='chiqim' else get_kirim_turs()
        buttons  = []
        for i,c in enumerate(cats):
            buttons.append([InlineKeyboardButton(f'🗑 {c}',callback_data=f'ADM_DELC_{cat_type}_{i}')])
        buttons.append([InlineKeyboardButton('🔙 Orqaga',callback_data=f'ADM_CAT_{"CH" if cat_type=="chiqim" else "KI"}')])
        await q.edit_message_text('🗑 O\'chirish uchun tanlang:',reply_markup=InlineKeyboardMarkup(buttons))
        return

    if d.startswith('ADM_DELC_'):
        parts    = d.split('_')
        cat_type = parts[2]
        idx      = int(parts[3])
        cats     = (get_chiqim_turs() if cat_type=='chiqim' else get_kirim_turs())[:]
        if idx < len(cats):
            deleted = cats.pop(idx)
            _cats[cat_type] = cats
            saved = await save_categories()
            await q.edit_message_text(
                f'{"✅" if saved else "⚠️"} <b>{deleted}</b> o\'chirildi. '
                f'{"Saqlandi ✓" if saved else "Saqlashda xatolik!"}',
                parse_mode='HTML')
        return

    if d in ('ADM_REN_chiqim','ADM_REN_kirim'):
        cat_type = d.split('_',2)[-1]
        cats     = get_chiqim_turs() if cat_type=='chiqim' else get_kirim_turs()
        buttons  = []
        for i,c in enumerate(cats):
            buttons.append([InlineKeyboardButton(f'✏️ {c}',callback_data=f'ADM_RENS_{cat_type}_{i}')])
        buttons.append([InlineKeyboardButton('🔙 Orqaga',callback_data=f'ADM_CAT_{"CH" if cat_type=="chiqim" else "KI"}')])
        await q.edit_message_text('✏️ O\'zgartirish uchun tanlang:',reply_markup=InlineKeyboardMarkup(buttons))
        return

    if d.startswith('ADM_RENS_'):
        parts    = d.split('_')
        cat_type = parts[2]
        idx      = int(parts[3])
        cats     = get_chiqim_turs() if cat_type=='chiqim' else get_kirim_turs()
        old_name = cats[idx] if idx < len(cats) else '?'
        ctx.user_data['admin_action'] = {'action':'rename','cat_type':cat_type,'idx':idx,'old':old_name}
        await q.edit_message_text(f'✏️ <b>{old_name}</b> uchun yangi nom yozing:',parse_mode='HTML')
        return

    if d == 'ADM_RELOAD':
        await load_categories()
        await q.edit_message_text('✅ Kategoriyalar qayta yuklandi!')
        return

async def admin_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Admin panel matn kiritish"""
    a = ctx.user_data.get('admin_action')
    if not a: return
    text     = update.message.text.strip().upper()
    action   = a['action']
    cat_type = a['cat_type']

    if action == 'add':
        cats = (get_chiqim_turs() if cat_type=='chiqim' else get_kirim_turs())[:]
        if text in cats:
            await update.message.reply_text(f'⚠️ <b>{text}</b> allaqachon mavjud.',parse_mode='HTML')
        else:
            cats.append(text)
            _cats[cat_type] = cats
            saved = await save_categories()
            await update.message.reply_text(
                f'{"✅" if saved else "⚠️"} <b>{text}</b> qo\'shildi. '
                f'{"Saqlandi ✓" if saved else "Saqlashda xatolik!"}',
                parse_mode='HTML')

    elif action == 'rename':
        idx  = a['idx']
        cats = (get_chiqim_turs() if cat_type=='chiqim' else get_kirim_turs())[:]
        if idx < len(cats):
            old = cats[idx]
            cats[idx] = text
            _cats[cat_type] = cats
            saved = await save_categories()
            await update.message.reply_text(
                f'{"✅" if saved else "⚠️"} <b>{old}</b> → <b>{text}</b> o\'zgartirildi. '
                f'{"Saqlandi ✓" if saved else "Saqlashda xatolik!"}',
                parse_mode='HTML')

    ctx.user_data.pop('admin_action', None)

# ══════════════════════════════════════════════════════════
# BOSHQA KOMANDALAR
# ══════════════════════════════════════════════════════════
async def help_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ok(update): return
    await update.message.reply_text(
        '📚 <b>Barcha komandalar:</b>\n\n'
        '/start — Asosiy menyu\n'
        '/qarz — Qarz tizimi\n'
        '/tasks — Bugungi vazifalar\n'
        '/memory — Xotiradan qidirish\n'
        '/namoz — Bugungi namoz vaqtlari\n'
        '/admin — Kategoriyalar boshqaruvi\n'
        '/debug — Tizim holati\n'
        '/hisobot — Hisobot filtri\n\n'
        '💡 <i>Rasm yuboring</i> — chek tahlili\n'
        '🎙 <i>Ovoz yuboring</i> — amal / task / xotira',
        parse_mode='HTML')

async def namoz_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ok(update): return
    msg = await update.message.reply_text('⏳ Namoz vaqtlari yuklanmoqda...')
    times = await get_prayer_times(datetime.now(TZ).strftime('%d-%m-%Y'))
    if not times:
        await msg.edit_text('❌ Namoz vaqtlari olinmadi. Internet yoki API muammosi.')
        return
    now_hm = datetime.now(TZ).strftime('%H:%M')
    sana   = datetime.now(TZ).strftime('%d.%m.%Y')
    txt    = f'🕌 <b>Namoz vaqtlari — {sana}</b>\n\n'
    for namoz, vaqt in times.items():
        emoji  = NAMOZ_EMOJI.get(namoz, '🕌')
        marker = '✅' if vaqt < now_hm else '⏰'
        txt   += f'{marker} {emoji} <b>{namoz.upper()}</b>: {vaqt}\n'
    await msg.edit_text(txt, parse_mode='HTML')

async def hisobot_start_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ok(update): return ConversationHandler.END
    ctx.user_data['h'] = {}
    await update.message.reply_text(
        '🔍 <b>HISOBOT</b>\n\nAmal turini tanlang:',
        parse_mode='HTML', reply_markup=kb_h_tip())
    return H_TIP

async def debug_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ok(update): return
    try:
        bal  = get_balance()
        rows = get_ss().worksheet('CHIQIM').get_all_values()
        lines = [f'Balance: {bal}', f'CHIQIM: {len(rows)} qator',
                 f'Kategoriyalar: {len(get_chiqim_turs())} ta']
        await update.message.reply_text('\n'.join(lines))
    except Exception as e:
        await update.message.reply_text(f'Xato: {e}')

async def qarz_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ok(update): return
    await ensure_qarz()
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton('➕ Qarz berish',  callback_data='QARZ_ADD_BER'),
         InlineKeyboardButton('➕ Qarz olish',   callback_data='QARZ_ADD_OL')],
        [InlineKeyboardButton('✅ Qaytarishlar', callback_data='QARZ_QAYT'),
         InlineKeyboardButton('📋 Ro\'yxat',    callback_data='QARZ_LIST')],
        [InlineKeyboardButton('📊 Statistika',  callback_data='QARZ_STAT')],
    ])
    await update.message.reply_text('💳 <b>QARZ TIZIMI</b>\n\nQuyidagilardan birini tanlang:',
        parse_mode='HTML', reply_markup=kb)

async def admin_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ok(update): return
    await load_categories()
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton('📂 Chiqim turlari', callback_data='ADM_CAT_CH'),
         InlineKeyboardButton('📂 Kirim turlari',  callback_data='ADM_CAT_KI')],
        [InlineKeyboardButton('🔄 Qayta yuklash',  callback_data='ADM_RELOAD')],
    ])
    await update.message.reply_text('⚙️ <b>ADMIN PANEL</b>\n\nKategoriyalarni boshqarish:',
        parse_mode='HTML', reply_markup=kb)

# ══════════════════════════════════════════════════════════
# TASKS + REMINDER TIZIMI
# ══════════════════════════════════════════════════════════

async def ensure_tasks_sheet():
    try:
        sh = get_ss()
        try:
            sh.worksheet('TASKS')
        except Exception:
            ws = sh.add_worksheet(title='TASKS', rows=1000, cols=8)
            ws.update('A1:G1', [['id', 'yaratilgan', 'vaqt', 'matn', 'egasi', 'holat', 'chat_id']])
    except Exception as e:
        logger.error(f'ensure_tasks_sheet: {e}')

async def task_reminder_job(ctx: ContextTypes.DEFAULT_TYPE):
    d       = ctx.job.data
    matn    = d['matn']
    egasi   = d['egasi']
    row_num = d['row']
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton('✅ Bajarildi', callback_data=f'TASK_DONE_{row_num}'),
        InlineKeyboardButton('⏭ O\'tkazish', callback_data=f'TASK_SKIP_{row_num}'),
    ]])
    txt = f'⏰ <b>ESLATMA!</b>\n\n📋 {matn}\n👤 {egasi}'
    if egasi == 'FERUDIN':   targets = [CHAT_1]
    elif egasi == 'GULOYIM': targets = [CHAT_2]
    else:                    targets = [CHAT_1, CHAT_2]
    for cid in targets:
        try: await ctx.bot.send_message(chat_id=cid, text=txt, parse_mode='HTML', reply_markup=kb)
        except Exception as e: logger.error(f'task_reminder {cid}: {e}')

async def tasks_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q     = update.callback_query
    await q.answer()
    parts  = q.data.split('_')   # TASK_DONE_5 / TASK_SKIP_5
    action = parts[1]
    row_n  = int(parts[2])
    status = 'BAJARILDI' if action == 'DONE' else 'O\'TKAZILDI'
    icon   = '✅' if action == 'DONE' else '⏭'
    def _upd():
        get_ss().worksheet('TASKS').update_cell(row_n, 6, status)
    try:
        await asyncio.to_thread(_upd)
        old = q.message.text or ''
        await q.edit_message_text(f'{icon} <b>{status}</b>\n\n{old}', parse_mode='HTML')
    except Exception as e:
        await q.edit_message_text(f'❌ Xatolik: {e}')

async def save_and_schedule_task(app_obj, matn: str, vaqt_str: str, egasi: str, chat_id: str):
    def _save():
        ws    = get_ss().worksheet('TASKS')
        vals  = ws.get_all_values()
        today = datetime.now(TZ).strftime('%d.%m.%Y')
        new_id = len(vals)
        ws.append_row([new_id, today, vaqt_str, matn, egasi, 'FAOL', chat_id],
                      value_input_option='USER_ENTERED')
        return new_id, len(vals) + 1
    try:
        task_id, row_num = await asyncio.to_thread(_save)
        try:
            reminder_dt = TZ.localize(datetime.strptime(vaqt_str, '%d.%m.%Y %H:%M'))
        except:
            return task_id, row_num
        if reminder_dt > datetime.now(TZ):
            app_obj.job_queue.run_once(
                task_reminder_job, when=reminder_dt,
                data={'matn': matn, 'egasi': egasi, 'row': row_num},
                name=f'task_{task_id}')
        return task_id, row_num
    except Exception as e:
        logger.error(f'save_and_schedule_task: {e}')
        return None, None

async def reschedule_pending_tasks(app_obj):
    try:
        def _get():
            return get_ss().worksheet('TASKS').get_all_values()
        vals = await asyncio.to_thread(_get)
        now  = datetime.now(TZ)
        count = 0
        for i, row in enumerate(vals[1:], start=2):
            if len(row) < 6 or row[5] != 'FAOL': continue
            try:
                reminder_dt = TZ.localize(datetime.strptime(row[2], '%d.%m.%Y %H:%M'))
            except: continue
            if reminder_dt <= now: continue
            app_obj.job_queue.run_once(
                task_reminder_job, when=reminder_dt,
                data={'matn': row[3], 'egasi': row[4], 'row': i},
                name=f'task_rs_{i}')
            count += 1
        if count: logger.info(f'Restart: {count} ta task qayta scheduled')
    except Exception as e:
        logger.error(f'reschedule_pending_tasks: {e}')

async def tasks_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ok(update): return
    def _get():
        ws    = get_ss().worksheet('TASKS')
        vals  = ws.get_all_values()
        today = datetime.now(TZ).date()
        result = []
        for row in vals[1:]:
            if len(row) < 6 or row[5] != 'FAOL': continue
            try:
                dt = TZ.localize(datetime.strptime(row[2], '%d.%m.%Y %H:%M'))
                if dt.date() >= today:
                    result.append({'vaqt': row[2], 'matn': row[3], 'egasi': row[4]})
            except: continue
        result.sort(key=lambda x: x['vaqt'])
        return result
    try:
        tasks = await asyncio.to_thread(_get)
        if not tasks:
            await update.message.reply_text("📋 Faol tasklar yo'q.", reply_markup=kb_main())
            return
        txt = '📋 <b>FAOL TASKLAR:</b>\n\n'
        for t in tasks[:15]:
            txt += f"⏰ <b>{t['vaqt']}</b>  👤 {t['egasi']}\n📝 {t['matn']}\n\n"
        await update.message.reply_text(txt, parse_mode='HTML', reply_markup=kb_main())
    except Exception as e:
        await update.message.reply_text(f'❌ Xatolik: {e}')

# ══════════════════════════════════════════════════════════
# MEMORY TIZIMI
# ══════════════════════════════════════════════════════════

async def ensure_memory_sheet():
    try:
        sh = get_ss()
        try:
            sh.worksheet('MEMORY')
        except Exception:
            ws = sh.add_worksheet(title='MEMORY', rows=1000, cols=6)
            ws.update('A1:E1', [['id', 'sana', 'kalit', 'qiymat', 'kim']])
    except Exception as e:
        logger.error(f'ensure_memory_sheet: {e}')

async def memory_save(kalit: str, qiymat: str, kim: str) -> str:
    def _save():
        ws    = get_ss().worksheet('MEMORY')
        vals  = ws.get_all_values()
        today = datetime.now(TZ).strftime('%d.%m.%Y')
        for i, row in enumerate(vals[1:], start=2):
            if len(row) >= 3 and row[2].lower() == kalit.lower():
                ws.update(f'B{i}:E{i}', [[today, kalit, qiymat, kim]])
                return 'yangilandi'
        ws.append_row([len(vals), today, kalit, qiymat, kim])
        return 'saqlandi'
    try:
        return await asyncio.to_thread(_save)
    except Exception as e:
        logger.error(f'memory_save: {e}')
        return 'xato'

async def memory_search(query: str) -> list:
    def _search():
        ws  = get_ss().worksheet('MEMORY')
        q   = query.lower()
        out = []
        for row in ws.get_all_values()[1:]:
            if len(row) < 4: continue
            if q in row[2].lower() or q in row[3].lower():
                out.append({'kalit': row[2], 'qiymat': row[3],
                            'kim': row[4] if len(row) > 4 else '', 'sana': row[1]})
        return out
    try:
        return await asyncio.to_thread(_search)
    except Exception as e:
        logger.error(f'memory_search: {e}')
        return []

async def memory_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ok(update): return
    query = ' '.join(ctx.args) if ctx.args else ''
    if not query:
        await update.message.reply_text(
            '🧠 <b>MEMORY</b>\n\n'
            'Qidirish: <code>/memory Xurshid</code>\n'
            'Saqlash: "Xurshid raqami: 90-123-45-67"\n'
            "So'rov: \"Xurshid raqami nima?\"",
            parse_mode='HTML')
        return
    results = await memory_search(query)
    if not results:
        await update.message.reply_text(f'🔍 "<b>{query}</b>" topilmadi.', parse_mode='HTML')
        return
    txt = f'🧠 <b>MEMORY — "{query}":</b>\n\n'
    for r in results[:10]:
        txt += f'📌 <b>{r["kalit"]}</b>: {r["qiymat"]}\n<i>{r["sana"]} | {r["kim"]}</i>\n\n'
    await update.message.reply_text(txt, parse_mode='HTML')

# ── Matn → Claude → to'g'ri tizimga yo'naltirish ─────────

async def analyze_and_route(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ANTHROPIC_API_KEY: return
    text = update.message.text.strip()
    if len(text) < 3: return
    cid    = str(update.effective_chat.id)
    kim    = 'FERUDIN' if cid == CHAT_1 else 'GULOYIM'
    today  = today_str()
    now_hm = datetime.now(TZ).strftime('%H:%M')
    import anthropic
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    prompt = (
        f'Xabar: "{text}"\nBugun: {today} {now_hm} Toshkent\n\n'
        f'Faqat JSON qaytargin:\n'
        f'{{"intent":"<task|memory_save|memory_query|ignore>",'
        f'"task_matn":"<null|vazifa>",'
        f'"task_vaqt":"<null|DD.MM.YYYY HH:MM>",'
        f'"task_egasi":"<FERUDIN|GULOYIM|IKKALASI>",'
        f'"memory_kalit":"<null|kalit>",'
        f'"memory_qiymat":"<null|qiymat>"}}\n\n'
        f'Qoidalar:\n'
        f'- task: aniq vaqtli eslatma ("ertaga 10da", "soat 15:00 da") — task_vaqt NULL bo\'lsa ignore\n'
        f'- memory_save: "X:Y", "X raqami Y", "eslab qol X=Y"\n'
        f'- memory_query: "X nima?", "X qancha?", "X qaerda?"\n'
        f'- ignore: oddiy suhbat, savol-javob'
    )
    msg = None
    try:
        msg = await update.message.reply_text('⏳')
        resp = await asyncio.to_thread(
            client.messages.create,
            model='claude-haiku-4-5-20251001',
            max_tokens=200,
            messages=[{'role': 'user', 'content': prompt}])
        raw  = re.sub(r'```(?:json)?\s*', '', resp.content[0].text.strip()).strip('`').strip()
        data = json.loads(raw)
        intent = data.get('intent', 'ignore')

        if intent == 'task':
            matn  = data.get('task_matn') or text
            vaqt  = data.get('task_vaqt')
            egasi = data.get('task_egasi', kim)
            if not vaqt:
                await msg.edit_text("⚠️ Vaqt aniqlanmadi. Masalan: \"02.05.2026 10:00 da bozor\"")
                return
            await save_and_schedule_task(ctx.application, matn, vaqt, egasi, cid)
            await msg.edit_text(
                f'✅ <b>Task saqlandi!</b>\n\n📋 {matn}\n⏰ {vaqt}\n👤 {egasi}',
                parse_mode='HTML')

        elif intent == 'memory_save':
            kalit  = data.get('memory_kalit') or ''
            qiymat = data.get('memory_qiymat') or ''
            if not kalit or not qiymat:
                await msg.delete(); return
            r = await memory_save(kalit, qiymat, kim)
            await msg.edit_text(
                f'🧠 <b>Xotiraga {r}!</b>\n\n📌 <b>{kalit}</b>: {qiymat}',
                parse_mode='HTML')

        elif intent == 'memory_query':
            kalit   = data.get('memory_kalit') or text
            results = await memory_search(kalit)
            if not results:
                await msg.edit_text(f'🔍 "<b>{kalit}</b>" topilmadi.', parse_mode='HTML')
            else:
                txt2 = f'🧠 <b>{kalit}:</b>\n\n'
                for r in results[:5]:
                    txt2 += f'📌 <b>{r["kalit"]}</b>: {r["qiymat"]}\n'
                await msg.edit_text(txt2, parse_mode='HTML')

        else:
            await msg.delete()

    except json.JSONDecodeError:
        if msg:
            try: await msg.delete()
            except: pass
    except Exception as e:
        if msg:
            try: await msg.delete()
            except: pass
        logger.error(f'analyze_and_route: {e}')

# ══════════════════════════════════════════════════════════
# NAMOZ TIZIMI
# ══════════════════════════════════════════════════════════
NAMOZ_UZ    = ['bomdod', 'peshin', 'asr', 'shom', 'xufton']
NAMOZ_COL   = {'bomdod': 2, 'peshin': 3, 'asr': 4, 'shom': 5, 'xufton': 6}
NAMOZ_EMOJI = {'bomdod': '🌅', 'peshin': '☀️', 'asr': '🌤', 'shom': '🌇', 'xufton': '🌙'}

async def ensure_namoz_sheet():
    try:
        sh = get_ss()
        try:
            sh.worksheet('NAMOZ')
        except Exception:
            ws = sh.add_worksheet(title='NAMOZ', rows=1000, cols=8)
            ws.update('A1:G1', [['sana', 'bomdod', 'peshin', 'asr', 'shom', 'xufton', 'kim']])
            logger.info("NAMOZ varag'i yaratildi")
    except Exception as e:
        logger.error(f'ensure_namoz_sheet: {e}')

def _fetch_prayer_times_sync(date_str: str) -> dict:
    url = (f'https://api.aladhan.com/v1/timings/{date_str}'
           f'?latitude=41.2995&longitude=69.2401&method=3')
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'FamilyBot/1.0'})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
        t = data['data']['timings']
        return {
            'bomdod': t['Fajr'][:5],
            'peshin': t['Dhuhr'][:5],
            'asr':    t['Asr'][:5],
            'shom':   t['Maghrib'][:5],
            'xufton': t['Isha'][:5],
        }
    except Exception as e:
        logger.error(f'_fetch_prayer_times_sync: {e}')
        return {}

async def get_prayer_times(date_str: str = None) -> dict:
    if not date_str:
        date_str = datetime.now(TZ).strftime('%d-%m-%Y')
    return await asyncio.to_thread(_fetch_prayer_times_sync, date_str)

def _parse_prayer_dt(time_str: str, date_obj) -> datetime:
    h, m = map(int, time_str.split(':')[:2])
    return TZ.localize(datetime(date_obj.year, date_obj.month, date_obj.day, h, m, 0))

def _save_namoz_sync(sana: str, namoz: str, kim: str, status: str):
    col_idx  = NAMOZ_COL[namoz]
    ws       = get_ss().worksheet('NAMOZ')
    all_vals = ws.get_all_values()
    target_row = None
    for i, row in enumerate(all_vals[1:], start=2):
        if len(row) >= 7 and row[0] == sana and row[6] == kim:
            target_row = i
            break
    if target_row is None:
        next_row = len(all_vals) + 1
        ws.update(f'A{next_row}:G{next_row}', [[sana, '', '', '', '', '', kim]])
        target_row = next_row
    ws.update_cell(target_row, col_idx, status)

async def save_namoz_response(sana: str, namoz: str, kim: str, status: str):
    try:
        await asyncio.to_thread(_save_namoz_sync, sana, namoz, kim, status)
    except Exception as e:
        logger.error(f'save_namoz_response: {e}')

async def prayer_reminder_job(ctx: ContextTypes.DEFAULT_TYPE):
    d     = ctx.job.data
    namoz = d['namoz']
    vaqt  = d['vaqt']
    txt = (f"{NAMOZ_EMOJI[namoz]} <b>{namoz.upper()}</b> namozi\n\n"
           f"⏰ 20 daqiqadan keyin ({vaqt})\nTayyorgarlik ko'ring! 🤲")
    for cid in [CHAT_1, CHAT_2]:
        try: await ctx.bot.send_message(chat_id=cid, text=txt, parse_mode='HTML')
        except Exception as e: logger.error(f'prayer_reminder {cid}: {e}')

async def prayer_time_job(ctx: ContextTypes.DEFAULT_TYPE):
    d     = ctx.job.data
    namoz = d['namoz']
    vaqt  = d['vaqt']
    txt = (f"{NAMOZ_EMOJI[namoz]} <b>{namoz.upper()} vaqti kirdi!</b>\n\n"
           f"🕌 {vaqt} — Alloh qabul qilsin! 🤲")
    for cid in [CHAT_1, CHAT_2]:
        try: await ctx.bot.send_message(chat_id=cid, text=txt, parse_mode='HTML')
        except Exception as e: logger.error(f'prayer_time {cid}: {e}')

async def prayer_question_job(ctx: ContextTypes.DEFAULT_TYPE):
    d     = ctx.job.data
    namoz = d['namoz']
    sana  = d['sana']
    for cid, kim in [(CHAT_1, 'FERUDIN'), (CHAT_2, 'GULOYIM')]:
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Ha, o'qidim", callback_data=f'NAMOZ_OK_{namoz}_{sana}_{cid}'),
            InlineKeyboardButton("❌ O'qimadim",   callback_data=f'NAMOZ_NO_{namoz}_{sana}_{cid}'),
        ]])
        txt = f"{NAMOZ_EMOJI[namoz]} <b>{namoz.upper()}</b> namozini o'qidingizmi?"
        try: await ctx.bot.send_message(chat_id=cid, text=txt, parse_mode='HTML', reply_markup=kb)
        except Exception as e: logger.error(f'prayer_question {cid}: {e}')

async def namoz_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    # Format: NAMOZ_OK_bomdod_01.05.2026_1938508551
    parts = q.data.split('_')
    if len(parts) < 5:
        return
    status_raw = parts[1]   # OK / NO
    namoz      = parts[2]   # bomdod, peshin, asr, shom, xufton
    sana       = parts[3]   # 01.05.2026
    cid        = parts[4]   # chat_id
    kim    = 'FERUDIN' if cid == CHAT_1 else 'GULOYIM'
    status = "O'QILDI" if status_raw == 'OK' else "O'QILMADI"
    icon   = '✅' if status_raw == 'OK' else '❌'
    await save_namoz_response(sana, namoz, kim, status)
    await q.edit_message_text(
        f'{icon} <b>{namoz.upper()}</b> — {status}\n📅 {sana} | 👤 {kim}',
        parse_mode='HTML')

async def schedule_todays_prayers(app_obj, date_obj=None):
    if date_obj is None:
        date_obj = datetime.now(TZ).date()
    date_str_api = date_obj.strftime('%d-%m-%Y')
    sana = date_obj.strftime('%d.%m.%Y')
    times = await get_prayer_times(date_str_api)
    if not times:
        logger.error('Namoz vaqtlari olinmadi — API javob bermadi')
        return
    now = datetime.now(TZ)
    for namoz, vaqt_str in times.items():
        prayer_dt   = _parse_prayer_dt(vaqt_str, date_obj)
        reminder_dt = prayer_dt - timedelta(minutes=20)
        question_dt = prayer_dt + timedelta(minutes=15)
        job_data = {'namoz': namoz, 'vaqt': vaqt_str, 'sana': sana}
        if reminder_dt > now:
            app_obj.job_queue.run_once(
                prayer_reminder_job, when=reminder_dt,
                data=job_data, name=f'reminder_{namoz}_{sana}')
        if prayer_dt > now:
            app_obj.job_queue.run_once(
                prayer_time_job, when=prayer_dt,
                data=job_data, name=f'prayer_{namoz}_{sana}')
        if question_dt > now:
            app_obj.job_queue.run_once(
                prayer_question_job, when=question_dt,
                data=job_data, name=f'question_{namoz}_{sana}')
    logger.info(f'{sana} namoz vaqtlari scheduled: {times}')

async def daily_prayer_scheduler(ctx: ContextTypes.DEFAULT_TYPE):
    tomorrow = (datetime.now(TZ) + timedelta(days=1)).date()
    await schedule_todays_prayers(ctx.application, tomorrow)

async def namoz_weekly_stats(ctx: ContextTypes.DEFAULT_TYPE):
    try:
        def _get_stats():
            ws      = get_ss().worksheet('NAMOZ')
            vals    = ws.get_all_values()
            today   = datetime.now(TZ).date()
            week_ago = today - timedelta(days=7)
            stats = {
                'FERUDIN': {n: {'ok': 0, 'no': 0} for n in NAMOZ_UZ},
                'GULOYIM': {n: {'ok': 0, 'no': 0} for n in NAMOZ_UZ},
            }
            for row in vals[1:]:
                if len(row) < 7: continue
                try: row_date = datetime.strptime(row[0], '%d.%m.%Y').date()
                except: continue
                if row_date < week_ago or row_date > today: continue
                kim = row[6]
                if kim not in stats: continue
                for i, namoz in enumerate(NAMOZ_UZ):
                    val = row[i + 1] if i + 1 < len(row) else ''
                    if val == "O'QILDI":     stats[kim][namoz]['ok'] += 1
                    elif val == "O'QILMADI": stats[kim][namoz]['no'] += 1
            return stats

        stats = await asyncio.to_thread(_get_stats)
        txt = '📊 <b>HAFTALIK NAMOZ STATISTIKASI</b>\n\n'
        for kim, data in stats.items():
            total_ok = sum(v['ok'] for v in data.values())
            total_no = sum(v['no'] for v in data.values())
            txt += f'👤 <b>{kim}</b>\n'
            for namoz, v in data.items():
                txt += f'  {NAMOZ_EMOJI[namoz]} {namoz}: ✅{v["ok"]} ❌{v["no"]}\n'
            txt += f'  📊 Jami: ✅{total_ok}/35 | Qazo: ❌{total_no}\n\n'
        for cid in [CHAT_1, CHAT_2]:
            try: await ctx.bot.send_message(chat_id=cid, text=txt, parse_mode='HTML')
            except Exception as e: logger.error(f'weekly_stats {cid}: {e}')
    except Exception as e:
        logger.error(f'namoz_weekly_stats: {e}')

# ══════════════════════════════════════════════════════════
# KUNLIK HISOBOT
# ══════════════════════════════════════════════════════════
async def daily_report(ctx: ContextTypes.DEFAULT_TYPE):
    try:
        dv  = get_bugun()
        bal = get_balance()
        txt = f'📊 <b>{today_str()} — Kunlik hisobot</b>\n\n<b>📤 Chiqimlar:</b>\n'
        txt += ('\n'.join(f'  • {c["tur"]}: {sstr(c["usd"],c["uzs"])}' for c in dv['ch'])) or "  Yo'q"
        txt += '\n\n<b>📥 Kirimlar:</b>\n'
        txt += ('\n'.join(f'  • {k["tur"]}: {sstr(k["usd"],k["uzs"])}' for k in dv['ki'])) or "  Yo'q"
        txt += (f'\n\n▪️ Jami chiqim: <b>{sstr(dv["chU"],dv["chZ"])}</b>'
                f'\n▪️ Jami kirim:  <b>{sstr(dv["kiU"],dv["kiZ"])}</b>'
                f'\n\n💰 <b>BALANCE: {round(bal,2)}$</b>')
        for cid in [CHAT_1, CHAT_2]:
            try: await ctx.bot.send_message(chat_id=cid, text=txt, parse_mode='HTML')
            except Exception as e: logger.error(f'daily {cid}: {e}')
    except Exception as e:
        logger.error(f'daily_report: {e}')

# ══════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════
def main():
    app = (Application.builder().token(TOKEN)
           .read_timeout(30).write_timeout(30).connect_timeout(30).build())

    # Startup: kategoriyalar va QARZ varag'ini yuklash
    async def on_startup(application):
        await load_categories()
        await ensure_qarz()
        await ensure_namoz_sheet()
        await ensure_tasks_sheet()
        await ensure_memory_sheet()
        await schedule_todays_prayers(application)
        await reschedule_pending_tasks(application)
        logger.info('Startup: barcha tizimlar yuklandi (QARZ, NAMOZ, TASKS, MEMORY)')
    app.post_init = on_startup

    # ── Hisobot conversation ─────────────────────────────
    hisobot_conv = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(hisobot_start, pattern='^MH$'),
            CommandHandler('hisobot', hisobot_start_cmd),
        ],
        states={
            H_TIP:       [CallbackQueryHandler(hisobot_tip)],
            H_DAVR:      [CallbackQueryHandler(hisobot_davr)],
            H_DATE_FROM: [MessageHandler(filters.TEXT & ~filters.COMMAND, hisobot_date_from)],
            H_DATE_TO:   [MessageHandler(filters.TEXT & ~filters.COMMAND, hisobot_date_to)],
            H_TUR:       [CallbackQueryHandler(hisobot_tur)],
        },
        fallbacks=[CommandHandler('start', start)],
        per_message=False
    )

    # ── Asosiy kirim/chiqim conversation ────────────────
    main_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(btn)],
        states={
            TUR:     [CallbackQueryHandler(btn)],
            EGASI:   [CallbackQueryHandler(btn)],
            TOLOV:   [CallbackQueryHandler(btn)],
            VALYUTA: [CallbackQueryHandler(btn)],
            SUMMA:   [MessageHandler(filters.TEXT & ~filters.COMMAND, get_summa),
                      CallbackQueryHandler(btn)],
            NOTE:    [MessageHandler(filters.TEXT & ~filters.COMMAND, get_note),
                      CallbackQueryHandler(btn)],
        },
        fallbacks=[CommandHandler('start', start)],
        per_message=False
    )

    # ── Handlers ro'yxatga olish ─────────────────────────
    app.add_handler(CommandHandler('start',  start))
    app.add_handler(CommandHandler('menu',   start))
    app.add_handler(CommandHandler('debug',  debug_cmd))
    app.add_handler(CommandHandler('qarz',   qarz_cmd))
    app.add_handler(CommandHandler('admin',  admin_cmd))
    app.add_handler(CommandHandler('tasks',  tasks_cmd))
    app.add_handler(CommandHandler('memory', memory_cmd))
    app.add_handler(CommandHandler('namoz',  namoz_cmd))
    app.add_handler(CommandHandler('help',   help_cmd))

    # AI handlers (photo + voice) — ConversationHandler dan OLDIN
    app.add_handler(MessageHandler(
        filters.PHOTO & filters.Chat(chat_id=[int(CHAT_1), int(CHAT_2)]),
        handle_photo
    ))
    app.add_handler(MessageHandler(
        (filters.VOICE | filters.AUDIO) & filters.Chat(chat_id=[int(CHAT_1), int(CHAT_2)]),
        handle_voice
    ))

    # AI callback — alohida, ConversationHandler dan tashqarida
    app.add_handler(CallbackQueryHandler(ai_callback,     pattern='^AI_'))
    app.add_handler(CallbackQueryHandler(qarz_callback,   pattern='^QARZ_'))
    app.add_handler(CallbackQueryHandler(admin_callback,  pattern='^(ADMIN_|ADM_)'))
    app.add_handler(CallbackQueryHandler(namoz_callback,  pattern='^NAMOZ_'))
    app.add_handler(CallbackQueryHandler(tasks_callback,  pattern='^TASK_'))

    # Conversation handlers
    app.add_handler(hisobot_conv)
    app.add_handler(main_conv)

    # Outer text handler — QARZ/AI edit/Admin uchun (conversation faol bo'lmaganda)
    app.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND & filters.Chat(chat_id=[int(CHAT_1), int(CHAT_2)]),
        outer_text_handler
    ))

    # Jobs
    app.job_queue.run_daily(daily_report,
        time=dtime(hour=18, minute=50, tzinfo=pytz.utc))   # 23:50 Tashkent
    app.job_queue.run_daily(qarz_notify_job,
        time=dtime(hour=4, minute=0, tzinfo=pytz.utc))     # 09:00 Tashkent
    # Namoz: har kuni yarim tunda ertangi vaqtlarni olish
    app.job_queue.run_daily(daily_prayer_scheduler,
        time=dtime(hour=19, minute=1, tzinfo=pytz.utc))    # 00:01 Tashkent
    # Haftalik namoz statistikasi — har dushanba 09:05 Tashkent
    app.job_queue.run_daily(namoz_weekly_stats,
        time=dtime(hour=4, minute=5, tzinfo=pytz.utc),     # 09:05 Tashkent
        days=(0,))  # PTB: 0 = dushanba

    logger.info('Bot ishga tushdi!')
    app.run_polling(drop_pending_updates=True)

# ══════════════════════════════════════════════════════════
# FASTAPI — REST API
# ══════════════════════════════════════════════════════════
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional

api = FastAPI(title='Family Accounting API')
api.add_middleware(CORSMiddleware, allow_origins=['*'], allow_methods=['*'], allow_headers=['*'])

class Transaction(BaseModel):
    type:    str
    sana:    str
    egasi:   str
    tur:     str
    tolov:   str
    valyuta: str
    summa:   float
    note:    str = ''

class UpdateTransaction(BaseModel):
    egasi:   Optional[str]   = None
    tur:     Optional[str]   = None
    tolov:   Optional[str]   = None
    valyuta: Optional[str]   = None
    summa:   Optional[float] = None
    note:    Optional[str]   = None
    sana:    Optional[str]   = None   # ← YANGI: sana ham tahrirlash mumkin

def read_sheet(sheet_name: str):
    sh   = get_ss().worksheet(sheet_name)
    rows = sh.get_all_values()
    result = []
    for i, row in enumerate(rows[2:], start=3):
        if len(row) < 5 or not row[2] or not row[4]: continue
        usd = num_clean(row[6]) if len(row) > 6 and row[6] else 0.0
        uzs = num_clean(row[7]) if len(row) > 7 and row[7] else 0.0
        result.append({
            'row':   i, 'type':  sheet_name,
            'sana':  norm_date(row[2]),
            'egasi': row[3] if len(row)>3 else '',
            'tur':   row[4] if len(row)>4 else '',
            'tolov': row[5] if len(row)>5 else '',
            'usd':   usd, 'uzs': uzs,
            'vaqt':  row[8] if len(row)>8 and row[8] else '',
            'note':  row[9] if len(row)>9 else '',
        })
    return result

def find_next_row(sh):
    col_c = sh.col_values(3)
    last  = 2
    for i, v in enumerate(col_c):
        if i < 2: continue
        if v and str(v).strip(): last = i + 1
    return last + 1

@api.get('/')
def root(): return {'status':'ok','message':'Family Accounting API'}

@api.get('/balance')
def balance_endpoint():
    try:
        ss = get_ss()
        for sheet, cell in [('KUNLIK_VIEW','E2'),('DASHBOARD','B2')]:
            try:
                raw = ss.worksheet(sheet).acell(cell).value
                if not raw: continue
                v = num_clean(raw)
                if v > 0: return {'balance':round(v,2),'formatted':f'{round(v,2)}$'}
            except: continue
        return {'balance':0,'formatted':'0$'}
    except Exception as e: raise HTTPException(500, str(e))

@api.get('/today')
def get_today_api():
    try:
        today  = today_str()
        ss     = get_ss()
        result = {'date':today,'chiqimlar':[],'kirimlar':[],
                  'total_ch_usd':0.0,'total_ch_uzs':0.0,
                  'total_ki_usd':0.0,'total_ki_uzs':0.0}
        for sname, key in [('CHIQIM','chiqimlar'),('KIRIM','kirimlar')]:
            sh=ss.worksheet(sname); dates=sh.col_values(3); turs=sh.col_values(5)
            egasi=sh.col_values(4); tolov=sh.col_values(6)
            usds=sh.col_values(7); uzss=sh.col_values(8); notes=sh.col_values(10)
            n=max(len(dates),len(turs))
            for i in range(2,n):
                d=str(dates[i]).strip() if i<len(dates) else ''
                if not d or norm_date(d)!=today: continue
                tur=str(turs[i]).strip() if i<len(turs) else ''
                if not tur: continue
                u=num_clean(usds[i] if i<len(usds) else '')
                z=num_clean(uzss[i] if i<len(uzss) else '')
                result[key].append({'row':i+1,'tur':tur,
                    'egasi':str(egasi[i]).strip() if i<len(egasi) else '',
                    'tolov':str(tolov[i]).strip() if i<len(tolov) else '',
                    'usd':u,'uzs':z,'note':str(notes[i]).strip() if i<len(notes) else ''})
                if key=='chiqimlar': result['total_ch_usd']+=u; result['total_ch_uzs']+=z
                else: result['total_ki_usd']+=u; result['total_ki_uzs']+=z
        return result
    except Exception as e: raise HTTPException(500, str(e))

@api.get('/by-date')
def get_by_date(date: str = Query(...)):
    try:
        ss=get_ss(); result={'date':date,'chiqimlar':[],'kirimlar':[]}
        for sname,key in [('CHIQIM','chiqimlar'),('KIRIM','kirimlar')]:
            sh=ss.worksheet(sname); dates=sh.col_values(3); turs=sh.col_values(5)
            egasi=sh.col_values(4); tolov=sh.col_values(6)
            usds=sh.col_values(7); uzss=sh.col_values(8); notes=sh.col_values(10)
            n=max(len(dates),len(turs))
            for i in range(2,n):
                d=str(dates[i]).strip() if i<len(dates) else ''
                if not d or norm_date(d)!=date: continue
                tur=str(turs[i]).strip() if i<len(turs) else ''
                if not tur: continue
                u=num_clean(usds[i] if i<len(usds) else '')
                z=num_clean(uzss[i] if i<len(uzss) else '')
                result[key].append({'row':i+1,'tur':tur,
                    'egasi':str(egasi[i]).strip() if i<len(egasi) else '',
                    'tolov':str(tolov[i]).strip() if i<len(tolov) else '',
                    'usd':u,'uzs':z,'note':str(notes[i]).strip() if i<len(notes) else ''})
        return result
    except Exception as e: raise HTTPException(500, str(e))

@api.get('/by-filter')
def get_by_filter(
    tip:str=Query('CHIQIM'),davr:str=Query('bu_oy'),
    tur:str=Query('BARCHASI'),date_from:str=Query(None),date_to:str=Query(None)
):
    try:
        rows,total_usd,total_uzs=get_filtered(tip,davr,tur,date_from,date_to)
        return {'rows':rows,'total_usd':round(total_usd,2),'total_uzs':round(total_uzs,2),'count':len(rows)}
    except Exception as e: raise HTTPException(500, str(e))

@api.get('/history')
def get_history(limit:int=100):
    try:
        all_tx=read_sheet('CHIQIM')+read_sheet('KIRIM')
        all_tx.sort(key=lambda x:datetime.strptime(x['sana'],'%d.%m.%Y') if x['sana'] else datetime.min, reverse=True)
        return {'transactions':all_tx[:limit],'total':len(all_tx)}
    except Exception as e: raise HTTPException(500, str(e))

@api.get('/stats')
def get_stats():
    try:
        ch=read_sheet('CHIQIM'); ki=read_sheet('KIRIM')
        ch_by={}; ki_by={}
        for t in ch:
            v=t['usd']+(t['uzs']/12000 if t['uzs'] else 0)
            ch_by[t['tur']]=ch_by.get(t['tur'],0)+v
        for t in ki:
            v=t['usd']+(t['uzs']/12000 if t['uzs'] else 0)
            ki_by[t['tur']]=ki_by.get(t['tur'],0)+v
        chs=sorted(ch_by.items(),key=lambda x:x[1],reverse=True)
        kis=sorted(ki_by.items(),key=lambda x:x[1],reverse=True)
        return {
            'chiqim':{'by_tur':chs,'top':chs[0] if chs else None,'bottom':chs[-1] if chs else None,'total_usd':round(sum(ch_by.values()),2),'count':len(ch)},
            'kirim': {'by_tur':kis,'top':kis[0] if kis else None,'bottom':kis[-1] if kis else None,'total_usd':round(sum(ki_by.values()),2),'count':len(ki)},
            'net':round(sum(ki_by.values())-sum(ch_by.values()),2),
        }
    except Exception as e: raise HTTPException(500, str(e))

@api.post('/transaction')
def add_transaction(tx: Transaction):
    try:
        ss=get_ss(); sh=ss.worksheet(tx.type)
        nr=find_next_row(sh)
        usd   = tx.summa if tx.valyuta=='USD' else ''
        uzs   = tx.summa if tx.valyuta=='UZS' else ''
        now_t = datetime.now(TZ).strftime('%H:%M')
        sh.update(f'B{nr}:I{nr}',[[nr-2,tx.sana,tx.egasi,tx.tur,tx.tolov,usd,uzs,now_t]])
        sh.update(f'J{nr}',[[tx.note or '']])
        return {'success':True,'row':nr,'message':'Saqlandi'}
    except Exception as e: raise HTTPException(500, str(e))

@api.put('/transaction/{sheet}/{row}')
def update_transaction(sheet:str, row:int, data:UpdateTransaction):
    try:
        if sheet not in ['CHIQIM','KIRIM']: raise HTTPException(400,"Sheet noto'g'ri")
        ss=get_ss(); sh=ss.worksheet(sheet)
        if data.sana:   sh.update(f'C{row}',[[data.sana]])   # ← YANGI
        if data.egasi:  sh.update(f'D{row}',[[data.egasi]])
        if data.tur:    sh.update(f'E{row}',[[data.tur]])
        if data.tolov:  sh.update(f'F{row}',[[data.tolov]])
        if data.summa is not None and data.summa > 0:
            if data.valyuta=='USD':
                sh.update(f'G{row}',[[data.summa]]); sh.update(f'H{row}',[['']])
            else:
                sh.update(f'H{row}',[[data.summa]]); sh.update(f'G{row}',[['']])
        if data.note is not None: sh.update(f'J{row}',[[data.note]])
        return {'success':True,'message':'Yangilandi'}
    except Exception as e: raise HTTPException(500, str(e))

# ── QARZ API ────────────────────────────────────────────
class QarzModel(BaseModel):
    tur:       str
    kim:       str
    summa_uzs: Optional[float] = None
    summa_usd: Optional[float] = None
    muddat:    str
    sana:      Optional[str]   = None
    note:      Optional[str]   = ''

@api.get('/qarz/list')
def qarz_list_api():
    try:
        ss = get_ss()
        try: ws = ss.worksheet('QARZ')
        except: return {'success':True,'data':[]}
        rows = ws.get_all_values()
        return {'success':True,'data':qarz_to_list(rows)}
    except Exception as e: raise HTTPException(500, str(e))

@api.post('/qarz/add')
def qarz_add_api(q: QarzModel):
    try:
        ss  = get_ss()
        try: ws = ss.worksheet('QARZ')
        except:
            ws = ss.add_worksheet(title='QARZ', rows=500, cols=12)
            ws.update('A1:J1',[['raqam','tur','kim','summa_uzs','summa_usd','sana','muddat','holat','qaytarilgan_sana','note']])
        rows = ws.get_all_values()
        num  = len(rows)
        from datetime import datetime as dt2
        today = dt2.now(TZ).strftime('%d.%m.%Y')
        ws.append_row([num,q.tur,q.kim,q.summa_uzs or '',q.summa_usd or '',
            q.sana or today,q.muddat,'AKTIV','',q.note or ''],
            value_input_option='USER_ENTERED')
        return {'success':True}
    except Exception as e: raise HTTPException(500, str(e))

@api.post('/qarz/close/{row_index}')
def qarz_close_api(row_index: int):
    try:
        ss    = get_ss()
        ws    = ss.worksheet('QARZ')
        today = datetime.now(TZ).strftime('%d.%m.%Y')

        # Qarz ma'lumotlarini olish
        row       = ws.row_values(row_index)
        tur       = row[1] if len(row) > 1 else 'BERILGAN'
        kim       = row[2] if len(row) > 2 else '?'
        summa_uzs = row[3] if len(row) > 3 and row[3] else ''
        summa_usd = row[4] if len(row) > 4 and row[4] else ''

        # QARZ ni TUGADI deb belgilash
        ws.update_cell(row_index, 8, 'TUGADI')
        ws.update_cell(row_index, 9, today)

        egasi = 'FERUDIN'
        if tur == 'BERILGAN':
            # Bergan qarzim qaytdi → KIRIM (balans +)
            qarz_to_sheet('KIRIM', egasi, summa_usd, summa_uzs,
                f'Qarz qaytdi: {kim}')
            effect = 'balance_plus'
        else:
            # Olgan qarzimni qaytardim → CHIQIM (balans −)
            qarz_to_sheet('CHIQIM', egasi, summa_usd, summa_uzs,
                f'Qarz qaytarildi: {kim}')
            effect = 'balance_minus'

        bal = get_balance()
        return {'success': True, 'effect': effect, 'balance': round(bal, 2), 'kim': kim}
    except Exception as e:
        raise HTTPException(500, str(e))

# ── TASKS API ────────────────────────────────────────────
class TaskModel(BaseModel):
    matn:    str
    vaqt:    str
    egasi:   str = 'FERUDIN'
    chat_id: str = ''

@api.get('/tasks')
def get_tasks_api(status: str = 'FAOL'):
    try:
        ws   = get_ss().worksheet('TASKS')
        vals = ws.get_all_values()
        now  = datetime.now(TZ)
        result = []
        for i, row in enumerate(vals[1:], start=2):
            if len(row) < 6: continue
            if status != 'ALL' and row[5] != status: continue
            overdue = False
            try:
                dt = TZ.localize(datetime.strptime(row[2], '%d.%m.%Y %H:%M'))
                overdue = dt < now and row[5] == 'FAOL'
            except: pass
            result.append({'row':i,'id':row[0],'yaratilgan':row[1],'vaqt':row[2],
                           'matn':row[3],'egasi':row[4],'holat':row[5],'overdue':overdue})
        result.sort(key=lambda x: x['vaqt'])
        return {'tasks':result,'count':len(result)}
    except Exception as e: raise HTTPException(500, str(e))

@api.post('/tasks')
def add_task_api(task: TaskModel):
    try:
        ws    = get_ss().worksheet('TASKS')
        vals  = ws.get_all_values()
        today = datetime.now(TZ).strftime('%d.%m.%Y')
        ws.append_row([len(vals),today,task.vaqt,task.matn,task.egasi,'FAOL',task.chat_id],
                      value_input_option='USER_ENTERED')
        return {'success':True}
    except Exception as e: raise HTTPException(500, str(e))

@api.post('/tasks/done/{row}')
def task_done_api(row: int):
    try:
        get_ss().worksheet('TASKS').update_cell(row, 6, 'BAJARILDI')
        return {'success':True}
    except Exception as e: raise HTTPException(500, str(e))

# ── MEMORY API ───────────────────────────────────────────
class MemoryModel(BaseModel):
    kalit:  str
    qiymat: str
    kim:    str = 'FERUDIN'

@api.get('/memory')
def get_memory_api(q: str = ''):
    try:
        ws   = get_ss().worksheet('MEMORY')
        vals = ws.get_all_values()
        ql   = q.lower()
        result = []
        for row in vals[1:]:
            if len(row) < 4: continue
            if not q or ql in row[2].lower() or ql in row[3].lower():
                result.append({'kalit':row[2],'qiymat':row[3],
                               'kim':row[4] if len(row)>4 else '','sana':row[1]})
        return {'memories':result,'count':len(result)}
    except Exception as e: raise HTTPException(500, str(e))

@api.post('/memory')
def save_memory_api(mem: MemoryModel):
    try:
        ws    = get_ss().worksheet('MEMORY')
        vals  = ws.get_all_values()
        today = datetime.now(TZ).strftime('%d.%m.%Y')
        for i, row in enumerate(vals[1:], start=2):
            if len(row) >= 3 and row[2].lower() == mem.kalit.lower():
                ws.update(f'B{i}:E{i}', [[today, mem.kalit, mem.qiymat, mem.kim]])
                return {'success':True,'action':'updated'}
        ws.append_row([len(vals), today, mem.kalit, mem.qiymat, mem.kim])
        return {'success':True,'action':'created'}
    except Exception as e: raise HTTPException(500, str(e))

# ── NAMOZ API ────────────────────────────────────────────
@api.get('/namoz/stats')
def namoz_stats_api():
    try:
        ws      = get_ss().worksheet('NAMOZ')
        vals    = ws.get_all_values()
        today   = datetime.now(TZ).date()
        wk_ago  = today - timedelta(days=7)
        nl      = ['bomdod','peshin','asr','shom','xufton']
        empty   = lambda: {n:{'ok':0,'no':0} for n in nl}
        stats   = {'FERUDIN':empty(),'GULOYIM':empty()}
        for row in vals[1:]:
            if len(row) < 7: continue
            try: rd = datetime.strptime(row[0],'%d.%m.%Y').date()
            except: continue
            if rd < wk_ago or rd > today: continue
            kim = row[6]
            if kim not in stats: continue
            for i,n in enumerate(nl):
                v = row[i+1] if i+1 < len(row) else ''
                if v == "O'QILDI":     stats[kim][n]['ok'] += 1
                elif v == "O'QILMADI": stats[kim][n]['no'] += 1
        return {'stats':stats}
    except Exception as e: raise HTTPException(500, str(e))

# ── KATEGORIYALAR API ────────────────────────────────────
@api.get('/categories')
def categories_api():
    return {'chiqim':get_chiqim_turs(),'kirim':get_kirim_turs()}

@api.post('/categories')
async def save_categories_api(payload: dict):
    if 'chiqim' in payload: _cats['chiqim'] = payload['chiqim']
    if 'kirim'  in payload: _cats['kirim']  = payload['kirim']
    saved = await save_categories()
    return {'success':saved}

# ── RUN ─────────────────────────────────────────────────
def run_api():
    import asyncio, uvicorn
    loop   = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    port   = int(os.environ.get('PORT', 8080))
    config = uvicorn.Config(api, host='0.0.0.0', port=port, loop='none')
    server = uvicorn.Server(config)
    loop.run_until_complete(server.serve())

if __name__ == '__main__':
    import threading
    logger.info(f'Starting API on port {os.environ.get("PORT", 8080)}')
    threading.Thread(target=run_api, daemon=True).start()
    main()
