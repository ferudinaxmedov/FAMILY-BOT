#!/usr/bin/env python3
"""
Family Accounting Bot — Kengaytirilgan versiya
Yangi: AI (rasm+ovoz), QARZ tizimi, Admin panel (kategoriyalar)
"""
import os, json, logging, base64, asyncio, re
from datetime import datetime, timedelta, time as dtime, date
from io import BytesIO
import pytz
import gspread
from google.oauth2.service_account import Credentials
import store
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup
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
PAX_UZBEK_DATE, PAX_APT_DATE, PAX_DEP_DATE, PAX_APT_NUM, PAX_PAYMENT, PAX_CONFIRM = range(11, 17)

# ── PENDING AI DATA (memory) ─────────────────────────────
pending_ai: dict = {}   # {user_id: {op_type, data, source}}
_pax_data: dict  = {}   # {user_id: passport registration dict}

# ── KATEGORIYALAR KESHI ──────────────────────────────────
_cats: dict = {'chiqim': None, 'kirim': None}

DEFAULT_CHIQIM = [
    'OZIQ OVQAT','BENZIN','RASSROCHKA','KIYIM KECHAK',
    'XURSHIDGA','ISHXONAMGA','UYDAGILARGA','SHTRAFLAR',
    'SHOPPPING','ISHXONA REG','SARTAROSH','BOSHQA',
    'QARZ BERILDI','QARZ QAYTARILDI'
]
DEFAULT_KIRIM = ['ISHXONA','SEEDBEE','BUSINESS','UYDAGILAR','BOSHQA',
                 'QARZ OLINDI','QARZ QAYTIB KELDI']

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

async def _mirror(label, fn, *args, **kwargs):
    """Sheets-ga fon rejimida (best-effort) yozish — xato bo'lsa faqat log,
    bot hech qachon Sheets tufayli to'xtamaydi."""
    try:
        await asyncio.to_thread(fn, *args, **kwargs)
    except Exception as e:
        logger.error(f'[sheets-mirror] {label}: {e}')

def _mirror_task(label, fn, *args, **kwargs):
    asyncio.create_task(_mirror(label, fn, *args, **kwargs))

# Supabase — ASOSIY o'qish/yozish manbai. Sheets — har yozuvdan keyin
# fon vazifasi orqali doimiy ko'chiriladigan zaxira nusxa.
async def get_balance():
    """Returns (balance_usd, balance_uzs) — KIRIM minus CHIQIM, valyuta bo'yicha."""
    try:
        return await store.get_balance()
    except Exception as e:
        logger.error(f'get_balance: {e}')
        return 0.0, 0.0

def _sheets_save_row(sheet_name, st, today, now_t):
    sh      = get_ss().worksheet(sheet_name)
    usd_val = float(st['summa']) if st['valyuta'] == 'USD' else ''
    uzs_val = float(st['summa']) if st['valyuta'] == 'UZS' else ''
    col_c   = sh.col_values(3)
    last    = 2
    for i, v in enumerate(col_c):
        if i < 2: continue
        if v and str(v).strip(): last = i + 1
    new_row = last + 1
    sh.update(f'B{new_row}:I{new_row}', [[
        new_row-2, today, st['egasi'], st['tur'],
        st['tolov'], usd_val, uzs_val, now_t
    ]], value_input_option='USER_ENTERED')
    sh.update(f'J{new_row}', [[st.get('note','')]])
    logger.info(f'[sheets-mirror] {sheet_name} row {new_row} saqlandi')

async def save_row(sheet_name, st):
    now_dt  = datetime.now(TZ)
    today   = now_dt.strftime('%d.%m.%Y')
    now_t   = now_dt.strftime('%H:%M')
    usd_val = st['summa'] if st['valyuta'] == 'USD' else 0
    uzs_val = st['summa'] if st['valyuta'] == 'UZS' else 0
    new_id  = await store.add_transaction(
        sheet_name, today, st['egasi'], st['tur'], st['tolov'],
        usd_val, uzs_val, now_t, st.get('note', ''))
    _mirror_task(f'save_row:{sheet_name}', _sheets_save_row, sheet_name, st, today, now_t)
    return new_id

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
    bal_str = sstr(*bal) if bal is not None else '—'
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

async def get_bugun():
    try:
        return await store.get_bugun(today_str())
    except Exception as e:
        logger.error(f'get_bugun: {e}')
        return dict(ch=[], ki=[], chU=0.0, chZ=0.0, kiU=0.0, kiZ=0.0)

async def get_filtered(tip, davr, tur, date_from=None, date_to=None):
    try:
        return await store.get_filtered(tip, davr, tur, date_from, date_to, now=datetime.now(TZ))
    except Exception as e:
        logger.error(f'get_filtered: {e}')
        return [], 0.0, 0.0

async def delete_messages(bot, chat_id, msg_ids):
    for mid in msg_ids:
        try: await bot.delete_message(chat_id=chat_id, message_id=mid)
        except: pass

# ══════════════════════════════════════════════════════════
# KATEGORIYALAR — SETTINGS VARAQI
# ══════════════════════════════════════════════════════════
def _sheets_save_categories(chiqim, kirim):
    sh = get_ss()
    try: ws = sh.worksheet('SETTINGS')
    except Exception:
        ws = sh.add_worksheet(title='SETTINGS', rows=100, cols=10)
        ws.update('A1', [['kalit','qiymat']])
    ws.update('A2', [['chiqim_turs', json.dumps(chiqim)]])
    ws.update('A3', [['kirim_turs',  json.dumps(kirim)]])

async def load_categories():
    """Supabase'dan (asosiy) kategoriyalarni yuklash; bo'sh bo'lsa default."""
    try:
        chiqim, kirim = await store.load_categories()
        _cats['chiqim'] = chiqim or DEFAULT_CHIQIM[:]
        _cats['kirim']  = kirim  or DEFAULT_KIRIM[:]
    except Exception as e:
        logger.error(f'load_categories: {e}')
        _cats['chiqim'] = _cats['chiqim'] or DEFAULT_CHIQIM[:]
        _cats['kirim']  = _cats['kirim']  or DEFAULT_KIRIM[:]

async def save_categories():
    try:
        chiqim = _cats.get('chiqim', DEFAULT_CHIQIM)
        kirim  = _cats.get('kirim',  DEFAULT_KIRIM)
        await store.save_categories(chiqim, kirim)
        _mirror_task('save_categories', _sheets_save_categories, chiqim, kirim)
        return True
    except Exception as e:
        logger.error(f'save_categories: {e}')
        return False

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

def kb_reply_main():
    return ReplyKeyboardMarkup([
        ['📤 Chiqim',     '📥 Kirim'],
        ['💰 Balans',     '📅 Bugun'],
        ['📊 Statistika', '🔍 Hisobot'],
        ['💳 Qarz',       '✅ Tasklar'],
        ['🧠 Xotira',     '🕌 Namoz'],
        ['⚙️ Admin',      '❓ Yordam'],
    ], resize_keyboard=True, is_persistent=True)

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
        '👋 <b>FAMILY ACCOUNTING</b>\n\nTugmani bosing 👇',
        parse_mode='HTML', reply_markup=kb_reply_main()
    )
    return ConversationHandler.END

async def handle_reply_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Reply keyboard: Chiqim / Kirim tugmalari → conversation boshlaydi"""
    if not ok(update): return ConversationHandler.END
    text = update.message.text.strip()
    ud   = ctx.user_data
    ud.clear()
    if 'Chiqim' in text:
        ud['type'] = 'CHIQIM'; ud['msgs'] = []
        m = await update.message.reply_text(
            '📤 <b>CHIQIM</b>\n\nXarajat turini tanlang:',
            parse_mode='HTML', reply_markup=kb_chiqim())
        ud['msgs'].append(m.message_id)
        return TUR
    ud['type'] = 'KIRIM'; ud['msgs'] = []
    m = await update.message.reply_text(
        '📥 <b>KIRIM</b>\n\nKirim turini tanlang:',
        parse_mode='HTML', reply_markup=kb_kirim())
    ud['msgs'].append(m.message_id)
    return TUR

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
        await q.message.reply_text('✅ Bekor qilindi.', reply_markup=kb_reply_main())
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
        rows, total_usd, total_uzs = await get_filtered(tip, davr, tur, df, dt)
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
        await q.message.reply_text(txt, parse_mode='HTML')
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
        await q.message.reply_text('✅ Bekor qilindi.', reply_markup=kb_reply_main())
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
        bal = await get_balance()
        await q.message.reply_text(f'💰 <b>Joriy balans: {sstr(*bal)}</b>', parse_mode='HTML')
        return ConversationHandler.END
    if d == 'MG':
        await q.message.reply_text("⏳ Ma'lumotlar yuklanmoqda...")
        dv  = await get_bugun()
        txt = f'📅 <b>{today_str()}</b>\n\n<b>📤 Chiqimlar:</b>\n'
        txt += ('\n'.join(f'  • {c["tur"]}: {sstr(c["usd"],c["uzs"])}' for c in dv['ch'])) or "  Yo'q"
        txt += '\n\n<b>📥 Kirimlar:</b>\n'
        txt += ('\n'.join(f'  • {k["tur"]}: {sstr(k["usd"],k["uzs"])}' for k in dv['ki'])) or "  Yo'q"
        await q.message.reply_text(txt, parse_mode='HTML')
        return ConversationHandler.END
    if d == 'MS':
        await q.message.reply_text('⏳ Statistika yuklanmoqda...')
        dv  = await get_bugun()
        bal = await get_balance()
        txt = (f'📊 <b>Statistika</b>\n\n'
               f'💰 Balans: <b>{sstr(*bal)}</b>\n'
               f'Bugungi chiqim: <b>{sstr(dv["chU"],dv["chZ"])}</b>\n'
               f'Bugungi kirim:  <b>{sstr(dv["kiU"],dv["kiZ"])}</b>')
        await q.message.reply_text(txt, parse_mode='HTML')
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
            await message.reply_text('Qaytadan boshlang.', reply_markup=kb_reply_main())
            return
        m_wait = await message.reply_text('⏳ Saqlanmoqda...')
        await save_row(st['type'], st)
        bal  = await get_balance()
        txt  = confirm_text(st, bal)
        msgs = list(st.get('msgs',[]))
        msgs.append(m_wait.message_id)
        ctx.user_data.clear()
        await message.reply_text(txt, parse_mode='HTML', reply_markup=kb_reply_main())
        try: await delete_messages(ctx.application.bot, message.chat_id, msgs)
        except Exception as de: logger.error(f'delete msgs: {de}')
    except Exception as e:
        logger.error(f'finalize: {e}')
        ctx.user_data.clear()
        await message.reply_text(f'❌ Xato: {e}', reply_markup=kb_reply_main())

# ══════════════════════════════════════════════════════════
# OUTER TEXT HANDLER — AI edit / QARZ input / Admin input
# ══════════════════════════════════════════════════════════
async def outer_text_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """ConversationHandler faol bo'lmaganda ishga tushadi"""
    if not ok(update): return

    # Passport registration flow
    uid  = update.effective_user.id
    pax  = _pax_data.get(uid)
    if pax:
        state = pax.get('_state')
        if state == 'PAX_UZBEK_DATE':
            await pax_uzbek_date(update, ctx); return
        elif state == 'PAX_APT_DATE':
            await pax_apt_date(update, ctx); return
        elif state == 'PAX_DEP_DATE':
            await pax_dep_date(update, ctx); return
        elif state == 'PAX_PAYMENT':
            await pax_payment(update, ctx); return

    # Aktiv holatlar
    if ctx.user_data.get('ai_editing'):
        await ai_edit_text(update, ctx); return
    if ctx.user_data.get('qarz_new'):
        await qarz_input(update, ctx); return
    if ctx.user_data.get('admin_action'):
        await admin_text(update, ctx); return

    # Reply keyboard routing
    text = (update.message.text or '').strip()
    kb   = kb_reply_main()
    if text == '💰 Balans':
        bal = await get_balance()
        await update.message.reply_text(
            f'💰 <b>Joriy balans: {sstr(*bal)}</b>',
            parse_mode='HTML', reply_markup=kb)
    elif text == '📅 Bugun':
        await update.message.reply_text("⏳ Yuklanmoqda...", reply_markup=kb)
        dv  = await get_bugun()
        txt = f'📅 <b>{today_str()}</b>\n\n<b>📤 Chiqimlar:</b>\n'
        txt += ('\n'.join(f'  • {c["tur"]}: {sstr(c["usd"],c["uzs"])}' for c in dv['ch'])) or "  Yo'q"
        txt += '\n\n<b>📥 Kirimlar:</b>\n'
        txt += ('\n'.join(f'  • {k["tur"]}: {sstr(k["usd"],k["uzs"])}' for k in dv['ki'])) or "  Yo'q"
        await update.message.reply_text(txt, parse_mode='HTML', reply_markup=kb)
    elif text == '📊 Statistika':
        dv  = await get_bugun()
        bal = await get_balance()
        await update.message.reply_text(
            f'📊 <b>Statistika</b>\n\n'
            f'💰 Balans: <b>{sstr(*bal)}</b>\n'
            f'Bugungi chiqim: <b>{sstr(dv["chU"],dv["chZ"])}</b>\n'
            f'Bugungi kirim:  <b>{sstr(dv["kiU"],dv["kiZ"])}</b>',
            parse_mode='HTML', reply_markup=kb)
    elif text == '💳 Qarz':
        await qarz_cmd(update, ctx)
    elif text == '✅ Tasklar':
        await tasks_cmd(update, ctx)
    elif text == '🧠 Xotira':
        await memory_cmd(update, ctx)
    elif text == '🕌 Namoz':
        await namoz_cmd(update, ctx)
    elif text == '⚙️ Admin':
        await admin_cmd(update, ctx)
    elif text == '❓ Yordam':
        await help_cmd(update, ctx)
    else:
        await analyze_and_route(update, ctx)

# ══════════════════════════════════════════════════════════
# AI — RASM (PHOTO) HANDLER
# ══════════════════════════════════════════════════════════
async def handle_photo(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Rasm → klassifikatsiya → namoz jadvali yoki chek handler"""
    if not ok(update): return
    msg = await update.message.reply_text('📸 Rasm tahlil qilinmoqda...')
    try:
        if not ANTHROPIC_API_KEY:
            await msg.edit_text('❌ ANTHROPIC_API_KEY Railway Variables ga qo\'shilmagan.')
            return
        import anthropic
        photo      = update.message.photo[-1]
        tg_file    = await ctx.bot.get_file(photo.file_id)
        fb         = await tg_file.download_as_bytearray()
        image_b64  = base64.standard_b64encode(bytes(fb)).decode('utf-8')
        media_type = 'image/png' if bytes(fb)[:8]==b'\x89PNG\r\n\x1a\n' else 'image/jpeg'

        # Rasm turini aniqlash (haiku — tez, arzon)
        client  = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        cl_resp = await asyncio.to_thread(
            client.messages.create,
            model='claude-haiku-4-5-20251001',
            max_tokens=5,
            messages=[{
                'role': 'user',
                'content': [
                    {'type':'image','source':{'type':'base64','media_type':media_type,'data':image_b64}},
                    {'type':'text','text':'1=namoz vaqtlari jadvali (Bomdod/Peshin/Asr/Shom/Xufton ko\'rinadi), 2=pasport yoki ID hujjat (ism, tug\'ilgan sana, pasport raqami ko\'rinadi), 3=chek/kvitansiya/boshqa. Faqat bitta raqam.'}
                ]
            }]
        )
        rasm_tur = cl_resp.content[0].text.strip()[:1]

        if rasm_tur == '1':
            await handle_namoz_photo(update, ctx, msg, image_b64, media_type)
            return

        if rasm_tur == '2':
            await handle_passport_photo(update, ctx, msg, image_b64, media_type)
            return

        # Chek/kvitansiya — mavjud logic
        chiqim_list = ', '.join(get_chiqim_turs())
        today       = today_str()
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
# PASSPORT FOTO → ARIZA (BnB ro'yxatga olish)
# ══════════════════════════════════════════════════════════
APT_LIST = ['23', '28', '68', '80', '84', '88', '701']

async def handle_passport_photo(update: Update, ctx: ContextTypes.DEFAULT_TYPE,
                                 msg, image_b64: str, media_type: str):
    """Pasport rasmidan ma'lumot ajratib olish → ro'yxatga olish suhbatini boshlash"""
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        await msg.edit_text('🪪 Pasport tahlil qilinmoqda...')

        resp = await asyncio.to_thread(
            client.messages.create,
            model='claude-opus-4-5',
            max_tokens=400,
            messages=[{
                'role': 'user',
                'content': [
                    {'type': 'image', 'source': {'type': 'base64', 'media_type': media_type, 'data': image_b64}},
                    {'type': 'text', 'text': (
                        'Bu pasport yoki ID hujjat rasmi. '
                        'Faqat JSON qaytargin (hech qanday izoh yozma):\n'
                        '{"name":"<to\'liq ism familya>","nationality":"<fuqaroligi, masalan: Россия>","dob":"<YYYY-MM-DD>","passportId":"<pasport raqami>"}'
                    )}
                ]
            }]
        )
        raw  = resp.content[0].text.strip()
        raw  = re.sub(r'```(?:json)?\s*', '', raw).strip('`').strip()
        pax  = json.loads(raw)

        uid  = update.effective_user.id
        _pax_data[uid] = {
            'guests': [pax],
            'paymentBy': 'direct',
            '_state': 'PAX_UZBEK_DATE',
        }

        dob_disp = pax.get('dob', '?')
        text = (
            f"🪪 <b>Pasport ma'lumotlari:</b>\n\n"
            f"👤 <b>{pax.get('name', '?')}</b>\n"
            f"🌍 Fuqaroligi: {pax.get('nationality', '?')}\n"
            f"🎂 Tug'ilgan: {dob_disp}\n"
            f"📋 Pasport: {pax.get('passportId', '?')}\n\n"
            f"📅 <b>O'zbekistonga qachon kirdi?</b>\n"
            f"Format: <code>DD.MM.YYYY</code>"
        )
        await msg.edit_text(text, parse_mode='HTML')

    except (json.JSONDecodeError, KeyError):
        await msg.edit_text('❌ Pasportdan ma\'lumot ajratib bo\'lmadi. Rasm aniq ekanligini tekshiring.')
    except Exception as e:
        await msg.edit_text(f'❌ Xatolik: {str(e)[:120]}')
        logger.error(f'handle_passport_photo: {e}')


async def pax_uzbek_date(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    text = update.message.text.strip()
    if text.lower() in ('bekor', 'cancel', '/cancel'):
        _pax_data.pop(uid, None)
        await update.message.reply_text('❌ Ro\'yxatga olish bekor qilindi.')
        return
    try:
        datetime.strptime(text, '%d.%m.%Y')
    except ValueError:
        await update.message.reply_text('❌ Format noto\'g\'ri. Misol: <code>15.05.2025</code>', parse_mode='HTML')
        return
    _pax_data[uid]['uzbekEntryDate'] = datetime.strptime(text, '%d.%m.%Y').strftime('%Y-%m-%d')
    _pax_data[uid]['_state'] = 'PAX_APT_DATE'
    await update.message.reply_text('🏠 Apartamentga qachon kirdi?\nFormat: <code>DD.MM.YYYY</code>', parse_mode='HTML')


async def pax_apt_date(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    text = update.message.text.strip()
    if text.lower() in ('bekor', 'cancel', '/cancel'):
        _pax_data.pop(uid, None)
        await update.message.reply_text('❌ Ro\'yxatga olish bekor qilindi.')
        return
    try:
        datetime.strptime(text, '%d.%m.%Y')
    except ValueError:
        await update.message.reply_text('❌ Format noto\'g\'ri. Misol: <code>01.06.2025</code>', parse_mode='HTML')
        return
    _pax_data[uid]['aptEntryDate'] = datetime.strptime(text, '%d.%m.%Y').strftime('%Y-%m-%d')
    _pax_data[uid]['_state'] = 'PAX_DEP_DATE'
    await update.message.reply_text('🚀 Qachon ketadi?\nFormat: <code>DD.MM.YYYY</code>', parse_mode='HTML')


async def pax_dep_date(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    text = update.message.text.strip()
    if text.lower() in ('bekor', 'cancel', '/cancel'):
        _pax_data.pop(uid, None)
        await update.message.reply_text('❌ Ro\'yxatga olish bekor qilindi.')
        return
    try:
        dep = datetime.strptime(text, '%d.%m.%Y')
    except ValueError:
        await update.message.reply_text('❌ Format noto\'g\'ri. Misol: <code>10.06.2025</code>', parse_mode='HTML')
        return
    d    = _pax_data[uid]
    dep_iso = dep.strftime('%Y-%m-%d')
    apt_iso = d.get('aptEntryDate', dep_iso)
    apt_dt  = datetime.strptime(apt_iso, '%Y-%m-%d')
    d['departureDate'] = dep_iso
    d['regStartDate']  = apt_dt.strftime('%Y-%m-%d')
    d['regEndDate']    = (dep + timedelta(days=1)).strftime('%Y-%m-%d')
    d['_state']        = 'PAX_APT_NUM'

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(f'#{a}', callback_data=f'PAX_APT_{a}') for a in APT_LIST[:4]],
        [InlineKeyboardButton(f'#{a}', callback_data=f'PAX_APT_{a}') for a in APT_LIST[4:]],
    ])
    await update.message.reply_text('🏢 Qaysi kvartira?', reply_markup=kb)


async def pax_apt_num(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q    = update.callback_query
    await q.answer()
    uid  = update.effective_user.id
    if uid not in _pax_data:
        await q.edit_message_text('❌ Sessiya tugagan. Pasportni qaytadan yuboring.')
        return
    apt  = q.data.replace('PAX_APT_', '')
    _pax_data[uid]['apartment'] = apt
    _pax_data[uid]['_state']    = 'PAX_PAYMENT'
    await q.edit_message_text(f'🚪 {apt}-kvartira tanlandi.\n\nQo\'shimcha xona raqami (bo\'lmasa "yo\'q" deb yozing):')


async def pax_payment(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    text = update.message.text.strip()
    if text.lower() in ('bekor', 'cancel', '/cancel'):
        _pax_data.pop(uid, None)
        await update.message.reply_text('❌ Ro\'yxatga olish bekor qilindi.')
        return
    d    = _pax_data[uid]
    d['room']   = '' if text.lower() in ("yo'q", 'yoq', '-', '') else text
    d['_state'] = 'PAX_CONFIRM'

    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton('🏠 AirBnB', callback_data='PAX_PAY_airbnb'),
        InlineKeyboardButton('💵 To\'g\'ridan-to\'g\'ri', callback_data='PAX_PAY_direct'),
    ]])
    await update.message.reply_text("💳 To'lov turi:", reply_markup=kb)


async def pax_confirm(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q   = update.callback_query
    await q.answer()
    uid = update.effective_user.id
    d   = _pax_data.get(uid)
    if not d:
        await q.edit_message_text('❌ Sessiya tugagan. Pasportni qaytadan yuboring.')
        return

    pay_type = 'airbnb' if 'airbnb' in q.data else 'direct'
    d['paymentBy'] = pay_type
    d.pop('_state', None)

    guest    = d.get('guests', [{}])[0]
    apt      = d.get('apartment', '28')

    def disp(iso):
        try: return datetime.strptime(iso, '%Y-%m-%d').strftime('%d.%m.%Y')
        except: return iso or '—'

    summary = (
        f"📋 <b>Ro'yxatga olish tasdiqlash</b>\n\n"
        f"👤 {guest.get('name', '?')}\n"
        f"🌍 {guest.get('nationality', '?')} · {guest.get('dob', '?')}\n"
        f"📋 {guest.get('passportId', '?')}\n"
        f"🇺🇿 O'zbekistonga: {disp(d.get('uzbekEntryDate',''))}\n"
        f"🏠 Kirish: {disp(d.get('aptEntryDate',''))} → {disp(d.get('departureDate',''))}\n"
        f"🏢 Kvartira: #{apt}"
        + (f", xona: {d.get('room')}" if d.get('room') else '') + "\n"
        f"💳 {pay_type.upper()}\n\n"
        f"Hujjatlar tayyorlanmoqda..."
    )
    await q.edit_message_text(summary, parse_mode='HTML')

    try:
        from generate_doc import generate_ariza_doc
        from bnb_services import get_drive_file, tg_send_file, APT_ISH, APT_CAD, FERUDIN_PDF_ID
        import asyncio as _asyncio

        ariza_bytes = generate_ariza_doc(d)
        guest_name  = guest.get('name', 'mehmon').replace(' ', '_')[:20]
        tg_send_file(update.effective_chat.id, ariza_bytes, f'Ariza_{apt}_{guest_name}.docx', '1/4 — ARIZA')

        ish_id = APT_ISH.get(apt, '')
        if ish_id:
            data = await _asyncio.to_thread(get_drive_file, ish_id)
            tg_send_file(update.effective_chat.id, data, f'Ishonchnoma_{apt}.pdf', '2/4 — Ishonchnoma')

        cad_id = APT_CAD.get(apt, '')
        if cad_id:
            data = await _asyncio.to_thread(get_drive_file, cad_id)
            tg_send_file(update.effective_chat.id, data, f'Kadastr_{apt}.pdf', '3/4 — Kadastr')

        if FERUDIN_PDF_ID:
            data = await _asyncio.to_thread(get_drive_file, FERUDIN_PDF_ID)
            tg_send_file(update.effective_chat.id, data, 'identification_card_FERUDIN.pdf', '4/4 — ID Card')

        await store.bnb_save(d)
        from bnb_services import save_bnb_to_sheets
        _mirror_task('bnb_save', save_bnb_to_sheets, d)

        await ctx.bot.send_message(
            update.effective_chat.id,
            f"✅ <b>Ro'yxatga olindi!</b> {guest.get('name')} — #{apt}",
            parse_mode='HTML'
        )
    except Exception as e:
        await ctx.bot.send_message(update.effective_chat.id, f'❌ Xatolik: {str(e)[:200]}')
        logger.error(f'pax_confirm: {e}')
    finally:
        _pax_data.pop(uid, None)


def _sheets_namoz_times_save(year, month, days):
    """NAMOZ_TIMES sahifasida shu oyga oid qatorlarni almashtiradi (zaxira)."""
    sh = get_ss()
    try:
        ws = sh.worksheet('NAMOZ_TIMES')
    except Exception:
        ws = sh.add_worksheet(title='NAMOZ_TIMES', rows=400, cols=10)
        ws.update('A1:I1', [['year','month','day','bomdod','quyosh','peshin','asr','shom','xufton']])
    all_rows = ws.get_all_values()
    rows_to_keep = [all_rows[0]] if all_rows else [['year','month','day','bomdod','quyosh','peshin','asr','shom','xufton']]
    for row in all_rows[1:]:
        if len(row) >= 3:
            try:
                if int(row[0]) == year and int(row[1]) == month:
                    continue
            except Exception:
                pass
        rows_to_keep.append(row)
    ws.clear()
    if rows_to_keep:
        ws.update('A1', rows_to_keep)
    for day_str, times in days.items():
        try: day_n = int(day_str)
        except Exception: continue
        ws.append_row([
            year, month, day_n,
            times.get('bomdod') or '', times.get('quyosh') or '',
            times.get('peshin') or '', times.get('asr') or '',
            times.get('shom') or '', times.get('xufton') or '',
        ], value_input_option='USER_ENTERED')

async def handle_namoz_photo(update: Update, ctx: ContextTypes.DEFAULT_TYPE,
                              msg, image_b64: str, media_type: str):
    """Namoz vaqtlari rasmini analiz qilib NAMOZ_TIMES varag'iga saqlash"""
    await msg.edit_text('🕌 Namoz jadvali tahlil qilinmoqda...')
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        now    = datetime.now(TZ)

        resp = await asyncio.to_thread(
            client.messages.create,
            model='claude-opus-4-5',
            max_tokens=2000,
            messages=[{
                'role': 'user',
                'content': [
                    {'type':'image','source':{'type':'base64','media_type':media_type,'data':image_b64}},
                    {'type':'text','text':(
                        f'Bu namoz vaqtlari jadvali. '
                        f'Har kun uchun vaqtlarni JSON formatida chiqar.\n'
                        f'Format (faqat JSON, boshqa narsa yozma):\n'
                        f'{{"month": <oy raqami 1-12>, "days": {{'
                        f'"1": {{"bomdod":"HH:MM","quyosh":"HH:MM","peshin":"HH:MM","asr":"HH:MM","shom":"HH:MM","xufton":"HH:MM"}},'
                        f'"2": {{...}}, ...}}}}\n'
                        f'Agar quyosh vaqti ko\'rsatilmagan bo\'lsa null qo\'y.'
                    )}
                ]
            }]
        )

        raw  = resp.content[0].text.strip()
        raw  = re.sub(r'```(?:json)?\s*', '', raw).strip('`').strip()
        data = json.loads(raw)

        month = data.get('month', now.month)
        days  = data.get('days', {})

        if not days:
            await msg.edit_text("❌ Jadvaldan ma'lumot o'qib bo'lmadi. Aniqroq rasm yuboring.")
            return

        year = now.year
        rows = []
        for day_str, t in days.items():
            try: day_n = int(day_str)
            except Exception: continue
            rows.append({'day': day_n, 'bomdod': t.get('bomdod') or '', 'quyosh': t.get('quyosh') or '',
                         'peshin': t.get('peshin') or '', 'asr': t.get('asr') or '',
                         'shom': t.get('shom') or '', 'xufton': t.get('xufton') or ''})
        await store.namoz_times_save(year, month, rows)
        _mirror_task('namoz_times_save', _sheets_namoz_times_save, year, month, days)

        month_names = {
            1:'Yanvar',2:'Fevral',3:'Mart',4:'Aprel',5:'May',6:'Iyun',
            7:'Iyul',8:'Avgust',9:'Sentabr',10:'Oktabr',11:'Noyabr',12:'Dekabr'
        }
        sample_key = list(days.keys())[0]
        sample     = days[sample_key]
        await msg.edit_text(
            f"✅ <b>{month_names.get(month,month)}-oy namoz vaqtlari saqlandi!</b>\n\n"
            f"📊 {len(days)} kun ma'lumoti yangilandi\n\n"
            f"Namuna ({sample_key}-kun):\n"
            f"🌅 Bomdod: {sample.get('bomdod','?')}\n"
            f"☀️ Quyosh: {sample.get('quyosh') or '—'}\n"
            f"☀️ Peshin:  {sample.get('peshin','?')}\n"
            f"🌤 Asr:    {sample.get('asr','?')}\n"
            f"🌇 Shom:   {sample.get('shom','?')}\n"
            f"🌙 Xufton: {sample.get('xufton','?')}",
            parse_mode='HTML'
        )

    except json.JSONDecodeError:
        await msg.edit_text("❌ Jadval o'qilmadi. Rasmni aniqroq qilib yuboring.")
    except Exception as e:
        await msg.edit_text(f"❌ Xatolik: {str(e)[:100]}")
        logger.error(f'handle_namoz_photo: {e}')

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
        await update.message.reply_text('❌ Tahrirlash sessiyasi tugadi. Qayta yuboring.', reply_markup=kb_reply_main())
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
        await q.edit_message_text('✅ Asosiy menyuga qaytildi.', parse_mode='HTML')
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
        try:
            lst = await store.qarz_active()
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
        try:
            lst = await store.qarz_active()
            if not lst:
                await q.edit_message_text('📭 Qaytaradigan faol qarzlar yo\'q.',
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('🔙 Orqaga',callback_data='QARZ_BACK')]]))
                return
            buttons = []
            for r in lst:
                s   = _qarz_sum(r)
                lbl = f"{'→' if r['tur']=='BERILGAN' else '←'} {r['kim']} ({s})"
                buttons.append([InlineKeyboardButton(lbl, callback_data=f"QARZ_DONE_{r['_id']}")])
            buttons.append([InlineKeyboardButton('🔙 Orqaga',callback_data='QARZ_BACK')])
            await q.edit_message_text('✅ <b>Qaysi qarz qaytarildi?</b>',
                parse_mode='HTML', reply_markup=InlineKeyboardMarkup(buttons))
        except Exception as e:
            await q.edit_message_text(f'❌ Xatolik: {e}')
        return

    if d.startswith('QARZ_DONE_'):
        qarz_id = d[len('QARZ_DONE_'):]
        try:
            today = today_str()
            r = await store.qarz_get(qarz_id)
            if not r:
                await q.edit_message_text("⚠️ Bu qarz topilmadi (ehtimol allaqachon yopilgan).")
                return
            tur       = r['tur']
            kim       = r['kim']
            summa_uzs = r.get('summa_uzs')
            summa_usd = r.get('summa_usd')

            await store.qarz_close(qarz_id, today)

            egasi = 'FERUDIN'
            if tur == 'BERILGAN':
                await _record_qarz_transaction(
                    'KIRIM', egasi, summa_usd, summa_uzs,
                    f'Qarz qaytdi: {kim}', 'QARZ QAYTIB KELDI')
                icon   = '✅💰'
                effect = "Balansga qaytdi (+)"
            else:
                await _record_qarz_transaction(
                    'CHIQIM', egasi, summa_usd, summa_uzs,
                    f'Qarz qaytarildi: {kim}', 'QARZ QAYTARILDI')
                icon   = '✅💸'
                effect = "Balansdan ayrildi (−)"

            bal = await get_balance()
            s_uzs = f"{float(summa_uzs):,.0f} UZS" if summa_uzs else ''
            s_usd = f"{float(summa_usd):.2f} USD"   if summa_usd else ''
            summa_str = ' / '.join(filter(None, [s_uzs, s_usd])) or '?'

            await q.edit_message_text(
                f'{icon} <b>{kim}</b> bilan hisob-kitob yakunlandi!\n\n'
                f'💰 {summa_str}\n'
                f'📅 {today}\n'
                f'<i>{effect}</i>\n'
                f'💰 Joriy balans: <b>{sstr(*bal)}</b>',
                parse_mode='HTML')
        except Exception as e:
            await q.edit_message_text(f'❌ Xatolik: {e}')
        return

    if d == 'QARZ_STAT':
        try:
            lst = await store.qarz_active()
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

async def _record_qarz_transaction(sheet_name, egasi, usd_val, uzs_val, note, tur):
    """Qarz natijasida balansga ta'sir qiluvchi tranzaksiya: Supabase (asosiy) + Sheets (zaxira)."""
    now_dt = datetime.now(TZ)
    today  = now_dt.strftime('%d.%m.%Y')
    now_t  = now_dt.strftime('%H:%M')
    usd_v  = usd_val or 0
    uzs_v  = uzs_val or 0
    await store.add_transaction(sheet_name, today, egasi, tur, 'CASH', usd_v, uzs_v, now_t, note)
    _mirror_task(f'qarz_tx:{sheet_name}', _sheets_qarz_to_sheet, sheet_name, egasi, usd_val, uzs_val, note, tur)

def _sheets_qarz_to_sheet(sheet_name: str, egasi: str, usd_val, uzs_val, note: str, tur: str = None) -> int:
    if tur is None:
        tur = 'QARZ BERILDI' if sheet_name == 'CHIQIM' else 'QARZ OLINDI'

    sh    = get_ss().worksheet(sheet_name)
    today = datetime.now(TZ).strftime('%d.%m.%Y')
    col_c = sh.col_values(3)
    last  = 2
    for i, v in enumerate(col_c):
        if i < 2: continue
        if v and str(v).strip(): last = i + 1
    new_row = last + 1

    try: usd = float(usd_val) if usd_val is not None and str(usd_val).strip() not in ('', '0', '0.0') else ''
    except: usd = ''
    try: uzs = float(uzs_val) if uzs_val is not None and str(uzs_val).strip() not in ('', '0', '0.0') else ''
    except: uzs = ''

    logger.info(f'qarz_to_sheet: {sheet_name} | tur={tur} | usd={usd} | uzs={uzs}')
    sh.update(f'B{new_row}:H{new_row}', [[
        new_row - 2, today, egasi, tur, 'CASH', usd, uzs
    ]], value_input_option='USER_ENTERED')
    sh.update(f'J{new_row}', [[note]])
    logger.info(f'SAQLANDI: {sheet_name} row {new_row}')
    return new_row

def _sheets_qarz_append(q, today):
    ws   = get_ss().worksheet('QARZ')
    rows = ws.get_all_values()
    num  = len(rows)
    ws.append_row([
        num, q['tur'], q['kim'],
        q.get('summa_uzs',''), q.get('summa_usd',''),
        today, q['muddat'], 'AKTIV', '', q.get('note','')
    ], value_input_option='USER_ENTERED')

async def _qarz_save(update, q: dict):
    try:
        today = today_str()
        tur   = q['tur']
        egasi = 'FERUDIN'
        usd_v = q.get('summa_usd') or None
        uzs_v = q.get('summa_uzs') or None

        await store.qarz_add(tur, q['kim'], q.get('summa_uzs') or None, q.get('summa_usd') or None,
                             today, q['muddat'], q.get('note', ''))
        _mirror_task('qarz_append', _sheets_qarz_append, q, today)

        if tur == 'BERILGAN':
            await _record_qarz_transaction('CHIQIM', egasi, usd_v, uzs_v,
                f"Qarz berildi: {q['kim']}", 'QARZ BERILDI')
            icon   = '💸'
            effect = "Balansdan ayrildi (−)"
        else:
            await _record_qarz_transaction('KIRIM', egasi, usd_v, uzs_v,
                f"Qarz olindi: {q['kim']}", 'QARZ OLINDI')
            icon   = '💰'
            effect = "Balansga qo'shildi (+)"

        bal = await get_balance()
        arr = '→' if tur == 'BERILGAN' else '←'
        s   = _qarz_sum(q)
        await update.message.reply_text(
            f'✅ <b>Qarz saqlandi!</b>\n\n'
            f'{icon} {arr} <b>{q["kim"]}</b>\n'
            f'💰 {s}\n'
            f'📅 {today}  ⏰ Muddat: {q["muddat"]}\n'
            f'<i>{effect}</i>\n'
            f'💰 Joriy balans: <b>{sstr(*bal)}</b>'
            + (f'\n📝 {q["note"]}' if q.get('note') else ''),
            parse_mode='HTML', reply_markup=kb_reply_main())
    except Exception as e:
        await update.message.reply_text(
            f'❌ Qarz saqlashda xatolik: {str(e)[:100]}',
            reply_markup=kb_reply_main())
        logger.error(f'_qarz_save: {e}')

# ══════════════════════════════════════════════════════════
# QARZ MUDDAT NOTIFICATION — kunlik 09:00
# ══════════════════════════════════════════════════════════
async def qarz_notify_job(ctx: ContextTypes.DEFAULT_TYPE):
    try:
        lst = await store.qarz_active()
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
        await q.edit_message_text('✅ Asosiy menyuga qaytildi.', parse_mode='HTML')
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
        parse_mode='HTML', reply_markup=kb_reply_main())

async def namoz_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ok(update): return
    times  = await get_prayer_times(datetime.now(TZ).date())
    now_hm = datetime.now(TZ).strftime('%H:%M')
    sana   = datetime.now(TZ).strftime('%d.%m.%Y')
    txt    = f'🕌 <b>Namoz vaqtlari — {sana}</b>\n\n'
    for namoz, vaqt in (times or {}).items():
        if namoz == 'quyosh':
            if vaqt:
                txt += f'   ☀️ <i>Quyosh: {vaqt} (bomdod shu vaqtgacha)</i>\n'
            continue
        emoji  = NAMOZ_EMOJI.get(namoz, '🕌')
        marker = '✅' if vaqt < now_hm else '⏰'
        txt   += f'{marker} {emoji} <b>{namoz.upper()}</b>: {vaqt}\n'
    await update.message.reply_text(txt, parse_mode='HTML', reply_markup=kb_reply_main())

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
        bal  = await get_balance()
        rows = get_ss().worksheet('CHIQIM').get_all_values()
        lines = [f'Balance: {sstr(*bal)}', f'CHIQIM: {len(rows)} qator',
                 f'Kategoriyalar: {len(get_chiqim_turs())} ta']
        await update.message.reply_text('\n'.join(lines), reply_markup=kb_reply_main())
    except Exception as e:
        await update.message.reply_text(f'Xato: {e}', reply_markup=kb_reply_main())

async def qarz_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ok(update): return
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

def _sheets_task_append(matn, vaqt_str, egasi, chat_id, today):
    ws   = get_ss().worksheet('TASKS')
    vals = ws.get_all_values()
    ws.append_row([len(vals), today, vaqt_str, matn, egasi, 'FAOL', chat_id],
                  value_input_option='USER_ENTERED')

def _sheets_task_mark(matn, egasi, status):
    """UUID Sheets'da yo'q — qatorni matn+egasi+holat='FAOL' bo'yicha topamiz (best-effort)."""
    ws   = get_ss().worksheet('TASKS')
    vals = ws.get_all_values()
    for i, row in enumerate(vals[1:], start=2):
        if len(row) >= 6 and row[3] == matn and row[4] == egasi and row[5] == 'FAOL':
            ws.update_cell(i, 6, status)
            return
    logger.warning(f"_sheets_task_mark: mos vazifa topilmadi ({matn[:30]}/{egasi})")

async def task_reminder_job(ctx: ContextTypes.DEFAULT_TYPE):
    d       = ctx.job.data
    matn    = d['matn']
    egasi   = d['egasi']
    task_id = d['task_id']
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton('✅ Bajarildi', callback_data=f'TASK_DONE_{task_id}'),
        InlineKeyboardButton('⏭ O\'tkazish', callback_data=f'TASK_SKIP_{task_id}'),
    ]])
    txt = f'⏰ <b>ESLATMA!</b>\n\n📋 {matn}\n👤 {egasi}'
    if egasi == 'FERUDIN':   targets = [CHAT_1]
    elif egasi == 'GULOYIM': targets = [CHAT_2]
    else:                    targets = [CHAT_1, CHAT_2]
    for cid in targets:
        try: await ctx.bot.send_message(chat_id=cid, text=txt, parse_mode='HTML', reply_markup=kb)
        except Exception as e: logger.error(f'task_reminder {cid}: {e}')

async def tasks_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    d = q.data   # TASK_DONE_<uuid> / TASK_SKIP_<uuid>
    if d.startswith('TASK_DONE_'):
        action, task_id = 'DONE', d[len('TASK_DONE_'):]
    else:
        action, task_id = 'SKIP', d[len('TASK_SKIP_'):]
    status = 'BAJARILDI' if action == 'DONE' else "O'TKAZILDI"
    icon   = '✅' if action == 'DONE' else '⏭'
    try:
        t = await store.task_get(task_id)
        if not t:
            await q.edit_message_text("⚠️ Bu vazifa topilmadi (ehtimol allaqachon belgilangan).")
            return
        await store.task_mark(task_id, status)
        _mirror_task('task_mark', _sheets_task_mark, t['matn'], t['egasi'], status)
        old = q.message.text or ''
        await q.edit_message_text(f'{icon} <b>{status}</b>\n\n{old}', parse_mode='HTML')
    except Exception as e:
        await q.edit_message_text(f'❌ Xatolik: {e}')

async def save_and_schedule_task(app_obj, matn: str, vaqt_str: str, egasi: str, chat_id: str):
    today = datetime.now(TZ).strftime('%d.%m.%Y')
    try:
        task_id = await store.task_add(matn, vaqt_str, egasi, chat_id, today)
        _mirror_task('task_append', _sheets_task_append, matn, vaqt_str, egasi, chat_id, today)
        try:
            reminder_dt = TZ.localize(datetime.strptime(vaqt_str, '%d.%m.%Y %H:%M'))
        except:
            return task_id
        if reminder_dt > datetime.now(TZ):
            app_obj.job_queue.run_once(
                task_reminder_job, when=reminder_dt,
                data={'matn': matn, 'egasi': egasi, 'task_id': task_id},
                name=f'task_{task_id}')
        return task_id
    except Exception as e:
        logger.error(f'save_and_schedule_task: {e}')
        return None

async def reschedule_pending_tasks(app_obj):
    try:
        lst   = await store.tasks_active()
        now   = datetime.now(TZ)
        count = 0
        for t in lst:
            try:
                reminder_dt = TZ.localize(datetime.strptime(t['vaqt'], '%d.%m.%Y %H:%M'))
            except: continue
            if reminder_dt <= now: continue
            app_obj.job_queue.run_once(
                task_reminder_job, when=reminder_dt,
                data={'matn': t['matn'], 'egasi': t['egasi'], 'task_id': t['_id']},
                name=f"task_rs_{t['_id']}")
            count += 1
        if count: logger.info(f'Restart: {count} ta task qayta scheduled')
    except Exception as e:
        logger.error(f'reschedule_pending_tasks: {e}')

async def tasks_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ok(update): return
    try:
        today = datetime.now(TZ).date()
        lst   = await store.tasks_active()
        tasks = []
        for t in lst:
            try:
                dt = TZ.localize(datetime.strptime(t['vaqt'], '%d.%m.%Y %H:%M'))
                if dt.date() >= today:
                    tasks.append(t)
            except: continue
        if not tasks:
            await update.message.reply_text("📋 Faol tasklar yo'q.", reply_markup=kb_reply_main())
            return
        txt = '📋 <b>FAOL TASKLAR:</b>\n\n'
        for t in tasks[:15]:
            txt += f"⏰ <b>{t['vaqt']}</b>  👤 {t['egasi']}\n📝 {t['matn']}\n\n"
        await update.message.reply_text(txt, parse_mode='HTML', reply_markup=kb_reply_main())
    except Exception as e:
        await update.message.reply_text(f'❌ Xatolik: {e}')

# ══════════════════════════════════════════════════════════
# MEMORY TIZIMI
# ══════════════════════════════════════════════════════════

def _sheets_memory_save(kalit, qiymat, kim, sana):
    """MEMORY sahifasida kalit (case-insensitive) bo'yicha topib yangilaydi, topilmasa qo'shadi."""
    ws   = get_ss().worksheet('MEMORY')
    vals = ws.get_all_values()
    for i, row in enumerate(vals[1:], start=2):
        if len(row) >= 3 and row[2].lower() == kalit.lower():
            ws.update(f'B{i}:E{i}', [[sana, kalit, qiymat, kim]])
            return
    ws.append_row([len(vals), sana, kalit, qiymat, kim], value_input_option='USER_ENTERED')

async def memory_save(kalit: str, qiymat: str, kim: str) -> str:
    today = today_str()
    try:
        action = await store.memory_save(kalit, qiymat, kim, today)
        _mirror_task('memory_save', _sheets_memory_save, kalit, qiymat, kim, today)
        return action
    except Exception as e:
        logger.error(f'memory_save: {e}')
        return 'xato'

async def memory_search(query: str) -> list:
    try:
        return await store.memory_search(query)
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

async def get_prayer_times(target_date=None) -> dict:
    """Namoz vaqtlari: avval Supabase (family_namoz_times) dan, bo'lmasa hardcode fallback."""
    if target_date is None:
        target_date = datetime.now(TZ).date()
    elif isinstance(target_date, str):
        try: target_date = datetime.strptime(target_date, '%d-%m-%Y').date()
        except: target_date = datetime.now(TZ).date()

    # 1. Supabase family_namoz_times dan olish (asosiy manba)
    try:
        times = await store.namoz_times_get(target_date)
        if times and any(times.get(k) for k in NAMOZ_UZ):
            return {k: (times.get(k) or '') for k in ['bomdod', 'quyosh', 'peshin', 'asr', 'shom', 'xufton']}
    except Exception as e:
        logger.warning(f'namoz_times_get xato: {e}')

    # 2. Hardcode fallback
    month = target_date.month
    day   = target_date.day

    if month == 5:
        day_times = {
            1:  ["03:51","12:20","17:15","19:24","20:45"],
            2:  ["03:50","12:19","17:16","19:25","20:46"],
            3:  ["03:48","12:19","17:16","19:26","20:48"],
            4:  ["03:46","12:19","17:17","19:27","20:49"],
            5:  ["03:45","12:19","17:18","19:28","20:51"],
            6:  ["03:43","12:19","17:18","19:29","20:52"],
            7:  ["03:41","12:19","17:19","19:30","20:54"],
            8:  ["03:40","12:19","17:19","19:31","20:55"],
            9:  ["03:38","12:19","17:20","19:32","20:56"],
            10: ["03:37","12:19","17:21","19:33","20:58"],
            11: ["03:35","12:19","17:21","19:34","20:59"],
            12: ["03:34","12:19","17:22","19:35","21:01"],
            13: ["03:32","12:19","17:22","19:36","21:02"],
            14: ["03:31","12:19","17:23","19:37","21:04"],
            15: ["03:29","12:19","17:23","19:38","21:05"],
            16: ["03:28","12:19","17:24","19:39","21:06"],
            17: ["03:27","12:19","17:25","19:40","21:08"],
            18: ["03:25","12:19","17:25","19:41","21:09"],
            19: ["03:24","12:19","17:26","19:42","21:11"],
            20: ["03:23","12:19","17:26","19:43","21:12"],
            21: ["03:22","12:19","17:27","19:44","21:13"],
            22: ["03:20","12:19","17:27","19:45","21:15"],
            23: ["03:19","12:19","17:28","19:46","21:16"],
            24: ["03:18","12:19","17:28","19:47","21:17"],
            25: ["03:17","12:19","17:29","19:48","21:18"],
            26: ["03:16","12:19","17:29","19:49","21:20"],
            27: ["03:15","12:19","17:30","19:50","21:21"],
            28: ["03:14","12:20","17:30","19:50","21:22"],
            29: ["03:13","12:20","17:31","19:51","21:23"],
            30: ["03:12","12:20","17:31","19:52","21:25"],
            31: ["03:11","12:20","17:32","19:53","21:26"],
        }
        times = day_times.get(day, day_times[15])
    else:
        # [Bomdod, Peshin, Asr, Shom, Xufton] — har oy uchun 3 ta davr: 1-10, 11-20, 21-oxir
        TASHKENT_2026 = {
            1:  [["06:45","13:10","15:38","17:30","18:52"],["06:40","13:13","15:50","17:42","19:04"],["06:32","13:16","16:04","17:57","19:19"]],
            2:  [["06:18","13:16","16:20","18:13","19:36"],["06:03","13:14","16:35","18:28","19:52"],["05:45","13:11","16:49","18:43","20:07"]],
            3:  [["05:26","13:07","17:02","18:57","20:21"],["05:05","13:02","17:14","19:10","20:35"],["04:44","12:56","17:26","19:24","20:50"]],
            4:  [["04:22","12:50","17:37","19:37","21:04"],["04:01","12:44","17:47","19:50","21:17"],["03:42","12:39","17:57","20:03","21:31"]],
            6:  [["03:11","12:33","18:21","20:37","22:07"],["03:15","12:35","18:22","20:39","22:09"],["03:22","12:37","18:20","20:37","22:06"]],
            7:  [["03:32","12:39","18:16","20:31","22:00"],["03:44","12:40","18:09","20:22","21:49"],["03:57","12:40","18:00","20:11","21:36"]],
            8:  [["04:11","12:39","17:48","19:57","21:21"],["04:25","12:36","17:33","19:40","21:03"],["04:39","12:31","17:16","19:22","20:44"]],
            9:  [["04:53","12:25","16:57","19:02","20:23"],["05:07","12:18","16:37","18:41","20:01"],["05:20","12:10","16:15","18:18","19:37"]],
            10: [["05:34","12:01","15:52","17:55","19:14"],["05:48","11:54","15:30","17:32","18:50"],["06:02","11:48","15:09","17:09","18:27"]],
            11: [["06:16","11:44","14:50","16:48","18:07"],["06:29","11:42","14:35","16:32","17:51"],["06:40","11:43","14:25","16:22","17:42"]],
            12: [["06:49","11:47","14:21","16:17","17:37"],["06:53","11:52","14:22","16:17","17:38"],["06:52","11:57","14:27","16:21","17:42"]],
        }
        periods = TASHKENT_2026.get(month, TASHKENT_2026[6])
        if day <= 10:   times = periods[0]
        elif day <= 20: times = periods[1]
        else:           times = periods[2]

    return {
        'bomdod': times[0], 'quyosh': '',
        'peshin': times[1], 'asr':    times[2],
        'shom':   times[3], 'xufton': times[4],
    }

def _parse_prayer_dt(time_str: str, date_obj) -> datetime:
    h, m = map(int, time_str.split(':')[:2])
    return TZ.localize(datetime(date_obj.year, date_obj.month, date_obj.day, h, m, 0))

def _sheets_namoz_log_set_all(sana, kim, statuses):
    """UUID/PK Sheets'da yo'q — sana+kim bo'yicha qatorni topib 5 ta ustunni birdaniga yozadi."""
    ws       = get_ss().worksheet('NAMOZ')
    all_vals = ws.get_all_values()
    target_row = None
    for i, row in enumerate(all_vals[1:], start=2):
        if len(row) >= 7 and row[0] == sana and row[6] == kim:
            target_row = i
            break
    if target_row is None:
        target_row = len(all_vals) + 1
    vals = [statuses.get(n, '') for n in NAMOZ_UZ]
    ws.update(f'A{target_row}:G{target_row}', [[sana, *vals, kim]])

# ── Kunlik yakuniy check-in: Xuftondan 20 daqiqa keyin, 5 vaqt birdaniga ──
_namoz_checkin = {}   # (chat_id, sana) -> {namoz: 'OK'|'NO'|None, ...}

def _kb_namoz_checkin(sana, state):
    rows = []
    for namoz in NAMOZ_UZ:
        cur     = state.get(namoz)
        ha_txt  = "✅ Ha ✓"   if cur == 'OK' else "✅ Ha"
        yoq_txt = "❌ Yo'q ✓" if cur == 'NO' else "❌ Yo'q"
        rows.append([
            InlineKeyboardButton(f"{NAMOZ_EMOJI[namoz]} {namoz.upper()}", callback_data='NCHK_NOOP'),
            InlineKeyboardButton(ha_txt,  callback_data=f'NCHK_{namoz}_OK_{sana}'),
            InlineKeyboardButton(yoq_txt, callback_data=f'NCHK_{namoz}_NO_{sana}'),
        ])
    return InlineKeyboardMarkup(rows)

async def prayer_checkin_job(ctx: ContextTypes.DEFAULT_TYPE):
    sana = ctx.job.data['sana']
    txt  = (f"🌙 <b>Kunlik namoz hisoboti — {sana}</b>\n\n"
            f"Xufton vaqtidan 20 daqiqa o'tdi. Bugungi 5 vaqt namozni "
            f"o'qidingizmi? Har biri uchun javob bering 👇")
    for cid, kim in [(CHAT_1, 'FERUDIN'), (CHAT_2, 'GULOYIM')]:
        state = {n: None for n in NAMOZ_UZ}
        _namoz_checkin[(str(cid), sana)] = state
        try:
            await ctx.bot.send_message(chat_id=cid, text=txt, parse_mode='HTML',
                                        reply_markup=_kb_namoz_checkin(sana, state))
        except Exception as e:
            logger.error(f'prayer_checkin {cid}: {e}')

async def namoz_checkin_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    d = q.data
    if d == 'NCHK_NOOP':
        await q.answer()
        return
    try:
        _, namoz, ans, sana = d.split('_', 3)
    except ValueError:
        await q.answer()
        return
    cid = str(update.effective_chat.id)
    kim = 'FERUDIN' if cid == CHAT_1 else 'GULOYIM'
    key = (cid, sana)
    state = _namoz_checkin.setdefault(key, {n: None for n in NAMOZ_UZ})
    state[namoz] = ans
    await q.answer('✅ Belgilandi' if ans == 'OK' else '❌ Belgilandi')
    try:
        await q.edit_message_reply_markup(reply_markup=_kb_namoz_checkin(sana, state))
    except Exception:
        pass
    if all(v is not None for v in state.values()):
        statuses = {n: ("O'QILDI" if state[n] == 'OK' else "O'QILMADI") for n in NAMOZ_UZ}
        await store.namoz_log_set_all(sana, kim, statuses)
        _mirror_task('namoz_checkin', _sheets_namoz_log_set_all, sana, kim, statuses)
        ok_n    = sum(1 for v in statuses.values() if v == "O'QILDI")
        icons   = {n: ('✅' if statuses[n] == "O'QILDI" else '❌') for n in NAMOZ_UZ}
        summary = '\n'.join(f"{NAMOZ_EMOJI[n]} {n.upper()}: {icons[n]}" for n in NAMOZ_UZ)
        try:
            await q.edit_message_text(
                f"🌙 <b>Kunlik namoz hisoboti — {sana}</b>\n👤 {kim}\n\n{summary}\n\n"
                f"📊 Jami: {ok_n}/5 o'qildi. Alloh qabul qilsin! 🤲",
                parse_mode='HTML')
        except Exception:
            pass
        _namoz_checkin.pop(key, None)

async def prayer_time_job(ctx: ContextTypes.DEFAULT_TYPE):
    d     = ctx.job.data
    namoz = d['namoz']
    vaqt  = d['vaqt']
    txt = (f"{NAMOZ_EMOJI[namoz]} <b>{namoz.upper()} vaqti kirdi!</b>\n\n"
           f"🕌 {vaqt} — Alloh qabul qilsin! 🤲")
    for cid in [CHAT_1, CHAT_2]:
        try: await ctx.bot.send_message(chat_id=cid, text=txt, parse_mode='HTML')
        except Exception as e: logger.error(f'prayer_time {cid}: {e}')

async def namoz_update_reminder(ctx: ContextTypes.DEFAULT_TYPE):
    """Har oy 1-sanada namoz jadvalini yangilash eslatmasi"""
    now = datetime.now(TZ)
    if now.day != 1:
        return
    month_names = {
        1:'Yanvar',2:'Fevral',3:'Mart',4:'Aprel',
        5:'May',6:'Iyun',7:'Iyul',8:'Avgust',
        9:'Sentabr',10:'Oktabr',11:'Noyabr',12:'Dekabr'
    }
    month_name = month_names.get(now.month, str(now.month))
    text = (
        f"🕌 <b>Namoz vaqtlarini yangilash vaqti!</b>\n\n"
        f"📅 {month_name} {now.year} oyi boshlandi.\n\n"
        f"Yangi oyning namoz vaqtlari jadvalini "
        f"(islom.uz yoki masjid taqvimi) rasm sifatida "
        f"yuboring — men avtomatik yangilayman. 📸"
    )
    for cid in [CHAT_1, CHAT_2]:
        try:
            await ctx.bot.send_message(chat_id=cid, text=text, parse_mode='HTML')
        except Exception as e:
            logger.error(f'namoz_update_reminder {cid}: {e}')


async def schedule_todays_prayers(app_obj, date_obj=None):
    if date_obj is None:
        date_obj = datetime.now(TZ).date()
    sana  = date_obj.strftime('%d.%m.%Y')
    times = await get_prayer_times(date_obj)
    if not times:
        logger.error('Namoz vaqtlari olinmadi — API javob bermadi')
        return
    now       = datetime.now(TZ)
    xufton_dt = None
    for namoz, vaqt_str in times.items():
        if namoz not in NAMOZ_COL or not vaqt_str:
            continue
        prayer_dt = _parse_prayer_dt(vaqt_str, date_obj)
        job_data  = {'namoz': namoz, 'vaqt': vaqt_str, 'sana': sana}
        if prayer_dt > now:
            app_obj.job_queue.run_once(
                prayer_time_job, when=prayer_dt,
                data=job_data, name=f'prayer_{namoz}_{sana}')
        if namoz == 'xufton':
            xufton_dt = prayer_dt
    if xufton_dt:
        checkin_dt = xufton_dt + timedelta(minutes=20)
        if checkin_dt > now:
            app_obj.job_queue.run_once(
                prayer_checkin_job, when=checkin_dt,
                data={'sana': sana}, name=f'checkin_{sana}')
    logger.info(f'{sana} namoz vaqtlari scheduled: {times}')

async def daily_prayer_scheduler(ctx: ContextTypes.DEFAULT_TYPE):
    tomorrow = (datetime.now(TZ) + timedelta(days=1)).date()
    await schedule_todays_prayers(ctx.application, tomorrow)

async def namoz_weekly_stats(ctx: ContextTypes.DEFAULT_TYPE):
    try:
        stats = await store.namoz_weekly_stats(datetime.now(TZ).date())
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
        dv  = await get_bugun()
        bal = await get_balance()
        txt = f'📊 <b>{today_str()} — Kunlik hisobot</b>\n\n<b>📤 Chiqimlar:</b>\n'
        txt += ('\n'.join(f'  • {c["tur"]}: {sstr(c["usd"],c["uzs"])}' for c in dv['ch'])) or "  Yo'q"
        txt += '\n\n<b>📥 Kirimlar:</b>\n'
        txt += ('\n'.join(f'  • {k["tur"]}: {sstr(k["usd"],k["uzs"])}' for k in dv['ki'])) or "  Yo'q"
        txt += (f'\n\n▪️ Jami chiqim: <b>{sstr(dv["chU"],dv["chZ"])}</b>'
                f'\n▪️ Jami kirim:  <b>{sstr(dv["kiU"],dv["kiZ"])}</b>'
                f'\n\n💰 <b>BALANCE: {sstr(*bal)}</b>')
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
        # Supabase env tekshirish
        sb_url  = os.environ.get('SUPABASE_URL', '')
        sb_key  = os.environ.get('SUPABASE_SERVICE_ROLE_KEY', '')
        if not sb_url or not sb_key:
            logger.error(f'SUPABASE env YOQLIGINI TEKSHIRING: URL={bool(sb_url)} KEY={bool(sb_key)}')
        try:
            await load_categories()
        except Exception as e:
            logger.error(f'load_categories xato (DB muammo?): {e}')
        try:
            await schedule_todays_prayers(application)
        except Exception as e:
            logger.error(f'schedule_todays_prayers xato: {e}')
        try:
            await reschedule_pending_tasks(application)
        except Exception as e:
            logger.error(f'reschedule_pending_tasks xato: {e}')
        logger.info('Bot ishga tushdi.')
    app.post_init = on_startup

    # ── Hisobot conversation ─────────────────────────────
    hisobot_conv = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(hisobot_start, pattern='^MH$'),
            CommandHandler('hisobot', hisobot_start_cmd),
            MessageHandler(
                filters.Regex('^🔍 Hisobot$') & filters.Chat(chat_id=[int(CHAT_1), int(CHAT_2)]),
                hisobot_start_cmd),
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
        entry_points=[
            CallbackQueryHandler(btn),
            MessageHandler(
                filters.Regex('^(📤 Chiqim|📥 Kirim)$') & filters.Chat(chat_id=[int(CHAT_1), int(CHAT_2)]),
                handle_reply_start),
        ],
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
    app.add_handler(CallbackQueryHandler(namoz_checkin_callback, pattern='^NCHK_'))
    app.add_handler(CallbackQueryHandler(tasks_callback,  pattern='^TASK_'))
    app.add_handler(CallbackQueryHandler(pax_apt_num,     pattern='^PAX_APT_'))
    app.add_handler(CallbackQueryHandler(pax_confirm,     pattern='^PAX_PAY_'))

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
    # Har oy 1-sanada namoz jadvalini yangilash eslatmasi — 11:00 Tashkent
    app.job_queue.run_daily(namoz_update_reminder,
        time=dtime(hour=6, minute=0, tzinfo=pytz.utc),
        name='namoz_update_reminder')

    logger.info('Bot ishga tushdi!')
    app.run_polling(drop_pending_updates=True)

# ══════════════════════════════════════════════════════════
# FASTAPI — REST API
# ══════════════════════════════════════════════════════════
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List

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

def _sheets_update_transaction(sheet_name, old, patch_disp):
    """UUID Sheets'da saqlanmagani uchun qatorni eski qiymatlar bo'yicha
    (sana+egasi+tur+summa+vaqt) moslashtirib topamiz — best-effort."""
    sh   = get_ss().worksheet(sheet_name)
    rows = sh.get_all_values()
    for i, row in enumerate(rows[2:], start=3):
        if len(row) < 8: continue
        if (norm_date(row[2] if len(row)>2 else '') == old['sana']
                and (row[3] if len(row)>3 else '') == old['egasi']
                and (row[4] if len(row)>4 else '') == old['tur']
                and num_clean(row[6] if len(row)>6 else '') == old['usd']
                and num_clean(row[7] if len(row)>7 else '') == old['uzs']
                and (row[8] if len(row)>8 else '') == old['vaqt']):
            if 'sana' in patch_disp:  sh.update(f'C{i}', [[patch_disp['sana']]])
            if 'egasi' in patch_disp: sh.update(f'D{i}', [[patch_disp['egasi']]])
            if 'tur' in patch_disp:   sh.update(f'E{i}', [[patch_disp['tur']]])
            if 'tolov' in patch_disp: sh.update(f'F{i}', [[patch_disp['tolov']]])
            if 'usd' in patch_disp:   sh.update(f'G{i}', [[patch_disp['usd']]])
            if 'uzs' in patch_disp:   sh.update(f'H{i}', [[patch_disp['uzs']]])
            if 'note' in patch_disp:  sh.update(f'J{i}', [[patch_disp['note']]])
            return
    logger.warning(f'_sheets_update_transaction: mos qator topilmadi ({sheet_name})')

@api.get('/')
def root(): return {'status':'ok','message':'Family Accounting API'}

@api.get('/balance')
async def balance_endpoint():
    try:
        usd, uzs = await get_balance()
        return {'balance_usd':usd,'balance_uzs':uzs,'formatted':sstr(usd,uzs)}
    except Exception as e: raise HTTPException(500, str(e))

@api.get('/today')
async def get_today_api():
    try:
        d = await get_bugun()
        return {'date':today_str(),'chiqimlar':d['ch'],'kirimlar':d['ki'],
                'total_ch_usd':round(d['chU'],2),'total_ch_uzs':round(d['chZ'],2),
                'total_ki_usd':round(d['kiU'],2),'total_ki_uzs':round(d['kiZ'],2)}
    except Exception as e: raise HTTPException(500, str(e))

@api.get('/by-date')
async def get_by_date(date: str = Query(...)):
    try:
        d = await store.get_bugun(date)
        return {'date':date,'chiqimlar':d['ch'],'kirimlar':d['ki']}
    except Exception as e: raise HTTPException(500, str(e))

@api.get('/by-filter')
async def get_by_filter(
    tip:str=Query('CHIQIM'),davr:str=Query('bu_oy'),
    tur:str=Query('BARCHASI'),date_from:str=Query(None),date_to:str=Query(None)
):
    try:
        rows,total_usd,total_uzs = await get_filtered(tip,davr,tur,date_from,date_to)
        return {'rows':rows,'total_usd':round(total_usd,2),'total_uzs':round(total_uzs,2),'count':len(rows)}
    except Exception as e: raise HTTPException(500, str(e))

@api.get('/history')
async def get_history(limit:int=100):
    try:
        txs = await store.get_history(limit)
        return {'transactions':txs,'total':len(txs)}
    except Exception as e: raise HTTPException(500, str(e))

@api.get('/stats')
async def get_stats():
    try:
        ch_by, ch_usd, ch_uzs = await store.get_stats('CHIQIM','bu_oy')
        ki_by, ki_usd, ki_uzs = await store.get_stats('KIRIM','bu_oy')
        def _shape(by_cat):
            ranked = sorted(by_cat.items(), key=lambda x: x[1]['usd']+x[1]['uzs']/12000, reverse=True)
            return [{'tur':t,'usd':round(v['usd'],2),'uzs':round(v['uzs'],2),'count':v['count']} for t,v in ranked]
        ch_items, ki_items = _shape(ch_by), _shape(ki_by)
        return {
            'chiqim':{'by_tur':ch_items,'top':ch_items[0] if ch_items else None,'bottom':ch_items[-1] if ch_items else None,
                      'total_usd':round(ch_usd,2),'total_uzs':round(ch_uzs,2),'count':sum(v['count'] for v in ch_by.values())},
            'kirim': {'by_tur':ki_items,'top':ki_items[0] if ki_items else None,'bottom':ki_items[-1] if ki_items else None,
                      'total_usd':round(ki_usd,2),'total_uzs':round(ki_uzs,2),'count':sum(v['count'] for v in ki_by.values())},
            'net_usd':round(ki_usd-ch_usd,2),'net_uzs':round(ki_uzs-ch_uzs,2),
        }
    except Exception as e: raise HTTPException(500, str(e))

@api.post('/transaction')
async def add_transaction_api(tx: Transaction):
    try:
        usd_val = tx.summa if tx.valyuta=='USD' else 0
        uzs_val = tx.summa if tx.valyuta=='UZS' else 0
        now_t   = datetime.now(TZ).strftime('%H:%M')
        new_id  = await store.add_transaction(tx.type, tx.sana, tx.egasi, tx.tur, tx.tolov,
                                               usd_val, uzs_val, now_t, tx.note)
        st = {'egasi':tx.egasi,'tur':tx.tur,'tolov':tx.tolov,'valyuta':tx.valyuta,'summa':tx.summa,'note':tx.note}
        _mirror_task(f'api_transaction:{tx.type}', _sheets_save_row, tx.type, st, tx.sana, now_t)
        return {'success':True,'id':new_id,'message':'Saqlandi'}
    except Exception as e: raise HTTPException(500, str(e))

@api.put('/transaction/{tx_id}')
async def update_transaction_api(tx_id: str, data: UpdateTransaction):
    try:
        old = await store.transaction_get(tx_id)
        if not old: raise HTTPException(404, "Tranzaksiya topilmadi")
        sheet_name = old['type']
        patch, patch_disp = {}, {}
        if data.sana is not None:
            try: patch['date'] = datetime.strptime(data.sana, '%d.%m.%Y').strftime('%Y-%m-%d')
            except Exception: patch['date'] = data.sana
            patch_disp['sana'] = data.sana
        if data.egasi is not None:  patch['owner'] = patch_disp['egasi'] = data.egasi
        if data.tur is not None:    patch['category'] = patch_disp['tur'] = data.tur
        if data.tolov is not None:  patch['payment_method'] = patch_disp['tolov'] = data.tolov
        if data.note is not None:   patch['note'] = patch_disp['note'] = data.note
        if data.summa is not None and data.summa > 0:
            if data.valyuta == 'USD':
                patch['amount_usd'], patch['amount_uzs'] = data.summa, None
                patch_disp['usd'], patch_disp['uzs'] = data.summa, ''
            elif data.valyuta == 'UZS':
                patch['amount_uzs'], patch['amount_usd'] = data.summa, None
                patch_disp['uzs'], patch_disp['usd'] = data.summa, ''
        if not patch:
            return {'success':True,'message':"O'zgarish yo'q"}
        await store.update_transaction(tx_id, patch)
        _mirror_task('api_update_transaction', _sheets_update_transaction, sheet_name, old, patch_disp)
        return {'success':True,'message':'Yangilandi'}
    except HTTPException: raise
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
async def qarz_list_api():
    try:
        return {'success':True,'data':await store.qarz_active()}
    except Exception as e: raise HTTPException(500, str(e))

@api.post('/qarz/add')
async def qarz_add_api(q: QarzModel):
    try:
        today = datetime.now(TZ).strftime('%d.%m.%Y')
        sana  = q.sana or today
        qarz_id = await store.qarz_add(q.tur, q.kim, q.summa_uzs or None, q.summa_usd or None,
                                        sana, q.muddat, q.note or '')
        _mirror_task('qarz_append', _sheets_qarz_append,
                     {'tur':q.tur,'kim':q.kim,'summa_uzs':q.summa_uzs or '','summa_usd':q.summa_usd or '',
                      'muddat':q.muddat,'note':q.note or ''}, sana)

        egasi = 'FERUDIN'
        if q.tur == 'BERILGAN':
            await _record_qarz_transaction('CHIQIM', egasi, q.summa_usd, q.summa_uzs,
                f'Qarz berildi: {q.kim}', 'QARZ BERILDI')
        else:
            await _record_qarz_transaction('KIRIM', egasi, q.summa_usd, q.summa_uzs,
                f'Qarz olindi: {q.kim}', 'QARZ OLINDI')

        return {'success': True, 'id': qarz_id}
    except Exception as e:
        raise HTTPException(500, str(e))

@api.post('/qarz/close/{qarz_id}')
async def qarz_close_api(qarz_id: str):
    try:
        today = today_str()
        r = await store.qarz_get(qarz_id)
        if not r: raise HTTPException(404, "Qarz topilmadi")
        tur, kim  = r['tur'], r['kim']
        summa_uzs = r.get('summa_uzs')
        summa_usd = r.get('summa_usd')

        await store.qarz_close(qarz_id, today)

        egasi = 'FERUDIN'
        if tur == 'BERILGAN':
            await _record_qarz_transaction('KIRIM', egasi, summa_usd, summa_uzs,
                f'Qarz qaytdi: {kim}', 'QARZ QAYTIB KELDI')
            effect = 'balance_plus'
        else:
            await _record_qarz_transaction('CHIQIM', egasi, summa_usd, summa_uzs,
                f'Qarz qaytarildi: {kim}', 'QARZ QAYTARILDI')
            effect = 'balance_minus'

        bal_usd, bal_uzs = await get_balance()
        return {'success': True, 'effect': effect, 'balance_usd': bal_usd, 'balance_uzs': bal_uzs, 'kim': kim}
    except HTTPException: raise
    except Exception as e:
        raise HTTPException(500, str(e))

# ── TASKS API ────────────────────────────────────────────
class TaskModel(BaseModel):
    matn:    str
    vaqt:    str
    egasi:   str = 'FERUDIN'
    chat_id: str = ''

@api.get('/tasks')
async def get_tasks_api(status: str = 'FAOL'):
    try:
        now = datetime.now(TZ)
        result = []
        for t in await store.tasks_list(status):
            overdue = False
            try:
                dt = TZ.localize(datetime.strptime(t['vaqt'], '%d.%m.%Y %H:%M'))
                overdue = dt < now and t['holat'] == 'FAOL'
            except Exception: pass
            result.append({**t, 'overdue': overdue})
        result.sort(key=lambda x: x['vaqt'])
        return {'tasks':result,'count':len(result)}
    except Exception as e: raise HTTPException(500, str(e))

@api.post('/tasks')
async def add_task_api(task: TaskModel):
    try:
        today   = datetime.now(TZ).strftime('%d.%m.%Y')
        chat_id = task.chat_id or str(CHAT_1)
        new_id  = await store.task_add(task.matn, task.vaqt, task.egasi, chat_id, today)
        _mirror_task('api_add_task', _sheets_task_append, task.matn, task.vaqt, task.egasi, chat_id, today)
        return {'success':True,'id':new_id}
    except Exception as e: raise HTTPException(500, str(e))

@api.post('/tasks/done/{task_id}')
async def task_done_api(task_id: str):
    try:
        t = await store.task_get(task_id)
        await store.task_mark(task_id, 'BAJARILDI')
        if t:
            _mirror_task('api_task_done', _sheets_task_mark, t['matn'], t['egasi'], 'BAJARILDI')
        return {'success':True}
    except Exception as e: raise HTTPException(500, str(e))

# ── MEMORY API ───────────────────────────────────────────
class MemoryModel(BaseModel):
    kalit:  str
    qiymat: str
    kim:    str = 'FERUDIN'

@api.get('/memory')
async def get_memory_api(q: str = ''):
    try:
        memories = await store.memory_search(q)
        return {'memories':memories,'count':len(memories)}
    except Exception as e: raise HTTPException(500, str(e))

@api.post('/memory')
async def save_memory_api(mem: MemoryModel):
    try:
        today  = today_str()
        action = await store.memory_save(mem.kalit, mem.qiymat, mem.kim, today)
        _mirror_task('api_memory_save', _sheets_memory_save, mem.kalit, mem.qiymat, mem.kim, today)
        return {'success':True,'action':'updated' if action=='yangilandi' else 'created'}
    except Exception as e: raise HTTPException(500, str(e))

# ── NAMOZ API ────────────────────────────────────────────
@api.get('/namoz/stats')
async def namoz_stats_api():
    try:
        stats = await store.namoz_weekly_stats(datetime.now(TZ).date())
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

# ── AI CHAT ─────────────────────────────────────────────
@api.post('/ai/chat')
async def ai_chat(payload: dict):
    try:
        anthropic_key = os.environ.get('ANTHROPIC_API_KEY')
        if not anthropic_key:
            return {'reply': '❌ ANTHROPIC_API_KEY sozlanmagan'}

        import anthropic as _anthropic
        client = _anthropic.Anthropic(api_key=anthropic_key)

        message = payload.get('message', '')
        user = payload.get('user', 'FERUDIN')

        if not message:
            return {'reply': '❌ Xabar bo\'sh'}

        response = client.messages.create(
            model='claude-haiku-4-5-20251001',
            max_tokens=1000,
            system=f"""Sen AFG Family Bot ning AI assistantisin.
Foydalanuvchi: {user}.
Bu oilaviy hisob-kitob va shaxsiy agent tizimi.
O'zbek, Rus va Ingliz tillarida gaplasha olasan.
Qisqa, aniq va foydali javoblar ber.
Hisob-kitob, vazifalar, eslatmalar, umumiy savollar — hamma narsada yordam ber.""",
            messages=[{'role': 'user', 'content': message}]
        )

        reply = response.content[0].text
        return {'reply': reply}

    except Exception as e:
        logger.error('AI chat xato: %s', e)
        return {'reply': f'❌ Xatolik: {str(e)[:100]}'}

# ── BNB MODELS ──────────────────────────────────────────
class GuestModel(BaseModel):
    name: str
    nationality: str
    dob: str
    passportId: str

class BnBRegistration(BaseModel):
    chat_id: str
    apartment: str
    guests: List[GuestModel]
    uzbekEntryDate: Optional[str] = ""
    aptEntryDate: Optional[str] = ""
    departureDate: Optional[str] = ""
    regStartDate: Optional[str] = ""
    regEndDate: Optional[str] = ""
    room: Optional[str] = ""
    paymentAmount: Optional[str] = ""
    paymentBy: Optional[str] = "guest"

class SendFileRequest(BaseModel):
    chat_id: str
    file_type: str  # "ish" | "cad" | "ferudin"
    apartment: Optional[str] = "28"

# ── BNB ENDPOINTS ────────────────────────────────────────
@api.post('/bnb/register')
async def bnb_register(reg: BnBRegistration):
    try:
        from bnb_services import (get_drive_file, tg_send_file,
                                   save_bnb_to_sheets, APT_ISH, APT_CAD, FERUDIN_PDF_ID)
        from generate_doc import generate_ariza_doc
        import asyncio as _asyncio
        import requests as _http

        reg_dict = reg.dict()
        chat_id = reg.chat_id
        apt = reg.apartment
        tok = os.environ.get("BOT_TOKEN", "")
        _http.post(f"https://api.telegram.org/bot{tok}/sendMessage",
            json={"chat_id": chat_id, "text": "📋 Hujjatlar tayyorlanmoqda..."},
            timeout=10)

        ariza_bytes = generate_ariza_doc(reg_dict)
        guest_name = (reg.guests[0].name if reg.guests else "mehmon").replace(" ", "_")[:20]
        tg_send_file(chat_id, ariza_bytes, f"Ariza_{apt}_{guest_name}.docx",
            f"1/4 - {apt}-xona ARIZA hujjati")

        ish_id = APT_ISH.get(apt, "")
        if ish_id:
            data = await _asyncio.to_thread(get_drive_file, ish_id)
            tg_send_file(chat_id, data, f"Ishonchnoma_{apt}.pdf",
                f"2/4 - {apt}-xona Ishonchnoma")

        cad_id = APT_CAD.get(apt, "")
        if cad_id:
            data = await _asyncio.to_thread(get_drive_file, cad_id)
            tg_send_file(chat_id, data, f"Kadastr_{apt}.pdf",
                f"3/4 - {apt}-xona Kadastr")

        if FERUDIN_PDF_ID:
            data = await _asyncio.to_thread(get_drive_file, FERUDIN_PDF_ID)
            tg_send_file(chat_id, data, "identification_card_FERUDIN.pdf",
                "4/4 - identification card FERUDIN")

        await store.bnb_save(reg_dict)
        _mirror_task('bnb_save', save_bnb_to_sheets, reg_dict)
        return {"success": True, "message": "Ro'yxatga olindi, hujjatlar yuborildi"}
    except Exception as e:
        raise HTTPException(500, str(e))

@api.post('/bnb/send-file')
async def bnb_send_file(req: SendFileRequest):
    try:
        from bnb_services import (get_drive_file, tg_send_file,
                                   APT_ISH, APT_CAD, FERUDIN_PDF_ID)
        import asyncio as _asyncio

        ft = req.file_type
        apt = req.apartment
        chat_id = req.chat_id

        if ft == "ferudin":
            file_id = FERUDIN_PDF_ID
            filename = "identification_card_FERUDIN.pdf"
            caption = "Ferudin ID Card"
        elif ft == "ish":
            file_id = APT_ISH.get(apt, "")
            filename = f"Ishonchnoma_{apt}.pdf"
            caption = f"{apt}-xona Ishonchnoma"
        elif ft == "cad":
            file_id = APT_CAD.get(apt, "")
            filename = f"Kadastr_{apt}.pdf"
            caption = f"{apt}-xona Kadastr"
        else:
            raise HTTPException(400, "Noto'g'ri file_type")

        if not file_id:
            raise HTTPException(404, f"{apt} uchun fayl ID topilmadi")

        file_bytes = await _asyncio.to_thread(get_drive_file, file_id)
        tg_send_file(chat_id, file_bytes, filename, caption)
        return {"success": True}
    except HTTPException: raise
    except Exception as e:
        raise HTTPException(500, str(e))

@api.get('/bnb/history')
async def bnb_history():
    try:
        return {"success": True, "data": await store.bnb_history()}
    except Exception as e:
        raise HTTPException(500, str(e))

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
