#!/usr/bin/env python3
import os, json, logging
from datetime import datetime, time as dtime
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

TOKEN          = os.environ['BOT_TOKEN']
CHAT_1         = os.environ['CHAT_1']
CHAT_2         = os.environ['CHAT_2']
SPREADSHEET_ID = os.environ['SPREADSHEET_ID']
CREDS_JSON     = os.environ['GOOGLE_CREDS_JSON']
TZ             = pytz.timezone('Asia/Tashkent')
ALLOWED        = {CHAT_1, CHAT_2}

TUR, EGASI, TOLOV, VALYUTA, SUMMA, NOTE = range(6)

def get_ss():
    info  = json.loads(CREDS_JSON)
    creds = Credentials.from_service_account_info(info, scopes=[
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive'
    ])
    return gspread.authorize(creds).open_by_key(SPREADSHEET_ID)

async def delete_messages(bot, chat_id, msg_ids):
    """Berilgan message_id larni o'chirish"""
    for mid in msg_ids:
        try:
            await bot.delete_message(chat_id=chat_id, message_id=mid)
        except Exception:
            pass

def clean_num(val):
    if not val: return None
    try:
        s = str(val)
        # Barcha keraksiz belgilarni tozalash
        s = s.replace('$','').replace(' ',' ')
        s = s.replace(' ',' ').replace(' ',' ')
        s = s.replace("so'm",'').replace('UZS','').replace("'","")
        # Bo'shliqlarni o'chirish
        s = s.strip().replace(' ','')
        # Vergulni nuqtaga almashtirish (agar oxirgi 3 raqamdan oldin)
        if ',' in s and '.' not in s:
            s = s.replace(',', '.')
        elif ',' in s and '.' in s:
            # "6.970,60" format — nuqta minglik, vergul kasr
            s = s.replace('.','').replace(',','.')
        return float(s)
    except: return None

def get_balance():
    ss = get_ss()
    # 1. KUNLIK_VIEW E2
    try:
        v = clean_num(ss.worksheet('KUNLIK_VIEW').acell('E2').value)
        if v is not None and v != 0: return v
    except Exception as e: logger.error(f'bal kunlik_view: {e}')
    # 2. DASHBOARD B2
    try:
        v = clean_num(ss.worksheet('DASHBOARD').acell('B2').value)
        if v is not None and v != 0: return v
    except Exception as e: logger.error(f'bal dashboard: {e}')
    # 3. DASHBOARD B3
    try:
        v = clean_num(ss.worksheet('DASHBOARD').acell('B3').value)
        if v is not None and v != 0: return v
    except Exception as e: logger.error(f'bal dashboard b3: {e}')
    logger.warning('Balance topilmadi, 0 qaytarildi')
    return 0.0

def save_row(sheet_name, st):
    sh      = get_ss().worksheet(sheet_name)
    today   = datetime.now(TZ).strftime('%d.%m.%Y')
    usd_val = float(st['summa']) if st['valyuta'] == 'USD' else ''
    uzs_val = float(st['summa']) if st['valyuta'] == 'UZS' else ''
    # C ustunidan oxirgi to'liq qatorni topish
    col_c = sh.col_values(3)
    last  = 2
    for i, v in enumerate(col_c):
        if i < 2: continue
        if v and v.strip(): last = i + 1
    new_row = last + 1
    row_num = new_row - 2
    # B:H ga yozish (I ustuni formula — o'tkazib yuboriladi)
    sh.update(f'B{new_row}:H{new_row}', [[
        row_num, today, st['egasi'], st['tur'],
        st['tolov'], usd_val, uzs_val
    ]], value_input_option='USER_ENTERED')
    # J ga note
    sh.update(f'J{new_row}', [[st.get('note', '')]])

    logger.info(f'Saved to {sheet_name} row {new_row}')
    return new_row

def parse_num(s):
    try:
        s = str(s).strip().replace(' ','').replace(',','.').replace('$','').replace("so'm",'')
        return float(s) if s else 0.0
    except: return 0.0

def get_bugun():
    today = today_str()
    ss    = get_ss()
    r     = dict(ch=[], ki=[], chU=0.0, chZ=0.0, kiU=0.0, kiZ=0.0)

    for sname, target in [('CHIQIM','ch'),('KIRIM','ki')]:
        try:
            sh    = ss.worksheet(sname)
            dates = sh.col_values(3)  # C ustun — sana (1-based)
            turs  = sh.col_values(5)  # E ustun — tur
            usds  = sh.col_values(7)  # G ustun — USD
            uzss  = sh.col_values(8)  # H ustun — UZS
            n = max(len(dates), len(turs))
            logger.info(f'{sname}: {n} rows, today={today}')
            for i in range(2, n):  # 0=row1(bo'sh), 1=row2(header), 2+=data
                d = str(dates[i]).strip() if i < len(dates) else ''
                if not d: continue
                if norm_date(d) != today:
                    logger.info(f'{sname} row{i+1}: date={repr(d)} norm={norm_date(d)} != {today}')
                    continue
                tur = str(turs[i]).strip() if i < len(turs) else ''
                if not tur: continue
                u = parse_num(usds[i] if i < len(usds) else '')
                z = parse_num(uzss[i] if i < len(uzss) else '')
                logger.info(f'{sname} row{i+1}: tur={tur} u={u} z={z}')
                item = {'tur':tur,'usd':u,'uzs':z}
                if target == 'ch':
                    r['ch'].append(item); r['chU']+=u; r['chZ']+=z
                else:
                    r['ki'].append(item); r['kiU']+=u; r['kiZ']+=z
        except Exception as e:
            logger.error(f'get_bugun {sname}: {e}')
    return r
def fmt(n):
    try: return f"{int(round(float(n))):,}".replace(',', ' ')
    except: return '0'

def sstr(u, z):
    p = []
    if u and float(u) > 0: p.append(f"{int(round(float(u)))}$")
    if z and float(z) > 0: p.append(f"{fmt(z)} so'm")
    return ' + '.join(p) if p else '0'

def today_str():
    return datetime.now(TZ).strftime('%d.%m.%Y')

def norm_date(s):
    """Har qanday formatdagi sanani DD.MM.YYYY ga o'tkazadi"""
    s = str(s).strip()
    if not s: return ''
    # DD.MM.YYYY - to'g'ri format
    if len(s) == 10 and s[2] == '.' and s[5] == '.':
        return s
    # Boshqa formatlarni parse qilish
    for fmt in ['%d/%m/%Y','%m/%d/%Y','%Y-%m-%d','%d-%m-%Y',
                '%d.%m.%y','%m/%d/%y']:
        try:
            from datetime import datetime as dt2
            return dt2.strptime(s, fmt).strftime('%d.%m.%Y')
        except: pass
    # Faqat raqam (date serial) bo'lsa
    try:
        n = float(s)
        from datetime import datetime as dt2, timedelta
        base = dt2(1899, 12, 30)
        return (base + timedelta(days=int(n))).strftime('%d.%m.%Y')
    except: pass
    return s

def smstr(st):
    if st['valyuta'] == 'USD': return f"{int(round(float(st['summa'])))}$"
    return f"{fmt(st['summa'])} so'm"

def confirm_text(st, bal=None):
    lbl = 'CHIQIM' if st['type'] == 'CHIQIM' else 'KIRIM'
    ico = '📤' if st['type'] == 'CHIQIM' else '📥'
    bal_str = f"{int(round(float(bal)))}$" if bal is not None else '—'
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

# ── KLAVIATURALAR ─────────────────────────────────────────
def kb_main():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('📤 CHIQIM', callback_data='MC'),
         InlineKeyboardButton('📥 KIRIM',  callback_data='MK')],
        [InlineKeyboardButton('💰 BALANS',  callback_data='MB'),
         InlineKeyboardButton('📅 BUGUN',   callback_data='MG')],
        [InlineKeyboardButton('📊 STATISTIKA', callback_data='MS')]
    ])

def kb_chiqim():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('🛒 Oziq ovqat',  callback_data='C|OZIQ OVQAT'),
         InlineKeyboardButton('⛽ Benzin',       callback_data='C|BENZIN')],
        [InlineKeyboardButton('💳 Rassrochka',  callback_data='C|RASSROCHKA'),
         InlineKeyboardButton('👗 Kiyim kechak',callback_data='C|KIYIM KECHAK')],
        [InlineKeyboardButton('👨 Xurshidga',   callback_data='C|XURSHIDGA'),
         InlineKeyboardButton('🏢 Ishxonamga',  callback_data='C|ISHXONAMGA')],
        [InlineKeyboardButton('🏠 Uydagilarga', callback_data='C|UYDAGILARGA'),
         InlineKeyboardButton('🚫 Shtraflar',   callback_data='C|SHTRAFLAR')],
        [InlineKeyboardButton('🛍 Shopping',    callback_data='C|SHOPPPING'),
         InlineKeyboardButton('📋 Ishxona reg', callback_data='C|ISHXONA REG')],
        [InlineKeyboardButton('✂️ Sartarosh',   callback_data='C|SARTAROSH'),
         InlineKeyboardButton('💡 Boshqa',      callback_data='C|BOSHQA')],
        [InlineKeyboardButton('🔙 Orqaga',      callback_data='BACK')]
    ])

def kb_kirim():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('🏢 Ishxona',   callback_data='K|ISHXONA'),
         InlineKeyboardButton('🌱 Seedbee',   callback_data='K|SEEDBEE')],
        [InlineKeyboardButton('💼 Business',  callback_data='K|BUSINESS'),
         InlineKeyboardButton('🏠 Uydagilar', callback_data='K|UYDAGILAR')],
        [InlineKeyboardButton('💡 Boshqa',    callback_data='K|BOSHQA')],
        [InlineKeyboardButton('🔙 Orqaga',    callback_data='BACK')]
    ])

kb_egasi   = lambda: InlineKeyboardMarkup([[InlineKeyboardButton('👨 Ferudin',callback_data='E|FERUDIN'),InlineKeyboardButton('👩 Guloyim',callback_data='E|GULOYIM')]])
kb_tolov   = lambda: InlineKeyboardMarkup([[InlineKeyboardButton('💵 Cash',callback_data='T|CASH'),InlineKeyboardButton('💳 Card',callback_data='T|CARD'),InlineKeyboardButton('📌 Other',callback_data='T|OTHER')]])
kb_valyuta = lambda: InlineKeyboardMarkup([[InlineKeyboardButton('💵 USD ($)',callback_data='V|USD'),InlineKeyboardButton("🇺🇿 UZS (so'm)",callback_data='V|UZS')]])
kb_note    = lambda: InlineKeyboardMarkup([[InlineKeyboardButton('✅ Done — note kerak emas',callback_data='SKIP')]])

def ok(update): return str(update.effective_chat.id) in ALLOWED

async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ok(update): return
    ctx.user_data.clear()
    await update.message.reply_text(
        '👋 <b>FAMILY ACCOUNTING</b>\n\nNima qilmoqchisiz?',
        parse_mode='HTML', reply_markup=kb_main()
    )
    return ConversationHandler.END

async def debug(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ok(update): return
    try:
        ss = get_ss()
        # CHIQIM dan birinchi 5 qatorni o'qish
        rows = ss.worksheet('CHIQIM').get_all_values()
        lines = [f"CHIQIM jami qator: {len(rows)}"]
        for i, row in enumerate(rows[2:7]):
            if any(row):
                lines.append(f"Qator {i+3}: C='{row[2] if len(row)>2 else '?'}' E='{row[4] if len(row)>4 else '?'}'")
        lines.append(f"\nBugun: '{today_str()}'")
        await update.message.reply_text('\n'.join(lines))
    except Exception as e:
        await update.message.reply_text(f'Xato: {e}')

async def btn(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if not ok(update): return
    d = q.data; ud = ctx.user_data

    if d == 'BACK':
        ud.clear()
        await q.message.reply_text('👋 <b>FAMILY ACCOUNTING</b>', parse_mode='HTML', reply_markup=kb_main())
        return ConversationHandler.END

    if d == 'MC':
        ud.clear(); ud['type'] = 'CHIQIM'; ud['msgs'] = []
        m = await q.message.reply_text('📤 <b>CHIQIM</b>\n\nXarajat turini tanlang:', parse_mode='HTML', reply_markup=kb_chiqim())
        ud['msgs'].append(m.message_id)
        return TUR

    if d == 'MK':
        ud.clear(); ud['type'] = 'KIRIM'; ud['msgs'] = []
        m = await q.message.reply_text('📥 <b>KIRIM</b>\n\nKirim turini tanlang:', parse_mode='HTML', reply_markup=kb_kirim())
        ud['msgs'].append(m.message_id)
        return TUR

    if d == 'MB':
        await q.message.reply_text('⏳ Balans tekshirilmoqda...', parse_mode='HTML')
        bal = get_balance()
        await q.message.reply_text(f'💰 <b>Joriy balans: {int(round(bal))}$</b>', parse_mode='HTML', reply_markup=kb_main())
        return ConversationHandler.END

    if d == 'MG':
        await q.message.reply_text('⏳ Ma\'lumotlar yuklanmoqda...')
        dv  = get_bugun()
        txt = f'📅 <b>{today_str()}</b>\n\n<b>📤 Chiqimlar:</b>\n'
        txt += ('\n'.join(f'  • {c["tur"]}: {sstr(c["usd"],c["uzs"])}' for c in dv['ch'])) or "  Yo'q"
        txt += f'\n\n<b>📥 Kirimlar:</b>\n'
        txt += ('\n'.join(f'  • {k["tur"]}: {sstr(k["usd"],k["uzs"])}' for k in dv['ki'])) or "  Yo'q"
        await q.message.reply_text(txt, parse_mode='HTML', reply_markup=kb_main())
        return ConversationHandler.END

    if d == 'MS':
        await q.message.reply_text('⏳ Statistika yuklanmoqda...')
        dv  = get_bugun(); bal = get_balance()
        txt = (f'📊 <b>Statistika</b>\n\n'
               f'💰 Balans: <b>{int(round(bal))}$</b>\n'
               f'Bugungi chiqim: <b>{sstr(dv["chU"],dv["chZ"])}</b>\n'
               f'Bugungi kirim:  <b>{sstr(dv["kiU"],dv["kiZ"])}</b>')
        await q.message.reply_text(txt, parse_mode='HTML', reply_markup=kb_main())
        return ConversationHandler.END

    if d.startswith('C|'):
        ud['tur'] = d[2:]
        m = await q.message.reply_text(f'📤 <b>{ud["tur"]}</b>\n\nKim sarfladi?', parse_mode='HTML', reply_markup=kb_egasi())
        ud.setdefault('msgs',[]).append(m.message_id)
        return EGASI

    if d.startswith('K|'):
        ud['tur'] = d[2:]
        m = await q.message.reply_text(f'📥 <b>{ud["tur"]}</b>\n\nKimning kirimi?', parse_mode='HTML', reply_markup=kb_egasi())
        ud.setdefault('msgs',[]).append(m.message_id)
        return EGASI

    if d.startswith('E|'):
        ud['egasi'] = d[2:]
        m = await q.message.reply_text(f'👤 <b>{ud["egasi"]}</b>\n\nTo\'lov turi?', parse_mode='HTML', reply_markup=kb_tolov())
        ud.setdefault('msgs',[]).append(m.message_id)
        return TOLOV

    if d.startswith('T|'):
        ud['tolov'] = d[2:]
        m = await q.message.reply_text(f'💳 <b>{ud["tolov"]}</b>\n\nValyuta:', parse_mode='HTML', reply_markup=kb_valyuta())
        ud.setdefault('msgs',[]).append(m.message_id)
        return VALYUTA

    if d.startswith('V|'):
        ud['valyuta'] = d[2:]
        hint = 'Masalan: 150' if ud['valyuta'] == 'USD' else 'Masalan: 350000'
        m = await q.message.reply_text(f'💱 <b>{ud["valyuta"]}</b>\n\nSummani yozing:\n<i>{hint}</i>', parse_mode='HTML')
        ud.setdefault('msgs',[]).append(m.message_id)
        return SUMMA

    if d == 'SKIP':
        ud['note'] = ''
        ud.setdefault('msgs', []).append(q.message.message_id)
        await _finalize(q.message, ctx)
        return ConversationHandler.END

    return ConversationHandler.END

async def get_summa(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ok(update): return
    txt = update.message.text.strip().replace(' ','').replace(',','.')
    try:
        num = float(txt); assert num > 0
    except:
        await update.message.reply_text('❌ Raqam kiriting.\n<i>Masalan: 150 yoki 350000</i>', parse_mode='HTML')
        return SUMMA
    ctx.user_data['summa'] = num
    await update.message.reply_text(
        f'✅ Summa: <b>{smstr(ctx.user_data)}</b>\n\nNote yozing yoki o\'tkazib yuboring:',
        parse_mode='HTML', reply_markup=kb_note()
    )
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
            await message.reply_text('Qaytadan boshlang:', reply_markup=kb_main())
            return
        m_wait = await message.reply_text('⏳ Saqlanmoqda...')
        save_row(st['type'], st)
        bal  = get_balance()
        txt  = confirm_text(st, bal)
        msgs = list(st.get('msgs', []))
        msgs.append(m_wait.message_id)
        ctx.user_data.clear()
        # Yakuniy xabar
        await message.reply_text(txt, parse_mode='HTML', reply_markup=kb_main())
        # Oraliq xabarlarni o'chirish — bot va chat_id ni message dan olamiz
        try:
            bot     = ctx.application.bot
            chat_id = message.chat_id
            await delete_messages(bot, chat_id, msgs)
        except Exception as de:
            logger.error(f'delete msgs: {de}')
    except Exception as e:
        logger.error(f'finalize: {e}')
        ctx.user_data.clear()
        await message.reply_text(f'❌ Xato: {e}', reply_markup=kb_main())

async def daily_report(ctx: ContextTypes.DEFAULT_TYPE):
    try:
        dv  = get_bugun(); bal = get_balance()
        txt = f'📊 <b>{today_str()} — Kunlik hisobot</b>\n\n<b>📤 Chiqimlar:</b>\n'
        txt += ('\n'.join(f'  • {c["tur"]}: {sstr(c["usd"],c["uzs"])}' for c in dv['ch'])) or "  Yo'q"
        txt += '\n\n<b>📥 Kirimlar:</b>\n'
        txt += ('\n'.join(f'  • {k["tur"]}: {sstr(k["usd"],k["uzs"])}' for k in dv['ki'])) or "  Yo'q"
        txt += (f'\n\n▪️ Jami chiqim: <b>{sstr(dv["chU"],dv["chZ"])}</b>'
                f'\n▪️ Jami kirim:  <b>{sstr(dv["kiU"],dv["kiZ"])}</b>'
                f'\n\n💰 <b>BALANCE: {int(round(bal))}$</b>')
        for cid in [CHAT_1, CHAT_2]:
            try: await ctx.bot.send_message(chat_id=cid, text=txt, parse_mode='HTML')
            except Exception as e: logger.error(f'daily {cid}: {e}')
    except Exception as e:
        logger.error(f'daily_report: {e}')

def main():
    app = Application.builder().token(TOKEN).read_timeout(30).write_timeout(30).connect_timeout(30).build()
    conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(btn)],
        states={
            TUR:    [CallbackQueryHandler(btn)],
            EGASI:  [CallbackQueryHandler(btn)],
            TOLOV:  [CallbackQueryHandler(btn)],
            VALYUTA:[CallbackQueryHandler(btn)],
            SUMMA:  [MessageHandler(filters.TEXT & ~filters.COMMAND, get_summa),
                     CallbackQueryHandler(btn)],
            NOTE:   [MessageHandler(filters.TEXT & ~filters.COMMAND, get_note),
                     CallbackQueryHandler(btn)],
        },
        fallbacks=[CommandHandler('start', start), CommandHandler('menu', start)],
        per_message=False
    )
    app.add_handler(CommandHandler('start', start))
    app.add_handler(CommandHandler('menu',  start))
    app.add_handler(CommandHandler('debug', debug))
    app.add_handler(conv)
    app.job_queue.run_daily(daily_report, time=dtime(hour=18, minute=50, tzinfo=pytz.utc))
    logger.info('Bot ishga tushdi!')
    app.run_polling(drop_pending_updates=True)


# ── FASTAPI ────────────────────────────────────────────────
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI(title='Family Accounting API')

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_methods=['*'],
    allow_headers=['*'],
)

# ── GOOGLE SHEETS ──────────────────────────────────────────
def fmt_num(s):
    try:
        return float(str(s).replace(' ','').replace(',','.').replace('$','').replace("so'm",''))
    except: return 0.0

# ── MODELS ─────────────────────────────────────────────────
class Transaction(BaseModel):
    type:    str  # CHIQIM | KIRIM
    sana:    str  # DD.MM.YYYY
    egasi:   str  # FERUDIN | GULOYIM
    tur:     str  # xarajat/kirim turi
    tolov:   str  # CASH | CARD | OTHER
    valyuta: str  # USD | UZS
    summa:   float
    note:    str = ''

class UpdateTransaction(BaseModel):
    egasi:   str = ''
    tur:     str = ''
    tolov:   str = ''
    valyuta: str = ''
    summa:   float = 0
    note:    str = ''

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
        # KUNLIK_VIEW E2 dan olish (Telegram bot kabi)
        try:
            val = ss.worksheet('KUNLIK_VIEW').acell('E2').value
            if val:
                bal = fmt_num(val)
                if bal > 0:
                    return {'balance': bal, 'formatted': f'{int(round(bal))}$'}
        except: pass
        # DASHBOARD B2 dan olish
        try:
            val = ss.worksheet('DASHBOARD').acell('B2').value
            if val:
                bal = fmt_num(val)
                if bal > 0:
                    return {'balance': bal, 'formatted': f'{int(round(bal))}$'}
        except: pass
        return {'balance': 0, 'formatted': '0$'}
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

def run_api():
    import asyncio, uvicorn
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    port = int(os.environ.get('PORT', 8000))
    config = uvicorn.Config(app, host='0.0.0.0', port=port, loop='none')
    server = uvicorn.Server(config)
    loop.run_until_complete(server.serve())

if __name__ == '__main__':
    import threading
    port = int(os.environ.get('PORT', 8000))
    logger.info(f'Starting API on port {port}')
    api_thread = threading.Thread(target=run_api, daemon=True)
    api_thread.start()
    main()
