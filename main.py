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

def num_clean(s):
    try:
        s = str(s)
        for ch in ['$', '\xa0', '\u202f', '\u00a0', ' ', "'", '"']:
            s = s.replace(ch, '')
        s = s.replace("so'm", '').replace('UZS', '').strip()
        if ',' in s and '.' not in s:
            s = s.replace(',', '.')
        elif ',' in s and '.' in s:
            s = s.replace('.', '').replace(',', '.')
        return float(s) if s else 0.0
    except:
        return 0.0

def get_balance():
    try:
        ss = get_ss()
        for sheet, cell in [('KUNLIK_VIEW', 'E2'), ('DASHBOARD', 'B2')]:
            try:
                raw = ss.worksheet(sheet).acell(cell).value
                if not raw: continue
                v = num_clean(raw)
                if v > 0: return v
            except Exception as e:
                logger.error(f'bal {sheet}: {e}')
        return 0.0
    except Exception as e:
        logger.error(f'get_balance: {e}')
        return 0.0

def save_row(sheet_name, st):
    sh      = get_ss().worksheet(sheet_name)
    today   = datetime.now(TZ).strftime('%d.%m.%Y')
    usd_val = float(st['summa']) if st['valyuta'] == 'USD' else ''
    uzs_val = float(st['summa']) if st['valyuta'] == 'UZS' else ''
    col_c   = sh.col_values(3)
    last    = 2
    for i, v in enumerate(col_c):
        if i < 2: continue
        if v and str(v).strip(): last = i + 1
    new_row = last + 1
    row_num = new_row - 2
    sh.update(f'B{new_row}:H{new_row}', [[
        row_num, today, st['egasi'], st['tur'],
        st['tolov'], usd_val, uzs_val
    ]], value_input_option='USER_ENTERED')
    sh.update(f'J{new_row}', [[st.get('note', '')]])
    logger.info(f'Saved to {sheet_name} row {new_row}')
    return new_row

def norm_date(s):
    s = str(s).strip()
    if not s: return ''
    if len(s) == 10 and s[2] == '.' and s[5] == '.': return s
    for fmt in ['%d/%m/%Y', '%m/%d/%Y', '%Y-%m-%d', '%d-%m-%Y', '%d.%m.%y', '%m/%d/%y']:
        try:
            from datetime import datetime as dt2
            return dt2.strptime(s, fmt).strftime('%d.%m.%Y')
        except: pass
    try:
        from datetime import datetime as dt2, timedelta
        return (dt2(1899, 12, 30) + timedelta(days=int(float(s)))).strftime('%d.%m.%Y')
    except: pass
    return s

def today_str():
    return datetime.now(TZ).strftime('%d.%m.%Y')

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
    bal_str = f"{round(float(bal), 2)}$" if bal is not None else '—'
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
    for sname, target in [('CHIQIM', 'ch'), ('KIRIM', 'ki')]:
        try:
            sh    = ss.worksheet(sname)
            dates = sh.col_values(3)
            turs  = sh.col_values(5)
            usds  = sh.col_values(7)
            uzss  = sh.col_values(8)
            n     = max(len(dates), len(turs))
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
        except Exception as e:
            logger.error(f'get_bugun {sname}: {e}')
    return r

async def delete_messages(bot, chat_id, msg_ids):
    for mid in msg_ids:
        try: await bot.delete_message(chat_id=chat_id, message_id=mid)
        except: pass

def kb_main():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('📤 CHIQIM', callback_data='MC'),
         InlineKeyboardButton('📥 KIRIM',  callback_data='MK')],
        [InlineKeyboardButton('💰 BALANS', callback_data='MB'),
         InlineKeyboardButton('📅 BUGUN',  callback_data='MG')],
        [InlineKeyboardButton('📊 STATISTIKA', callback_data='MS')]
    ])

def kb_chiqim():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('🛒 Oziq ovqat',  callback_data='C|OZIQ OVQAT'),
         InlineKeyboardButton('⛽ Benzin',        callback_data='C|BENZIN')],
        [InlineKeyboardButton('💳 Rassrochka',   callback_data='C|RASSROCHKA'),
         InlineKeyboardButton('👗 Kiyim kechak', callback_data='C|KIYIM KECHAK')],
        [InlineKeyboardButton('👨 Xurshidga',    callback_data='C|XURSHIDGA'),
         InlineKeyboardButton('🏢 Ishxonamga',   callback_data='C|ISHXONAMGA')],
        [InlineKeyboardButton('🏠 Uydagilarga',  callback_data='C|UYDAGILARGA'),
         InlineKeyboardButton('🚫 Shtraflar',    callback_data='C|SHTRAFLAR')],
        [InlineKeyboardButton('🛍 Shopping',     callback_data='C|SHOPPPING'),
         InlineKeyboardButton('📋 Ishxona reg',  callback_data='C|ISHXONA REG')],
        [InlineKeyboardButton('✂️ Sartarosh',    callback_data='C|SARTAROSH'),
         InlineKeyboardButton('💡 Boshqa',       callback_data='C|BOSHQA')],
        [InlineKeyboardButton('🔙 Orqaga',       callback_data='BACK')]
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

kb_egasi   = lambda: InlineKeyboardMarkup([[InlineKeyboardButton('👨 Ferudin', callback_data='E|FERUDIN'), InlineKeyboardButton('👩 Guloyim', callback_data='E|GULOYIM')]])
kb_tolov   = lambda: InlineKeyboardMarkup([[InlineKeyboardButton('💵 Cash', callback_data='T|CASH'), InlineKeyboardButton('💳 Card', callback_data='T|CARD'), InlineKeyboardButton('📌 Other', callback_data='T|OTHER')]])
kb_valyuta = lambda: InlineKeyboardMarkup([[InlineKeyboardButton('💵 USD ($)', callback_data='V|USD'), InlineKeyboardButton("🇺🇿 UZS (so'm)", callback_data='V|UZS')]])
kb_note    = lambda: InlineKeyboardMarkup([[InlineKeyboardButton('✅ Done — note kerak emas', callback_data='SKIP')]])

def ok(update): return str(update.effective_chat.id) in ALLOWED

async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ok(update): return
    ctx.user_data.clear()
    await update.message.reply_text('👋 <b>FAMILY ACCOUNTING</b>\n\nNima qilmoqchisiz?', parse_mode='HTML', reply_markup=kb_main())
    return ConversationHandler.END

async def debug(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ok(update): return
    try:
        ss  = get_ss()
        bal = get_balance()
        rows = ss.worksheet('CHIQIM').get_all_values()
        lines = [f"Balance: {bal}", f"CHIQIM: {len(rows)} qator"]
        for i, row in enumerate(rows[2:5]):
            if any(row):
                g = row[6] if len(row) > 6 else '?'
                h = row[7] if len(row) > 7 else '?'
                lines.append(f"R{i+3}: C={row[2]} E={row[4]} G={repr(g)} H={repr(h)}")
        await update.message.reply_text('\n'.join(lines))
    except Exception as e:
        await update.message.reply_text(f'Xato: {e}')

async def btn(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q  = update.callback_query
    await q.answer()
    if not ok(update): return
    d  = q.data
    ud = ctx.user_data

    if d == 'BACK':
        ud.clear()
        await q.message.reply_text('👋 <b>FAMILY ACCOUNTING</b>', parse_mode='HTML', reply_markup=kb_main())
        return ConversationHandler.END
    if d == 'MC':
        ud.clear(); ud['type'] = 'CHIQIM'; ud['msgs'] = []
        m = await q.message.reply_text('📤 <b>CHIQIM</b>\n\nXarajat turini tanlang:', parse_mode='HTML', reply_markup=kb_chiqim())
        ud['msgs'].append(m.message_id); return TUR
    if d == 'MK':
        ud.clear(); ud['type'] = 'KIRIM'; ud['msgs'] = []
        m = await q.message.reply_text('📥 <b>KIRIM</b>\n\nKirim turini tanlang:', parse_mode='HTML', reply_markup=kb_kirim())
        ud['msgs'].append(m.message_id); return TUR
    if d == 'MB':
        await q.message.reply_text('⏳ Balans tekshirilmoqda...')
        bal = get_balance()
        await q.message.reply_text(f'💰 <b>Joriy balans: {round(bal, 2)}$</b>', parse_mode='HTML', reply_markup=kb_main())
        return ConversationHandler.END
    if d == 'MG':
        await q.message.reply_text("⏳ Ma'lumotlar yuklanmoqda...")
        dv  = get_bugun()
        txt = f'📅 <b>{today_str()}</b>\n\n<b>📤 Chiqimlar:</b>\n'
        txt += ('\n'.join(f'  • {c["tur"]}: {sstr(c["usd"],c["uzs"])}' for c in dv['ch'])) or "  Yo'q"
        txt += '\n\n<b>📥 Kirimlar:</b>\n'
        txt += ('\n'.join(f'  • {k["tur"]}: {sstr(k["usd"],k["uzs"])}' for k in dv['ki'])) or "  Yo'q"
        await q.message.reply_text(txt, parse_mode='HTML', reply_markup=kb_main())
        return ConversationHandler.END
    if d == 'MS':
        await q.message.reply_text('⏳ Statistika yuklanmoqda...')
        dv  = get_bugun()
        bal = get_balance()
        txt = (f'📊 <b>Statistika</b>\n\n'
               f'💰 Balans: <b>{round(bal, 2)}$</b>\n'
               f'Bugungi chiqim: <b>{sstr(dv["chU"], dv["chZ"])}</b>\n'
               f'Bugungi kirim:  <b>{sstr(dv["kiU"], dv["kiZ"])}</b>')
        await q.message.reply_text(txt, parse_mode='HTML', reply_markup=kb_main())
        return ConversationHandler.END
    if d.startswith('C|'):
        ud['tur'] = d[2:]
        m = await q.message.reply_text(f'📤 <b>{ud["tur"]}</b>\n\nKim sarfladi?', parse_mode='HTML', reply_markup=kb_egasi())
        ud.setdefault('msgs', []).append(m.message_id); return EGASI
    if d.startswith('K|'):
        ud['tur'] = d[2:]
        m = await q.message.reply_text(f'📥 <b>{ud["tur"]}</b>\n\nKimning kirimi?', parse_mode='HTML', reply_markup=kb_egasi())
        ud.setdefault('msgs', []).append(m.message_id); return EGASI
    if d.startswith('E|'):
        ud['egasi'] = d[2:]
        m = await q.message.reply_text(f'👤 <b>{ud["egasi"]}</b>\n\nTo\'lov turi?', parse_mode='HTML', reply_markup=kb_tolov())
        ud.setdefault('msgs', []).append(m.message_id); return TOLOV
    if d.startswith('T|'):
        ud['tolov'] = d[2:]
        m = await q.message.reply_text(f'💳 <b>{ud["tolov"]}</b>\n\nValyuta:', parse_mode='HTML', reply_markup=kb_valyuta())
        ud.setdefault('msgs', []).append(m.message_id); return VALYUTA
    if d.startswith('V|'):
        ud['valyuta'] = d[2:]
        hint = 'Masalan: 150' if ud['valyuta'] == 'USD' else 'Masalan: 350000'
        m = await q.message.reply_text(f'💱 <b>{ud["valyuta"]}</b>\n\nSummani yozing:\n<i>{hint}</i>', parse_mode='HTML')
        ud.setdefault('msgs', []).append(m.message_id); return SUMMA
    if d == 'SKIP':
        ud['note'] = ''
        ud.setdefault('msgs', []).append(q.message.message_id)
        await _finalize(q.message, ctx)
        return ConversationHandler.END
    return ConversationHandler.END

async def get_summa(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ok(update): return
    txt = update.message.text.strip().replace(' ', '').replace(',', '.')
    try:
        num = float(txt); assert num > 0
    except:
        await update.message.reply_text('❌ Raqam kiriting.\n<i>Masalan: 150 yoki 350000</i>', parse_mode='HTML')
        return SUMMA
    ctx.user_data['summa'] = num
    ctx.user_data.setdefault('msgs', []).append(update.message.message_id)
    m = await update.message.reply_text(
        f'✅ Summa: <b>{smstr(ctx.user_data)}</b>\n\nNote yozing yoki o\'tkazib yuboring:',
        parse_mode='HTML', reply_markup=kb_note()
    )
    ctx.user_data['msgs'].append(m.message_id)
    return NOTE

async def get_note(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ok(update): return
    ctx.user_data.setdefault('msgs', []).append(update.message.message_id)
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
        await message.reply_text(txt, parse_mode='HTML', reply_markup=kb_main())
        try:
            await delete_messages(ctx.application.bot, message.chat_id, msgs)
        except Exception as de:
            logger.error(f'delete msgs: {de}')
    except Exception as e:
        logger.error(f'finalize: {e}')
        ctx.user_data.clear()
        await message.reply_text(f'❌ Xato: {e}', reply_markup=kb_main())

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
                f'\n\n💰 <b>BALANCE: {round(bal, 2)}$</b>')
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
            TUR:     [CallbackQueryHandler(btn)],
            EGASI:   [CallbackQueryHandler(btn)],
            TOLOV:   [CallbackQueryHandler(btn)],
            VALYUTA: [CallbackQueryHandler(btn)],
            SUMMA:   [MessageHandler(filters.TEXT & ~filters.COMMAND, get_summa), CallbackQueryHandler(btn)],
            NOTE:    [MessageHandler(filters.TEXT & ~filters.COMMAND, get_note), CallbackQueryHandler(btn)],
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

# ── FASTAPI ───────────────────────────────────────────────
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

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
    egasi:   str = ''
    tur:     str = ''
    tolov:   str = ''
    valyuta: str = ''
    summa:   float = 0
    note:    str = ''

def read_sheet(sheet_name: str):
    sh   = get_ss().worksheet(sheet_name)
    rows = sh.get_all_values()
    result = []
    for i, row in enumerate(rows[2:], start=3):
        if len(row) < 5 or not row[2] or not row[4]: continue
        usd = num_clean(row[6]) if len(row) > 6 and row[6] else 0.0
        uzs = num_clean(row[7]) if len(row) > 7 and row[7] else 0.0
        result.append({
            'row':   i,
            'type':  sheet_name,
            'sana':  norm_date(row[2]),
            'egasi': row[3] if len(row) > 3 else '',
            'tur':   row[4] if len(row) > 4 else '',
            'tolov': row[5] if len(row) > 5 else '',
            'usd':   usd,
            'uzs':   uzs,
            'note':  row[9] if len(row) > 9 else '',
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
def root(): return {'status': 'ok', 'message': 'Family Accounting API'}

@api.get('/balance')
def balance_endpoint():
    try:
        ss = get_ss()
        for sheet, cell in [('KUNLIK_VIEW', 'E2'), ('DASHBOARD', 'B2')]:
            try:
                raw = ss.worksheet(sheet).acell(cell).value
                if not raw: continue
                v = num_clean(raw)
                if v > 0: return {'balance': round(v, 2), 'formatted': f'{round(v, 2)}$'}
            except: continue
        return {'balance': 0, 'formatted': '0$'}
    except Exception as e:
        raise HTTPException(500, str(e))

@api.get('/today')
def get_today():
    try:
        today  = today_str()
        ss     = get_ss()
        result = {'date': today, 'chiqimlar': [], 'kirimlar': [],
                  'total_ch_usd': 0.0, 'total_ch_uzs': 0.0,
                  'total_ki_usd': 0.0, 'total_ki_uzs': 0.0}
        for sname, key in [('CHIQIM', 'chiqimlar'), ('KIRIM', 'kirimlar')]:
            sh    = ss.worksheet(sname)
            dates = sh.col_values(3)
            turs  = sh.col_values(5)
            egasi = sh.col_values(4)
            tolov = sh.col_values(6)
            usds  = sh.col_values(7)
            uzss  = sh.col_values(8)
            notes = sh.col_values(10)
            n     = max(len(dates), len(turs))
            for i in range(2, n):
                d = str(dates[i]).strip() if i < len(dates) else ''
                if not d or norm_date(d) != today: continue
                tur = str(turs[i]).strip() if i < len(turs) else ''
                if not tur: continue
                u = num_clean(usds[i] if i < len(usds) else '')
                z = num_clean(uzss[i] if i < len(uzss) else '')
                result[key].append({
                    'row':   i + 1, 'tur': tur,
                    'egasi': str(egasi[i]).strip() if i < len(egasi) else '',
                    'tolov': str(tolov[i]).strip() if i < len(tolov) else '',
                    'usd': u, 'uzs': z,
                    'note': str(notes[i]).strip() if i < len(notes) else '',
                })
                if key == 'chiqimlar':
                    result['total_ch_usd'] += u; result['total_ch_uzs'] += z
                else:
                    result['total_ki_usd'] += u; result['total_ki_uzs'] += z
        return result
    except Exception as e:
        raise HTTPException(500, str(e))

@api.get('/by-date')
def get_by_date(date: str = Query(...)):
    try:
        ss     = get_ss()
        result = {'date': date, 'chiqimlar': [], 'kirimlar': []}
        for sname, key in [('CHIQIM', 'chiqimlar'), ('KIRIM', 'kirimlar')]:
            sh    = ss.worksheet(sname)
            dates = sh.col_values(3)
            turs  = sh.col_values(5)
            egasi = sh.col_values(4)
            tolov = sh.col_values(6)
            usds  = sh.col_values(7)
            uzss  = sh.col_values(8)
            notes = sh.col_values(10)
            n     = max(len(dates), len(turs))
            for i in range(2, n):
                d = str(dates[i]).strip() if i < len(dates) else ''
                if not d or norm_date(d) != date: continue
                tur = str(turs[i]).strip() if i < len(turs) else ''
                if not tur: continue
                u = num_clean(usds[i] if i < len(usds) else '')
                z = num_clean(uzss[i] if i < len(uzss) else '')
                result[key].append({
                    'row': i+1, 'tur': tur,
                    'egasi': str(egasi[i]).strip() if i < len(egasi) else '',
                    'tolov': str(tolov[i]).strip() if i < len(tolov) else '',
                    'usd': u, 'uzs': z,
                    'note': str(notes[i]).strip() if i < len(notes) else '',
                })
        return result
    except Exception as e:
        raise HTTPException(500, str(e))

@api.get('/history')
def get_history(limit: int = 100):
    try:
        all_tx = read_sheet('CHIQIM') + read_sheet('KIRIM')
        all_tx.sort(key=lambda x: datetime.strptime(x['sana'], '%d.%m.%Y') if x['sana'] else datetime.min, reverse=True)
        return {'transactions': all_tx[:limit], 'total': len(all_tx)}
    except Exception as e:
        raise HTTPException(500, str(e))

@api.get('/stats')
def get_stats():
    try:
        ch = read_sheet('CHIQIM')
        ki = read_sheet('KIRIM')
        ch_by = {}
        for t in ch:
            v = t['usd'] + (t['uzs'] / 12000 if t['uzs'] else 0)
            ch_by[t['tur']] = ch_by.get(t['tur'], 0) + v
        ki_by = {}
        for t in ki:
            v = t['usd'] + (t['uzs'] / 12000 if t['uzs'] else 0)
            ki_by[t['tur']] = ki_by.get(t['tur'], 0) + v
        chs = sorted(ch_by.items(), key=lambda x: x[1], reverse=True)
        kis = sorted(ki_by.items(), key=lambda x: x[1], reverse=True)
        return {
            'chiqim': {'by_tur': chs, 'top': chs[0] if chs else None, 'bottom': chs[-1] if chs else None, 'total_usd': round(sum(ch_by.values()), 2), 'count': len(ch)},
            'kirim':  {'by_tur': kis, 'top': kis[0] if kis else None, 'bottom': kis[-1] if kis else None, 'total_usd': round(sum(ki_by.values()), 2), 'count': len(ki)},
            'net': round(sum(ki_by.values()) - sum(ch_by.values()), 2),
        }
    except Exception as e:
        raise HTTPException(500, str(e))

@api.post('/transaction')
def add_transaction(tx: Transaction):
    try:
        ss  = get_ss()
        sh  = ss.worksheet(tx.type)
        nr  = find_next_row(sh)
        usd = tx.summa if tx.valyuta == 'USD' else ''
        uzs = tx.summa if tx.valyuta == 'UZS' else ''
        sh.update(f'B{nr}:H{nr}', [[nr-2, tx.sana, tx.egasi, tx.tur, tx.tolov, usd, uzs]])
        sh.update(f'J{nr}', [[tx.note or '']])
        return {'success': True, 'row': nr, 'message': 'Saqlandi'}
    except Exception as e:
        raise HTTPException(500, str(e))

@api.put('/transaction/{sheet}/{row}')
def update_transaction(sheet: str, row: int, data: UpdateTransaction):
    try:
        if sheet not in ['CHIQIM', 'KIRIM']: raise HTTPException(400, "Sheet noto'g'ri")
        ss = get_ss()
        sh = ss.worksheet(sheet)
        if data.egasi:  sh.update(f'D{row}', [[data.egasi]])
        if data.tur:    sh.update(f'E{row}', [[data.tur]])
        if data.tolov:  sh.update(f'F{row}', [[data.tolov]])
        if data.summa and data.summa > 0:
            if data.valyuta == 'USD':
                sh.update(f'G{row}', [[data.summa]]); sh.update(f'H{row}', [['']])
            else:
                sh.update(f'H{row}', [[data.summa]]); sh.update(f'G{row}', [['']])
        if data.note is not None: sh.update(f'J{row}', [[data.note]])
        return {'success': True, 'message': 'Yangilandi'}
    except Exception as e:
        raise HTTPException(500, str(e))

def run_api():
    import asyncio, uvicorn
    loop = asyncio.new_event_loop()
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
