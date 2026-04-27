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

def get_bugun():
    today = datetime.now(TZ).strftime('%d.%m.%Y')
    ss    = get_ss()
    r     = dict(ch=[], ki=[], chU=0.0, chZ=0.0, kiU=0.0, kiZ=0.0)
    try:
        for row in ss.worksheet('CHIQIM').get_all_values()[2:]:
            if len(row) < 8 or not row[2] or row[2].strip() != today: continue
            if not row[4] or not row[4].strip(): continue
            u = float(row[6]) if row[6] and row[6].strip() else 0.0
            z = float(row[7]) if row[7] and row[7].strip() else 0.0
            r['ch'].append({'tur': row[4], 'usd': u, 'uzs': z})
            r['chU'] += u; r['chZ'] += z
    except Exception as e: logger.error(f'bugun ch: {e}')
    try:
        for row in ss.worksheet('KIRIM').get_all_values()[2:]:
            if len(row) < 8 or not row[2] or row[2].strip() != today: continue
            if not row[4] or not row[4].strip(): continue
            u = float(row[6]) if row[6] and row[6].strip() else 0.0
            z = float(row[7]) if row[7] and row[7].strip() else 0.0
            r['ki'].append({'tur': row[4], 'usd': u, 'uzs': z})
            r['kiU'] += u; r['kiZ'] += z
    except Exception as e: logger.error(f'bugun ki: {e}')
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
        ud.clear(); ud['type'] = 'CHIQIM'
        await q.message.reply_text('📤 <b>CHIQIM</b>\n\nXarajat turini tanlang:', parse_mode='HTML', reply_markup=kb_chiqim())
        return TUR

    if d == 'MK':
        ud.clear(); ud['type'] = 'KIRIM'
        await q.message.reply_text('📥 <b>KIRIM</b>\n\nKirim turini tanlang:', parse_mode='HTML', reply_markup=kb_kirim())
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
        await q.message.reply_text(f'📤 <b>{ud["tur"]}</b>\n\nKim sarfladi?', parse_mode='HTML', reply_markup=kb_egasi())
        return EGASI

    if d.startswith('K|'):
        ud['tur'] = d[2:]
        await q.message.reply_text(f'📥 <b>{ud["tur"]}</b>\n\nKimning kirimi?', parse_mode='HTML', reply_markup=kb_egasi())
        return EGASI

    if d.startswith('E|'):
        ud['egasi'] = d[2:]
        await q.message.reply_text(f'👤 <b>{ud["egasi"]}</b>\n\nTo\'lov turi?', parse_mode='HTML', reply_markup=kb_tolov())
        return TOLOV

    if d.startswith('T|'):
        ud['tolov'] = d[2:]
        await q.message.reply_text(f'💳 <b>{ud["tolov"]}</b>\n\nValyuta:', parse_mode='HTML', reply_markup=kb_valyuta())
        return VALYUTA

    if d.startswith('V|'):
        ud['valyuta'] = d[2:]
        hint = 'Masalan: 150' if ud['valyuta'] == 'USD' else 'Masalan: 350000'
        await q.message.reply_text(f'💱 <b>{ud["valyuta"]}</b>\n\nSummani yozing:\n<i>{hint}</i>', parse_mode='HTML')
        return SUMMA

    if d == 'SKIP':
        ud['note'] = ''
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
        await message.reply_text('⏳ Saqlanmoqda...')
        save_row(st['type'], st)
        bal = get_balance()
        txt = confirm_text(st, bal)
        ctx.user_data.clear()
        await message.reply_text(txt, parse_mode='HTML', reply_markup=kb_main())
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
    app.add_handler(conv)
    app.job_queue.run_daily(daily_report, time=dtime(hour=18, minute=50, tzinfo=pytz.utc))
    logger.info('Bot ishga tushdi!')
    app.run_polling(drop_pending_updates=True)

if __name__ == '__main__':
    main()
