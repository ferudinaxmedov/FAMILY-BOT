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

def get_spreadsheet():
    info  = json.loads(CREDS_JSON)
    creds = Credentials.from_service_account_info(info, scopes=[
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive'
    ])
    return gspread.authorize(creds).open_by_key(SPREADSHEET_ID)

def get_balance():
    try:
        val = get_spreadsheet().worksheet('DASHBOARD').acell('B2').value
        return float(str(val).replace(',','.').replace(' ','') or 0)
    except Exception as e:
        logger.error(f'balance error: {e}')
        return 0.0

def save_row(sheet_name, st):
    sh      = get_spreadsheet().worksheet(sheet_name)
    rows    = sh.get_all_values()
    # Oxirgi to'liq qatorni topish
    last_row = 2
    for i, row in enumerate(rows):
        if any(cell.strip() for cell in row):
            last_row = i + 1
    new_row = last_row + 1
    today   = datetime.now(TZ).strftime('%d.%m.%Y')
    usd = st['summa'] if st['valyuta'] == 'USD' else ''
    uzs = st['summa'] if st['valyuta'] == 'UZS' else ''
    sh.update(f'B{new_row}', [[
        new_row-2, today, st['egasi'], st['tur'],
        st['tolov'], usd, uzs, '', st.get('note',''), '', ''
    ]])
def get_bugun():
    today = datetime.now(TZ).strftime('%d.%m.%Y')
    ss    = get_spreadsheet()
    r     = dict(ch=[], ki=[], chU=0, chZ=0, kiU=0, kiZ=0)
    for row in ss.worksheet('CHIQIM').get_all_values()[2:]:
        if len(row)<8 or not row[2] or not row[4] or row[2]!=today: continue
        u=float(row[6] or 0); z=float(row[7] or 0)
        r['ch'].append({'tur':row[4],'usd':u,'uzs':z}); r['chU']+=u; r['chZ']+=z
    for row in ss.worksheet('KIRIM').get_all_values()[2:]:
        if len(row)<8 or not row[2] or not row[4] or row[2]!=today: continue
        u=float(row[6] or 0); z=float(row[7] or 0)
        r['ki'].append({'tur':row[4],'usd':u,'uzs':z}); r['kiU']+=u; r['kiZ']+=z
    return r

def fmt(n):    return f"{int(round(n)):,}".replace(',',' ')
def sstr(u,z): parts=[f"{int(round(u))}$" if u>0 else None, f"{fmt(z)} so'm" if z>0 else None]; return ' + '.join(x for x in parts if x) or '0'
def smstr(st): return f"{int(st['summa'])}$" if st['valyuta']=='USD' else f"{fmt(st['summa'])} so'm"

def confirm_text(st):
    lbl=('CHIQIM' if st['type']=='CHIQIM' else 'KIRIM'); ico=('📤' if st['type']=='CHIQIM' else '📥')
    return (f"{ico} <b>{datetime.now(TZ).strftime('%d.%m.%Y')}</b>\n\n"
            f"{lbl} TURI: <b>{st['tur']}</b>\n{lbl} EGASI: <b>{st['egasi']}</b>\n"
            f"VALYUTA: <b>{st['valyuta']}</b>   SUMMA: <b>{smstr(st)}</b>\n"
            f"TO'LOV: <b>{st['tolov']}</b>\nNOTE: <b>{st.get('note') or '—'}</b>\n\n"
            f"💰 BALANCE: <b>{int(round(get_balance()))}$</b>")

def kb_main():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('📤 Chiqim',callback_data='MC'),InlineKeyboardButton('📥 Kirim',callback_data='MK')],
        [InlineKeyboardButton('💰 Balans',callback_data='MB'),InlineKeyboardButton('📅 Bugun',callback_data='MG')],
        [InlineKeyboardButton('📊 Statistika',callback_data='MS')]
    ])

def kb_chiqim():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('🛒 Oziq ovqat',callback_data='C|OZIQ OVQAT'),InlineKeyboardButton('⛽ Benzin',callback_data='C|BENZIN')],
        [InlineKeyboardButton('💳 Rassrochka',callback_data='C|RASSROCHKA'),InlineKeyboardButton('👗 Kiyim kechak',callback_data='C|KIYIM KECHAK')],
        [InlineKeyboardButton('👨 Xurshidga',callback_data='C|XURSHIDGA'),InlineKeyboardButton('🏢 Ishxonamga',callback_data='C|ISHXONAMGA')],
        [InlineKeyboardButton('🏠 Uydagilarga',callback_data='C|UYDAGILARGA'),InlineKeyboardButton('🚫 Shtraflar',callback_data='C|SHTRAFLAR')],
        [InlineKeyboardButton('🛍 Shopping',callback_data='C|SHOPPPING'),InlineKeyboardButton('📋 Ishxona reg',callback_data='C|ISHXONA REG')],
        [InlineKeyboardButton('✂️ Sartarosh',callback_data='C|SARTAROSH'),InlineKeyboardButton('💡 Boshqa',callback_data='C|BOSHQA')],
        [InlineKeyboardButton('🔙 Orqaga',callback_data='BACK')]
    ])

def kb_kirim():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('🏢 Ishxona',callback_data='K|ISHXONA')],
        [InlineKeyboardButton('🌱 Seedbee',callback_data='K|SEEDBEE')],
        [InlineKeyboardButton('💼 Business',callback_data='K|BUSINESS')],
        [InlineKeyboardButton('🏠 Uydagilar',callback_data='K|UYDAGILAR')],
        [InlineKeyboardButton('💡 Boshqa',callback_data='K|BOSHQA')],
        [InlineKeyboardButton('🔙 Orqaga',callback_data='BACK')]
    ])

kb_egasi  = lambda: InlineKeyboardMarkup([[InlineKeyboardButton('👨 Ferudin',callback_data='E|FERUDIN'),InlineKeyboardButton('👩 Guloyim',callback_data='E|GULOYIM')]])
kb_tolov  = lambda: InlineKeyboardMarkup([[InlineKeyboardButton('💵 Cash',callback_data='T|CASH'),InlineKeyboardButton('💳 Card',callback_data='T|CARD'),InlineKeyboardButton('📌 Other',callback_data='T|OTHER')]])
kb_valyuta= lambda: InlineKeyboardMarkup([[InlineKeyboardButton('💵 USD ($)',callback_data='V|USD'),InlineKeyboardButton("🇺🇿 UZS (so'm)",callback_data='V|UZS')]])
kb_note   = lambda: InlineKeyboardMarkup([[InlineKeyboardButton('✅ Done — note kerak emas',callback_data='SKIP')]])

def allowed(update): return str(update.effective_chat.id) in ALLOWED

async def start(update,ctx):
    if not allowed(update): return
    ctx.user_data.clear()
    await update.message.reply_text('👋 Assalomu alaykum! Nima qilmoqchisiz?',reply_markup=kb_main())
    return ConversationHandler.END

async def btn(update,ctx):
    q=update.callback_query; await q.answer()
    if not allowed(update): return
    d=q.data; ud=ctx.user_data

    if d=='BACK':   ud.clear(); await q.message.reply_text('Asosiy menyu:',reply_markup=kb_main()); return ConversationHandler.END
    if d=='MC':     ud.clear(); ud['type']='CHIQIM'; await q.message.reply_text('📤 Xarajat turini tanlang:',reply_markup=kb_chiqim()); return TUR
    if d=='MK':     ud.clear(); ud['type']='KIRIM';  await q.message.reply_text('📥 Kirim turini tanlang:',reply_markup=kb_kirim());   return TUR
    if d=='MB':     await q.message.reply_text(f'💰 <b>Hozirgi balans: {int(round(get_balance()))}$</b>',parse_mode='HTML',reply_markup=kb_main()); return ConversationHandler.END
    if d=='MS':
        dv=get_bugun(); bal=get_balance()
        await q.message.reply_text(f"📊 <b>Statistika</b>\n\n💰 Balance: <b>{int(round(bal))}$</b>\nBugungi chiqim: <b>{sstr(dv['chU'],dv['chZ'])}</b>\nBugungi kirim: <b>{sstr(dv['kiU'],dv['kiZ'])}</b>",parse_mode='HTML',reply_markup=kb_main())
        return ConversationHandler.END
    if d=='MG':
        dv=get_bugun(); today=datetime.now(TZ).strftime('%d.%m.%Y')
        txt=f'📅 <b>{today}</b>\n\n<b>📤 Chiqimlar:</b>\n'
        txt+=('\n'.join(f"• {c['tur']}: {sstr(c['usd'],c['uzs'])}" for c in dv['ch']) or "Yo'q") + '\n'
        txt+='\n<b>📥 Kirimlar:</b>\n'
        txt+=('\n'.join(f"• {k['tur']}: {sstr(k['usd'],k['uzs'])}" for k in dv['ki']) or "Yo'q") + '\n'
        txt+=f"\n💰 Balance: <b>{int(round(get_balance()))}$</b>"
        await q.message.reply_text(txt,parse_mode='HTML',reply_markup=kb_main()); return ConversationHandler.END

    if d.startswith('C|'): ud['tur']=d[2:]; await q.message.reply_text(f"✅ Tur: <b>{ud['tur']}</b>\n\nKim sarfladi?",parse_mode='HTML',reply_markup=kb_egasi()); return EGASI
    if d.startswith('K|'): ud['tur']=d[2:]; await q.message.reply_text(f"✅ Tur: <b>{ud['tur']}</b>\n\nKimning kirimi?",parse_mode='HTML',reply_markup=kb_egasi()); return EGASI
    if d.startswith('E|'): ud['egasi']=d[2:]; await q.message.reply_text(f"✅ Egasi: <b>{ud['egasi']}</b>\n\nTo'lov turi?",parse_mode='HTML',reply_markup=kb_tolov()); return TOLOV
    if d.startswith('T|'): ud['tolov']=d[2:]; await q.message.reply_text(f"✅ To'lov: <b>{ud['tolov']}</b>\n\nValyuta:",parse_mode='HTML',reply_markup=kb_valyuta()); return VALYUTA
    if d.startswith('V|'):
        ud['valyuta']=d[2:]
        hint='Masalan: 200' if ud['valyuta']=='USD' else 'Masalan: 345000'
        await q.message.reply_text(f"✅ Valyuta: <b>{ud['valyuta']}</b>\n\nSummani yozing:\n<i>{hint}</i>",parse_mode='HTML')
        return SUMMA
    if d=='SKIP': ud['note']=''; await _finalize(q.message,ctx); return ConversationHandler.END

    return ConversationHandler.END

async def get_summa(update,ctx):
    if not allowed(update): return
    try:
        num=float(update.message.text.strip().replace(' ','').replace(',','.')); assert num>0
    except:
        await update.message.reply_text('❌ Faqat raqam kiriting.\n<i>Masalan: 200 yoki 345000</i>',parse_mode='HTML'); return SUMMA
    ctx.user_data['summa']=num
    v=ctx.user_data.get('valyuta','USD')
    ss=f"{int(num)}$" if v=='USD' else f"{fmt(num)} so'm"
    await update.message.reply_text(f"✅ Summa: <b>{ss}</b>\n\nNote yozing yoki o'tkazib yuboring:",parse_mode='HTML',reply_markup=kb_note())
    return NOTE

async def get_note(update,ctx):
    if not allowed(update): return
    ctx.user_data['note']=update.message.text.strip()
    await _finalize(update.message,ctx); return ConversationHandler.END

async def _finalize(message,ctx):
    st=ctx.user_data
    try:
        save_row(st['type'],st)
        txt=confirm_text(st); ctx.user_data.clear()
        await message.reply_text(txt,parse_mode='HTML',reply_markup=kb_main())
    except Exception as e:
        logger.error(f'finalize: {e}'); ctx.user_data.clear()
        await message.reply_text(f'❌ Xato: {e}',reply_markup=kb_main())

async def daily_report(ctx):
    dv=get_bugun(); bal=get_balance(); today=datetime.now(TZ).strftime('%d.%m.%Y')
    txt=f'📊 <b>{today} — Kunlik hisobot</b>\n\n<b>📤 Bugungi chiqimlar:</b>\n'
    txt+=('\n'.join(f"• {c['tur']}: {sstr(c['usd'],c['uzs'])}" for c in dv['ch']) or "Yo'q")+'\n'
    txt+='\n<b>📥 Bugungi kirimlar:</b>\n'
    txt+=('\n'.join(f"• {k['tur']}: {sstr(k['usd'],k['uzs'])}" for k in dv['ki']) or "Yo'q")+'\n'
    txt+=f"\nJami chiqim: <b>{sstr(dv['chU'],dv['chZ'])}</b>\nJami kirim: <b>{sstr(dv['kiU'],dv['kiZ'])}</b>\n\n💰 <b>BALANCE: {int(round(bal))}$</b>"
    for cid in [CHAT_1,CHAT_2]:
        try: await ctx.bot.send_message(chat_id=cid,text=txt,parse_mode='HTML')
        except Exception as e: logger.error(f'daily {cid}: {e}')

def main():
    app=Application.builder().token(TOKEN).build()
    conv=ConversationHandler(
        entry_points=[CallbackQueryHandler(btn)],
        states={
            TUR:    [CallbackQueryHandler(btn)],
            EGASI:  [CallbackQueryHandler(btn)],
            TOLOV:  [CallbackQueryHandler(btn)],
            VALYUTA:[CallbackQueryHandler(btn)],
            SUMMA:  [MessageHandler(filters.TEXT&~filters.COMMAND,get_summa),CallbackQueryHandler(btn)],
            NOTE:   [MessageHandler(filters.TEXT&~filters.COMMAND,get_note),CallbackQueryHandler(btn)],
        },
        fallbacks=[CommandHandler('cancel',lambda u,c:(c.user_data.clear(),u.message.reply_text('Bekor qilindi.',reply_markup=kb_main()),ConversationHandler.END)[-1]),
                   CommandHandler('start',start)],
        per_message=False
    )
    app.add_handler(CommandHandler('start',start))
    app.add_handler(CommandHandler('menu',start))
    app.add_handler(conv)
    app.job_queue.run_daily(daily_report,time=dtime(hour=18,minute=50,tzinfo=pytz.utc))
    logger.info('Bot ishga tushdi!')
    app.run_polling(drop_pending_updates=True)

if __name__=='__main__':
    main()
