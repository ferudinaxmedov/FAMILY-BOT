"""
store.py — Supabase-backed data layer for FAMILY-BOT.

Supabase (`family_*` tables) is now the PRIMARY source for every read.
Google Sheets stays as a permanent write-through mirror: every write here is
followed by a best-effort mirror write via `sheets_mirror.*` (failures are
logged, never raised — Sheets going down must not break the bot).

Design notes:
  * Dates are passed/returned as 'dd.mm.yyyy' strings (the format the rest of
    the bot already speaks); conversion to/from Postgres `date` (yyyy-mm-dd)
    happens at the edges of this module.
  * IDs are Postgres UUIDs (text). Telegram callback_data has a 64-byte limit;
    a prefix like 'QARZ_DONE_' + uuid (36 chars) fits comfortably.
  * get_balance() now returns a (usd, uzs) tuple computed directly from
    family_transactions (sum of KIRIM minus CHIQIM per currency). The old
    Sheets version read a single pre-computed cell from a DASHBOARD view whose
    formula blended currencies in a way we can't faithfully reproduce — a
    per-currency balance is the correct, lossless replacement and matches how
    every other balance-like figure in the bot is already displayed (sstr()).
"""
import os, asyncio, logging
from datetime import datetime, date as date_cls, timedelta

from supabase import create_client, Client

logger = logging.getLogger(__name__)

_sb: Client | None = None


def sb() -> Client:
    global _sb
    if _sb is None:
        _sb = create_client(os.environ['SUPABASE_URL'], os.environ['SUPABASE_SERVICE_ROLE_KEY'])
    return _sb


# ── date helpers: 'dd.mm.yyyy' (bot) <-> 'yyyy-mm-dd' (Postgres) ─────────────
def _iso(d):
    if d in (None, ''):
        return None
    if isinstance(d, (date_cls, datetime)):
        return d.strftime('%Y-%m-%d')
    s = str(d).strip()
    if len(s) == 10 and s[4] == '-':
        return s
    try:
        return datetime.strptime(s, '%d.%m.%Y').strftime('%Y-%m-%d')
    except Exception:
        return s


def _disp(d):
    if not d:
        return ''
    s = str(d).strip()
    try:
        return datetime.strptime(s[:10], '%Y-%m-%d').strftime('%d.%m.%Y')
    except Exception:
        return s


def _num(v):
    """None/'' -> None, else float — keeps Supabase numeric columns clean."""
    if v in (None, '', '0', '0.0'):
        return None
    try:
        f = float(v)
        return f if f != 0 else None
    except Exception:
        return None


async def _run(fn):
    return await asyncio.to_thread(fn)


# ════════════════════════════════════════════════════════════
# TRANSACTIONS (CHIQIM / KIRIM)
# ════════════════════════════════════════════════════════════
async def add_transaction(type_, sana, egasi, tur, tolov, usd, uzs, vaqt, note=''):
    """type_: 'CHIQIM' | 'KIRIM'. sana: 'dd.mm.yyyy'. Returns new row id (uuid str)."""
    row = {
        'type': type_, 'date': _iso(sana), 'owner': egasi, 'category': tur,
        'payment_method': tolov, 'amount_usd': _num(usd), 'amount_uzs': _num(uzs),
        'time': vaqt, 'note': note or None,
    }

    def _ins():
        return sb().table('family_transactions').insert(row).execute()
    res = await _run(_ins)
    new_id = res.data[0]['id'] if res.data else None
    logger.info(f'[supabase] family_transactions +{type_} {egasi}/{tur} -> {new_id}')
    return new_id


async def transaction_get(tx_id):
    def _q():
        return (sb().table('family_transactions').select('*')
                .eq('id', tx_id).maybe_single().execute())
    res = await _run(_q)
    if not res or not res.data:
        return None
    r = res.data
    return {'type': r['type'], 'sana': _disp(r['date']), 'egasi': r['owner'],
            'tur': r['category'], 'tolov': r['payment_method'],
            'usd': float(r['amount_usd'] or 0), 'uzs': float(r['amount_uzs'] or 0),
            'vaqt': r['time'] or '', 'note': r['note'] or ''}


async def update_transaction(tx_id, patch: dict):
    """patch: Supabase column names (owner/category/payment_method/date/amount_usd/amount_uzs/note)."""
    def _upd():
        return sb().table('family_transactions').update(patch).eq('id', tx_id).execute()
    res = await _run(_upd)
    return res.data[0] if res.data else None


async def get_balance():
    """Returns (balance_usd, balance_uzs) = sum(KIRIM) - sum(CHIQIM) per currency."""
    def _q():
        return sb().table('family_transactions').select('type,amount_usd,amount_uzs').execute()
    res = await _run(_q)
    usd = uzs = 0.0
    for r in (res.data or []):
        sign = 1 if r['type'] == 'KIRIM' else -1
        usd += sign * float(r['amount_usd'] or 0)
        uzs += sign * float(r['amount_uzs'] or 0)
    return round(usd, 2), round(uzs, 2)


async def get_bugun(today_str):
    """today_str: 'dd.mm.yyyy'. Same return shape as the old get_bugun()."""
    def _q():
        return (sb().table('family_transactions')
                .select('type,category,amount_usd,amount_uzs')
                .eq('date', _iso(today_str)).execute())
    res = await _run(_q)
    r = dict(ch=[], ki=[], chU=0.0, chZ=0.0, kiU=0.0, kiZ=0.0)
    for row in (res.data or []):
        u = float(row['amount_usd'] or 0); z = float(row['amount_uzs'] or 0)
        item = {'tur': row['category'], 'usd': u, 'uzs': z}
        if row['type'] == 'CHIQIM':
            r['ch'].append(item); r['chU'] += u; r['chZ'] += z
        else:
            r['ki'].append(item); r['kiU'] += u; r['kiZ'] += z
    return r


async def get_filtered(tip, davr, tur, date_from=None, date_to=None, now=None):
    """tip: 'CHIQIM'|'KIRIM'. davr: bu_oy/otgan_oy/bu_yil/custom/barchasi.
    Returns (rows, total_usd, total_uzs) — rows shaped like the old version."""
    now = now or datetime.now()
    q = sb().table('family_transactions').select('date,category,owner,amount_usd,amount_uzs,note').eq('type', tip)

    if davr == 'bu_oy':
        start = now.replace(day=1).strftime('%Y-%m-%d')
        q = q.gte('date', start)
    elif davr == 'otgan_oy':
        first_this = now.replace(day=1)
        last_prev = first_this - timedelta(days=1)
        start = last_prev.replace(day=1).strftime('%Y-%m-%d')
        end = last_prev.strftime('%Y-%m-%d')
        q = q.gte('date', start).lte('date', end)
    elif davr == 'bu_yil':
        q = q.gte('date', now.replace(month=1, day=1).strftime('%Y-%m-%d'))
    elif davr == 'custom':
        if date_from:
            q = q.gte('date', _iso(date_from))
        if date_to:
            q = q.lte('date', _iso(date_to))

    if tur and tur != 'BARCHASI':
        q = q.eq('category', tur)

    def _run_q():
        return q.order('date', desc=True).execute()
    res = await _run(_run_q)
    result, total_usd, total_uzs = [], 0.0, 0.0
    for row in (res.data or []):
        u = float(row['amount_usd'] or 0); z = float(row['amount_uzs'] or 0)
        result.append({'sana': _disp(row['date']), 'tur': row['category'],
                       'egasi': row['owner'], 'usd': u, 'uzs': z, 'note': row['note'] or ''})
        total_usd += u; total_uzs += z
    return result, total_usd, total_uzs


async def get_history(limit=100):
    def _q():
        return (sb().table('family_transactions')
                .select('type,date,owner,category,payment_method,amount_usd,amount_uzs,time,note')
                .order('created_at', desc=True).limit(limit).execute())
    res = await _run(_q)
    return [{**row, 'date': _disp(row['date'])} for row in (res.data or [])]


async def get_stats(tip='CHIQIM', davr='bu_oy', now=None):
    rows, tu, tz = await get_filtered(tip, davr, 'BARCHASI', now=now)
    by_cat: dict = {}
    for r in rows:
        c = by_cat.setdefault(r['tur'], {'usd': 0.0, 'uzs': 0.0, 'count': 0})
        c['usd'] += r['usd']; c['uzs'] += r['uzs']; c['count'] += 1
    return by_cat, tu, tz


# ════════════════════════════════════════════════════════════
# CATEGORIES
# ════════════════════════════════════════════════════════════
async def load_categories():
    def _q():
        return (sb().table('family_categories').select('kind,name')
                .order('sort_order').execute())
    res = await _run(_q)
    chiqim = [r['name'] for r in (res.data or []) if r['kind'] == 'chiqim']
    kirim = [r['name'] for r in (res.data or []) if r['kind'] == 'kirim']
    return chiqim, kirim


async def save_categories(chiqim: list, kirim: list):
    def _save():
        client = sb()
        client.table('family_categories').delete().neq('id', '00000000-0000-0000-0000-000000000000').execute()
        rows = ([{'kind': 'chiqim', 'name': n, 'sort_order': i} for i, n in enumerate(chiqim)] +
                [{'kind': 'kirim', 'name': n, 'sort_order': i} for i, n in enumerate(kirim)])
        if rows:
            client.table('family_categories').insert(rows).execute()
    await _run(_save)
    return True


# ════════════════════════════════════════════════════════════
# QARZ (debt tracking)
# ════════════════════════════════════════════════════════════
async def qarz_add(tur, kim, summa_uzs, summa_usd, sana, muddat, note=''):
    def _q():
        cnt = sb().table('family_qarz').select('id', count='exact').execute()
        return cnt.count or 0

    def _ins(number):
        row = {'number': number, 'type': tur, 'person': kim, 'amount_uzs': _num(summa_uzs),
               'amount_usd': _num(summa_usd), 'date': _iso(sana), 'deadline': _iso(muddat),
               'status': 'AKTIV', 'note': note or None}
        return sb().table('family_qarz').insert(row).execute()
    number = await _run(_q)
    res = await _run(lambda: _ins(number))
    return res.data[0]['id'] if res.data else None


async def qarz_active():
    def _q():
        return (sb().table('family_qarz').select('*')
                .eq('status', 'AKTIV').order('created_at').execute())
    res = await _run(_q)
    out = []
    for r in (res.data or []):
        out.append({'_id': r['id'], 'tur': r['type'], 'kim': r['person'],
                    'summa_uzs': r['amount_uzs'], 'summa_usd': r['amount_usd'],
                    'sana': _disp(r['date']), 'muddat': _disp(r['deadline']),
                    'holat': r['status'], 'note': r.get('note') or ''})
    return out


async def qarz_get(qarz_id):
    def _q():
        return sb().table('family_qarz').select('*').eq('id', qarz_id).single().execute()
    res = await _run(_q)
    r = res.data
    if not r:
        return None
    return {'_id': r['id'], 'tur': r['type'], 'kim': r['person'],
            'summa_uzs': r['amount_uzs'], 'summa_usd': r['amount_usd'],
            'sana': _disp(r['date']), 'muddat': _disp(r['deadline']), 'holat': r['status']}


async def qarz_close(qarz_id, qaytarilgan_sana):
    def _upd():
        return (sb().table('family_qarz').update(
            {'status': 'TUGADI', 'returned_date': _iso(qaytarilgan_sana)})
            .eq('id', qarz_id).execute())
    await _run(_upd)


async def qarz_overdue(today: date_cls):
    def _q():
        return (sb().table('family_qarz').select('person,deadline,type,amount_usd,amount_uzs')
                .eq('status', 'AKTIV').lt('deadline', today.strftime('%Y-%m-%d')).execute())
    res = await _run(_q)
    return [{'kim': r['person'], 'muddat': _disp(r['deadline']), 'tur': r['type'],
             'summa_usd': r['amount_usd'], 'summa_uzs': r['amount_uzs']} for r in (res.data or [])]


# ════════════════════════════════════════════════════════════
# TASKS
# ════════════════════════════════════════════════════════════
async def task_add(matn, vaqt_str, egasi, chat_id, sana):
    """vaqt_str: 'dd.mm.yyyy HH:MM' (kept verbatim for the reminder scheduler)."""
    def _ins():
        row = {'created_date': _iso(sana), 'scheduled_at': None, 'text': matn,
               'owner': egasi, 'status': 'FAOL', 'chat_id': str(chat_id)}
        # scheduled_at stored as raw text-derived timestamptz when parseable
        try:
            dt = datetime.strptime(vaqt_str, '%d.%m.%Y %H:%M')
            row['scheduled_at'] = dt.strftime('%Y-%m-%dT%H:%M:00')
        except Exception:
            pass
        return sb().table('family_tasks').insert(row).execute()
    res = await _run(_ins)
    return res.data[0]['id'] if res.data else None


async def task_mark(task_id, status):
    def _upd():
        return sb().table('family_tasks').update({'status': status}).eq('id', task_id).execute()
    await _run(_upd)


async def tasks_active(min_date_str=None):
    q = sb().table('family_tasks').select('*').eq('status', 'FAOL')

    def _q():
        return q.order('scheduled_at').execute()
    res = await _run(_q)
    out = []
    for r in (res.data or []):
        if not r['scheduled_at']:
            continue
        dt = datetime.fromisoformat(r['scheduled_at'])
        out.append({'_id': r['id'], 'vaqt': dt.strftime('%d.%m.%Y %H:%M'),
                    'matn': r['text'], 'egasi': r['owner'], 'chat_id': r['chat_id']})
    return out


async def tasks_list(status='FAOL'):
    """status='ALL' -> barcha vazifalar; aks holda shu holat bo'yicha (API uchun)."""
    q = sb().table('family_tasks').select('*')
    if status != 'ALL':
        q = q.eq('status', status)

    def _q():
        return q.order('scheduled_at', desc=True).execute()
    res = await _run(_q)
    out = []
    for r in (res.data or []):
        vaqt = ''
        if r['scheduled_at']:
            try:
                vaqt = datetime.fromisoformat(r['scheduled_at']).strftime('%d.%m.%Y %H:%M')
            except Exception:
                pass
        out.append({'id': r['id'], 'yaratilgan': _disp(r['created_date']), 'vaqt': vaqt,
                    'matn': r['text'], 'egasi': r['owner'], 'holat': r['status']})
    return out


async def task_get(task_id):
    def _q():
        return sb().table('family_tasks').select('text,owner').eq('id', task_id).maybe_single().execute()
    res = await _run(_q)
    if not res or not res.data:
        return None
    return {'matn': res.data['text'], 'egasi': res.data['owner']}


# ════════════════════════════════════════════════════════════
# MEMORY (key-value notes)
# ════════════════════════════════════════════════════════════
async def memory_save(kalit, qiymat, kim, sana):
    def _upsert():
        existing = (sb().table('family_memory').select('id').ilike('key', kalit).execute()).data
        if existing:
            sb().table('family_memory').update(
                {'value': qiymat, 'owner': kim, 'date': _iso(sana)}).eq('id', existing[0]['id']).execute()
            return 'yangilandi'
        sb().table('family_memory').insert(
            {'key': kalit, 'value': qiymat, 'owner': kim, 'date': _iso(sana)}).execute()
        return 'saqlandi'
    return await _run(_upsert)


async def memory_search(query):
    q = query.lower()

    def _q():
        return (sb().table('family_memory').select('key,value,owner,date')
                .or_(f'key.ilike.%{query}%,value.ilike.%{query}%').execute())
    res = await _run(_q)
    return [{'kalit': r['key'], 'qiymat': r['value'], 'kim': r['owner'] or '',
             'sana': _disp(r['date'])} for r in (res.data or [])]


# ════════════════════════════════════════════════════════════
# NAMOZ — prayer times & daily check-in log
# ════════════════════════════════════════════════════════════
NAMOZ_UZ = ['bomdod', 'peshin', 'asr', 'shom', 'xufton']


async def namoz_times_get(date_obj):
    def _q():
        return (sb().table('family_namoz_times').select('*')
                .eq('year', date_obj.year).eq('month', date_obj.month).eq('day', date_obj.day)
                .maybe_single().execute())
    res = await _run(_q)
    if not res or not res.data:
        return None
    r = res.data
    return {k: r.get(k) for k in ['bomdod', 'quyosh', 'peshin', 'asr', 'shom', 'xufton']}


async def namoz_times_save(year, month, rows):
    """rows: list of dicts with day + 6 prayer-time strings ('HH:MM')."""
    def _save():
        client = sb()
        for d in rows:
            payload = {'year': year, 'month': month, 'day': d['day'],
                       'bomdod': d.get('bomdod') or None, 'quyosh': d.get('quyosh') or None,
                       'peshin': d.get('peshin') or None, 'asr': d.get('asr') or None,
                       'shom': d.get('shom') or None, 'xufton': d.get('xufton') or None}
            client.table('family_namoz_times').upsert(payload, on_conflict='year,month,day').execute()
    await _run(_save)


async def namoz_log_set(sana, kim, namoz, status):
    """sana: 'dd.mm.yyyy'. Upserts a single prayer's status for (date, owner)."""
    def _upsert():
        client = sb()
        existing = (client.table('family_namoz_log').select('id')
                    .eq('date', _iso(sana)).eq('owner', kim).execute()).data
        if existing:
            client.table('family_namoz_log').update({namoz: status, 'updated_at': 'now()'}).eq('id', existing[0]['id']).execute()
        else:
            client.table('family_namoz_log').insert(
                {'date': _iso(sana), 'owner': kim, namoz: status}).execute()
    await _run(_upsert)


async def namoz_log_set_all(sana, kim, statuses: dict):
    """statuses: {'bomdod': 'O'QILDI'|"O'QILMADI", ...} — used by the end-of-day check-in."""
    def _upsert():
        client = sb()
        payload = {'date': _iso(sana), 'owner': kim, **statuses, 'updated_at': 'now()'}
        existing = (client.table('family_namoz_log').select('id')
                    .eq('date', _iso(sana)).eq('owner', kim).execute()).data
        if existing:
            client.table('family_namoz_log').update(payload).eq('id', existing[0]['id']).execute()
        else:
            client.table('family_namoz_log').insert(payload).execute()
    await _run(_upsert)


async def namoz_weekly_stats(today: date_cls):
    week_ago = today - timedelta(days=7)

    def _q():
        return (sb().table('family_namoz_log').select('*')
                .gte('date', week_ago.strftime('%Y-%m-%d')).lte('date', today.strftime('%Y-%m-%d'))
                .execute())
    res = await _run(_q)
    stats = {'FERUDIN': {n: {'ok': 0, 'no': 0} for n in NAMOZ_UZ},
             'GULOYIM': {n: {'ok': 0, 'no': 0} for n in NAMOZ_UZ}}
    for row in (res.data or []):
        kim = row['owner']
        if kim not in stats:
            continue
        for namoz in NAMOZ_UZ:
            val = row.get(namoz)
            if val == "O'QILDI":
                stats[kim][namoz]['ok'] += 1
            elif val == "O'QILMADI":
                stats[kim][namoz]['no'] += 1
    return stats


# ════════════════════════════════════════════════════════════
# BNB (guest registration)
# ════════════════════════════════════════════════════════════
async def bnb_save(registration):
    def _ins():
        rows = []
        for guest in registration.get('guests', []):
            rows.append({
                'passport_id': guest.get('passportId') or None,
                'guest_name': guest.get('name') or None,
                'nationality': guest.get('nationality') or None,
                'dob': _iso(guest.get('dob')),
                'uzbek_entry_date': _iso(registration.get('uzbekEntryDate')),
                'apt_entry_date': _iso(registration.get('aptEntryDate')),
                'departure_date': _iso(registration.get('departureDate')),
                'reg_start_date': _iso(registration.get('regStartDate')),
                'reg_end_date': _iso(registration.get('regEndDate')),
                'apartment': registration.get('apartment') or None,
                'room': registration.get('room') or None,
                'payment_amount': str(registration.get('paymentAmount') or '') or None,
                'payment_by': registration.get('paymentBy') or None,
            })
        if rows:
            sb().table('family_bnb_registrations').insert(rows).execute()
    await _run(_ins)


async def bnb_history(limit=50):
    def _q():
        return (sb().table('family_bnb_registrations').select('*')
                .order('created_at', desc=True).limit(limit).execute())
    res = await _run(_q)
    out = []
    for r in (res.data or []):
        out.append({'apartment': r['apartment'] or '', 'guestName': r['guest_name'] or '',
                    'nationality': r['nationality'] or '', 'aptEntryDate': _disp(r['apt_entry_date']),
                    'departureDate': _disp(r['departure_date']),
                    'createdAt': (r['created_at'] or '')[:16].replace('T', ' ')})
    return out
