"""Python port of the supplied xBACKUP bulk account engines."""
from __future__ import annotations
from src.safe_errors import safe_error
import asyncio
import copy
import json
import random
import secrets
import uuid
import re
from datetime import datetime, timezone
from pathlib import Path
from database import db
from src import player_actions as actions
from src.auth_account import login
from src.player_records_saver import save_player_record, save_player_record_raw
from src.car_manager import (fetch_cars, prepare_car, resolve_cpm_id, merge_cars_by_id,
                             build_car_entries, save_car_status, save_car_encrypted)
from datas import load_json
from src.generation_settings import validate_prefix, player_name
from src.bulk_worker import signup_limiter, BulkStageError


def short_email(prefix=None, used=None):
    clean = validate_prefix(prefix) or ''.join(secrets.choice('abcdefghijklmnopqrstuvwxyz') for _ in range(5))
    used = used if used is not None else set()
    for _ in range(128):
        suffix = ''.join(secrets.choice('abcdefghijklmnopqrstuvwxyz0123456789') for _ in range(5))
        email = f'{clean}{suffix}@gmail.com'
        if email.lower() not in used:
            used.add(email.lower())
            return email
    raise RuntimeError('Could not generate a unique email suffix; no duplicate was submitted')


def usage_status(telegram_id):
    limits = dict(db.get_effective_bulk_limits(telegram_id))
    day = datetime.now(timezone.utc).date().isoformat()
    db._exec('CREATE TABLE IF NOT EXISTS bulk_usage (user_id INTEGER, kind TEXT, day TEXT, count INTEGER NOT NULL, PRIMARY KEY(user_id,kind,day))')
    rows = db._fetchall('SELECT kind,count FROM bulk_usage WHERE user_id=? AND day=?', (telegram_id, day))
    usage = {row['kind']: row['count'] for row in rows}
    usage['max'] = usage.get('max', 0) + usage.get('generate', 0)
    clone_left = max(0, limits['cloneDailyLimit'] - usage.get('clone', 0))
    max_left = max(0, limits['maxDailyLimit'] - usage.get('max', 0))
    remaining = min(clone_left, max_left)
    limits.update(cloneUsed=usage.get('clone', 0), maxUsed=usage.get('max', 0),
                  cloneRemaining=clone_left, maxRemaining=max_left, remaining=remaining,
                  sharedDailyQuota=False, resetTimezone='UTC')
    if db.is_admin(telegram_id):
        limits.update(cloneRemaining=None, maxRemaining=None, remaining=None)
    return limits


def quota_error(kind, remaining, count):
    label = 'clone' if kind == 'clone' else 'max account'
    return (f'Your daily {label} limit has only {remaining} accounts left, but you requested {count}. '
            f'Enter {remaining} or fewer. No quota was deducted.')


def _reservation_schema():
    db._conn.execute('CREATE TABLE IF NOT EXISTS bulk_reservations (id TEXT PRIMARY KEY,user_id INTEGER,kind TEXT,day TEXT,count INTEGER,settled INTEGER DEFAULT 0)')
    columns = {row[1] for row in db._conn.execute('PRAGMA table_info(bulk_reservations)')}
    if 'released' not in columns:
        db._conn.execute('ALTER TABLE bulk_reservations ADD COLUMN released INTEGER NOT NULL DEFAULT 0')


def release_failed_usage(reservation, failed):
    if not reservation:
        return
    with db._lock:
        _reservation_schema()
        db._conn.execute('SAVEPOINT quota_failed')
        try:
            row = db._conn.execute('SELECT * FROM bulk_reservations WHERE id=?', (reservation,)).fetchone()
            if row and not row['settled']:
                released = min(row['count'], max(row['released'], int(failed)))
                delta = released - row['released']
                db._conn.execute('UPDATE bulk_usage SET count=MAX(0,count-?) WHERE user_id=? AND kind=? AND day=?',
                                 (delta, row['user_id'], row['kind'], row['day']))
                db._conn.execute('UPDATE bulk_reservations SET released=? WHERE id=?', (released, reservation))
            db._conn.execute('RELEASE quota_failed')
        except BaseException:
            db._conn.execute('ROLLBACK TO quota_failed')
            db._conn.execute('RELEASE quota_failed')
            raise


def reserve_usage(telegram_id, kind, count):
    if kind not in ('clone', 'max'):
        raise ValueError('Unknown bulk operation')
    if isinstance(count, bool) or not isinstance(count, int) or count < 1:
        raise ValueError('Count must be a positive integer')
    if db.is_admin(telegram_id):
        return None
    with db._lock:
        limits = usage_status(telegram_id)
        if limits[kind + 'Remaining'] == 0:
            raise ValueError(f'Limit ended: your {kind} daily bulk limit is exhausted')
        day = datetime.now(timezone.utc).date().isoformat()
        _reservation_schema()
        db._conn.execute('BEGIN IMMEDIATE')
        try:
            used = db._conn.execute('SELECT COALESCE(SUM(count),0) FROM bulk_usage WHERE user_id=? AND day=? AND kind IN (?,?)',
                                    (telegram_id, day, kind, 'generate' if kind == 'max' else kind)).fetchone()[0]
            remaining = max(0, limits[kind + 'DailyLimit'] - used)
            if count > remaining:
                raise ValueError(f'Limit ended: your {kind} daily bulk limit is exhausted. No quota was deducted.'
                                 if remaining == 0 else quota_error(kind, remaining, count))
            if count > limits['batchLimit']:
                raise ValueError(f"You requested {count} accounts, but the batch limit is {limits['batchLimit']}. No quota was deducted.")
            reservation = uuid.uuid4().hex
            db._conn.execute('INSERT INTO bulk_usage VALUES(?,?,?,?) ON CONFLICT(user_id,kind,day) DO UPDATE SET count=count+excluded.count',
                             (telegram_id, kind, day, count))
            db._conn.execute('INSERT INTO bulk_reservations(id,user_id,kind,day,count) VALUES(?,?,?,?,?)',
                             (reservation, telegram_id, kind, day, count))
            db._conn.execute('COMMIT')
            return reservation
        except BaseException:
            db._conn.execute('ROLLBACK')
            raise


def settle_usage(reservation, successful):
    if not reservation:
        return
    with db._lock:
        _reservation_schema()
        db._conn.execute('SAVEPOINT quota_settle')
        try:
            row = db._conn.execute('SELECT * FROM bulk_reservations WHERE id=?', (reservation,)).fetchone()
            if row and not row['settled']:
                outstanding = row['count'] - row['released']
                refund = outstanding - min(outstanding, max(0, int(successful)))
                db._conn.execute('UPDATE bulk_usage SET count=MAX(0,count-?) WHERE user_id=? AND kind=? AND day=?',
                                 (refund, row['user_id'], row['kind'], row['day']))
                db._conn.execute('UPDATE bulk_reservations SET settled=1 WHERE id=?', (reservation,))
            db._conn.execute('RELEASE quota_settle')
        except BaseException:
            db._conn.execute('ROLLBACK TO quota_settle')
            db._conn.execute('RELEASE quota_settle')
            raise


# ═══════════════════════════════════════════════════════════
# CREATE + VERIFY
# ═══════════════════════════════════════════════════════════

async def create_and_verify(email, password, clone=False, name=None):
    """Register, initialise the profile, verify login. Retries only on retryable errors."""
    await signup_limiter.acquire()

    result = None
    for attempt in range(2):
        result = await actions.register_account(email, password, with_stock=False)
        if result.get('success') or 'already registered' in str(result.get('error', '')).lower() or 'EMAIL_EXISTS' in str(result.get('error', '')):
            break
        err = str(result.get('error', '')).lower()
        if any(t in err for t in ('too many', 'rate limit', '429', 'weak_password', 'invalid_email', 'email_exists')):
            break
        if not actions.retryable_error(result):
            break
        await asyncio.sleep((1 if clone else .4) * (attempt + 1))

    initial = dict(
        Name=player_name(name), money=25000, coin=0, localID='',
        floats=[0] * 54, integers=[0] * 120, LevelsDoneTime=[0] * 110,
        animations=[], wheels=[], emojiPacks=[], boughtFsos=[], FriendsID=[],
        favouriteWheels=[], favouriteVinyls=[], favouriteEmojis=[],
        fcar=[0], flags={}, boughtPoliceLights=[], boughtPoliceSirens=[])
    initial['floats'][1] = 1
    initial['floats'][40] = 25000

    if result and result.get('success'):
        await asyncio.sleep(1.2 if clone else 0.5)
        saved = await save_player_record(result['token'], initial, result['uid'], None)
        if not saved.get('success'):
            raise BulkStageError('Initial profile', saved.get('error') or 'Initial profile save failed',
                                 email, password)
    elif not result or not any(x in str(result.get('error', '')).lower()
                                for x in ('already registered', 'email_exists')):
        raise BulkStageError('Registration', (result or {}).get('error') or 'Registration failed',
                             email, password)

    verified = None
    for attempt in range(3):
        verified = await login(email, password)
        if verified.get('status'):
            profile = await actions.get_player_records(email, password, True)
            if profile.get('success'):
                if (profile.get('record') or {}).get('Name') != initial['Name']:
                    named = await actions.set_name(profile['token'], profile['uid'],
                                                   profile['record'], initial['Name'])
                    if not named.get('success'):
                        raise BulkStageError('Name',
                                             named.get('error') or 'Generated account name could not be confirmed',
                                             email, password)
                return profile
            if profile.get('message') == 'Player records is empty':
                recovered = await save_player_record(verified['idToken'], initial,
                                                     verified['localId'], None)
                if not recovered.get('success'):
                    raise BulkStageError('Profile init',
                                         recovered.get('error') or 'Profile initialization rejected',
                                         email, password)
        await asyncio.sleep((1 if clone else .25) * (attempt + 1))

    raise BulkStageError('Login',
                         (verified or {}).get('message') or 'Login verification failed',
                         email, password)


# ═══════════════════════════════════════════════════════════
# MAX SINGLE ACCOUNT — matches the working Node implementation
# ═══════════════════════════════════════════════════════════

def _rank_payload():
    try:
        return json.loads((Path(__file__).parent.parent / 'datas/legacy_bulk_rank.json').read_text())
    except Exception:
        return {
            "time": 1e22, "cars": 1e16, "car_fix": 1e13,
            "car_collided": 1e12, "car_exchange": 1e13,
            "car_trade": 1e13, "car_wash": 1e13,
            "slicer_cut": 1e13, "drift_max": 1e14,
            "drift": 1e14, "cargo": 1e5, "delivery": 1e5,
            "race_win": 3e20, "taxi": 1e10,
            "levels": 10000990000, "gifts": 1e9,
            "fuel": 1e10, "offroad": 1e10,
            "speed_banner": 1e9, "reactions": 1e17,
            "run": 1e9, "real_estate": 1e9,
            "t_distance": 1e10, "treasure": 1e10,
            "block_post": 1e10, "push_ups": 1e12,
            "burnt_tire": 1e10, "passanger_distance": 1e8,
        }


async def _upload_cars_parallel(token, uid, cars, email, password,
                                concurrency=20, progress=None):
    if not cars:
        return 0, 0
    sem = asyncio.Semaphore(concurrency)
    ok = 0
    fail = 0
    lock = asyncio.Lock()
    total = len(cars)

    async def upload(car):
        nonlocal ok, fail
        async with sem:
            result = None
            for attempt in range(3):
                result = await save_car_encrypted(token, car, uid, retries=1)
                if result.get('success'):
                    async with lock:
                        ok += 1
                        if progress:
                            progress.stage(f"cars {ok}/{total}")
                    return
                if not actions.retryable_error(result):
                    break
                await asyncio.sleep(0.5 * (attempt + 1))
            async with lock:
                fail += 1
                if progress:
                    progress.stage(f"cars {ok + fail}/{total}")

    await asyncio.gather(*(upload(c) for c in cars))
    return ok, fail


async def max_single(email, password, progress=None):
    """Max one account using the same step-by-step flow as the working Node version.

    Never aborts on a single step failure — counts failures and continues.
    """
    def mark(msg):
        if progress:
            progress.stage(msg)

    mark('login')
    auth = await actions.authenticate(email, password)
    if not auth.get('success'):
        raise BulkStageError('Login', auth.get('message') or 'Login failed', email, password)
    token, uid, record = auth['token'], auth['uid'], copy.deepcopy(auth['record'])

    failed_steps = []

    async def safe_save(patch, label, raw=False):
        """Save with retry. Never raises — returns True/False."""
        result = None
        for attempt in range(2):
            try:
                result = await (save_player_record_raw(token, patch, uid)
                                if raw else
                                save_player_record(token, patch, uid, record))
            except Exception as exc:
                result = {"success": False, "error": safe_error(exc)}
            if result.get('success'):
                if not raw:
                    record.update(copy.deepcopy(patch))
                await asyncio.sleep(0.2)
                return True
            if not actions.retryable_error(result):
                break
            await asyncio.sleep(0.3 * (attempt + 1))
        failed_steps.append(label)
        if progress:
            progress.error(f"{label}: {(result or {}).get('error') or 'rejected'}")
        return False

    # ── STEP 1: Money & Coins ──
    mark('money')
    await safe_save({'money': 50_000_000, 'coin': 500_000}, 'money')

    # ── STEP 2: Features (floats) ──
    mark('features')
    floats = list(record.get('floats') or [])
    while len(floats) < 54:
        floats.append(0)
    for idx in (3, 16, 27, 28, 29, 30, 31, 32, 33, 34):
        floats[idx] = 1
    await safe_save({'floats': floats}, 'features')

    # ── STEP 3: Animations ──
    mark('animations')
    animations = sorted(set(list(record.get('animations') or []) + list(range(301))))
    await safe_save({'animations': animations}, 'animations')

    # ── STEP 4: Wheels + Integers (houses) ──
    mark('wheels')
    wheels = sorted(set(list(record.get('wheels') or []) + list(range(73, 221))))
    integers = list(record.get('integers') or [])
    while len(integers) < 120:
        integers.append(0)
    for idx in (0, 1, 2, 3, 4, 5, 8, 110, 111, 112):
        integers[idx] = 1
    await safe_save({'wheels': wheels, 'integers': integers}, 'wheels+houses')

    # ── STEP 5: Levels ──
    mark('levels')
    levels = [0] + [120 if i == 43 else 1 for i in range(1, 110)]
    await safe_save({'LevelsDoneTime': levels}, 'levels')

    # ── STEP 6: Sirens ──
    mark('sirens')
    await safe_save({
        'boughtPoliceSirens': [1],
        'boughtPoliceLights': [0, 9, 511, 1, 4, 7, 2, 2, 1, 3, 2, 1, 4, 1, 0],
    }, 'sirens')

    # ── STEP 7: Clothes (raw) ──
    mark('clothes')
    try:
        from src.player_actions import _load_clothes_fields
        await safe_save(_load_clothes_fields(), 'clothes', raw=True)
    except Exception as exc:
        failed_steps.append('clothes')
        if progress:
            progress.error(f"clothes: {safe_error(exc)[:150]}")

    # ── STEP 8: Rank ──
    mark('rank')
    try:
        response = await actions._post_json(
            actions.SET_USER_RATING_URL,
            {'data': json.dumps({'RatingData': _rank_payload()})},
            {'Content-Type': 'application/json',
             'Authorization': f'Bearer {token}',
             'User-Agent': 'UnityPlayer/2022.3.62f2 (UnityWebRequest/1.0, libcurl/8.10.1-DEV)',
             'X-Unity-Version': '2022.3.62f2'})
        if not 200 <= response.status_code < 300:
            failed_steps.append(f'rank (HTTP {response.status_code})')
    except Exception as exc:
        failed_steps.append('rank')
        if progress:
            progress.error(f"rank: {safe_error(exc)[:150]}")

    # ── STEP 9: All cars ──
    mark('loading cars')
    existing = []
    try:
        existing_result = await fetch_cars(token)
        if existing_result.get('success'):
            existing = existing_result.get('record') or []
    except Exception:
        existing = []

    ids = {int(c['CarID']) for c in existing if str(c.get('CarID', '')).lstrip('-').isdigit()}
    cpm_id = resolve_cpm_id(record)
    missing = [prepare_car(c, cpm_id) for c in load_json('allStockCars')
               if int(c['CarID']) not in ids]

    if missing:
        mark('saving ownership')
        try:
            status = await save_car_status(token, uid,
                                           build_car_entries(merge_cars_by_id(existing, missing), cpm_id))
            if not status.get('success'):
                failed_steps.append('cars ownership')
        except Exception:
            failed_steps.append('cars ownership')

        await _upload_cars_parallel(token, uid, missing, email, password,
                                    concurrency=20, progress=progress)

    # Verification is best-effort
    mark('verifying')
    try:
        checked = await actions.verify_profile_patch(
            token, uid,
            {k: record[k] for k in ('Name', 'money', 'coin', 'floats', 'integers',
                                    'wheels', 'animations', 'LevelsDoneTime') if k in record})
        if not checked.get('success'):
            failed_steps.append('verify')
    except Exception:
        failed_steps.append('verify')

    return {
        'success': True,
        'carSuccess': len(missing),
        'carFail': 0,
        'failedSteps': failed_steps,
    }