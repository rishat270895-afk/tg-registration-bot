import asyncio
import os
import tempfile
from datetime import datetime, date, timedelta

import aiosqlite
from openpyxl import Workbook
from openpyxl.utils import get_column_letter

from aiogram import Bot, Dispatcher
from aiogram.filters import CommandStart, Command
from aiogram.types import (
    Message,
    KeyboardButton,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    FSInputFile,
)
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext


# ================== CONFIG ==================
BOT_TOKEN = os.getenv("BOT_TOKEN")
RESET_PASSWORD = os.getenv("RESET_PASSWORD", "")  # set in Railway -> Variables

DB_PATH = "participants.sqlite"
# If you enabled Railway Volume mounted to /data, use this instead:
# DB_PATH = "/data/participants.sqlite"

# Put your Telegram user_ids here (2 admins supported)
ADMIN_IDS = {922603146, 700087896}


# ================== FSM ==================
class Reg(StatesGroup):
    waiting_consent = State()
    waiting_phone = State()
    waiting_first_name = State()
    # last_name removed


class AdminFSM(StatesGroup):
    reset_wait_password = State()
    reset_confirm = State()
    export_wait_from = State()
    export_wait_to = State()
    list_wait_from = State()
    list_wait_to = State()


# ================== DB ==================
# NOTE: keep last_name column for backward compatibility with your old DB.
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS participants (
    id INTEGER PRIMARY KEY AUTOINCREMENT,       -- participant number (1,2,3,...)
    telegram_id INTEGER UNIQUE NOT NULL,
    phone TEXT UNIQUE NOT NULL,                 -- unique by phone
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,                    -- kept for compatibility, stored as "" for new users
    consent INTEGER NOT NULL,                   -- 1 = agreed
    created_at TEXT NOT NULL                    -- UTC ISO string
);
"""


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(CREATE_TABLE_SQL)
        await db.commit()


async def get_by_telegram_id(telegram_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT id, telegram_id, phone, first_name, last_name, consent, created_at "
            "FROM participants WHERE telegram_id = ?",
            (telegram_id,),
        )
        return await cur.fetchone()


async def get_by_phone(phone: str):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT id, telegram_id, phone, first_name, last_name, consent, created_at "
            "FROM participants WHERE phone = ?",
            (phone,),
        )
        return await cur.fetchone()


async def insert_participant(telegram_id: int, phone: str, first_name: str, consent: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """
            INSERT INTO participants (telegram_id, phone, first_name, last_name, consent, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (telegram_id, phone, first_name, "", consent, datetime.utcnow().isoformat()),
        )
        await db.commit()
        return cur.lastrowid


def _range_where_clause(from_iso: str | None, to_iso: str | None):
    # ISO timestamps can be compared lexicographically
    if from_iso and to_iso:
        return "WHERE created_at >= ? AND created_at < ?", (from_iso, to_iso)
    if from_iso:
        return "WHERE created_at >= ?", (from_iso,)
    if to_iso:
        return "WHERE created_at < ?", (to_iso,)
    return "", ()


async def fetch_participants(from_iso: str | None = None, to_iso: str | None = None):
    where_sql, params = _range_where_clause(from_iso, to_iso)
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            f"SELECT id, telegram_id, phone, first_name, last_name, consent, created_at "
            f"FROM participants {where_sql} ORDER BY id ASC",
            params
        )
        return await cur.fetchall()


async def count_participants(from_iso: str | None = None, to_iso: str | None = None) -> int:
    where_sql, params = _range_where_clause(from_iso, to_iso)
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(f"SELECT COUNT(*) FROM participants {where_sql}", params)
        (cnt,) = await cur.fetchone()
        return cnt


async def reset_database():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM participants")
        await db.execute("DELETE FROM sqlite_sequence WHERE name='participants'")
        await db.commit()


# ================== Keyboards ==================
def user_start_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="🚀 Старт")]],
        resize_keyboard=True
    )


def consent_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="✅ Согласен"), KeyboardButton(text="❌ Не согласен")]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def contact_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📱 Отправить номер телефона", request_contact=True)]],
        resize_keyboard=True,
        one_time_keyboard=True
    )


def admin_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📋 Список"), KeyboardButton(text="📤 Экспорт")],
            [KeyboardButton(text="📤 Экспорт сегодня")],
            [KeyboardButton(text="🧹 Ресет базы")],
            [KeyboardButton(text="⬅️ Закрыть меню")]
        ],
        resize_keyboard=True
    )


def admin_reset_confirm_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="✅ Да, стереть всё"), KeyboardButton(text="❌ Отмена")]],
        resize_keyboard=True,
        one_time_keyboard=True
    )


def admin_filter_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Сегодня"), KeyboardButton(text="Все")],
            [KeyboardButton(text="Диапазон дат")],
            [KeyboardButton(text="⬅️ Назад")]
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )


def admin_back_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="⬅️ Назад")]],
        resize_keyboard=True,
        one_time_keyboard=True
    )


# ================== Helpers ==================
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def normalize_phone(phone: str) -> str:
    return phone.strip().replace(" ", "")


def parse_ymd(s: str) -> date | None:
    try:
        return datetime.strptime(s.strip(), "%Y-%m-%d").date()
    except Exception:
        return None


def day_range_utc(d: date) -> tuple[str, str]:
    start = datetime(d.year, d.month, d.day)
    end = start + timedelta(days=1)
    return start.isoformat(), end.isoformat()


def range_from_args(args_text: str) -> tuple[str | None, str | None, str | None]:
    """
    Accepts:
      - "" -> no filter
      - "today" -> today's UTC day
      - "YYYY-MM-DD YYYY-MM-DD" -> inclusive date range
    Returns: (from_iso, to_iso, error)
    """
    t = (args_text or "").strip()
    if not t:
        return None, None, None

    if t.lower() == "today":
        f, to = day_range_utc(datetime.utcnow().date())
        return f, to, None

    parts = t.split()
    if len(parts) == 2:
        d1 = parse_ymd(parts[0])
        d2 = parse_ymd(parts[1])
        if not d1 or not d2:
            return None, None, "Неверный формат даты. Используй YYYY-MM-DD YYYY-MM-DD"
        f = datetime(d1.year, d1.month, d1.day).isoformat()
        to_excl_date = d2 + timedelta(days=1)
        to = datetime(to_excl_date.year, to_excl_date.month, to_excl_date.day).isoformat()
        return f, to, None

    return None, None, "Неверные аргументы. Примеры: /export today или /export 2026-02-01 2026-02-06"


def autosize_worksheet_columns(ws):
    for col in range(1, ws.max_column + 1):
        max_len = 0
        col_letter = get_column_letter(col)
        for row in range(1, ws.max_row + 1):
            val = ws.cell(row=row, column=col).value
            if val is None:
                continue
            max_len = max(max_len, len(str(val)))
        ws.column_dimensions[col_letter].width = min(max_len + 2, 50)


async def export_to_excel_and_send(message: Message, rows, suffix: str):
    """
    Save to /tmp because Railway filesystem may be read-only in app dir.
    """
    wb = Workbook()
    ws = wb.active
    ws.title = "Participants"

    # NO LAST NAME in export
    ws.append(["Номер", "Telegram ID", "Телефон", "Имя", "Согласие", "Дата регистрации (UTC)"])
    for r in rows:
        pid, tid, phone, fn, _ln, consent, created_at = r
        ws.append([pid, tid, phone, fn, "Да" if consent else "Нет", created_at])

    autosize_worksheet_columns(ws)

    with tempfile.NamedTemporaryFile(prefix=f"participants_{suffix}_", suffix=".xlsx", delete=False, dir="/tmp") as tmp:
        tmp_path = tmp.name

    wb.save(tmp_path)

    try:
        await message.answer_document(
            FSInputFile(tmp_path),
            caption=f"Выгрузка участников: {len(rows)} записей"
        )
    finally:
        try:
            os.remove(tmp_path)
        except Exception:
            pass


# ================== Shared admin output helpers (NO fake Message) ==================
async def send_list(message: Message, args: str):
    from_iso, to_iso, err = range_from_args(args)
    if err:
        await message.answer(err)
        return

    cnt = await count_participants(from_iso, to_iso)
    rows = await fetch_participants(from_iso, to_iso)

    preview = rows[:30]
    # NO LAST NAME in list
    lines = [f"{pid}. {fn} — {phone}" for (pid, _tid, phone, fn, _ln, _consent, _created_at) in preview]

    label = "все записи"
    if args.strip().lower() == "today":
        label = "сегодня (UTC)"
    elif args.strip():
        label = f"диапазон: {args.strip()} (UTC)"

    text = f"Фильтр: <b>{label}</b>\nВсего участников: <b>{cnt}</b>\n"
    text += "Первые записи:\n" + ("\n".join(lines) if lines else "Пока пусто.")
    if cnt > len(lines):
        text += f"\n…и ещё {cnt - len(lines)}"

    await message.answer(text, parse_mode="HTML")


async def send_export(message: Message, args: str):
    from_iso, to_iso, err = range_from_args(args)
    if err:
        await message.answer(err)
        return

    rows = await fetch_participants(from_iso, to_iso)
    if not rows:
        await message.answer("По этому фильтру нет участников.")
        return

    suffix = "all"
    if args.strip().lower() == "today":
        suffix = "today_utc"
    elif args.strip():
        suffix = args.strip().replace(" ", "_")

    await export_to_excel_and_send(message, rows, suffix)


# ================== Public flow ==================
async def show_user_start(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "Нажмите кнопку «🚀 Старт», чтобы начать регистрацию.",
        reply_markup=user_start_kb()
    )


async def start(message: Message, state: FSMContext):
    existing = await get_by_telegram_id(message.from_user.id)
    if existing:
        pid, _tid, phone, fn, _ln, _consent, _created_at = existing
        await state.clear()
        await message.answer(
            f"Вы уже зарегистрированы ✅\n"
            f"Номер участника: <b>{pid}</b>\n"
            f"Имя: {fn}\n"
            f"Телефон: {phone}\n\n"
            "Нажмите «🚀 Старт», чтобы открыть начало.",
            parse_mode="HTML",
            reply_markup=user_start_kb()
        )
        return

    await show_user_start(message, state)


async def on_user_start_button(message: Message, state: FSMContext):
    existing = await get_by_telegram_id(message.from_user.id)
    if existing:
        pid = existing[0]
        await state.clear()
        await message.answer(
            f"Вы уже зарегистрированы ✅\nНомер участника: <b>{pid}</b>\n\nКоманда /my — показать номер.",
            parse_mode="HTML",
            reply_markup=user_start_kb()
        )
        return

    await message.answer(
        "Перед регистрацией нужно согласие на обработку да
