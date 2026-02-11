import asyncio
import os
from datetime import datetime

import aiosqlite
from aiogram import Bot, Dispatcher
from aiogram.filters import CommandStart, Command
from aiogram.types import (
    Message,
    KeyboardButton,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext


BOT_TOKEN = os.getenv("BOT_TOKEN")
RESET_PASSWORD = os.getenv("RESET_PASSWORD", "")

DB_PATH = "participants.sqlite"

ADMIN_IDS = {922603146, 700087896}


# ================= FSM =================

class Reg(StatesGroup):
    waiting_consent = State()
    waiting_phone = State()
    waiting_name = State()


class AdminFSM(StatesGroup):
    waiting_password = State()


# ================= DATABASE =================

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS participants (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    telegram_id INTEGER UNIQUE NOT NULL,
    phone TEXT UNIQUE NOT NULL,
    first_name TEXT NOT NULL,
    consent INTEGER NOT NULL,
    created_at TEXT NOT NULL
);
"""


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(CREATE_TABLE_SQL)
        await db.commit()


async def get_user(telegram_id):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT id, phone, first_name FROM participants WHERE telegram_id = ?",
            (telegram_id,),
        )
        return await cur.fetchone()


async def add_user(telegram_id, phone, first_name, consent):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """
            INSERT INTO participants (telegram_id, phone, first_name, consent, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (telegram_id, phone, first_name, consent, datetime.utcnow().isoformat()),
        )
        await db.commit()
        return cur.lastrowid


async def reset_database():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM participants")
        await db.execute("DELETE FROM sqlite_sequence WHERE name='participants'")
        await db.commit()


# ================= KEYBOARDS =================

def start_kb():
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="🚀 Старт")]],
        resize_keyboard=True
    )


def consent_kb():
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="✅ Согласен"), KeyboardButton(text="❌ Не согласен")]],
        resize_keyboard=True,
        one_time_keyboard=True
    )


def contact_kb():
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📱 Отправить номер телефона", request_contact=True)]],
        resize_keyboard=True,
        one_time_keyboard=True
    )


def admin_kb():
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="🧹 Ресет базы")]],
        resize_keyboard=True
    )


def is_admin(user_id):
    return user_id in ADMIN_IDS


# ================= HANDLERS =================

async def start(message: Message, state: FSMContext):
    user = await get_user(message.from_user.id)
    if user:
        pid, phone, name = user
        await message.answer(
            f"Вы уже зарегистрированы ✅\n"
            f"Ваш номер: <b>{pid}</b>\n"
            f"Имя: {name}\n"
            f"Телефон: {phone}",
            parse_mode="HTML",
            reply_markup=start_kb()
        )
        return

    await message.answer("Нажмите «🚀 Старт» для регистрации.", reply_markup=start_kb())


async def begin_registration(message: Message, state: FSMContext):
    await message.answer("Вы согласны на обработку данных?", reply_markup=consent_kb())
    await state.set_state(Reg.waiting_consent)


async def consent_handler(message: Message, state: FSMContext):
    if message.text == "❌ Не согласен":
        await message.answer("Регистрация отменена.", reply_markup=start_kb())
        await state.clear()
        return

    if message.text != "✅ Согласен":
        await message.answer("Выберите кнопку.", reply_markup=consent_kb())
        return

    await state.update_data(consent=1)
    await message.answer("Отправьте номер телефона:", reply_markup=contact_kb())
    await state.set_state(Reg.waiting_phone)


async def phone_handler(message: Message, state: FSMContext):
    if not message.contact:
        await message.answer("Отправьте номер через кнопку.", reply_markup=contact_kb())
        return

    await state.update_data(phone=message.contact.phone_number)
    await message.answer("Введите имя:", reply_markup=ReplyKeyboardRemove())
    await state.set_state(Reg.waiting_name)


async def name_handler(message: Message, state: FSMContext):
    data = await state.get_data()
    phone = data["phone"]
    consent = data["consent"]

    pid = await add_user(message.from_user.id, phone, message.text.strip(), consent)

    await message.answer(
        f"Готово! Вы зарегистрированы ✅\n"
        f"Ваш порядковый номер: <b>{pid}</b>",
        parse_mode="HTML",
        reply_markup=start_kb()
    )
    await state.clear()


async def my_handler(message: Message):
    user = await get_user(message.from_user.id)
    if not user:
        await message.answer("Вы не зарегистрированы.", reply_markup=start_kb())
        return

    pid, phone, name = user
    await message.answer(
        f"Ваш номер: <b>{pid}</b>\nИмя: {name}\nТелефон: {phone}",
        parse_mode="HTML"
    )


async def admin_reset(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return

    await message.answer("Введите пароль для сброса:")
    await state.set_state(AdminFSM.waiting_password)


async def reset_password_handler(message: Message, state: FSMContext):
    if message.text != RESET_PASSWORD:
        await message.answer("Неверный пароль.")
        return

    await reset_database()
    await message.answer("База очищена ✅", reply_markup=admin_kb())
    await state.clear()


# ================= MAIN =================

async def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN not set")

    await init_db()

    bot = Bot(BOT_TOKEN)
    dp = Dispatcher()

    dp.message.register(start, CommandStart())
    dp.message.register(begin_registration, lambda m: m.text == "🚀 Старт")
    dp.message.register(my_handler, Command("my"))

    dp.message.register(consent_handler, Reg.waiting_consent)
    dp.message.register(phone_handler, Reg.waiting_phone)
    dp.message.register(name_handler, Reg.waiting_name)

    dp.message.register(admin_reset, lambda m: m.text == "🧹 Ресет базы")
    dp.message.register(reset_password_handler, AdminFSM.waiting_password)

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())


if __name__ == "__main__":
    asyncio.run(main())
