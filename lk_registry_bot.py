from __future__ import annotations

import json
import logging
import os
import warnings
from pathlib import Path
from typing import Dict, Iterable, List, Sequence

# Подавляем предупреждение о per_message=False с CallbackQueryHandler
# Должно быть ДО импорта telegram, чтобы фильтр успел примениться
warnings.filterwarnings(
    "ignore",
    message=".*per_message=False.*CallbackQueryHandler.*",
)
warnings.filterwarnings(
    "ignore",
    message=".*per_message=False.*",
    module="telegram.ext._conversationhandler",
)

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)
from telegram.error import Conflict

DATA_FILE = Path(__file__).with_name("lk_registry.json")
STATUSES = [
    "NEW-white",
    "NEW-not white",
    "в работе",
    "на отдыхе",
    "Вылет",
    "заблокирован",
]
STATUS_REQUIRING_FUNDS = {"Вылет", "заблокирован"}
MAX_BUTTONS_PER_MESSAGE = 25
DEFAULT_TOKEN = "8531740207:AAGFJeyQmj1mcHAO-0sFnRfhoAOqidCTlRU"

(
    ADD_BANK,
    ADD_NAME,
    ADD_PHONE,
    ADD_CARD,
    ADD_STATUS,
    ADD_FUNDS,
    FILTER_QUERY,
    EDIT_STATUS,
    EDIT_FUNDS,
) = range(9)

Record = Dict[str, str]

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


def load_records() -> List[Record]:
    if not DATA_FILE.exists():
        return []
    try:
        return json.loads(DATA_FILE.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        logger.warning("Файл данных поврежден, создаю новый пустой список.")
        return []


def save_records(records: List[Record]) -> None:
    DATA_FILE.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")


def generate_id(records: List[Record]) -> str:
    if not records:
        return "1"
    max_id = max(int(rec["id"]) for rec in records)
    return str(max_id + 1)


def get_status_emoji(status: str) -> str:
    """Возвращает эмодзи для статуса"""
    status_emojis = {
        "NEW-white": "🆕⚪",
        "NEW-not white": "🆕⚫",
        "в работе": "⚙️",
        "на отдыхе": "🏖️",
        "Вылет": "✈️",
        "заблокирован": "🚫",
    }
    return status_emojis.get(status, "📌")


def format_record(record: Record) -> str:
    """Форматирует запись с эмодзи и красивым оформлением"""
    status_emoji = get_status_emoji(record.get('status', ''))
    
    text = f"━━━━━━━━━━━━━━━━━━━━\n"
    text += f"🆔 <b>ID:</b> {record['id']}\n"
    text += f"🏦 <b>Банк:</b> {record['bank']}\n"
    text += f"👤 <b>ФИО:</b> {record['full_name']}\n"
    text += f"📞 <b>Телефон:</b> {record['phone']}\n"
    text += f"💳 <b>Карта:</b> {record['card']}\n"
    text += f"{status_emoji} <b>Статус:</b> {record['status']}\n"
    
    if record.get("remaining_funds"):
        text += f"💰 <b>Остаток:</b> {record['remaining_funds']}\n"
    
    text += f"━━━━━━━━━━━━━━━━━━━━"
    return text


def chunk_sequence(items: Sequence[Record], size: int) -> Iterable[Sequence[Record]]:
    for idx in range(0, len(items), size):
        yield items[idx : idx + size]


def build_main_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("➕ Добавить новый ЛК", callback_data="ADD_LK")],
            [InlineKeyboardButton("📋 Показать все ЛК", callback_data="LIST_LK")],
            [
                InlineKeyboardButton("🔍 Поиск", callback_data="SEARCH_ANY"),
                InlineKeyboardButton("👤 По имени", callback_data="FILTER_NAME"),
            ],
            [
                InlineKeyboardButton("🏦 По банку", callback_data="FILTER_BANK"),
                InlineKeyboardButton("📌 По статусу", callback_data="FILTER_STATUS"),
            ],
        ]
    )


def build_status_keyboard(prefix: str) -> InlineKeyboardMarkup:
    """Создает клавиатуру со статусами с эмодзи"""
    keyboard = []
    for idx, status in enumerate(STATUSES):
        emoji = get_status_emoji(status)
        keyboard.append([InlineKeyboardButton(
            text=f"{emoji} {status}", 
            callback_data=f"{prefix}_{idx}"
        )])
    return InlineKeyboardMarkup(keyboard)


def build_record_action_keyboard(record_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "✏️ Изменить статус", callback_data=f"EDIT_STATUS_{record_id}"
                )
            ],
            [
                InlineKeyboardButton("🗑 Удалить", callback_data=f"DELETE_{record_id}"),
                InlineKeyboardButton("⬅ Меню", callback_data="BACK_MENU"),
            ],
        ]
    )


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    welcome_text = (
        "👋 <b>Добро пожаловать!</b>\n\n"
        "Я помогу вам управлять базой данных ЛК (личных кабинетов).\n\n"
        "✨ <b>Что я умею:</b>\n"
        "• Добавлять новые ЛК\n"
        "• Просматривать все записи\n"
        "• Искать и фильтровать данные\n"
        "• Изменять статусы\n\n"
        "Выберите действие в меню ниже 👇"
    )
    await update.effective_chat.send_message(
        welcome_text,
        reply_markup=build_main_menu(),
        parse_mode="HTML",
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    help_text = (
        "📖 <b>Справка по командам</b>\n\n"
        "🔹 <b>Основные команды:</b>\n"
        "/start - показать главное меню\n"
        "/menu - открыть меню\n"
        "/help - эта справка\n\n"
        "🔹 <b>Работа с ЛК:</b>\n"
        "/add_lk - добавить новый ЛК\n"
        "/lk - показать все ЛК\n\n"
        "🔹 <b>Поиск и фильтры:</b>\n"
        "/search - поиск по всем полям\n"
        "/filter_name - фильтр по имени\n"
        "/filter_bank - фильтр по банку\n"
        "/filter_status - фильтр по статусу\n\n"
        "🔹 <b>Прочее:</b>\n"
        "/cancel - отменить текущую операцию\n\n"
        "💡 <i>Или просто используйте кнопки меню!</i>"
    )
    await update.effective_chat.send_message(
        help_text,
        parse_mode="HTML",
        reply_markup=build_main_menu(),
    )


async def show_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.effective_chat.send_message(
        "📱 <b>Главное меню</b>", 
        reply_markup=build_main_menu(),
        parse_mode="HTML",
    )


async def list_records(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    records = load_records()
    if not records:
        await update.effective_chat.send_message(
            "📭 <b>База данных пуста</b>\n\n"
            "Добавьте первый ЛК, используя кнопку ➕ в меню!",
            parse_mode="HTML",
            reply_markup=build_main_menu(),
        )
        return

    total = len(records)
    for chunk_idx, chunk in enumerate(chunk_sequence(records, MAX_BUTTONS_PER_MESSAGE)):
        keyboard = [
            [
                InlineKeyboardButton(
                    f"{get_status_emoji(rec.get('status', ''))} {rec['id']} | {rec['full_name'][:25]}",
                    callback_data=f"VIEW_{rec['id']}",
                )
            ]
            for rec in chunk
        ]
        if chunk_idx == 0:
            title = f"📋 <b>Всего ЛК: {total}</b>\n\nВыберите для просмотра:"
        else:
            title = f"📄 Продолжение ({chunk_idx + 1}):"
        await update.effective_chat.send_message(
            title, 
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="HTML",
        )


async def list_records_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    await list_records(update, context)


async def view_record(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    record_id = update.message.text.strip()
    await send_record_details(update, record_id)


async def view_record_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    record_id = query.data.split("VIEW_", maxsplit=1)[1]
    await send_record_details(update, record_id)


async def send_record_details(update: Update, record_id: str) -> None:
    record = next((rec for rec in load_records() if rec["id"] == record_id), None)
    if not record:
        await update.effective_chat.send_message(
            "❌ <b>Ошибка</b>\n\nЛК с таким ID не найден.",
            parse_mode="HTML",
            reply_markup=build_main_menu(),
        )
        return
    text = format_record(record)
    keyboard = build_record_action_keyboard(record_id)

    if update.callback_query:
        await update.callback_query.message.reply_text(
            text, 
            reply_markup=keyboard,
            parse_mode="HTML",
        )
    else:
        await update.effective_chat.send_message(
            text, 
            reply_markup=keyboard,
            parse_mode="HTML",
        )


async def start_add(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if update.callback_query:
        await update.callback_query.answer()
    context.user_data["new_record"] = {}
    # список ID служебных сообщений, которые нужно удалить после завершения ввода
    context.user_data["cleanup_messages"] = []
    msg = await update.effective_chat.send_message(
        "➕ <b>Добавление нового ЛК</b>\n\n"
        "🏦 <b>Шаг 1/5:</b> Укажите название банка:",
        parse_mode="HTML",
    )
    context.user_data["cleanup_messages"].append(msg.message_id)
    return ADD_BANK


async def add_bank(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    bank = update.message.text.strip()
    cleanup = context.user_data.setdefault("cleanup_messages", [])
    if not bank:
        msg = await update.message.reply_text(
            "⚠️ Поле не может быть пустым!\n\n🏦 Укажите название банка:",
            parse_mode="HTML",
        )
        cleanup.append(msg.message_id)
        return ADD_BANK
    context.user_data["new_record"]["bank"] = bank
    try:
        await update.message.delete()
    except Exception:
        pass
    msg = await update.message.reply_text(
        "👤 <b>Шаг 2/5:</b> Укажите ФИО:",
        parse_mode="HTML",
    )
    cleanup.append(msg.message_id)
    return ADD_NAME


async def add_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    name = update.message.text.strip()
    cleanup = context.user_data.setdefault("cleanup_messages", [])
    if not name:
        msg = await update.message.reply_text(
            "⚠️ Поле не может быть пустым!\n\n👤 Укажите ФИО:",
            parse_mode="HTML",
        )
        cleanup.append(msg.message_id)
        return ADD_NAME
    context.user_data["new_record"]["full_name"] = name
    try:
        await update.message.delete()
    except Exception:
        pass
    msg = await update.message.reply_text(
        "📞 <b>Шаг 3/5:</b> Укажите номер телефона:",
        parse_mode="HTML",
    )
    cleanup.append(msg.message_id)
    return ADD_PHONE


async def add_phone(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    phone = update.message.text.strip()
    cleanup = context.user_data.setdefault("cleanup_messages", [])
    if not phone:
        msg = await update.message.reply_text(
            "⚠️ Поле не может быть пустым!\n\n📞 Укажите номер телефона:",
            parse_mode="HTML",
        )
        cleanup.append(msg.message_id)
        return ADD_PHONE
    context.user_data["new_record"]["phone"] = phone
    try:
        await update.message.delete()
    except Exception:
        pass
    msg = await update.message.reply_text(
        "💳 <b>Шаг 4/5:</b> Укажите номер карты:",
        parse_mode="HTML",
    )
    cleanup.append(msg.message_id)
    return ADD_CARD


async def add_card(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    card = update.message.text.strip()
    cleanup = context.user_data.setdefault("cleanup_messages", [])
    if not card:
        msg = await update.message.reply_text(
            "⚠️ Поле не может быть пустым!\n\n💳 Укажите номер карты:",
            parse_mode="HTML",
        )
        cleanup.append(msg.message_id)
        return ADD_CARD
    context.user_data["new_record"]["card"] = card
    try:
        await update.message.delete()
    except Exception:
        pass
    msg = await update.message.reply_text(
        "📌 <b>Шаг 5/5:</b> Выберите статус:",
        reply_markup=build_status_keyboard("STATUS_CHOICE"),
        parse_mode="HTML",
    )
    cleanup.append(msg.message_id)
    return ADD_STATUS


async def add_status_selected(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    cleanup = context.user_data.setdefault("cleanup_messages", [])
    idx = int(query.data.split("STATUS_CHOICE_", maxsplit=1)[1])
    status = STATUSES[idx]
    context.user_data["new_record"]["status"] = status
    if status in STATUS_REQUIRING_FUNDS:
        msg = await query.message.reply_text(
            "💰 Укажите остаток денежных средств:",
            parse_mode="HTML",
        )
        cleanup.append(msg.message_id)
        return ADD_FUNDS
    await finalize_record(update, context)
    return ConversationHandler.END


async def add_funds(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    funds = update.message.text.strip()
    cleanup = context.user_data.setdefault("cleanup_messages", [])
    if not funds:
        msg = await update.message.reply_text(
            "⚠️ Поле не может быть пустым!\n\n💰 Укажите сумму или комментарий:",
            parse_mode="HTML",
        )
        cleanup.append(msg.message_id)
        return ADD_FUNDS
    context.user_data["new_record"]["remaining_funds"] = funds
    try:
        await update.message.delete()
    except Exception:
        pass
    await finalize_record(update, context)
    return ConversationHandler.END


async def finalize_record(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    record = context.user_data.pop("new_record", {})
    if not record:
        await update.effective_chat.send_message(
            "❌ <b>Ошибка</b>\n\nНет данных для сохранения.",
            parse_mode="HTML",
        )
        return
    records = load_records()
    record["id"] = generate_id(records)
    records.append(record)
    save_records(records)

    # Удаляем служебные сообщения диалога добавления
    cleanup_ids = context.user_data.pop("cleanup_messages", [])
    chat_id = update.effective_chat.id
    for mid in cleanup_ids:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=mid)
        except Exception:
            pass

    # Итоговое сообщение с краткой инфой и кнопкой для просмотра всех ЛК
    from telegram import InlineKeyboardMarkup, InlineKeyboardButton

    keyboard = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📋 Все ЛК", callback_data="LIST_LK")],
            [InlineKeyboardButton("⬅ Меню", callback_data="BACK_MENU")],
        ]
    )
    status_emoji = get_status_emoji(record.get('status', ''))
    await update.effective_chat.send_message(
        f"✅ <b>Успешно!</b>\n\n"
        f"ЛК <b>#{record['id']}</b> добавлен в базу данных.\n\n"
        f"{status_emoji} Статус: {record['status']}\n"
        f"👤 {record['full_name']}",
        reply_markup=keyboard,
        parse_mode="HTML",
    )


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data.pop("new_record", None)
    cleanup_ids = context.user_data.pop("cleanup_messages", [])
    # Удаляем сообщение пользователя/кнопок, чтобы оставить только меню
    try:
        if update.callback_query:
            await update.callback_query.message.delete()
        elif update.message:
            await update.message.delete()
    except Exception:
        pass

    chat_id = update.effective_chat.id
    for mid in cleanup_ids:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=mid)
        except Exception:
            pass

    await update.effective_chat.send_message(
        "❌ <b>Операция отменена</b>", 
        reply_markup=build_main_menu(),
        parse_mode="HTML",
    )
    return ConversationHandler.END


async def start_filter_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if update.callback_query:
        await update.callback_query.answer()
    context.user_data["filter_type"] = "full_name"
    await update.effective_chat.send_message(
        "👤 <b>Поиск по имени</b>\n\n"
        "Введите имя или его часть:",
        parse_mode="HTML",
    )
    return FILTER_QUERY


async def start_filter_bank(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if update.callback_query:
        await update.callback_query.answer()
    context.user_data["filter_type"] = "bank"
    await update.effective_chat.send_message(
        "🏦 <b>Поиск по банку</b>\n\n"
        "Введите название банка или его часть:",
        parse_mode="HTML",
    )
    return FILTER_QUERY


async def start_search_any(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if update.callback_query:
        await update.callback_query.answer()
    context.user_data["filter_type"] = "any"
    await update.effective_chat.send_message(
        "🔍 <b>Поиск по всем полям</b>\n\n"
        "Введите поисковый запрос (имя, банк, телефон, карта и т.д.):",
        parse_mode="HTML",
    )
    return FILTER_QUERY


async def filter_query_received(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    term = update.message.text.strip().lower()
    if not term:
        await update.message.reply_text(
            "⚠️ Запрос не может быть пустым!\n\nПовторите поиск:",
            parse_mode="HTML",
        )
        return FILTER_QUERY
    filter_type = context.user_data.pop("filter_type", "any")
    records = load_records()
    if filter_type == "any":
        matches = [rec for rec in records if any(term in str(val).lower() for val in rec.values())]
    else:
        matches = [rec for rec in records if term in rec.get(filter_type, "").lower()]
    if not matches:
        await update.message.reply_text(
            "🔍 <b>Результаты поиска</b>\n\n"
            "❌ Совпадений не найдено.",
            parse_mode="HTML",
            reply_markup=build_main_menu(),
        )
        return ConversationHandler.END
    await update.message.reply_text(
        f"🔍 <b>Найдено записей:</b> {len(matches)}",
        parse_mode="HTML",
    )
    for chunk_idx, chunk in enumerate(chunk_sequence(matches, MAX_BUTTONS_PER_MESSAGE)):
        keyboard = [
            [InlineKeyboardButton(
                f"{get_status_emoji(rec.get('status', ''))} {rec['id']} | {rec['full_name'][:25]}", 
                callback_data=f"VIEW_{rec['id']}"
            )]
            for rec in chunk
        ]
        if chunk_idx == 0:
            title = f"📋 <b>Результаты поиска ({len(matches)}):</b>"
        else:
            title = f"📄 Продолжение ({chunk_idx + 1}):"
        await update.effective_chat.send_message(
            title, 
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="HTML",
        )
    return ConversationHandler.END


async def filter_status_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.callback_query:
        await update.callback_query.answer()
    await update.effective_chat.send_message(
        "📌 <b>Фильтр по статусу</b>\n\nВыберите статус:",
        reply_markup=build_status_keyboard("FILTER_STATUS_VALUE"),
        parse_mode="HTML",
    )


async def filter_status_selected(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    idx = int(query.data.split("FILTER_STATUS_VALUE_", maxsplit=1)[1])
    status = STATUSES[idx]
    records = [rec for rec in load_records() if rec.get("status") == status]
    status_emoji = get_status_emoji(status)
    if not records:
        await query.message.reply_text(
            f"🔍 <b>Результаты фильтра</b>\n\n"
            f"❌ Совпадений не найдено для статуса: {status_emoji} {status}",
            parse_mode="HTML",
            reply_markup=build_main_menu(),
        )
        return
    await query.message.reply_text(
        f"🔍 <b>Найдено записей:</b> {len(records)}\n"
        f"📌 Статус: {status_emoji} {status}",
        parse_mode="HTML",
    )
    for chunk_idx, chunk in enumerate(chunk_sequence(records, MAX_BUTTONS_PER_MESSAGE)):
        keyboard = [
            [InlineKeyboardButton(
                f"{status_emoji} {rec['id']} | {rec['full_name'][:25]}", 
                callback_data=f"VIEW_{rec['id']}"
            )]
            for rec in chunk
        ]
        if chunk_idx == 0:
            title = f"📋 <b>{status_emoji} {status} ({len(records)}):</b>"
        else:
            title = f"📄 Продолжение ({chunk_idx + 1}):"
        await query.message.reply_text(
            title, 
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="HTML",
        )


async def back_to_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    # Удаляем сообщение с деталями ЛК и кнопками
    try:
        await query.message.delete()
    except Exception:
        pass
    await query.message.chat.send_message(
        "📱 <b>Главное меню</b>", 
        reply_markup=build_main_menu(),
        parse_mode="HTML",
    )


async def delete_record_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    record_id = query.data.split("DELETE_", maxsplit=1)[1]
    records = load_records()
    record = next((rec for rec in records if rec.get("id") == record_id), None)
    new_records = [rec for rec in records if rec.get("id") != record_id]
    if len(new_records) == len(records):
        await query.message.reply_text(
            "❌ <b>Ошибка</b>\n\nЛК не найден.",
            parse_mode="HTML",
            reply_markup=build_main_menu(),
        )
        return
    save_records(new_records)

    # Удаляем сообщение с ЛК
    try:
        await query.message.delete()
    except Exception:
        pass

    name = record.get('full_name', '') if record else ''
    await query.message.chat.send_message(
        f"🗑 <b>Удалено</b>\n\n"
        f"ЛК <b>#{record_id}</b> успешно удален из базы данных.\n"
        f"👤 {name}",
        reply_markup=build_main_menu(),
        parse_mode="HTML",
    )


async def edit_status_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    record_id = query.data.split("EDIT_STATUS_", maxsplit=1)[1]
    context.user_data["edit_record_id"] = record_id
    await query.message.reply_text(
        "✏️ <b>Изменение статуса</b>\n\nВыберите новый статус:",
        reply_markup=build_status_keyboard("EDIT_STATUS_CHOICE"),
        parse_mode="HTML",
    )
    return EDIT_STATUS


async def edit_status_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    idx = int(query.data.split("EDIT_STATUS_CHOICE_", maxsplit=1)[1])
    status = STATUSES[idx]
    record_id = context.user_data.get("edit_record_id")
    if not record_id:
        await query.message.reply_text("Нет выбранного ЛК для изменения статуса.")
        return ConversationHandler.END

    if status in STATUS_REQUIRING_FUNDS:
        context.user_data["edit_new_status"] = status
        await query.message.reply_text(
            "💰 Укажите остаток денежных средств:",
            parse_mode="HTML",
        )
        return EDIT_FUNDS

    records = load_records()
    for rec in records:
        if rec.get("id") == record_id:
            rec["status"] = status
            rec.pop("remaining_funds", None)
            break
    else:
        await query.message.reply_text("ЛК не найден.")
        return ConversationHandler.END

    save_records(records)
    context.user_data.pop("edit_record_id", None)
    status_emoji = get_status_emoji(status)
    await query.message.reply_text(
        f"✅ <b>Статус обновлен</b>\n\n"
        f"{status_emoji} Новый статус: {status}",
        parse_mode="HTML",
    )
    return ConversationHandler.END


async def edit_status_funds(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    funds = update.message.text.strip()
    if not funds:
        await update.message.reply_text(
            "⚠️ Поле не может быть пустым!\n\n💰 Укажите сумму или комментарий:",
            parse_mode="HTML",
        )
        return EDIT_FUNDS

    record_id = context.user_data.pop("edit_record_id", None)
    status = context.user_data.pop("edit_new_status", None)
    if not record_id or not status:
        await update.message.reply_text(
            "❌ <b>Ошибка</b>\n\nНет данных для изменения статуса.",
            parse_mode="HTML",
        )
        return ConversationHandler.END

    records = load_records()
    for rec in records:
        if rec.get("id") == record_id:
            rec["status"] = status
            rec["remaining_funds"] = funds
            break
    else:
        await update.message.reply_text(
            "❌ <b>Ошибка</b>\n\nЛК не найден.",
            parse_mode="HTML",
        )
        return ConversationHandler.END

    save_records(records)
    status_emoji = get_status_emoji(status)
    await update.message.reply_text(
        f"✅ <b>Обновлено</b>\n\n"
        f"{status_emoji} Статус: {status}\n"
        f"💰 Остаток: {funds}",
        parse_mode="HTML",
    )
    return ConversationHandler.END


def run_bot() -> None:
    # Можно переопределить токен через переменную окружения BOT_TOKEN,
    # иначе используется токен по умолчанию из константы.
    token = os.getenv("BOT_TOKEN") or DEFAULT_TOKEN
    if not token:
        raise RuntimeError("Не найден BOT_TOKEN в переменных окружения.")

    application = ApplicationBuilder().token(token).build()

    add_conversation = ConversationHandler(
        entry_points=[
            CommandHandler("add_lk", start_add),
            CommandHandler("add", start_add),
            CallbackQueryHandler(start_add, pattern="^ADD_LK$"),
        ],
        states={
            ADD_BANK: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_bank)],
            ADD_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_name)],
            ADD_PHONE: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_phone)],
            ADD_CARD: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_card)],
            ADD_STATUS: [CallbackQueryHandler(add_status_selected, pattern="^STATUS_CHOICE_\\d+$")],
            ADD_FUNDS: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_funds)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        name="add_lk",
        per_message=False,
    )

    filter_conversation = ConversationHandler(
        entry_points=[
            CommandHandler("filter_name", start_filter_name),
            CommandHandler("filter_bank", start_filter_bank),
            CommandHandler("search", start_search_any),
            CallbackQueryHandler(start_filter_name, pattern="^FILTER_NAME$"),
            CallbackQueryHandler(start_filter_bank, pattern="^FILTER_BANK$"),
            CallbackQueryHandler(start_search_any, pattern="^SEARCH_ANY$"),
        ],
        states={
            FILTER_QUERY: [MessageHandler(filters.TEXT & ~filters.COMMAND, filter_query_received)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        name="filters",
        per_message=False,
    )

    edit_status_conversation = ConversationHandler(
        entry_points=[CallbackQueryHandler(edit_status_start, pattern="^EDIT_STATUS_\\d+$")],
        states={
            EDIT_STATUS: [
                CallbackQueryHandler(
                    edit_status_choice, pattern="^EDIT_STATUS_CHOICE_\\d+$"
                )
            ],
            EDIT_FUNDS: [MessageHandler(filters.TEXT & ~filters.COMMAND, edit_status_funds)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        name="edit_status",
        per_message=False,
    )

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("menu", show_menu))
    application.add_handler(CommandHandler("lk", list_records))
    application.add_handler(CommandHandler("list", list_records))
    application.add_handler(CommandHandler("view", view_record))
    application.add_handler(add_conversation)
    application.add_handler(filter_conversation)
    application.add_handler(edit_status_conversation)
    application.add_handler(CallbackQueryHandler(list_records_callback, pattern="^LIST_LK$"))
    application.add_handler(CallbackQueryHandler(filter_status_prompt, pattern="^FILTER_STATUS$"))
    application.add_handler(CallbackQueryHandler(filter_status_selected, pattern="^FILTER_STATUS_VALUE_\\d+$"))
    application.add_handler(CallbackQueryHandler(view_record_callback, pattern="^VIEW_\\d+$"))
    application.add_handler(CallbackQueryHandler(delete_record_callback, pattern="^DELETE_\\d+$"))
    application.add_handler(CallbackQueryHandler(back_to_menu, pattern="^BACK_MENU$"))

    # Обработка ошибок
    async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Обработчик ошибок"""
        error = context.error
        if isinstance(error, Conflict):
            # Conflict - это нормально при перезапуске, просто логируем как предупреждение
            logger.warning(
                "Конфликт: другой экземпляр бота пытается получить обновления. "
                "Это нормально при перезапуске. Бот продолжит работу."
            )
            return  # Не логируем как ошибку
        else:
            logger.error(f"Необработанная ошибка: {error}", exc_info=error)
    
    application.add_error_handler(error_handler)
    
    # Настраиваем логирование для telegram.ext, чтобы не показывать Conflict как ошибку
    telegram_logger = logging.getLogger("telegram.ext")
    telegram_logger.setLevel(logging.WARNING)  # Показываем только WARNING и выше
    
    logger.info("Бот запущен и ожидает обновления.")
    application.run_polling(
        drop_pending_updates=True,  # Игнорируем старые обновления при перезапуске
    )


if __name__ == "__main__":
    run_bot()
