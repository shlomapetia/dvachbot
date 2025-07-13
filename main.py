from __future__ import annotations
import asyncio
import gc
import io
import json
import logging
import time
import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import re 
import glob
import random
import secrets
import pickle
import gzip
from aiogram import types
import weakref
from collections import deque, defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import Tuple
import aiohttp
from aiohttp import web
import shutil
from aiogram.types import Message
from aiogram.utils.media_group import MediaGroupBuilder
from aiogram.exceptions import TelegramRetryAfter
from asyncio import Semaphore
from aiogram import Bot, Dispatcher, F, types
from aiogram.exceptions import (
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramNetworkError,
    TelegramConflictError,
)
from aiogram.filters import Command
from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
import subprocess
import signal
from datetime import datetime, timedelta, timezone, UTC
from japanese_translator import anime_transform, get_random_anime_image
from ukrainian_mode import ukrainian_transform, UKRAINIAN_PHRASES
import deanonymizer 
from conan import conan_roaster, conan_phrase
from zaputin_mode import zaputin_transform, PATRIOTIC_PHRASES 
from deanonymizer import process_deanon_command, DEANON_SURNAMES, DEANON_CITIES, DEANON_PROFESSIONS, DEANON_FETISHES, DEANON_DETAILS, generate_deanon_info
from help_text import HELP_TEXT
from help_broadcaster import help_broadcaster

# ========== Глобальные настройки досок ==========
# Идентификаторы досок. 'b' - основная.
BOARDS = ['b', 'po', 'a', 'sex', 'vg']

# Информация о каждой доске
BOARD_INFO = {
    'b': {"name": "/b/", "description": "Бред", "username": "@dvach_chatbot"},
    'po': {"name": "/po/", "description": "Политика", "username": "@dvach_po_chatbot"},
    'a': {"name": "/a/", "description": "Аниме", "username": "@dvach_a_chatbot"},
    'sex': {"name": "/sex/", "description": "Сексач", "username": "@dvach_sex_chatbot"},
    'vg': {"name": "/vg/", "description": "Видеоигры", "username": "@dvach_vg_chatbot"},
}

BOT_TOKENS = {
    'b': os.environ.get('BOT_TOKEN'), # Ваш старый основной токен
    'po': os.environ.get('BOT_TOKEN_PO'),
    'a': os.environ.get('BOT_TOKEN_A'),
    'sex': os.environ.get('BOT_TOKEN_SEX'),
    'vg': os.environ.get('BOT_TOKEN_VG'),
}

BOT_TOKENS = {k: v for k, v in BOT_TOKENS.items() if v is not None}

bots = {}
bot_id_to_board = {}

# Очереди сообщений для каждой доски
message_queues = {board: asyncio.Queue(maxsize=9000) for board in BOT_TOKENS.keys()}

# ========== Глобальные переменные и настройки ==========
is_shutting_down = False
git_executor = ThreadPoolExecutor(max_workers=1)
send_executor = ThreadPoolExecutor(max_workers=100)
git_semaphore = asyncio.Semaphore(1)

# --- ГЛОБАЛЬНЫЙ СЧЕТЧИК ПОСТОВ (ОДИН НА ВСЕХ!) ---
global_post_counter = 0

# --- СЛОВАРИ ДЛЯ ХРАНЕНИЯ ДАННЫХ КАЖДОЙ ДОСКИ ---
# Каждый элемент будет инициализирован для каждой доски ('b', 'po', и т.д.)
boards_state = {}
mutes = {}
shadow_mutes = {}
spam_tracker = {}
spam_violations = {}
post_to_messages = {}
message_to_post = {}
last_user_msgs = {}
last_texts = {}
last_stickers = {}
sticker_times = {}

# --- Режимы чата (теперь для каждой доски отдельно) ---
board_modes = {}

# --- ГЛОБАЛЬНОЕ ХРАНИЛИЩЕ ПОСТОВ ---
# Оно должно быть единым, т.к. нумерация постов сквозная
messages_storage = {}

MODE_COOLDOWN = 3600  # 1 час в секундах
MAX_ACTIVE_USERS_IN_MEMORY = 5000

# Отключаем стандартную обработку сигналов в aiogram
os.environ["AIORGRAM_DISABLE_SIGNAL_HANDLERS"] = "1"

# Правила спама (общие для всех досок)
SPAM_RULES = {
    'text': {
        'max_repeats': 3,
        'min_length': 2,
        'window_sec': 15,
        'max_per_window': 6,
        'penalty': [60, 300, 600]
    },
    'sticker': {
        'max_per_window': 6,
        'window_sec': 15,
        'penalty': [60, 600, 900]
    },
    'animation': {
        'max_per_window': 5,
        'window_sec': 24,
        'penalty': [60, 600, 900]
    }
}

def initialize_boards_data():
    """Создает пустые структуры данных для каждой активной доски."""
    global global_post_counter
    if os.path.exists('global_counter.json'):
        with open('global_counter.json', 'r') as f:
            data = json.load(f)
            global_post_counter = data.get('post_counter', 0)

    for board_id in BOT_TOKENS.keys():
        boards_state[board_id] = {
            'users_data': {'active': set(), 'banned': set()},
            'message_counter': {},
            'settings': {'dvach_enabled': False},
            'user_activity': {}
        }
        mutes[board_id] = {}
        shadow_mutes[board_id] = {}
        spam_tracker[board_id] = defaultdict(list)
        spam_violations[board_id] = defaultdict(dict)
        last_texts[board_id] = defaultdict(lambda: deque(maxlen=5))
        last_stickers[board_id] = defaultdict(lambda: deque(maxlen=5))
        sticker_times[board_id] = defaultdict(list)
        last_user_msgs[board_id] = {}

        # Хранилища связей сообщений (остаются для каждой доски)
        post_to_messages[board_id] = {}
        message_to_post[board_id] = {}

        board_modes[board_id] = {
            'anime_mode': False, 'zaputin_mode': False,
            'slavaukraine_mode': False, 'suka_blyat_mode': False,
            'last_mode_activation': None, 'suka_blyat_counter': 0
        }
        
# --- НОВОЕ: Вспомогательная функция для определения доски по сообщению ---
def get_board_id_from_message(message: types.Message) -> str | None:
    """Определяет ID доски ('b', 'po', etc.) по ID бота, от которого пришло сообщение."""
    return bot_id_to_board.get(message.bot.id)

# Временные данные (не сохраняются)
last_activity_time = datetime.now()
daily_log = io.StringIO()

# Добавьте рядом с другими глобальными переменными
sent_media_groups = set()  # Для отслеживания уже отправленных медиа-групп

# Хранит информацию о текущих медиа-группах: media_group_id -> данные
current_media_groups = {}
media_group_timers = {}

def suka_blyatify_text(text: str) -> str:
    if not text:
        return text
    global suka_blyat_counter
    words = text.split()
    for i in range(len(words)):
        if random.random() < 0.3:
            words[i] = random.choice(MAT_WORDS)
    suka_blyat_counter += 1
    if suka_blyat_counter % 3 == 0:
        words.append("... СУКА БЛЯТЬ!")
    return ' '.join(words)

def restore_backup_on_start():
    """Забирает свежий state.json и reply_cache.json из backup-репозитория при запуске"""
    repo_url = "https://github.com/shlomapetia/dvachbot-backup.git"
    backup_dir = "/tmp/backup"
    try:
        if os.path.exists(backup_dir):
            shutil.rmtree(backup_dir)
        subprocess.run(["git", "clone", repo_url, backup_dir], check=True)

        for fname in ["state.json", "reply_cache.json"]:
            src = os.path.join(backup_dir, fname)
            dst = os.path.join(os.getcwd(), fname)
            if os.path.exists(src):
                shutil.copy2(src, dst)
                print(f"Восстановлен {fname} из backup-репозитория")
            else:
                print(f"{fname} не найден в backup-репозитории")
    except Exception as e:
        print(f"Ошибка при восстановлении backup: {e}")

async def healthcheck(request):
    print("🚀 Получен запрос на healthcheck")
    return web.Response(text="Bot is alive")


async def start_healthcheck():
    port = int(os.environ.get('PORT', 8080))
    app = web.Application()
    app.router.add_get("/", healthcheck)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    try:
        print(f"🟢 Попытка запустить healthcheck сервер на порту {port}")
        await site.start()  # Попробуем запустить сервер
        print(f"🟢 Healthcheck-сервер успешно запущен на порту {port}")
    except Exception as e:
        print(f"Ошибка запуска healthcheck сервера: {str(e)}")
        raise


GITHUB_REPO = "https://github.com/shlomapetia/dvachbot-backup.git"
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")  # Проверь, что переменная есть в Railway!

async def git_commit_and_push():
    """Надежная функция бэкапа в GitHub"""
    global is_shutting_down

    # Разрешаем выполнение при shutdown
    if git_executor._shutdown and not is_shutting_down:
        print("⚠️ Git executor завершен, пропускаем бэкап")
        return False

    async with git_semaphore:
        try:
            token = os.getenv("GITHUB_TOKEN")
            if not token:
                print("❌ Нет GITHUB_TOKEN")
                return False

            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(
                git_executor, 
                sync_git_operations, 
                token
            )
        except Exception as e:
            print(f"⛔ Ошибка в git_commit_and_push: {str(e)}")
            return False

def sync_git_operations(token: str) -> bool:
    """Синхронные Git-операции"""
    try:
        work_dir = "/tmp/git_backup"
        os.makedirs(work_dir, exist_ok=True)
        repo_url = f"https://{token}@github.com/shlomapetia/dvachbot-backup.git"

        if not os.path.exists(os.path.join(work_dir, ".git")):
            # Клонирование репозитория
            clone_cmd = ["git", "clone", repo_url, work_dir]
            result = subprocess.run(clone_cmd, capture_output=True, text=True)
            if result.returncode != 0:
                print(f"❌ Ошибка клонирования: {result.stderr}")
                return False
            print("✅ Репозиторий клонирован")
        else:
            # Обновление репозитория
            pull_cmd = ["git", "-C", work_dir, "pull"]
            result = subprocess.run(pull_cmd, capture_output=True, text=True)
            if result.returncode != 0:
                print(f"⚠️ Ошибка обновления: {result.stderr}")

        # Копирование файлов
        files_to_copy = ["state.json", "reply_cache.json"]
        copied_files = []

        for fname in files_to_copy:
            src = os.path.join(os.getcwd(), fname)
            if os.path.exists(src):
                shutil.copy2(src, work_dir)
                copied_files.append(fname)

        if not copied_files:
            print("⚠️ Нет файлов для бэкапа")
            return False

        # Git операции
        subprocess.run(["git", "-C", work_dir, "config", "user.name", "Backup Bot"], check=True)
        subprocess.run(["git", "-C", work_dir, "config", "user.email", "backup@dvachbot.com"], check=True)

        subprocess.run(["git", "-C", work_dir, "add", "."], check=True)

        commit_msg = f"Backup: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}"
        subprocess.run(["git", "-C", work_dir, "commit", "-m", commit_msg], check=True)

        push_cmd = ["git", "-C", work_dir, "push", "-u", "origin", "main"]
        result = subprocess.run(push_cmd, capture_output=True, text=True)

        if result.returncode != 0:
            print(f"❌ Ошибка пуша: {result.stderr}")
            return False

        print(f"✅ Бекапы сохранены в GitHub: {', '.join(copied_files)}")
        return True

    except Exception as e:
        print(f"⛔ Синхронная Git ошибка: {str(e)}")
        return False

async def save_state_and_backup():
    """Сохраняет state и reply_cache для ВСЕХ досок, а также глобальный счетчик."""
    try:
        # Сохраняем глобальный счетчик постов
        with open('global_counter.json', 'w', encoding='utf-8') as f:
            json.dump({'post_counter': global_post_counter}, f, ensure_ascii=False, indent=2)

        # Сохраняем состояние для каждой доски
        for board_id in BOT_TOKENS.keys():
            state_file_path = f'state_{board_id}.json'
            with open(state_file_path, 'w', encoding='utf-8') as f:
                state_data = boards_state[board_id]
                # Используем .get() для безопасного доступа к ключам
                json.dump({
                    'users_data': {
                        'active': list(state_data.get('users_data', {}).get('active', [])),
                        'banned': list(state_data.get('users_data', {}).get('banned', [])),
                    },
                    'message_counter': state_data.get('message_counter', {}),
                    'settings': state_data.get('settings', {}),
                    'user_activity': {
                        k: v.isoformat()
                        for k, v in state_data.get('user_activity', {}).items()
                    }
                }, f, ensure_ascii=False, indent=2)
            # Вызываем сохранение кэша для каждой доски
            save_reply_cache_for_board(board_id)

        print("💾 Обновление state и reply_cache для всех досок...")
        return await git_commit_and_push()
    except Exception as e:
        print(f"⛔ Ошибка сохранения state: {e}")
        return False

dp = Dispatcher()
# Настройка логирования - только важные сообщения
logging.basicConfig(
    level=logging.WARNING,  # Только предупреждения и ошибки
    format="%(message)s",  # Просто текст без дат
    datefmt="%H:%M:%S"  # Если время нужно
)
# Отключаем логирование для aiohttp (веб-сервер)
aiohttp_log = logging.getLogger('aiohttp')
aiohttp_log.setLevel(logging.CRITICAL)  # Только критические ошибки
# Отключаем логирование для aiogram (бот)
aiogram_log = logging.getLogger('aiogram')
aiogram_log.setLevel(logging.WARNING)  # Только предупреждения


def clean_html_tags(text: str) -> str:
    """Удаляет HTML-теги из текста, оставляя только содержимое"""
    if not text:
        return text
    return re.sub(r'<[^>]+>', '', text)

def add_you_to_my_posts(text: str, user_id: int) -> str:
    """Добавляет (You) к упоминаниям постов, если это ответ на свой же пост"""
    if not text:
        return text

    pattern = r">>(\d+)"
    matches = re.findall(pattern, text)

    for post_str in matches:
        try:
            post_num = int(post_str)
            post_data = messages_storage.get(post_num, {})
            original_author = post_data.get("author_id")

            if original_author == user_id:
                # Добавляем "(You)", если её ещё нет
                target = f">>{post_num}"
                replacement = f">>{post_num} (You)"

                if target in text and replacement not in text:
                    text = text.replace(target, replacement)
        except (ValueError, KeyError):
            continue

    return text


async def shutdown():
    """Cleanup tasks before shutdown"""
    print("Shutting down...")
    try:
        # Останавливаем healthcheck сервер
        if 'healthcheck_site' in globals():
            await healthcheck_site.stop()
            print("🛑 Healthcheck server stopped")

        # Останавливаем executors корректно
        git_executor.shutdown(wait=True, cancel_futures=True)
        send_executor.shutdown(wait=True, cancel_futures=True)

        # Закрываем хранилище диспетчера
        if hasattr(dp, 'storage') and dp.storage:
            await dp.storage.close()
    except Exception as e:
        print(f"Error during shutdown: {e}")

    # Закрываем сессию бота
    if 'bot' in globals() and bot.session:
        await bot.session.close()

    # Отменяем все фоновые задачи
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()

    # Ждем завершения задач с таймаутом
    await asyncio.wait_for(
        asyncio.gather(*tasks, return_exceptions=True),
        timeout=5.0
    )
    print("All background tasks stopped")


            
# Настройка сборщика мусора
gc.set_threshold(
    700, 10, 10)  # Оптимальные настройки для баланса памяти/производительности

async def save_state_and_backup():
    """Сохраняет state.json и reply_cache.json для ВСЕХ досок, пушит в GitHub"""
    try:
        # Сохраняем глобальный счетчик постов
        with open('global_counter.json', 'w', encoding='utf-8') as f:
            json.dump({'post_counter': global_post_counter}, f, ensure_ascii=False, indent=2)

        # Сохраняем состояние для каждой доски
        for board_id in BOT_TOKENS.keys():
            # Сохраняем основной state
            state_file_path = f'state_{board_id}.json'
            with open(state_file_path, 'w', encoding='utf-8') as f:
                state_data = boards_state[board_id]
                json.dump({
                    'users_data': {
                        'active': list(state_data['users_data']['active']),
                        'banned': list(state_data['users_data']['banned']),
                    },
                    'message_counter': state_data['message_counter'],
                    'settings': state_data['settings'],
                    'user_activity': {
                        k: v.isoformat()
                        for k, v in state_data['user_activity'].items()
                    }
                }, f, ensure_ascii=False, indent=2)

            # Сохраняем кэш ответов
            save_reply_cache_for_board(board_id)

        print("💾 Обновление state и reply_cache для всех досок, пушим в GitHub...")
        return await git_commit_and_push()
    except Exception as e:
        print(f"⛔ Ошибка сохранения state: {e}")
        return False

# Конфиг
BOT_TOKEN = os.environ.get('BOT_TOKEN')
ADMINS = {int(x) for x in os.getenv("ADMINS", "").split(",") if x}
SPAM_LIMIT = 14
SPAM_WINDOW = 15
STATE_FILE = 'state.json'
SAVE_INTERVAL = 3600  # секунд
STICKER_WINDOW = 10  # секунд
STICKER_LIMIT = 7
REST_SECONDS = 30  # время блокировки
REPLY_CACHE = 500  # сколько постов держать
REPLY_FILE = "reply_cache.json"  # отдельный файл для reply
# В начале файла с константами
MAX_MESSAGES_IN_MEMORY = 1110  # храним только последние 1000 постов


WEEKDAYS = [
    "Понедельник", "Вторник", "Среда", 
    "Четверг", "Пятница", "Суббота", "Воскресенье"
]

# Мотивационные сообщения для приглашений
MOTIVATIONAL_MESSAGES = [
    "Чем больше анонов, тем эпичнее треды",
    "Один анон - не анон. Зови братишек",
    "Больше анонов - больше рофлов",
    "Твой друг всё ещё сидит в телеге как нормис? Исправь это",
    "Каждый приглашённый анон = -1 нормис в мире",
    "Сидишь тут один? Заебал, зови друзей, не будь аутистом",
    "Больше анонов - меньше вероятность что тред умрёт",
    "Приведи друга - получи двойную дозу лулзов",
    "Твои кенты до сих пор в вк? Пора их спасать",
    "Анонимусы не размножаются почкованием. Зови новых",
    "Скучно? Позови анонов, будет веселее",
    "Маленький чат = мёртвый чат. Действуй",
    "Анонимность - это не только анонимность. Это и дружба",
    "Абу сосет хуй. Зови друзей",
    "Тгач - это не только чат. Это аноны",
    "Возрождаем сосач. Аноны, зовите друзей",
    "Добро пожаловать. Снова",
    "Привет, анон. Ты не один. Зови друзей",
    "Да ты заебал, приглашай анонов",
    "Пора бы пропиарить тгач. Разошли в свои конфы",
]

# Тексты для копирования
INVITE_TEXTS = [
    "Анон, залетай в Тгач @dvach_chatbot\nТут можно постить что угодно анонимно",
    "Есть телега? Есть желание постить анонимно? \n@dvach_chatbot - добро пожаловать",
    "Устал от цензуры? Хочешь анонимности?\n Велкам в Тгач - @dvach_chatbot - настоящий двач в телеге",
    "@dvach_chatbot - анонимный чат в телеге\nБез регистрации и смс",
    "Тгач: @dvach_chatbot\nПиши что думаешь, никто не узнает кто ты",
    "Скучаешь по двачу? Он тут:  Тгач @dvach_chatbot\nПолная анонимность гарантирована",
    "Залетай в @dvach_chatbot\nАнонимный чат где можно всё",
    "@dvach_chatbot - для тех кто устал от обычных чатов\n100% анонимность",
    "Анонимный чат в телеге: @dvach_chatbot\nПиши что хочешь, никто не узнает кто ты",
    "Тгач в телеге: @dvach_chatbot\nБез регистрации и смс",
    "@dvach_chatbot - анонимный чат в телеге\nПиши что думаешь, никто не узнает кто ты",
    "Сап тгач: @dvach_chatbot\nАнонимный чат в телеге",
    "Добро пожаловать. Снова. @dvach_chatbot",
    "Привет, анон. Ты не один. Зови друзей. @dvach_chatbot",
    "Тгач - двач в телеге @dvach_chatbot",
]

# Для /suka_blyat
MAT_WORDS = ["сука", "блядь", "пиздец", "ебать", "нах", "пизда", "хуйня", "ебал", "блять", "отъебись", "ебаный", "еблан", "ХУЙ", "ПИЗДА"]

# хранит 5 последних sticker_file_id для проверки «одинаковых»
last_stickers: dict[int, deque[str]] = defaultdict(lambda: deque(maxlen=5))

# тайм-штампы всех пришедших стикеров за последние 15 c
sticker_times: dict[int, list[datetime]] = defaultdict(list)

# Временная зона МСК
MSK = timezone(timedelta(hours=3))

# ─── Глобальный error-handler ──────────────────────────────────
@dp.errors()
async def global_error_handler(event: types.ErrorEvent) -> bool:
    """Улучшенный обработчик ошибок для aiogram"""
    exception = event.exception
    update = event.update

    if exception is None:
        if update:
            update_info = f"Update {update.update_id}"
            if update.message:
                update_info += f" from user {update.message.from_user.id}"
            print(f"⚠️ Event without exception: {update_info}")
        else:
            print("⚠️ Получено событие без исключения и без update")
        return True

    # Логирование ошибки
    error_msg = f"⚠️ Ошибка: {type(exception).__name__}"
    if str(exception):
        error_msg += f": {exception}"
    print(error_msg)

    # Обработка TelegramForbiddenError (пользователь заблокировал бота)
    if isinstance(exception, TelegramForbiddenError):
        user_id = None
        if update and update.message:
            user_id = update.message.from_user.id
        elif update and update.callback_query:
            user_id = update.callback_query.from_user.id

        if user_id:
            state['users_data']['active'].discard(user_id)
            print(f"🚫 Пользователь {user_id} заблокировал бота, удален из активных")
        return True

    # Обработка сетевых ошибок и конфликтов
    elif isinstance(exception, (TelegramNetworkError, TelegramConflictError, aiohttp.ClientError)):
        print(f"🌐 Сетевая ошибка: {exception}")
        await asyncio.sleep(10)  # Увеличиваем задержку перед повторной попыткой
        return False  # Можно перезапустить polling

    # Обработка KeyError (проблемы с хранилищем)
    elif isinstance(exception, KeyError):
        print(f"🔑 KeyError: {exception}. Пропускаем обработку этого сообщения.")
        return True

    # Все остальные ошибки
    else:
        print(f"⛔ Непредвиденная ошибка: {exception}")
        if update:
            print(f"Update: {update.model_dump_json(exclude_none=True)}")
        await asyncio.sleep(10)  # Задержка перед повторной попыткой
        return False

def escape_html(text: str) -> str:
    """Экранирует HTML символы"""
    if not text:
        return text
    return text.replace('&', '&amp;').replace('<', '&lt;').replace(
        '>', '&gt;').replace('"', '&quot;')


def is_admin(uid: int) -> bool:
    return uid in ADMINS

async def save_state():
    """Только сохранение state.json и push в GitHub"""
    try:
        # Сохраняем state.json
        with open('state.json', 'w', encoding='utf-8') as f:
            json.dump({
                'post_counter': state['post_counter'],
                'users_data': {
                    'active': list(state['users_data']['active']),
                    'banned': list(state['users_data']['banned']),
                },
                'message_counter': state['message_counter'],
                'settings': state['settings'],
                'recent_post_mappings': {
                    str(k): v for k, v in list(post_to_messages.items())[-500:]
                }
            }, f, ensure_ascii=False, indent=2)

        # Всегда пушим при изменении
        print("💾 Обновление state.json, пушим в GitHub...")
        return await git_commit_and_push()

    except Exception as e:
        print(f"⛔ Ошибка сохранения state: {e}")
        return False

def save_reply_cache_for_board(board_id: str):
    """Сохраняет кэш ответов для указанной доски."""
    try:
        # Получаем данные для конкретной доски
        board_messages_storage = messages_storage.get(board_id, {})
        board_post_to_messages = post_to_messages.get(board_id, {})
        board_message_to_post = message_to_post.get(board_id, {})

        if not board_messages_storage:
            return # Нечего сохранять

        recent_posts = sorted(board_messages_storage.keys())[-REPLY_CACHE:]
        new_data = {
            "post_to_messages": {
                str(p): board_post_to_messages[p]
                for p in recent_posts
                if p in board_post_to_messages
            },
            "message_to_post": {
                f"{uid}_{mid}": p
                for (uid, mid), p in board_message_to_post.items()
                if p in recent_posts
            },
            "messages_storage_meta": {
                str(p): {
                    "author_id": board_messages_storage[p].get("author_id", ""),
                    "timestamp": board_messages_storage[p].get("timestamp", datetime.now(UTC)).isoformat(),
                    "author_msg": board_messages_storage[p].get("author_message_id")
                }
                for p in recent_posts if p in board_messages_storage
            }
        }

        reply_file_path = f"reply_cache_{board_id}.json"

        # Проверяем существующий файл
        old_data = {}
        if os.path.exists(reply_file_path):
            try:
                with open(reply_file_path, 'r', encoding='utf-8') as f:
                    old_data = json.load(f)
            except (json.JSONDecodeError, OSError):
                old_data = {}

        if old_data == new_data:
            # print(f"ℹ️ reply_cache_{board_id}.json без изменений") # Можно раскомментировать для отладки
            return

        with open(reply_file_path, 'w', encoding='utf-8') as f:
            json.dump(new_data, f, ensure_ascii=False, indent=2)
            # print(f"✅ reply_cache_{board_id}.json обновлен") # Можно раскомментировать для отладки

    except Exception as e:
        print(f"⛔ Ошибка сохранения reply_cache для доски {board_id}: {str(e)[:200]}")
        
def load_all_states():
    """Загружает состояния для всех досок, которые определены в BOT_TOKENS."""
    global global_post_counter

    # Загружаем глобальный счетчик
    try:
        if os.path.exists('global_counter.json'):
            with open('global_counter.json', 'r', encoding='utf-8') as f:
                data = json.load(f)
                global_post_counter = data.get('post_counter', 0)
                print(f"✅ Глобальный счетчик постов загружен: {global_post_counter}")
    except Exception as e:
        print(f"⚠️ Не удалось загрузить глобальный счетчик: {e}")


    # Загружаем состояние для каждой доски
    for board_id in BOT_TOKENS.keys():
        load_state_for_board(board_id)

def load_state_for_board(board_id: str):
    """Загружает состояние для одной указанной доски."""
    state_file_path = f'state_{board_id}.json'
    if not os.path.exists(state_file_path):
        print(f"ℹ️ Файл состояния для доски '{board_id}' не найден.")
        return

    try:
        with open(state_file_path, 'r', encoding='utf-8') as f: data = json.load(f)

        state = boards_state[board_id]
        state['users_data']['active'] = set(data.get('users_data', {}).get('active', []))
        state['users_data']['banned'] = set(data.get('users_data', {}).get('banned', []))
        state['message_counter'] = data.get('message_counter', {})
        state['settings'] = data.get('settings', {'dvach_enabled': False})
        
        # Добавляем загрузку user_activity
        user_activity_raw = data.get('user_activity', {})
        state['user_activity'] = {
            int(k): datetime.fromisoformat(v)
            for k, v in user_activity_raw.items()
        }

        print(f"✅ Состояние для доски '{board_id}' загружено.")
        load_reply_cache_for_board(board_id)
    except Exception as e:
        print(f"⛔ Ошибка загрузки состояния для доски '{board_id}': {e}")
        
def load_reply_cache_for_board(board_id: str):
    """Читаем reply_cache_{board_id}.json, восстанавливаем словари для доски."""
    reply_file_path = f"reply_cache_{board_id}.json"
    if not os.path.exists(reply_file_path):
        return

    try:
        if os.path.getsize(reply_file_path) == 0: return
        with open(reply_file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        print(f"reply_cache для доски '{board_id}' повреждён ({e}), игнорирую")
        return

    board_message_to_post = message_to_post[board_id]
    board_post_to_messages = post_to_messages[board_id]
    board_message_to_post.clear()
    board_post_to_messages.clear()

    for key, post_num in data.get("message_to_post", {}).items():
        try:
            uid, mid = map(int, key.split("_"))
            board_message_to_post[(uid, mid)] = post_num
        except: continue

    for p_str, mapping in data.get("post_to_messages", {}).items():
        board_post_to_messages[int(p_str)] = {int(uid): mid for uid, mid in mapping.items()}

    # ИСПРАВЛЕНИЕ: Мы работаем с ГЛОБАЛЬНЫМ messages_storage, а не с пустым локальным
    for p_str, meta in data.get("messages_storage_meta", {}).items():
        p = int(p_str)
        if 'timestamp' in meta:
            dt = datetime.fromisoformat(meta['timestamp']).replace(tzinfo=UTC)
            if p not in messages_storage:
                messages_storage[p] = {}
            # Добавляем/обновляем метаданные поста, добавляя board_id
            messages_storage[p].update({
                "board_id": board_id,
                "author_id": meta.get("author_id"),
                "timestamp": dt,
                "author_message_id": meta.get("author_msg"),
            })

    print(f"✅ Reply-cache для доски '{board_id}' загружен: {len(board_post_to_messages)} постов, {len(board_message_to_post)} сообщений.")

async def graceful_shutdown():
    """Обработчик graceful shutdown (корректное сохранение перед остановкой)"""
    global is_shutting_down
    if is_shutting_down:
        return

    is_shutting_down = True
    print("🛑 Получен сигнал shutdown, сохраняем данные...")

    # 1. Остановить polling чтобы не принимались новые сообщения
    try:
        await dp.stop_polling()
        print("⏸ Polling остановлен для shutdown")
    except Exception as e:
        print(f"⚠️ Не удалось остановить polling: {e}")

    # 2. Ждать пока очередь сообщений опустеет (макс 10 сек)
    for _ in range(10):
        if message_queue and message_queue.empty():
            break
        await asyncio.sleep(1)

    # 3. Сохраняем и пушим данные
    await save_state_and_backup()

    # 4. Останавливаем всё остальное как было
    try:
        if 'healthcheck_site' in globals():
            await healthcheck_site.stop()
            print("🛑 Healthcheck server stopped")

        git_executor.shutdown(wait=True, cancel_futures=True)
        send_executor.shutdown(wait=True, cancel_futures=True)

        if hasattr(dp, 'storage') and dp.storage:
            await dp.storage.close()
    except Exception as e:
        print(f"Error during shutdown: {e}")

    if 'bot' in globals() and bot.session:
        await bot.session.close()

    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    print("✅ Все задачи остановлены, завершаем работу")

async def auto_memory_cleaner():
    """Раз в 5 минут чистит память для каждой доски и глобально."""
    while True:
        await asyncio.sleep(300)

        # 1. Очистка старых постов (старше 7 дней) из глобального хранилища
        current_time = datetime.now(UTC)
        seven_days_ago = current_time - timedelta(days=7)
        old_posts = [pnum for pnum, data in messages_storage.items() if data.get('timestamp', current_time) < seven_days_ago]
        
        if old_posts:
            print(f"🧹 Очистка старых постов: найдено {len(old_posts)} постов старше 7 дней...")
            for pnum in old_posts:
                messages_storage.pop(pnum, None)
                # Также чистим ссылки в словарях всех досок
                for board_id in BOT_TOKENS.keys():
                    post_to_messages[board_id].pop(pnum, None)
                    keys_to_del = [k for k, v in message_to_post[board_id].items() if v == pnum]
                    for k in keys_to_del:
                        message_to_post[board_id].pop(k, None)
            print(f"✅ Очистка старых постов завершена.")

        # 2. Очистка данных неактивных пользователей по доскам
        for board_id, state in boards_state.items():
            time_threshold = datetime.now(UTC) - timedelta(hours=25)
            users_to_clear = [uid for uid, timestamp in state.get('user_activity', {}).items() if timestamp < time_threshold]
            if users_to_clear:
                print(f"🧹 [{board_id}] Найдено {len(users_to_clear)} неактивных юзеров для очистки.")
                for uid in users_to_clear:
                    state['user_activity'].pop(uid, None)
                    # Также можно чистить их из других словарей, если нужно
                    spam_tracker[board_id].pop(uid, None)
                    spam_violations[board_id].pop(uid, None)

        # 3. Принудительная сборка мусора Python
        gc.collect()
        print("🗑️ Сборка мусора Python завершена.")


async def cross_board_stats_broadcaster():
    """Раз в 1-2 часа рассылает общую статистику по всем доскам."""
    await asyncio.sleep(3600) # Ждем час после старта

    while True:
        try:
            delay = random.randint(3600, 7200) # 1-2 часа
            await asyncio.sleep(delay)

            # Выбираем случайную доску, на которую будем постить (но только если там есть юзеры)
            active_boards = [b_id for b_id, data in boards_state.items() if data['users_data']['active']]
            if not active_boards:
                continue
            
            target_board_id = random.choice(active_boards)
            target_board_recipients = boards_state[target_board_id]['users_data']['active']
            if not target_board_recipients:
                continue

            # Собираем статистику
            stats_text = "📊 **Общая статистика ТГАЧА** 📊\n\n"
            total_users = 0
            
            # Сортируем доски для красивого вывода
            sorted_boards = sorted(BOARD_INFO.keys(), key=lambda x: BOARDS.index(x))

            for board_id in sorted_boards:
                if board_id in boards_state:
                    data = boards_state[board_id]
                    users_count = len(data['users_data']['active'])
                    total_users += users_count
                    
                    # Считаем скорость постов (пока заглушка, т.к. требует сложного подсчета)
                    # В будущем можно будет реализовать подсчет постов за час
                    posts_speed = random.randint(5, 50) # Временно
                    
                    stats_text += f"**{BOARD_INFO[board_id]['name']}**: {users_count} анонов | ~{posts_speed} п/час\n"

            stats_text += f"\n**Всего постов**: {global_post_counter}\n"
            stats_text += f"**Всего анонов онлайн**: {total_users}"

            # Формируем и отправляем
            header = "### Статистика ###"
            global global_post_counter
            global_post_counter += 1
            post_num = global_post_counter

            content = {"type": "text", "header": header, "text": stats_text}

            await message_queues[target_board_id].put({
                "recipients": target_board_recipients,
                "content": content,
                "post_num": post_num
            })

            print(f"✅ Рассылка статистики отправлена на доску /{target_board_id}/.")

        except Exception as e:
            print(f"Ошибка в cross_board_stats_broadcaster: {e}")
            await asyncio.sleep(60)


async def check_spam(board_id: str, user_id: int, msg: Message) -> bool:
    """Проверяет спам для конкретной доски."""
    msg_type = msg.content_type
    content = None
    if msg_type == 'text':
        content = msg.text
    elif msg.content_type in ['photo', 'video', 'document'] and msg.caption:
        msg_type = 'text' # Считаем как текстовый спам
        content = msg.caption
    
    rules = SPAM_RULES.get(msg_type)
    if not rules:
        return True

    # Получаем хранилища для этой доски
    board_spam_violations = spam_violations[board_id]
    board_spam_tracker = spam_tracker[board_id]
    
    now = datetime.now(UTC)
    user_violations = board_spam_violations.get(user_id)

    # Инициализация или сброс данных пользователя
    if not user_violations or (now - user_violations.get('last_reset', now)) > timedelta(hours=1):
        board_spam_violations[user_id] = {'level': 0, 'last_reset': now, 'last_contents': deque(maxlen=4)}
    
    user_violations = board_spam_violations[user_id]
    
    # Проверка на повторы текста
    if msg_type == 'text' and content:
        user_violations['last_contents'].append(content)
        if len(user_violations['last_contents']) >= rules['max_repeats']:
            if len(set(user_violations['last_contents'])) == 1:
                user_violations['level'] += 1
                return False
    
    # Проверка на частоту сообщений
    window_start = now - timedelta(seconds=rules['window_sec'])
    user_tracker = board_spam_tracker.get(user_id, [])
    user_tracker = [t for t in user_tracker if t > window_start]
    user_tracker.append(now)
    board_spam_tracker[user_id] = user_tracker

    if len(user_tracker) > rules['max_per_window']:
        user_violations['level'] += 1
        return False
        
    return True

async def apply_penalty(board_id: str, user_id: int, msg_type: str):
    """Применяет мут для конкретной доски."""
    rules = SPAM_RULES.get(msg_type, {})
    if not rules or 'penalty' not in rules:
        return

    # Получаем хранилища для этой доски
    board_mutes = mutes[board_id]
    board_spam_violations = spam_violations[board_id]
    bot_instance = bots[board_id] # Нужен для отправки уведомления

    level = board_spam_violations.get(user_id, {}).get('level', 0)
    # Уровень не может быть больше, чем количество определенных наказаний
    level = min(level, len(rules['penalty']) - 1)
    
    mute_seconds = rules['penalty'][level]
    board_mutes[user_id] = datetime.now(UTC) + timedelta(seconds=mute_seconds)
    
    violation_type = {'text': "текстовый спам", 'sticker': "спам стикерами", 'animation': "спам гифками"}.get(msg_type, "спам")
    time_str = f"{mute_seconds // 60}м {mute_seconds % 60}с" if mute_seconds >= 60 else f"{mute_seconds}с"

    try:
        await bot_instance.send_message(
            user_id,
            f"🚫 Вы получили мут на доске /{board_id}/ на {time_str} за {violation_type}.\n"
            f"Следующее нарушение увеличит срок мута."
        )
    except Exception:
        pass # Не страшно, если не дошло

def format_header(board_id: str) -> Tuple[str, int]:
    """Форматирование заголовка с учетом режимов конкретной доски и глобального счетчика."""
    global global_post_counter
    global_post_counter += 1
    post_num = global_post_counter

    # Получаем режимы для конкретной доски
    modes = board_modes.get(board_id, {})
    slavaukraine_mode = modes.get('slavaukraine_mode', False)
    zaputin_mode = modes.get('zaputin_mode', False)
    anime_mode = modes.get('anime_mode', False)
    suka_blyat_mode = modes.get('suka_blyat_mode', False)


    # Режим /slavaukraine
    if slavaukraine_mode:
        return f"💙💛 Пiст №{post_num}", post_num

    # Режим /zaputin
    if zaputin_mode:
        return f"🇷🇺 Пост №{post_num}", post_num

    # Режим /anime
    if anime_mode:
        return f"🌸 投稿 {post_num} 番", post_num

    # Режим /suka_blyat
    if suka_blyat_mode:
        return f"💢 Пост №{post_num}", post_num

    # Обычный режим
    rand = random.random()
    if rand < 0.003:
        circle = "🔴 "
    elif rand < 0.006:
        circle = "🟢 "
    else:
        circle = ""

    prefix = ""
    rand_prefix = random.random()
    if rand_prefix < 0.005:  # 0.5%
        prefix = "### АДМИН ### "
    elif rand_prefix < 0.008:  # 0.3%
        prefix = "Абу - "
    elif rand_prefix < 0.01:   # 0.2%
        prefix = "Пидор - "
    elif rand_prefix < 0.012:  # 0.2%
        prefix = "### ДЖУЛУП ### "
    elif rand_prefix < 0.014:   # 0.2%
        prefix = "### Хуесос ### "
    elif rand_prefix < 0.016:   # 0.2%
        prefix = "Пыня - "
    elif rand_prefix < 0.018:   # 0.2%
        prefix = "Нариман Намазов - "
    elif rand_prefix < 0.021:
        prefix = "ИМПЕРАТОР КОНАН - "
    elif rand_prefix < 0.023:
        prefix = "Антон Бабкин - "
    elif rand_prefix < 0.025:
        prefix = "### НАРИМАН НАМАЗОВ ### "
    elif rand_prefix < 0.027:
        prefix = "### ПУТИН ### "
    elif rand_prefix < 0.028:
        prefix = "Гей - "
    elif rand_prefix < 0.030:
        prefix = "Анархист - "
    elif rand_prefix < 0.033:
        prefix = "### Имбецил ### "
    elif rand_prefix < 0.035:
        prefix = "### ЧМО ### "
    elif rand_prefix < 0.037:
        prefix = "### ОНАНИСТ ### "
    elif rand_prefix < 0.040:
        prefix = "### ЧЕЧЕНЕЦ ### "
    elif rand_prefix < 0.042:
        prefix = "Томоко Куроки - "
    elif rand_prefix < 0.044:
        prefix = "### Аниме девочка ### "

    # Формируем итоговый заголовок
    header_text = f"{circle}{prefix}Пост №{post_num}"
    return header_text, post_num

async def delete_user_posts(board_id: str, user_id: int, time_period_minutes: int) -> int:
    """Удаляет сообщения пользователя за указанный период на КОНКРЕТНОЙ доске."""
    deleted_count = 0
    try:
        time_threshold = datetime.now(UTC) - timedelta(minutes=time_period_minutes)
        board_post_to_messages = post_to_messages[board_id]
        board_message_to_post = message_to_post[board_id]
        bot_instance = bots[board_id]

        posts_to_delete = [
            pnum for pnum, data in messages_storage.items()
            if (data.get('board_id') == board_id and
                data.get('author_id') == user_id and
                data.get('timestamp') >= time_threshold)
        ]

        if not posts_to_delete: return 0

        for pnum in posts_to_delete:
            if pnum in board_post_to_messages:
                for uid, mid in list(board_post_to_messages[pnum].items()):
                    try:
                        await bot_instance.delete_message(uid, mid)
                        deleted_count += 1
                    except Exception: pass
            
            messages_storage.pop(pnum, None)
            board_post_to_messages.pop(pnum, None)
            keys_to_del = [key for key, val in board_message_to_post.items() if val == pnum]
            for key in keys_to_del:
                board_message_to_post.pop(key, None)

        print(f"[{board_id}] Удалено {deleted_count} сообщений пользователя {user_id}.")
        return deleted_count
    except Exception as e:
        print(f"Ошибка в delete_user_posts для доски {board_id}: {e}")
        return deleted_count

async def process_media_group_message(message: Message, media_group_id: str):
    """Обработка отдельного сообщения в медиа-группе"""
    try:
        media_group = state['media_groups'][media_group_id]

        # Сохраняем информацию о сообщении
        media_data = {
            'type': message.content_type,
            'file_id': None,
            'caption': message.caption
        }

        if message.photo:
            media_data['file_id'] = message.photo[-1].file_id
        elif message.video:
            media_data['file_id'] = message.video.file_id
        elif message.document:
            media_data['file_id'] = message.document.file_id
        elif message.audio:
            media_data['file_id'] = message.audio.file_id

        media_group['messages'].append(media_data)

        # Удаляем оригинальное сообщение
        await message.delete()

        # Если это последнее сообщение в группе (определяем по таймауту)
        await asyncio.sleep(1)  # Ждем 1 секунду для сбора всех сообщений группы

        if media_group_id in state['media_groups']:
            # Проверяем, не было ли новых сообщений в группе
            await asyncio.sleep(2)  # Дополнительное время для сбора всех сообщений

            # Отправляем собранную медиа-группу
            await send_media_group(media_group_id)

    except Exception as e:
        print(f"Ошибка обработки медиа-группы: {e}")

async def handle_audio_message(message: Message):
    """Обработчик отдельных аудио сообщений"""
    user_id = message.from_user.id

    if user_id in state['users_data']['banned']:
        await message.delete()
        return

    if mutes.get(user_id) and mutes[user_id] > datetime.now(UTC):
        await message.delete()
        return

    # Форматируем заголовок
    header, current_post_num = format_header()

    # Проверяем, это ответ?
    reply_to_post = None
    reply_info = {}

    if message.reply_to_message:
        reply_mid = message.reply_to_message.message_id
        reply_to_post = None
        for (uid, mid), pnum in message_to_post.items():
            if mid == reply_mid:
                reply_to_post = pnum
                break
        if reply_to_post and reply_to_post in post_to_messages:
            reply_info = post_to_messages[reply_to_post]
        else:
            reply_to_post = None

    # Удаляем оригинальное сообщение
    try:
        await message.delete()
    except:
        pass

    # Применяем преобразования к подписи
    caption = message.caption
    if slavaukraine_mode and caption:
        caption = ukrainian_transform(caption)
    if suka_blyat_mode and caption:
        caption = suka_blyatify_text(caption)
    if anime_mode and caption:
        caption = anime_transform(caption)
    if zaputin_mode and caption:
        caption = zaputin_transform(caption)      

    # Формируем контент
    content = {
        'type': 'audio',
        'header': header,
        'file_id': message.audio.file_id,
        'caption': caption,
        'reply_to_post': reply_to_post
    }

    # Сохраняем сообщение
    messages_storage[current_post_num] = {
        'author_id': user_id,
        'timestamp': datetime.now(MSK),
        'content': content,
        'reply_to': reply_to_post
    }

    # Отправляем автору
    reply_to_message_id = reply_info.get(user_id) if reply_info else None

    caption_text = f"<i>{header}</i>"
    if caption:
        caption_text += f"\n\n{escape_html(caption)}"

    sent_to_author = await bot.send_audio(
        user_id,
        message.audio.file_id,
        caption=caption_text,
        reply_to_message_id=reply_to_message_id,
        parse_mode="HTML"
    )

    # Сохраняем для ответов
    if sent_to_author:
        messages_storage[current_post_num]['author_message_id'] = sent_to_author.message_id
        if current_post_num not in post_to_messages:
            post_to_messages[current_post_num] = {}
        post_to_messages[current_post_num][user_id] = sent_to_author.message_id
        message_to_post[(user_id, sent_to_author.message_id)] = current_post_num

    # Отправляем остальным
    recipients = state['users_data']['active'] - {user_id}
    if recipients:
        await message_queue.put({
            'recipients': recipients,
            'content': content,
            'post_num': current_post_num,
            'reply_info': reply_info if reply_info else None,
            'user_id': user_id
        })

async def send_media_group(media_group_id: str):
    """Отправка собранной медиа-группы"""
    try:
        media_group = state['media_groups'].get(media_group_id)
        if not media_group or not media_group['messages']:
            return

        # Создаем сборщик медиа-группы
        builder = MediaGroupBuilder(
            caption=media_group['header']
        )

        # Добавляем все медиафайлы
        for idx, media in enumerate(media_group['messages']):
            if media['type'] == 'photo':
                if idx == 0 and media['caption']:
                    builder.add_photo(
                        media=media['file_id'],
                        caption=f"{media_group['header']}\n\n{escape_html(media['caption'])}"
                    )
                else:
                    builder.add_photo(media=media['file_id'])
            elif media['type'] == 'video':
                if idx == 0 and media['caption']:
                    builder.add_video(
                        media=media['file_id'],
                        caption=f"{media_group['header']}\n\n{escape_html(media['caption'])}"
                    )
                else:
                    builder.add_video(media=media['file_id'])
            elif media['type'] == 'document':
                if idx == 0 and media['caption']:
                    builder.add_document(
                        media=media['file_id'],
                        caption=f"{media_group['header']}\n\n{escape_html(media['caption'])}"
                    )
                else:
                    builder.add_document(media=media['file_id'])

        # Отправляем автору
        sent_messages = await bot.send_media_group(
            chat_id=media_group['author_id'],
            media=builder.build()
        )

        # Сохраняем в основном хранилище
        post_num = media_group['post_num']
        messages_storage[post_num] = {
            'author_id': media_group['author_id'],
            'timestamp': media_group['timestamp'],
            'content': {
                'type': 'media_group',
                'header': media_group['header'],
                'media': media_group['messages']
            }
        }

        if sent_messages:
            messages_storage[post_num]['author_message_id'] = sent_messages[0].message_id
            post_to_messages[post_num] = {media_group['author_id']: sent_messages[0].message_id}
            for msg in sent_messages:
                message_to_post[(media_group['author_id'], msg.message_id)] = post_num

        # Отправляем остальным через очередь
        recipients = state['users_data']['active'] - {media_group['author_id']}
        if recipients:
            await message_queue.put({
                'recipients': recipients,
                'content': {
                    'type': 'media_group',
                    'header': media_group['header'],
                    'media': media_group['messages']
                },
                'post_num': post_num
            })

        # Удаляем временные данные
        del state['media_groups'][media_group_id]

    except Exception as e:
        print(f"Ошибка отправки медиа-группы: {e}")
    finally:
        if media_group_id in state['media_groups']:
            del state['media_groups'][media_group_id]

async def send_moderation_notice(user_id: int, action: str, duration: str = None, deleted_posts: int = 0):
    """Отправляет уведомление о модерационном действии в чат"""
    state['post_counter'] += 1
    post_num = state['post_counter']
    header = "### Админ ###"

    if action == "ban":
        text = (f"🚨 Хуесос был забанен за спам. Помянем.")
    elif action == "mute":
        text = (f"🔇 Ебаного пидораса замутили на {duration}. "
               "Хорош спамить, хуйло ебаное!")
    else:
        return

    # Сохраняем сообщение
    messages_storage[post_num] = {
        'author_id': 0,  # 0 = системное сообщение
        'timestamp': datetime.now(MSK),
        'content': {
            'type': 'text',
            'header': header,
            'text': text
        }
    }

    # Отправляем всем
    await message_queue.put({
        "recipients": state["users_data"]["active"],
        "content": {
            "type": "text",
            "header": header,
            "text": text
        },
        "post_num": post_num
    })


async def send_message_to_users(
    bot_instance: Bot,
    recipients: set[int],
    content: dict,
    reply_info: dict | None = None,
) -> list:
    """
    Оптимизированная рассылка сообщений пользователям с использованием конкретного бота
    и поддержкой режимов для конкретной доски.
    """
    if not recipients:
        return []
    if not content or 'type' not in content:
        return []

    # 1. Определяем доску по ID бота, который будет делать рассылку
    board_id = bot_id_to_board.get(bot_instance.id)
    if not board_id:
        print(f"⚠️ Не удалось определить доску для бота {bot_instance.id}")
        return []

    # 2. Получаем режимы для этой доски и применяем трансформации
    modes = board_modes.get(board_id, {})
    modified_content = content.copy()

    # Применение трансформаций текста и подписей в зависимости от режимов
    text_to_transform = None
    is_caption = False
    if modified_content.get('text'):
        text_to_transform = modified_content.get('text')
    elif modified_content.get('caption'):
        text_to_transform = modified_content.get('caption')
        is_caption = True

    if text_to_transform is not None:
        if modes.get('slavaukraine_mode'):
            text_to_transform = ukrainian_transform(text_to_transform)
        elif modes.get('zaputin_mode'):
            text_to_transform = zaputin_transform(text_to_transform)
        elif modes.get('suka_blyat_mode'):
            # Счетчик должен быть из словаря режимов доски
            modes['suka_blyat_counter'] += 1
            if modes['suka_blyat_counter'] % 3 == 0:
                 text_to_transform = suka_blyatify_text(text_to_transform) + " ... СУКА БЛЯТЬ!"
            else:
                 text_to_transform = suka_blyatify_text(text_to_transform)
        elif modes.get('anime_mode'):
            text_to_transform = anime_transform(text_to_transform)

        if is_caption:
            modified_content['caption'] = text_to_transform
        else:
            modified_content['text'] = text_to_transform

    # 3. Внутренняя функция для отправки одному пользователю
    async def really_send(uid: int, reply_to: int | None):
        try:
            ct = modified_content["type"]
            head = f"<i>{modified_content['header']}</i>"

            # Определяем автора оригинального поста для тега (You)
            reply_to_post = modified_content.get('reply_to_post')
            original_author = None
            if reply_to_post and reply_to_post in messages_storage:
                original_author = messages_storage[reply_to_post].get('author_id')

            # Формируем полный текст сообщения
            reply_text = ""
            if reply_to_post:
                reply_text = f">>{reply_to_post}"
                if uid == original_author:
                    reply_text += " (You)"
                reply_text += "\n"

            main_text = ""
            if modified_content.get('text'):
                main_text = add_you_to_my_posts(modified_content['text'], uid)
            elif modified_content.get('caption'):
                main_text = add_you_to_my_posts(modified_content['caption'], uid)

            # Собираем все вместе
            full_caption = f"{head}\n\n{reply_text}{main_text}"

            # 4. Отправка в зависимости от типа контента, используя bot_instance
            if ct == "text":
                return await bot_instance.send_message(uid, full_caption, reply_to_message_id=reply_to, parse_mode="HTML")
            
            # Обрезаем подпись для медиа, если она слишком длинная
            if len(full_caption) > 1024:
                full_caption = full_caption[:1021] + "..."

            if ct == "photo":
                file_id = modified_content.get('image_url') or modified_content.get("file_id")
                return await bot_instance.send_photo(uid, file_id, caption=full_caption, reply_to_message_id=reply_to, parse_mode="HTML")
            elif ct == "video":
                return await bot_instance.send_video(uid, modified_content["file_id"], caption=full_caption, reply_to_message_id=reply_to, parse_mode="HTML")
            elif ct == "animation":
                return await bot_instance.send_animation(uid, modified_content["file_id"], caption=full_caption, reply_to_message_id=reply_to, parse_mode="HTML")
            elif ct == "document":
                return await bot_instance.send_document(uid, modified_content["file_id"], caption=full_caption, reply_to_message_id=reply_to, parse_mode="HTML")
            elif ct == "audio":
                return await bot_instance.send_audio(uid, modified_content["file_id"], caption=full_caption, reply_to_message_id=reply_to, parse_mode="HTML")
            elif ct == "sticker":
                return await bot_instance.send_sticker(uid, modified_content["file_id"], reply_to_message_id=reply_to)
            elif ct == "voice":
                return await bot_instance.send_voice(uid, modified_content["file_id"], caption=head, reply_to_message_id=reply_to, parse_mode="HTML")
            elif ct == "video_note":
                return await bot_instance.send_video_note(uid, modified_content["file_id"], reply_to_message_id=reply_to)

            # Медиа-группы обрабатываются отдельно и здесь не должны появляться, но на всякий случай
            elif ct == "media_group":
                 print(f"⚠️ Попытка отправить медиа-группу через send_message_to_users. Это не поддерживается.")
                 return None

        except TelegramForbiddenError:
            # Пользователь заблокировал этого бота, удаляем его из active этой доски
            boards_state[board_id]['users_data']['active'].discard(uid)
            print(f"🚫 [{board_id}] Пользователь {uid} заблокировал бота, удален из активных.")
            return None
        except Exception as e:
            # print(f"❌ [{board_id}] Ошибка отправки пользователю {uid}: {e}") # Можно раскомментировать для отладки
            return None

    # 5. Асинхронная рассылка всем получателям
    tasks = [really_send(uid, reply_info.get(uid) if reply_info else None) for uid in recipients]
    results = await asyncio.gather(*tasks)

    return list(zip(recipients, results))
    
async def message_broadcaster():
    """Создает и запускает по одному обработчику (worker) для каждой очереди сообщений."""
    tasks = []
    for board_id in BOT_TOKENS.keys():
        # Для каждой доски запускаем свою корутину-обработчик
        tasks.append(asyncio.create_task(message_worker_for_board(board_id)))
        print(f"📢 Воркер для очереди доски /{board_id}/ запущен.")
    await asyncio.gather(*tasks)

async def message_worker_for_board(board_id: str):
    """Индивидуальный обработчик, который слушает очередь сообщений конкретной доски."""
    worker_name = f"Worker-{board_id}"
    board_queue = message_queues[board_id]
    bot_instance = bots[board_id] # Получаем инстанс бота для этой доски

    while True:
        try:
            msg_data = await board_queue.get()
            if not msg_data:
                await asyncio.sleep(0.05)
                continue

            # Быстрая проверка формата
            if not await validate_message_format(msg_data):
                continue

            # Извлекаем данные
            recipients = msg_data['recipients']
            content = msg_data['content']
            post_num = msg_data['post_num']
            reply_info = msg_data.get('reply_info', {})

            # Быстрая фильтрация получателей (используем state доски)
            active_recipients = {
                uid for uid in recipients
                if uid not in boards_state[board_id]['users_data']['banned']
            }

            if not active_recipients:
                board_queue.task_done()
                continue

            # Основная отправка через функцию, которая теперь принимает инстанс бота
            results = await send_message_to_users(
                bot_instance, # Передаем нужного бота!
                active_recipients,
                content,
                reply_info
            )

            # Обрабатываем успешные отправки, указывая доску
            await process_successful_messages(board_id, post_num, results)
            
            board_queue.task_done()

        except Exception as e:
            print(f"{worker_name} | ⛔ Критическая ошибка: {str(e)[:200]}")
            await asyncio.sleep(1)
            
async def validate_message_format(msg_data: dict) -> bool:
    """Быстрая валидация формата сообщения"""
    if not isinstance(msg_data, dict):
        return False

    required = ['recipients', 'content', 'post_num']
    if any(key not in msg_data for key in required):
        return False

    if not isinstance(msg_data['recipients'], (set, list)):
        return False

    if not isinstance(msg_data['content'], dict):
        return False

    if (msg_data['content'].get('type') == 'media_group' and 
        not isinstance(msg_data['content'].get('media'), list)):
        return False

    return True

async def process_successful_messages(board_id: str, post_num: int, results: list):
    """Обработка успешных отправок для конкретной доски."""
    board_post_to_messages = post_to_messages[board_id]
    board_message_to_post = message_to_post[board_id]

    board_post_to_messages.setdefault(post_num, {})

    for uid, msg in results:
        if not msg:
            continue

        if isinstance(msg, list):  # Медиагруппа
            board_post_to_messages[post_num][uid] = msg[0].message_id
            for m in msg:
                board_message_to_post[(uid, m.message_id)] = post_num
        else:  # Одиночное сообщение
            board_post_to_messages[post_num][uid] = msg.message_id
            board_message_to_post[(uid, msg.message_id)] = post_num

def self_clean_memory():
    """Автоматическая очистка старых сообщений"""
    if len(post_to_messages) > MAX_MESSAGES_IN_MEMORY * 1.5:
        # Удаляем 20% самых старых постов
        all_posts = sorted(post_to_messages.keys())
        to_delete = int(len(all_posts) * 0.2)
        posts_to_delete = all_posts[:to_delete]

        deleted = 0
        for post in posts_to_delete:
            # Удаляем из всех словарей
            deleted += len(post_to_messages.get(post, {}))
            post_to_messages.pop(post, None)
            messages_storage.pop(post, None)

        # Чистим message_to_post
        global message_to_post
        message_to_post = {
            k: v
            for k, v in message_to_post.items() if v not in posts_to_delete
        }

        print(f"🧹 Очистка памяти: удалено {deleted} сообщений")
        gc.collect()

async def reset_violations_after_hour(user_id: int):
    """Сбрасывает счетчик нарушений через час"""
    await asyncio.sleep(3600)  # 1 час
    if user_id in spam_violations:
        spam_violations[user_id] = 0

async def fetch_dvach_thread(board: str, only_new: bool = False):
    """Получает случайный тред с двача"""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                    f'https://2ch.hk/{board}/catalog.json') as response:
                if response.status != 200:
                    return None

                data = await response.json()

                if not data or 'threads' not in data:
                    return None

                threads = data['threads']
                if not threads:
                    return None

                if only_new and board == 'news':
                    threads.sort(key=lambda x: x.get('timestamp', 0),
                                 reverse=True)
                    threads = threads[:10]

                thread = random.choice(threads)
                thread_num = thread.get('num')

                if not thread_num:
                    return None

                async with session.get(
                        f'https://2ch.hk/{board}/res/{thread_num}.json'
                ) as thread_response:
                    if thread_response.status != 200:
                        return None

                    thread_data = await thread_response.json()

                    if not thread_data or 'threads' not in thread_data:
                        return None

                    posts = thread_data['threads'][0]['posts']
                    if not posts:
                        return None

                    op_post = posts[0]
                    text = op_post.get('comment', '')
                    text = re.sub(r'<[^>]+>', '', text)
                    text = text.replace('&gt;', '>')
                    text = text.replace('&lt;', '<')
                    text = text.replace('&amp;', '&')
                    text = text.replace('&quot;', '"')
                    text = text.replace('&#47;', '/')
                    text = text.replace('<br>', '\n')

                    if len(text) > 500:
                        text = text[:500] + '...'

                    link = f"https://2ch.hk/{board}/res/{thread_num}.html"

                    if board == 'news' or random.random() > 0.5:
                        result = f"Тред с /{board}/:\n\n"
                        result += f"{text}\n\n"
                        result += link
                    else:
                        comment = random.choice(THREAD_COMMENTS)
                        result = f"{link}\n\n{comment}"
                        if text and random.random() > 0.3:
                            result = f"{text}\n\n{link}\n\n{comment}"

                    return result

    except Exception as e:
        print(f"Ошибка получения треда с /{board}/: {e}")
        return None

async def news_poster():
    """Постер новостей с /news/"""
    while True:
        try:
            await asyncio.sleep(news_interval * 90)

            if not state['settings']['dvach_enabled'] or not state[
                    'users_data']['active']:
                continue

            thread_text = await fetch_dvach_thread('news', only_new=True)
            if not thread_text:
                continue

            await post_dvach_thread(thread_text, 'news')

        except Exception as e:
            print(f"Ошибка в news постере: {e}")
            await asyncio.sleep(90)


async def boards_poster():
    """Постер тредов с /b/ и /po/"""
    while True:
        try:
            wait_minutes = random.randint(boards_interval_min,
                                          boards_interval_max)
            await asyncio.sleep(wait_minutes * 20)

            if not state['settings']['dvach_enabled'] or not state[
                    'users_data']['active']:
                continue

            board = random.choice(['b', 'po'])
            thread_text = await fetch_dvach_thread(board)
            if not thread_text:
                continue

            await post_dvach_thread(thread_text, board)

        except Exception as e:
            print(f"Ошибка в boards постере: {e}")
            await asyncio.sleep(50)


async def post_dvach_thread(thread_text: str, board: str):
    """Отправляет тред всем пользователям через очередь"""
    active_users = list(state['users_data']['active'] -
                        state['users_data']['banned'])
    if not active_users:
        return

    fake_sender = random.choice(active_users)
    recipients = state['users_data']['active'] - {
        fake_sender
    } - state['users_data']['banned']

    if recipients:
        header, current_post_num = format_header()

        post_to_messages[current_post_num] = {}
        messages_storage[current_post_num] = {
            'author': fake_sender,
            'messages': {}
        }

        await message_queue.put({
            'recipients': recipients,
            'content': {
                'type': 'text',
                'header': header,
                'text': escape_html(thread_text)
            },
            'post_num': current_post_num
        })

        print(f"Тред с /{board}/ добавлен в очередь")


def generate_bot_message():
    """Генерирует сообщение в стиле двача"""
    msg_type = random.choice(
        ['starter', 'topic', 'combined', 'question', 'reaction', 'statement'])

    if msg_type == 'starter':
        return random.choice(DVACH_STARTERS)

    elif msg_type == 'topic':
        topic = random.choice(DVACH_TOPICS)
        ending = random.choice(DVACH_ENDINGS)
        connector = random.choice([" - ", ". ", "! ", "? ", ", "])
        return f"{topic.capitalize()}{connector}{ending}"

    elif msg_type == 'combined' and len(last_messages) > 5:
        words = []
        for msg in random.sample(list(last_messages),
                                 min(5, len(last_messages))):
            if msg and isinstance(msg, str):
                msg_words = msg.split()
                if msg_words:
                    words.extend(
                        random.sample(msg_words, min(2, len(msg_words))))

        if words:
            random.shuffle(words)
            phrase = ' '.join(words[:random.randint(3, 6)])
            return f"{phrase}, {random.choice(DVACH_ENDINGS).lower()}"

    elif msg_type == 'question':
        questions = [
            "Че по {topic}?", "{topic} - за или против?",
            "Кто шарит за {topic}?", "Анон, расскажи про {topic}",
            "{topic} это база или кринж?", "Почему все хейтят {topic}?",
            "Когда уже запретят {topic}?", "{topic} переоценен?",
            "Кто-нибудь юзает {topic}?", "Стоит ли начинать с {topic}?"
        ]
        topic = random.choice(DVACH_TOPICS)
        return random.choice(questions).format(topic=topic)

    elif msg_type == 'reaction':
        reactions = [
            "пиздец", "ору", "база", "кринж", "ахуеть", "топчик", "годнота",
            "хуйня", "збс", "четко", "ебать", "нихуя себе", "охуенно", "хуево",
            "норм"
        ]
        return random.choice(reactions)

    elif msg_type == 'statement':
        statements = [
            "короче я заебался", "все хуйня, давай по новой",
            "я ебал эту жизнь", "заебись живем", "ну и времена настали",
            "раньше лучше было", "деградируем потихоньку",
            "эволюция в обратную сторону"
        ]
        return random.choice(statements)

    return random.choice(DVACH_STARTERS)

async def motivation_broadcaster():
    """Отправляет мотивационные сообщения раз в 2-4 часа"""
    await asyncio.sleep(9)  # Ждём 9 сек после старта

    while True:
        try:
            # Случайная задержка от 2 до 4 часов
            delay = random.randint(7200, 14400)  # 2-4 часа в секундах
            await asyncio.sleep(delay)

            # Проверяем что есть активные пользователи
            if not state['users_data']['active']:
                print("ℹ️ Нет активных пользователей для мотивационного сообщения")
                continue

            # Выбираем случайные тексты
            motivation = random.choice(MOTIVATIONAL_MESSAGES)
            invite_text = random.choice(INVITE_TEXTS)

            # Формируем сообщение
            now = datetime.now(MSK)
            date_str = now.strftime("%d/%m/%y")
            weekday = WEEKDAYS[now.weekday()]
            time_str = now.strftime("%H:%M:%S")
            header = f"<i>### АДМИН ### {date_str} ({weekday}) {time_str}</i>"
            state['post_counter'] += 1
            post_num = state['post_counter']

            # ИСПРАВЛЕНИЕ: Убрано дублирование текста, добавлено экранирование HTML
            message_text = (
                f"💭 {motivation}\n\n"
                f"Скопируй и отправь анончикам:\n"
                f"<code>{escape_html(invite_text)}</code>"
            )

            # Формируем контент для очереди
            content = {
                'type': 'text',
                'header': header,
                'text': message_text
            }

            # ИСПРАВЛЕНИЕ: Правильный расчет получателей
            recipients = state['users_data']['active'] - state['users_data']['banned']

            if not recipients:
                print("ℹ️ Нет получателей для мотивационного сообщения")
                continue

            # Добавляем в очередь рассылки
            await message_queue.put({
                'recipients': recipients,
                'content': content,
                'post_num': post_num,
                'reply_info': None
            })

            print(f"✅ Мотивационное сообщение #{post_num} добавлено в очередь")

        except Exception as e:
            print(f"❌ Ошибка в motivation_broadcaster: {e}")
            await asyncio.sleep(60)  # Ждем минуту при ошибке

async def check_cooldown(board_id: str, message: Message) -> bool:
    """Проверяет кулдаун на активацию режимов для конкретной доски."""
    modes = board_modes[board_id]
    last_activation = modes.get('last_mode_activation')

    if last_activation is None:
        return True

    elapsed = (datetime.now(UTC) - last_activation).total_seconds()
    if elapsed < MODE_COOLDOWN:
        time_left = MODE_COOLDOWN - elapsed
        minutes = int(time_left // 60)
        seconds = int(time_left % 60)
        try:
            await message.answer(f"⏳ Режимы на этой доске можно менять раз в час. Осталось: {minutes}м {seconds}с")
            await message.delete()
        except Exception: pass
        return False
    return True

# ========== КОМАНДЫ ==========

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    # Автоматически добавляем в активные
    if user_id not in state['users_data']['active']:
        state['users_data']['active'].add(user_id)
        print(f"✅ Новый пользователь через /start: ID {user_id}")
        logging.info(f"Новый пользователь через /start: ID {user_id}")

    await message.answer(
        "Добро пожаловать в ТГАЧ!\n\n"
        "Правила:\n"
        "- Анонимность\n"
        "- Без CP\n"
        "- Не спамить\n\n"
        "Просто пиши сообщения, они будут отправлены всем анонимно. Всем от всех."
        "Команды: \n /roll ролл \n /stats стата \n /face лицо \n / \n /help помощь \n /invite пригласить \n /zaputin кремлебот режим \n /slavaukraine хохол режим \n /suka_blyat злой режим \n /anime аниме режим")
    await message.delete()


AHE_EYES = ['😵', '🤤', '😫', '😩', '😳', '😖', '🥵']
AHE_TONGUE = ['👅', '💦', '😛', '🤪', '😝']
AHE_EXTRA = ['💕', '💗', '✨', '🥴', '']


@dp.message(Command("face"))
async def cmd_face(message: types.Message):
    face = (secrets.choice(AHE_EYES) + secrets.choice(AHE_TONGUE) +
            secrets.choice(AHE_EXTRA))

    # Публикуем всем вместо личного ответа
    header, pnum = format_header()
    await message_queue.put({
        "recipients": state['users_data']['active'],
        "content": {
            "type": "text",
            "header": header,
            "text": face
        },
        "post_num": pnum,
    })

    await message.delete()


@dp.message(Command("help"))
async def cmd_help(message: types.Message):
    await message.answer(HELP_TEXT)
    await message.delete()


@dp.message(Command("roll"))
async def cmd_roll(message: types.Message):
    result = random.randint(1, 100)

    header, pnum = format_header()
    await message_queue.put({
        "recipients": state['users_data']['active'],
        "content": {
            "type": "text",
            "header": header,
            "text": f"🎲 Нароллил: {result}"
        },
        "post_num": pnum,
    })

    await message.delete()

@dp.message(Command("slavaukraine"))
async def cmd_slavaukraine(message: types.Message):
    board_id = get_board_id_from_message(message)
    if not board_id: return

    if not await check_cooldown(board_id, message):
        return

    modes = board_modes[board_id]
    modes['anime_mode'] = False
    modes['zaputin_mode'] = False
    modes['slavaukraine_mode'] = True
    modes['suka_blyat_mode'] = False
    modes['last_mode_activation'] = datetime.now(UTC)

    header = "### Админ ###"
    global global_post_counter
    global_post_counter += 1
    pnum = global_post_counter

    activation_text = (
        "УВАГА! АКТИВОВАНО УКРАЇНСЬКИЙ РЕЖИМ!\n\n"
        "💙💛 СЛАВА УКРАЇНІ! 💛💙\n"
        "ГЕРОЯМ СЛАВА!\n\n"
        "Хто не скаже 'Путін хуйло' - той москаль і підар!"
    )

    await message_queues[board_id].put({
        "recipients": boards_state[board_id]['users_data']['active'],
        "content": {
            "type": "text",
            "header": header,
            "text": activation_text
        },
        "post_num": pnum,
    })

    asyncio.create_task(disable_mode_after_delay(board_id, 'slavaukraine_mode', 300))
    await message.delete()```

async def disable_mode_after_delay(board_id: str, mode_name: str, delay: int):
    """
    Универсальная функция для отключения любого режима на указанной доске через N секунд.
    """
    await asyncio.sleep(delay)

    # Проверяем, существует ли доска и включен ли еще этот режим
    if board_id in board_modes and board_modes[board_id].get(mode_name, False):
        board_modes[board_id][mode_name] = False

        end_text_map = {
            'anime_mode': "Аниме было ошибкой. Режим отключен. Возвращаемся к обычному общению.",
            'zaputin_mode': "Бунт кремлеботов окончился. Всем спасибо, все свободны.",
            'slavaukraine_mode': "Визг хохлов закончен. Возвращаемся к обычному трёпу.",
            'suka_blyat_mode': "СУКА БЛЯТЬ КОНЧИЛОСЬ. Теперь можно и помолчать."
        }
        end_text = end_text_map.get(mode_name, "Режим отключен.")

        header = "### Админ ###"
        global global_post_counter
        global_post_counter += 1
        pnum = global_post_counter

        await message_queues[board_id].put({
            "recipients": boards_state[board_id]['users_data']['active'],
            "content": {"type": "text", "header": header, "text": end_text},
            "post_num": pnum,
        })
        print(f"[{board_id}] Режим {mode_name} автоматически отключен.")

async def reset_violations_after_hour(user_id: int):
    await asyncio.sleep(3600)
    if user_id in spam_violations:
        spam_violations[user_id]['level'] = 0

@dp.message(Command("stop"))
async def cmd_stop(message: types.Message):
    """Остановка любых активных режимов без уведомления"""
    global zaputin_mode, suka_blyat_mode, slavaukraine_mode

    if not is_admin(message.from_user.id):
        await message.delete()
        return

    # Сбрасываем все режимы
    zaputin_mode = False
    suka_blyat_mode = False
    slavaukraine_mode = False

    await message.answer("Все активные режимы остановлены")
    await message.delete()

@dp.message(Command("invite"))
async def cmd_invite(message: types.Message):
    """Получить текст для приглашения анонов"""
    invite_text = random.choice(INVITE_TEXTS)

    await message.answer(
        f"📨 <b>Текст для приглашения анонов:</b>\n\n"
        f"<code>{invite_text}</code>\n\n"
        f"<i>Просто скопируй и отправь</i>",
        parse_mode="HTML")


@dp.message(Command("stats"))
async def cmd_stats(message: types.Message):
    board_id = get_board_id_from_message(message)
    if not board_id: return

    # --- Статистика для текущей доски ---
    state = boards_state[board_id]
    total_users_on_board = len(state['users_data']['active'])
    
    now = datetime.now(UTC)
    last_24_hours = timedelta(hours=24)
    active_posters_24h_board = sum(
        1 for timestamp in state['user_activity'].values()
        if now - timestamp < last_24_hours
    )

    # --- Общая статистика по всем доскам ---
    total_users_all_boards = 0
    # Для подсчета уникальных постеров по всем доскам
    all_active_posters_ids = set() 
    all_active_users = set()

    for b_id, b_state in boards_state.items():
        # Считаем уникальных пользователей по всем доскам
        all_active_users.update(b_state['users_data']['active'])
        
        # Собираем ID постеров за 24 часа с каждой доски
        for uid, timestamp in b_state['user_activity'].items():
            if now - timestamp < last_24_hours:
                all_active_posters_ids.add(uid)
    
    total_users_all_boards = len(all_active_users)
    total_active_posters_24h = len(all_active_posters_ids)

    # --- Формируем текст ---
    stats_text = (
        f"📊 **Статистика доски {BOARD_INFO[board_id]['name']}**\n"
        f"👥 Анонимов здесь: {total_users_on_board}\n"
        f"✍️ Активных за сутки: {active_posters_24h_board}\n\n"
        f"--- **Общая статистика** ---\n"
        f"👥 Всего уникальных анонов: {total_users_all_boards}\n"
        f"✍️ Всего активных постеров: {total_active_posters_24h}\n"
        f"📨 Всего постов: {global_post_counter}"
    )

    header, pnum = format_header(board_id)
    await message_queues[board_id].put({
        'recipients': state['users_data']['active'],
        'content': {'type': 'text', 'header': header, 'text': stats_text},
        'post_num': pnum
    })
    await message.delete()
    
@dp.message(Command("shadowmute"))
async def cmd_shadowmute(message: Message, command: CommandObject):
    board_id = get_board_id_from_message(message)
    if not board_id or not is_admin(message.from_user.id):
        await message.delete()
        return

    board_shadow_mutes = shadow_mutes[board_id]
    target_id = None
    
    if message.reply_to_message:
        target_id = get_author_id_by_reply(message, board_id)
    
    if not target_id and command.args:
        try:
            target_id = int(command.args.split()[0])
        except (ValueError, IndexError): pass

    if not target_id:
        await message.answer("❌ Не удалось определить пользователя. Ответьте на сообщение или укажите /shadowmute <ID> [время].")
        await message.delete()
        return

    duration_str = "24h"
    if command.args:
        args = command.args.split()
        if message.reply_to_message and len(args) >= 1: duration_str = args[0]
        elif not message.reply_to_message and len(args) >= 2: duration_str = args[1]

    try:
        if duration_str.endswith("m"): total_seconds = int(duration_str[:-1]) * 60
        elif duration_str.endswith("h"): total_seconds = int(duration_str[:-1]) * 3600
        elif duration_str.endswith("d"): total_seconds = int(duration_str[:-1]) * 86400
        else: total_seconds = int(duration_str) * 60
        
        total_seconds = min(total_seconds, 2592000)
        board_shadow_mutes[target_id] = datetime.now(UTC) + timedelta(seconds=total_seconds)
        time_str = str(timedelta(seconds=total_seconds))
        await message.answer(f"👻 [{board_id}] Пользователь {target_id} тихо замучен на {time_str}")
    except ValueError:
        await message.answer("❌ Неверный формат времени. Примеры: 30m, 2h, 1d")
    
    await message.delete()

@dp.message(Command("unshadowmute"))
async def cmd_unshadowmute(message: Message):
    board_id = get_board_id_from_message(message)
    if not board_id or not is_admin(message.from_user.id):
        await message.delete()
        return

    target_id = None
    if message.reply_to_message:
        target_id = get_author_id_by_reply(message, board_id)

    if not target_id:
        parts = message.text.split()
        if len(parts) >= 2 and parts[1].isdigit():
            target_id = int(parts[1])

    if not target_id:
        await message.answer("Использование: /unshadowmute <user_id> или ответ на сообщение")
        await message.delete()
        return

    board_shadow_mutes = shadow_mutes[board_id]
    if target_id in board_shadow_mutes:
        del board_shadow_mutes[target_id]
        await message.answer(f"👻 [{board_id}] Пользователь {target_id} тихо размучен")
    else:
        await message.answer(f"ℹ️ [{board_id}] Пользователь {target_id} не в shadow-муте")
    
    await message.delete()


@dp.message(Command("anime"))
async def cmd_anime(message: types.Message):
    board_id = get_board_id_from_message(message)
    if not board_id: return

    # Проверяем кулдаун для этой доски
    if not await check_cooldown(board_id, message):
        return

    # Получаем и изменяем режимы для конкретной доски
    modes = board_modes[board_id]
    modes['anime_mode'] = True
    modes['zaputin_mode'] = False
    modes['slavaukraine_mode'] = False
    modes['suka_blyat_mode'] = False
    modes['last_mode_activation'] = datetime.now(UTC)

    header = "### 管理者 ###"
    global global_post_counter
    global_post_counter += 1
    pnum = global_post_counter

    activation_text = (
        "にゃあ～！アニメモードがアクティベートされました！\n\n"
        "^_^"
    )

    # Кладем сообщение в очередь этой доски
    await message_queues[board_id].put({
        "recipients": boards_state[board_id]['users_data']['active'],
        "content": {
            "type": "text",
            "header": header,
            "text": activation_text
        },
        "post_num": pnum,
    })

    # Запускаем таймер на отключение режима для этой доски
    asyncio.create_task(disable_mode_after_delay(board_id, 'anime_mode', 300))
    await message.delete()

@dp.message(Command("deanon"))
async def cmd_deanon(message: Message):
    board_id = get_board_id_from_message(message)
    if not board_id: return

    if not message.reply_to_message:
        await message.answer("⚠️ Ответь на сообщение для деанона!")
        await message.delete()
        return
        
    # Адаптируем вызов, передавая все нужные данные
    await process_deanon_command(
        message=message,
        board_id=board_id,
        board_message_to_post=message_to_post[board_id],
        global_messages_storage=messages_storage,
        board_state=boards_state[board_id],
        board_post_to_messages=post_to_messages[board_id],
        board_queue=message_queues[board_id],
        format_header_func=format_header
    )
    
# ====== ZAPUTIN ======
@dp.message(Command("zaputin"))
async def cmd_zaputin(message: types.Message):
    board_id = get_board_id_from_message(message)
    if not board_id: return

    if not await check_cooldown(board_id, message):
        return

    modes = board_modes[board_id]
    modes['anime_mode'] = False
    modes['zaputin_mode'] = True
    modes['slavaukraine_mode'] = False
    modes['suka_blyat_mode'] = False
    modes['last_mode_activation'] = datetime.now(UTC)

    header = "### Админ ###"
    global global_post_counter
    global_post_counter += 1
    pnum = global_post_counter

    activation_text = (
        "🇷🇺 СЛАВА РОССИИ! ПУТИН - НАШ ПРЕЗИДЕНТ! 🇷🇺\n\n"
        "Активирован режим кремлеботов! Все несогласные будут приравнены к пидорасам и укронацистам!"
    )

    await message_queues[board_id].put({
        "recipients": boards_state[board_id]['users_data']['active'],
        "content": {
            "type": "text",
            "header": header,
            "text": activation_text
        },
        "post_num": pnum,
    })

    asyncio.create_task(disable_mode_after_delay(board_id, 'zaputin_mode', 300))
    await message.delete()


@dp.message(Command("suka_blyat"))
async def cmd_suka_blyat(message: types.Message):
    board_id = get_board_id_from_message(message)
    if not board_id: return

    if not await check_cooldown(board_id, message):
        return

    modes = board_modes[board_id]
    modes['anime_mode'] = False
    modes['zaputin_mode'] = False
    modes['slavaukraine_mode'] = False
    modes['suka_blyat_mode'] = True
    modes['last_mode_activation'] = datetime.now(UTC)
    modes['suka_blyat_counter'] = 0 # Сбрасываем счетчик при активации

    header = "### Админ ###"
    global global_post_counter
    global_post_counter += 1
    pnum = global_post_counter

    activation_text = (
        "💢💢💢 Активирован режим СУКА БЛЯТЬ! 💢💢💢\n\n"
        "Всех нахуй разъебало!"
    )

    await message_queues[board_id].put({
        "recipients": boards_state[board_id]['users_data']['active'],
        "content": {
            "type": "text",
            "header": header,
            "text": activation_text
        },
        "post_num": pnum,
    })

    asyncio.create_task(disable_mode_after_delay(board_id, 'suka_blyat_mode', 300))
    await message.delete()

# ========== АДМИН КОМАНДЫ ==========


@dp.message(Command("admin"))
async def cmd_admin(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.delete()
        return

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📊 Статистика", callback_data="stats"),
            InlineKeyboardButton(text="🚫 Забаненные", callback_data="banned")
        ],
        [
            InlineKeyboardButton(text="💾 Сохранить", callback_data="save"),
            InlineKeyboardButton(text="👥 Топ спамеров",
                                 callback_data="spammers")
        ]
    ])

    await message.answer(
        f"Админка:\n\n"
        f"Команды:\n"
        f"/ban /mute /del /wipe /unban /unmute\n",
        reply_markup=keyboard)
    await message.delete()


# ===== Вспомогательная функция =====================================
def get_author_id_by_reply(message: types.Message, board_id: str) -> int | None:
    """Получаем ID автора поста по reply для КОНКРЕТНОЙ доски."""
    if not message.reply_to_message:
        return None
    board_message_to_post = message_to_post.get(board_id, {})
    reply_mid = message.reply_to_message.message_id
    
    # Ищем пост в словаре ответов этой доски
    post_num = None
    # Итерируемся по копии, чтобы избежать ошибок изменения словаря во время итерации
    for (uid, mid), pnum in list(board_message_to_post.items()):
        if mid == reply_mid:
            post_num = pnum
            break
            
    if post_num and post_num in messages_storage:
        return messages_storage[post_num].get("author_id")
    return None

# ===== /id ==========================================================
@dp.message(Command("id"))
@dp.message(Command("id"))
async def cmd_get_id(message: types.Message):
    board_id = get_board_id_from_message(message)
    if not board_id: return

    target_id = None
    if message.reply_to_message:
        board_message_to_post = message_to_post[board_id]
        reply_mid = message.reply_to_message.message_id
        for (uid, mid), pnum in board_message_to_post.items():
            if mid == reply_mid:
                target_id = messages_storage.get(pnum, {}).get("author_id")
                break
    else:
        # Если не реплай, показываем ID того, кто вызвал команду
        target_id = message.from_user.id
    
    if target_id:
        try:
            author = await message.bot.get_chat(target_id)
            info = f"🆔 <b>Информация о пользователе на доске /{board_id}/:</b>\n\n"
            info += f"ID: <code>{author_id}</code>\n"
            info += f"Имя: {escape_html(author.first_name)}\n"
            if author.last_name:
                info += f"Фамилия: {escape_html(author.last_name)}\n"
            if author.username:
                info += f"Username: @{author.username}\n"

            # Статус на этой доске
            state = boards_state[board_id]
            if target_id in state['users_data']['banned']:
                info += "\n⛔️ Статус: **ЗАБАНЕН**"
            elif target_id in mutes.get(board_id, {}):
                 info += "\n🔇 Статус: В муте"
            elif target_id in state['users_data']['active']:
                info += "\n✅ Статус: Активен"
            else:
                info += "\n⚪️ Статус: Неактивен"
                
            await message.answer(info, parse_mode="HTML")
        except Exception:
            await message.answer(f"ID пользователя: <code>{target_id}</code>", parse_mode="HTML")
    else:
        await message.answer("Не удалось определить ID пользователя.")
        
    await message.delete()

@dp.message(Command("ban"))
async def cmd_ban(message: types.Message):
    """
    /ban  – reply-бан автора сообщения
    /ban <id> – бан по ID
    (Работает для конкретной доски)
    """
    board_id = get_board_id_from_message(message)
    if not board_id or not is_admin(message.from_user.id):
        await message.delete()
        return

    state = boards_state[board_id]
    board_message_to_post = message_to_post[board_id]
    target_id: int | None = None

    if message.reply_to_message:
        reply_mid = message.reply_to_message.message_id
        for (uid, mid), pnum in board_message_to_post.items():
            if mid == reply_mid:
                target_id = messages_storage.get(pnum, {}).get("author_id")
                break
    else:
        parts = message.text.split()
        if len(parts) == 2 and parts[1].isdigit():
            target_id = int(parts[1])

    if not target_id:
        await message.answer("Нужно ответить на сообщение или указать ID: /ban <id>")
        return

    # ИСПОЛЬЗУЕМ АДАПТИРОВАННУЮ ФУНКЦИЮ
    deleted_posts = await delete_user_posts(board_id, target_id, 60) # Удаляем посты за последний час

    state['users_data']['banned'].add(target_id)
    state['users_data']['active'].discard(target_id)

    deleted_posts_msg = f"Удалено его постов за последний час: {deleted_posts}"

    await message.answer(
        f"✅ [{board_id}] Пользователь <code>{target_id}</code> забанен.\n{deleted_posts_msg}",
        parse_mode="HTML")

    header = "### Админ ###"
    global global_post_counter
    global_post_counter += 1
    post_num = global_post_counter
    ban_text = f"🚨 Пользователь был забанен. {deleted_posts_msg}"
    
    await message_queues[board_id].put({
        "recipients": state['users_data']['active'],
        "content": {"type": "text", "header": header, "text": ban_text},
        "post_num": post_num,
    })

    try:
        await message.bot.send_message(
            target_id,
            f"Вы были забанены на доске /{board_id}/.\n"
            f"Удалено ваших постов за последний час: {deleted_posts}"
        )
    except Exception:
        pass

    await message.delete()

@dp.message(Command("mute"))
async def cmd_mute(message: Message, command: CommandObject):
    """
    /mute [время] – reply-мут автора
    /mute <id> [время] – мут по ID
    (Работает для конкретной доски)
    """
    board_id = get_board_id_from_message(message)
    if not board_id or not is_admin(message.from_user.id):
        await message.delete()
        return

    # Получаем хранилища для этой доски
    board_mutes = mutes[board_id]
    board_message_to_post = message_to_post[board_id]
    args = command.args
    
    if not args:
        await message.answer("Использование: reply + /mute [время] или /mute <user_id> [время]")
        await message.delete()
        return

    parts = args.split()
    target_id = None
    duration_str = "1h"  # значение по умолчанию

    if message.reply_to_message:
        reply_mid = message.reply_to_message.message_id
        for (uid, mid), pnum in board_message_to_post.items():
            if mid == reply_mid:
                target_id = messages_storage.get(pnum, {}).get('author_id')
                break
        if target_id and parts:
            duration_str = parts[0]
    else:
        if len(parts) >= 1 and parts[0].isdigit():
            target_id = int(parts[0])
            if len(parts) >= 2:
                duration_str = parts[1]
        else:
             await message.answer("Неверный ID пользователя.")
             await message.delete()
             return

    if not target_id:
        await message.answer("❌ Не удалось определить пользователя для мута.")
        await message.delete()
        return

    try:
        duration_str = duration_str.lower().replace(" ", "")
        if duration_str.endswith("m"): mute_seconds = int(duration_str[:-1]) * 60
        elif duration_str.endswith("h"): mute_seconds = int(duration_str[:-1]) * 3600
        elif duration_str.endswith("d"): mute_seconds = int(duration_str[:-1]) * 86400
        else: mute_seconds = int(duration_str) * 60
        
        mute_seconds = min(mute_seconds, 2592000) # Максимум 30 дней
        mute_duration = timedelta(seconds=mute_seconds)
        duration_text = str(mute_duration)

    except (ValueError, AttributeError):
        await message.answer("❌ Неверный формат времени (Примеры: 30m, 2h, 1d).")
        await message.delete()
        return

    # Применяем мут для этой доски
    board_mutes[target_id] = datetime.now(UTC) + mute_duration

    await message.answer(f"🔇 [{board_id}] Пидор {target_id} замучен на {duration_text}.")
    
    # Личное уведомление пользователю
    try:
        await message.bot.send_message(
            target_id,
            f"🔇 Хуйло словило мут на доске /{board_id}/ на срок {duration_text}.\n"
            "Вы можете читать сообщения, но не можете писать. Соси хуй."
        )
    except Exception:
        pass

    await message.delete()


@dp.message(Command("wipe"))
async def cmd_wipe(message: types.Message):
    """
    Удалить ВСЕ сообщения указанного юзера.
    Использование:
       reply + /wipe        – автор реплая
       /wipe <id>           – по ID
    """
    global message_to_post  # ДОЛЖНО БЫТЬ В САМОМ НАЧАЛЕ ФУНКЦИИ!
    if not is_admin(message.from_user.id):
        return

    target_id = None
    if message.reply_to_message:
        # Получаем ID автора сообщения, на которое ответили
        reply_key = (message.from_user.id, message.reply_to_message.message_id)
        post_num = message_to_post.get(reply_key)
        if post_num:
            target_id = messages_storage.get(post_num, {}).get('author_id')
    else:
        parts = message.text.split()
        if len(parts) == 2 and parts[1].isdigit():
            target_id = int(parts[1])

    if not target_id:
        await message.answer("reply + /wipe  или  /wipe <id>")
        return

    # Находим все посты этого пользователя
    posts_to_delete = [
        pnum for pnum, info in messages_storage.items()
        if info.get("author_id") == target_id
    ]

    deleted = 0
    for pnum in posts_to_delete:
        # Удаляем у всех получателей
        for uid, mid in post_to_messages.get(pnum, {}).items():
            try:
                await bot.delete_message(uid, mid)
                deleted += 1
            except Exception as e:
                print(f"Ошибка удаления сообщения {mid} у пользователя {uid}: {e}")

        # Удаляем у автора (если сохранили author_message_id)
        author_mid = messages_storage[pnum].get("author_message_id")
        if author_mid:
            try:
                await bot.delete_message(target_id, author_mid)
                deleted += 1
            except Exception as e:
                print(f"Ошибка удаления сообщения у автора {target_id}: {e}")

        # Удаляем из хранилищ
        post_to_messages.pop(pnum, None)
        messages_storage.pop(pnum, None)

    # Также удаляем все связи в message_to_post
    message_to_post = {
        k: v for k, v in message_to_post.items() 
        if v not in posts_to_delete
    }

    await message.answer(
        f"🗑 Удалено {len(posts_to_delete)} постов ({deleted} сообщений) пользователя {target_id}"
    )


@dp.message(Command("unmute"))
@dp.message(Command("unmute"))
async def cmd_unmute(message: types.Message):
    board_id = get_board_id_from_message(message)
    if not board_id or not is_admin(message.from_user.id):
        await message.delete()
        return

    # Получаем хранилища для этой доски
    board_mutes = mutes[board_id]
    board_message_to_post = message_to_post[board_id]
    target_id = None

    if message.reply_to_message:
        reply_mid = message.reply_to_message.message_id
        for (uid, mid), pnum in board_message_to_post.items():
            if mid == reply_mid:
                target_id = messages_storage.get(pnum, {}).get("author_id")
                break
    else:
        parts = message.text.split()
        if len(parts) == 2 and parts[1].isdigit():
            target_id = int(parts[1])

    if not target_id:
        await message.answer("Нужно ответить на сообщение или указать ID: /unmute <id>")
        return

    if target_id in board_mutes:
        board_mutes.pop(target_id, None)
        await message.answer(f"🔈 [{board_id}] Пользователь {target_id} размучен.")
        try:
            await message.bot.send_message(target_id, f"С вас снят мут на доске /{board_id}/.")
        except:
            pass
    else:
        await message.answer(f"ℹ️ [{board_id}] Пользователь {target_id} не в муте на этой доске.")
    
    await message.delete()


@dp.message(Command("unban"))
async def cmd_unban(message: types.Message):
    board_id = get_board_id_from_message(message)
    if not board_id or not is_admin(message.from_user.id):
        await message.delete()
        return

    args = message.text.split()
    if len(args) < 2 or not args[1].isdigit():
        await message.answer("Использование: /unban <user_id>")
        await message.delete()
        return

    try:
        user_id = int(args[1])
        state = boards_state[board_id]
        
        if user_id in state['users_data']['banned']:
            state['users_data']['banned'].discard(user_id)
            state['users_data']['active'].add(user_id) # Возвращаем в активные
            await message.answer(f"✅ [{board_id}] Пользователь {user_id} разбанен.")
            try:
                await message.bot.send_message(user_id, f"Вы были разбанены на доске /{board_id}/.")
            except:
                pass
        else:
            await message.answer(f"ℹ️ [{board_id}] Пользователь {user_id} не был забанен на этой доске.")

    except ValueError:
        await message.answer("Неверный ID пользователя.")

    await message.delete()


@dp.message(Command("del"))
async def cmd_del(message: types.Message):
    board_id = get_board_id_from_message(message)
    if not board_id or not is_admin(message.from_user.id):
        await message.delete()
        return

    if not message.reply_to_message:
        await message.answer("Ответь на сообщение, которое нужно удалить.")
        return

    # Получаем хранилища для этой доски
    board_message_to_post = message_to_post[board_id]
    board_post_to_messages = post_to_messages[board_id]
    
    target_mid = message.reply_to_message.message_id
    post_num = None
    
    # Ищем пост в словаре ответов этой доски
    for (uid, mid), pnum in board_message_to_post.items():
        if mid == target_mid:
            post_num = pnum
            break

    if post_num is None:
        await message.answer("Не нашёл этот пост в базе данной доски.")
        return

    deleted_count = 0
    # Удаляем у всех получателей на этой доске
    if post_num in board_post_to_messages:
        # Проходим по копии словаря, чтобы его можно было изменять
        for uid, mid in list(board_post_to_messages[post_num].items()):
            try:
                # Определяем, какой бот должен удалить сообщение
                # (предполагаем, что все юзеры на доске общаются с одним ботом)
                bot_to_delete_with = bots[board_id]
                await bot_to_delete_with.delete_message(uid, mid)
                deleted_count += 1
            except Exception:
                pass # Сообщение могло быть уже удалено

    # Чистим словари этой доски
    board_post_to_messages.pop(post_num, None)
    
    # Удаляем все записи, связанные с этим номером поста
    keys_to_del = [key for key, val in board_message_to_post.items() if val == post_num]
    for key in keys_to_del:
        del board_message_to_post[key]
        
    # Удаляем сам пост из глобального хранилища
    messages_storage.pop(post_num, None)

    await message.answer(f"Пост №{post_num} удалён у {deleted_count} пользователей доски /{board_id}/.")
    await message.delete()
# ========== CALLBACK HANDLERS ==========

@dp.callback_query(F.data == "save")
async def admin_save(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        return

    await save_state()
    await callback.answer("Состояние сохранено")

@dp.callback_query(F.data == "stats")
async def admin_stats(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа")
        return

    total_users_all_boards = len(set(uid for data in boards_state.values() for uid in data['users_data']['active']))
    total_banned_all_boards = len(set(uid for data in boards_state.values() for uid in data['users_data']['banned']))
    
    text = (
        f"<b>Общая статистика по всем доскам:</b>\n\n"
        f"🤖 Всего досок: {len(bots)}\n"
        f"👥 Всего уникальных анонов: {total_users_all_boards}\n"
        f"🚫 Всего уникальных забаненных: {total_banned_all_boards}\n"
        f"📨 Всего постов: {global_post_counter}\n"
        f"🧠 Постов в памяти: {len(messages_storage)}\n"
    )
    await callback.message.answer(text, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "spammers")
async def admin_spammers(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа")
        return

    # Собираем счетчики со всех досок
    total_message_counter = defaultdict(int)
    for board_id, state in boards_state.items():
        for user_id, count in state['message_counter'].items():
            total_message_counter[user_id] += count

    sorted_users = sorted(total_message_counter.items(), key=lambda x: x[1], reverse=True)[:15]

    text = "<b>Топ-15 спамеров (суммарно по всем доскам):</b>\n\n"
    for user_id, count in sorted_users:
        text += f"ID <code>{user_id}</code>: {count} сообщ.\n"

    await callback.message.answer(text, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "banned")
async def admin_banned(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа")
        return

    text = "<b>Забаненные пользователи (по доскам):</b>\n\n"
    has_bans = False
    for board_id, state in boards_state.items():
        if state['users_data']['banned']:
            has_bans = True
            text += f"<b>Доска /{board_id}/:</b>\n"
            text += ", ".join(f"<code>{uid}</code>" for uid in state['users_data']['banned'])
            text += "\n\n"

    if not has_bans:
        await callback.message.answer("Нет забаненных ни на одной доске.")
    else:
        await callback.message.answer(text, parse_mode="HTML")
    await callback.answer()

async def memory_cleaner():
    """Раз в час чистим старые связи"""
    while True:
        await asyncio.sleep(3600)  # каждый час

        # Оставляем только связи для последних N постов
        if len(messages_storage) > 0:
            recent_posts = set(sorted(messages_storage.keys())[-500:])

            # Чистим message_to_post
            to_delete = []
            for (uid, mid), post_num in message_to_post.items():
                if post_num not in recent_posts:
                    to_delete.append((uid, mid))

            for key in to_delete:
                del message_to_post[key]

            print(f"Очистка памяти: удалено {len(to_delete)} старых связей")

# ========== ОСНОВНОЙ ОБРАБОТЧИК СООБЩЕНИЙ ==========
async def process_complete_media_group(media_group_id: str):
    if media_group_id in sent_media_groups or media_group_id not in current_media_groups:
        return

    group = current_media_groups[media_group_id]
    if not group['media']:
        del current_media_groups[media_group_id]
        return

    sent_media_groups.add(media_group_id)
    
    board_id = group['board_id']
    post_num = group['post_num']
    user_id = group['author_id']
    bot_instance = bots[board_id]
    board_post_to_messages = post_to_messages[board_id]
    board_message_to_post = message_to_post[board_id]

    content = {
        'type': 'media_group',
        'header': group['header'],
        'media': group['media'],
        'caption': group.get('caption'),
        'reply_to_post': group.get('reply_to_post')
    }
    
    messages_storage[post_num] = {
        'board_id': board_id,
        'author_id': user_id,
        'timestamp': group['timestamp'],
        'content': content,
        'reply_to': group.get('reply_to_post')
    }

    try:
        builder = MediaGroupBuilder()
        reply_info = board_post_to_messages.get(group['reply_to_post'], {}) if group.get('reply_to_post') else {}
        reply_to_message_id = reply_info.get(user_id)

        # Формируем подпись для автора
        author_caption = f"<i>{group['header']}</i>"
        if group.get('reply_to_post'):
            author_caption += f"\n\n>>{group.get('reply_to_post')}"
        if group.get('caption'):
            author_caption += f"\n\n{escape_html(group['caption'])}"

        for idx, media in enumerate(group['media']):
            caption = author_caption if idx == 0 else None
            if media['type'] == 'photo': builder.add_photo(media=media['file_id'], caption=caption, parse_mode="HTML" if caption else None)
            elif media['type'] == 'video': builder.add_video(media=media['file_id'], caption=caption, parse_mode="HTML" if caption else None)
            elif media['type'] == 'document': builder.add_document(media=media['file_id'], caption=caption, parse_mode="HTML" if caption else None)
            elif media['type'] == 'audio': builder.add_audio(media=media['file_id'], caption=caption, parse_mode="HTML" if caption else None)
        
        if builder.build():
            sent_messages = await bot_instance.send_media_group(chat_id=user_id, media=builder.build(), reply_to_message_id=reply_to_message_id)
            if sent_messages:
                messages_storage[post_num]['author_message_id'] = sent_messages[0].message_id
                board_post_to_messages.setdefault(post_num, {})[user_id] = sent_messages[0].message_id
                for msg in sent_messages:
                    board_message_to_post[(user_id, msg.message_id)] = post_num
    except Exception as e:
        print(f"Ошибка отправки медиа-альбома автору на доске {board_id}: {e}")

    recipients = boards_state[board_id]['users_data']['active'] - {user_id}
    if recipients:
        await message_queues[board_id].put({
            'recipients': recipients,
            'content': content,
            'post_num': post_num,
            'reply_info': reply_info
        })

    if media_group_id in current_media_groups:
        del current_media_groups[media_group_id]

@dp.message(F.audio)
async def handle_audio(message: Message):
    await handle_audio_message(message)

@dp.message(F.voice)
async def handle_voice(message: Message):
    """Обработчик голосовых сообщений"""
    user_id = message.from_user.id

    if user_id in state['users_data']['banned']:
        await message.delete()
        return

    if mutes.get(user_id) and mutes[user_id] > datetime.now(UTC):
        await message.delete()
        return

    # Форматируем заголовок
    header, current_post_num = format_header()

    # Проверяем, это ответ?
    reply_to_post = None
    reply_info = {}

    if message.reply_to_message:
        reply_mid = message.reply_to_message.message_id
        reply_to_post = None
        for (uid, mid), pnum in message_to_post.items():
            if mid == reply_mid:
                reply_to_post = pnum
                break
        if reply_to_post and reply_to_post in post_to_messages:
            reply_info = post_to_messages[reply_to_post]
        else:
            reply_to_post = None

    # Удаляем оригинальное сообщение
    try:
        await message.delete()
    except:
        pass

    # Формируем контент
    content = {
        'type': 'voice',
        'header': header,
        'file_id': message.voice.file_id,
        'reply_to_post': reply_to_post
    }

    # Сохраняем сообщение
    messages_storage[current_post_num] = {
        'author_id': user_id,
        'timestamp': datetime.now(MSK),
        'content': content,
        'reply_to': reply_to_post
    }

    # Отправляем автору только если это не медиа-группа
    if not message.media_group_id:
        reply_to_message_id = reply_info.get(user_id) if reply_info else None

        sent_to_author = await bot.send_voice(
            user_id,
            message.voice.file_id,
            caption=f"<i>{header}</i>",
            reply_to_message_id=reply_to_message_id,
            parse_mode="HTML"
        )

        # Сохраняем для ответов
        if sent_to_author:
            messages_storage[current_post_num]['author_message_id'] = sent_to_author.message_id
            if current_post_num not in post_to_messages:
                post_to_messages[current_post_num] = {}
            post_to_messages[current_post_num][user_id] = sent_to_author.message_id
            message_to_post[(user_id, sent_to_author.message_id)] = current_post_num

    # Отправляем остальным через очередь
    recipients = state['users_data']['active'] - {user_id}
    if recipients:
        await message_queue.put({
            'recipients': recipients,
            'content': content,
            'post_num': current_post_num,
            'reply_info': reply_info if reply_info else None,
            'user_id': user_id
        })

@dp.message(F.media_group_id)
@dp.message(F.media_group_id)
async def handle_media_group_init(message: Message):
    board_id = get_board_id_from_message(message)
    if not board_id: return

    user_id = message.from_user.id
    state = boards_state[board_id]
    board_mutes = mutes[board_id]

    if user_id in state['users_data']['banned'] or (user_id in board_mutes and board_mutes[user_id] > datetime.now(UTC)):
        await message.delete()
        return

    media_group_id = message.media_group_id
    if not media_group_id or media_group_id in sent_media_groups:
        await message.delete()
        return

    # Проверяем ответ на пост
    reply_to_post = None
    if message.reply_to_message:
        board_message_to_post = message_to_post[board_id]
        reply_mid = message.reply_to_message.message_id
        for (r_uid, r_mid), pnum in board_message_to_post.items():
            if r_mid == reply_mid:
                reply_to_post = pnum
                break

    # Инициализация группы
    if media_group_id not in current_media_groups:
        header, post_num = format_header(board_id)
        modes = board_modes[board_id]
        caption = message.caption or ""
        
        if modes.get('slavaukraine_mode'): caption = ukrainian_transform(caption)
        elif modes.get('suka_blyat_mode'): caption = suka_blyatify_text(caption)
        elif modes.get('anime_mode'): caption = anime_transform(caption)
        elif modes.get('zaputin_mode'): caption = zaputin_transform(caption)

        current_media_groups[media_group_id] = {
            'board_id': board_id,
            'post_num': post_num,
            'header': header,
            'author_id': user_id,
            'timestamp': datetime.now(UTC),
            'media': [],
            'caption': caption,
            'reply_to_post': reply_to_post,
            'processed_messages': set()
        }

    group = current_media_groups[media_group_id]
    if message.message_id not in group['processed_messages']:
        media_data = {'type': message.content_type, 'file_id': None, 'message_id': message.message_id}
        if message.photo: media_data['file_id'] = message.photo[-1].file_id
        elif message.video: media_data['file_id'] = message.video.file_id
        elif message.document: media_data['file_id'] = message.document.file_id
        elif message.audio: media_data['file_id'] = message.audio.file_id
        
        if media_data['file_id']:
            group['media'].append(media_data)
            group['processed_messages'].add(message.message_id)

    await message.delete()

    if media_group_id in media_group_timers:
        media_group_timers[media_group_id].cancel()
    
    media_group_timers[media_group_id] = asyncio.create_task(
        complete_media_group_after_delay(media_group_id, delay=1.5)
    )


async def complete_media_group_after_delay(media_group_id, delay=1.5):
    try:
        await asyncio.sleep(delay)
        # Перед отправкой снова проверяем, не отправили ли уже эту группу
        if media_group_id in sent_media_groups:
            return
        await process_complete_media_group(media_group_id)
    except asyncio.CancelledError:
        pass
    finally:
        # Очищаем таймер
        if media_group_id in media_group_timers:
            del media_group_timers[media_group_id]
            
@dp.message()
async def handle_message(message: Message):
    board_id = get_board_id_from_message(message)
    if not board_id: return

    if message.media_group_id: return

    state = boards_state[board_id]
    board_mutes = mutes[board_id]
    board_shadow_mutes = shadow_mutes[board_id]
    board_message_to_post = message_to_post[board_id]
    board_post_to_messages = post_to_messages[board_id]
    modes = board_modes[board_id]

    user_id = message.from_user.id
    state['user_activity'][user_id] = datetime.now(UTC)

    if user_id in board_shadow_mutes and board_shadow_mutes[user_id] > datetime.now(UTC):
        await message.delete()
        return

    if not message.text and not message.caption and not message.content_type:
        await message.delete()
        return

    try:
        until = board_mutes.get(user_id)
        if until and until > datetime.now(UTC):
            left = until - datetime.now(UTC)
            await message.delete()
            try:
                await message.bot.send_message(user_id, f"🔇 Ты в муте на доске /{board_id}/ ещё {int(left.total_seconds())}с.")
            except: pass
            return
        elif until:
            board_mutes.pop(user_id, None)

        if user_id not in state['users_data']['active']:
            state['users_data']['active'].add(user_id)
            print(f"✅ [{board_id}] Новый пользователь: ID {user_id}")

        if user_id in state['users_data']['banned']:
            await message.delete()
            return
            
        get_user_msgs_deque(user_id).append(message)

        spam_check = await check_spam(board_id, user_id, message)
        if not spam_check:
            await message.delete()
            msg_type = message.content_type
            if msg_type in ['photo', 'video', 'document'] and message.caption:
                msg_type = 'text'
            await apply_penalty(board_id, user_id, msg_type)
            return

        recipients = state['users_data']['active'] - {user_id}
        
        reply_to_post = None
        reply_info = {}
        if message.reply_to_message:
            reply_mid = message.reply_to_message.message_id
            for (r_uid, r_mid), pnum in board_message_to_post.items():
                if r_mid == reply_mid:
                    reply_to_post = pnum
                    break
            if reply_to_post and reply_to_post in board_post_to_messages:
                reply_info = board_post_to_messages[reply_to_post]
            else:
                reply_to_post = None
        
        header, current_post_num = format_header(board_id)

        if message.text: daily_log.write(f"[{datetime.now(UTC).isoformat()}] [{board_id}] {message.text}\n")
        
        content_type = message.content_type
        await message.delete()

        content = {'type': content_type, 'header': header, 'reply_to_post': reply_to_post}
        
        text_content = ""
        if content_type == 'text': text_content = message.html_text if message.entities else escape_html(message.text)
        elif message.caption: text_content = escape_html(message.caption)

        if text_content:
            if modes.get('suka_blyat_mode'):
                modes['suka_blyat_counter'] += 1
                if modes['suka_blyat_counter'] % 3 == 0: text_content = suka_blyatify_text(text_content) + " ... СУКА БЛЯТЬ!"
                else: text_content = suka_blyatify_text(text_content)
            if modes.get('slavaukraine_mode'): text_content = ukrainian_transform(text_content)
            if modes.get('anime_mode'): text_content = anime_transform(text_content)
            if modes.get('zaputin_mode'): text_content = zaputin_transform(text_content)
        
        if content_type == 'text': content['text'] = text_content
        elif message.caption: content['caption'] = text_content

        if content_type == 'photo': content['file_id'] = message.photo[-1].file_id
        elif content_type == 'video': content['file_id'] = message.video.file_id
        elif content_type == 'animation': content['file_id'] = message.animation.file_id
        elif content_type == 'document': content['file_id'] = message.document.file_id
        elif content_type == 'sticker': content['file_id'] = message.sticker.file_id
        elif content_type == 'audio': content['file_id'] = message.audio.file_id
        elif content_type == 'voice': content['file_id'] = message.voice.file_id
        elif content_type == 'video_note': content['file_id'] = message.video_note.file_id
        
        messages_storage[current_post_num] = {'board_id': board_id, 'author_id': user_id, 'timestamp': datetime.now(UTC), 'content': content, 'reply_to': reply_to_post, 'author_message_id': None}

        reply_to_message_id = reply_info.get(user_id)
        bot_to_send_with = message.bot
        
        try:
            author_caption_text = f"<i>{header}</i>"
            if reply_to_post:
                 author_caption_text += f"\n\n>>{reply_to_post}"

            post_main_text = content.get('text') or content.get('caption') or ''
            if post_main_text:
                if message.entities and content_type == 'text':
                    author_caption_text += f"\n\n{content['text']}"
                else:
                    author_caption_text += f"\n\n{escape_html(post_main_text)}"
            
            sent_to_author = None
            if content_type == 'text':
                sent_to_author = await bot_to_send_with.send_message(user_id, author_caption_text, reply_to_message_id=reply_to_message_id, parse_mode="HTML")
            elif content_type in ['photo', 'video', 'animation', 'document', 'audio']:
                if len(author_caption_text) > 1024: author_caption_text = author_caption_text[:1021] + "..."
                if content_type == 'photo': sent_to_author = await bot_to_send_with.send_photo(user_id, content['file_id'], caption=author_caption_text, reply_to_message_id=reply_to_message_id, parse_mode="HTML")
                elif content_type == 'video': sent_to_author = await bot_to_send_with.send_video(user_id, content['file_id'], caption=author_caption_text, reply_to_message_id=reply_to_message_id, parse_mode="HTML")
                elif content_type == 'animation': sent_to_author = await bot_to_send_with.send_animation(user_id, content['file_id'], caption=author_caption_text, reply_to_message_id=reply_to_message_id, parse_mode="HTML")
                elif content_type == 'document': sent_to_author = await bot_to_send_with.send_document(user_id, content['file_id'], caption=author_caption_text, reply_to_message_id=reply_to_message_id, parse_mode="HTML")
                elif content_type == 'audio': sent_to_author = await bot_to_send_with.send_audio(user_id, content['file_id'], caption=author_caption_text, reply_to_message_id=reply_to_message_id, parse_mode="HTML")
            elif content_type == 'sticker':
                sent_to_author = await bot_to_send_with.send_sticker(user_id, content['file_id'], reply_to_message_id=reply_to_message_id)
            elif content_type == 'voice':
                sent_to_author = await bot_to_send_with.send_voice(user_id, content['file_id'], caption=f"<i>{header}</i>", reply_to_message_id=reply_to_message_id, parse_mode="HTML")
            elif content_type == 'video_note':
                sent_to_author = await bot_to_send_with.send_video_note(user_id, content['file_id'], reply_to_message_id=reply_to_message_id)

            if sent_to_author:
                messages_storage[current_post_num]['author_message_id'] = sent_to_author.message_id
                # ИСПРАВЛЕНИЕ: Мы работаем со словарем для конкретной доски
                board_post_to_messages.setdefault(current_post_num, {})[user_id] = sent_to_author.message_id
                board_message_to_post[(user_id, sent_to_author.message_id)] = current_post_num
        except Exception:
             pass

        if recipients:
            # ИСПРАВЛЕНИЕ: Убираем board_id из payload, он больше не нужен в очереди
            queue_payload = {
                'recipients': recipients,
                'content': content,
                'post_num': current_post_num,
                'reply_info': reply_info
            }
            await message_queues[board_id].put(queue_payload)
    except Exception as e:
        import traceback
        print(f"Критическая ошибка в handle_message для доски [{board_id}]: {e}")
        traceback.print_exc()

async def save_state_and_backup_periodically():
    while True:
        await asyncio.sleep(3600) # Каждый час
        print("💾 Запуск периодического сохранения...")
        await save_state_and_backup()
        
async def start_background_tasks():
    """Поднимаем все фоновые корутины ОДИН раз за весь runtime."""
    
    def get_and_increment_counter():
        global global_post_counter
        global_post_counter += 1
        return global_post_counter

    tasks = [
        asyncio.create_task(save_state_and_backup_periodically()),
        asyncio.create_task(message_broadcaster()),
        asyncio.create_task(conan_roaster(
            format_header, boards_state, messages_storage,
            message_to_post, post_to_messages, message_queues
        )),
        asyncio.create_task(help_broadcaster(
            get_and_increment_counter, boards_state,
            BOARD_INFO, message_queues
        )),
        asyncio.create_task(cross_board_stats_broadcaster()),
        # Запускаем нашу новую единую функцию очистки
        asyncio.create_task(auto_memory_cleaner()),
    ]
    print(f"✓ Запущено фоновых задач: {len(tasks)}")
    return tasks
    
async def supervisor():
    # Блокировка от множественного запуска
    lock_file = "bot.lock"
    if os.path.exists(lock_file):
        print("⛔ Bot already running! Exiting...")
        sys.exit(1)

    with open(lock_file, "w") as f:
        f.write(str(os.getpid()))

    try:
        global is_shutting_down, healthcheck_site

        loop = asyncio.get_running_loop()

        # Восстановление бэкапа перед инициализацией
        restore_backup_on_start()

        if hasattr(signal, 'SIGTERM'):
            loop.add_signal_handler(signal.SIGTERM, lambda: asyncio.create_task(graceful_shutdown()))
        if hasattr(signal, 'SIGINT'):
            loop.add_signal_handler(signal.SIGINT, lambda: asyncio.create_task(graceful_shutdown()))

        # --- Инициализация всех досок ---
        initialize_boards_data()
        load_all_states()
        healthcheck_site = await start_healthcheck()

        # --- Создание и запуск всех ботов ---
        polling_tasks = []
        for board_id, token in BOT_TOKENS.items():
            bot_instance = Bot(token=token)
            bots[board_id] = bot_instance
            bot_info = await bot_instance.get_me()
            bot_id_to_board[bot_info.id] = board_id
            polling_tasks.append(dp.start_polling(bot_instance, skip_updates=True))
            print(f"🤖 Бот для доски /{board_id}/ ({bot_info.username}) готов к запуску.")

        if not polling_tasks:
            print("❌ Не найдено ни одного токена бота. Завершение работы.")
            sys.exit(1)

        # Запуск фоновых задач
        background_tasks = await start_background_tasks()
        print("✅ Фоновые задачи запущены")

        # Запускаем polling для всех ботов одновременно
        await asyncio.gather(*polling_tasks, *background_tasks)

    except Exception as e:
        print(f"🔥 Critical error in supervisor: {e}")
    finally:
        if not is_shutting_down:
            await graceful_shutdown()
        if os.path.exists(lock_file):
            os.remove(lock_file)

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(supervisor())
