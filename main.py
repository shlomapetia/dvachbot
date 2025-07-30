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
from aiogram.client.default import DefaultBotProperties
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
from deanonymizer import DEANON_SURNAMES, DEANON_CITIES, DEANON_PROFESSIONS, DEANON_FETISHES, DEANON_DETAILS, generate_deanon_info
from help_text import HELP_TEXT
from ghost_machine import ghost_poster

# ========== Глобальные настройки досок ==========
# Вставляем новую конфигурационную структуру
BOARD_CONFIG = {
    'b': {
        "name": "/b/",
        "description": "БРЕД - основная доска",
        "username": "@dvach_chatbot",
        "token": os.getenv("BOT_TOKEN"),  # Основной бот
        "admins": {int(x) for x in os.getenv("ADMINS", "").split(",") if x}
    },
    'po': {
        "name": "/po/",
        "description": "ПОЛИТАЧ - (срачи, политика)",
        "username": "@dvach_po_chatbot",
        "token": os.getenv("PO_BOT_TOKEN"),
        "admins": {int(x) for x in os.getenv("PO_ADMINS", "").split(",") if x}
    },
    'a': {
        "name": "/a/",
        "description": "АНИМЕ - (манга, Япония, хентай)",
        "username": "@dvach_a_chatbot",
        "token": os.getenv("A_BOT_TOKEN"),
        "admins": {int(x) for x in os.getenv("A_ADMINS", "").split(",") if x}
    },
    'sex': {
        "name": "/sex/",
        "description": "СЕКСАЧ - (отношения, секс, тян, еот, блекпилл)",
        "username": "@dvach_sex_chatbot",
        "token": os.getenv("SEX_BOT_TOKEN"),
        "admins": {int(x) for x in os.getenv("SEX_ADMINS", "").split(",") if x}
    },
    'vg': {
        "name": "/vg/",
        "description": "ВИДЕОИГРЫ - (ПК, игры, хобби)",
        "username": "@dvach_vg_chatbot",
        "token": os.getenv("VG_BOT_TOKEN"),
        "admins": {int(x) for x in os.getenv("VG_ADMINS", "").split(",") if x}
    }
}

# Извлекаем список ID досок для удобства
BOARDS = list(BOARD_CONFIG.keys())

# Очереди сообщений для каждой доски
message_queues = {board: asyncio.Queue(maxsize=9000) for board in BOARDS}

# ========== Глобальные переменные и настройки ==========
is_shutting_down = False
git_executor = ThreadPoolExecutor(max_workers=1)
send_executor = ThreadPoolExecutor(max_workers=100)
save_executor = ThreadPoolExecutor(max_workers=os.cpu_count() or 1) # Executor для сохранения файлов
git_semaphore = asyncio.Semaphore(1)
post_counter_lock = asyncio.Lock()


# ВВОДИМ НОВУЮ СТРУКТУРУ ДЛЯ ДАННЫХ КАЖДОЙ ДОСКИ
board_data = defaultdict(lambda: {
    # --- Режимы ---
    'anime_mode': False,
    'zaputin_mode': False,
    'slavaukraine_mode': False,
    'suka_blyat_mode': False,
    'last_suka_blyat': None,
    'suka_blyat_counter': 0,
    'last_mode_activation': None,
    # --- Данные пользователей для спам-фильтров ---
    'last_texts': defaultdict(lambda: deque(maxlen=5)),
    'last_stickers': defaultdict(lambda: deque(maxlen=5)),
    'last_animations': defaultdict(lambda: deque(maxlen=5)),
    'spam_violations': defaultdict(dict),
    'spam_tracker': defaultdict(list),
    # --- Муты и баны ---
    'mutes': {},
    'shadow_mutes': {},
    # --- Пользовательские данные ---
    'users': {
        'active': set(),
        'banned': set()
    },
    'message_counter': defaultdict(int),
    # --- Кэш последних сообщений (для анти-спама) ---
    'last_user_msgs': {},
})

# ========== ОБЩИЕ ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ (остаются без изменений) ==========
MODE_COOLDOWN = 3600  # 1 час в секундах
MAX_ACTIVE_USERS_IN_MEMORY = 5000 # Лимит на юзера в памяти для get_user_msgs_deque

# --- ОБЩЕЕ ХРАНИЛИЩЕ ПОСТОВ (сквозная нумерация) ---
state = {
    'post_counter': 0,
    # 'message_counter', 'users_data' и 'settings' теперь будут управляться внутри board_data
    # и загружаться/сохраняться для каждой доски отдельно.
    # Но для обратной совместимости при первом запуске оставим post_counter здесь.
}
messages_storage = {}
post_to_messages = {}
message_to_post = {}
last_messages = deque(maxlen=500) # Используется для генерации сообщений, можно оставить общим
last_activity_time = datetime.now()
daily_log = io.StringIO()
sent_media_groups = deque(maxlen=1000)
current_media_groups = {}
media_group_timers = {}

# Отключаем стандартную обработку сигналов в aiogram
os.environ["AIORGRAM_DISABLE_SIGNAL_HANDLERS"] = "1"


SPAM_RULES = {
    'text': {
        'max_repeats': 5,  # Макс одинаковых текстов подряд
        'min_length': 2,  # Минимальная длина текста
        'window_sec': 15,  # Окно для проверки (сек)
        'max_per_window': 7,  # Макс сообщений в окне
        'penalty': [60, 300, 600]  # Шкала наказаний: [1 мин, 5мин, 10 мин]
    },
    'sticker': {
        'max_per_window': 6,  # 6 стикеров за 18 сек
        'window_sec': 18,
        'penalty': [60, 600, 900]  # 1мин, 10мин, 15 мин
    },
    'animation': {  # Гифки
        'max_per_window': 5,  # 5 гифки за 30 сек
        'window_sec': 20,
        'penalty': [60, 600, 900]  # 1мин, 10мин, 15 мин
    }
}


# Хранит информацию о текущих медиа-группах: media_group_id -> данные
current_media_groups = {}
media_group_timers = {}

def suka_blyatify_text(text: str, board_id: str) -> str:
    """
    Применяет к тексту преобразования режима "сука блять".
    Эта версия адаптирована для работы с данными конкретной доски.
    """
    if not text:
        return text

    # Получаем срез данных для конкретной доски
    b_data = board_data[board_id]
    words = text.split()

    for i in range(len(words)):
        if random.random() < 0.3:
            words[i] = random.choice(MAT_WORDS)

    # Используем счетчик, привязанный к доске
    b_data['suka_blyat_counter'] += 1
    if b_data['suka_blyat_counter'] % 3 == 0:
        words.append("... СУКА БЛЯТЬ!")
    
    return ' '.join(words)

def restore_backup_on_start():
    """Забирает все файлы *_state.json и *_reply_cache.json из backup-репозитория при запуске"""
    repo_url = "https://github.com/shlomapetia/dvachbot-backup.git"
    backup_dir = "/tmp/backup"
    try:
        if os.path.exists(backup_dir):
            shutil.rmtree(backup_dir)
        subprocess.run(["git", "clone", repo_url, backup_dir], check=True)

        # Ищем все файлы нужного формата
        backup_files = glob.glob(os.path.join(backup_dir, "*_state.json"))
        backup_files += glob.glob(os.path.join(backup_dir, "*_reply_cache.json"))

        if not backup_files:
            print("Файлы для восстановления в backup-репозитории не найдены.")
            return

        for src_path in backup_files:
            fname = os.path.basename(src_path)
            dst_path = os.path.join(os.getcwd(), fname)
            shutil.copy2(src_path, dst_path)
            print(f"Восстановлен {fname} из backup-репозитория")

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
    """Синхронные Git-операции для бэкапа с жесткими таймаутами и подробным логированием."""
    GIT_TIMEOUT = 20  # Секунд на каждую сетевую git-операцию
    try:
        work_dir = "/tmp/git_backup"
        os.makedirs(work_dir, exist_ok=True)
        repo_url = f"https://{token}@github.com/shlomapetia/dvachbot-backup.git"

        # --- Клонирование или Обновление ---
        if not os.path.exists(os.path.join(work_dir, ".git")):
            clone_cmd = ["git", "clone", "--depth=1", repo_url, work_dir]
            print(f"Git: Выполняю: {' '.join(clone_cmd)}")
            result = subprocess.run(clone_cmd, capture_output=True, text=True, timeout=GIT_TIMEOUT)
            if result.returncode != 0:
                print(f"❌ Ошибка клонирования (код {result.returncode}):\n--- stderr ---\n{result.stderr}\n--- stdout ---\n{result.stdout}")
                return False
            print("✅ Git: Репозиторий успешно клонирован.")
        else:
            pull_cmd = ["git", "-C", work_dir, "pull"]
            print(f"Git: Выполняю: {' '.join(pull_cmd)}")
            result = subprocess.run(pull_cmd, capture_output=True, text=True, timeout=GIT_TIMEOUT)
            if result.returncode != 0:
                print(f"⚠️ Ошибка обновления (код {result.returncode}):\n--- stderr ---\n{result.stderr}\n--- stdout ---\n{result.stdout}")
                # Не критично, продолжаем, но это плохой знак

        # --- Копирование файлов ---
        files_to_copy = glob.glob(os.path.join(os.getcwd(), "*_state.json"))
        files_to_copy += glob.glob(os.path.join(os.getcwd(), "*_reply_cache.json"))
        
        if not files_to_copy:
            print("⚠️ Нет файлов для бэкапа, пропуск.")
            return True # Успешное завершение, так как нет работы

        for src_path in files_to_copy:
            shutil.copy2(src_path, work_dir)

        # --- Локальные Git операции (быстрые, короткий таймаут) ---
        subprocess.run(["git", "-C", work_dir, "config", "user.name", "Backup Bot"], check=True, timeout=5)
        subprocess.run(["git", "-C", work_dir, "config", "user.email", "backup@dvachbot.com"], check=True, timeout=5)
        subprocess.run(["git", "-C", work_dir, "add", "."], check=True, timeout=5)
        
        # Проверяем, есть ли что коммитить
        status_result = subprocess.run(["git", "-C", work_dir, "status", "--porcelain"], capture_output=True, text=True, timeout=5)
        if not status_result.stdout:
            print("✅ Git: Нет изменений для коммита.")
            return True

        commit_msg = f"Backup: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}"
        subprocess.run(["git", "-C", work_dir, "commit", "-m", commit_msg], check=True, timeout=5)

        # --- Push - самая важная операция ---
        push_cmd = ["git", "-C", work_dir, "push", "origin", "main"]
        print(f"Git: Выполняю: {' '.join(push_cmd)}")
        result = subprocess.run(push_cmd, capture_output=True, text=True, timeout=GIT_TIMEOUT)

        if result.returncode == 0:
            print(f"✅ Бекап успешно отправлен в GitHub.\n--- stdout ---\n{result.stdout}")
            return True
        else:
            print(f"❌ КРИТИЧЕСКАЯ ОШИБКА PUSH (код {result.returncode}):\n--- stderr ---\n{result.stderr}\n--- stdout ---\n{result.stdout}")
            return False

    except subprocess.TimeoutExpired as e:
        print(f"⛔ КРИТИЧЕСКАЯ ОШИБКА: Таймаут операции git! Команда '{' '.join(e.cmd)}' не завершилась за {e.timeout} секунд.")
        print(f"--- stderr ---\n{e.stderr or '(пусто)'}\n--- stdout ---\n{e.stdout or '(пусто)'}")
        return False
    except Exception as e:
        print(f"⛔ КРИТИЧЕСКАЯ ОШИБКА в sync_git_operations: {e}")
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


async def auto_backup():
    """Автоматическое сохранение данных ВСЕХ досок и бэкап каждые 1 ч"""
    while True:
        try:
            await asyncio.sleep(900)  # 15 м

            if is_shutting_down:
                break
            
            # Новая функция сохраняет всё и делает бэкап
            await save_all_boards_and_backup()

        except Exception as e:
            print(f"❌ Ошибка в auto_backup: {e}")
            await asyncio.sleep(60)
            
# Настройка сборщика мусора
gc.set_threshold(
    700, 10, 10)  # Оптимальные настройки для баланса памяти/производительности


def get_user_msgs_deque(user_id: int, board_id: str):
    """Получаем deque для юзера на конкретной доске, ограничиваем количество юзеров в памяти"""
    last_user_msgs_for_board = board_data[board_id]['last_user_msgs']
    
    if user_id not in last_user_msgs_for_board:
        if len(last_user_msgs_for_board) >= MAX_ACTIVE_USERS_IN_MEMORY:
            oldest_user = next(iter(last_user_msgs_for_board))
            del last_user_msgs_for_board[oldest_user]

        last_user_msgs_for_board[user_id] = deque(maxlen=10)

    return last_user_msgs_for_board[user_id]

# Конфиг
BOT_TOKEN = os.environ.get('BOT_TOKEN')
ADMINS = {int(x) for x in os.getenv("ADMINS", "").split(",") if x}
SPAM_LIMIT = 14
SPAM_WINDOW = 15
STATE_FILE = 'state.json'
SAVE_INTERVAL = 900  # секунд
STICKER_WINDOW = 10  # секунд
STICKER_LIMIT = 7
REST_SECONDS = 30  # время блокировки
REPLY_CACHE = 500  # сколько постов держать
REPLY_FILE = "reply_cache.json"  # отдельный файл для reply
# В начале файла с константами
MAX_MESSAGES_IN_MEMORY = 600  # храним только последние 600 постов


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
    "Анонимность - это не только анонимность. Это и мужской эротический флирт.",
    "Абу сосет хуй. Зови друзей",
    "Тгач - это не только чат. Это аноны",
    "Возрождаем сосач. Аноны, зовите друзей",
    "Добро пожаловать. Снова",
    "Привет, анон. Ты не один. Зови друзей",
    "Да ты заебал, приглашай анонов",
    "Пора бы пропиарить тгач. Эй уёбок, разошли в свои конфы",
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
MAT_WORDS = ["сука", "блядь", "пиздец", "ебать", "нах", "пизда", "хуйня", "ебал", "блять", "отъебись", "ебаный", "еблан", "ХУЙ", "ПИЗДА", "хуйло", "долбаёб", "пидорас"]

# Временная зона МСК
MSK = timezone(timedelta(hours=3))

# ─── Глобальный error-handler ──────────────────────────────────

@dp.errors()
async def global_error_handler(event: types.ErrorEvent) -> bool:
    """Улучшенный обработчик ошибок для aiogram (адаптирован для досок)."""
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

    # Обработка TelegramForbiddenError (пользователь заблокировал бота)
    if isinstance(exception, TelegramForbiddenError):
        user_id = None
        telegram_object = None

        if update and update.message:
            user_id = update.message.from_user.id
            telegram_object = update.message
        elif update and update.callback_query:
            user_id = update.callback_query.from_user.id
            telegram_object = update.callback_query

        if user_id and telegram_object:
            # Определяем, какого именно бота заблокировали
            board_id = get_board_id(telegram_object)
            if board_id:
                # Удаляем пользователя из активных на конкретной доске
                board_data[board_id]['users']['active'].discard(user_id)
                print(f"🚫 [{board_id}] Пользователь {user_id} заблокировал бота, удален из активных.")
        return True

    # Логирование остальных ошибок
    error_msg = f"⚠️ Ошибка: {type(exception).__name__}"
    if str(exception):
        error_msg += f": {exception}"
    print(error_msg)

    # Обработка сетевых ошибок и конфликтов
    if isinstance(exception, (TelegramNetworkError, TelegramConflictError, aiohttp.ClientError)):
        print(f"🌐 Сетевая ошибка: {exception}")
        await asyncio.sleep(10)
        return False

    # Обработка KeyError (проблемы с хранилищем)
    elif isinstance(exception, KeyError):
        print(f"🔑 KeyError: {exception}. Пропускаем обработку этого сообщения.")
        return True

    # Все остальные ошибки
    else:
        print(f"⛔ Непредвиденная ошибка: {exception}")
        if update:
            try:
                print(f"Update: {update.model_dump_json(exclude_none=True, indent=2)}")
            except Exception as json_e:
                print(f"Не удалось сериализовать update: {json_e}")
        await asyncio.sleep(10)
        return False
        
def escape_html(text: str) -> str:
    """Экранирует HTML символы"""
    if not text:
        return text
    return text.replace('&', '&amp;').replace('<', '&lt;').replace(
        '>', '&gt;').replace('"', '&quot;')


def is_admin(uid: int, board_id: str) -> bool:
    """Проверяет, является ли пользователь админом на КОНКРЕТНОЙ доске."""
    if not board_id:
        return False
    return uid in BOARD_CONFIG.get(board_id, {}).get('admins', set())
    
def _sync_save_board_state(board_id: str):
    """Синхронная, блокирующая функция для сохранения state.json."""
    state_file = f"{board_id}_state.json"
    b_data = board_data[board_id]
    
    try:
        post_counter_to_save = state['post_counter'] if board_id == 'b' else None
        
        # --- ИЗМЕНЕНО: Логика подсчета постов ---
        # Теперь мы не пересчитываем посты, а берем актуальное значение из памяти,
        # которое инкрементируется в format_header.
        board_post_count = b_data.get('board_post_count', 0)
        
        data_to_save = {
            'users_data': {
                'active': list(b_data['users']['active']),
                'banned': list(b_data['users']['banned']),
            },
            'message_counter': dict(b_data['message_counter']),
            'board_post_count': board_post_count, # Записываем актуальное значение из памяти
        }
        if post_counter_to_save is not None:
            # Сохраняем 'post_counter' для 'b' для ясности и обратной совместимости
            data_to_save['post_counter'] = post_counter_to_save

        with open(state_file, 'w', encoding='utf-8') as f:
            json.dump(data_to_save, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        print(f"⛔ [{board_id}] Ошибка в потоке сохранения state: {e}")
        return False

async def save_board_state(board_id: str):
    """Асинхронная обертка для неблокирующего сохранения state.json."""
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(
        save_executor,
        _sync_save_board_state,
        board_id
    )

async def save_all_boards_and_backup():
    """Сохраняет данные ВСЕХ досок параллельно и делает один общий бэкап в Git."""
    print("💾 Запуск параллельного сохранения и бэкапа...")

    # 1. Создаем задачи для параллельного сохранения всех файлов
    save_tasks = []
    for board_id in BOARDS:
        save_tasks.append(save_board_state(board_id))
        save_tasks.append(save_reply_cache(board_id))
    
    # 2. Запускаем все задачи сохранения одновременно и ждем их завершения
    await asyncio.gather(*save_tasks)
    
    print("💾 Все файлы состояний обновлены, пушим в GitHub...")
    success = await git_commit_and_push()
    if success:
        print("✅ Бэкап всех досок успешно отправлен в GitHub.")
    else:
        print("❌ Не удалось отправить бэкап в GitHub.")
    return success

def _sync_save_reply_cache(board_id: str):
    """Синхронная, блокирующая функция для сохранения кэша. Выполняется в отдельном потоке."""
    reply_file = f"{board_id}_reply_cache.json"
    try:
        # 1. Определяем посты, принадлежащие ТОЛЬКО этой доске
        board_post_keys = {
            p_num for p_num, data in messages_storage.items() 
            if data.get("board_id") == board_id
        }
        
        # 2. Ограничиваем количество постов для сохранения (медленная операция)
        recent_board_posts = sorted(list(board_post_keys))[-REPLY_CACHE:]
        recent_posts_set = set(recent_board_posts)

        if not recent_posts_set:
            if os.path.exists(reply_file):
                os.remove(reply_file)
            return True

        # 3. Собираем данные для сохранения
        new_data = {
            "post_to_messages": {
                str(p_num): data
                for p_num, data in post_to_messages.items()
                if p_num in recent_posts_set
            },
            "message_to_post": {
                f"{uid}_{mid}": p_num
                for (uid, mid), p_num in message_to_post.items()
                if p_num in recent_posts_set
            },
            "messages_storage_meta": {
                str(p_num): {
                    "author_id": messages_storage[p_num].get("author_id", ""),
                    "timestamp": messages_storage[p_num].get("timestamp", datetime.now(UTC)).isoformat(),
                    "author_message_id": messages_storage[p_num].get("author_message_id"),
                    "board_id": board_id
                }
                for p_num in recent_board_posts
                if p_num in messages_storage
            }
        }

        # 4. Сохраняем новые данные (блокирующая операция I/O)
        with open(reply_file, 'w', encoding='utf-8') as f:
            json.dump(new_data, f, ensure_ascii=False, indent=2)

        return True

    except Exception as e:
        print(f"⛔ [{board_id}] Ошибка в потоке сохранения reply_cache: {str(e)[:200]}")
        return False

async def save_reply_cache(board_id: str):
    """Асинхронная обертка для неблокирующего сохранения кэша ответов."""
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(
        save_executor, 
        _sync_save_reply_cache, 
        board_id
    )

def load_state():
    """Загружает состояния для ВСЕХ досок в board_data."""
    global state # Только для post_counter

    # Загружаем общий счетчик постов из файла основного бота 'b'
    # Это обеспечивает сквозную нумерацию
    state_file_b = 'b_state.json'
    if os.path.exists(state_file_b):
        try:
            with open(state_file_b, 'r', encoding='utf-8') as f:
                data = json.load(f)
                state['post_counter'] = data.get('post_counter', 0)
                print(f"Общий счетчик постов загружен: {state['post_counter']}")
        except (json.JSONDecodeError, OSError):
             print(f"Не удалось загрузить общий счетчик постов из {state_file_b}.")

    # Загружаем данные для каждой доски
    for board_id in BOARDS:
        state_file = f"{board_id}_state.json"
        if not os.path.exists(state_file):
            print(f"Файл состояния для доски '{board_id}' не найден, пропуск.")
            continue

        try:
            with open(state_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Загружаем данные в board_data[board_id]
            b_data = board_data[board_id]
            b_data['users']['active'] = set(data.get('users_data', {}).get('active', []))
            b_data['users']['banned'] = set(data.get('users_data', {}).get('banned', []))
            b_data['message_counter'].update(data.get('message_counter', {}))
            
            # --- КЛЮЧЕВОЕ ИЗМЕНЕНИЕ ---
            loaded_post_count = data.get('board_post_count', 0)
            
            # Устанавливаем начальное значение для 'b', только если счетчик пуст.
            # Это предотвращает потерю новых постов при перезапуске и
            # гарантирует, что "добавление" произойдет только один раз.
            if board_id == 'b' and loaded_post_count == 0:
                b_data['board_post_count'] = 37004
            else:
                b_data['board_post_count'] = loaded_post_count

            print(f"[{board_id}] Состояние загружено: "
                  f"активных = {len(b_data['users']['active'])}, "
                  f"забаненных = {len(b_data['users']['banned'])}, "
                  f"постов = {b_data['board_post_count']}") # <-- Теперь показывает актуальное значение

            # Загружаем кэш ответов для этой доски
            load_reply_cache(board_id)

        except (json.JSONDecodeError, OSError) as e:
            print(f"Ошибка загрузки состояния для доски '{board_id}': {e}")
            
def load_archived_post(post_num):
    """Ищем пост в архивах"""
    for archive_file in glob.glob("archive_*.pkl.gz"):
        with gzip.open(archive_file, "rb") as f:
            data = pickle.load(f)
            if post_num in data:
                return data[post_num]
    return None

def load_reply_cache(board_id: str):
    """Читаем reply_cache для конкретной доски, восстанавливаем общие словари."""
    global message_to_post, post_to_messages, messages_storage
    
    reply_file = f"{board_id}_reply_cache.json"
    if not os.path.exists(reply_file) or os.path.getsize(reply_file) == 0:
        return

    try:
        with open(reply_file, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        print(f"Файл {reply_file} повреждён ({e}), игнорирую")
        return

    # Восстанавливаем общие словари, они пополняются данными со всех досок
    for key, post_num in data.get("message_to_post", {}).items():
        uid, mid = map(int, key.split("_"))
        message_to_post[(uid, mid)] = post_num

    for p_str, mapping in data.get("post_to_messages", {}).items():
        post_to_messages[int(p_str)] = {
            int(uid): mid
            for uid, mid in mapping.items()
        }

    for p_str, meta in data.get("messages_storage_meta", {}).items():
        p = int(p_str)
        if 'timestamp' in meta:
            dt = datetime.fromisoformat(meta['timestamp'])
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            messages_storage[p] = {
                "author_id": meta["author_id"],
                "timestamp": dt,
                "author_message_id": meta.get("author_msg"),
                "board_id": board_id # Важно: сохраняем, с какой доски пришел пост
            }
            
    print(f"[{board_id}] reply-cache загружен: {len(data.get('post_to_messages', {}))} постов")


async def graceful_shutdown(bots: list[Bot]):
    """Обработчик корректного сохранения данных ВСЕХ досок перед остановкой."""
    global is_shutting_down
    if is_shutting_down:
        return

    is_shutting_down = True
    print("🛑 Получен сигнал shutdown, начинаем процедуру завершения...")

    # 1. Остановить polling, чтобы не принимались новые сообщения от всех ботов
    try:
        await dp.stop_polling()
        print("⏸ Polling для всех ботов остановлен.")
    except Exception as e:
        print(f"⚠️ Не удалось остановить polling: {e}")

    # 2. Ждать пока все очереди сообщений опустеют (макс 10 сек)
    print("Ожидание опустошения очередей...")
    all_queues_empty = False
    for _ in range(10):
        if all(q.empty() for q in message_queues.values()):
            all_queues_empty = True
            break
        await asyncio.sleep(1)
    
    if all_queues_empty:
        print("✅ Все очереди сообщений обработаны.")
    else:
        print("⚠️ Таймаут ожидания очередей. Некоторые сообщения могли не отправиться.")

    # 3. Сохраняем и пушим данные. САМЫЙ ВАЖНЫЙ ЭТАП.
    # Принудительно ограничиваем время на бэкап, чтобы успеть до SIGKILL от хостинга.
    # --- НАЧАЛО ИЗМЕНЕНИЙ ---
    print("💾 Попытка финального сохранения и бэкапа в GitHub (таймаут 50 секунд)...")
    try:
        await asyncio.wait_for(save_all_boards_and_backup(), timeout=50.0)
        print("✅ Финальный бэкап успешно завершен в рамках таймаута.")
    except asyncio.TimeoutError:
        print("⛔ КРИТИЧЕСКАЯ ОШИБКА: Финальный бэкап не успел выполниться за 50 секунд и был прерван!")
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---
    except Exception as e:
        print(f"⛔ КРИТИЧЕСКАЯ ОШИБКА: Не удалось выполнить финальный бэкап: {e}")


    # 4. Останавливаем всё остальное, несмотря на результат бэкапа
    print("Завершение остальных компонентов...")
    try:
        if 'healthcheck_site' in globals() and globals()['healthcheck_site']:
            await globals()['healthcheck_site'].stop()
            print("🛑 Healthcheck server stopped")

        # Отправляем сигнал на завершение пулов потоков, не дожидаясь их.
        # Если бэкап был прерван по таймауту, поток git все еще может выполняться.
        # wait=True здесь привел бы к зависанию.
        git_executor.shutdown(wait=False, cancel_futures=True)
        send_executor.shutdown(wait=False, cancel_futures=True)
        print("🛑 Executors shutdown initiated.")

        if hasattr(dp, 'storage') and dp.storage:
            await dp.storage.close()
        
        # Закрываем сессию каждого бота
        for bot_instance in bots:
            if bot_instance.session:
                await bot_instance.session.close()
        print("✅ Все сессии ботов закрыты.")

    except Exception as e:
        print(f"Error during final shutdown procedures: {e}")

    # Отменяем оставшиеся задачи
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
    
    await asyncio.gather(*tasks, return_exceptions=True)
    print("✅ Все задачи остановлены, завершаем работу.")
    
async def auto_memory_cleaner():
    """Единственная унифицированная функция очистки памяти, запускается каждые 10 минут."""
    while True:
        await asyncio.sleep(600)  # 10 минут

        # 1. Очистка старых постов (ОБЩАЯ ЛОГИКА)
        if len(messages_storage) > MAX_MESSAGES_IN_MEMORY:
            to_delete_count = len(messages_storage) - MAX_MESSAGES_IN_MEMORY
            oldest_post_keys = sorted(messages_storage.keys())[:to_delete_count]
            posts_to_delete_set = set(oldest_post_keys)

            for post_num in oldest_post_keys:
                messages_storage.pop(post_num, None)
                post_to_messages.pop(post_num, None)

            # --- НАЧАЛО ИЗМЕНЕНИЙ: Эффективное удаление ---
            # Вместо полного пересоздания словаря, итеративно удаляем устаревшие ключи.
            # Это значительно эффективнее по памяти и CPU при больших размерах словаря.
            keys_to_delete_from_m2p = [
                key for key, post_num in message_to_post.items()
                if post_num in posts_to_delete_set
            ]
            for key in keys_to_delete_from_m2p:
                message_to_post.pop(key, None)
            # --- КОНЕЦ ИЗМЕНЕНИЙ ---
            
            print(f"🧹 Очистка памяти: удалено {len(oldest_post_keys)} старых постов.")

        # 2. Очистка данных для КАЖДОЙ ДОСКИ
        now_utc = datetime.now(UTC)
        for board_id in BOARDS:
            b_data = board_data[board_id]

            # 2.1. Чистим счетчик сообщений доски - оставляем топ 100
            if len(b_data['message_counter']) > 100:
                top_users = sorted(b_data['message_counter'].items(),
                                   key=lambda x: x[1],
                                   reverse=True)[:100]
                b_data['message_counter'] = defaultdict(int, top_users)
                print(f"🧹 [{board_id}] Очистка счетчика сообщений.")


            # 2.2. Чистим spam_tracker доски от старых записей
            spam_tracker_board = b_data['spam_tracker']
            for user_id in list(spam_tracker_board.keys()):
                # Окно для спам-трекера берем из общей конфигурации
                window_sec = SPAM_RULES.get('text', {}).get('window_sec', 15)
                window_start = now_utc - timedelta(seconds=window_sec)
                
                spam_tracker_board[user_id] = [
                    t for t in spam_tracker_board[user_id]
                    if t > window_start
                ]
                if not spam_tracker_board[user_id]:
                    del spam_tracker_board[user_id]
            
            # --- НАЧАЛО ИЗМЕНЕНИЙ ---
            # 2.3. Чистим spam_violations от давно неактивных пользователей
            inactive_threshold = now_utc - timedelta(hours=24) # Порог неактивности - 24 часа
            spam_violations_board = b_data['spam_violations']
            
            # Собираем ID пользователей для удаления, чтобы не изменять словарь во время итерации
            users_to_purge = [
                user_id for user_id, data in spam_violations_board.items()
                if data.get('last_reset', now_utc) < inactive_threshold
            ]
            
            if users_to_purge:
                for user_id in users_to_purge:
                    spam_violations_board.pop(user_id, None)
                print(f"🧹 [{board_id}] Очистка spam_violations: удалено {len(users_to_purge)} записей старых пользователей.")
            # --- КОНЕЦ ИЗМЕНЕНИЙ ---

        # 3. Агрессивная сборка мусора (ОБЩАЯ)
        gc.collect()

async def board_statistics_broadcaster():
    """Раз в час собирает общую статистику и рассылает на каждую доску."""
    await asyncio.sleep(300)  # Начальная задержка 5 минут

    while True:
        try:
            await asyncio.sleep(3600)  # Выполняется раз в час

            # --- НАЧАЛО ИЗМЕНЕНИЙ ---
            # 1. Сбор статистики по постам за последний час
            now = datetime.now(UTC)
            hour_ago = now - timedelta(hours=1)
            
            # Структура для хранения почасовой статистики
            posts_per_hour = defaultdict(int)

            for post_data in messages_storage.values():
                b_id = post_data.get('board_id')
                if b_id and post_data.get('timestamp', now) > hour_ago:
                    posts_per_hour[b_id] += 1
            
            # 2. Формирование единого текста сообщения
            stats_lines = []
            for b_id, config in BOARD_CONFIG.items():
                # Берем почасовую статистику из посчитанного
                hour_stat = posts_per_hour[b_id]
                # Берем ОБЩУЮ статистику из надежного счетчика доски
                total_stat = board_data[b_id].get('board_post_count', 0)
                
                stats_lines.append(
                    f"<b>{config['name']}</b> - {hour_stat} пст/час, всего: {total_stat}"
                )
            full_stats_text = "📊 Статистика досок:\n" + "\n".join(stats_lines)
            # --- КОНЕЦ ИЗМЕНЕНИЙ ---

            # 3. Рассылка готового сообщения по очередям всех досок
            header = "### Статистика ###"
            for board_id in BOARDS:
                b_data = board_data[board_id]
                recipients = b_data['users']['active'] - b_data['users']['banned']

                if not recipients:
                    continue

                _, post_num = await format_header(board_id)
                content = {
                    "type": "text",
                    "header": header,
                    "text": full_stats_text,
                    "is_system_message": True
                }
                
                # --- НАЧАЛО ИЗМЕНЕНИЙ: Сохранение поста в хранилище ---
                # Важно: системное сообщение тоже должно быть в базе
                messages_storage[post_num] = {
                    'author_id': 0,
                    'timestamp': now,
                    'content': content,
                    'board_id': board_id
                }
                # --- КОНЕЦ ИЗМЕНЕНИЙ ---
                
                # Постановка в очередь
                await message_queues[board_id].put({
                    "recipients": recipients,
                    "content": content,
                    "post_num": post_num,
                    "board_id": board_id
                })
                
                print(f"✅ [{board_id}] Статистика досок #{post_num} добавлена в очередь.")

        except Exception as e:
            print(f"❌ Ошибка в board_statistics_broadcaster: {e}")
            await asyncio.sleep(120)
    
async def setup_pinned_messages(bots: dict[str, Bot]):
    """Устанавливает или обновляет закрепленное сообщение для каждого бота."""
    
    board_links = "\n".join(
        f"<b>{config['name']}</b> {config['description']} - {config['username']}"
        for config in BOARD_CONFIG.values()
    )

    help_with_boards = (
        f"{HELP_TEXT}\n\n"
        f"🌐 <b>Все доски:</b>\n{board_links}"
    )
    
    for board_id, bot_instance in bots.items():
        # Получаем список активных пользователей для этой доски
        b_data = board_data[board_id]
        # Мы не можем просто закрепить сообщение в личке,
        # поэтому мы будем отправлять его каждому активному пользователю
        # при старте и при команде /start
        
        # Вместо настоящего "закрепа", мы сохраним этот текст
        # для использования в команде /start
        b_data['start_message_text'] = help_with_boards
        
        print(f"📌 [{board_id}] Текст для команды /start и закрепа подготовлен.")

async def check_spam(user_id: int, msg: Message, board_id: str) -> bool:
    """Проверяет спам с прогрессивным наказанием и сбросом уровня (с поддержкой досок)"""
    b_data = board_data[board_id]

    # Определяем тип контента
    if msg.content_type == 'text':
        msg_type = 'text'
        content = msg.text
    elif msg.content_type == 'sticker':
        msg_type = 'sticker'
        content = None
    elif msg.content_type == 'animation':
        msg_type = 'animation'
        content = None
    elif msg.content_type in ['photo', 'video', 'document'] and msg.caption:
        msg_type = 'text'
        content = msg.caption
    else:
        return True

    rules = SPAM_RULES.get(msg_type)
    if not rules:
        return True

    now = datetime.now(UTC)
    # Инициализация данных пользователя в контексте доски
    if user_id not in b_data['spam_violations'] or not b_data['spam_violations'][user_id]:
        b_data['spam_violations'][user_id] = {
            'level': 0,
            'last_reset': now,
            'last_contents': deque(maxlen=4)
        }
    # Сброс уровня, если прошло больше 1 часа
    if (now - b_data['spam_violations'][user_id]['last_reset']) > timedelta(hours=1):
        b_data['spam_violations'][user_id] = {
            'level': 0,
            'last_reset': now,
            'last_contents': deque(maxlen=4)
        }
    # Проверка повторяющихся текстов/подписей
    if msg_type == 'text' and content:
        b_data['spam_violations'][user_id]['last_contents'].append(content)
        # 3 одинаковых подряд
        if len(b_data['spam_violations'][user_id]['last_contents']) >= 4:
            # --- ИЗМЕНЕНИЕ ЛОГИКИ: Проверяем последние 3 элемента ---
            last_three = list(b_data['spam_violations'][user_id]['last_contents'])[-4:]
            if len(set(last_three)) == 1:
                b_data['spam_violations'][user_id]['level'] = min(
                    b_data['spam_violations'][user_id]['level'] + 1,
                    len(rules['penalty']) - 1)
                return False
        # Чередование двух текстов
        if len(b_data['spam_violations'][user_id]['last_contents']) == 4:
            unique = set(b_data['spam_violations'][user_id]['last_contents'])
            if len(unique) == 2:
                contents = list(b_data['spam_violations'][user_id]['last_contents'])
                p1 = [contents[0], contents[1]] * 2
                p2 = [contents[1], contents[0]] * 2
                if contents == p1 or contents == p2:
                    b_data['spam_violations'][user_id]['level'] = min(
                        b_data['spam_violations'][user_id]['level'] + 1,
                        len(rules['penalty']) - 1)
                    return False

    # Проверка на повтор стикеров
    elif msg_type == 'sticker':
        last_items = b_data['last_stickers'][user_id]
        last_items.append(msg.sticker.file_id)
        # --- НАЧАЛО ИЗМЕНЕНИЙ ---
        # 3 одинаковых подряд
        if len(last_items) >= 4:
            # Проверяем только последние 3 элемента
            last_three = list(last_items)[-4:]
            if len(set(last_three)) == 1:
                b_data['spam_violations'][user_id]['level'] = min(
                    b_data['spam_violations'][user_id]['level'] + 1,
                    len(rules['penalty']) - 1)
                return False
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---
            
    # Проверка на повтор гифок
    elif msg_type == 'animation':
        last_items = b_data['last_animations'][user_id]
        last_items.append(msg.animation.file_id)
        # --- НАЧАЛО ИЗМЕНЕНИЙ ---
        # 3 одинаковых подряд
        if len(last_items) >= 3:
            # Проверяем только последние 3 элемента
            last_three = list(last_items)[-3:]
            if len(set(last_three)) == 1:
                b_data['spam_violations'][user_id]['level'] = min(
                    b_data['spam_violations'][user_id]['level'] + 1,
                    len(rules['penalty']) - 1)
                return False
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---

    # Проверка лимита по времени
    window_start = now - timedelta(seconds=rules['window_sec'])
    b_data['spam_tracker'][user_id] = [t for t in b_data['spam_tracker'][user_id] if t > window_start]
    b_data['spam_tracker'][user_id].append(now)
    if len(b_data['spam_tracker'][user_id]) >= rules['max_per_window']:
        b_data['spam_violations'][user_id]['level'] = min(
            b_data['spam_violations'][user_id]['level'] + 1,
            len(rules['penalty']) - 1)
        return False
    return True

async def apply_penalty(bot_instance: Bot, user_id: int, msg_type: str, board_id: str):
    """Применяет мут согласно текущему уровню нарушения (с поддержкой досок)"""
    b_data = board_data[board_id]
    rules = SPAM_RULES.get(msg_type, {})
    if not rules:
        return
        
    level = b_data['spam_violations'].get(user_id, {}).get('level', 0)
    level = min(level, len(rules.get('penalty', [])) - 1)
    mute_seconds = rules['penalty'][level] if rules.get('penalty') else 30
    
    # Применяем мут в контексте доски
    b_data['mutes'][user_id] = datetime.now(UTC) + timedelta(seconds=mute_seconds)
    
    # Определяем тип нарушения
    violation_type = {
        'text': "текстовый спам",
        'sticker': "спам стикерами",
        'animation': "спам гифками"
    }.get(msg_type, "спам")
    
    # Уведомление
    try:
        if mute_seconds < 60:
            time_str = f"{mute_seconds} сек"
        elif mute_seconds < 3600:
            time_str = f"{mute_seconds // 60} мин"
        else:
            time_str = f"{mute_seconds // 3600} час"
        
        # Используем переданный экземпляр бота
        await bot_instance.send_message(
            user_id,
            f"🚫 Эй пидор ты в муте на {time_str} за {violation_type} на доске {BOARD_CONFIG[board_id]['name']}\n"
            f"Спамишь дальше - получишь бан",
            parse_mode="HTML")
        
        # Передаем bot_instance в send_moderation_notice (потребует адаптации на след. шагах)
        await send_moderation_notice(user_id, "mute", board_id, duration=time_str)
    except Exception as e:
        print(f"Ошибка отправки уведомления о муте: {e}")

async def format_header(board_id: str) -> Tuple[str, int]:
    """Асинхронное форматирование заголовка с блокировкой для безопасного инкремента счетчика постов."""
    async with post_counter_lock:
        state['post_counter'] += 1
        post_num = state['post_counter']
        
        # --- ИЗМЕНЕНИЕ ЗДЕСЬ ---
        # Инкрементируем счетчик постов для конкретной доски
        # Это обеспечивает актуальность счетчика в памяти в реальном времени
        board_data[board_id].setdefault('board_post_count', 0)
        board_data[board_id]['board_post_count'] += 1
    
    b_data = board_data[board_id]

    # Режим /slavaukraine
    if b_data['slavaukraine_mode']:
        return f"💙💛 Пiст №{post_num}", post_num

    # Режим /zaputin
    if b_data['zaputin_mode']:
        return f"🇷🇺 Пост №{post_num}", post_num

    # Режим /anime
    if b_data['anime_mode']:
        return f"🌸 投稿 {post_num} 番", post_num

    # Режим /suka_blyat
    if b_data['suka_blyat_mode']:
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
        prefix = "АААААААА - "
    elif rand_prefix < 0.044:
        prefix = "### Аниме девочка ### "

    # Формируем итоговый заголовок
    header_text = f"{circle}{prefix}Пост №{post_num}"
    return header_text, post_num

async def delete_user_posts(bot_instance: Bot, user_id: int, time_period_minutes: int, board_id: str) -> int:
    """Удаляет сообщения пользователя за период в пределах КОНКРЕТНОЙ доски, используя нужный экземпляр бота."""
    try:
        time_threshold = datetime.now(UTC) - timedelta(minutes=time_period_minutes)
        posts_to_delete = []
        deleted_messages = 0

        # Итерируемся напрямую по .items(), избегая создания полной копии словаря в памяти.
        for post_num, post_data in messages_storage.items():
            post_time = post_data.get('timestamp')
            if not post_time:
                continue

            if (post_data.get('author_id') == user_id and
                post_data.get('board_id') == board_id and
                post_time >= time_threshold):
                posts_to_delete.append(post_num)
        
        if not posts_to_delete:
            return 0

        # Собираем ВСЕ сообщения для удаления
        messages_to_delete = []
        for post_num in posts_to_delete:
            if post_num in post_to_messages:
                for uid, mid in post_to_messages[post_num].items():
                    messages_to_delete.append((uid, mid))

        # Удаляем каждое сообщение с повторными попытками
        for (uid, mid) in messages_to_delete:
            try:
                await bot_instance.delete_message(uid, mid)
                deleted_messages += 1
            except (TelegramBadRequest, TelegramForbiddenError):
                # Игнорируем ошибки, если сообщение уже удалено или бот заблокирован
                continue
            except Exception as e:
                print(f"Ошибка удаления {mid} у {uid}: {e}")

        # --- НАЧАЛО ИЗМЕНЕНИЙ: Эффективная очистка хранилищ ---
        # 1. Удаляем записи из message_to_post, используя уже собранные ключи (uid, mid).
        #    Это намного эффективнее, чем полный перебор всего словаря.
        for uid, mid in messages_to_delete:
            message_to_post.pop((uid, mid), None)
            
        # 2. Удаляем посты из остальных хранилищ.
        posts_to_delete_set = set(posts_to_delete)
        for post_num in posts_to_delete_set:
            post_to_messages.pop(post_num, None)
            messages_storage.pop(post_num, None)
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---

        return deleted_messages
    except Exception as e:
        print(f"Ошибка в delete_user_posts: {e}")
        return 0
        
async def delete_single_post(post_num: int, bot_instance: Bot) -> int:
    """Удаляет один конкретный пост (и все его копии у пользователей)."""
    if post_num not in post_to_messages:
        return 0

    deleted_count = 0
    # Собираем копии для удаления, messages_to_delete - это список кортежей (uid, mid)
    messages_to_delete = list(post_to_messages.get(post_num, {}).items())

    for uid, mid in messages_to_delete:
        try:
            await bot_instance.delete_message(uid, mid)
            deleted_count += 1
        except (TelegramBadRequest, TelegramForbiddenError):
            continue  # Сообщение уже удалено или бот заблокирован
        except Exception as e:
            print(f"Ошибка при удалении сообщения {mid} у {uid}: {e}")

    # --- НАЧАЛО ИЗМЕНЕНИЙ: Эффективное удаление из message_to_post ---
    # Очистка глобальных хранилищ
    messages_storage.pop(post_num, None)
    post_to_messages.pop(post_num, None)

    # Вместо полного перебора `message_to_post`, мы теперь используем
    # собранный ранее список `messages_to_delete` для точечного удаления.
    # Ключ для `message_to_post` - это кортеж (uid, mid).
    for uid, mid in messages_to_delete:
        message_to_post.pop((uid, mid), None)
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---
    
    return deleted_count
    
async def send_moderation_notice(user_id: int, action: str, board_id: str, duration: str = None, deleted_posts: int = 0):
    """Отправляет уведомление о модерационном действии в чат конкретной доски."""
    b_data = board_data[board_id]
    if not b_data['users']['active']:
        return

    _, post_num = await format_header(board_id)
    header = "### Админ ###"

    if action == "ban":
        text = (f"🚨 Хуесос был забанен за спам. Помянем.")
    elif action == "mute":
        text = (f"🔇 Пидораса замутили ненадолго")
    else:
        return

    content = {
        'type': 'text',
        'header': header,
        'text': text,
        'is_system_message': True
    }

    messages_storage[post_num] = {
        'author_id': 0,
        'timestamp': datetime.now(UTC),
        'content': content,
        'board_id': board_id
    }

    await message_queues[board_id].put({
        "recipients": b_data["users"]["active"],
        "content": content,
        "post_num": post_num,
        "board_id": board_id
    })

async def _apply_mode_transformations(content: dict, board_id: str) -> dict:
    """
    Централизованно применяет все трансформации режимов (аниме, и т.д.)
    к полям 'text' и 'caption' в словаре content.
    """
    b_data = board_data[board_id]
    modified_content = content.copy()

    # --- Применение трансформаций к 'text' ---
    if 'text' in modified_content and modified_content['text']:
        text = modified_content['text']
        if b_data['anime_mode']:
            text = anime_transform(text)
        elif b_data['slavaukraine_mode']:
            text = ukrainian_transform(text)
        elif b_data['zaputin_mode']:
            text = zaputin_transform(text)
        elif b_data['suka_blyat_mode']:
            words = text.split()
            for i in range(len(words)):
                if random.random() < 0.3: words[i] = random.choice(MAT_WORDS)
            text = ' '.join(words)
            # Счетчик и добавочная фраза для suka_blyat обрабатываются в send_message_to_users,
            # так как они должны применяться один раз на всё сообщение.
        modified_content['text'] = text

    # --- Применение трансформаций к 'caption' ---
    if 'caption' in modified_content and modified_content['caption']:
        caption = modified_content['caption']
        if b_data['anime_mode']:
            caption = anime_transform(caption)
        elif b_data['slavaukraine_mode']:
            caption = ukrainian_transform(caption)
        elif b_data['zaputin_mode']:
            caption = zaputin_transform(caption)
        elif b_data['suka_blyat_mode']:
            words = caption.split()
            for i in range(len(words)):
                if random.random() < 0.3: words[i] = random.choice(MAT_WORDS)
            caption = ' '.join(words)
        modified_content['caption'] = caption

    # --- Добавление фраз для режимов ---
    # Эта логика останется в send_message_to_users, так как она добавляет текст
    # ко всему сообщению, а не просто трансформирует поле.
    # Но основные преобразования текста/подписи сделаны здесь.

    return modified_content

async def send_message_to_users(
    bot_instance: Bot,
    recipients: set[int],
    content: dict,
    reply_info: dict | None = None,
) -> list:
    """Оптимизированная рассылка сообщений пользователям (с динамическим вызовом)."""
    if not recipients or not content or 'type' not in content:
        return []

    board_id = next((b_id for b_id, config in BOARD_CONFIG.items() if config['token'] == bot_instance.token), None)
    if not board_id:
        print(f"КРИТИЧЕСКАЯ ОШИБКА: Не удалось найти доску для бота с токеном ...{bot_instance.token[-6:]}")
        return []

    b_data = board_data[board_id]
    modified_content = content.copy()

    # Добавление фраз для режимов (эта логика остается)
    if b_data['suka_blyat_mode']:
        b_data['suka_blyat_counter'] += 1
        if b_data['suka_blyat_counter'] % 3 == 0:
            if 'text' in modified_content and modified_content['text']: modified_content['text'] += " ... СУКА БЛЯТЬ!"
            elif 'caption' in modified_content and modified_content['caption']: modified_content['caption'] += " ... СУКА БЛЯТЬ!"
    if b_data['slavaukraine_mode'] and random.random() < 0.3:
        phrase = "\n\n" + random.choice(UKRAINIAN_PHRASES)
        if 'text' in modified_content and modified_content['text']: modified_content['text'] += phrase
        elif 'caption' in modified_content and modified_content['caption']: modified_content['caption'] += phrase
    elif b_data['zaputin_mode'] and random.random() < 0.3:
        phrase = "\n\n" + random.choice(PATRIOTIC_PHRASES)
        if 'text' in modified_content and modified_content['text']: modified_content['text'] += phrase
        elif 'caption' in modified_content and modified_content['caption']: modified_content['caption'] += phrase

    blocked_users = set()
    active_recipients = {uid for uid in recipients if uid not in b_data['users']['banned']}
    if not active_recipients:
        return []

    async def really_send(uid: int, reply_to: int | None):
        try:
            ct_raw = modified_content["type"]
            ct = ct_raw.value if hasattr(ct_raw, 'value') else ct_raw
            
            header_text = modified_content['header']
            head = f"<i>{header_text}</i>"

            reply_to_post = modified_content.get('reply_to_post')
            original_author = messages_storage.get(reply_to_post, {}).get('author_id') if reply_to_post else None

            if uid == original_author:
                head = head.replace("Пост", "🔴 Пост")

            reply_text = f">>{reply_to_post} (You)\n" if uid == original_author else (f">>{reply_to_post}\n" if reply_to_post else "")
            main_text_raw = modified_content.get('text') or modified_content.get('caption') or ''
            main_text = add_you_to_my_posts(main_text_raw, uid)
            
            if ct == "media_group":
                if not modified_content.get('media'): return None
                builder = MediaGroupBuilder()
                full_text_for_group = f"{head}\n\n{reply_text}{main_text}" if (reply_text or main_text) else head
                for idx, media in enumerate(modified_content['media']):
                    caption = full_text_for_group if idx == 0 else None
                    builder.add(type=media['type'], media=media['file_id'], caption=caption, parse_mode="HTML" if caption else None)
                return await bot_instance.send_media_group(chat_id=uid, media=builder.build(), reply_to_message_id=reply_to)
            
            method_name = f"send_{ct}"
            if ct == 'text':
                method_name = 'send_message'

            send_method = getattr(bot_instance, method_name)
            kwargs = {'reply_to_message_id': reply_to}
            
            full_text = f"{head}\n\n{reply_text}{main_text}" if (reply_text or main_text) else head

            if ct == 'text':
                kwargs.update(text=full_text, parse_mode="HTML")
            elif ct in ['photo', 'video', 'animation', 'document', 'audio']:
                if len(full_text) > 1024: full_text = full_text[:1021] + "..."
                kwargs.update(caption=full_text, parse_mode="HTML")
                file_source = modified_content.get('image_url') or modified_content.get("file_id")
                kwargs[ct] = file_source
            elif ct == 'voice':
                kwargs.update(caption=head, parse_mode="HTML")
                kwargs[ct] = modified_content["file_id"]
            elif ct in ['sticker', 'video_note']:
                kwargs[ct] = modified_content["file_id"]
            else:
                print(f"❌ Неизвестный тип контента для отправки: {ct}")
                return None
                
            return await send_method(uid, **kwargs)

        except TelegramRetryAfter as e:
            await asyncio.sleep(e.retry_after + 1)
            return await really_send(uid, reply_to)
        except TelegramForbiddenError:
            blocked_users.add(uid)
            return None
        except TelegramBadRequest as e:
            if "VOICE_MESSAGES_FORBIDDEN" in e.message and modified_content.get("type") == "voice":
                print(f"ℹ️ Пользователь {uid} запретил голосовые. Отправляю как аудио...")
                try:
                    return await bot_instance.send_audio(
                        chat_id=uid,
                        audio=modified_content["file_id"],
                        caption=f"<i>{modified_content['header']}</i>",
                        parse_mode="HTML",
                        reply_to_message_id=reply_to
                    )
                except Exception as audio_e:
                    print(f"❌ Не удалось отправить как аудио для {uid}: {audio_e}")
                    return None
            # --- НАЧАЛО ИЗМЕНЕНИЙ ---
            elif "VIDEO_MESSAGES_FORBIDDEN" in e.message and modified_content.get("type") == "video_note":
                print(f"ℹ️ Пользователь {uid} запретил видеосообщения. Отправляю как видео...")
                try:
                    # Повторяем отправку, но уже как обычное видео
                    return await bot_instance.send_video(
                        chat_id=uid,
                        video=modified_content["file_id"],
                        caption=f"<i>{modified_content['header']}</i>", # "Кружки" не имеют подписи, добавляем только заголовок
                        parse_mode="HTML",
                        reply_to_message_id=reply_to
                    )
                except Exception as video_e:
                    print(f"❌ Не удалось отправить как видео для {uid}: {video_e}")
                    return None
            # --- КОНЕЦ ИЗМЕНЕНИЙ ---
            else:
                print(f"❌ Ошибка отправки (BadRequest) {uid} ботом {bot_instance.id}: {e}")
                return None
        except Exception as e:
            print(f"❌ Ошибка отправки {uid} ботом {bot_instance.id}: {e}")
            return None

    semaphore = asyncio.Semaphore(100)
    async def send_with_semaphore(uid):
        async with semaphore:
            reply_to = reply_info.get(uid) if reply_info else None
            return await really_send(uid, reply_to)

    tasks = [send_with_semaphore(uid) for uid in active_recipients]
    results = await asyncio.gather(*tasks)

    if content.get('post_num'):
        post_num = content['post_num']
        for uid, msg in zip(active_recipients, results):
            if not msg: continue
            messages_to_save = msg if isinstance(msg, list) else [msg]
            for m in messages_to_save:
                post_to_messages.setdefault(post_num, {})[uid] = m.message_id
                message_to_post[(uid, m.message_id)] = post_num

    if blocked_users:
        for uid in blocked_users:
            if uid in b_data['users']['active']:
                b_data['users']['active'].discard(uid)
                print(f"🚫 [{board_id}] Пользователь {uid} заблокировал бота, удален из активных")

    return list(zip(active_recipients, results))
    
async def message_broadcaster(bots: dict[str, Bot]):
    """Обработчик очереди сообщений с воркерами для каждой доски."""
    tasks = [
        asyncio.create_task(message_worker(f"Worker-{board_id}", board_id, bot_instance))
        for board_id, bot_instance in bots.items()
    ]
    await asyncio.gather(*tasks)

async def message_worker(worker_name: str, board_id: str, bot_instance: Bot):
    """Индивидуальный обработчик сообщений для одной доски."""
    queue = message_queues[board_id]
    b_data = board_data[board_id]
    
    while True:
        try:
            msg_data = await queue.get()
            if not msg_data:
                await asyncio.sleep(0.05)
                continue

            if not await validate_message_format(msg_data):
                continue

            recipients = msg_data['recipients']
            content = msg_data['content']
            post_num = msg_data['post_num']  # Извлекаем post_num
            reply_info = msg_data.get('reply_info', {})
            active_recipients = {uid for uid in recipients if uid not in b_data['users']['banned']}

            if not active_recipients:
                continue
            
            # --- КЛЮЧЕВОЕ ИЗМЕНЕНИЕ ---
            # Добавляем post_num в словарь content перед отправкой
            content['post_num'] = post_num

            await send_message_to_users(
                bot_instance,
                active_recipients,
                content,
                reply_info
            )
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

async def process_successful_messages(post_num: int, results: list):
    """Обработка успешных отправок (вынесено в отдельную функцию)"""
    post_to_messages.setdefault(post_num, {})

    for uid, msg in results:
        if not msg:
            continue

        if isinstance(msg, list):  # Медиагруппа
            post_to_messages[post_num][uid] = msg[0].message_id
            for m in msg:
                message_to_post[(uid, m.message_id)] = post_num
        else:  # Одиночное сообщение
            post_to_messages[post_num][uid] = msg.message_id
            message_to_post[(uid, msg.message_id)] = post_num

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

async def dvach_thread_poster():
    """
    Периодически (раз в 2-5 часов) постит случайный тред с 2ch
    на СЛУЧАЙНУЮ из наших досок.
    """
    await asyncio.sleep(300) # 5 минут начальная задержка

    # Доски 2ch, с которых будем парсить треды
    SOURCE_BOARDS = ['b', 'po', 'a', 'sex', 'vg', 'news']

    while True:
        try:
            # Случайная задержка от 2 до 5 часов
            delay = random.randint(7200, 18000)
            await asyncio.sleep(delay)

            # 1. Выбираем случайную доску ИСТОЧНИК для парсинга
            source_board = random.choice(SOURCE_BOARDS)
            
            # 2. Выбираем случайную доску НАЗНАЧЕНИЯ из наших ботов
            destination_board_id = random.choice(BOARDS)
            
            b_data = board_data[destination_board_id]
            recipients = b_data['users']['active'] - b_data['users']['banned']

            if not recipients:
                print(f"ℹ️ [{destination_board_id}] Пропуск постинга треда с 2ch, нет получателей.")
                continue

            # 3. Получаем тред
            thread_text = await fetch_dvach_thread(source_board)
            if not thread_text:
                continue
            
            # 4. Формируем и отправляем пост в очередь доски назначения
            header, post_num = await format_header(destination_board_id)
            
            # Добавляем информацию об источнике в заголовок
            header_with_source = f"{header} (/{source_board}/)"

            content = {
                'type': 'text',
                'header': header_with_source,
                'text': thread_text, # Передаем как есть
            }

            messages_storage[post_num] = {
                'author_id': 0, # Системное сообщение
                'timestamp': datetime.now(UTC),
                'content': content,
                'board_id': destination_board_id
            }

            await message_queues[destination_board_id].put({
                'recipients': recipients,
                'content': content,
                'post_num': post_num,
                'board_id': destination_board_id
            })

            print(f"✅ Тред с /{source_board}/ добавлен в очередь для доски /{destination_board_id}/")

        except Exception as e:
            print(f"❌ Ошибка в dvach_thread_poster: {e}")
            await asyncio.sleep(300) # Ждем 5 минут при ошибке

async def motivation_broadcaster():
    """Отправляет мотивационные сообщения на каждую доску в разное время."""
    await asyncio.sleep(15)  # Начальная задержка

    async def board_motivation_worker(board_id: str):
        """Индивидуальный воркер для одной доски."""
        while True:
            try:
                # Случайная задержка от 2 до 4 часов
                delay = random.randint(7200, 14400)
                await asyncio.sleep(delay)
                
                b_data = board_data[board_id]
                recipients = b_data['users']['active'] - b_data['users']['banned']

                if not recipients:
                    # print(f"ℹ️ [{board_id}] Нет активных пользователей для мотивационного сообщения")
                    continue

                motivation = random.choice(MOTIVATIONAL_MESSAGES)
                invite_text = random.choice(INVITE_TEXTS)

                now = datetime.now(MSK)
                date_str = now.strftime("%d/%m/%y")
                weekday = WEEKDAYS[now.weekday()]
                time_str = now.strftime("%H:%M:%S")
                
                header = f"<i>### АДМИН ### {date_str} ({weekday}) {time_str}</i>"
                # Используем общую функцию format_header для инкремента счетчика
                _, post_num = await format_header(board_id)

                message_text = (
                    f"💭 {motivation}\n\n"
                    f"Скопируй и отправь анончикам:\n"
                    f"<code>{escape_html(invite_text)}</code>"
                )

                content = {
                    'type': 'text',
                    'header': header,
                    'text': message_text,
                    'is_system_message': True # Флаг для особых сообщений
                }

                # Добавляем в очередь рассылки для конкретной доски
                await message_queues[board_id].put({
                    'recipients': recipients,
                    'content': content,
                    'post_num': post_num,
                    'reply_info': None,
                    'board_id': board_id
                })

                # Сохраняем системное сообщение в общем хранилище
                messages_storage[post_num] = {
                    'author_id': 0,
                    'timestamp': datetime.now(UTC),
                    'content': content,
                    'board_id': board_id
                }

                print(f"✅ [{board_id}] Мотивационное сообщение #{post_num} добавлено в очередь")

            except Exception as e:
                print(f"❌ [{board_id}] Ошибка в motivation_broadcaster: {e}")
                await asyncio.sleep(120)  # Ждем 2 минуты при ошибке

    # Запускаем по одному воркеру на каждую доску
    tasks = [asyncio.create_task(board_motivation_worker(bid)) for bid in BOARDS]
    await asyncio.gather(*tasks)
    
async def check_cooldown(message: Message, board_id: str) -> bool:
    """Проверяет кулдаун на активацию режимов для конкретной доски"""
    b_data = board_data[board_id]
    last_activation = b_data.get('last_mode_activation')

    if last_activation is None:
        return True

    elapsed = (datetime.now(UTC) - last_activation).total_seconds()
    if elapsed < MODE_COOLDOWN:
        time_left = MODE_COOLDOWN - elapsed
        minutes = int(time_left // 60)
        seconds = int(time_left % 60)

        try:
            await message.answer(
                f"⏳ Эй пидор, не спеши! Режимы на этой доске можно включать раз в час.\n"
                f"Жди еще: {minutes} минут {seconds} секунд\n\n"
                f"А пока посиди в углу и подумай о своем поведении",
                parse_mode="HTML"
            )
        except Exception as e:
            print(f"Ошибка отправки кулдауна: {e}")

        await message.delete()
        return False

    return True

def get_board_id(telegram_object: types.Message | types.CallbackQuery) -> str | None:
    """
    Определяет ID доски ('b', 'po', etc.) по объекту сообщения или колбэка.
    Это ключевая функция для работы с несколькими ботами.
    """
    bot_token = telegram_object.bot.token
    for board_id, config in BOARD_CONFIG.items():
        if config['token'] == bot_token:
            return board_id
    
    # Эта ситуация не должна происходить при правильной настройке
    print(f"⚠️ CRITICAL: Не удалось определить board_id для бота с токеном, заканчивающимся на ...{bot_token[-6:]}")
    return None

# ========== КОМАНДЫ ==========

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    board_id = get_board_id(message)
    if not board_id: return
    
    b_data = board_data[board_id]

    if user_id not in b_data['users']['active']:
        b_data['users']['active'].add(user_id)
        print(f"✅ [{board_id}] Новый пользователь через /start: ID {user_id}")
    
    start_text = b_data.get('start_message_text', "Добро пожаловать в ТГАЧ!")
    
    await message.answer(start_text, parse_mode="HTML", disable_web_page_preview=True)
    await message.delete()

    

AHE_EYES = ['😵', '🤤', '😫', '😩', '😳', '😖', '🥵']
AHE_TONGUE = ['👅', '💦', '😛', '🤪', '😝']
AHE_EXTRA = ['💕', '💗', '✨', '🥴', '']


@dp.message(Command("face"))
async def cmd_face(message: types.Message):
    board_id = get_board_id(message)
    if not board_id: return

    face = (secrets.choice(AHE_EYES) + secrets.choice(AHE_TONGUE) +
            secrets.choice(AHE_EXTRA))

    header, pnum = await format_header(board_id)
    content = {"type": "text", "header": header, "text": face}
    
    messages_storage[pnum] = {'author_id': 0, 'timestamp': datetime.now(UTC), 'content': content, 'board_id': board_id}
    
    await message_queues[board_id].put({
        "recipients": board_data[board_id]['users']['active'],
        "content": content,
        "post_num": pnum,
        "board_id": board_id
    })
    await message.delete()


@dp.message(Command("help"))
async def cmd_help(message: types.Message):
    board_id = get_board_id(message)
    if not board_id: return

    # Отправляем текст помощи с ссылками на все доски
    start_text = board_data[board_id].get('start_message_text', "Нет информации о помощи.")
    await message.answer(start_text, parse_mode="HTML", disable_web_page_preview=True)
    await message.delete()


@dp.message(Command("roll"))
async def cmd_roll(message: types.Message):
    board_id = get_board_id(message)
    if not board_id: return
    
    result = random.randint(1, 100)

    header, pnum = await format_header(board_id)
    content = {"type": "text", "header": header, "text": f"🎲 Нароллил: {result}"}

    messages_storage[pnum] = {'author_id': 0, 'timestamp': datetime.now(UTC), 'content': content, 'board_id': board_id}

    await message_queues[board_id].put({
        "recipients": board_data[board_id]['users']['active'],
        "content": content,
        "post_num": pnum,
        "board_id": board_id
    })
    await message.delete()

@dp.message(Command("slavaukraine"))
async def cmd_slavaukraine(message: types.Message):
    board_id = get_board_id(message)
    if not board_id: return

    b_data = board_data[board_id]

    if not await check_cooldown(message, board_id):
        return

    # Активация режима и деактивация других на ЭТОЙ доске
    b_data['slavaukraine_mode'] = True
    b_data['last_mode_activation'] = datetime.now(UTC)
    b_data['zaputin_mode'] = False
    b_data['suka_blyat_mode'] = False
    b_data['anime_mode'] = False

    _, pnum = await format_header(board_id)
    header = "### Админ ###"

    activation_text = (
        "УВАГА! АКТИВОВАНО УКРАЇНСЬКИЙ РЕЖИМ!\n\n"
        "💙💛 СЛАВА УКРАЇНІ! 💛💙\n"
        "ГЕРОЯМ СЛАВА!\n\n"
        "Хто не скаже 'Путін хуйло' - той москаль і підар!"
    )

    # --- НАЧАЛО ИЗМЕНЕНИЙ ---
    content = {
        "type": "text",
        "header": header,
        "text": activation_text
    }

    messages_storage[pnum] = {
        'author_id': 0,
        'timestamp': datetime.now(UTC),
        'content': content,
        'board_id': board_id
    }

    await message_queues[board_id].put({
        "recipients": b_data['users']['active'],
        "content": content,
        "post_num": pnum,
    })
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---

    asyncio.create_task(disable_slavaukraine_mode(310, board_id))
    await message.delete()


async def disable_slavaukraine_mode(delay: int, board_id: str):
    await asyncio.sleep(delay)
    
    b_data = board_data[board_id]
    b_data['slavaukraine_mode'] = False

    _, pnum = await format_header(board_id)
    header = "### Админ ###"

    end_text = (
        "💀 Визг хохлов закончен!\n\n"
        "Украинский режим отключен. Возвращаемся к обычному трёпу."
    )
    
    # --- НАЧАЛО ИЗМЕНЕНИЙ ---
    content = {
        "type": "text",
        "header": header,
        "text": end_text
    }
    
    messages_storage[pnum] = {
        'author_id': 0, # Системное сообщение
        'timestamp': datetime.now(UTC),
        'content': content,
        'board_id': board_id
    }
    
    await message_queues[board_id].put({
        "recipients": b_data['users']['active'],
        "content": content,
        "post_num": pnum,
    })
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---

@dp.message(Command("stop"))
async def cmd_stop(message: types.Message):
    """Остановка любых активных режимов на текущей доске."""
    board_id = get_board_id(message)
    if not board_id: return

    if not is_admin(message.from_user.id, board_id):
        await message.delete()
        return

    # Получаем срез данных для текущей доски
    b_data = board_data[board_id]

    # Сбрасываем все флаги режимов для ЭТОЙ доски
    b_data['zaputin_mode'] = False
    b_data['suka_blyat_mode'] = False
    b_data['slavaukraine_mode'] = False
    b_data['anime_mode'] = False
    
    # Сбрасываем кулдаун, чтобы можно было сразу включить новый режим
    b_data['last_mode_activation'] = None

    await message.answer(f"Все активные режимы на доске {BOARD_CONFIG[board_id]['name']} остановлены.")
    await message.delete()

@dp.message(Command("invite"))
async def cmd_invite(message: types.Message):
    board_id = get_board_id(message)
    if not board_id: return

    board_username = BOARD_CONFIG[board_id]['username']
    
    # Генерируем приглашение с юзернеймом конкретного бота
    invite_texts_specific = [
        f"Анон, залетай в Тгач {board_username}\nТут можно постить что угодно анонимно",
        f"Есть телега? Есть желание постить анонимно? \n{board_username} - добро пожаловать",
        f"Устал от цензуры? Хочешь анонимности?\n Велкам в Тгач - {board_username} - настоящий двач в телеге",
        f"{board_username} - анонимный чат в телеге\nБез регистрации и смс",
    ]
    invite_text = random.choice(invite_texts_specific)

    await message.answer(
        f"📨 <b>Текст для приглашения анонов на эту доску:</b>\n\n"
        f"<code>{escape_html(invite_text)}</code>\n\n"
        f"<i>Просто скопируй и отправь</i>",
        parse_mode="HTML")
    await message.delete() # Удаляем саму команду /invite


@dp.message(Command("stats"))
async def cmd_stats(message: types.Message):
    board_id = get_board_id(message)
    if not board_id: return
    
    b_data = board_data[board_id]
    total_users_on_board = len(b_data['users']['active'])
    
    # --- КЛЮЧЕВОЕ ИЗМЕНЕНИЕ: Замена медленного подсчета на быстрый доступ ---
    total_posts_on_board = b_data.get('board_post_count', 0)
    
    # Получаем общее количество уникальных пользователей с доски 'b'
    total_users_b = len(board_data['b']['users']['active'])

    stats_text = (f"📊 Статистика доски {BOARD_CONFIG[board_id]['name']}:\n\n"
                  f"👥 Анонимов на доске: {total_users_on_board}\n"
                  f"👥 Всего анонов в Тгаче: {total_users_b}\n"
                  f"📨 Постов на доске: {total_posts_on_board}\n"
                  f"📈 Всего постов в тгаче: {state['post_counter']}")

    header, pnum = await format_header(board_id)
    content = {'type': 'text', 'header': header, 'text': stats_text}
    
    messages_storage[pnum] = {'author_id': 0, 'timestamp': datetime.now(UTC), 'content': content, 'board_id': board_id}

    await message_queues[board_id].put({
        'recipients': b_data['users']['active'],
        'content': content,
        'post_num': pnum,
        'board_id': board_id
    })
    await message.delete()

@dp.message(Command("anime"))
async def cmd_anime(message: types.Message):
    board_id = get_board_id(message)
    if not board_id: return

    b_data = board_data[board_id]

    if not await check_cooldown(message, board_id):
        return

    b_data['anime_mode'] = True
    b_data['zaputin_mode'] = False
    b_data['slavaukraine_mode'] = False
    b_data['suka_blyat_mode'] = False
    b_data['last_mode_activation'] = datetime.now(UTC)

    header = "### 管理者 ###"
    _, pnum = await format_header(board_id)

    activation_text = (
        "にゃあ～！アニメモードがアクティベートされました！\n\n"
        "^_^"
    )

    # --- НАЧАЛО ИЗМЕНЕНИЙ ---
    content = {
        "type": "text",
        "header": header,
        "text": activation_text
    }

    messages_storage[pnum] = {
        'author_id': 0,
        'timestamp': datetime.now(UTC),
        'content': content,
        'board_id': board_id
    }

    await message_queues[board_id].put({
        "recipients": b_data['users']['active'],
        "content": content,
        "post_num": pnum,
    })
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---

    asyncio.create_task(disable_anime_mode(330, board_id))
    await message.delete()


async def disable_anime_mode(delay: int, board_id: str):
    await asyncio.sleep(delay)
    
    b_data = board_data[board_id]
    b_data['anime_mode'] = False

    header = "### Админ ###"
    _, pnum = await format_header(board_id)

    end_text = (
        "アニメモードが終了しました！\n\n"
        "通常のチャットに戻ります！"
    )

    # --- НАЧАЛО ИЗМЕНЕНИЙ ---
    content = {
        "type": "text",
        "header": header,
        "text": end_text
    }

    messages_storage[pnum] = {
        'author_id': 0, # Системное сообщение
        'timestamp': datetime.now(UTC),
        'content': content,
        'board_id': board_id
    }

    await message_queues[board_id].put({
        "recipients": b_data['users']['active'],
        "content": content,
        "post_num": pnum,
    })
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---
    
@dp.message(Command("deanon"))
async def cmd_deanon(message: Message):
    board_id = get_board_id(message)
    if not board_id: return
    
    if not message.reply_to_message:
        await message.answer("⚠️ Ответь на сообщение для деанона!")
        await message.delete()
        return

    # Находим номер поста, на который ответил пользователь
    target_mid = message.reply_to_message.message_id
    user_id = message.from_user.id
    target_post = message_to_post.get((user_id, target_mid))

    if not target_post or target_post not in messages_storage:
        await message.answer("🚫 Не удалось найти пост для деанона (возможно, вы ответили на чужую копию или старое сообщение).")
        await message.delete()
        return

    # --- НАЧАЛО ИЗМЕНЕНИЙ: Проверка на системное сообщение ---
    # Получаем ID автора оригинального поста
    original_author_id = messages_storage[target_post].get('author_id')

    # Запрещаем деанонить системные сообщения (у которых автор 0)
    if original_author_id == 0:
        await message.answer("⚠️ Нельзя деанонимизировать системные сообщения.")
        await message.delete()
        return
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---

    name, surname, city, profession, fetish, detail = generate_deanon_info()
    ip = f"{random.randint(10,250)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(0,255)}"
    age = random.randint(18, 45)
    
    deanon_text = (f"\nЭтого анона зовут: {name} {surname}\n"
                   f"Возраст: {age}\n"
                   f"Адрес проживания: {city}\n"
                   f"Профессия: {profession}\n"
                   f"Фетиш: {fetish}\n"
                   f"IP-адрес: {ip}\n"
                   f"Дополнительная информация о нём: {detail}")

    header = "### ДЕАНОН ###"
    _, pnum = await format_header(board_id)
    content = {"type": "text", "header": header, "text": deanon_text, "reply_to_post": target_post}

    messages_storage[pnum] = {'author_id': 0, 'timestamp': datetime.now(UTC), 'content': content, 'board_id': board_id}

    # Деанон отправляется на ту же доску, где была вызвана команда
    await message_queues[board_id].put({
        "recipients": board_data[board_id]['users']['active'],
        "content": content,
        "post_num": pnum,
        "reply_info": post_to_messages.get(target_post, {}),
        "board_id": board_id
    })
    await message.delete()
    
@dp.message(Command("zaputin"))
async def cmd_zaputin(message: types.Message):
    board_id = get_board_id(message)
    if not board_id: return

    b_data = board_data[board_id]

    if not await check_cooldown(message, board_id):
        return

    b_data['zaputin_mode'] = True
    b_data['suka_blyat_mode'] = False
    b_data['slavaukraine_mode'] = False
    b_data['anime_mode'] = False
    b_data['last_mode_activation'] = datetime.now(UTC)

    header = "### Админ ###"
    _, pnum = await format_header(board_id)

    activation_text = (
        "🇷🇺 СЛАВА РОССИИ! ПУТИН - НАШ ПРЕЗИДЕНТ! 🇷🇺\n\n"
        "Активирован режим кремлеботов! Все несогласные будут приравнены к пидорасам и укронацистам!"
    )

    # --- НАЧАЛО ИЗМЕНЕНИЙ ---
    content = {
        "type": "text",
        "header": header,
        "text": activation_text
    }

    messages_storage[pnum] = {
        'author_id': 0,
        'timestamp': datetime.now(UTC),
        'content': content,
        'board_id': board_id
    }

    await message_queues[board_id].put({
        "recipients": b_data['users']['active'],
        "content": content,
        "post_num": pnum,
    })
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---

    asyncio.create_task(disable_zaputin_mode(309, board_id))
    await message.delete()


async def disable_zaputin_mode(delay: int, board_id: str):
    await asyncio.sleep(delay)
    b_data = board_data[board_id]
    b_data['zaputin_mode'] = False

    header = "### Админ ###"
    _, pnum = await format_header(board_id)

    end_text = "💀 Бунт кремлеботов окончился. Всем спасибо, все свободны."

    # --- НАЧАЛО ИЗМЕНЕНИЙ ---
    content = {
        "type": "text",
        "header": header,
        "text": end_text
    }

    messages_storage[pnum] = {
        'author_id': 0, # Системное сообщение
        'timestamp': datetime.now(UTC),
        'content': content,
        'board_id': board_id
    }

    await message_queues[board_id].put({
        "recipients": b_data['users']['active'],
        "content": content,
        "post_num": pnum,
    })
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---```

@dp.message(Command("suka_blyat"))
async def cmd_suka_blyat(message: types.Message):
    board_id = get_board_id(message)
    if not board_id: return

    b_data = board_data[board_id]

    if not await check_cooldown(message, board_id):
        return

    b_data['suka_blyat_mode'] = True
    b_data['zaputin_mode'] = False
    b_data['slavaukraine_mode'] = False
    b_data['anime_mode'] = False
    b_data['last_mode_activation'] = datetime.now(UTC)

    header = "### Админ ###"
    _, pnum = await format_header(board_id)

    activation_text = (
        "💢💢💢 Активирован режим СУКА БЛЯТЬ! 💢💢💢\n\n"
        "Всех нахуй разъебало!"
    )

    # --- НАЧАЛО ИЗМЕНЕНИЙ ---
    content = {
        "type": "text",
        "header": header,
        "text": activation_text
    }
    
    messages_storage[pnum] = {
        'author_id': 0,
        'timestamp': datetime.now(UTC),
        'content': content,
        'board_id': board_id
    }

    await message_queues[board_id].put({
        "recipients": b_data['users']['active'],
        "content": content,
        "post_num": pnum,
    })
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---

    asyncio.create_task(disable_suka_blyat_mode(303, board_id))
    await message.delete()


async def disable_suka_blyat_mode(delay: int, board_id: str):
    await asyncio.sleep(delay)
    b_data = board_data[board_id]
    b_data['suka_blyat_mode'] = False

    header = "### Админ ###"
    _, pnum = await format_header(board_id)

    end_text = "💀 СУКА БЛЯТЬ КОНЧИЛОСЬ. Теперь можно и помолчать."

    # --- НАЧАЛО ИЗМЕНЕНИЙ ---
    content = {
        "type": "text",
        "header": header,
        "text": end_text
    }

    messages_storage[pnum] = {
        'author_id': 0, # Системное сообщение
        'timestamp': datetime.now(UTC),
        'content': content,
        'board_id': board_id
    }

    await message_queues[board_id].put({
        "recipients": b_data['users']['active'],
        "content": content,
        "post_num": pnum,
    })
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---
    
# ========== АДМИН КОМАНДЫ ==========

@dp.message(Command("admin"))
async def cmd_admin(message: types.Message):
    board_id = get_board_id(message)
    if not is_admin(message.from_user.id, board_id):
        await message.delete()
        return

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Статистика доски", callback_data=f"stats_{board_id}")],
        [InlineKeyboardButton(text="🚫 Забаненные на доске", callback_data=f"banned_{board_id}")],
        [InlineKeyboardButton(text="💾 Сохранить ВСЕ", callback_data="save_all")],
    ])
    await message.answer(f"Админка доски {BOARD_CONFIG[board_id]['name']}:", reply_markup=keyboard)
    await message.delete()

@dp.callback_query(F.data == "save_all")
async def admin_save_all(callback: types.CallbackQuery):
    # Проверяем, является ли юзер админом ХОТЯ БЫ ОДНОЙ доски
    is_any_admin = any(is_admin(callback.from_user.id, b_id) for b_id in BOARDS)
    if not is_any_admin:
        await callback.answer("Отказано в доступе", show_alert=True)
        return

    await callback.answer("Запуск сохранения всех данных...")
    await save_all_boards_and_backup()
    await callback.message.edit_text("✅ Состояние всех досок сохранено и отправлено в GitHub.")

@dp.callback_query(F.data.startswith("stats_"))
async def admin_stats_board(callback: types.CallbackQuery):
    board_id = callback.data.split("_")[1]
    if not is_admin(callback.from_user.id, board_id):
        await callback.answer("Отказано в доступе", show_alert=True)
        return

    b_data = board_data[board_id]
    stats_text = (
        f"Статистика доски {BOARD_CONFIG[board_id]['name']}:\n\n"
        f"Активных: {len(b_data['users']['active'])}\n"
        f"Забаненных: {len(b_data['users']['banned'])}\n"
        f"В очереди: {message_queues[board_id].qsize()}"
    )
    await callback.message.edit_text(stats_text)
    await callback.answer()


@dp.callback_query(F.data.startswith("banned_"))
async def admin_banned_board(callback: types.CallbackQuery):
    board_id = callback.data.split("_")[1]
    if not is_admin(callback.from_user.id, board_id):
        await callback.answer("Отказано в доступе", show_alert=True)
        return

    banned_users = board_data[board_id]['users']['banned']
    if not banned_users:
        await callback.message.edit_text(f"На доске {BOARD_CONFIG[board_id]['name']} нет забаненных.")
        await callback.answer()
        return

    text = f"Забаненные на доске {BOARD_CONFIG[board_id]['name']}:\n\n"
    text += "\n".join([f"ID <code>{uid}</code>" for uid in banned_users])
    
    await callback.message.edit_text(text, parse_mode="HTML")
    await callback.answer()

# ===== Вспомогательная функция =====================================
def get_author_id_by_reply(msg: types.Message) -> int | None:
    """
    Получает ID автора поста по ответу на его копию.
    Работает через прямой поиск по ключу, а не перебор.
    """
    if not msg.reply_to_message:
        return None

    # Ключ для поиска - это ID того, КТО ответил (админ), 
    # и ID сообщения, НА КОТОРОЕ он ответил.
    admin_id = msg.from_user.id
    reply_mid = msg.reply_to_message.message_id
    lookup_key = (admin_id, reply_mid)

    # Прямой поиск поста по этому ключу
    post_num = message_to_post.get(lookup_key)

    if post_num and post_num in messages_storage:
        return messages_storage[post_num].get("author_id")

    return None

@dp.message(Command("id"))
async def cmd_get_id(message: types.Message):
    """ /id — вывести ID и инфу автора реплай-поста или свою, если без reply """
    board_id = get_board_id(message)
    if not board_id: return
    
    # Проверяем, является ли вызвавший команду админом на текущей доске
    if not is_admin(message.from_user.id, board_id):
        await message.delete()
        return

    target_id = message.from_user.id
    info_header = "🆔 <b>Информация о вас:</b>\n\n"
    
    if message.reply_to_message:
        replied_author_id = get_author_id_by_reply(message)
        
        # --- НАЧАЛО ИЗМЕНЕНИЙ ---
        if replied_author_id == 0:
            await message.answer("ℹ️ Вы ответили на системное сообщение (автор: бот).")
            await message.delete()
            return
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---

        if replied_author_id:
            target_id = replied_author_id
            info_header = "🆔 <b>Информация о пользователе:</b>\n\n"
        # Если replied_author_id is None, target_id останется равным ID админа,
        # и команда покажет его инфу (например, при ответе на чужую копию).

    try:
        user_chat_info = await message.bot.get_chat(target_id)
        
        info = info_header
        info += f"ID: <code>{target_id}</code>\n"
        if user_chat_info.first_name:
            info += f"Имя: {escape_html(user_chat_info.first_name)}\n"
        if user_chat_info.last_name:
            info += f"Фамилия: {escape_html(user_chat_info.last_name)}\n"
        if user_chat_info.username:
            info += f"Username: @{user_chat_info.username}\n"

        b_data = board_data[board_id]
        if target_id in b_data['users']['banned']:
            info += f"\n⛔️ Статус на доске {BOARD_CONFIG[board_id]['name']}: ЗАБАНЕН"
        elif target_id in b_data['users']['active']:
            info += f"\n✅ Статус на доске {BOARD_CONFIG[board_id]['name']}: Активен"
        else:
            info += f"\nℹ️ Статус на доске {BOARD_CONFIG[board_id]['name']}: Неактивен"
            
        await message.answer(info, parse_mode="HTML")

    except Exception:
        await message.answer(f"ID пользователя: <code>{target_id}</code>", parse_mode="HTML")
    
    await message.delete()

@dp.message(Command("ban"))
async def cmd_ban(message: types.Message):
    board_id = get_board_id(message)
    if not is_admin(message.from_user.id, board_id):
        return

    target_id: int | None = None
    if message.reply_to_message:
        target_id = get_author_id_by_reply(message)

    parts = message.text.split()
    if len(parts) == 2 and parts[1].isdigit():
        target_id = int(parts[1])

    if not target_id:
        await message.answer("Нужно ответить на сообщение или указать ID: /ban <id>")
        return

    deleted_posts = await delete_user_posts(message.bot, target_id, 5, board_id)

    b_data = board_data[board_id]
    b_data['users']['banned'].add(target_id)
    b_data['users']['active'].discard(target_id)

    await message.answer(
        f"✅ Хуесос под номером <code>{target_id}</code> забанен на доске {BOARD_CONFIG[board_id]['name']}\n"
        f"Удалено его постов за последние 5 минут: {deleted_posts}",
        parse_mode="HTML")

    await send_moderation_notice(target_id, "ban", board_id, deleted_posts=deleted_posts)

    try:
        await message.bot.send_message(
            target_id,
            f"Пидорас ебаный, ты нас так заебал, что тебя блокнули нахуй на доске {BOARD_CONFIG[board_id]['name']}.\n"
            f"Удалено твоих постов за последние 5 минут: {deleted_posts}\n"
            "Пиздуй отсюда."
        )
    except:
        pass
    await message.delete()

@dp.message(Command("mute"))
async def cmd_mute(message: Message):
    board_id = get_board_id(message)
    if not is_admin(message.from_user.id, board_id):
        await message.delete()
        return

    command_args = message.text.split()[1:]
    if not command_args and not message.reply_to_message:
        await message.answer("Использование: /mute <user_id> [время] или ответом на сообщение.")
        await message.delete()
        return

    target_id = None
    duration_str = "24h"

    if message.reply_to_message:
        target_id = get_author_id_by_reply(message)
        if command_args:
            duration_str = command_args[0]
    elif command_args:
        try:
            target_id = int(command_args[0])
            if len(command_args) > 1:
                duration_str = command_args[1]
        except ValueError:
            await message.answer("❌ Неверный ID пользователя")
            await message.delete()
            return
            
    if not target_id:
        await message.answer("❌ Не удалось определить пользователя")
        await message.delete()
        return

    try:
        duration_str = duration_str.lower().replace(" ", "")
        if duration_str.endswith("m"): mute_seconds, duration_text = int(duration_str[:-1]) * 60, f"{int(duration_str[:-1])} минут"
        elif duration_str.endswith("h"): mute_seconds, duration_text = int(duration_str[:-1]) * 3600, f"{int(duration_str[:-1])} часов"
        elif duration_str.endswith("d"): mute_seconds, duration_text = int(duration_str[:-1]) * 86400, f"{int(duration_str[:-1])} дней"
        else: mute_seconds, duration_text = int(duration_str) * 60, f"{int(duration_str)} минут"
        mute_seconds = min(mute_seconds, 2592000)
    except (ValueError, AttributeError):
        await message.answer("❌ Неверный формат времени (Примеры: 30m, 2h, 1d)")
        await message.delete()
        return

    deleted_count = await delete_user_posts(message.bot, target_id, 5, board_id)
    
    b_data = board_data[board_id]
    b_data['mutes'][target_id] = datetime.now(UTC) + timedelta(seconds=mute_seconds)

    await message.answer(
        f"🔇 Хуила {target_id} замучен на {duration_text} на доске {BOARD_CONFIG[board_id]['name']}\n"
        f"Удалено сообщений за последние 5 минут: {deleted_count}",
        parse_mode="HTML"
    )

    await send_moderation_notice(target_id, "mute", board_id, duration=duration_text, deleted_posts=deleted_count)

    try:
        await message.bot.send_message(
            target_id,
            f"🔇 Пидор ебаный, тебя замутили на доске {BOARD_CONFIG[board_id]['name']} на {duration_text}.\n"
            f"Удалено твоих сообщений за последние 5 минут: {deleted_count}",
            parse_mode="HTML"
        )
    except:
        pass
    await message.delete()

@dp.message(Command("wipe"))
async def cmd_wipe(message: types.Message):
    board_id = get_board_id(message)
    if not is_admin(message.from_user.id, board_id):
        return

    target_id = None
    if message.reply_to_message:
        target_id = get_author_id_by_reply(message)
    else:
        parts = message.text.split()
        if len(parts) == 2 and parts[1].isdigit():
            target_id = int(parts[1])

    if not target_id:
        await message.answer("reply + /wipe или /wipe <id>")
        return

    # Удаляем все посты пользователя (большой период времени) только с текущей доски
    deleted_messages = await delete_user_posts(message.bot, target_id, 999999, board_id)

    await message.answer(
        f"🗑 Удалено {deleted_messages} сообщений пользователя {target_id} с доски {BOARD_CONFIG[board_id]['name']}."
    )
    await message.delete()

@dp.message(Command("unmute"))
async def cmd_unmute(message: types.Message):
    board_id = get_board_id(message)
    if not is_admin(message.from_user.id, board_id):
        return

    target_id = None
    if message.reply_to_message:
        target_id = get_author_id_by_reply(message)
    else:
        parts = message.text.split()
        if len(parts) == 2 and parts[1].isdigit():
            target_id = int(parts[1])

    if not target_id:
        await message.answer("Нужно reply или /unmute <id>")
        return

    b_data = board_data[board_id]
    if b_data['mutes'].pop(target_id, None):
        await message.answer(f"🔈 Пользователь {target_id} размучен на доске {BOARD_CONFIG[board_id]['name']}.")
        try:
            await message.bot.send_message(target_id, f"Тебя размутили на доске {BOARD_CONFIG[board_id]['name']}.")
        except:
            pass
    else:
        await message.answer(f"Пользователь {target_id} не был в муте на этой доске.")
    await message.delete()


@dp.message(Command("unban"))
async def cmd_unban(message: types.Message):
    board_id = get_board_id(message)
    if not is_admin(message.from_user.id, board_id):
        return

    args = message.text.split()
    if len(args) < 2:
        await message.answer("Использование: /unban <user_id>")
        return

    try:
        user_id = int(args[1])
        b_data = board_data[board_id]
        if b_data['users']['banned'].discard(user_id):
             await message.answer(f"Пользователь {user_id} разбанен на доске {BOARD_CONFIG[board_id]['name']}.")
        else:
            await message.answer(f"Пользователь {user_id} не был забанен на этой доске.")
    except ValueError:
        await message.answer("Неверный ID пользователя")
    await message.delete()


@dp.message(Command("del"))
async def cmd_del(message: types.Message):
    board_id = get_board_id(message)
    if not is_admin(message.from_user.id, board_id):
        return

    if not message.reply_to_message:
        await message.answer("Ответь на сообщение, которое нужно удалить")
        return

    target_mid = message.reply_to_message.message_id
    lookup_key = (message.from_user.id, target_mid)
    post_num = message_to_post.get(lookup_key)

    if post_num is None:
        await message.answer("Не нашёл этот пост в базе (возможно, вы ответили на чужую копию).")
        return

    # Используем новую, точную функцию для удаления одного поста
    deleted_count = await delete_single_post(post_num, message.bot)

    await message.answer(f"Пост №{post_num} и все его копии ({deleted_count} сообщений) удалены.")
    await message.delete()


@dp.message(Command("shadowmute"))
async def cmd_shadowmute(message: Message):
    board_id = get_board_id(message)
    if not is_admin(message.from_user.id, board_id):
        await message.delete()
        return

    args = message.text.split()[1:]
    target_id = None
    duration_str = "24h"

    if message.reply_to_message:
        target_id = get_author_id_by_reply(message)
        if args:
            duration_str = args[0]
    elif args:
        try:
            target_id = int(args[0])
            if len(args) > 1:
                duration_str = args[1]
        except ValueError:
            pass

    if not target_id:
        await message.answer("Использование: /shadowmute <user_id> [время] или ответ на сообщение.")
        return

    try:
        duration_str = duration_str.lower().replace(" ", "")
        if duration_str.endswith("m"): total_seconds, time_str = int(duration_str[:-1]) * 60, f"{int(duration_str[:-1])} мин"
        elif duration_str.endswith("h"): total_seconds, time_str = int(duration_str[:-1]) * 3600, f"{int(duration_str[:-1])} час"
        elif duration_str.endswith("d"): total_seconds, time_str = int(duration_str[:-1]) * 86400, f"{int(duration_str[:-1])} дней"
        else: total_seconds, time_str = int(duration_str) * 60, f"{int(duration_str)} мин"
        
        total_seconds = min(total_seconds, 2592000)
        b_data = board_data[board_id]
        b_data['shadow_mutes'][target_id] = datetime.now(UTC) + timedelta(seconds=total_seconds)

        await message.answer(f"👻 Тихо замучен пользователь {target_id} на {time_str} на доске {BOARD_CONFIG[board_id]['name']}.")
    except ValueError:
        await message.answer("❌ Неверный формат времени. Примеры: 30m, 2h, 1d")
    await message.delete()


@dp.message(Command("unshadowmute"))
async def cmd_unshadowmute(message: Message):
    board_id = get_board_id(message)
    if not is_admin(message.from_user.id, board_id):
        return

    target_id = None
    if message.reply_to_message:
        target_id = get_author_id_by_reply(message)
    else:
        parts = message.text.split()
        if len(parts) >= 2 and parts[1].isdigit():
            target_id = int(parts[1])

    if not target_id:
        await message.answer("Использование: /unshadowmute <user_id> или ответ на сообщение.")
        return
    
    b_data = board_data[board_id]
    if b_data['shadow_mutes'].pop(target_id, None):
        await message.answer(f"👻 Пользователь {target_id} тихо размучен на доске {BOARD_CONFIG[board_id]['name']}.")
    else:
        await message.answer(f"ℹ️ Пользователь {target_id} не в shadow-муте на этой доске.")
    await message.delete()

# ========== ОСНОВНОЙ ОБРАБОТЧИК СООБЩЕНИЙ ==========

@dp.message(F.audio)
async def handle_audio(message: Message):
    """Адаптированный обработчик аудио сообщений."""
    user_id = message.from_user.id
    board_id = get_board_id(message)
    if not board_id: return
    
    b_data = board_data[board_id]

    if user_id in b_data['users']['banned']:
        await message.delete()
        return

    if b_data['mutes'].get(user_id) and b_data['mutes'][user_id] > datetime.now(UTC):
        await message.delete()
        return

    header, current_post_num = await format_header(board_id)
    reply_to_post, reply_info = None, {}

    if message.reply_to_message:
        reply_mid = message.reply_to_message.message_id
        lookup_key = (user_id, reply_mid)
        reply_to_post = message_to_post.get(lookup_key)
        
        if reply_to_post and reply_to_post in post_to_messages:
            reply_info = post_to_messages[reply_to_post]

    try:
        await message.delete()
    except TelegramBadRequest:
        pass # Сообщение уже удалено или не может быть удалено, игнорируем
    
    content = {
        'type': 'audio', 'header': header, 'file_id': message.audio.file_id,
        'caption': message.caption or "", 'reply_to_post': reply_to_post
    }

    content = await _apply_mode_transformations(content, board_id)

    messages_storage[current_post_num] = {
        'author_id': user_id, 'timestamp': datetime.now(UTC), 'content': content,
        'reply_to': reply_to_post, 'board_id': board_id
    }

    # --- НАЧАЛО ИЗМЕНЕНИЙ ---
    try:
        reply_to_message_id = reply_info.get(user_id)
        
        caption_text = f"<i>{header}</i>"
        if content['caption']: 
            caption_text += f"\n\n{content['caption']}"

        sent_to_author = await message.bot.send_audio(
            user_id, message.audio.file_id, caption=caption_text,
            reply_to_message_id=reply_to_message_id, parse_mode="HTML"
        )

        if sent_to_author:
            messages_storage[current_post_num]['author_message_id'] = sent_to_author.message_id
            post_to_messages.setdefault(current_post_num, {})[user_id] = sent_to_author.message_id
            message_to_post[(user_id, sent_to_author.message_id)] = current_post_num

    except TelegramForbiddenError:
        b_data['users']['active'].discard(user_id)
        print(f"🚫 [{board_id}] Пользователь {user_id} заблокировал бота, удален из активных (из handle_audio).")
    except Exception as e:
        print(f"⚠️ Ошибка отправки аудио-поста #{current_post_num} автору {user_id}: {e}")

    recipients = b_data['users']['active'] - {user_id}
    if recipients and user_id in b_data['users']['active']:
        try:
            await message_queues[board_id].put({
                'recipients': recipients, 'content': content, 'post_num': current_post_num,
                'reply_info': reply_info, 'board_id': board_id
            })
        except Exception as e:
            print(f"❌ Критическая ошибка постановки в очередь аудио-поста. Пост #{current_post_num} удален. Ошибка: {e}")
            messages_storage.pop(current_post_num, None)
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---


@dp.message(F.voice)
async def handle_voice(message: Message):
    """Адаптированный обработчик голосовых сообщений."""
    user_id = message.from_user.id
    board_id = get_board_id(message)
    if not board_id: return
        
    b_data = board_data[board_id]

    if user_id in b_data['users']['banned']:
        await message.delete()
        return

    if b_data['mutes'].get(user_id) and b_data['mutes'][user_id] > datetime.now(UTC):
        await message.delete()
        return

    header, current_post_num = await format_header(board_id)
    reply_to_post, reply_info = None, {}

    if message.reply_to_message:
        reply_mid = message.reply_to_message.message_id
        # Эффективный поиск: используем ID пользователя, который отвечает, и ID сообщения, на которое он отвечает
        lookup_key = (user_id, reply_mid)
        reply_to_post = message_to_post.get(lookup_key)
        
        if reply_to_post and reply_to_post in post_to_messages:
            reply_info = post_to_messages[reply_to_post]

    try:
        await message.delete()
    except TelegramBadRequest:
        pass # Сообщение уже удалено или не может быть удалено, игнорируем

    content = {
        'type': 'voice', 'header': header, 'file_id': message.voice.file_id,
        'reply_to_post': reply_to_post
    }

    messages_storage[current_post_num] = {
        'author_id': user_id, 'timestamp': datetime.now(UTC), 'content': content,
        'reply_to': reply_to_post, 'board_id': board_id
    }

    # --- НАЧАЛО ИЗМЕНЕНИЙ ---
    try:
        reply_to_message_id = reply_info.get(user_id)
        sent_to_author = await message.bot.send_voice(
            user_id, message.voice.file_id, caption=f"<i>{header}</i>",
            reply_to_message_id=reply_to_message_id, parse_mode="HTML"
        )

        if sent_to_author:
            messages_storage[current_post_num]['author_message_id'] = sent_to_author.message_id
            post_to_messages.setdefault(current_post_num, {})[user_id] = sent_to_author.message_id
            message_to_post[(user_id, sent_to_author.message_id)] = current_post_num

    except TelegramForbiddenError:
        b_data['users']['active'].discard(user_id)
        print(f"🚫 [{board_id}] Пользователь {user_id} заблокировал бота, удален из активных (из handle_voice).")
    except Exception as e:
        print(f"⚠️ Ошибка отправки голосового поста #{current_post_num} автору {user_id}: {e}")

    recipients = b_data['users']['active'] - {user_id}
    if recipients and user_id in b_data['users']['active']:
        try:
            await message_queues[board_id].put({
                'recipients': recipients, 'content': content, 'post_num': current_post_num,
                'reply_info': reply_info, 'board_id': board_id
            })
        except Exception as e:
            print(f"❌ Критическая ошибка постановки в очередь голосового поста. Пост #{current_post_num} удален. Ошибка: {e}")
            messages_storage.pop(current_post_num, None)
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---
        
@dp.message(F.media_group_id)
async def handle_media_group_init(message: Message):
    media_group_id = message.media_group_id
    # Ранний выход для уже обработанных групп
    if not media_group_id or media_group_id in sent_media_groups:
        # НЕ УДАЛЯЕМ СООБЩЕНИЕ, ЧТОБЫ ИЗБЕЖАТЬ FLOOD-ОШИБОК
        return

    user_id = message.from_user.id
    board_id = get_board_id(message)
    if not board_id: return

    b_data = board_data[board_id]

    # Быстрый выход для забаненных или замученных
    if user_id in b_data['users']['banned'] or \
       (b_data['mutes'].get(user_id) and b_data['mutes'][user_id] > datetime.now(UTC)):
        # НЕ УДАЛЯЕМ СООБЩЕНИЕ
        return
    
    group = current_media_groups.get(media_group_id)
    is_leader = False

    # setdefault является атомарной операцией. Первый, кто ее вызовет, создаст запись
    # и сможет стать лидером. Остальные получат уже существующий объект.
    if group is None:
        group = current_media_groups.setdefault(media_group_id, {'is_initializing': True})
        if group.get('is_initializing'):
            is_leader = True
    
    # Только "лидер" выполняет этот блок для инициализации группы
    if is_leader:
        # Симулируем текстовое сообщение, чтобы применить общие лимиты частоты, а не специфичные для гифок.
        # Это предотвращает ложное срабатывание на повтор и использует более адекватные лимиты.
        fake_text_message = types.Message(
            message_id=message.message_id,
            date=message.date,
            chat=message.chat,
            from_user=message.from_user,
            content_type='text',
            text=f"media_group_{media_group_id}" # Уникальный текст для избежания ложного детекта повторов
        )
        
        spam_check_passed = await check_spam(user_id, fake_text_message, board_id)
        
        if not spam_check_passed:
            current_media_groups.pop(media_group_id, None) 
            # НЕ УДАЛЯЕМ СООБЩЕНИЕ
            await apply_penalty(message.bot, user_id, 'text', board_id)
            return
        
        reply_to_post = None
        if message.reply_to_message:
            lookup_key = (user_id, message.reply_to_message.message_id)
            reply_to_post = message_to_post.get(lookup_key)

        header, post_num = await format_header(board_id)
        caption = message.caption or ""
        
        # --- ИЗМЕНЕНИЕ ЗДЕСЬ ---
        # Инициализируем хранилище для ID исходных сообщений
        group.update({
            'board_id': board_id, 'post_num': post_num, 'header': header, 'author_id': user_id,
            'timestamp': datetime.now(UTC), 'media': [], 'caption': caption,
            'reply_to_post': reply_to_post, 'processed_messages': set(),
            'source_message_ids': set() # <--- НОВОЕ ПОЛЕ
        })
        # Снимаем флаг инициализации, разрешая остальным продолжать
        group.pop('is_initializing', None)
    else:
        # "Последователи" ждут, пока "лидер" закончит инициализацию
        while group is not None and group.get('is_initializing'):
            await asyncio.sleep(0.05)
            group = current_media_groups.get(media_group_id)
        
        # Если лидер отменил создание группы (из-за спама), выходим
        if media_group_id not in current_media_groups:
            return

    # Этот блок теперь выполняется всеми сообщениями ПОСЛЕ инициализации
    if not group:
        return
        
    # --- ИЗМЕНЕНИЕ ЗДЕСЬ ---
    # Собираем ID всех сообщений из группы для последующего пакетного удаления
    group.get('source_message_ids', set()).add(message.message_id)
        
    if message.message_id not in group['processed_messages']:
        media_data = {'type': message.content_type, 'file_id': None}
        if message.photo: media_data['file_id'] = message.photo[-1].file_id
        elif message.video: media_data['file_id'] = message.video.file_id
        elif message.document: media_data['file_id'] = message.document.file_id
        elif message.audio: media_data['file_id'] = message.audio.file_id
        
        if media_data['file_id']:
            group['media'].append(media_data)
            group['processed_messages'].add(message.message_id)

    # --- ИЗМЕНЕНИЕ ЗДЕСЬ ---
    # Удаление отдельного сообщения убрано. Удаление будет пакетным на следующем шаге.

    # Перезапускаем таймер с каждым новым сообщением
    if media_group_id in media_group_timers:
        media_group_timers[media_group_id].cancel()
    
    media_group_timers[media_group_id] = asyncio.create_task(
        complete_media_group_after_delay(media_group_id, message.bot, delay=1.5)
    )
    
async def complete_media_group_after_delay(media_group_id: str, bot_instance: Bot, delay: float = 1.5):
    try:
        await asyncio.sleep(delay)

        # Атомарно извлекаем группу из словаря. Если ее там уже нет, выходим.
        group = current_media_groups.pop(media_group_id, None)
        if not group or media_group_id in sent_media_groups:
            return

        # Удаляем таймер, так как мы его сейчас выполним.
        media_group_timers.pop(media_group_id, None)

        # --- НАЧАЛО ИЗМЕНЕНИЙ: Пакетное удаление ---
        # Извлекаем ID собранных сообщений и ID автора (который является chat_id)
        source_message_ids = group.get('source_message_ids')
        author_id = group.get('author_id')

        if source_message_ids and author_id:
            try:
                # Используем специальный метод API для удаления нескольких сообщений одним запросом.
                # Это предотвращает срабатывание flood-лимитов Telegram.
                await bot_instance.delete_messages(
                    chat_id=author_id,
                    message_ids=list(source_message_ids)
                )
            except TelegramBadRequest as e:
                # Игнорируем ошибки, если сообщения уже удалены или слишком старые.
                print(f"ℹ️ Не удалось выполнить пакетное удаление для media group {media_group_id}: {e}")
            except Exception as e:
                print(f"❌ Ошибка при пакетном удалении для media group {media_group_id}: {e}")
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---

        # Передаем извлеченные данные в обработчик.
        await process_complete_media_group(media_group_id, group, bot_instance)

    except asyncio.CancelledError:
        # Если таймер был отменен (пришло новое сообщение в группу) - это норма.
        pass
    except Exception as e:
        print(f"❌ Ошибка в complete_media_group_after_delay для {media_group_id}: {e}")
        # Подчищаем в случае непредвиденной ошибки
        current_media_groups.pop(media_group_id, None)
        media_group_timers.pop(media_group_id, None)

async def process_complete_media_group(media_group_id: str, group: dict, bot_instance: Bot):
    if not group or not group.get('media'):
        return

    # Помечаем ID как обработанный, чтобы игнорировать возможные дубликаты
    sent_media_groups.append(media_group_id)

    # --- НАЧАЛО ИЗМЕНЕНИЙ: "Нарезка" и последовательная отправка ---
    all_media = group.get('media', [])
    CHUNK_SIZE = 10  # Лимит API Telegram
    media_chunks = [all_media[i:i + CHUNK_SIZE] for i in range(0, len(all_media), CHUNK_SIZE)]

    for i, chunk in enumerate(media_chunks):
        if not chunk: continue

        user_id = group['author_id']
        board_id = group['board_id']
        b_data = board_data[board_id]
        
        # Первый чанк - основной пост, последующие - продолжение
        if i == 0:
            post_num = group['post_num']
            header = group['header']
            caption = group.get('caption')
            reply_to_post = group.get('reply_to_post')
        else:
            header, post_num = await format_header(board_id)
            caption = None
            reply_to_post = None

        content = {
            'type': 'media_group', 'header': header, 'media': chunk,
            'caption': caption, 'reply_to_post': reply_to_post
        }

        content = await _apply_mode_transformations(content, board_id)

        messages_storage[post_num] = {
            'author_id': user_id, 'timestamp': group['timestamp'], 'content': content,
            'reply_to': reply_to_post, 'board_id': board_id
        }

        reply_info = {}
        try:
            builder = MediaGroupBuilder()
            reply_to_message_id = None
            if reply_to_post:
                reply_info = post_to_messages.get(reply_to_post, {})
                reply_to_message_id = reply_info.get(user_id)
            
            header_html = f"<i>{header}</i>"
            
            full_caption = header_html
            # Добавляем основной текст только к первому чанку
            if i == 0 and content.get('caption'):
                full_caption += f"\n\n{content['caption']}"

            for idx, media in enumerate(chunk):
                caption_for_media = full_caption if idx == 0 else None
                builder.add(type=media['type'], media=media['file_id'], caption=caption_for_media, parse_mode="HTML" if caption_for_media else None)
            
            if builder.build():
                sent_messages = await bot_instance.send_media_group(
                    chat_id=user_id, media=builder.build(), reply_to_message_id=reply_to_message_id
                )
                if sent_messages:
                    messages_storage[post_num]['author_message_id'] = sent_messages[0].message_id
                    post_to_messages.setdefault(post_num, {})[user_id] = sent_messages[0].message_id
                    for msg in sent_messages: message_to_post[(user_id, msg.message_id)] = post_num
        
        except TelegramForbiddenError:
            b_data['users']['active'].discard(user_id)
            print(f"🚫 [{board_id}] Пользователь {user_id} заблокировал бота (media_group).")
        except Exception as e:
            print(f"⚠️ Ошибка отправки медиа-альбома #{post_num} автору {user_id}: {e}")
        
        recipients = b_data['users']['active'] - {user_id}
        if recipients and user_id in b_data['users']['active']:
            try:
                await message_queues[board_id].put({
                    'recipients': recipients, 'content': content, 'post_num': post_num,
                    'reply_info': reply_info, 'board_id': board_id
                })
            except Exception as e:
                print(f"❌ Критическая ошибка постановки в очередь медиагруппы #{post_num}: {e}")
                messages_storage.pop(post_num, None)
        
        if len(media_chunks) > 1:
            await asyncio.sleep(1) # Небольшая задержка между отправкой частей альбома
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---
            
def apply_greentext_formatting(text: str) -> str:
    """
    Применяет форматирование 'Greentext' к строкам, начинающимся с '>'.
    Работает с уже HTML-экранированным текстом.
    """
    if not text:
        return text

    processed_lines = []
    lines = text.split('\n')
    for line in lines:
        # Проверяем, начинается ли строка (без ведущих пробелов) с HTML-сущности '>'
        if line.lstrip().startswith('>'):
            # Оборачиваем всю строку в тег <code> для моноширинного зеленого текста
            processed_lines.append(f"<code>{line}</code>")
        else:
            processed_lines.append(line)
            
    return '\n'.join(processed_lines)


@dp.message()
async def handle_message(message: Message):
    user_id = message.from_user.id
    
    # 1. Определяем доску, с которой пришло сообщение
    board_id = get_board_id(message)
    if not board_id:
        return # Не обрабатываем сообщения от незарегистрированных ботов
    
    # 2. Получаем срез данных для текущей доски
    b_data = board_data[board_id]

    # 3. Проверка shadow-мута на конкретной доске.
    is_shadow_muted = (user_id in b_data['shadow_mutes'] and 
                       b_data['shadow_mutes'][user_id] > datetime.now(UTC))

    if is_shadow_muted:
        try:
            await message.delete()
        except TelegramBadRequest:
            pass # Сообщение уже удалено или не может быть удалено, игнорируем
    
    if not message.text and not message.caption and not message.content_type:
        if not is_shadow_muted:
            try:
                await message.delete()
            except TelegramBadRequest:
                pass # Сообщение уже удалено или не может быть удалено, игнорируем
        return
        
    if message.media_group_id:
        return
        
    try:
        # 4. Проверка обычного мута
        until = b_data['mutes'].get(user_id)
        if until and until > datetime.now(UTC):
            left = until - datetime.now(UTC)
            minutes = int(left.total_seconds() // 60)
            seconds = int(left.total_seconds() % 60)
            try:
                await message.delete()
                await message.bot.send_message(
                    user_id, 
                    f"🔇 Эй пидор, ты в муте на доске {BOARD_CONFIG[board_id]['name']} ещё {minutes}м {seconds}с\nСпамишь дальше - получишь бан",
                    parse_mode="HTML"
                )
            except: pass
            return
        elif until:
            b_data['mutes'].pop(user_id, None)

        # 5. Добавление/проверка пользователя
        if user_id not in b_data['users']['active']:
            b_data['users']['active'].add(user_id)
            print(f"✅ [{board_id}] Добавлен новый пользователь: ID {user_id}")

        if user_id in b_data['users']['banned']:
            if not is_shadow_muted:
                try:
                    await message.delete()
                except TelegramBadRequest:
                    pass # Сообщение уже удалено или не может быть удалено, игнорируем
            return

        # 6. Спам-проверка
        spam_check = await check_spam(user_id, message, board_id)
        if not spam_check:
            if not is_shadow_muted:
                try:
                    await message.delete()
                except TelegramBadRequest:
                    pass # Сообщение уже удалено или не может быть удалено, игнорируем
            msg_type = message.content_type
            if message.content_type in ['photo', 'video', 'document'] and message.caption:
                msg_type = 'text'
            await apply_penalty(message.bot, user_id, msg_type, board_id)
            return
        
        # 7. Получение информации об ответе
        recipients = b_data['users']['active'] - {user_id}
        reply_to_post, reply_info = None, {}
        if message.reply_to_message:
            lookup_key = (user_id, message.reply_to_message.message_id)
            reply_to_post = message_to_post.get(lookup_key)
            if reply_to_post and reply_to_post in post_to_messages:
                reply_info = post_to_messages[reply_to_post]
            else:
                reply_to_post = None

        # 8. Формирование заголовка и удаление исходного сообщения
        header, current_post_num = await format_header(board_id)
        if not is_shadow_muted:
            try:
                await message.delete()
            except TelegramBadRequest:
                pass # Сообщение уже удалено или не может быть удалено, игнорируем

        # 9. Формирование СЫРОГО словаря `content`
        content = {'type': message.content_type, 'header': header, 'reply_to_post': reply_to_post}
        
        # --- НАЧАЛО ИЗМЕНЕНИЙ ---
        
        if message.content_type == 'text':
            text_content = message.html_text if message.entities else escape_html(message.text)
            
            # Применяем Greentext форматирование
            text_content = apply_greentext_formatting(text_content)

            if b_data['anime_mode'] and random.random() < 0.41:
                anime_img_url = await get_random_anime_image()
                if anime_img_url:
                    content.update({'type': 'photo', 'caption': text_content, 'image_url': anime_img_url})
                else: 
                    content.update({'text': text_content})
            else: 
                content.update({'text': text_content})
                
        elif message.content_type in ['photo', 'video', 'animation', 'document', 'audio']:
            file_id = getattr(message, message.content_type, [])
            if isinstance(file_id, list): file_id = file_id[-1]
            
            # Экранируем и применяем Greentext к подписи
            caption_content = escape_html(message.caption or "")
            caption_content = apply_greentext_formatting(caption_content)
            
            content.update({'file_id': file_id.file_id, 'caption': caption_content})
            
        elif message.content_type in ['sticker', 'voice', 'video_note']:
            file_id = getattr(message, message.content_type)
            content['file_id'] = file_id.file_id

        # --- КОНЕЦ ИЗМЕНЕНИЙ ---

        # Применяем трансформации режимов ДО отправки автору и ДО постановки в очередь
        content = await _apply_mode_transformations(content, board_id)

        # 10. Сохранение в хранилище
        messages_storage[current_post_num] = {
            'author_id': user_id, 'timestamp': datetime.now(UTC), 'content': content,
            'reply_to': reply_to_post, 'author_message_id': None, 'board_id': board_id
        }

        # 11. Отправка сообщения автору (уже с трансформированным текстом)
        reply_to_message_id = reply_info.get(user_id)
        sent_to_author = None
        header_text = f"<i>{header}</i>"
        reply_text = f">>{reply_to_post}\n" if reply_to_post else ""

        try:
            if content.get('type') == 'text':
                full_text = f"{header_text}\n\n{reply_text}{content['text']}"
                sent_to_author = await message.bot.send_message(user_id, full_text, reply_to_message_id=reply_to_message_id, parse_mode="HTML")
            
            elif content.get('type') in ['photo', 'video', 'animation', 'document', 'audio']:
                caption_for_author = header_text
                final_caption = content.get('caption', '')
                if reply_text or final_caption:
                    caption_for_author += f"\n\n{reply_text}{final_caption}"
                
                media_to_send = content.get('image_url') or content.get('file_id')
                
                if content['type'] == 'photo':
                    sent_to_author = await message.bot.send_photo(user_id, media_to_send, caption=caption_for_author, reply_to_message_id=reply_to_message_id, parse_mode="HTML")
                elif content['type'] == 'video':
                    sent_to_author = await message.bot.send_video(user_id, content['file_id'], caption=caption_for_author, reply_to_message_id=reply_to_message_id, parse_mode="HTML")
                elif content['type'] == 'animation':
                    sent_to_author = await message.bot.send_animation(user_id, content['file_id'], caption=caption_for_author, reply_to_message_id=reply_to_message_id, parse_mode="HTML")
                elif content['type'] == 'document':
                    sent_to_author = await message.bot.send_document(user_id, content['file_id'], caption=caption_for_author, reply_to_message_id=reply_to_message_id, parse_mode="HTML")
                elif content['type'] == 'audio':
                    sent_to_author = await message.bot.send_audio(user_id, content['file_id'], caption=caption_for_author, reply_to_message_id=reply_to_message_id, parse_mode="HTML")

            elif content['type'] == 'sticker':
                sent_to_author = await message.bot.send_sticker(user_id, content['file_id'], reply_to_message_id=reply_to_message_id)
            elif content['type'] == 'voice':
                 sent_to_author = await message.bot.send_voice(user_id, content['file_id'], caption=f"<i>{header}</i>\n\n{reply_text}", reply_to_message_id=reply_to_message_id, parse_mode="HTML")
            elif content['type'] == 'video_note':
                sent_to_author = await message.bot.send_video_note(user_id, content['file_id'], reply_to_message_id=reply_to_message_id)

            if sent_to_author:
                messages_storage[current_post_num]['author_message_id'] = sent_to_author.message_id
                post_to_messages.setdefault(current_post_num, {})[user_id] = sent_to_author.message_id
                message_to_post[(user_id, sent_to_author.message_id)] = current_post_num

            if not is_shadow_muted and recipients:
                await message_queues[board_id].put({
                    'recipients': recipients, 'content': content, 'post_num': current_post_num,
                    'reply_info': reply_info if reply_info else None, 'board_id': board_id
                })

        except TelegramForbiddenError:
            # Пользователь заблокировал бота, удаляем его из активных
            b_data['users']['active'].discard(user_id)
            print(f"🚫 [{board_id}] Пользователь {user_id} заблокировал бота, удален из активных (из handle_message).")
        except Exception as e:
            print(f"Ошибка при отправке или постановке в очередь: {e}")
            messages_storage.pop(current_post_num, None)

    except Exception as e:
        print(f"Критическая ошибка в handle_message: {e}")
        
async def start_background_tasks(bots: dict[str, Bot]):
    """Поднимаем все фоновые корутины ОДИН раз за весь runtime"""
    # --- ИЗМЕНЕНИЕ ЗДЕСЬ: Локальный импорт для разрыва цикла ---
    from help_broadcaster import help_broadcaster
    
    tasks = [
        asyncio.create_task(auto_backup()),
        asyncio.create_task(message_broadcaster(bots)),
        asyncio.create_task(conan_roaster(
            state, messages_storage, post_to_messages, message_to_post,
            message_queues, format_header, board_data
        )),
        asyncio.create_task(motivation_broadcaster()),
        asyncio.create_task(auto_memory_cleaner()),
        asyncio.create_task(help_broadcaster()),
        asyncio.create_task(board_statistics_broadcaster()),
        asyncio.create_task(ghost_poster( # <--- ОБНОВИТЕ ЭТОТ БЛОК
            last_messages,
            message_queues,
            messages_storage,
            post_to_messages, # <--- ДОБАВЬТЕ ЭТОТ АРГУМЕНТ
            format_header,
            board_data,
            BOARDS
        )),
        # asyncio.create_task(dvach_thread_poster()), ПОКА НЕ НАДО
    ]
    print(f"✓ Background tasks started: {len(tasks)}")
    return tasks


async def supervisor():
    lock_file = "bot.lock"
    if os.path.exists(lock_file):
        print("⛔ Bot already running! Exiting...")
        sys.exit(1)
    
    with open(lock_file, "w") as f:
        f.write(str(os.getpid()))
    
    try:
        global is_shutting_down
        loop = asyncio.get_running_loop()

        restore_backup_on_start()
        load_state()

        bots = {}
        # --- НАЧАЛО ИЗМЕНЕНИЙ: Обновление инициализации Bot ---
        # Создаем объект с настройками по умолчанию один раз
        default_properties = DefaultBotProperties(parse_mode="HTML")
        
        for board_id, config in BOARD_CONFIG.items():
            token = config.get("token")
            if token:
                # Используем новый синтаксис с default=...
                bots[board_id] = Bot(token=token, default=default_properties)
            else:
                print(f"⚠️ Токен для доски '{board_id}' не найден, пропуск.")
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---
        
        if not bots:
            print("❌ Не найдено ни одного токена бота. Завершение работы.")
            return

        print(f"✅ Инициализировано {len(bots)} ботов: {list(bots.keys())}")
        
        # Обработчики сигналов теперь определяются ПОСЛЕ создания `bots` и передают его в `graceful_shutdown`
        bots_list = list(bots.values())
        if hasattr(signal, 'SIGTERM'):
            loop.add_signal_handler(signal.SIGTERM, lambda: asyncio.create_task(graceful_shutdown(bots_list)))
        if hasattr(signal, 'SIGINT'):
            loop.add_signal_handler(signal.SIGINT, lambda: asyncio.create_task(graceful_shutdown(bots_list)))
        
        await setup_pinned_messages(bots)
        healthcheck_site = await start_healthcheck()
        background_tasks = await start_background_tasks(bots)

        print("🚀 Запускаем polling для всех ботов...")
        await dp.start_polling(*bots.values(), skip_updates=True)

    except Exception as e:
        print(f"🔥 Critical error in supervisor: {e}")
    finally:
        if not is_shutting_down:
             await graceful_shutdown(list(bots.values()))
        if os.path.exists(lock_file):
            os.remove(lock_file)
            
if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(supervisor())
