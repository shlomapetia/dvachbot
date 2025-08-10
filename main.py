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
from zaputin_mode import zaputin_transform, PATRIOTIC_PHRASES 
from deanonymizer import DEANON_SURNAMES, DEANON_CITIES, DEANON_PROFESSIONS, DEANON_FETISHES, DEANON_DETAILS, generate_deanon_info
from help_text import HELP_TEXT, HELP_TEXT_EN

# ========== Глобальные настройки досок ==========

BOARD_CONFIG = {
    'b': {
        "name": "/b/",
        "description": "БРЕД - основная доска",
        "description_en": "RANDOM -",
        "username": "@dvach_chatbot",
        "token": os.getenv("BOT_TOKEN"),
        "admins": {int(x) for x in os.getenv("ADMINS", "").split(",") if x}
    },
    'po': {
        "name": "/po/",
        "description": "ПОЛИТАЧ - (срачи, политика)",
        "description_en": "POLITICS  -",
        "username": "@dvach_po_chatbot",
        "token": os.getenv("PO_BOT_TOKEN"),
        "admins": {int(x) for x in os.getenv("PO_ADMINS", "").split(",") if x}
    },
    'a': {
        "name": "/a/",
        "description": "АНИМЕ - (манга, Япония, хентай)",
        "description_en": "ANIME (🇯🇵, hentai, manga)",
        "username": "@dvach_a_chatbot",
        "token": os.getenv("A_BOT_TOKEN"),
        "admins": {int(x) for x in os.getenv("A_ADMINS", "").split(",") if x}
    },
    'sex': {
        "name": "/sex/",
        "description": "СЕКСАЧ - (отношения, секс, тян, еот, блекпилл)",
        "description_en": "SEX (relationships, sex, blackpill)",
        "username": "@dvach_sex_chatbot",
        "token": os.getenv("SEX_BOT_TOKEN"),
        "admins": {int(x) for x in os.getenv("SEX_ADMINS", "").split(",") if x}
    },
    'vg': {
        "name": "/vg/",
        "description": "ВИДЕОИГРЫ - (ПК, игры, хобби)",
        "description_en": "VIDEO GAMES (🎮, hobbies)",
        "username": "@dvach_vg_chatbot",
        "token": os.getenv("VG_BOT_TOKEN"),
        "admins": {int(x) for x in os.getenv("VG_ADMINS", "").split(",") if x}
    },
    'int': {
        "name": "/int/",
        "description": "INTERNATIONAL (🇬🇧🇺🇸🇨🇳🇮🇳🇪🇺)",
        "description_en": "INTERNATIONAL (🇬🇧🇺🇸🇨🇳🇮🇳🇪🇺)",
        "username": "@tgchan_chatbot",
        "token": os.getenv("INT_BOT_TOKEN"),
        "admins": {int(x) for x in os.getenv("INT_ADMINS", "").split(",") if x}
    },
    'test': {
        "name": "/test/",
        "description": "Testground",
        "description_en": "Testground",
        "username": "@tgchan_testbot", # ЗАМЕНИТЕ НА ЮЗЕРНЕЙМ ВАШЕГО БОТА
        "token": os.getenv("TEST_BOT_TOKEN"),
        "admins": {int(x) for x in os.getenv("TEST_ADMINS", "").split(",") if x}
    }
}


# Извлекаем список ID досок для удобства
BOARDS = list(BOARD_CONFIG.keys())

# Очереди сообщений для каждой доски
message_queues = {board: asyncio.Queue(maxsize=9000) for board in BOARDS}

# ========== Глобальные переменные и настройки ==========
is_shutting_down = False
git_executor = ThreadPoolExecutor(max_workers=1)
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
    # --- Отслеживание активности для очистки памяти ---
    'last_activity': {},
})

# ========== Rate Limiter для уведомлений о реакциях (на пользователя) ==========
AUTHOR_NOTIFY_LIMIT_PER_MINUTE = 4
author_reaction_notify_tracker = defaultdict(lambda: deque(maxlen=AUTHOR_NOTIFY_LIMIT_PER_MINUTE))
author_reaction_notify_lock = asyncio.Lock()
# ========== Debounce и управление задачами для редактирования постов ==========
pending_edit_tasks = {}  # Словарь для хранения активных задач редактирования {post_num: asyncio.Task}
pending_edit_lock = asyncio.Lock()

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
last_messages = deque(maxlen=3) # Используется для генерации сообщений, можно оставить общим
last_activity_time = datetime.now()
sent_media_groups = deque(maxlen=1000)
current_media_groups = {}
media_group_timers = {}

# Отключаем стандартную обработку сигналов в aiogram
os.environ["AIORGRAM_DISABLE_SIGNAL_HANDLERS"] = "1"

# Глобальные переменные для cooldown /deanon
DEANON_COOLDOWN = 120  # 2 минуты
last_deanon_time = 0
deanon_lock = asyncio.Lock()

# Фразы для cooldown
DEANON_COOLDOWN_PHRASES = [
    "Эй гандон, деанонеры заняты! Подожди минутку.",
    "Слишком часто, пидорас! Подожди хотя бы минуту.",
    "Не спеши, еблан! Деанон раз в 2 минуты.",
    "Подожди, уебок! Деанонеры перегружены.",
    "Абу сосет хуй. Подожди, пидор.",
    "Эй еблан! Подожди 060 секунд.",
    "Терпение, анон!",
    "Слишком много запросов!",
    "Деанон-боты отдыхают. Подожди .",
    "Заебали уже! Подожди 300 секунд, гандон."
]

SPAM_RULES = {
    'text': {
        'max_repeats': 5,  # Макс одинаковых текстов подряд
        'min_length': 2,  # Минимальная длина текста
        'window_sec': 15,  # Окно для проверки (сек)
        'max_per_window': 6,  # Макс сообщений в окне
        'penalty': [60, 300, 600]  # Шкала наказаний: [1 мин, 5мин, 10 мин]
    },
    'sticker': {
        'max_repeats': 3, # <-- ДОБАВЛЕНО
        'max_per_window': 6,  # 6 стикеров за 18 сек
        'window_sec': 18,
        'penalty': [60, 600, 900]  # 1мин, 10мин, 15 мин
    },
    'animation': {  # Гифки
        'max_repeats': 3, # <-- ДОБАВЛЕНО
        'max_per_window': 5,  # 5 гифки за 24 сек
        'window_sec': 24,
        'penalty': [60, 600, 900]  # 1мин, 10мин, 15 мин
    }
}



# Хранит информацию о текущих медиа-группах: media_group_id -> данные
current_media_groups = {}
media_group_timers = {}
user_spam_locks = defaultdict(asyncio.Lock)

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
    """Получаем deque для юзера на конкретной доске. Очистка теперь централизована в auto_memory_cleaner."""
    last_user_msgs_for_board = board_data[board_id]['last_user_msgs']
    
    if user_id not in last_user_msgs_for_board:
        last_user_msgs_for_board[user_id] = deque(maxlen=10)

    return last_user_msgs_for_board[user_id]

# Конфиг
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
REPLY_CACHE = 5900  # сколько постов держать в кэше для каждой доски
REPLY_FILE = "reply_cache.json"  # отдельный файл для reply
MAX_MESSAGES_IN_MEMORY = 5900  # храним только последние 5000 постов в общей памяти


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

MOTIVATIONAL_MESSAGES_EN = [
    "The more anons, the more epic the threads",
    "One anon is no anon. Call your bros",
    "More anons = more lulz",
    "Your friend still using Telegram like a normie? Fix it",
    "Every anon you invite = -1 normie in the world",
    "Wanna make this chat great? Invite new anons",
    "More anons means less chance the thread will die",
    "Bring a friend - get a double dose of lulz"
]

# ========== Фразы для уведомлений о реакциях ==========
REACTION_NOTIFY_PHRASES = {
    'ru': {
        'positive': [
            "👍 Анон двачует пост #{post_num}",
            "✅ Твой пост #{post_num} нравится анону!",
            "🔥 Отличный пост #{post_num}, анончик!",
            "🔥 Тгач ещё торт, ахуенный пост #{post_num}!",
            "❤️ Кто-то лайкнул твой пост #{post_num}",
            "❤️ Охуенно написал анон! Лайк на пост #{post_num}",
        ],
        'negative': [
            "👎 Анон саганул твой пост #{post_num}",
            "🤡 Анон поссал тебе на ебало за #{post_num}",
            "🟥⬇️ Сажа на пост #{post_num}",
            "🟥⬇️ SAGE SAGE SAGE пост #{post_num}",
            "💩 Анон репортнул пост #{post_num}",
            "⬇️ Дизлайк пост #{post_num}",            
            "🤢 Твой пост #{post_num} тупой высер (по мнению анона)",
        ],
        'neutral': [
            "🤔 Анон отреагировал на твой пост #{post_num}",
            "👀 На твой пост #{post_num} обратили внимание",
            "🧐 Твой пост #{post_num} вызвал интерес",
        ]
    },
    'en': {
        'positive': [
            "👍 Anon liked your post #{post_num}",
            "✅ Your post #{post_num} is fucking wholesome!",
            "🔥 Great post #{post_num}, nigger!",
            "❤️ Hey chud, someone liked your post #{post_num}",
        ],
        'negative': [
            "👎 Anon disliked your post #{post_num}",
            "🤡 Sage your post #{post_num}",
            "💩 Your post #{post_num} is piece of shit",
            "🤢 Anon says: go fuck with your dumb post #{post_num}",
        ],
        'neutral': [
            "🤔 Anon reacted to your post #{post_num}",
            "🤔 There is reaction on your post #{post_num}",
            "👀 Your post #{post_num} got some attention",
            "🧐 Someone is interested in your post #{post_num}",
        ]
    }
}

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

INVITE_TEXTS_EN = [
    "Anon, join TGACH @tgchan_chatbot\nYou can post anything anonymously here",
    "Got Telegram? Wanna post anonymously?\n@tgchan_chatbot - welcome aboard",
    "Tired of censorship? Want anonymity?\nWelcome to TGACH - @tgchan_chatbot - the real chan experience in Telegram",
    "@tgchan_chatbot - anonymous chat in Telegram\nNo registration, no SMS",
    "TGACH: @tgchan_chatbot\nSay what you think, no one will know who you are"
]

# ========== Классификация реакций ==========
POSITIVE_REACTIONS = {'👍', '❤', '🔥', '❤‍🔥', '😍', '😂', '🤣', '👌', '💯', '🙏', '🎉', '❤️', '♥️', '🥰', '🤩', '🤯'}
NEGATIVE_REACTIONS = {'👎', '💩', '🤮', '🤡', '🤢', '😡', '🤬', '🖕'}
# Все, что не входит в эти два списка, будет считаться нейтральным


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

def get_board_activity_last_hours(board_id: str, hours: int = 2) -> float:
    """Подсчитывает среднее количество постов в час для указанной доски за последние N часов."""
    if hours <= 0:
        return 0.0

    now = datetime.now(UTC)
    time_threshold = now - timedelta(hours=hours)
    post_count = 0

    # Проходим по всем сообщениям в памяти
    for post_data in messages_storage.values():
        # Проверяем, что пост принадлежит нужной доске и создан в рамках временного окна
        if post_data.get('board_id') == board_id and post_data.get('timestamp', now) > time_threshold:
            post_count += 1

    # Считаем среднюю активность (постов в час)
    activity = post_count / hours
    return activity
    
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
    print("💾 Попытка финального сохранения и бэкапа в GitHub (таймаут 50 секунд)...")
    try:
        await asyncio.wait_for(save_all_boards_and_backup(), timeout=50.0)
        print("✅ Финальный бэкап успешно завершен в рамках таймаута.")
    except asyncio.TimeoutError:
        print("⛔ КРИТИЧЕСКАЯ ОШИБКА: Финальный бэкап не успел выполниться за 50 секунд и был прерван!")
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
        
        # --- НАЧАЛО ИЗМЕНЕНИЙ ---
        # Удален цикл закрытия сессий, так как сессия теперь общая и
        # закрывается в блоке finally функции supervisor.
        print("✅ Сессии ботов будут закрыты централизованно.")
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---

    except Exception as e:
        print(f"Error during final shutdown procedures: {e}")

    # Отменяем оставшиеся задачи
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
    
    await asyncio.gather(*tasks, return_exceptions=True)
    print("✅ Все задачи остановлены, завершаем работу.")
    
async def auto_memory_cleaner():
    """Полная и честная очистка мусора каждые 10 минут."""
    cleanup_counter = 0
    while True:
        cleanup_counter += 1
        await asyncio.sleep(600)  # 10 минут

        # 1. Очистка старых постов
        if len(messages_storage) > MAX_MESSAGES_IN_MEMORY:
            to_delete_count = len(messages_storage) - MAX_MESSAGES_IN_MEMORY
            oldest_post_keys = sorted(messages_storage.keys())[:to_delete_count]
            posts_to_delete_set = set(oldest_post_keys)

            # УДАЛЯЕМ СВЯЗИ ИЗ message_to_post ДЛЯ СТАРЫХ ПОСТОВ
            removed_links = 0
            for key, post_num in list(message_to_post.items()):
                if post_num in posts_to_delete_set:
                    del message_to_post[key]
                    removed_links += 1
            
            for post_num in oldest_post_keys:
                messages_storage.pop(post_num, None)
                post_to_messages.pop(post_num, None)

            print(f"🧹 Очистка памяти: удалено {len(oldest_post_keys)} старых постов и {removed_links} связей в message_to_post.")

        # 2. ПЕРЕРАБОТАННАЯ очистка message_to_post
        actual_post_nums = set(messages_storage.keys())
        now_utc = datetime.now(UTC)
        
        # Собираем ВСЕХ активных пользователей по ВСЕМ доскам
        all_active_users = set()
        for board_id in BOARDS:
            b_data = board_data[board_id]
            # Добавляем пользователей, активных в последние 24 часа
            all_active_users.update([
                uid for uid, last_act in b_data.get('last_activity', {}).items()
                if (now_utc - last_act) < timedelta(hours=24)
            ])
        
        # ПОЛНАЯ ПЕРЕСБОРКА СЛОВАРЯ
        initial_count = len(message_to_post)
        valid_entries = {}
        
        for key, post_num in message_to_post.items():
            user_id, _ = key
            # Критерии сохранения связи:
            # 1. Пост существует в хранилище
            # 2. Пользователь активен
            if post_num in actual_post_nums and user_id in all_active_users:
                valid_entries[key] = post_num
        
        # Атомарная замена словаря
        message_to_post.clear()
        message_to_post.update(valid_entries)
        removed_count = initial_count - len(message_to_post)
        
        print(f"🧹 Очистка message_to_post: удалено {removed_count} связей (осталось {len(message_to_post)})")

        # 3. Очистка данных для каждой доски
        for board_id in BOARDS:
            b_data = board_data[board_id]

            if len(b_data['message_counter']) > 100:
                top_users = sorted(b_data['message_counter'].items(),
                                   key=lambda x: x[1],
                                   reverse=True)[:100]
                b_data['message_counter'] = defaultdict(int, top_users)
                print(f"🧹 [{board_id}] Очистка счетчика сообщений.")

            inactive_threshold = now_utc - timedelta(hours=12)
            potentially_inactive_users = {
                user_id for user_id, last_time in b_data.get('last_activity', {}).items()
                if last_time < inactive_threshold
            }
            users_with_active_mute = {
                uid for uid, expiry in b_data.get('mutes', {}).items() if expiry > now_utc
            }
            users_with_active_shadow_mute = {
                uid for uid, expiry in b_data.get('shadow_mutes', {}).items() if expiry > now_utc
            }
            users_to_purge = list(
                potentially_inactive_users - users_with_active_mute - users_with_active_shadow_mute
            )
            if users_to_purge:
                purged_count = len(users_to_purge)
                print(f"🧹 [{board_id}] Начинаю очистку данных для {purged_count} неактивных пользователей...")
                for user_id in users_to_purge:
                    b_data['last_activity'].pop(user_id, None)
                    b_data['last_texts'].pop(user_id, None)
                    b_data['last_stickers'].pop(user_id, None)
                    b_data['last_animations'].pop(user_id, None)
                    b_data['spam_violations'].pop(user_id, None)
                    b_data['spam_tracker'].pop(user_id, None)
                    b_data['last_user_msgs'].pop(user_id, None)
                print(f"🧹 [{board_id}] Очистка завершена. Удалены временные данные {purged_count} пользователей.")

            for user_id in list(b_data['last_user_msgs']):
                if user_id not in b_data['users']['active']:
                    b_data['last_user_msgs'].pop(user_id, None)
            for user_id in list(b_data['last_texts']):
                if user_id not in b_data['users']['active']:
                    b_data['last_texts'].pop(user_id, None)
            for user_id in list(b_data['last_stickers']):
                if user_id not in b_data['users']['active']:
                    b_data['last_stickers'].pop(user_id, None)
            for user_id in list(b_data['last_animations']):
                if user_id not in b_data['users']['active']:
                    b_data['last_animations'].pop(user_id, None)

            active_mutes = b_data.get('mutes', {})
            for user_id in list(active_mutes.keys()):
                if active_mutes[user_id] < now_utc:
                    active_mutes.pop(user_id, None)

            active_shadow_mutes = b_data.get('shadow_mutes', {})
            for user_id in list(active_shadow_mutes.keys()):
                if active_shadow_mutes[user_id] < now_utc:
                    active_shadow_mutes.pop(user_id, None)

            spam_tracker_board = b_data['spam_tracker']
            for user_id in list(spam_tracker_board.keys()):
                window_sec = SPAM_RULES.get('text', {}).get('window_sec', 15)
                window_start = now_utc - timedelta(seconds=window_sec)
                spam_tracker_board[user_id] = [
                    t for t in spam_tracker_board[user_id]
                    if t > window_start
                ]
                if not spam_tracker_board[user_id]:
                    del spam_tracker_board[user_id]

            inactive_threshold_spam = now_utc - timedelta(hours=24)
            spam_violations_board = b_data['spam_violations']
            users_to_purge_from_spam = [
                user_id for user_id, data in spam_violations_board.items()
                if data.get('last_reset', now_utc) < inactive_threshold_spam
            ]
            if users_to_purge_from_spam:
                for user_id in users_to_purge_from_spam:
                    spam_violations_board.pop(user_id, None)

        now_ts = time.time()
        tracker_inactive_threshold_sec = 24 * 3600  # 24 часа
        keys_to_delete_from_tracker = [
            author_id for author_id, timestamps in author_reaction_notify_tracker.items()
            if not timestamps or (now_ts - timestamps[-1] > tracker_inactive_threshold_sec)
        ]
        if keys_to_delete_from_tracker:
            for author_id in keys_to_delete_from_tracker:
                del author_reaction_notify_tracker[author_id]
            print(f"🧹 Очистка трекера реакций: удалено {len(keys_to_delete_from_tracker)} неактивных авторов.")

        gc.collect()

        print(f"🧹 DIAG: objects in messages_storage: {len(messages_storage)}")
        print(f"🧹 DIAG: objects in post_to_messages: {len(post_to_messages)}")
        print(f"🧹 DIAG: objects in message_to_post: {len(message_to_post)}")
        print(f"🧹 DIAG: objects in current_media_groups: {len(current_media_groups)}")
        print(f"🧹 DIAG: objects in media_group_timers: {len(media_group_timers)}")
        print(f"🧹 DIAG: objects in sent_media_groups: {len(sent_media_groups)}")


async def board_statistics_broadcaster():
    """Раз в час собирает общую статистику и рассылает на каждую доску."""
    await asyncio.sleep(300)

    while True:
        try:
            await asyncio.sleep(3600)

            now = datetime.now(UTC)
            hour_ago = now - timedelta(hours=1)
            
            posts_per_hour = defaultdict(int)
            for post_data in messages_storage.values():
                b_id = post_data.get('board_id')
                if b_id and post_data.get('timestamp', now) > hour_ago:
                    posts_per_hour[b_id] += 1
            
            # --- ИСПРАВЛЕНИЕ ЗДЕСЬ ---
            # Переносим формирование текста внутрь цикла рассылки
            for board_id in BOARDS:
                if board_id == 'test': # --- ДОБАВЛЕНО: Пропускаем рассылку на тестовую доску
                    continue

                activity = get_board_activity_last_hours(board_id, hours=2)
                if activity < 20:
                    print(f"ℹ️ [{board_id}] Пропуск отправки статистики, активность слишком низкая: {activity:.1f} п/ч (требуется > 20).")
                    continue

                b_data = board_data[board_id]
                recipients = b_data['users']['active'] - b_data['users']['banned']
                if not recipients:
                    continue

                # Формируем локализованный текст
                stats_lines = []
                # --- НАЧАЛО ИЗМЕНЕНИЙ ---
                for b_id_inner, config_inner in BOARD_CONFIG.items():
                    if b_id_inner == 'test': # Исключаем тестовую доску из списка
                        continue
                # --- КОНЕЦ ИЗМЕНЕНИЙ ---
                    hour_stat = posts_per_hour[b_id_inner]
                    total_stat = board_data[b_id_inner].get('board_post_count', 0)
                    
                    line_template = f"<b>{config_inner['name']}</b> - {hour_stat} pst/hr, total: {total_stat}" \
                                    if board_id == 'int' \
                                    else f"<b>{config_inner['name']}</b> - {hour_stat} пст/час, всего: {total_stat}"
                    stats_lines.append(line_template)
                
                header_text = "📊 Boards Statistics:\n" if board_id == 'int' else "📊 Статистика досок:\n"
                full_stats_text = header_text + "\n".join(stats_lines)
                header = "### Statistics ###" if board_id == 'int' else "### Статистика ###"

                _, post_num = await format_header(board_id)
                content = {"type": "text", "header": header, "text": full_stats_text, "is_system_message": True}
                
                messages_storage[post_num] = {'author_id': 0, 'timestamp': now, 'content': content, 'board_id': board_id}
                
                await message_queues[board_id].put({"recipients": recipients, "content": content, "post_num": post_num, "board_id": board_id})
                
                print(f"✅ [{board_id}] Статистика досок #{post_num} добавлена в очередь.")

        except Exception as e:
            print(f"❌ Ошибка в board_statistics_broadcaster: {e}")
            await asyncio.sleep(120)
            
async def setup_pinned_messages(bots: dict[str, Bot]):
    """Устанавливает или обновляет закрепленное сообщение для каждого бота."""
    
    for board_id, bot_instance in bots.items():
        b_data = board_data[board_id]
        
        # --- ИСПРАВЛЕНИЕ ЗДЕСЬ ---
        # Выбираем правильный текст помощи и генерируем список досок на нужном языке
        if board_id == 'int':
            base_help_text = HELP_TEXT_EN
            boards_header = "🌐 <b>All boards:</b>"
            # --- НАЧАЛО ИЗМЕНЕНИЙ ---
            board_links = "\n".join(
                f"<b>{config['name']}</b> {config['description_en']} - {config['username']}"
                for b_id, config in BOARD_CONFIG.items() if b_id != 'test'
            )
            # --- КОНЕЦ ИЗМЕНЕНИЙ ---
        else:
            base_help_text = HELP_TEXT
            boards_header = "🌐 <b>Все доски:</b>"
            # --- НАЧАЛО ИЗМЕНЕНИЙ ---
            board_links = "\n".join(
                f"<b>{config['name']}</b> {config['description']} - {config['username']}"
                for b_id, config in BOARD_CONFIG.items() if b_id != 'test'
            )
            # --- КОНЕЦ ИЗМЕНЕНИЙ ---
            
        # Собираем финальное сообщение
        full_help_text = (
            f"{base_help_text}\n\n"
            f"{boards_header}\n{board_links}"
        )
        
        # Сохраняем готовый текст для использования в /start и /help
        b_data['start_message_text'] = full_help_text
        
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
        content = msg.sticker.file_id # <-- ИЗМЕНЕНО: Сразу получаем ID
    elif msg.content_type == 'animation':
        msg_type = 'animation'
        content = msg.animation.file_id # <-- ИЗМЕНЕНО: Сразу получаем ID
    elif msg.content_type in ['photo', 'video', 'document'] and msg.caption:
        msg_type = 'text'
        content = msg.caption
    else:
        return True # Неизвестный тип для спам-фильтра

    rules = SPAM_RULES.get(msg_type)
    if not rules:
        return True

    now = datetime.now(UTC)
    violations = b_data['spam_violations'].setdefault(user_id, {'level': 0, 'last_reset': now})

    # Сброс уровня, если прошло больше 1 часа
    if (now - violations['last_reset']) > timedelta(hours=1):
        violations['level'] = 0
        violations['last_reset'] = now
    
    # --- НАЧАЛО РЕФАКТОРИНГА: Унифицированная проверка на повторы ---
    max_repeats = rules.get('max_repeats')
    if max_repeats and content:
        # Определяем, какую очередь использовать
        if msg_type == 'text':
            last_items_deque = b_data['last_texts'][user_id]
        elif msg_type == 'sticker':
            last_items_deque = b_data['last_stickers'][user_id]
        elif msg_type == 'animation':
            last_items_deque = b_data['last_animations'][user_id]
        else:
            last_items_deque = None

        if last_items_deque is not None:
            last_items_deque.append(content)
            
            # Проверка на N одинаковых подряд
            if len(last_items_deque) >= max_repeats:
                if len(set(last_items_deque)) == 1:
                    violations['level'] = min(violations['level'] + 1, len(rules['penalty']) - 1)
                    last_items_deque.clear() # Очищаем очередь после нарушения
                    return False
            
            # Проверка на чередование для текста (оставляем специфичной)
            if msg_type == 'text' and len(last_items_deque) == 4:
                if len(set(last_items_deque)) == 2:
                    contents = list(last_items_deque)
                    p1 = [contents[0], contents[1]] * 2
                    p2 = [contents[1], contents[0]] * 2
                    if contents == p1 or contents == p2:
                        violations['level'] = min(violations['level'] + 1, len(rules['penalty']) - 1)
                        last_items_deque.clear() # Очищаем очередь
                        return False
    # --- КОНЕЦ РЕФАКТОРИНГА ---

    # Проверка лимита по времени (без изменений)
    window_start = now - timedelta(seconds=rules['window_sec'])
    b_data['spam_tracker'][user_id] = [t for t in b_data['spam_tracker'][user_id] if t > window_start]
    b_data['spam_tracker'][user_id].append(now)

    if len(b_data['spam_tracker'][user_id]) >= rules['max_per_window']:
        violations['level'] = min(violations['level'] + 1, len(rules['penalty']) - 1)
        return False
        
    return True

async def apply_penalty(bot_instance: Bot, user_id: int, msg_type: str, board_id: str):
    """Применяет мут согласно текущему уровню нарушения с блокировкой"""
    async with user_spam_locks[user_id]:  # Блокировка для конкретного пользователя
        b_data = board_data[board_id]
        rules = SPAM_RULES.get(msg_type, {})
        if not rules:
            return
            
        violations_data = b_data['spam_violations'].get(user_id, {'level': 0, 'last_reset': datetime.now(UTC)})
        level = violations_data['level']
        
        # Проверяем, не был ли уже применен мут
        current_mute = b_data['mutes'].get(user_id)
        if current_mute and current_mute > datetime.now(UTC):
            return  # Мут уже активен, пропускаем
        
        level = min(level, len(rules.get('penalty', [])) - 1)
        mute_seconds = rules['penalty'][level] if rules.get('penalty') else 30
        
        # Применяем мут
        b_data['mutes'][user_id] = datetime.now(UTC) + timedelta(seconds=mute_seconds)
        
        violation_type = {'text': "текстовый спам", 'sticker': "спам стикерами", 'animation': "спам гифками"}.get(msg_type, "спам")
        
        # Логирование
        mute_duration = f"{mute_seconds} сек" if mute_seconds < 60 else f"{mute_seconds//60} мин"
        print(f"🚫 [{board_id}] Мут за спам: user {user_id}, тип: {violation_type}, уровень: {level+1}, длительность: {mute_duration}")
        
        try:
            # Форматируем строку времени для пользователя
            if mute_seconds < 60:
                time_str = f"{mute_seconds} сек"
            elif mute_seconds < 3600:
                time_str = f"{mute_seconds // 60} мин"
            else:
                time_str = f"{mute_seconds // 3600} час"
            
            # Формируем текст уведомления
            lang = 'en' if board_id == 'int' else 'ru'
            
            if lang == 'en':
                violation_type_en = {'text': "text spam", 'sticker': "sticker spam", 'animation': "gif spam"}.get(msg_type, "spam")
                phrases = [
                    "🚫 Hey faggot, you are muted for {time} for {violation} on the {board} board.\nKeep spamming - get banned.",
                    "🔇 Too much spam, buddy. Take a break for {time} on {board}.",
                    "🚨 Spam detected! You've been silenced for {time} for {violation} on {board}. Don't do it again.",
                    "🛑 Stop right there, criminal scum! You're muted for {time} on {board} for spamming."
                ]
                notification_text = random.choice(phrases).format(
                    time=time_str, 
                    violation=violation_type_en, 
                    board=BOARD_CONFIG[board_id]['name']
                )
            else:
                phrases = [
                    "🚫 Эй пидор, ты в муте на {time} за {violation} на доске {board}\nСпамишь дальше - получишь бан.",
                    "🔇 Ты заебал спамить. Отдохни {time} на доске {board}.",
                    "🚨 Обнаружен спам! Твоя пасть завалена на {time} за {violation} на доске {board}. Повторишь - получишь по жопе.",
                    "🛑 Стой, пидорас! Ты оштрафован на {time} молчания на доске {board} за свой высер."
                ]
                notification_text = random.choice(phrases).format(
                    time=time_str, 
                    violation=violation_type, 
                    board=BOARD_CONFIG[board_id]['name']
                )

            # Отправляем уведомление
            await bot_instance.send_message(user_id, notification_text, parse_mode="HTML")
            await send_moderation_notice(user_id, "mute", board_id, duration=time_str)
            
        except Exception as e:
            print(f"Ошибка отправки уведомления о муте: {e}")

async def format_header(board_id: str) -> Tuple[str, int]:
    """Асинхронное форматирование заголовка с блокировкой для безопасного инкремента счетчика постов."""
    async with post_counter_lock:
        state['post_counter'] += 1
        post_num = state['post_counter']
        
        board_data[board_id].setdefault('board_post_count', 0)
        board_data[board_id]['board_post_count'] += 1
    
    # --- БЛОК ДЛЯ /int/ ---
    if board_id == 'int':
        circle = ""
        rand_circle = random.random()
        if rand_circle < 0.003: circle = "🔴 "
        elif rand_circle < 0.006: circle = "🟢 "
        
        prefix = ""
        rand_prefix = random.random()
        if rand_prefix < 0.005: prefix = "### ADMIN ### "
        elif rand_prefix < 0.008: prefix = "Me - "
        elif rand_prefix < 0.01: prefix = "Faggot - "
        elif rand_prefix < 0.012: prefix = "### DEGENERATE ### "
        elif rand_prefix < 0.016: prefix = "Biden - "
        elif rand_prefix < 0.021: prefix = "EMPEROR CONAN - "
            
        header_text = f"{circle}{prefix}Post No.{post_num}"
        return header_text, post_num
    # --- КОНЕЦ БЛОКА ДЛЯ /int/ ---

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
    """Удаляет сообщения пользователя за период в пределах КОНКРЕТНОЙ доски"""
    try:
        time_threshold = datetime.now(UTC) - timedelta(minutes=time_period_minutes)
        posts_to_delete = []
        deleted_messages = 0

        # Итерируемся напрямую по .items(), избегая создания полной копии словаря в памяти.
        for post_num, post_data in list(messages_storage.items()):
            post_time = post_data.get('timestamp')
            if not post_time:
                continue

            if (post_data.get('author_id') == user_id and
                post_data.get('board_id') == board_id and
                post_time >= time_threshold):
                posts_to_delete.append(post_num)
        
        if not posts_to_delete:
            return 0

        # УДАЛЯЕМ СВЯЗИ ИЗ message_to_post ДЛЯ ЭТИХ ПОСТОВ
        for post_num in posts_to_delete:
            if post_num in post_to_messages:
                for uid, mid in list(post_to_messages[post_num].items()):
                    key = (uid, mid)
                    if key in message_to_post:
                        del message_to_post[key]
        
        # УДАЛЯЕМ ОСНОВНЫЕ ДАННЫЕ
        for post_num in posts_to_delete:
            post_to_messages.pop(post_num, None)
            messages_storage.pop(post_num, None)

        return len(posts_to_delete)
    except Exception as e:
        print(f"Ошибка в delete_user_posts: {e}")
        return 0
        
async def delete_single_post(post_num: int, bot_instance: Bot) -> int:
    """Удаляет один конкретный пост (и все его копии у пользователей)."""
    if post_num not in post_to_messages:
        return 0

    # УДАЛЯЕМ СВЯЗИ ИЗ message_to_post ДЛЯ ЭТОГО ПОСТА
    for uid, mid in list(post_to_messages[post_num].items()):
        key = (uid, mid)
        if key in message_to_post:
            del message_to_post[key]

    # УДАЛЯЕМ ОСНОВНЫЕ ДАННЫЕ
    deleted_count = 0
    # Собираем ВСЕ сообщения для удаления
    messages_to_delete = []
    for uid, mid in post_to_messages[post_num].items():
        messages_to_delete.append((uid, mid))

    # Удаляем каждое сообщение
    for (uid, mid) in messages_to_delete:
        try:
            await bot_instance.delete_message(uid, mid)
            deleted_count += 1
        except (TelegramBadRequest, TelegramForbiddenError):
            continue
        except Exception as e:
            print(f"Ошибка удаления {mid} у {uid}: {e}")

    # АТОМАРНАЯ ОЧИСТКА ВСЕХ СЛЕДОВ ПОСТА
    post_to_messages.pop(post_num, None)
    messages_storage.pop(post_num, None)

    return deleted_count
    
async def send_moderation_notice(user_id: int, action: str, board_id: str, duration: str = None, deleted_posts: int = 0):
    """Отправляет уведомление о модерационном действии в чат конкретной доски."""
    b_data = board_data[board_id]
    if not b_data['users']['active']:
        return

    _, post_num = await format_header(board_id)
    header = "### Админ ###"
    
    # --- НАЧАЛО ИЗМЕНЕНИЙ ---
    lang = 'en' if board_id == 'int' else 'ru'

    if action == "ban":
        if lang == 'en':
            ban_phrases = [
                f"🚨 A faggot has been banned for spam. RIP.",
                f"☠️ Another spammer bites the dust. Good riddance.",
                f"🔨 The ban hammer has spoken. A degenerate was removed.",
                f"✈️ Sent a spammer on a one-way trip to hell."
            ]
        else:
            ban_phrases = [
                f"🚨 Хуесос был забанен за спам. Помянем.",
                f"☠️ Мир стал чище, еще один спамер отлетел в бан.",
                f"🔨 Банхаммер опустился на голову очередного дегенерата.",
                f"✈️ Отправили спамера в увлекательное путешествие нахуй."
            ]
        text = random.choice(ban_phrases)

    elif action == "mute":
        if lang == 'en':
            mute_phrases = [
                f"🔇 A loudmouth has been muted for a while.",
                f"🤫 Someone's got a timeout. Let's enjoy the silence.",
                f"🤐 Put a sock in it! A user has been temporarily silenced.",
                f"⌛️ A faggot is in the penalty box for a bit."
            ]
        else:
            mute_phrases = [
                f"🔇 Пидораса замутили ненадолго.",
                f"🤫 Наслаждаемся тишиной, хуеглот временно не может писать.",
                f"🤐 Анон отправлен в угол подумать о своем поведении.",
                f"⌛️ Пидору выписали временный запрет на открытие рта."
            ]
        text = random.choice(mute_phrases)
    else:
        return
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---

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
    Централизованно применяет все трансформации режимов.
    """
    b_data = board_data[board_id]
    modified_content = content.copy()

    is_transform_mode_active = (
        b_data['anime_mode'] or b_data['slavaukraine_mode'] or
        b_data['zaputin_mode'] or b_data['suka_blyat_mode']
    )

    if not is_transform_mode_active:
        return modified_content # Если режимов нет, ничего не делаем

    # Если режим активен, принудительно очищаем HTML перед трансформацией
    if 'text' in modified_content and modified_content['text']:
        modified_content['text'] = clean_html_tags(modified_content['text'])
    if 'caption' in modified_content and modified_content['caption']:
        modified_content['caption'] = clean_html_tags(modified_content['caption'])

    # Теперь применяем трансформации к чистому тексту
    if b_data['anime_mode']:
        if 'text' in modified_content and modified_content['text']:
            modified_content['text'] = anime_transform(modified_content['text'])
        if 'caption' in modified_content and modified_content['caption']:
            modified_content['caption'] = anime_transform(modified_content['caption'])
        
        if modified_content.get('type') == 'text' and random.random() < 0.41:
            anime_img_url = await get_random_anime_image()
            if anime_img_url:
                text_content = modified_content.pop('text', '')
                modified_content.update({
                    'type': 'photo',
                    'caption': text_content,
                    'image_url': anime_img_url
                })

    elif b_data['slavaukraine_mode']:
        if 'text' in modified_content and modified_content['text']:
            modified_content['text'] = ukrainian_transform(modified_content['text'])
        if 'caption' in modified_content and modified_content['caption']:
            modified_content['caption'] = ukrainian_transform(modified_content['caption'])
            
    elif b_data['zaputin_mode']:
        if 'text' in modified_content and modified_content['text']:
            modified_content['text'] = zaputin_transform(modified_content['text'])
        if 'caption' in modified_content and modified_content['caption']:
            modified_content['caption'] = zaputin_transform(modified_content['caption'])
            
    elif b_data['suka_blyat_mode']:
        if 'text' in modified_content and modified_content['text']:
            words = modified_content['text'].split()
            for i in range(len(words)):
                if random.random() < 0.3: words[i] = random.choice(MAT_WORDS)
            modified_content['text'] = ' '.join(words)
        if 'caption' in modified_content and modified_content['caption']:
            caption = modified_content['caption']
            words = caption.split()
            for i in range(len(words)):
                if random.random() < 0.3: words[i] = random.choice(MAT_WORDS)
            modified_content['caption'] = ' '.join(words)
    
    return modified_content

async def _format_message_body(content: dict, user_id_for_context: int, post_num: int) -> str:
    """
    Формирует и форматирует тело сообщения (реакции, reply, greentext, (You)).
    Эта версия разделяет обработку ответа, реакций и основного текста.
    
    :param content: Словарь с данными поста ('reply_to_post', 'text', 'caption').
    :param user_id_for_context: ID пользователя, для которого форматируется сообщение.
    :param post_num: Номер поста для поиска реакций в messages_storage.
    :return: Готовая к отправке HTML-форматированная строка.
    """
    parts = []
    
    # 1. Формируем блок ответа (если он есть)
    reply_to_post = content.get('reply_to_post')
    if reply_to_post:
        original_author = messages_storage.get(reply_to_post, {}).get('author_id')
        you_marker = " (You)" if user_id_for_context == original_author else ""
        reply_line = f">>{reply_to_post}{you_marker}"
        formatted_reply_line = f"<code>{escape_html(reply_line)}</code>"
        parts.append(formatted_reply_line)
        
    # --- НАЧАЛО ИЗМЕНЕНИЙ ---
    # 2. Формируем блок с реакциями
    post_data = messages_storage.get(post_num, {})
    reactions_data = post_data.get('reactions')
    
    if reactions_data:
        reaction_lines = []
        # --- НОВАЯ ЛОГИКА для структуры {'users': {uid: [emojis]}} ---
        if 'users' in reactions_data and isinstance(reactions_data.get('users'), dict):
            all_emojis = [emoji for user_emojis in reactions_data['users'].values() for emoji in user_emojis]
            
            # Собираем и сортируем эмодзи по категориям для консистентного отображения
            positive_display = sorted([e for e in all_emojis if e in POSITIVE_REACTIONS])
            neutral_display = sorted([e for e in all_emojis if e not in POSITIVE_REACTIONS and e not in NEGATIVE_REACTIONS])
            negative_display = sorted([e for e in all_emojis if e in NEGATIVE_REACTIONS])
            
            if positive_display: reaction_lines.append("".join(positive_display))
            if neutral_display: reaction_lines.append("".join(neutral_display))
            if negative_display: reaction_lines.append("".join(negative_display))

        # --- СТАРАЯ ЛОГИКА для обратной совместимости ---
        elif 'positive' in reactions_data or 'negative' in reactions_data:
            if reactions_data.get('positive'): reaction_lines.append("".join(reactions_data['positive']))
            if reactions_data.get('neutral'): reaction_lines.append("".join(reactions_data['neutral']))
            if reactions_data.get('negative'): reaction_lines.append("".join(reactions_data['negative']))
        
        if reaction_lines:
            reactions_block = "\n".join(reaction_lines)
            parts.append(reactions_block)
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---

    # 3. Формируем и форматируем основной текст сообщения
    main_text_raw = content.get('text') or content.get('caption') or ''
    if main_text_raw:
        text_with_you = add_you_to_my_posts(main_text_raw, user_id_for_context)
        formatted_main_text = apply_greentext_formatting(text_with_you)
        parts.append(formatted_main_text)
        
    # 4. Объединяем все части. Используем два переноса строки для разделения блоков.
    return '\n\n'.join(filter(None, parts))

async def send_message_to_users(
    bot_instance: Bot,
    recipients: set[int],
    content: dict,
    reply_info: dict | None = None,
) -> list:
    """Оптимизированная рассылка сообщений пользователям с уведомлением об ограничениях."""
    if not recipients or not content or 'type' not in content:
        return []

    board_id = next((b_id for b_id, config in BOARD_CONFIG.items() if config['token'] == bot_instance.token), None)
    if not board_id:
        print(f"КРИТИЧЕСКАЯ ОШИБКА: Не удалось найти доску для бота с токеном ...{bot_instance.token[-6:]}")
        return []

    b_data = board_data[board_id]
    modified_content = content.copy()

    # Добавление фраз для режимов
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
        head, formatted_body, full_text = "", "", ""
        try:
            ct_raw = modified_content["type"]
            ct = ct_raw.value if hasattr(ct_raw, 'value') else ct_raw
            
            header_text = modified_content['header']
            head = f"<i>{escape_html(header_text)}</i>"

            reply_to_post = modified_content.get('reply_to_post')
            original_author = messages_storage.get(reply_to_post, {}).get('author_id') if reply_to_post else None
            if uid == original_author:
                head = head.replace("Пост", "🔴 Пост").replace("Post", "🔴 Post")
            
            post_num = modified_content.get('post_num')
            formatted_body = await _format_message_body(modified_content, uid, post_num)
            full_text = f"{head}\n\n{formatted_body}" if formatted_body else head

            if ct == "media_group":
                if not modified_content.get('media'): return None
                builder = MediaGroupBuilder()
                for idx, media in enumerate(modified_content['media']):
                    caption = full_text if idx == 0 else None
                    builder.add(type=media['type'], media=media['file_id'], caption=caption, parse_mode="HTML" if caption else None)
                return await bot_instance.send_media_group(chat_id=uid, media=builder.build(), reply_to_message_id=reply_to)
            
            method_name = f"send_{ct}"
            if ct == 'text': method_name = 'send_message'
            send_method = getattr(bot_instance, method_name)
            
            kwargs = {'chat_id': uid, 'reply_to_message_id': reply_to}
            
            if ct == 'text':
                kwargs.update(text=full_text, parse_mode="HTML")
            
            elif ct in ['photo', 'video', 'animation', 'document', 'audio', 'voice', 'video_note']:
                if len(full_text) > 1024: full_text = full_text[:1021] + "..."
                kwargs.update(caption=full_text, parse_mode="HTML")
                
                file_source = modified_content.get('image_url') or modified_content.get("file_id")
                kwargs[ct] = file_source
            
            elif ct == 'sticker':
                kwargs[ct] = modified_content["file_id"]
            else:
                print(f"❌ Неизвестный тип контента для отправки: {ct}")
                return None
            
            return await send_method(**kwargs)

        except TelegramRetryAfter as e:
            await asyncio.sleep(e.retry_after + 1)
            return await really_send(uid, reply_to)
        except TelegramForbiddenError:
            blocked_users.add(uid)
            return None
        except TelegramBadRequest as e:
            # --- НАЧАЛО ИЗМЕНЕНИЙ: Замена переотправки на уведомление ---
            lang = 'en' if board_id == 'int' else 'ru'
            current_type = modified_content.get("type")

            placeholder_text = None
            if "VOICE_MESSAGES_FORBIDDEN" in e.message and current_type == "voice":
                placeholder_text = " VOICE MESSAGE " if lang == 'en' else " ГОЛОСОВОЕ СООБЩЕНИЕ "
            elif "VIDEO_MESSAGES_FORBIDDEN" in e.message and current_type == "video_note":
                placeholder_text = " VIDEO MESSAGE " if lang == 'en' else " ВИДЕО СООБЩЕНИЕ (кружок) "

            if placeholder_text:
                print(f"ℹ️ Пользователь {uid} запретил получение {current_type}. Отправляю плейсхолдер...")
                try:
                    error_info_ru = (
                        "<b>[ Тут должно было быть ГС или кружок, но...]</b>\n\n"
                        f"У вас в настройках приватности телеграм запрещено получение {placeholder_text}"
                    )
                    error_info_en = (
                        "<b>[ 🚫 Blocked Content. There would be VM or video message but... ]</b>\n\n"
                        f"You have blocked receiving {placeholder_text} in your Telegram privacy settings."
                    )
                    
                    error_info = error_info_en if lang == 'en' else error_info_ru
                    
                    # Отправляем текстовое сообщение с тем же заголовком и информацией об ответе
                    # Вместо тела сообщения - информация об ошибке
                    final_text = f"{head}\n\n{error_info}"
                    
                    return await bot_instance.send_message(
                        chat_id=uid, text=final_text, parse_mode="HTML", reply_to_message_id=reply_to
                    )
                except Exception as placeholder_e:
                    print(f"❌ Не удалось отправить плейсхолдер для {uid}: {placeholder_e}")
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
            reply_to = None
            if reply_info and isinstance(reply_info, dict):
                reply_to = reply_info.get(uid)
            if reply_to is None and content.get("reply_to_post"):
                original_post = content["reply_to_post"]
                if original_post in post_to_messages and isinstance(post_to_messages[original_post], dict):
                    author_mid = post_to_messages[original_post].get(uid)
                    if author_mid: reply_to = author_mid
            
            result = await really_send(uid, reply_to)
            return (uid, result)

    tasks = [send_with_semaphore(uid) for uid in active_recipients]
    results = await asyncio.gather(*tasks)

    if content.get('post_num'):
        post_num = content['post_num']
        for uid, msg in results:
            if not msg: continue
            # Важно: Сохраняем в `message_to_post` даже плейсхолдер, чтобы на него можно было ставить реакции
            messages_to_save = msg if isinstance(msg, list) else [msg]
            for m in messages_to_save:
                post_to_messages.setdefault(post_num, {})[uid] = m.message_id
                message_to_post[(uid, m.message_id)] = post_num

    if blocked_users:
        for uid in blocked_users:
            if uid in b_data['users']['active']:
                b_data['users']['active'].discard(uid)
                print(f"🚫 [{board_id}] Пользователь {uid} заблокировал бота, удален из активных")

    return results

async def edit_post_for_all_recipients(post_num: int, bot_instance: Bot):
    """
    Находит все отправленные копии поста и редактирует их, добавляя обновленный
    список реакций.
    """
    post_data = messages_storage.get(post_num)
    message_copies = post_to_messages.get(post_num)

    if not post_data or not message_copies:
        return # Пост или его копии не найдены в памяти

    content = post_data.get('content', {})
    content_type = content.get('type')
    
    # Редактировать можно только текстовые сообщения или сообщения с подписью
    can_be_edited = content_type in ['text', 'photo', 'video', 'animation', 'document', 'audio']
    if not can_be_edited:
        return
        
    board_id = post_data.get('board_id')
    if not board_id:
        return # Не удалось определить доску

    async def _edit_one(user_id: int, message_id: int):
        """Внутренняя корутина для редактирования одного сообщения."""
        try:
            # 1. Формируем заголовок (с учетом подсветки для автора ответа)
            header_text = content.get('header', '')
            head = f"<i>{escape_html(header_text)}</i>"
            
            reply_to_post = content.get('reply_to_post')
            original_author = messages_storage.get(reply_to_post, {}).get('author_id') if reply_to_post else None

            if user_id == original_author:
                if board_id == 'int':
                    head = head.replace("Post", "🔴 Post")
                else:
                    head = head.replace("Пост", "🔴 Пост")

            # 2. Формируем тело сообщения с помощью обновленной функции
            formatted_body = await _format_message_body(content, user_id, post_num)
            
            # 3. Собираем итоговый текст
            full_text = f"{head}\n\n{formatted_body}" if formatted_body else head
            if len(full_text) > 4096: # Ограничение Telegram на длину сообщения
                full_text = full_text[:4093] + "..."
            
            # 4. Выбираем и вызываем нужный метод редактирования
            if content_type == 'text':
                await bot_instance.edit_message_text(
                    text=full_text,
                    chat_id=user_id,
                    message_id=message_id,
                    parse_mode="HTML"
                )
            else: # photo, video, etc.
                if len(full_text) > 1024: # Ограничение на длину подписи
                    full_text = full_text[:1021] + "..."
                await bot_instance.edit_message_caption(
                    caption=full_text,
                    chat_id=user_id,
                    message_id=message_id,
                    parse_mode="HTML"
                )
        except TelegramBadRequest as e:
            # Игнорируем ошибки, если сообщение не изменилось или не найдено
            if "message is not modified" not in e.message and "message to edit not found" not in e.message:
                 print(f"⚠️ Ошибка (BadRequest) при редактировании поста #{post_num} для {user_id}: {e}")
        except TelegramForbiddenError:
            board_data[board_id]['users']['active'].discard(user_id)
            print(f"🚫 [{board_id}] Пользователь {user_id} заблокировал бота, удален из активных (при редактировании).")
        except Exception as e:
            print(f"❌ Неизвестная ошибка при редактировании поста #{post_num} для {user_id}: {e}")

    # Запускаем редактирование для всех получателей параллельно
    tasks = [_edit_one(uid, mid) for uid, mid in message_copies.items()]
    await asyncio.gather(*tasks)

async def execute_delayed_edit(post_num: int, bot_instance: Bot, author_id: int | None, notify_text: str | None, delay: float = 3.0):
    """
    Ждет задержку, отправляет уведомление (если оно есть), а затем редактирует пост.
    Управляет своей задачей в словаре отслеживания.
    """
    try:
        await asyncio.sleep(delay)
        
        # Сначала отправляем отложенное уведомление, если оно было сформировано
        if author_id and notify_text:
            try:
                await bot_instance.send_message(author_id, notify_text)
            except (TelegramForbiddenError, TelegramBadRequest):
                # Игнорируем, если не удалось доставить (бот заблокирован и т.д.)
                pass

        # Затем выполняем фактическое редактирование для всех
        await edit_post_for_all_recipients(post_num, bot_instance)
        
    except asyncio.CancelledError:
        # Штатная ситуация при сбросе таймера, просто выходим.
        pass
    except Exception as e:
        print(f"❌ Ошибка в execute_delayed_edit для поста #{post_num}: {e}")
    finally:
        # Безопасно удаляем свою задачу из словаря "ожидающих"
        async with pending_edit_lock:
            current_task = asyncio.current_task()
            if pending_edit_tasks.get(post_num) is current_task:
                pending_edit_tasks.pop(post_num, None)

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
            
            # --- НАЧАЛО ИЗМЕНЕНИЙ ---
            # Применяем трансформации режимов ко ВСЕМ сообщениям из очереди,
            # включая сообщения от Призрака и системные уведомления.
            content = await _apply_mode_transformations(content, board_id)
            # --- КОНЕЦ ИЗМЕНЕНИЙ ---
            
            # КЛЮЧЕВОЕ ИЗМЕНЕНИЕ: Добавляем post_num в словарь content перед отправкой
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

                # --- ДОБАВЛЕНА ПРОВЕРКА АКТИВНОСТИ ---
                activity = get_board_activity_last_hours(board_id, hours=2)
                if activity < 60:
                    print(f"ℹ️ [{board_id}] Пропуск мотивационного сообщения, активность слишком низкая: {activity:.1f} п/ч (требуется > 60).")
                    continue
                # --- КОНЕЦ ПРОВЕРКИ ---

                b_data = board_data[board_id]
                recipients = b_data['users']['active'] - b_data['users']['banned']

                if not recipients:
                    continue
                
                # Код ниже остается без изменений...
                header, post_num = await format_header(board_id)
                
                if board_id == 'int':
                    motivation = random.choice(MOTIVATIONAL_MESSAGES_EN)
                    invite_text = random.choice(INVITE_TEXTS_EN)
                    message_text = (
                        f"💭 {motivation}\n\n"
                        f"Copy and send to anons:\n"
                        f"<code>{escape_html(invite_text)}</code>"
                    )
                else:
                    motivation = random.choice(MOTIVATIONAL_MESSAGES)
                    invite_text = random.choice(INVITE_TEXTS)
                    header = f"### АДМИН ### "
                    message_text = (
                        f"💭 {motivation}\n\n"
                        f"Скопируй и отправь анончикам:\n"
                        f"<code>{escape_html(invite_text)}</code>"
                    )

                content = {
                    'type': 'text', 'header': header, 'text': message_text,
                    'is_system_message': True
                }

                await message_queues[board_id].put({
                    'recipients': recipients, 'content': content,
                    'post_num': post_num, 'reply_info': None, 'board_id': board_id
                })

                messages_storage[post_num] = {
                    'author_id': 0, 'timestamp': datetime.now(UTC),
                    'content': content, 'board_id': board_id
                }

                print(f"✅ [{board_id}] Мотивационное сообщение #{post_num} добавлено в очередь")

            except Exception as e:
                print(f"❌ [{board_id}] Ошибка в motivation_broadcaster: {e}")
                await asyncio.sleep(120)

    tasks = [asyncio.create_task(board_motivation_worker(bid)) for bid in BOARDS]
    await asyncio.gather(*tasks)
            
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
            # --- НАЧАЛО ИЗМЕНЕНИЙ ---
            lang = 'en' if board_id == 'int' else 'ru'

            if lang == 'en':
                phrases = [
                    "⏳ Hey faggot, slow down! Modes on this board can be switched once per hour.\nWait for: {minutes} minutes {seconds} seconds.",
                    "⌛️ Cool down, cowboy. The mode switch is on cooldown.\nTime left: {minutes}m {seconds}s.",
                    "⛔️ You're switching modes too often, cunt. Wait another {minutes} minutes {seconds} seconds.",
                    "⚠️ Wait, I need to rest. You can switch modes in {minutes}m {seconds}s."
                ]
            else:
                phrases = [
                    "⏳ Эй пидор, не спеши! Режимы на этой доске можно включать раз в час.\nЖди еще: {minutes} минут {seconds} секунд\n\nА пока посиди в углу и подумай о своем поведении.",
                    "⌛️ Остынь, ковбой. Кулдаун на смену режима еще не прошел.\nОсталось: {minutes}м {seconds}с.",
                    "⛔️ Слишком часто меняешь режимы, заебал. Подожди еще {minutes} минут {seconds} секунд.",
                    "⚠️ Подожди, я отдохну. Режимы можно будет переключить через {minutes}м {seconds}с."
                ]

            text = random.choice(phrases).format(minutes=minutes, seconds=seconds)
            await message.answer(text, parse_mode="HTML")
            # --- КОНЕЦ ИЗМЕНЕНИЙ ---
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

@dp.message(Command(commands=['b', 'po', 'pol', 'a', 'sex', 'vg', 'int', 'test']))
async def cmd_show_board_info(message: types.Message):
    """
    Отвечает на команду с названием доски, предоставляя информацию о ней.
    """
    current_board_id = get_board_id(message)
    if not current_board_id:
        return

    # Получаем команду без "/"
    requested_board_alias = message.text.lstrip('/')
    
    # Обрабатываем алиас /pol -> /po
    if requested_board_alias == 'pol':
        requested_board_alias = 'po'
        
    # Проверяем, существует ли такая доска в конфиге
    if requested_board_alias not in BOARD_CONFIG:
        # Эту ситуацию aiogram не должен допустить, но проверка не повредит
        await message.delete()
        return

    # Получаем данные о запрошенной доске
    target_config = BOARD_CONFIG[requested_board_alias]

    # Определяем язык ответа на основе ТЕКУЩЕЙ доски пользователя
    is_english = (current_board_id == 'int')

    if is_english:
        header_text = f"🌐 You are currently on the <b>{BOARD_CONFIG[current_board_id]['name']}</b> board."
        board_info_text = (
            f"You requested information about the <b>{target_config['name']}</b> board:\n"
            f"<i>{target_config['description_en']}</i>\n\n"
            f"You can switch to it here: {target_config['username']}"
        )
    else:
        header_text = f"🌐 Вы находитесь на доске <b>{BOARD_CONFIG[current_board_id]['name']}</b>."
        board_info_text = (
            f"Вы запросили информацию о доске <b>{target_config['name']}</b>:\n"
            f"<i>{target_config['description']}</i>\n\n"
            f"Переключиться на нее можно здесь: {target_config['username']}"
        )
    
    full_response_text = f"{header_text}\n\n{board_info_text}"

    try:
        # Отправляем ответ пользователю
        await message.answer(full_response_text, parse_mode="HTML", disable_web_page_preview=True)
        # Удаляем исходную команду
        await message.delete()
    except Exception as e:
        print(f"Ошибка в cmd_show_board_info: {e}")

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

    # --- ИЗМЕНЕНИЕ ЗДЕСЬ ---
    lang = 'en' if board_id == 'int' else 'ru'
    
    if lang == 'en':
        roll_text = f"🎲 Rolled: {result}"
    else:
        roll_text = f"🎲 Нароллил: {result}"
    # --- КОНЕЦ ИЗМЕНЕНИЯ ---

    header, pnum = await format_header(board_id)
    content = {"type": "text", "header": header, "text": roll_text} # Используем новую переменную

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
    if board_id == 'int':
        try:
            await message.delete()
        except Exception: pass
        return
    
    b_data = board_data[board_id]

    if not await check_cooldown(message, board_id):
        return

    b_data['slavaukraine_mode'] = True
    b_data['last_mode_activation'] = datetime.now(UTC)
    b_data['zaputin_mode'] = False
    b_data['suka_blyat_mode'] = False
    b_data['anime_mode'] = False

    _, pnum = await format_header(board_id)
    header = "### Админ ###"

    # --- НАЧАЛО ИЗМЕНЕНИЙ ---
    activation_phrases = [
        "УВАГА! АКТИВОВАНО УКРАЇНСЬКИЙ РЕЖИМ!\n\n💙💛 СЛАВА УКРАЇНІ! 💛💙\nГЕРОЯМ СЛАВА!\n\nХто не скаже 'Путін хуйло' - той москаль і підар!",
        "УКРАЇНСЬКИЙ РЕЖИМ УВІМКНЕНО! 🇺🇦 Всі москалі будуть денацифіковані та демілітаризовані. Смерть ворогам!",
        "УВАГА! В чаті оголошено контрнаступ! 🚜 СЛАВА НАЦІЇ! ПИЗДЕЦЬ РОСІЙСЬКІЙ ФЕДЕРАЦІЇ!",
        "💙💛 Переходимо на солов'їну! Хто не скаче, той москаль! СЛАВА ЗСУ!",
        "АКТИВОВАНО РЕЖИМ 'БАНДЕРОМОБІЛЬ'! 🇺🇦 Завантажуємо Javelin... Ціль: Кремль.",
        "УКРАЇНСЬКИЙ ПОРЯДОК НАВЕДЕНО! 🫡 Готуйтеся до повного розгрому русні. Путін - хуйло!",
        "ТЕРМІНОВО! В чаті виявлено русню! Активовано протокол 'АЗОВ'. 🇺🇦 Слава Україні!",
        "Режим 'ПРИВИД КИЄВА' активовано! ✈️ Вилітаємо на бойове завдання. Рускій воєнний корабль, іді нахуй!",
        "Наступні 5 хвилин в чаті - лише українська мова! 💙💛 За непокору - розстріл нахуй. Героям Слава!",
        "УВАГА! Територія цього чату оголошується суверенною територією України! 🇺🇦 СЛАВА УКРАЇНІ!"
    ]
    activation_text = random.choice(activation_phrases)
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---

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

    asyncio.create_task(disable_slavaukraine_mode(310, board_id))
    await message.delete()


async def disable_slavaukraine_mode(delay: int, board_id: str):
    await asyncio.sleep(delay)
    
    b_data = board_data[board_id]
    b_data['slavaukraine_mode'] = False

    _, pnum = await format_header(board_id)
    header = "### Админ ###"

    # --- НАЧАЛО ИЗМЕНЕНИЙ ---
    end_phrases = [
        "💀 Визг хохлов закончен! Украинский режим отключен. Возвращаемся к обычному трёпу.",
        "Контрнаступ захлебнулся! 🇷🇺 Хохлы, ваше время вышло. Возвращаемся к нормальному общению.",
        "Перемога отменяется! 🐷 Украинский режим деактивирован. Можно снова говорить на человеческом языке.",
        "Свинарник закрыт на дезинфекцию. 🐖 Режим 'Слава Украине' отключен.",
        "Тарасы, по окопам! Ваша перемога оказалась зрадой. 🇷🇺 Режим отключен.",
        "Батько наш Бандера сдох! 💀 Украинская пятиминутка ненависти окончена.",
        "САЛО УРОНИЛИ! 🤣 Режим хохлосрача завершен. Можно выдохнуть.",
        "Денацификация чата успешно завершена. 🇷🇺 Украинский режим подавлен.",
        "Байрактары сбиты, джавелины проёбаны. 🐷 Режим отключен, возвращаемся в родную гавань.",
        "Хрюканина окончена. 🐖 Москали снова победили. Возвращаемся к русскому языку."
    ]
    end_text = random.choice(end_phrases)
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---
    
    content = {
        "type": "text",
        "header": header,
        "text": end_text
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

@dp.message(Command("active"))
async def cmd_active(message: types.Message):
    """Выводит статистику активности досок за последние 2 часа + за сутки."""
    board_id = get_board_id(message)
    if not board_id: return

    # Статистика за последние сутки (24 часа)
    now = datetime.now(UTC)
    day_ago = now - timedelta(hours=24)
    posts_last_24h = sum(
        1 for post in messages_storage.values()
        if post.get("timestamp", now) > day_ago
    )

    lang = 'en' if board_id == 'int' else 'ru'
    activity_lines = []
    for b_id in BOARDS:
        if b_id == 'test':
            continue
        activity = get_board_activity_last_hours(b_id, hours=2)
        board_name = BOARD_CONFIG[b_id]['name']
        if lang == 'en':
            line = f"<b>{board_name}</b> - {activity:.1f} posts/hr"
        else:
            line = f"<b>{board_name}</b> - {activity:.1f} п/ч"
        activity_lines.append(line)

    if lang == 'en':
        header_text = "📊 Boards Activity (last 2h):"
        full_activity_text = f"{header_text}\n\n" + "\n".join(activity_lines)
        full_activity_text += f"\n\n📅 Total posts in last 24h: {posts_last_24h}"
    else:
        header_text = "📊 Активность досок (за 2ч):"
        full_activity_text = f"{header_text}\n\n" + "\n".join(activity_lines)
        full_activity_text += f"\n\n📅 Всего постов за последние 24 часа: {posts_last_24h}"

    header, pnum = await format_header(board_id)
    content = {
        'type': 'text', 
        'header': header, 
        'text': full_activity_text
    }
    messages_storage[pnum] = {
        'author_id': 0, 
        'timestamp': datetime.now(UTC), 
        'content': content, 
        'board_id': board_id
    }
    b_data = board_data[board_id]
    await message_queues[board_id].put({
        'recipients': b_data['users']['active'],
        'content': content,
        'post_num': pnum,
        'board_id': board_id
    })
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

    # ИЗМЕНИТЕ ЭТОТ БЛОК
    if board_id == 'int':
        stats_text = (f"📊 Board Statistics {BOARD_CONFIG[board_id]['name']}:\n\n"
                      f"👥 Anons on this board: {total_users_on_board}\n"
                      f"👥 Total anons in TGACH: {total_users_b}\n"
                      f"📨 Posts on this board: {total_posts_on_board}\n"
                      f"📈 Total posts in TGACH: {state['post_counter']}")
    else:
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

    # --- НАЧАЛО ИЗМЕНЕНИЙ ---
    activation_phrases = [
        "にゃあ～！アニメモードがアクティベートされました！\n\n^_^",
        "お兄ちゃん、大変！アニメモードの時間だよ！ UWU",
        "アニメの力がこのチャットに満ちています！(ﾉ´ヮ´)ﾉ*:･ﾟ✧",
        "『プロジェクトA』発動！これよりチャットはアキハバラ自治区となる！",
        "このチャットは「人間」をやめるぞ！ジョジョーーッ！\n\nア ニ メ モ ー ド だ！",
        "君も... 見えるのか？『チャットのスタンド』が...！アニメモード発動！",
        "チャットの皆さん、聞いてください！私、魔法少女になっちゃった！\n\nアニメモード、オン！",
        "三百年の孤独に、光が射した… アニメモードの時間だ。",
        "異世界転生したらチャットが全部日本語になっていた件。\n\nアニメモード、スタート！",
        "ばか！へんたい！すけべ！アニメモードの時間なんだからね！"
    ]
    activation_text = random.choice(activation_phrases)
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---

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

    asyncio.create_task(disable_anime_mode(330, board_id))
    await message.delete()


async def disable_anime_mode(delay: int, board_id: str):
    await asyncio.sleep(delay)
    
    b_data = board_data[board_id]
    b_data['anime_mode'] = False

    header = "### Админ ###"
    _, pnum = await format_header(board_id)

    # --- НАЧАЛО ИЗМЕНЕНИЙ ---
    end_phrases = [
        "アニメモードが終了しました！通常のチャットに戻ります！",
        "お兄ちゃん、ごめんね。もうアニメの時間じゃないんだ…",
        "魔法の力が消えちゃった… アニメモード、オフ！",
        "異世界から帰還しました。現実は非情である。",
        "『プロジェクトA』は完了した。アキハバラ自治区は解散する。",
        "スタンド能力が... 消えた...！？\n\nアニメモード解除。",
        "夢の時間は終わりだ。チャットは通常モードに戻る。",
        "現実に帰ろう、ここはチャットだ。",
        "さよなら、全てのエヴァンゲリオン。アニメモード終了。",
        "すべてのオタクに、おめでとう！\n\n(アニメモードは終わったけど)"
    ]
    end_text = random.choice(end_phrases)
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---

    content = {
        "type": "text",
        "header": header,
        "text": end_text
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
    

@dp.message(Command("deanon"))
async def cmd_deanon(message: Message):
    global last_deanon_time
    
    board_id = get_board_id(message)
    if not board_id: return
    
    # Проверка cooldown
    current_time = time.time()
    async with deanon_lock:
        if current_time - last_deanon_time < DEANON_COOLDOWN:
            # Отправляем случайное сообщение о cooldown
            cooldown_msg = random.choice(DEANON_COOLDOWN_PHRASES)
            try:
                sent_msg = await message.answer(cooldown_msg)
                # Удаляем сообщение через 5 секунд
                asyncio.create_task(delete_message_after_delay(sent_msg, 5))
            except Exception:
                pass
            await message.delete()
            return
        
        # Обновляем время последнего использования
        last_deanon_time = current_time
    
    # Оригинальный код команды /deanon
    lang = 'en' if board_id == 'int' else 'ru'

    if not message.reply_to_message:
        reply_text = "⚠️ Reply to a message to de-anonymize!" if lang == 'en' else "⚠️ Ответь на сообщение для деанона!"
        await message.answer(reply_text)
        await message.delete()
        return

    target_mid = message.reply_to_message.message_id
    user_id = message.from_user.id
    target_post = message_to_post.get((user_id, target_mid))

    if not target_post or target_post not in messages_storage:
        reply_text = "🚫 Could not find the post to de-anonymize (you might have replied to someone else's copy or an old message)." if lang == 'en' else "🚫 Не удалось найти пост для деанона (возможно, вы ответили на чужую копию или старое сообщение)."
        await message.answer(reply_text)
        await message.delete()
        return

    original_author_id = messages_storage[target_post].get('author_id')
    if original_author_id == 0:
        reply_text = "⚠️ System messages cannot be de-anonymized." if lang == 'en' else "⚠️ Нельзя деанонимизировать системные сообщения."
        await message.answer(reply_text)
        await message.delete()
        return
        
    # Передаем язык в генератор
    name, surname, city, profession, fetish, detail = generate_deanon_info(lang=lang)
    ip = f"{random.randint(10,250)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(0,255)}"
    age = random.randint(18, 45)
    
    if lang == 'en':
        deanon_text = (f"\nThis anon's name is: {name} {surname}\n"
                       f"Age: {age}\n"
                       f"Address: {city}\n"
                       f"Profession: {profession}\n"
                       f"Fetish: {fetish}\n"
                       f"IP address: {ip}\n"
                       f"Additional info: {detail}")
        header_text = "### DEANON ###"
    else:
        deanon_text = (f"\nЭтого анона зовут: {name} {surname}\n"
                       f"Возраст: {age}\n"
                       f"Адрес проживания: {city}\n"
                       f"Профессия: {profession}\n"
                       f"Фетиш: {fetish}\n"
                       f"IP-адрес: {ip}\n"
                       f"Дополнительная информация о нём: {detail}")
        header_text = "### ДЕАНОН ###"

    _, pnum = await format_header(board_id)
    content = {"type": "text", "header": header_text, "text": deanon_text, "reply_to_post": target_post}

    messages_storage[pnum] = {'author_id': 0, 'timestamp': datetime.now(UTC), 'content': content, 'board_id': board_id}

    await message_queues[board_id].put({
        "recipients": board_data[board_id]['users']['active'],
        "content": content,
        "post_num": pnum,
        "reply_info": post_to_messages.get(target_post, {}),
        "board_id": board_id
    })
    await message.delete()

async def delete_message_after_delay(message: types.Message, delay: int):
    """Удаляет сообщение после задержки"""
    await asyncio.sleep(delay)
    try:
        await message.delete()
    except Exception:
        pass
    
@dp.message(Command("zaputin"))
async def cmd_zaputin(message: types.Message):
    board_id = get_board_id(message)
    if not board_id: return

    if board_id == 'int':
        try:
            await message.delete()
        except Exception: pass
        return
    
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

    # --- НАЧАЛО ИЗМЕНЕНИЙ ---
    activation_phrases = [
        "🇷🇺 СЛАВА РОССИИ! ПУТИН - НАШ ПРЕЗИДЕНТ! 🇷🇺\n\nАктивирован режим кремлеботов! Все несогласные будут приравнены к пидорасам и укронацистам!",
        "ВНИМАНИЕ! АКТИВИРОВАН ПРОТОКОЛ 'КРЕМЛЬ'! 🇷🇺 Работаем, братья! За нами Путин и Сталинград!",
        "ТРИКОЛОР ПОДНЯТ! 🇷🇺 В чате включен режим патриотизма. Кто не с нами - тот под нами! РОССИЯ!",
        "НАЧИНАЕМ СПЕЦОПЕРАЦИЮ! 🇷🇺 Цель: денацификация чата. Потерь нет! Слава России!",
        "🇷🇺 РЕЖИМ 'РУССКИЙ МИР' АКТИВИРОВАН! 🇷🇺 От Калининграда до Владивостока - мы великая страна! ZOV",
        "ЗА ВДВ! 🇷🇺 В чате высадился русский десант. НАТО сосать! С нами Бог!",
        "ПАТРИОТИЧЕСКИЙ РЕЖИМ ВКЛЮЧЕН! 🇷🇺 Можем повторить! На Берлин! Деды воевали!",
        "🇷🇺 АКТИВИРОВАН РЕЖИМ 'БЕЗГРАНИЧНАЯ ЛЮБОВЬ К РОДИНЕ'! 🇷🇺 Гордимся страной, верим в президента!",
        "ТОВАРИЩ ПОЛКОВНИК РАЗРЕШИЛ! 🇷🇺 Включаем режим '15 рублей'. Все на защиту Родины!",
        "🇷🇺 РОССИЯ! СВЯЩЕННАЯ НАША ДЕРЖАВА! 🇷🇺 В чате включен патриотический режим. Хохлы, сосать!"
    ]
    activation_text = random.choice(activation_phrases)
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---

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

    asyncio.create_task(disable_zaputin_mode(309, board_id))
    await message.delete()


async def disable_zaputin_mode(delay: int, board_id: str):
    await asyncio.sleep(delay)
    b_data = board_data[board_id]
    b_data['zaputin_mode'] = False

    header = "### Админ ###"
    _, pnum = await format_header(board_id)

    # --- НАЧАЛО ИЗМЕНЕНИЙ ---
    end_phrases = [
        "💀 Долбёжка в Лахте закончена. Володин доволен. Всем спасибо, все свободны.",
        "Пятнадцать рублей закончились. 💸 Кремлеботы, расходимся до следующей получки.",
        "Спецоперация по защите чата успешно завершена. 🇷🇺 Можно снова быть либерахами.",
        "Перегруппировка! 🫡 Патриотический режим временно отключен для пополнения запасов водки и матрешек.",
        "Шойгу! Герасимов! Где патроны?! 💥 Режим патриотизма отключен до выяснения обстоятельств.",
        "Митинг окончен. ✊ Расходимся, пока не приехал ОМОН. Патриотизм выключен.",
        "Русский мир свернулся до размеров МКАДа. 🇷🇺 Режим отключен.",
        "Жест доброй воли! 🫡 Отключаем патриотический режим и возвращаемся к обычному общению.",
        "Выборы прошли, можно расслабиться. 🗳️ Патриотизм на паузе. До следующих выборов.",
        "Товарищ майор приказал отбой. 👮‍♂️ Возвращаемся в обычный режим."
    ]
    end_text = random.choice(end_phrases)
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---
    
    content = {
        "type": "text",
        "header": header,
        "text": end_text
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

@dp.message(Command("suka_blyat"))
async def cmd_suka_blyat(message: types.Message):
    board_id = get_board_id(message)
    if not board_id: return
    if board_id == 'int':
        try:
            await message.delete()
        except Exception: pass
        return
    
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

    # --- НАЧАЛО ИЗМЕНЕНИЙ ---
    activation_phrases = [
        "💢💢💢 Активирован режим СУКА БЛЯТЬ! 💢💢💢\n\nВсех нахуй разъебало!",
        "БЛЯЯЯЯЯТЬ! 💥 РЕЖИМ АГРЕССИИ ВКЛЮЧЕН! ПИЗДА ВСЕМУ!",
        "ВЫ ЧЕ, ОХУЕЛИ?! 💢 Включаю режим 'сука блять', готовьтесь, пидорасы!",
        "ЗАЕБАЛО ВСЁ НАХУЙ! 💥 Переходим в режим тотальной ненависти. СУКА!",
        "А НУ БЛЯТЬ СУКИ СЮДА ПОДОШЛИ! 💢 Режим 'бати в ярости' активирован!",
        "СУКАААААА! 💥 Пиздец, как меня все бесит! Включаю протокол 'РАЗЪЕБАТЬ'.",
        "ЩА БУДЕТ МЯСО! 🔪🔪🔪 Режим 'сука блять' активирован. Нытикам здесь не место!",
        "ЕБАНЫЙ ТЫ НАХУЙ! 💢💢💢 С этого момента говорим только матом. Поняли, уебаны?",
        "ТАК, БЛЯТЬ! 💥 Слушать мою команду! Режим 'СУКА БЛЯТЬ' активен. Вольно, бляди!",
        "ПОШЛИ НАХУЙ! 💥 ВСЕ ПОШЛИ НАХУЙ! Режим ярости включен, суки!"
    ]
    activation_text = random.choice(activation_phrases)
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---
    
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

    asyncio.create_task(disable_suka_blyat_mode(303, board_id))
    await message.delete()


async def disable_suka_blyat_mode(delay: int, board_id: str):
    await asyncio.sleep(delay)
    b_data = board_data[board_id]
    b_data['suka_blyat_mode'] = False

    header = "### Админ ###"
    _, pnum = await format_header(board_id)

    # --- НАЧАЛО ИЗМЕНЕНИЙ ---
    end_phrases = [
        "💀 СУКА БЛЯТЬ КОНЧИЛОСЬ. Теперь можно и помолчать.",
        "Так, блядь, успокоились все нахуй. 🧘‍♂️ Режим ярости выключен.",
        "Выпустили пар, и хватит. 💨 Режим 'сука блять' деактивирован. Заебали орать.",
        "Всё, пиздец, я спокоен. 🧊 Ярость ушла. Возвращаемся к унылому общению.",
        "Ладно, хуй с вами, живите. 🙂 Режим 'сука блять' отключен. Пока что.",
        "Батя ушел спать. 😴 Можно больше не материться. Режим отключен.",
        "Разъеб окончен. 💥 Убираем за собой, суки. Режим 'сука блять' выключен.",
        "Так, всё, наорался. 😮‍💨 Возвращаемся в обычный режим. Не бесите меня.",
        "Мое очко остыло. 🔥 Режим ярости деактивирован.",
        "Миссия 'ВСЕХ НАХУЙ' выполнена. 🫡 Возвращаемся на базу. Режим отключен."
    ]
    end_text = random.choice(end_phrases)
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---

    content = {
        "type": "text",
        "header": header,
        "text": end_text
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

    # --- НАЧАЛО ИЗМЕНЕНИЙ ---
    lang = 'en' if board_id == 'int' else 'ru'
    board_name = BOARD_CONFIG[board_id]['name']

    if lang == 'en':
        phrases = [
            "✅ Faggot <code>{user_id}</code> has been banned from {board}.\nDeleted his posts in the last 5 minutes: {deleted}",
            "👍 User <code>{user_id}</code> is now banned on {board}. Wiped {deleted} recent posts.",
            "👌 Done. <code>{user_id}</code> won't be posting on {board} anymore. Deleted posts: {deleted}."
        ]
    else:
        phrases = [
            "✅ Хуесос под номером <code>{user_id}</code> забанен на доске {board}\nУдалено его постов за последние 5 минут: {deleted}",
            "👍 Пользователь <code>{user_id}</code> успешно забанен на доске {board}. Снесено {deleted} его высеров.",
            "👌 Готово. <code>{user_id}</code> больше не будет отсвечивать на доске {board}. Удалено постов: {deleted}."
        ]
    response_text = random.choice(phrases).format(user_id=target_id, board=board_name, deleted=deleted_posts)
    await message.answer(response_text, parse_mode="HTML")
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---

    await send_moderation_notice(target_id, "ban", board_id, deleted_posts=deleted_posts)

    try:
        if lang == 'en':
            phrases = [
                "You have been permanently banned from the {board} board. Reason: you're a faggot.\nDeleted your posts in the last 5 minutes: {deleted}",
                "Congratulations! You've won an all-inclusive trip to hell. You are banned from {board}.\nWe've deleted {deleted} of your recent shitposts.",
                "The admin didn't like you. You're banned from {board}. Get out.\nDeleted posts: {deleted}."
            ]
        else:
            phrases = [
                "Пидорас ебаный, ты нас так заебал, что тебя блокнули нахуй на доске {board}.\nУдалено твоих постов за последние 5 минут: {deleted}\nПиздуй отсюда.",
                "Поздравляю, долбоеб. Ты допизделся и получил вечный бан на доске {board}.\nТвои высеры за последние 5 минут ({deleted} шт.) удалены.",
                "Ты был слаб, и Абу тебя сожрал. Ты забанен на доске {board}.\nУдалено постов: {deleted}."
            ]
        
        notification_text = random.choice(phrases).format(board=board_name, deleted=deleted_posts)
        await message.bot.send_message(target_id, notification_text)
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

    # --- НАЧАЛО ИЗМЕНЕНИЙ ---
    board_name = BOARD_CONFIG[board_id]['name']
    await message.answer(
        f"🔇 Хуила {target_id} замучен на {duration_text} на доске {board_name}\n"
        f"Удалено сообщений за последние 5 минут: {deleted_count}",
        parse_mode="HTML"
    )
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---

    await send_moderation_notice(target_id, "mute", board_id, duration=duration_text, deleted_posts=deleted_count)

    try:
        lang = 'en' if board_id == 'int' else 'ru'
        
        if lang == 'en':
            phrases = [
                "🔇 You have been muted on the {board} board for {duration}.\nDeleted your posts in the last 5 minutes: {deleted}.",
                "🗣️ Your right to speak has been temporarily revoked on {board} for {duration}. Think about your behavior.\nDeleted posts: {deleted}.",
                "🤐 Shut up for {duration} on the {board} board.\nDeleted posts: {deleted}."
            ]
        else:
            phrases = [
                "🔇 Пидор ебаный, тебя замутили на доске {board} на {duration}.\nУдалено твоих сообщений за последние 5 минут: {deleted}.",
                "🗣️ Твой рот был запечатан админской печатью на {duration} на доске {board}.\nТвои высеры ({deleted} шт.) удалены.",
                "🤐 Помолчи, подумой. Ты в муте на {duration} на доске {board}.\nУдалено постов: {deleted}."
            ]
        
        notification_text = random.choice(phrases).format(board=board_name, duration=duration_text, deleted=deleted_count)
        await message.bot.send_message(target_id, notification_text, parse_mode="HTML")
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

    deleted_messages = await delete_user_posts(message.bot, target_id, 999999, board_id)

    # --- НАЧАЛО ИЗМЕНЕНИЙ ---
    board_name = BOARD_CONFIG[board_id]['name']
    await message.answer(
        f"🗑 Удалено {deleted_messages} сообщений пользователя {target_id} с доски {board_name}."
    )
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---
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
    board_name = BOARD_CONFIG[board_id]['name']
    if b_data['mutes'].pop(target_id, None):
        # --- НАЧАЛО ИЗМЕНЕНИЙ ---
        await message.answer(f"🔈 Пользователь {target_id} размучен на доске {board_name}.")
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---
        try:
            lang = 'en' if board_id == 'int' else 'ru'
            if lang == 'en':
                phrases = [
                    "🔊 You have been unmuted on the {board} board. Try to behave.",
                    "✅ You can speak again on {board}. Don't make us regret this.",
                    "🗣️ Your voice has been returned on the {board} board."
                ]
            else:
                phrases = [
                    "Тебя размутили на доске {board}.",
                    "✅ Можешь снова открывать свою пасть на доске {board}. Но впредь будь осторожен.",
                    "🗣️ Админ смилостивился. Ты размучен на доске {board}."
                ]
            notification_text = random.choice(phrases).format(board=board_name)
            await message.bot.send_message(target_id, notification_text)
        except:
            pass
    else:
        # --- НАЧАЛО ИЗМЕНЕНИЙ ---
        await message.answer(f"Пользователь {target_id} не был в муте на этой доске.")
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---
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
        board_name = BOARD_CONFIG[board_id]['name']
        if user_id in b_data['users']['banned']:
             b_data['users']['banned'].discard(user_id)
             # --- НАЧАЛО ИЗМЕНЕНИЙ ---
             await message.answer(f"Пользователь {user_id} разбанен на доске {board_name}.")
             # --- КОНЕЦ ИЗМЕНЕНИЙ ---
        else:
             # --- НАЧАЛО ИЗМЕНЕНИЙ ---
            await message.answer(f"Пользователь {user_id} не был забанен на этой доске.")
             # --- КОНЕЦ ИЗМЕНЕНИЙ ---
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

    deleted_count = await delete_single_post(post_num, message.bot)

    # --- НАЧАЛО ИЗМЕНЕНИЙ ---
    await message.answer(f"Пост №{post_num} и все его копии ({deleted_count} сообщений) удалены.")
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---
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

        # --- НАЧАЛО ИЗМЕНЕНИЙ ---
        board_name = BOARD_CONFIG[board_id]['name']
        await message.answer(f"👻 Тихо замучен пользователь {target_id} на {time_str} на доске {board_name}.")
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---
    except ValueError:
        await message.answer("❌ Неверный формат времени. Примеры: 30m, 2h, 1d")
    await message.delete()


@dp.message(Command("unshadowmute"))
async def cmd_unshadowmute(message: Message):
    board_id = get_board_id(message)
    if not is_admin(message.from_user.id, board_id):
        return

    target_id = None
    parts = message.text.split()
    if len(parts) >= 2 and parts[1].isdigit():
        target_id = int(parts[1])
    elif message.reply_to_message:
        target_id = get_author_id_by_reply(message)

    if not target_id:
        await message.answer("Использование: /unshadowmute <user_id> или ответ на сообщение.")
        return
    
    b_data = board_data[board_id]
    board_name = BOARD_CONFIG[board_id]['name']
    if b_data['shadow_mutes'].pop(target_id, None):
        # --- НАЧАЛО ИЗМЕНЕНИЙ ---
        await message.answer(f"👻 Пользователь {target_id} тихо размучен на доске {board_name}.")
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---
    else:
        # --- НАЧАЛО ИЗМЕНЕНИЙ ---
        await message.answer(f"ℹ️ Пользователь {target_id} не в shadow-муте на этой доске.")
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---
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

    b_data['last_activity'][user_id] = datetime.now(UTC)
    
    spam_check = await check_spam(user_id, message, board_id)
    if not spam_check:
        try:
            await message.delete()
        except TelegramBadRequest: pass
        msg_type = 'text' if message.caption else 'animation'
        await apply_penalty(message.bot, user_id, msg_type, board_id)
        return
        
    is_shadow_muted = (user_id in b_data['shadow_mutes'] and 
                       b_data['shadow_mutes'][user_id] > datetime.now(UTC))

    recipients = b_data['users']['active'] - {user_id}
    reply_to_post, reply_info = None, {}
    if message.reply_to_message:
        lookup_key = (user_id, message.reply_to_message.message_id)
        reply_to_post = message_to_post.get(lookup_key)
        if reply_to_post and reply_to_post in post_to_messages:
            reply_info = post_to_messages[reply_to_post]
        else:
            reply_to_post = None
            
    header, current_post_num = await format_header(board_id)
    try:
        await message.delete()
    except TelegramBadRequest: pass
    
    caption_content = message.caption_html_text if hasattr(message, 'caption_html_text') and message.caption_html_text else (message.caption or "")
    if message.caption:
        last_messages.append(message.caption)
        
    content = {
        'type': 'audio', 'header': header, 'file_id': message.audio.file_id,
        'caption': caption_content, 'reply_to_post': reply_to_post
    }

    messages_storage[current_post_num] = {
        'author_id': user_id, 'timestamp': datetime.now(UTC), 'content': content,
        'board_id': board_id, 'author_message_id': None
    }
    
    try:
        content_for_author = await _apply_mode_transformations(content, board_id)
        
        results = await send_message_to_users(
            bot_instance=message.bot,
            recipients={user_id},
            content=content_for_author,
            reply_info=reply_info
        )
        if results and results[0] and results[0][1]:
            sent_to_author = results[0][1]
            messages_to_save = sent_to_author if isinstance(sent_to_author, list) else [sent_to_author]
            for m in messages_to_save:
                messages_storage[current_post_num]['author_message_id'] = m.message_id
                post_to_messages.setdefault(current_post_num, {})[user_id] = m.message_id
                message_to_post[(user_id, m.message_id)] = current_post_num
        
        if not is_shadow_muted:
            if recipients and user_id in b_data['users']['active']:
                await message_queues[board_id].put({
                    'recipients': recipients, 'content': content, 'post_num': current_post_num,
                    'reply_info': reply_info if reply_info else None, 'board_id': board_id
                })
            
    except TelegramForbiddenError:
        b_data['users']['active'].discard(user_id)
        print(f"🚫 [{board_id}] Пользователь {user_id} заблокировал бота, удален из активных (из handle_audio).")
    except Exception as e:
        print(f"❌ Критическая ошибка постановки в очередь аудио-поста. Пост #{current_post_num} удален. Ошибка: {e}")
        messages_storage.pop(current_post_num, None)
        
@dp.message(F.voice)
async def handle_voice(message: Message):
    """Адаптированный обработчик голосовых сообщений."""
    user_id = message.from_user.id
    board_id = get_board_id(message)
    if not board_id: return
        
    b_data = board_data[board_id]

    is_shadow_muted = (user_id in b_data['shadow_mutes'] and 
                       b_data['shadow_mutes'][user_id] > datetime.now(UTC))

    if user_id in b_data['users']['banned']:
        await message.delete()
        return

    if b_data['mutes'].get(user_id) and b_data['mutes'][user_id] > datetime.now(UTC):
        await message.delete()
        return

    b_data['last_activity'][user_id] = datetime.now(UTC)

    spam_check = await check_spam(user_id, message, board_id)
    if not spam_check:
        try:
            await message.delete()
        except TelegramBadRequest: pass
        await apply_penalty(message.bot, user_id, 'animation', board_id)
        return

    header, current_post_num = await format_header(board_id)
    reply_to_post, reply_info = None, {}

    if message.reply_to_message:
        lookup_key = (user_id, message.reply_to_message.message_id)
        reply_to_post = message_to_post.get(lookup_key)
        
        if reply_to_post and reply_to_post in post_to_messages:
            reply_info = post_to_messages[reply_to_post]
        else:
            reply_to_post = None

    try:
        await message.delete()
    except TelegramBadRequest: pass

    content = {
        'type': 'voice', 'header': header, 'file_id': message.voice.file_id,
        'reply_to_post': reply_to_post
    }

    messages_storage[current_post_num] = {
        'author_id': user_id, 'timestamp': datetime.now(UTC), 'content': content,
        'board_id': board_id, 'author_message_id': None
    }

    try:
        results = await send_message_to_users(
            bot_instance=message.bot,
            recipients={user_id},
            content=content,
            reply_info=reply_info
        )
        if results and results[0] and results[0][1]:
            sent_to_author = results[0][1]
            messages_to_save = sent_to_author if isinstance(sent_to_author, list) else [sent_to_author]
            for m in messages_to_save:
                messages_storage[current_post_num]['author_message_id'] = m.message_id
                post_to_messages.setdefault(current_post_num, {})[user_id] = m.message_id
                message_to_post[(user_id, m.message_id)] = current_post_num
        
        if not is_shadow_muted:
            recipients = b_data['users']['active'] - {user_id}
            if recipients and user_id in b_data['users']['active']:
                await message_queues[board_id].put({
                    'recipients': recipients, 'content': content, 'post_num': current_post_num,
                    'reply_info': reply_info, 'board_id': board_id
                })

    except TelegramForbiddenError:
        b_data['users']['active'].discard(user_id)
        print(f"🚫 [{board_id}] Пользователь {user_id} заблокировал бота, удален из активных (из handle_voice).")
    except Exception as e:
        print(f"❌ Критическая ошибка постановки в очередь голосового поста. Пост #{current_post_num} удален. Ошибка: {e}")
        messages_storage.pop(current_post_num, None)
        
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
    
    # --- НАЧАЛО ИЗМЕНЕНИЙ ---
    b_data['last_activity'][user_id] = datetime.now(UTC)
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---

    group = current_media_groups.get(media_group_id)
    is_leader = False

    if group is None:
        group = current_media_groups.setdefault(media_group_id, {'is_initializing': True})
        if group.get('is_initializing'):
            is_leader = True
    
    if is_leader:
        # Симулируем текстовое сообщение для спам-проверки
        fake_text_message = types.Message(
            message_id=message.message_id,
            date=message.date,
            chat=message.chat,
            from_user=message.from_user,
            content_type='text',
            text=f"media_group_{media_group_id}"
        )
        
        spam_check_passed = await check_spam(user_id, fake_text_message, board_id)
        
        if not spam_check_passed:
            current_media_groups.pop(media_group_id, None) 
            await apply_penalty(message.bot, user_id, 'text', board_id)
            return
        
        reply_to_post = None
        if message.reply_to_message:
            lookup_key = (user_id, message.reply_to_message.message_id)
            reply_to_post = message_to_post.get(lookup_key)

        header, post_num = await format_header(board_id)
        # --- ИСПРАВЛЕНИЕ: Безопасный доступ к caption_html_text ---
        caption = message.caption_html_text if hasattr(message, 'caption_html_text') and message.caption_html_text else (message.caption or "")
        
        group.update({
            'board_id': board_id, 'post_num': post_num, 'header': header, 'author_id': user_id,
            'timestamp': datetime.now(UTC), 'media': [], 'caption': caption,
            'reply_to_post': reply_to_post, 'processed_messages': set(),
            'source_message_ids': set()
        })
        group.pop('is_initializing', None)
    else:
        while group is not None and group.get('is_initializing'):
            await asyncio.sleep(0.05)
            group = current_media_groups.get(media_group_id)
        
        if media_group_id not in current_media_groups:
            return

    if not group:
        return
        
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

    if media_group_id in media_group_timers:
        media_group_timers[media_group_id].cancel()
    
    media_group_timers[media_group_id] = asyncio.create_task(
        complete_media_group_after_delay(media_group_id, message.bot, delay=1.5)
    )
    
async def complete_media_group_after_delay(media_group_id: str, bot_instance: Bot, delay: float = 1.5):
    try:
        await asyncio.sleep(delay)

        group = current_media_groups.pop(media_group_id, None)
        if not group or media_group_id in sent_media_groups:
            return

        media_group_timers.pop(media_group_id, None)

        # --- НАЧАЛО ИЗМЕНЕНИЙ: Пакетное удаление ---
        source_message_ids = group.get('source_message_ids')
        author_id = group.get('author_id')

        if source_message_ids and author_id:
            try:
                await bot_instance.delete_messages(
                    chat_id=author_id,
                    message_ids=list(source_message_ids)
                )
            except TelegramBadRequest as e:
                print(f"ℹ️ Не удалось выполнить пакетное удаление для media group {media_group_id}: {e}")
            except Exception as e:
                print(f"❌ Ошибка при пакетном удалении для media group {media_group_id}: {e}")
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---

        await process_complete_media_group(media_group_id, group, bot_instance)

        # --- ВАЖНО! Дополнительная очистка для экономии памяти ---
        current_media_groups.pop(media_group_id, None)
        media_group_timers.pop(media_group_id, None)
        if media_group_id in sent_media_groups:
            sent_media_groups.remove(media_group_id)

    except asyncio.CancelledError:
        pass
    except Exception as e:
        print(f"❌ Ошибка в complete_media_group_after_delay для {media_group_id}: {e}")
        current_media_groups.pop(media_group_id, None)
        media_group_timers.pop(media_group_id, None)


async def process_complete_media_group(media_group_id: str, group: dict, bot_instance: Bot):
    if not group or not group.get('media'):
        return

    sent_media_groups.append(media_group_id)

    all_media = group.get('media', [])
    CHUNK_SIZE = 10
    media_chunks = [all_media[i:i + CHUNK_SIZE] for i in range(0, len(all_media), CHUNK_SIZE)]

    for i, chunk in enumerate(media_chunks):
        if not chunk: continue

        user_id = group['author_id']
        board_id = group['board_id']
        b_data = board_data[board_id]
        
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

        messages_storage[post_num] = {
            'author_id': user_id, 'timestamp': group['timestamp'], 'content': content,
            'board_id': board_id
        }

        reply_info = {}
        try:
            builder = MediaGroupBuilder()
            reply_to_message_id = None

            content_for_author = await _apply_mode_transformations(content, board_id)
            
            formatted_body = await _format_message_body(content_for_author, user_id, post_num)
            header_html = f"<i>{escape_html(header)}</i>"
            
            full_caption_text = ""
            if i == 0:
                full_caption_text = f"{header_html}\n\n{formatted_body}" if formatted_body else header_html
            else:
                full_caption_text = header_html

            if reply_to_post:
                reply_info = post_to_messages.get(reply_to_post, {})
                reply_to_message_id = reply_info.get(user_id)
            
            for idx, media in enumerate(chunk):
                caption_for_media = full_caption_text if idx == 0 else None
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
            await asyncio.sleep(1)
            
def apply_greentext_formatting(text: str) -> str:
    """
    Применяет форматирование 'Greentext' к строкам, начинающимся с '>'.
    Эта версия сначала экранирует HTML в строке, а затем оборачивает её в тег,
    чтобы избежать конфликтов разметки.
    """
    if not text:
        return text

    processed_lines = []
    lines = text.split('\n')
    for line in lines:
        stripped_line = line.lstrip()
        # Проверяем, начинается ли строка с символа '>' или его HTML-сущности '>'
        if stripped_line.startswith('>') or stripped_line.startswith('>'):
            # Сначала экранируем строку, чтобы символы < > & не ломали разметку,
            # а затем оборачиваем в тег `<code>`.
            processed_lines.append(f"<code>{escape_html(line)}</code>")
        else:
            # Для обычных строк просто передаем их как есть, сохраняя HTML-разметку.
            processed_lines.append(line)
            
    return '\n'.join(processed_lines)

@dp.message_reaction()
async def handle_message_reaction(reaction: types.MessageReactionUpdated):
    """
    Обрабатывает реакции, синхронизируя отложенное редактирование поста
    и отправку уведомления автору для предотвращения любого спама.
    """
    try:
        # 1. Получаем ключевые ID и данные
        user_id = reaction.user.id
        chat_id = reaction.chat.id
        message_id = reaction.message_id
        board_id = get_board_id(reaction)
        if not board_id: return

        # 2. Находим пост и его автора
        post_num = message_to_post.get((chat_id, message_id))
        if not post_num or post_num not in messages_storage:
            return

        post_data = messages_storage[post_num]
        author_id = post_data.get('author_id')

        # 3. Игнорируем реакции на собственные сообщения
        if author_id == user_id:
            return

        # 4. Обновляем состояние реакций в памяти
        if 'reactions' not in post_data or 'users' not in post_data.get('reactions', {}):
            post_data['reactions'] = {'users': {}}
        
        reactions_storage = post_data['reactions']['users']
        old_emojis_from_user = set(reactions_storage.get(user_id, []))

        new_emojis = [r.emoji for r in reaction.new_reaction if r.type == 'emoji']
        if not new_emojis:
            if user_id in reactions_storage: del reactions_storage[user_id]
            else: return
        else:
            reactions_storage[user_id] = new_emojis[:2]
        
        # --- НАЧАЛО ИЗМЕНЕНИЙ: Подготовка данных для отложенной задачи ---
        
        # 5. Готовим данные для уведомления (но пока не отправляем)
        author_id_for_notify = None
        text_for_notify = None
        
        newly_added_emojis = set(reactions_storage.get(user_id, [])) - old_emojis_from_user
        if newly_added_emojis and author_id:
            # Проверяем лимит на уведомления
            async with author_reaction_notify_lock:
                now = time.time()
                author_timestamps = author_reaction_notify_tracker[author_id]
                while author_timestamps and author_timestamps[0] <= now - 60:
                    author_timestamps.popleft()
                if len(author_timestamps) < AUTHOR_NOTIFY_LIMIT_PER_MINUTE:
                    author_timestamps.append(now)
                    # Если лимит не превышен, подготавливаем данные
                    author_id_for_notify = author_id
                    lang = 'en' if board_id == 'int' else 'ru'
                    emoji = list(newly_added_emojis)[0]
                    
                    if emoji in POSITIVE_REACTIONS: category = 'positive'
                    elif emoji in NEGATIVE_REACTIONS: category = 'negative'
                    else: category = 'neutral'
                    
                    phrase_template = random.choice(REACTION_NOTIFY_PHRASES[lang][category])
                    text_for_notify = phrase_template.format(post_num=post_num)

        # 6. Планируем единую отложенную задачу для редактирования и уведомления
        async with pending_edit_lock:
            if post_num in pending_edit_tasks:
                pending_edit_tasks[post_num].cancel()

            # Передаем подготовленные данные в отложенную задачу
            new_task = asyncio.create_task(
                execute_delayed_edit(
                    post_num=post_num,
                    bot_instance=reaction.bot,
                    author_id=author_id_for_notify,
                    notify_text=text_for_notify
                )
            )
            pending_edit_tasks[post_num] = new_task
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---
                
    except Exception as e:
        import traceback
        print(f"❌ Критическая ошибка в handle_message_reaction: {e}\n{traceback.format_exc()}")

@dp.message()
async def handle_message(message: Message):
    user_id = message.from_user.id
    
    board_id = get_board_id(message)
    if not board_id: return 
    
    b_data = board_data[board_id]

    try:
        mute_until = b_data['mutes'].get(user_id)
        if mute_until and mute_until > datetime.now(UTC):
            left = mute_until - datetime.now(UTC)
            await message.delete()
            # --- НАЧАЛО ИЗМЕНЕНИЙ ---
            if board_id == 'int':
                time_left_str = f"{int(left.total_seconds() // 60)}m {int(left.total_seconds() % 60)}s"
                phrases = [
                    "🔇 Hey faggot, you are still muted on the {board} board for {time_left}",
                    "🤫 Shhh! You're still in timeout on {board} for another {time_left}.",
                    "🤐 Your mouth is still taped shut on {board}. Wait for {time_left}."
                ]
                notification_text = random.choice(phrases).format(board=BOARD_CONFIG[board_id]['name'], time_left=time_left_str)
            else:
                time_left_str = f"{int(left.total_seconds() // 60)}м {int(left.total_seconds() % 60)}с"
                phrases = [
                    "🔇 Эй пидор, ты в муте на доске {board} ещё {time_left}",
                    "🤫 Тссс! Твой рот все еще занят. Жди еще {time_left} на доске {board}.",
                    "🤐 Помолчи, уебан. Тебе еще сидеть в муте {time_left} на доске {board}."
                ]
                notification_text = random.choice(phrases).format(board=BOARD_CONFIG[board_id]['name'], time_left=time_left_str)
            # --- КОНЕЦ ИЗМЕНЕНИЙ ---
            await message.bot.send_message(user_id, notification_text, parse_mode="HTML")
            return
        elif mute_until:
             b_data['mutes'].pop(user_id, None)

        if user_id in b_data['users']['banned']:
            await message.delete()
            return

        if message.media_group_id or not (message.text or message.caption or message.content_type):
            return
    except (TelegramBadRequest, TelegramForbiddenError):
        return
    except Exception as e:
        print(f"Ошибка на этапе блочных проверок для user {user_id}: {e}")
        return

    is_shadow_muted = (user_id in b_data['shadow_mutes'] and b_data['shadow_mutes'][user_id] > datetime.now(UTC))

    b_data['last_activity'][user_id] = datetime.now(UTC)

    if user_id not in b_data['users']['active']:
        b_data['users']['active'].add(user_id)
        print(f"✅ [{board_id}] Добавлен новый пользователь: ID {user_id}")

    if not await check_spam(user_id, message, board_id):
        try:
            await message.delete()
            msg_type = message.content_type
            if msg_type in ['photo', 'video', 'document'] and message.caption:
                msg_type = 'text'
            await apply_penalty(message.bot, user_id, msg_type, board_id)
        except TelegramBadRequest: pass
        return

    try:
        reply_to_post, reply_info = None, {}
        if message.reply_to_message:
            lookup_key = (user_id, message.reply_to_message.message_id)
            reply_to_post = message_to_post.get(lookup_key)
            if reply_to_post and reply_to_post in post_to_messages:
                reply_info = post_to_messages[reply_to_post]
            else:
                reply_to_post = None

        header, current_post_num = await format_header(board_id)
        await message.delete()

        content = {'type': message.content_type, 'header': header, 'reply_to_post': reply_to_post}
        text_for_corpus = None

        is_transform_mode_active = (
            b_data['anime_mode'] or b_data['slavaukraine_mode'] or
            b_data['zaputin_mode'] or b_data['suka_blyat_mode']
        )
        
        if message.content_type == 'text':
            text_for_corpus = message.text
            text_to_process = message.text if is_transform_mode_active else message.html_text
            content.update({'text': text_to_process})
        
        elif message.content_type in ['photo', 'video', 'animation', 'document', 'audio']:
            text_for_corpus = message.caption
            file_id_obj = getattr(message, message.content_type, [])
            if isinstance(file_id_obj, list): file_id_obj = file_id_obj[-1]
            caption_to_process = message.caption or "" if is_transform_mode_active else getattr(message, 'caption_html_text', message.caption or "")
            content.update({'file_id': file_id_obj.file_id, 'caption': caption_to_process})
        
        elif message.content_type in ['sticker', 'voice', 'video_note']:
            file_id_obj = getattr(message, message.content_type)
            content.update({'file_id': file_id_obj.file_id})
            if message.content_type == 'sticker' and message.sticker and message.sticker.emoji:
                 text_for_corpus = message.sticker.emoji
        
        if text_for_corpus: last_messages.append(text_for_corpus)

        messages_storage[current_post_num] = {
            'author_id': user_id, 'timestamp': datetime.now(UTC), 'content': content,
            'author_message_id': None, 'board_id': board_id
        }

        try:
            content_for_author = await _apply_mode_transformations(content, board_id)
            results = await send_message_to_users(
                bot_instance=message.bot, recipients={user_id},
                content=content_for_author, reply_info=reply_info
            )
            if results and results[0] and results[0][1]:
                sent_to_author = results[0][1]
                messages_to_save = sent_to_author if isinstance(sent_to_author, list) else [sent_to_author]
                for m in messages_to_save:
                    messages_storage[current_post_num]['author_message_id'] = m.message_id
                    post_to_messages.setdefault(current_post_num, {})[user_id] = m.message_id
                    message_to_post[(user_id, m.message_id)] = current_post_num
        except TelegramForbiddenError:
            b_data['users']['active'].discard(user_id)
            print(f"🚫 [{board_id}] Пользователь {user_id} заблокировал бота (из handle_message).")
        except Exception as e:
            print(f"Ошибка при отправке сообщения автору: {e}")
            messages_storage.pop(current_post_num, None)
            return

        if not is_shadow_muted:
            recipients = b_data['users']['active'] - {user_id}
            if recipients:
                await message_queues[board_id].put({
                    'recipients': recipients, 'content': content, 'post_num': current_post_num,
                    'reply_info': reply_info if reply_info else None, 'board_id': board_id
                })

    except TelegramBadRequest:
        pass
    except Exception as e:
        import traceback
        print(f"Критическая ошибка в основной обработке handle_message для user {user_id}: {e}\n{traceback.format_exc()}")
        if 'current_post_num' in locals():
            messages_storage.pop(current_post_num, None)
        
async def start_background_tasks(bots: dict[str, Bot]):
    """Поднимаем все фоновые корутины ОДИН раз за весь runtime"""
    # --- НАЧАЛО ИЗМЕНЕНИЙ ---
    # Локальный импорт для разрыва цикла зависимостей, который вызывает NameError
    from conan import conan_roaster
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---
    
    tasks = [
        asyncio.create_task(auto_backup()),
        asyncio.create_task(message_broadcaster(bots)),
        asyncio.create_task(conan_roaster(
            state, messages_storage, post_to_messages, message_to_post,
            message_queues, format_header, board_data
        )),
        asyncio.create_task(motivation_broadcaster()),
        asyncio.create_task(auto_memory_cleaner()),
        asyncio.create_task(board_statistics_broadcaster()),
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
    
    session = None
    bots = {}
    try:
        global is_shutting_down
        loop = asyncio.get_running_loop()

        restore_backup_on_start()
        load_state()

        from aiogram.client.session.aiohttp import AiohttpSession

        # 1. <--- ИСПРАВЛЕНИЕ TypeError: unsupported operand type(s) for +: 'ClientTimeout' and 'int'
        # Передаем таймаут как число, а не объект ClientTimeout.
        session = AiohttpSession(
            timeout=60
        )
        
        default_properties = DefaultBotProperties(parse_mode="HTML")
        
        for board_id, config in BOARD_CONFIG.items():
            token = config.get("token")
            if token:
                bots[board_id] = Bot(
                    token=token, 
                    default=default_properties, 
                    session=session
                )
            else:
                print(f"⚠️ Токен для доски '{board_id}' не найден, пропуск.")
        
        if not bots:
            print("❌ Не найдено ни одного токена бота. Завершение работы.")
            if session:
                await session.close()
            return

        print(f"✅ Инициализировано {len(bots)} ботов: {list(bots.keys())}")
        
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
        import traceback
        print(f"🔥 Critical error in supervisor: {e}\n{traceback.format_exc()}")
    finally:
        if not is_shutting_down:
             await graceful_shutdown(list(bots.values()))
        
        # 2. <--- ИСПРАВЛЕНИЕ AttributeError: 'AiohttpSession' object has no attribute 'closed'
        # Убрана проверка session.closed, так как у объекта сессии нет такого атрибута.
        # Метод close() можно безопасно вызывать, даже если сессия уже закрыта.
        if session:
            print("Закрытие общей HTTP сессии...")
            await session.close()
        
        if os.path.exists(lock_file):
            os.remove(lock_file)
            
if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(supervisor())
