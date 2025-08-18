from __future__ import annotations
import asyncio
import gc
import glob
import gzip
import json
import logging
import os
import pickle
import random
import re
import secrets
import shutil
import signal
import subprocess
import sys
import time
import weakref
from asyncio import Semaphore
from collections import deque, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone, UTC
from typing import Tuple

import aiohttp
from aiohttp import web
from aiogram import Bot, Dispatcher, F, types
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import (
    TelegramBadRequest,
    TelegramConflictError,
    TelegramForbiddenError,
    TelegramNetworkError,
    TelegramRetryAfter,
)
from aiogram.filters import Command, CommandStart
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message
from aiogram.utils.media_group import MediaGroupBuilder
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
time.sleep(3)

import deanonymizer
from deanonymizer import (
    DEANON_CITIES,
    DEANON_DETAILS,
    DEANON_FETISHES,
    DEANON_PROFESSIONS,
    DEANON_SURNAMES,
    generate_deanon_info,
)
from help_text import HELP_TEXT, HELP_TEXT_EN
from japanese_translator import anime_transform, get_random_anime_image
from summarize import summarize_text_with_hf
from thread_texts import thread_messages
from ukrainian_mode import UKRAINIAN_PHRASES, ukrainian_transform
from zaputin_mode import PATRIOTIC_PHRASES, zaputin_transform
from aiogram import BaseMiddleware
from aiogram.types import TelegramObject
from typing import Callable, Dict, Any, Awaitable

class BoardMiddleware(BaseMiddleware):
    """
    Middleware для определения `board_id` и передачи его в обработчики.
    Это избавляет от необходимости вызывать get_board_id() в каждом хендлере.
    """
    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any]
    ) -> Any:
        # get_board_id() может работать как с Message, так и с CallbackQuery,
        # поэтому мы можем безопасно использовать 'event' в качестве аргумента.
        board_id = get_board_id(event)
        
        # Добавляем board_id в словарь 'data', чтобы он был доступен
        # в хендлере как именованный аргумент.
        data['board_id'] = board_id
        
        # Вызываем следующий обработчик в цепочке.
        return await handler(event, data)

# ========== Глобальные настройки досок ==========

BOARD_CONFIG = {
    'b': {
        "name": "/b/",
        "description": "БРЕД - основная доска",
        "description_en": "RANDOM -",
        "username": "@dvach_chatbot",
        "token": os.getenv("BOT_TOKEN"),
        # --- НАЧАЛО ИЗМЕНЕНИЙ ---
        "admins": {int(x.strip()) for x in os.getenv("ADMINS", "").split(",") if x}
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---
    },
    'po': {
        "name": "/po/",
        "description": "ПОЛИТАЧ - (срачи, политика)",
        "description_en": "POLITICS  -",
        "username": "@dvach_po_chatbot",
        "token": os.getenv("PO_BOT_TOKEN"),
        # --- НАЧАЛО ИЗМЕНЕНИЙ ---
        "admins": {int(x.strip()) for x in os.getenv("PO_ADMINS", "").split(",") if x}
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---
    },
    'a': {
        "name": "/a/",
        "description": "АНИМЕ - (манга, Япония, хентай)",
        "description_en": "ANIME (🇯🇵, hentai, manga)",
        "username": "@dvach_a_chatbot",
        "token": os.getenv("A_BOT_TOKEN"),
        # --- НАЧАЛО ИЗМЕНЕНИЙ ---
        "admins": {int(x.strip()) for x in os.getenv("A_ADMINS", "").split(",") if x}
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---
    },
    'sex': {
        "name": "/sex/",
        "description": "СЕКСАЧ - (отношения, секс, тян, еот, блекпилл)",
        "description_en": "SEX (relationships, sex, blackpill)",
        "username": "@dvach_sex_chatbot",
        "token": os.getenv("SEX_BOT_TOKEN"),
        # --- НАЧАЛО ИЗМЕНЕНИЙ ---
        "admins": {int(x.strip()) for x in os.getenv("SEX_ADMINS", "").split(",") if x}
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---
    },
    'vg': {
        "name": "/vg/",
        "description": "ВИДЕОИГРЫ - (ПК, игры, хобби)",
        "description_en": "VIDEO GAMES (🎮, hobbies)",
        "username": "@dvach_vg_chatbot",
        "token": os.getenv("VG_BOT_TOKEN"),
        # --- НАЧАЛО ИЗМЕНЕНИЙ ---
        "admins": {int(x.strip()) for x in os.getenv("VG_ADMINS", "").split(",") if x}
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---
    },
    'int': {
        "name": "/int/",
        "description": "INTERNATIONAL (🇬🇧🇺🇸🇨🇳🇮🇳🇪🇺)",
        "description_en": "INTERNATIONAL (🇬🇧🇺🇸🇨🇳🇮🇳🇪🇺)",
        "username": "@tgchan_chatbot",
        "token": os.getenv("INT_BOT_TOKEN"),
        # --- НАЧАЛО ИЗМЕНЕНИЙ ---
        "admins": {int(x.strip()) for x in os.getenv("INT_ADMINS", "").split(",") if x}
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---
    },
    'thread': {
        "name": "/thread/",
        "description": "ТРЕДЫ - доска для создания тредов",
        "description_en": "THREADS - board for creating threads",
        "username": "@thread_chatbot", 
        "token": os.getenv("THREAD_BOT_TOKEN"),
        # --- НАЧАЛО ИЗМЕНЕНИЙ ---
        "admins": {int(x.strip()) for x in os.getenv("THREAD_ADMINS", "").split(",") if x}
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---
    },
    'test': {
        "name": "/test/",
        "description": "Testground",
        "description_en": "Testground",
        "username": "@tgchan_testbot", 
        "token": os.getenv("TEST_BOT_TOKEN"),
        # --- НАЧАЛО ИЗМЕНЕНИЙ ---
        "admins": {int(x.strip()) for x in os.getenv("TEST_ADMINS", "").split(",") if x}
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---
    }
}

# ========== НОВЫЕ КОНСТАНТЫ ДЛЯ СИСТЕМЫ ТРЕДОВ ==========
THREAD_BOARDS = {'thread', 'test'} # Доски, на которых будет работать система тредов
DATA_DIR = "data"  # Папка для хранения данных (например, архивов тредов)
os.makedirs(DATA_DIR, exist_ok=True) # Гарантируем, что папка существует

# --- Конфигурация уведомлений ---
THREAD_NOTIFY_THRESHOLD = 30 # Порог постов для отправки уведомления об активности
last_checked_post_counter_for_notify = 0 # Глобальный счетчик для уведомителя
THREAD_BUMP_LIMIT_WARNING_THRESHOLD = 40 # За сколько постов до лимита слать уведомление

# --- Конфигурация жизненного цикла тредов ---
MAX_ACTIVE_THREADS = 100 # Макс. активных тредов на доске
MAX_POSTS_PER_THREAD = 300 # Макс. постов в треде до архивации

# --- Конфигурация кулдаунов ---
THREAD_CREATE_COOLDOWN_USER = 1800  # 30 минут в секундах
THREAD_HISTORY_COOLDOWN = 300 # 5 минут в секундах
OP_COMMAND_COOLDOWN = 60 # 1 минута кулдауна для команд модерации ОПа в треде
LOCATION_SWITCH_COOLDOWN = 5 # 5 секунд на смену локации (вход/выход)
SUMMARIZE_COOLDOWN = 300 # 5 минут в секундах для команды /summarize

class ThreadCreateStates(StatesGroup):
    waiting_for_op_post = State()      # Состояние ожидания текста ОП-поста
    waiting_for_confirmation = State() # Состояние ожидания подтверждения создания


# Извлекаем список ID досок для удобства
BOARDS = list(BOARD_CONFIG.keys())

# --- Конфигурация канала для архивов ---
# ID канала, куда будут поститься архивы. Должен быть в переменных окружения.
ARCHIVE_CHANNEL_ID = int(os.getenv("ARCHIVE_CHANNEL_ID", -1002827087363))
# Базовый URL для формирования прямых ссылок на файлы в GitHub-репозитории
GITHUB_ARCHIVE_BASE_URL = "https://github.com/shlomapetia/dvachbot-backup/blob/main/archives"
# ID бота из BOARD_CONFIG, который имеет права администратора в канале архивов
ARCHIVE_POSTING_BOT_ID = 'test' 

# Очереди сообщений для каждой доски
message_queues = {board: asyncio.Queue(maxsize=9000) for board in BOARDS}

# ========== Глобальные переменные и настройки ==========
GLOBAL_BOTS = {} # Словарь для хранения всех экземпляров ботов
is_shutting_down = False
git_executor = ThreadPoolExecutor(max_workers=1)
save_executor = ThreadPoolExecutor(max_workers=os.cpu_count() or 1) # Executor для сохранения файлов
git_semaphore = asyncio.Semaphore(1)
post_counter_lock = asyncio.Lock()
storage_lock = asyncio.Lock()  # Блокировка для доступа к messages_storage, post_to_messages и т.д.


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
    # --- НАЧАЛО ИЗМЕНЕНИЙ ---
    'last_summarize_time': 0, # Время последнего успешного вызова /summarize
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---
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
    # --- НОВЫЕ СТРУКТУРЫ ДЛЯ СИСТЕМЫ ТРЕДОВ ---
    'threads_data': {},  # {thread_id: {'op_id', 'title', ...}}
    'user_state': {},    # {user_id: {'location', 'last_seen_main', ...}}
    'thread_locks': defaultdict(asyncio.Lock), # !!! ДОБАВЛЕНО: Словарь для блокировок тредов
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

# --- Конфигурация "счастливых" номеров постов ---
SPECIAL_NUMERALS_CONFIG = {
    # Уровень: { 'label': 'Название', 'emojis': ('эмодзи', ...) }
    3: {'label': 'Трипл', 'emojis': ('🎲', '✨', '🎉', '🎰')},
    4: {'label': 'Квадрипл', 'emojis': ('🎯', '🚀', '🔥', '🍀')},
    5: {'label': 'Пентипл', 'emojis': ('🏆', '⭐', '🥇', '💫')},
    6: {'label': 'Секстипл', 'emojis': ('💎', '👑', ' JACKPOT ', '🤩')},
    7: {'label': 'Септипл', 'emojis': ('🤯', '🌌', '🌠', '🪐')},
    8: {'label': 'Октипл', 'emojis': ('🦄', '👽', '💠', '🔱')}
}

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


# Хранит информацию о текущих медиа-группах: media_group_id -> данные
current_media_groups = {}
media_group_timers = {}
user_spam_locks = defaultdict(asyncio.Lock)
media_group_creation_lock = asyncio.Lock() # <-- ДОБАВЛЕНО

# Отключаем стандартную обработку сигналов в aiogram
os.environ["AIORGRAM_DISABLE_SIGNAL_HANDLERS"] = "1"

# Глобальные переменные для cooldown /deanon
DEANON_COOLDOWN = 180  # 3 минуты
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
    "🕵️‍♂️ Деанонеры в перерыве на минет. Подожди, пидор",
    "🔞 Слишком много деанона! ФСБ запыхалась!",
    "♻️ Перезарядка деанон-пушки. 060 секунд, сука!",
    "Заебали уже! Подожди 300 секунд, гандон."
]

SPAM_RULES = {
    'text': {
        'max_repeats': 5,  # Макс одинаковых текстов подряд
        'min_length': 2,  # Минимальная длина текста
        'window_sec': 15,  # Окно для проверки (сек)
        'max_per_window': 7,  # Макс сообщений в окне
        'penalty': [60, 120, 300]  # Шкала наказаний: [1 мин, 5мин, 10 мин]
    },
    'sticker': {
        'max_repeats': 3, # <-- ДОБАВЛЕНО
        'max_per_window': 6,  # 6 стикеров за 18 сек
        'window_sec': 18,
        'penalty': [60, 300, 600]  # 1мин, 10мин, 15 мин
    },
    'animation': {  # Гифки
        'max_repeats': 3, # <-- ДОБАВЛЕНО
        'max_per_window': 5,  # 5 гифки за 24 сек
        'window_sec': 24,
        'penalty': [60, 300, 600]  # 1мин, 10мин, 15 мин
    }
}




def restore_backup_on_start():
    """
    Забирает файлы из backup-репозитория с повторными попытками.
    Возвращает True в случае успеха, False в случае полной неудачи.
    """
    repo_url = "https://github.com/shlomapetia/dvachbot-backup.git"
    backup_dir = "/app/backup"
    max_attempts = 3  # Количество попыток
    
    # --- НАЧАЛО ИЗМЕНЕНИЙ: Проверка на наличие локальных файлов ---
    # Если файлы state уже существуют локально, считаем это успешным запуском,
    # чтобы не зависеть от GitHub при каждом перезапуске.
    if glob.glob("*_state.json"):
        print("✅ Локальные файлы состояния найдены, восстановление из git не требуется.")
        return True
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---
    
    for attempt in range(max_attempts):
        try:
            if os.path.exists(backup_dir):
                shutil.rmtree(backup_dir)
            
            print(f"Попытка клонирования бэкапа #{attempt+1}...")
            subprocess.run([
                "git", "clone", "--depth", "1", repo_url, backup_dir
            ], check=True, timeout=120)
            
            backup_files = glob.glob(os.path.join(backup_dir, "*_state.json"))
            if not backup_files:
                # Это не ошибка, а случай, когда бэкап еще не создан.
                print("⚠️ Файлы для восстановления в репозитории не найдены, запуск с чистого состояния.")
                return True # Считаем успешным, чтобы бот мог создать первое состояние.
            
            backup_files += glob.glob(os.path.join(backup_dir, "*_reply_cache.json"))
            
            for src_path in backup_files:
                shutil.copy2(src_path, os.getcwd())
            
            print(f"✅ Восстановлено {len(backup_files)} файлов из backup")
            return True # Успешное восстановление
        
        except Exception as e:
            print(f"❌ Ошибка при восстановлении (попытка {attempt+1}): {e}")
            time.sleep(5)  # Пауза перед повторной попыткой
    
    print("⛔ КРИТИЧЕСКАЯ ОШИБКА: Все попытки восстановления из git провалились.")
    return False # Полная неудача

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
    GIT_LOCAL_TIMEOUT = 15 # Секунд на каждую локальную git-операцию
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

        # --- Копирование файлов состояний ---
        files_to_copy_state = glob.glob(os.path.join(os.getcwd(), "*_state.json"))
        files_to_copy_state += glob.glob(os.path.join(os.getcwd(), "*_reply_cache.json"))
        
        for src_path in files_to_copy_state:
            shutil.copy2(src_path, work_dir)

        # --- НАЧАЛО ИЗМЕНЕНИЙ: Копирование архивов тредов ---
        archives_dir_in_repo = os.path.join(work_dir, "archives")
        os.makedirs(archives_dir_in_repo, exist_ok=True) # Создаем папку archives в репозитории

        # Ищем все HTML архивы в локальной папке DATA_DIR (по умолчанию "data")
        files_to_copy_archives = glob.glob(os.path.join(DATA_DIR, "archive_*.html"))
        
        if files_to_copy_archives:
            print(f"Git: Найдено {len(files_to_copy_archives)} файлов архивов для бэкапа.")
            for src_path in files_to_copy_archives:
                shutil.copy2(src_path, archives_dir_in_repo)
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---

        if not files_to_copy_state and not files_to_copy_archives:
            print("⚠️ Нет файлов для бэкапа, пропуск.")
            return True # Успешное завершение, так как нет работы

        # --- Локальные Git операции (быстрые, короткий таймаут) ---
        subprocess.run(["git", "-C", work_dir, "config", "user.name", "Backup Bot"], check=True, timeout=GIT_LOCAL_TIMEOUT)
        subprocess.run(["git", "-C", work_dir, "config", "user.email", "backup@dvachbot.com"], check=True, timeout=GIT_LOCAL_TIMEOUT)
        subprocess.run(["git", "-C", work_dir, "add", "."], check=True, timeout=GIT_LOCAL_TIMEOUT)
        
        # Проверяем, есть ли что коммитить
        status_result = subprocess.run(["git", "-C", work_dir, "status", "--porcelain"], capture_output=True, text=True, timeout=GIT_LOCAL_TIMEOUT)
        if not status_result.stdout:
            print("✅ Git: Нет изменений для коммита.")
            return True

        commit_msg = f"Backup: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}"
        subprocess.run(["git", "-C", work_dir, "commit", "-m", commit_msg], check=True, timeout=GIT_LOCAL_TIMEOUT)

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
dp.update.middleware(BoardMiddleware()) # <-- ДОБАВЛЕНО
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
    """Добавляет (You) к упоминаниям постов, если это ответ на свой же пост.
    Должна вызываться из-под storage_lock."""
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
REPLY_CACHE = 15000  # сколько постов держать в кэше для каждой доски
REPLY_FILE = "reply_cache.json"  # отдельный файл для reply
MAX_MESSAGES_IN_MEMORY = 15000  # храним только последние 5900 постов в общей памяти


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
    "Чат умер? Заведи новый тред с порнухой!",
    "Приведи друга - получи бесплатный виртуальный хуй",
    "Твой друг всё ещё в вк? СРОЧНО СПАСАЙ ЕГО ОТ НОРМИСОВСТВА!",
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

DVACH_STATS_CAPTIONS = [
    "Какие же мы все долбаёбы",
    "вам самим не заебало ещё?",
    "Двачеры, сколько вас тут набежало, а?",
    "Покажите мамке — она ахуеет!",
    "Тгачеры, давайте до миллиона дожимать!",
    "Тут все свои, даже если все — долбоебы!",
    "Больше постов — больше срачей!",
    "Соси, если мало постишь!",
    "Мне не нравится эта статистика",
    "Перегоняем сосач.",
    "Может ну его нахуй, а?",
    "Вот кому-то делать нехуй)",
    "У нас просто ебанутый чат.",
    "99% постов от ботов, нейронок и цепей Маркова",
    "👁️‍🗨️ Количество реальных анонов (не ботов) - 3 чел.",
    "Спасибо Анонам.",
    "Абу одобряет такую активность!",
    "Постов столько, что даже Пыня ахуел бы!",
    "В этом чате больше жизни, чем у меня!",
    "Пиши ещё, анон! Не будь овощем!",
    "За каждый пост — плюс к карме, минус к личной жизни!",
    "Гордись, ты в числе этих долбоёбов!",
    "Тут аноны, тут движ, тут пиздец!",
]

DVACH_STATS_CAPTIONS_EN = [
    "Statistics so Abu doesn't cry!",
    "More posts — closer to the banhammer!",
    "How many of you degenerates showed up here?",
    "Show this to your mom — she'll fucking faint!",
    "Not posting enough? Go get a job, loser!",
    "Every post = minus one normie in the world!",
    "100 posts — now you're officially a channer!",
    "Let's push this to a million, anons!",
    "Less posts — more sadness!",
    "If you're not in the top, you're a loser!",
    "Everyone's a friend here, even if all are dumbfucks!",
    "More posts — more shitstorms!",
    "Suck it if you post too little!",
    "Abu approves this activity!",
    "Anons, don't sleep — post more!",
    "More anons here than friends you ever had!",
    "So many posts even Putin would freak out!",
    "If you're reading this stats — you're NOT a bot!",
    "More life in this chat than I ever had!",
    "Post more, anon! Don't be a vegetable!",
    "Every post = karma up, personal life down!",
    "Everyone here is an expert... on everything!",
    "Abu said: post, shit, don't be sad!",
    "Even your mom would post here if she knew how!",
    "Be proud, you're one of these dumbfucks!",
    "Anons, action, and pure chaos here!",
    "Posting means you're alive!",
    "More posts here than brains combined!",
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
            "🍆 Кто-то кончил от твоего поста #{post_num}",
            "🫶 Анон обнял твой пост #{post_num} и заплакал",
        ],
        'negative': [
            "👎 Анон саганул твой пост #{post_num}",
            "🤡 Анон поссал тебе на ебало за #{post_num}",
            "🟥⬇️ Сажа на пост #{post_num}",
            "💩 Анон насрал тебе на пост #{post_num}",
            "👺 Твой пост #{post_num} признан высером года",
            "🟥⬇️ SAGE SAGE SAGE пост #{post_num}",
            "💩 Анон репортнул пост #{post_num}",
            "⬇️ Дизлайк пост #{post_num}",            
            "🤢 Твой пост #{post_num} тупой высер (по мнению анона)",
        ],
        'neutral': [
            "🤔 Анон отреагировал на твой пост #{post_num}",
            "👁️‍🗨️ На пост #{post_num} кто-то посмотрел с подозрением",
            "🫥 Кто-то проигнорил твой пост #{post_num} максимально выразительно",
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
            "🍑 Anon clapped your post #{post_num} cheeks",
            "💦 Post #{post_num} made someone wet",
        ],
        'negative': [
            "👎 Anon disliked your post #{post_num}",
            "🤡 Sage your post #{post_num}",
            "💩 Your post #{post_num} is piece of shit",
            "🤢 Anon says: go fuck with your dumb post #{post_num}",
            "🤮 Anon vomited on your post #{post_num}",
            "💀 Your post #{post_num} got ratioed into oblivion",
        ],
        'neutral': [
            "🤔 Anon reacted to your post #{post_num}",
            "🤔 There is reaction on your post #{post_num}",
            "👽 Someone reacted to post #{post_num} with alien technology",
            "🫠 Your post #{post_num} caused a mild existential crisis",
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
    "@dvach_chatbot - здесь можно срать в чат и не мыть руки",
    "Устал от девочек? Заходи в @dvach_chatbot - тут только мужики и анонимность!",
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
            board_id = get_board_id(telegram_object)
            if board_id:
                board_data[board_id]['users']['active'].discard(user_id)
                print(f"🚫 [{board_id}] Пользователь {user_id} заблокировал бота, удален из активных.")
        return True

    # Обработка сетевых ошибок и конфликтов
    if isinstance(exception, (TelegramNetworkError, TelegramConflictError, aiohttp.ClientError)):
        print(f"🌐 Сетевая ошибка: {type(exception).__name__}: {exception}")
        await asyncio.sleep(10)
        return False

    # Обработка KeyError (проблемы с хранилищем) - логируем, но не останавливаемся
    elif isinstance(exception, KeyError):
        print(f"🔑 Потенциальная проблема с данными (KeyError): {exception}. Пропускаем обработку этого сообщения.")
        return True

    # --- НАЧАЛО ИЗМЕНЕНИЙ: ЛОГИРОВАНИЕ ПОЛНОГО TRACEBACK ---
    # Все остальные ошибки считаются непредвиденными и требуют детальной отладки
    else:
        import traceback
        print("⛔⛔⛔ НЕПРЕДВИДЕННАЯ КРИТИЧЕСКАЯ ОШИБКА ⛔⛔⛔")
        print(f"Exception: {type(exception).__name__}: {exception}")
        
        # Печатаем полный traceback
        traceback.print_exc()

        # Логируем тело update, если оно есть, для контекста
        if update:
            try:
                update_json = update.model_dump_json(exclude_none=True, indent=2)
                print(f"--- Update Context ---\n{update_json}\n--- End Update Context ---")
            except Exception as json_e:
                print(f"Не удалось сериализовать update: {json_e}")
        
        # Возвращаем True, чтобы бот не пытался повторно обработать ошибочный апдейт,
        # но при этом продолжил работу со следующими.
        return True
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---
        
def escape_html(text: str) -> str:
    """Экранирует HTML символы"""
    if not text:
        return text
    return text.replace('&', '&amp;').replace('<', '&lt;').replace(
        '>', '&gt;').replace('"', '&quot;')


def sanitize_html(text: str) -> str:
    """
    Нейтрализует опасные HTML-теги (ссылки), сохраняя безопасные теги форматирования.
    Используется для предотвращения HTML-инъекций и деанонимизации через ссылки.
    Сохраняет: <b>, <i>, <u>, <s>, <code>.
    Удаляет: <a>.
    """
    if not text:
        return text
    # Удаляем открывающие и закрывающие теги <a>, включая все их атрибуты (href и т.д.).
    # Контент внутри тега при этом остается.
    # Флаг re.IGNORECASE для обработки <A HREF...>, <A> и т.д.
    sanitized_text = re.sub(r'</?a\b[^>]*>', '', text, flags=re.IGNORECASE)
    return sanitized_text


def is_admin(uid: int, board_id: str) -> bool:
    """Проверяет, является ли пользователь админом на КОНКРЕТНОЙ доске."""
    if not board_id:
        return False
    return uid in BOARD_CONFIG.get(board_id, {}).get('admins', set())

async def get_board_activity_last_hours(board_id: str, hours: int = 2) -> float:
    """Подсчитывает среднее количество постов в час для указанной доски за последние N часов."""
    if hours <= 0:
        return 0.0

    now = datetime.now(UTC)
    time_threshold = now - timedelta(hours=hours)
    post_count = 0

    # --- НАЧАЛО ИЗМЕНЕНИЙ ---
    # Итерация происходит под блокировкой, но без создания полной копии
    # всего хранилища, что значительно экономит память и процессорное время.
    async with storage_lock:
        for post_data in messages_storage.values():
            if post_data.get('board_id') == board_id and post_data.get('timestamp', now) > time_threshold:
                post_count += 1
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---
            
    activity = post_count / hours
    return activity
    
def _sync_save_board_state(board_id: str, data_to_save: dict):
    """Синхронная, блокирующая функция для сохранения state.json. Работает только с переданными данными."""
    state_file = f"{board_id}_state.json"
    try:
        with open(state_file, 'w', encoding='utf-8') as f:
            json.dump(data_to_save, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        print(f"⛔ [{board_id}] Ошибка в потоке сохранения state: {e}")
        return False

async def save_board_state(board_id: str):
    """Асинхронная обертка для неблокирующего и потокобезопасного сохранения state.json."""
    loop = asyncio.get_running_loop()
    
    # Потокобезопасно читаем и копируем все необходимые данные под блокировкой
    async with storage_lock:
        b_data = board_data[board_id]
        post_counter_to_save = state.get('post_counter') if board_id == 'b' else None
        
        # --- НАЧАЛО ИЗМЕНЕНИЙ ---
        # Подготавливаем shadow_mutes к сериализации в JSON
        # Конвертируем datetime в строки формата ISO 8601
        shadow_mutes_to_save = {
            str(user_id): expiry.isoformat()
            for user_id, expiry in b_data['shadow_mutes'].items()
        }
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---
        
        # Создаем словарь с копией данных для передачи в другой поток
        data_for_sync_func = {
            'users_data': {
                'active': list(b_data['users']['active']),
                'banned': list(b_data['users']['banned']),
            },
            'message_counter': dict(b_data['message_counter']),
            'board_post_count': b_data.get('board_post_count', 0),
            'shadow_mutes': shadow_mutes_to_save, # <-- ДОБАВЛЕНО
        }
        if post_counter_to_save is not None:
            data_for_sync_func['post_counter'] = post_counter_to_save

    # Передаем скопированные данные в функцию, выполняемую в другом потоке
    await loop.run_in_executor(
        save_executor,
        _sync_save_board_state,
        board_id,
        data_for_sync_func
    )
def _sync_save_threads_data(board_id: str):
    """Синхронная функция для сохранения данных о тредах."""
    if board_id not in THREAD_BOARDS:
        return True
    
    threads_file = os.path.join(DATA_DIR, f"{board_id}_threads.json")
    try:
        original_data = board_data[board_id].get('threads_data', {})
        
        # Создаем копию данных, пригодную для сериализации
        data_to_save = {}
        for thread_id, thread_info in original_data.items():
            # Копируем всю информацию о треде
            serializable_info = thread_info.copy()
            # Если есть 'subscribers' и это set, конвертируем в list
            if 'subscribers' in serializable_info and isinstance(serializable_info['subscribers'], set):
                serializable_info['subscribers'] = list(serializable_info['subscribers'])
            data_to_save[thread_id] = serializable_info

        with open(threads_file, 'w', encoding='utf-8') as f:
            json.dump(data_to_save, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        print(f"⛔ [{board_id}] Ошибка в потоке сохранения _threads.json: {e}")
        return False

def _sync_save_user_states(board_id: str):
    """Синхронная функция для сохранения состояний пользователей в тредах."""
    if board_id not in THREAD_BOARDS:
        return True
        
    user_states_file = os.path.join(DATA_DIR, f"{board_id}_user_states.json")
    try:
        data_to_save = board_data[board_id].get('user_state', {})
        with open(user_states_file, 'w', encoding='utf-8') as f:
            json.dump(data_to_save, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        print(f"⛔ [{board_id}] Ошибка в потоке сохранения _user_states.json: {e}")
        return False

async def save_user_states(board_id: str):
    """Асинхронная обертка для сохранения состояний пользователей в тредах."""
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(
        save_executor,
        _sync_save_user_states,
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
        # --- НАЧАЛО ИЗМЕНЕНИЙ ---
        if board_id in THREAD_BOARDS:
            save_tasks.append(save_threads_data(board_id))
            save_tasks.append(save_user_states(board_id))
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---

    # 2. Запускаем все задачи сохранения одновременно и ждем их завершения
    await asyncio.gather(*save_tasks)
    
    print("💾 Все файлы состояний обновлены, пушим в GitHub...")
    success = await git_commit_and_push()
    if success:
        print("✅ Бэкап всех досок успешно отправлен в GitHub.")
    else:
        print("❌ Не удалось отправить бэкап в GitHub.")
    return success

def _sync_save_reply_cache(
    board_id: str,
    recent_board_posts: list,
    all_post_to_messages: dict,
    all_message_to_post: dict,
    all_messages_storage_meta: dict
):
    """Синхронная, блокирующая функция для сохранения кэша. Работает с переданными ей данными."""
    reply_file = f"{board_id}_reply_cache.json"
    try:
        recent_posts_set = set(recent_board_posts)

        if not recent_posts_set:
            if os.path.exists(reply_file):
                os.remove(reply_file)
            return True

        # Собираем данные для сохранения из переданных копий, фильтруя по recent_posts_set
        new_data = {
            "post_to_messages": {
                str(p_num): data
                for p_num, data in all_post_to_messages.items()
                if p_num in recent_posts_set
            },
            "message_to_post": {
                f"{uid}_{mid}": p_num
                for (uid, mid), p_num in all_message_to_post.items()
                if p_num in recent_posts_set
            },
            # Фильтруем метаданные по списку recent_board_posts, как в оригинале
            "messages_storage_meta": {
                str(p_num): all_messages_storage_meta[p_num]
                for p_num in recent_board_posts
                if p_num in all_messages_storage_meta
            }
        }

        # Сохраняем новые данные
        with open(reply_file, 'w', encoding='utf-8') as f:
            json.dump(new_data, f, ensure_ascii=False, indent=2)

        return True

    except Exception as e:
        print(f"⛔ [{board_id}] Ошибка в потоке сохранения reply_cache: {str(e)[:200]}")
        return False

async def save_reply_cache(board_id: str):
    """Асинхронная обертка для неблокирующего и потокобезопасного сохранения кэша ответов."""
    # --- НАЧАЛО ИЗМЕНЕНИЙ: Оптимизированный сбор данных ---
    async with storage_lock:
        # 1. Определяем все посты доски
        board_post_keys = {
            p_num for p_num, data in messages_storage.items()
            if data.get("board_id") == board_id
        }
        
        # 2. Определяем недавние посты для этой доски
        recent_board_posts = sorted(list(board_post_keys))[-REPLY_CACHE:]
        recent_board_posts_set = set(recent_board_posts)

        # 3. Эффективно собираем только необходимые данные, а не копируем всё
        post_to_messages_copy = {
            p_num: data.copy()
            for p_num, data in post_to_messages.items()
            if p_num in recent_board_posts_set
        }
        
        message_to_post_copy = {
            key: p_num
            for key, p_num in message_to_post.items()
            if p_num in recent_board_posts_set
        }
        
        messages_storage_meta_copy = {
            p_num: {
                "author_id": data.get("author_id", ""),
                "timestamp": data.get("timestamp", datetime.now(UTC)).isoformat(),
                "author_message_id": data.get("author_message_id"),
                "board_id": data.get("board_id")
            }
            # Итерируемся только по недавним постам, а не по всему хранилищу
            for p_num in recent_board_posts
            if (data := messages_storage.get(p_num))
        }
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---

    loop = asyncio.get_running_loop()
    await loop.run_in_executor(
        save_executor,
        _sync_save_reply_cache,
        board_id,
        recent_board_posts,
        post_to_messages_copy,
        message_to_post_copy,
        messages_storage_meta_copy
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

            # --- НАЧАЛО ИЗМЕНЕНИЙ ---
            # Загружаем shadow_mutes с преобразованием типов
            loaded_shadow_mutes = data.get('shadow_mutes', {})
            if loaded_shadow_mutes:
                deserialized_shadow_mutes = {}
                now_utc = datetime.now(UTC)
                for user_id_str, expiry_str in loaded_shadow_mutes.items():
                    try:
                        # Конвертируем ключ в int, а строку времени обратно в datetime
                        user_id = int(user_id_str)
                        expiry_dt = datetime.fromisoformat(expiry_str)
                        # Добавляем только те муты, которые еще не истекли
                        if expiry_dt > now_utc:
                             deserialized_shadow_mutes[user_id] = expiry_dt
                    except (ValueError, TypeError) as e:
                        # Логируем ошибку, если данные в файле повреждены
                        print(f"[{board_id}] Ошибка десериализации shadow_mute для '{user_id_str}': {e}")
                
                b_data['shadow_mutes'] = deserialized_shadow_mutes
            # --- КОНЕЦ ИЗМЕНЕНИЙ ---
            
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

            # --- НАЧАЛО ИНТЕГРАЦИИ ЛОГИКИ ТРЕДОВ ---
            if board_id in THREAD_BOARDS:
                threads_file = os.path.join(DATA_DIR, f'{board_id}_threads.json')
                if os.path.exists(threads_file):
                    try:
                        with open(threads_file, 'r', encoding='utf-8') as f:
                            loaded_threads_data = json.load(f)
                        
                        # Обратная конвертация 'subscribers' из list в set
                        for thread_id, thread_info in loaded_threads_data.items():
                            if 'subscribers' in thread_info and isinstance(thread_info['subscribers'], list):
                                thread_info['subscribers'] = set(thread_info['subscribers'])
                        
                        b_data['threads_data'] = loaded_threads_data
                        print(f"[{board_id}] Данные тредов загружены и обработаны.")

                    except (json.JSONDecodeError, IOError) as e:
                        print(f"[{board_id}] Ошибка загрузки _threads.json: {e}")
                        b_data['threads_data'] = {}
                
                user_states_file = os.path.join(DATA_DIR, f'{board_id}_user_states.json')
                if os.path.exists(user_states_file):
                    try:
                        with open(user_states_file, 'r', encoding='utf-8') as f:
                            # Ключи в JSON всегда строки, преобразуем их обратно в int
                            loaded_states = json.load(f)
                            b_data['user_state'] = {int(k): v for k, v in loaded_states.items()}
                            print(f"[{board_id}] Состояния пользователей тредов загружены.")
                    except (json.JSONDecodeError, IOError) as e:
                        print(f"[{board_id}] Ошибка загрузки _user_states.json: {e}")
                        b_data['user_state'] = {}
            # --- КОНЕЦ ИНТЕГРАЦИИ ЛОГИКИ ТРЕДОВ ---

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

    # --- НАЧАЛО ИЗМЕНЕНИЙ: Управление бэкапом через Task для контроля таймаута ---
    # 3. Сохраняем и пушим данные. САМЫЙ ВАЖНЫЙ ЭТАП.
    print("💾 Попытка финального сохранения и бэкапа в GitHub (таймаут 50 секунд)...")
    backup_task = asyncio.create_task(save_all_boards_and_backup())
    
    try:
        await asyncio.wait_for(backup_task, timeout=50.0)
        print("✅ Финальный бэкап успешно завершен в рамках таймаута.")
    except asyncio.TimeoutError:
        print("⛔ КРИТИЧЕСКАЯ ОШИБКА: Финальный бэкап не успел выполниться за 50 секунд и был прерван!")
        backup_task.cancel() # Принудительно отменяем задачу
        try:
            await backup_task # Даем возможность обработать отмену
        except asyncio.CancelledError:
            print("ℹ️ Задача бэкапа успешно отменена.")
    except Exception as e:
        print(f"⛔ КРИТИЧЕСКАЯ ОШИБКА: Не удалось выполнить финальный бэкап: {e}")
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---

    # 4. Останавливаем всё остальное, несмотря на результат бэкапа
    print("Завершение остальных компонентов...")
    try:
        if 'healthcheck_site' in globals() and globals()['healthcheck_site']:
            await globals()['healthcheck_site'].stop()
            print("🛑 Healthcheck server stopped")

        # Отправляем сигнал на завершение пулов потоков, не дожидаясь их.
        git_executor.shutdown(wait=False, cancel_futures=True)
        save_executor.shutdown(wait=False, cancel_futures=True)
        print("🛑 Executors shutdown initiated.")

        if hasattr(dp, 'storage') and dp.storage:
            await dp.storage.close()
        
        print("✅ Сессии ботов будут закрыты централизованно.")

    except Exception as e:
        print(f"Error during final shutdown procedures: {e}")

    # Отменяем оставшиеся задачи
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
    
    await asyncio.gather(*tasks, return_exceptions=True)
    print("✅ Все задачи остановлены, завершаем работу.")
    
def _sync_clean_message_to_post(
    current_message_to_post: dict, 
    actual_post_nums: set
) -> dict:
    """
    Синхронная функция для выполнения ресурсоемкой фильтрации словаря.
    Удаляет только те записи, которые ссылаются на уже удаленные посты.
    """
    # Создаем новый словарь только с валидными записями
    valid_entries = {
        key: post_num
        for key, post_num in current_message_to_post.items()
        if post_num in actual_post_nums
    }
    return valid_entries

async def auto_memory_cleaner():
    """Полная и честная очистка мусора каждые 10 минут с выносом тяжелых операций."""
    cleanup_counter = 0
    loop = asyncio.get_running_loop()

    while True:
        cleanup_counter += 1
        await asyncio.sleep(600)  # 10 минут

        # --- Блок 1: Очистка старых постов (быстрые операции под блокировкой) ---
        deleted_posts_count = 0
        async with storage_lock:
            if len(messages_storage) > MAX_MESSAGES_IN_MEMORY:
                to_delete_count = len(messages_storage) - MAX_MESSAGES_IN_MEMORY
                oldest_post_keys = sorted(messages_storage.keys())[:to_delete_count]
                deleted_posts_count = len(oldest_post_keys)
                
                # Удаляем из messages_storage и post_to_messages
                for post_num in oldest_post_keys:
                    messages_storage.pop(post_num, None)
                    post_to_messages.pop(post_num, None)

        if deleted_posts_count > 0:
            print(f"🧹 Очистка памяти: удалено {deleted_posts_count} старых постов.")
        
        # --- НАЧАЛО ИЗМЕНЕНИЙ: Блок 2 заменен на новую, корректную логику ---
        # Неблокирующая очистка message_to_post от ссылок на удаленные посты
        async with storage_lock:
            # Копируем данные для безопасной работы в другом потоке
            actual_post_nums = set(messages_storage.keys())
            message_to_post_copy = message_to_post.copy()
        
        # Выполняем тяжелую фильтрацию в отдельном потоке
        cleaned_message_to_post = await loop.run_in_executor(
            save_executor,
            _sync_clean_message_to_post,
            message_to_post_copy,
            actual_post_nums
        )
        
        # Атомарно обновляем основной словарь
        async with storage_lock:
            initial_count = len(message_to_post)
            message_to_post.clear()
            message_to_post.update(cleaned_message_to_post)
            removed_count = initial_count - len(message_to_post)
        
        if removed_count > 0:
            print(f"🧹 Очистка message_to_post: удалено {removed_count} неактуальных связей (осталось {len(message_to_post)})")
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---

        # --- Блок 3: Очистка данных досок (без изменений) ---
        async with storage_lock:
            for board_id in BOARDS:
                b_data = board_data[board_id]
                now_utc = datetime.now(UTC)

                if len(b_data['message_counter']) > 100:
                    top_users = sorted(b_data['message_counter'].items(), key=lambda x: x[1], reverse=True)[:100]
                    b_data['message_counter'] = defaultdict(int, top_users)

                inactive_threshold = now_utc - timedelta(hours=12)
                active_mutes = {uid for uid, expiry in b_data.get('mutes', {}).items() if expiry > now_utc}
                active_shadow_mutes = {uid for uid, expiry in b_data.get('shadow_mutes', {}).items() if expiry > now_utc}
                
                users_to_purge = [
                    uid for uid, last_time in b_data.get('last_activity', {}).items()
                    if last_time < inactive_threshold and uid not in active_mutes and uid not in active_shadow_mutes
                ]
                
                for user_id in users_to_purge:
                    b_data['last_activity'].pop(user_id, None)
                    b_data['last_texts'].pop(user_id, None)
                    b_data['last_stickers'].pop(user_id, None)
                    b_data['last_animations'].pop(user_id, None)
                    b_data['spam_violations'].pop(user_id, None)
                    b_data['spam_tracker'].pop(user_id, None)
                    b_data['last_user_msgs'].pop(user_id, None)

                for user_id in list(b_data.get('mutes', {}).keys()):
                    if b_data['mutes'][user_id] < now_utc:
                        b_data['mutes'].pop(user_id, None)
                for user_id in list(b_data.get('shadow_mutes', {}).keys()):
                    if b_data['shadow_mutes'][user_id] < now_utc:
                        b_data['shadow_mutes'].pop(user_id, None)
                
                spam_tracker_board = b_data['spam_tracker']
                window_sec = SPAM_RULES.get('text', {}).get('window_sec', 15)
                window_start = now_utc - timedelta(seconds=window_sec)
                for user_id in list(spam_tracker_board.keys()):
                    spam_tracker_board[user_id] = [t for t in spam_tracker_board[user_id] if t > window_start]
                    if not spam_tracker_board[user_id]:
                        del spam_tracker_board[user_id]
        
        # --- Блок 4: Очистка трекера реакций (без изменений) ---
        now_ts = time.time()
        tracker_inactive_threshold_sec = 24 * 3600
        keys_to_delete_from_tracker = [
            author_id for author_id, timestamps in author_reaction_notify_tracker.items()
            if not timestamps or (now_ts - timestamps[-1] > tracker_inactive_threshold_sec)
        ]
        if keys_to_delete_from_tracker:
            for author_id in keys_to_delete_from_tracker:
                del author_reaction_notify_tracker[author_id]

        gc.collect()
        print(f"🧹 Очистка памяти завершена. Следующая через 10 минут.")
        
async def board_statistics_broadcaster():
    """Раз в час собирает общую статистику и рассылает на каждую доску."""
    await asyncio.sleep(300)

    while True:
        try:
            await asyncio.sleep(3600)

            now = datetime.now(UTC)
            hour_ago = now - timedelta(hours=1)
            
            posts_per_hour = defaultdict(int)
            # --- НАЧАЛО ИЗМЕНЕНИЙ ---
            async with storage_lock:
                # Итерация по данным теперь происходит внутри блокировки,
                # что предотвращает ошибки изменения словаря (RuntimeError)
                # и является более эффективным по памяти, чем создание полной копии.
                for post_data in messages_storage.values():
                    b_id = post_data.get('board_id')
                    if b_id and post_data.get('timestamp', now) > hour_ago:
                        posts_per_hour[b_id] += 1
            # --- КОНЕЦ ИЗМЕНЕНИЙ ---
            
            for board_id in BOARDS:
                if board_id == 'test':
                    continue

                activity = await get_board_activity_last_hours(board_id, hours=2)
                if activity < 40:
                    print(f"ℹ️ [{board_id}] Пропуск отправки статистики, активность слишком низкая: {activity:.1f} п/ч (требуется > 40).")
                    continue

                b_data = board_data[board_id]
                recipients = b_data['users']['active'] - b_data['users']['banned']
                if not recipients:
                    continue

                stats_lines = []
                for b_id_inner, config_inner in BOARD_CONFIG.items():
                    if b_id_inner == 'test': continue
                    hour_stat = posts_per_hour[b_id_inner]
                    total_stat = board_data[b_id_inner].get('board_post_count', 0)
                    if board_id == 'int':
                        line_template = f"<b>{config_inner['name']}</b> - {hour_stat} pst/hr, total: {total_stat}"
                    else:
                        line_template = f"<b>{config_inner['name']}</b> - {hour_stat} пст/час, всего: {total_stat}"
                    stats_lines.append(line_template)
                
                header_text = "📊 Boards Statistics:\n" if board_id == 'int' else "📊 Статистика досок:\n"
                full_stats_text = header_text + "\n".join(stats_lines)
                header = "### Statistics ###" if board_id == 'int' else "### Статистика ###"

                if random.random() < 0.66:
                    if board_id == 'int':
                        dvach_caption = random.choice(DVACH_STATS_CAPTIONS_EN)
                    else:
                        dvach_caption = random.choice(DVACH_STATS_CAPTIONS)
                    full_stats_text = f"{full_stats_text}\n\n<i>{dvach_caption}</i>"

                _, post_num = await format_header(board_id)
                content = {"type": "text", "header": header, "text": full_stats_text, "is_system_message": True}
                
                async with storage_lock:
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
        
        # Выбираем правильный текст помощи и генерируем список досок на нужном языке
        if board_id == 'int':
            base_help_text = HELP_TEXT_EN
            boards_header = "🌐 <b>All boards:</b>"
            board_links = "\n".join(
                f"<b>{config['name']}</b> {config['description_en']} - {config['username']}"
                for b_id, config in BOARD_CONFIG.items() if b_id != 'test'
            )
            # --- НАЧАЛО ИЗМЕНЕНИЙ ---
            if board_id in THREAD_BOARDS:
                thread_info = (
                    "\n\n<b>This board supports threads!</b>\n"
                    "/create &lt;title&gt; - Create a new thread\n"
                    "/threads - View active threads\n"
                    "/leave - Return to the main board from a thread"
                )
            else:
                thread_info = ""
            # --- КОНЕЦ ИЗМЕНЕНИЙ ---

        else:
            base_help_text = HELP_TEXT
            boards_header = "🌐 <b>Все доски:</b>"
            board_links = "\n".join(
                f"<b>{config['name']}</b> {config['description']} - {config['username']}"
                for b_id, config in BOARD_CONFIG.items() if b_id != 'test'
            )
            # --- НАЧАЛО ИЗМЕНЕНИЙ ---
            if board_id in THREAD_BOARDS:
                thread_info = (
                    "\n\n<b>На этой доске есть треды!</b>\n"
                    "/create &lt;заголовок&gt; - Создать новый тред\n"
                    "/threads - Посмотреть активные треды\n"
                    "/leave - Вернуться на доску из треда"
                )
            else:
                thread_info = ""
            # --- КОНЕЦ ИЗМЕНЕНИЙ ---

        # Собираем финальное сообщение
        full_help_text = (
            f"{base_help_text}\n"
            f"{thread_info}\n\n" # Добавляем блок с информацией о тредах
            f"{boards_header}\n{board_links}"
        )
        
        # Сохраняем готовый текст для использования в /start и /help
        b_data['start_message_text'] = full_help_text
        
        print(f"📌 [{board_id}] Текст для команды /start и закрепа подготовлен.")

async def get_board_chunk(board_id: str, hours: int = 6, thread_id: str | None = None) -> str:
    """Собирает и очищает текстовый чанк постов доски или конкретного треда для саммаризации."""
    now = datetime.now(UTC)
    time_threshold = now - timedelta(hours=hours)
    lines = []
    
    # --- НАЧАЛО ИЗМЕНЕНИЙ: Адаптация логики для тредов ---
    async with storage_lock:
        storage_copy = list(messages_storage.values())

    post_iterator = storage_copy
    
    # Если указан thread_id, фильтруем посты только из него
    if thread_id:
        b_data = board_data[board_id]
        thread_info = b_data.get('threads_data', {}).get(thread_id)
        if not thread_info:
            return "" # Возвращаем пустую строку, если тред не найден
            
        thread_post_nums = set(thread_info.get('posts', []))
        # Фильтруем основной итератор, оставляя только посты из нужного треда
        post_iterator = [p for p_num, p in messages_storage.items() if p_num in thread_post_nums]
        # Для треда игнорируем ограничение по времени (hours)
        time_threshold = datetime.min.replace(tzinfo=UTC)

    for post in post_iterator:
        try:
            # Проверка на доску остается в любом случае
            if post.get('board_id') != board_id:
                continue
            # Для основного чанка - проверка по времени, для треда - она будет проигнорирована
            if post.get('timestamp', now) < time_threshold:
                continue
            if post.get('author_id') == 0: # Игнорируем системные сообщения
                continue

            content = post.get('content', {})
            ttype = content.get('type')

            if ttype == 'text':
                text = content.get('text', '')
                text = clean_html_tags(text)
                text = re.sub(r'^(Пост №\d+.*?\n|Post No\.\d+.*?\n)', '', text, flags=re.MULTILINE)
                text = re.sub(r'^(###.*?###|<i>.*?</i>)\s*\n?', '', text, flags=re.MULTILINE)
                text = text.strip()
                if text:
                    lines.append(text)
        except Exception as e:
            print(f"[summarize] Error while chunking post: {e}, post: {post}")
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---

    full_text = "\n".join(lines)
    cleaned_chunk = re.sub(r'\n{2,}', '\n', full_text).strip()

    context_name = f"thread {thread_id}" if thread_id else f"board {board_id}"
    print(f"[summarize] Chunk for {context_name} built, len={len(cleaned_chunk)}")
    
    return cleaned_chunk[:35000]
    
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
        # --- НАЧАЛО ИЗМЕНЕНИЙ ---
        # Очищаем трекер после фиксации нарушения, чтобы избежать повторного бана
        del b_data['spam_tracker'][user_id]
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---
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

def _get_random_header_prefix(lang: str = 'ru') -> str:
    """Вспомогательная функция для генерации случайного префикса заголовка."""
    rand_prefix = random.random()
    
    if lang == 'en':
        if rand_prefix < 0.005: return "### ADMIN ### "
        if rand_prefix < 0.008: return "Me - "
        if rand_prefix < 0.01: return "Faggot - "
        if rand_prefix < 0.012: return "### DEGENERATE ### "
        if rand_prefix < 0.016: return "Biden - "
        if rand_prefix < 0.021: return "EMPEROR CONAN - "
        return ""

    # Russian prefixes (default)
    if rand_prefix < 0.005: return "### АДМИН ### "
    if rand_prefix < 0.008: return "Абу - "
    if rand_prefix < 0.01: return "Пидор - "
    if rand_prefix < 0.012: return "### ДЖУЛУП ### "
    if rand_prefix < 0.014: return "### Хуесос ### "
    if rand_prefix < 0.016: return "Пыня - "
    if rand_prefix < 0.018: return "Нариман Намазов - "
    if rand_prefix < 0.021: return "ИМПЕРАТОР КОНАН - "
    if rand_prefix < 0.023: return "Антон Бабкин - "
    if rand_prefix < 0.025: return "### НАРИМАН НАМАЗОВ ### "
    if rand_prefix < 0.027: return "### ПУТИН ### "
    if rand_prefix < 0.028: return "Гей - "
    if rand_prefix < 0.030: return "Анархист - "
    if rand_prefix < 0.033: return "### Имбецил ### "
    if rand_prefix < 0.035: return "### ЧМО ### "
    if rand_prefix < 0.037: return "### ОНАНИСТ ### "
    if rand_prefix < 0.040: return "### ЧЕЧЕНЕЦ ### "
    if rand_prefix < 0.042: return "АААААААА - "
    if rand_prefix < 0.044: return "### Аниме девочка ### "
    return ""

async def format_thread_post_header(board_id: str, local_post_num: int, author_id: int, thread_info: dict) -> str:
    """Форматирует заголовок для поста ВНУТРИ треда с локальной нумерацией и меткой (OP)."""
    b_data = board_data[board_id]
    op_marker = ""
    if author_id != 0 and author_id == thread_info.get('op_id'):
        op_marker = " (OP)"

    if b_data['slavaukraine_mode']:
        return f"💙💛 Пiст №{local_post_num}/{MAX_POSTS_PER_THREAD}{op_marker}"
    if b_data['zaputin_mode']:
        return f"🇷🇺 Пост №{local_post_num}/{MAX_POSTS_PER_THREAD}{op_marker}"
    if b_data['anime_mode']:
        return f"🌸 投稿 {local_post_num}/{MAX_POSTS_PER_THREAD} 番{op_marker}"
    if b_data['suka_blyat_mode']:
        return f"💢 Пост №{local_post_num}/{MAX_POSTS_PER_THREAD}{op_marker}"

    rand = random.random()
    circle = ""
    if rand < 0.003: circle = "🔴 "
    elif rand < 0.006: circle = "🟢 "
    
    # --- ИЗМЕНЕНИЕ: Используем новую вспомогательную функцию ---
    prefix = _get_random_header_prefix(lang='ru')
    
    header_text = f"{circle}{prefix}Пост №{local_post_num}/{MAX_POSTS_PER_THREAD}{op_marker}"
    return header_text

async def format_header(board_id: str) -> Tuple[str, int]:
    """Асинхронное форматирование заголовка с блокировкой для безопасного инкремента счетчика постов."""
    async with post_counter_lock:
        state['post_counter'] += 1
        post_num = state['post_counter']
        
        board_data[board_id].setdefault('board_post_count', 0)
        board_data[board_id]['board_post_count'] += 1
    
    if board_id == 'int':
        circle = ""
        rand_circle = random.random()
        if rand_circle < 0.003: circle = "🔴 "
        elif rand_circle < 0.006: circle = "🟢 "
        
        # --- ИЗМЕНЕНИЕ: Используем новую вспомогательную функцию ---
        prefix = _get_random_header_prefix(lang='en')
        header_text = f"{circle}{prefix}Post No.{post_num}"
        return header_text, post_num

    b_data = board_data[board_id]

    if b_data['slavaukraine_mode']:
        return f"💙💛 Пiст №{post_num}", post_num
    if b_data['zaputin_mode']:
        return f"🇷🇺 Пост №{post_num}", post_num
    if b_data['anime_mode']:
        return f"🌸 投稿 {post_num} 番", post_num
    if b_data['suka_blyat_mode']:
        return f"💢 Пост №{post_num}", post_num

    rand = random.random()
    circle = ""
    if rand < 0.003: circle = "🔴 "
    elif rand < 0.006: circle = "🟢 "
    
    # --- ИЗМЕНЕНИЕ: Используем новую вспомогательную функцию ---
    prefix = _get_random_header_prefix(lang='ru')
    header_text = f"{circle}{prefix}Пост №{post_num}"
    return header_text, post_num

async def delete_user_posts(bot_instance: Bot, user_id: int, time_period_minutes: int, board_id: str) -> int:
    """Удаляет сообщения пользователя за период в пределах КОНКРЕТНОЙ доски, включая очистку из тредов."""
    try:
        time_threshold = datetime.now(UTC) - timedelta(minutes=time_period_minutes)
        
        # --- ЭТАП 1: Сбор информации для удаления под блокировкой ---
        posts_to_delete_info = [] # (post_num, thread_id)
        messages_to_delete_from_api = [] # (chat_id, message_id)

        async with storage_lock:
            storage_copy = list(messages_storage.items())

            # Находим все посты пользователя за указанный период
            for post_num, post_data in storage_copy:
                post_time = post_data.get('timestamp')
                if not post_time: continue

                if (post_data.get('author_id') == user_id and
                    post_data.get('board_id') == board_id and
                    post_time >= time_threshold):
                    
                    posts_to_delete_info.append((post_num, post_data.get('thread_id')))
                    
                    # Собираем все физические сообщения, связанные с этим постом
                    if post_num in post_to_messages:
                        for uid, mid in post_to_messages[post_num].items():
                            messages_to_delete_from_api.append((uid, mid))

            if not posts_to_delete_info:
                return 0
            
            # --- ЭТАП 2: Очистка внутренних хранилищ под блокировкой ---
            for post_num, thread_id in posts_to_delete_info:
                # Очищаем обратные ссылки
                if post_num in post_to_messages:
                    for uid, mid in list(post_to_messages[post_num].items()):
                        message_to_post.pop((uid, mid), None)
                
                # Удаляем из основных хранилищ
                post_to_messages.pop(post_num, None)
                messages_storage.pop(post_num, None)

            # Очищаем посты из данных тредов
            if board_id in THREAD_BOARDS:
                threads_data = board_data[board_id].get('threads_data', {})
                for post_num, thread_id in posts_to_delete_info:
                    if thread_id and thread_id in threads_data:
                        try:
                            if 'posts' in threads_data[thread_id]:
                                threads_data[thread_id]['posts'].remove(post_num)
                        except (ValueError, KeyError):
                            pass
        
        # --- ЭТАП 3: Физическое удаление сообщений (вне блокировки) ---
        deleted_count = 0
        for chat_id, message_id in messages_to_delete_from_api:
            try:
                await bot_instance.delete_message(chat_id, message_id)
                deleted_count += 1
            except (TelegramBadRequest, TelegramForbiddenError):
                # Игнорируем ошибки, если сообщение уже удалено или бот заблокирован
                continue
            except Exception as e:
                print(f"Ошибка при удалении сообщения {message_id} в чате {chat_id}: {e}")
        
        return deleted_count

    except Exception as e:
        print(f"Ошибка в delete_user_posts: {e}")
        return 0
        
async def delete_single_post(post_num: int, bot_instance: Bot) -> int:
    """Удаляет один конкретный пост, включая очистку из треда."""
    messages_to_delete_info = []

    # --- НАЧАЛО ИЗМЕНЕНИЙ: Блок сбора данных под защитой ---
    async with storage_lock:
        if post_num not in messages_storage:
            return 0

        post_data = messages_storage.get(post_num, {})
        board_id = post_data.get('board_id')

        # 1. Очищаем пост из данных треда
        if board_id and board_id in THREAD_BOARDS:
            thread_id = post_data.get('thread_id')
            if thread_id:
                threads_data = board_data[board_id].get('threads_data', {})
                if thread_id in threads_data:
                    try:
                        if 'posts' in threads_data[thread_id]:
                             threads_data[thread_id]['posts'].remove(post_num)
                    except (ValueError, KeyError):
                        pass

        # 2. Собираем все сообщения для удаления, пока держим блокировку
        if post_num in post_to_messages:
            for uid, mid in post_to_messages[post_num].items():
                messages_to_delete_info.append((uid, mid))

        # 3. Очищаем данные из глобальных хранилищ
        for uid, mid in messages_to_delete_info:
            message_to_post.pop((uid, mid), None)
        
        post_to_messages.pop(post_num, None)
        messages_storage.pop(post_num, None)
    # --- КОНЕЦ ИЗМЕНЕНИЙ: Блокировка освобождена ---

    # 4. Выполняем медленные сетевые операции уже после освобождения блокировки
    deleted_count = 0
    for (uid, mid) in messages_to_delete_info:
        try:
            await bot_instance.delete_message(uid, mid)
            deleted_count += 1
        except (TelegramBadRequest, TelegramForbiddenError):
            continue
        except Exception as e:
            print(f"Ошибка удаления {mid} у {uid}: {e}")

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

async def process_new_post(
    bot_instance: Bot,
    board_id: str,
    user_id: int,
    content: dict,
    reply_to_post: int | None,
    is_shadow_muted: bool
):
    """
    Унифицированная функция для обработки, сохранения и постановки в очередь нового поста.
    (ФИНАЛЬНАЯ ВЕРСИЯ)
    """
    b_data = board_data[board_id]
    current_post_num = None
    
    try:
        user_location = b_data.get('user_state', {}).get(user_id, {}).get('location', 'main')
        thread_id = None
        recipients = set()
        
        if board_id in THREAD_BOARDS and user_location != 'main':
            thread_id = user_location
            thread_info = b_data.get('threads_data', {}).get(thread_id)

            if not thread_info or thread_info.get('is_archived'):
                b_data['user_state'].setdefault(user_id, {})['location'] = 'main'
                lang = 'en' if board_id == 'int' else 'ru'
                await bot_instance.send_message(user_id, random.choice(thread_messages[lang]['thread_not_found']))
                return

            # Проверка локального мута в треде
            if user_id in thread_info.get('local_mutes', {}):
                if time.time() < thread_info['local_mutes'][user_id]: return
                else: del thread_info['local_mutes'][user_id]
            
            # --- НАЧАЛО ИЗМЕНЕНИЙ: Добавлена проверка локального ТЕНЕВОГО мута ---
            if user_id in thread_info.get('local_shadow_mutes', {}):
                expires_ts = thread_info['local_shadow_mutes'][user_id]
                if time.time() < expires_ts:
                    # Если локальный теневой мут активен, форсируем флаг is_shadow_muted
                    is_shadow_muted = True
                else:
                    # Если мут истек, удаляем его
                    del thread_info['local_shadow_mutes'][user_id]
            # --- КОНЕЦ ИЗМЕНЕНИЙ ---

            async with b_data['thread_locks'][thread_id]:
                local_post_num = len(thread_info.get('posts', [])) + 1
                header_text = await format_thread_post_header(board_id, local_post_num, user_id, thread_info)
                _, current_post_num = await format_header(board_id)
                thread_info['posts'].append(current_post_num)
                thread_info['last_activity_at'] = time.time()
            
            recipients = thread_info.get('subscribers', set()) - {user_id}

            # --- НАЧАЛО ИЗМЕНЕНИЙ: Проверка майлстоунов ---
            posts_count = len(thread_info.get('posts', []))
            milestones = [50, 150, 220]
            for milestone in milestones:
                if posts_count == milestone and milestone not in thread_info.get('announced_milestones', []):
                    # Отмечаем, что этот майлстоун достигнут
                    thread_info.setdefault('announced_milestones', []).append(milestone)
                    # Запускаем отправку уведомления
                    asyncio.create_task(post_thread_notification_to_channel(
                        bots=GLOBAL_BOTS,
                        board_id=board_id,
                        thread_id=thread_id,
                        thread_info=thread_info,
                        event_type='milestone',
                        details={'posts': milestone}
                    ))
            # --- КОНЕЦ ИЗМЕНЕНИЙ ---

        else:
            header_text, current_post_num = await format_header(board_id)
            recipients = b_data['users']['active'] - {user_id}

        numeral_level = check_post_numerals(current_post_num)
        if numeral_level:
            asyncio.create_task(post_special_num_to_channel(
                bots=GLOBAL_BOTS, board_id=board_id, post_num=current_post_num,
                level=numeral_level, content=content, author_id=user_id
            ))

        content['header'] = header_text
        content['reply_to_post'] = reply_to_post
        content['post_num'] = current_post_num # <-- Явное добавление post_num в content

        reply_info_for_author = {}
        async with storage_lock:
            messages_storage[current_post_num] = {
                'author_id': user_id, 'timestamp': datetime.now(UTC), 'content': content,
                'author_message_id': None, 'board_id': board_id, 'thread_id': thread_id
            }
            if reply_to_post:
                reply_info_for_author = post_to_messages.get(reply_to_post, {})

        content_for_author = await _apply_mode_transformations(content, board_id)
        author_results = await send_message_to_users(
            bot_instance=bot_instance, recipients={user_id},
            content=content_for_author, reply_info=reply_info_for_author
        )
        
        if author_results and author_results[0] and author_results[0][1]:
            sent_to_author = author_results[0][1]
            messages_to_save = sent_to_author if isinstance(sent_to_author, list) else [sent_to_author]
            async with storage_lock:
                for m in messages_to_save:
                    if current_post_num in messages_storage:
                        messages_storage[current_post_num]['author_message_id'] = m.message_id
                    post_to_messages.setdefault(current_post_num, {})[user_id] = m.message_id
                    message_to_post[(user_id, m.message_id)] = current_post_num

        if not is_shadow_muted and recipients:
            await message_queues[board_id].put({
                'recipients': recipients, 'content': content, 'post_num': current_post_num,
                'board_id': board_id, 'thread_id': thread_id
            })

    except TelegramForbiddenError:
        b_data['users']['active'].discard(user_id)
        print(f"🚫 [{board_id}] Пользователь {user_id} заблокировал бота (из process_new_post).")
        if current_post_num:
            async with storage_lock:
                messages_storage.pop(current_post_num, None)
    except Exception as e:
        import traceback
        print(f"❌ Критическая ошибка в process_new_post для user {user_id}: {e}\n{traceback.format_exc()}")
        if current_post_num:
            async with storage_lock:
                messages_storage.pop(current_post_num, None)
                
async def _apply_mode_transformations(content: dict, board_id: str) -> dict:
    """
    Централизованно применяет все трансформации режимов с улучшенной обработкой аниме-изображений.
    Картинка в режиме аниме прикрепляется без проверки HEAD-запросом!
    """
    b_data = board_data[board_id]
    modified_content = content.copy()

    is_transform_mode_active = (
        b_data['anime_mode'] or b_data['slavaukraine_mode'] or
        b_data['zaputin_mode'] or b_data['suka_blyat_mode']
    )

    if not is_transform_mode_active:
        return modified_content  # Если режимов нет, ничего не делаем

    # --- НАЧАЛО ИЗМЕНЕНИЙ: Принудительная очистка HTML ---
    # Если режим активен, принудительно очищаем HTML перед трансформацией,
    # чтобы предотвратить инъекции через функции-трансформеры.
    if 'text' in modified_content and modified_content['text']:
        modified_content['text'] = clean_html_tags(modified_content['text'])
    if 'caption' in modified_content and modified_content['caption']:
        modified_content['caption'] = clean_html_tags(modified_content['caption'])
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---

    # Теперь применяем трансформации к чистому тексту
    if b_data['anime_mode']:
        if 'text' in modified_content and modified_content['text']:
            modified_content['text'] = anime_transform(modified_content['text'])
        if 'caption' in modified_content and modified_content['caption']:
            modified_content['caption'] = anime_transform(modified_content['caption'])
        
        # Картинка аниме без HEAD-запроса!
        if modified_content.get('type') == 'text' and random.random() < 0.41:
            anime_img_url = await get_random_anime_image()
            print(f"[ANIME DEBUG] Got anime_img_url: {anime_img_url}")

            if anime_img_url:
                text_content = modified_content.pop('text', '')
                modified_content.update({
                    'type': 'photo',
                    'caption': text_content,
                    'image_url': anime_img_url
                })
                print(f"[ANIME DEBUG] Картинка прикреплена: {anime_img_url}")
            else:
                print("[ANIME DEBUG] Не удалось получить картинку, fallback emoji")
                modified_content['text'] = f"🌸 {modified_content.get('text', '')}"

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

async def _format_message_body(
    content: dict, 
    user_id_for_context: int, 
    post_data: dict, # <-- ИЗМЕНЕНИЕ: Принимаем готовые данные поста
    reply_to_post_author_id: int | None # <-- ИЗМЕНЕНИЕ: Принимаем ID автора ответа
) -> str:
    """
    Формирует и форматирует тело сообщения (реакции, reply, greentext, (You)).
    Эта функция больше НЕ обращается к глобальным хранилищам и НЕ использует блокировки.
    """
    parts = []
    
    # Блок ответа
    reply_to_post = content.get('reply_to_post')
    if reply_to_post:
        # Используем переданный ID автора, а не лезем в messages_storage
        you_marker = " (You)" if user_id_for_context == reply_to_post_author_id else ""
        reply_line = f">>{reply_to_post}{you_marker}"
        formatted_reply_line = f"<code>{escape_html(reply_line)}</code>"
        parts.append(formatted_reply_line)
        
    # Блок реакций
    # Используем переданные данные поста
    reactions_data = post_data.get('reactions')
    
    if reactions_data:
        reaction_lines = []
        if 'users' in reactions_data and isinstance(reactions_data.get('users'), dict):
            all_emojis = [emoji for user_emojis in reactions_data['users'].values() for emoji in user_emojis]
            
            positive_display = sorted([e for e in all_emojis if e in POSITIVE_REACTIONS])
            neutral_display = sorted([e for e in all_emojis if e not in POSITIVE_REACTIONS and e not in NEGATIVE_REACTIONS])
            negative_display = sorted([e for e in all_emojis if e in NEGATIVE_REACTIONS])
            
            if positive_display: reaction_lines.append("".join(positive_display))
            if neutral_display: reaction_lines.append("".join(neutral_display))
            if negative_display: reaction_lines.append("".join(negative_display))

        elif 'positive' in reactions_data or 'negative' in reactions_data:
            if reactions_data.get('positive'): reaction_lines.append("".join(reactions_data['positive']))
            if reactions_data.get('neutral'): reaction_lines.append("".join(reactions_data['neutral']))
            if reactions_data.get('negative'): reaction_lines.append("".join(reactions_data['negative']))
        
        if reaction_lines:
            reactions_block = "\n".join(reaction_lines)
            parts.append(reactions_block)

    # Основной текст
    main_text_raw = content.get('text') or content.get('caption') or ''
    if main_text_raw:
        # Важно: add_you_to_my_posts теперь должна вызываться ИЗНУТРИ блокировки в вызывающей функции
        # Здесь мы просто форматируем greentext
        formatted_main_text = apply_greentext_formatting(main_text_raw)
        parts.append(formatted_main_text)
        
    return '\n\n'.join(filter(None, parts))

async def send_message_to_users(
    bot_instance: Bot,
    recipients: set[int],
    content: dict,
    reply_info: dict | None = None,
    keyboard: InlineKeyboardMarkup | None = None, # <-- ИЗМЕНЕНИЕ: Добавлен новый аргумент
) -> list:
    """Оптимизированная рассылка сообщений пользователям с исправлением для video_note"""
    if not recipients or not content or 'type' not in content:
        return []

    board_id = next((b_id for b_id, config in BOARD_CONFIG.items() if config['token'] == bot_instance.token), None)
    if not board_id:
        print(f"КРИТИЧЕСКАЯ ОШИБКА: Не удалось найти доску для бота с токеном ...{bot_instance.token[-6:]}")
        return []

    b_data = board_data[board_id]
    modified_content = content.copy()
    modified_content = await _apply_mode_transformations(modified_content, board_id)
    
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
            
            async with storage_lock:
                post_num = modified_content.get('post_num')
                post_data = messages_storage.get(post_num, {})
                
                reply_to_post = modified_content.get('reply_to_post')
                reply_author_id = messages_storage.get(reply_to_post, {}).get('author_id') if reply_to_post else None

                if uid == reply_author_id:
                    if "Пост" in head: head = head.replace("Пост", "🔴 Пост")
                    if "Post" in head: head = head.replace("Post", "🔴 Post")

                content_for_user = modified_content.copy()
                text_or_caption = content_for_user.get('text') or content_for_user.get('caption')
                if text_or_caption:
                    text_with_you = add_you_to_my_posts(text_or_caption, uid)
                    if 'text' in content_for_user:
                        content_for_user['text'] = text_with_you
                    elif 'caption' in content_for_user:
                        content_for_user['caption'] = text_with_you

                formatted_body = await _format_message_body(
                    content=content_for_user,
                    user_id_for_context=uid,
                    post_data=post_data,
                    reply_to_post_author_id=reply_author_id
                )

            full_text = f"{head}\n\n{formatted_body}" if formatted_body else head

            if ct == "media_group":
                if not modified_content.get('media'): return None
                builder = MediaGroupBuilder()
                for idx, media in enumerate(modified_content['media']):
                    caption = full_text if idx == 0 else None
                    builder.add(type=media['type'], media=media['file_id'], caption=caption, parse_mode="HTML" if caption else None)
                # --- ИЗМЕНЕНИЕ: Клавиатура не поддерживается для медиагрупп, поэтому здесь ее не добавляем ---
                return await bot_instance.send_media_group(chat_id=uid, media=builder.build(), reply_to_message_id=reply_to)
            
            method_name = f"send_{ct}"
            if ct == 'text': method_name = 'send_message'
            send_method = getattr(bot_instance, method_name)
            
            # --- ИЗМЕНЕНИЕ: Добавляем reply_markup в основной словарь kwargs ---
            kwargs = {'chat_id': uid, 'reply_to_message_id': reply_to, 'reply_markup': keyboard}
            
            if ct == 'text':
                kwargs.update(text=full_text, parse_mode="HTML")
                return await send_method(**kwargs)
            
            elif ct in ['photo', 'video', 'animation', 'document', 'audio', 'voice']:
                if len(full_text) > 1024: full_text = full_text[:1021] + "..."
                kwargs.update(caption=full_text, parse_mode="HTML")
                
                file_source = modified_content.get('image_url') or modified_content.get("file_id")
                kwargs[ct] = file_source
                
                if ct == 'photo' and 'image_url' in modified_content:
                    try:
                        return await send_method(**kwargs)
                    except TelegramBadRequest as e:
                        if "failed to get HTTP URL content" in e.message or "wrong type" in e.message:
                            error_text = "⚠️ [Изображение недоступно]"
                            fallback_content = f"{head}\n\n{error_text}\n\n{formatted_body}"
                            return await bot_instance.send_message(
                                chat_id=uid, 
                                text=fallback_content, 
                                parse_mode="HTML",
                                reply_to_message_id=reply_to,
                                reply_markup=keyboard
                            )
                        else:
                            raise
                return await send_method(**kwargs)
            
            elif ct == 'video_note':
                kwargs[ct] = modified_content.get("file_id")
                return await send_method(**kwargs)
            
            elif ct == 'sticker':
                kwargs[ct] = modified_content["file_id"]
                return await send_method(**kwargs)
            
            else:
                print(f"❌ Неизвестный тип контента для отправки: {ct}")
                return None

        except TelegramRetryAfter as e:
            await asyncio.sleep(e.retry_after + 1)
            return await really_send(uid, reply_to)
        except TelegramForbiddenError:
            blocked_users.add(uid)
            return None
        except TelegramBadRequest as e:
            current_type = modified_content.get("type")
            placeholder_text = None
            
            if "VOICE_MESSAGES_FORBIDDEN" in e.message and current_type == "voice":
                placeholder_text = " VOICE MESSAGE "
            elif "VIDEO_MESSAGES_FORBIDDEN" in e.message and current_type == "video_note":
                placeholder_text = " VIDEO MESSAGE (кружок) "
            
            if placeholder_text:
                lang = 'en' if board_id == 'int' else 'ru'
                if lang == 'en':
                    error_info = (
                        "<b>[ 🚫 Blocked Content ]</b>\n\n"
                        f"You have blocked receiving {placeholder_text} in your Telegram privacy settings."
                    )
                else:
                    error_info = (
                        "<b>[ Тут должно было быть медиа, но... ]</b>\n\n"
                        f"У вас в настройках приватности телеграм запрещено получение {placeholder_text}"
                    )
                
                final_text = f"{head}\n\n{error_info}"
                return await bot_instance.send_message(
                    chat_id=uid, 
                    text=final_text, 
                    parse_mode="HTML", 
                    reply_to_message_id=reply_to,
                    reply_markup=keyboard
                )
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
            async with storage_lock:
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

    async with storage_lock:
        if content.get('post_num'):
            post_num = content['post_num']
            for uid, msg in results:
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

    return results

async def edit_post_for_all_recipients(post_num: int, bot_instance: Bot):
    """
    Находит все отправленные копии поста и редактирует их, добавляя обновленный
    список реакций. (ОПТИМИЗИРОВАННАЯ ВЕРСИЯ)
    """
    # --- НАЧАЛО ИЗМЕНЕНИЙ: Вся тяжелая обработка вынесена за пределы lock ---
    message_copies_copy = {}
    content_copy = {}
    board_id = None
    content_type = None
    reply_to_author_id_global = None
    post_data_copy = {}
    
    # ЭТАП 1: Захватываем блокировку ОДИН РАЗ для максимально быстрого сбора сырых данных.
    async with storage_lock:
        post_data = messages_storage.get(post_num)
        message_copies = post_to_messages.get(post_num)

        if not post_data or not message_copies:
            return

        # Копируем только то, что необходимо
        content_copy = post_data.get('content', {}).copy()
        post_data_copy = post_data.copy()
        message_copies_copy = message_copies.copy()
        board_id = post_data.get('board_id')
        content_type = content_copy.get('type')

        can_be_edited = content_type in ['text', 'photo', 'video', 'animation', 'document', 'audio']
        if not can_be_edited or not board_id:
            return

        reply_to_post = content_copy.get('reply_to_post')
        reply_to_author_id_global = messages_storage.get(reply_to_post, {}).get('author_id') if reply_to_post else None

    # ЭТАП 2: Выполняем ресурсоемкую подготовку данных ПОСЛЕ освобождения блокировки.
    user_specific_bodies = {}
    text_or_caption_raw = content_copy.get('text') or content_copy.get('caption') or ""
    
    # Этот цикл теперь выполняется без блокировки
    for user_id in message_copies_copy.keys():
        content_for_user = content_copy.copy()
        if text_or_caption_raw:
            # add_you_to_my_posts требует блокировки, но мы можем симулировать ее логику
            # без блокировки, так как данные для этого уже скопированы.
            you_marker_text = text_or_caption_raw
            if post_data_copy.get("author_id") == user_id:
                 # Простая и быстрая симуляция без обращения к глобальному storage
                 pattern = r">>(\d+)"
                 matches = re.findall(pattern, you_marker_text)
                 for post_str in matches:
                     target = f">>{post_str}"
                     replacement = f">>{post_str} (You)"
                     if target in you_marker_text and replacement not in you_marker_text:
                         you_marker_text = you_marker_text.replace(target, replacement)

            if 'text' in content_for_user:
                content_for_user['text'] = you_marker_text
            elif 'caption' in content_for_user:
                content_for_user['caption'] = you_marker_text
        
        # _format_message_body теперь тоже вызывается вне блокировки
        user_specific_bodies[user_id] = await _format_message_body(
            content=content_for_user,
            user_id_for_context=user_id,
            post_data=post_data_copy,
            reply_to_post_author_id=reply_to_author_id_global
        )

    # ЭТАП 3: Выполняем медленные сетевые операции (без изменений).
    async def _edit_one(user_id: int, message_id: int, formatted_body: str):
        try:
            header_text = content_copy.get('header', '')
            head = f"<i>{escape_html(header_text)}</i>"

            if user_id == reply_to_author_id_global:
                if board_id == 'int': head = head.replace("Post", "🔴 Post")
                else: head = head.replace("Пост", "🔴 Post")

            full_text = f"{head}\n\n{formatted_body}" if formatted_body else head
            if len(full_text) > 4096: full_text = full_text[:4093] + "..."

            if content_type == 'text':
                await bot_instance.edit_message_text(text=full_text, chat_id=user_id, message_id=message_id, parse_mode="HTML")
            else:
                if len(full_text) > 1024: full_text = full_text[:1021] + "..."
                await bot_instance.edit_message_caption(caption=full_text, chat_id=user_id, message_id=message_id, parse_mode="HTML")
        except TelegramBadRequest as e:
            if "message is not modified" not in e.message and "message to edit not found" not in e.message:
                 print(f"⚠️ Ошибка (BadRequest) при редактировании поста #{post_num} для {user_id}: {e}")
        except TelegramForbiddenError:
            board_data[board_id]['users']['active'].discard(user_id)
            print(f"🚫 [{board_id}] Пользователь {user_id} заблокировал бота, удален из активных (при редактировании).")
        except Exception as e:
            print(f"❌ Неизвестная ошибка при редактировании поста #{post_num} для {user_id}: {e}")

    tasks = [
        _edit_one(uid, mid, user_specific_bodies.get(uid, ''))
        for uid, mid in message_copies_copy.items()
    ]
    await asyncio.gather(*tasks)
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---
    
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
    """Индивидуальный обработчик сообщений для одной доски. (ИСПРАВЛЕННАЯ ВЕРСИЯ)"""
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

            initial_recipients = msg_data['recipients']
            content = msg_data['content']
            post_num = msg_data['post_num']
            # --- НАЧАЛО ИЗМЕНЕНИЙ ---
            keyboard = msg_data.get('keyboard') # Получаем клавиатуру, если она есть
            # --- КОНЕЦ ИЗМЕНЕНИЙ ---
            
            thread_id = msg_data.get('thread_id')
            
            recipients_at_location = set()
            user_states = b_data.get('user_state', {})

            for uid in initial_recipients:
                user_location = user_states.get(uid, {}).get('location', 'main')
                
                if thread_id:
                    if user_location == thread_id:
                        recipients_at_location.add(uid)
                else:
                    if user_location == 'main':
                        recipients_at_location.add(uid)
            
            active_recipients = {uid for uid in recipients_at_location if uid not in b_data['users']['banned']}

            if not active_recipients:
                continue
            
            content = await _apply_mode_transformations(content, board_id)
            content['post_num'] = post_num

            reply_info_copy = {}
            async with storage_lock:
                if post_num in post_to_messages:
                    reply_info_copy = post_to_messages[post_num].copy()

            # --- ИЗМЕНЕНИЕ: Передаем клавиатуру в send_message_to_users ---
            await send_message_to_users(
                bot_instance,
                active_recipients,
                content,
                reply_info_copy,
                keyboard=keyboard 
            )
        except Exception as e:
            print(f"{worker_name} | ⛔ Критическая ошибка: {str(e)[:200]}")
            import traceback
            traceback.print_exc()
            await asyncio.sleep(1)

async def _send_single_missed_post(bot: Bot, user_id: int, post_num: int):
    """
    Вспомогательная функция для получения данных и отправки одного пропущенного поста.
    Инкапсулирует логику для избежания дублирования.
    """
    try:
        content_copy, reply_info_copy = None, {}
        async with storage_lock:
            post_data = messages_storage.get(post_num)
            if post_data:
                content_copy = post_data.get('content', {}).copy()
                reply_info_copy = post_to_messages.get(post_num, {})

        if content_copy:
            await send_message_to_users(bot, {user_id}, content_copy, reply_info_copy)
            await asyncio.sleep(0.1)  # Небольшая задержка между сообщениями

    except Exception as e:
        print(f"Ошибка отправки пропущенного сообщения #{post_num} юзеру {user_id}: {e}")

async def send_missed_messages(bot: Bot, board_id: str, user_id: int, target_location: str) -> bool:
    """
    Отправляет пользователю пропущенные сообщения. Если их > 30, старые группируются
    в текстовые чанки, а 30 последних отправляются отдельно.
    Возвращает True, если хотя бы одно сообщение было отправлено.
    """
    b_data = board_data[board_id]
    user_s = b_data['user_state'].setdefault(user_id, {})
    
    missed_post_nums = []
    last_seen_post = 0
    
    async with storage_lock:
        if target_location == 'main':
            last_seen_post = user_s.get('last_seen_main', 0)
            all_main_posts = [p_num for p_num, p_data in messages_storage.items() if p_data.get('board_id') == board_id and not p_data.get('thread_id')]
            all_main_posts.sort()
            missed_post_nums = [p_num for p_num in all_main_posts if p_num > last_seen_post]
        else:
            thread_id = target_location
            last_seen_threads = user_s.setdefault('last_seen_threads', {})
            last_seen_post = last_seen_threads.get(thread_id, 0)
            
            thread_info = b_data.get('threads_data', {}).get(thread_id)
            if thread_info:
                all_thread_posts = sorted(thread_info.get('posts', []))
                missed_post_nums = [p_num for p_num in all_thread_posts if p_num > last_seen_post]

    if not missed_post_nums:
        return False

    MAX_MISSED_TO_SEND = 70
    if len(missed_post_nums) > MAX_MISSED_TO_SEND:
        missed_post_nums = missed_post_nums[-MAX_MISSED_TO_SEND:]

    # --- НАЧАЛО ИЗМЕНЕНИЙ: Новая условная логика ---
    THRESHOLD = 30
    
    # СЦЕНАРИЙ 1: Пропущено много сообщений, применяем гибридную отправку
    if len(missed_post_nums) > THRESHOLD:
        posts_to_chunk = missed_post_nums[:-THRESHOLD]
        latest_posts = missed_post_nums[-THRESHOLD:]

        # --- Этап 1.1: Отправка старых сообщений чанками ---
        text_chunk = []
        async def send_chunk():
            if text_chunk:
                full_text = "\n\n".join(text_chunk)
                if len(full_text) > 4096: full_text = full_text[:4093] + "..."
                try:
                    await bot.send_message(user_id, full_text, parse_mode="HTML")
                except (TelegramForbiddenError, TelegramBadRequest) as e:
                    print(f"Ошибка отправки чанка юзеру {user_id}: {e}")
                finally:
                    text_chunk.clear()
                    await asyncio.sleep(0.1)

        for post_num in posts_to_chunk:
            post_data = messages_storage.get(post_num)
            if not post_data: continue
            
            content = post_data.get('content', {})
            if content.get('type') == 'text' and content.get('text'):
                # --- Логика формирования текста для чанка (как в предыдущей версии) ---
                header_text = f"<i>{escape_html(content['header'])}</i>"
                reply_to_post = content.get('reply_to_post')
                reply_author_id = messages_storage.get(reply_to_post, {}).get('author_id') if reply_to_post else None
                text_with_you = add_you_to_my_posts(content['text'], user_id)
                content['text'] = text_with_you
                formatted_body = await _format_message_body(content=content, user_id_for_context=user_id, post_data=post_data, reply_to_post_author_id=reply_author_id)
                full_post_text = f"{header_text}\n\n{formatted_body}" if formatted_body else header_text
                
                if len("\n\n".join(text_chunk)) + len(full_post_text) > 4096:
                    await send_chunk()
                text_chunk.append(full_post_text)
            else:
                await send_chunk() # Отправляем накопленный текст
                await _send_single_missed_post(bot, user_id, post_num) # Отправляем медиа
        
        await send_chunk() # Отправляем последний чанк старых сообщений

        # --- Этап 1.2: Отправка последних сообщений по отдельности ---
        for post_num in latest_posts:
            await _send_single_missed_post(bot, user_id, post_num)

    # СЦЕНАРИЙ 2: Пропущено мало сообщений, отправляем всё по отдельности
    else:
        for post_num in missed_post_nums:
            await _send_single_missed_post(bot, user_id, post_num)
    
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---

    # Финальное сообщение с кнопками (без изменений)
    lang = 'en' if board_id == 'int' else 'ru'
    final_text = "All new messages loaded." if lang == 'en' else "Все новые сообщения загружены."
    entry_keyboard = _get_thread_entry_keyboard(board_id)
    
    try:
        await bot.send_message(user_id, final_text, reply_markup=entry_keyboard, parse_mode="HTML")
    except (TelegramForbiddenError, TelegramBadRequest):
        pass

    # Обновляем состояние пользователя
    new_last_seen = missed_post_nums[-1]
    if target_location == 'main':
        user_s['last_seen_main'] = new_last_seen
    else:
        user_s.setdefault('last_seen_threads', {})[target_location] = new_last_seen
    
    return True
            
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

                # --- НАЧАЛО ИЗМЕНЕНИЙ ---
                # Добавлен await для корректного вызова асинхронной функции
                activity = await get_board_activity_last_hours(board_id, hours=2)
                if activity < 60:
                # --- КОНЕЦ ИЗМЕНЕНИЙ ---
                    print(f"ℹ️ [{board_id}] Пропуск мотивационного сообщения, активность слишком низкая: {activity:.1f} п/ч (требуется > 60).")
                    continue

                b_data = board_data[board_id]
                recipients = b_data['users']['active'] - b_data['users']['banned']

                if not recipients:
                    continue
                
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

async def save_threads_data(board_id: str):
    """Асинхронная обертка для сохранения данных о тредах."""
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(
        save_executor,
        _sync_save_threads_data,
        board_id
    )

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

def check_post_numerals(post_num: int) -> int | None:
    """
    Проверяет номер поста на наличие повторяющихся цифр в конце.
    Использует оптимизированный посимвольный анализ с конца.
    Возвращает "уровень редкости" (количество повторов) или None.
    """
    s = str(post_num)
    length = len(s)
    if length < 3:
        return None

    last_char = s[-1]
    count = 1
    
    # Идем в обратном порядке от предпоследнего символа
    for i in range(length - 2, -1, -1):
        if s[i] == last_char:
            count += 1
        else:
            # Прерываемся, как только найден другой символ
            break
            
    # Проверяем, есть ли найденное количество в нашем конфиге
    if count in SPECIAL_NUMERALS_CONFIG:
        return count

    return None

def get_board_id(telegram_object: types.Message | types.CallbackQuery) -> str | None:
    """
    Определяет ID доски ('b', 'po', etc.) по объекту сообщения или колбэка.
    Это ключевая функция для работы с несколькими ботами.
    """
    # --- НАЧАЛО ИЗМЕНЕНИЙ: Добавлена проверка на наличие атрибута .bot ---
    try:
        # Пытаемся получить токен, как и раньше
        bot_token = telegram_object.bot.token
    except AttributeError:
        # Если у объекта события нет атрибута .bot, он не может быть
        # ассоциирован с конкретным ботом. Безопасно возвращаем None.
        return None
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---

    for board_id, config in BOARD_CONFIG.items():
        if config['token'] == bot_token:
            return board_id
    
    # Эта ситуация не должна происходить при правильной настройке
    print(f"⚠️ CRITICAL: Не удалось определить board_id для бота с токеном, заканчивающимся на ...{bot_token[-6:]}")
    return None

async def _send_thread_info_if_applicable(message: types.Message, board_id: str):
    """
    Отправляет информационное сообщение о тредах, если они активны на доске.
    """
    if board_id not in THREAD_BOARDS:
        return

    lang = 'en' if board_id == 'int' else 'ru'

    if lang == 'en':
        info_text = (
            "<b>This board supports threads!</b>\n\n"
            "You can create your own temporary discussion rooms. "
            "Use /create to start a new thread or /threads to view active ones."
        )
        button_create_text = "🚀 Create a New Thread"
        button_view_text = "📋 View Active Threads"
    else:
        info_text = (
            "<b>На этой доске поддерживаются треды!</b>\n\n"
            "Вы можете создавать собственные временные комнаты для обсуждений. "
            "Используйте /create, чтобы начать новый тред, или /threads, чтобы посмотреть активные."
        )
        button_create_text = "🚀 Создать новый тред"
        button_view_text = "📋 Посмотреть треды"

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=button_create_text, callback_data="create_thread_start")],
        [InlineKeyboardButton(text=button_view_text, callback_data="show_active_threads")]
    ])

    try:
        await message.answer(info_text, reply_markup=keyboard, parse_mode="HTML")
    except (TelegramForbiddenError, TelegramBadRequest):
        pass # Игнорируем, если не удалось доставить

# ========== КОМАНДЫ ==========



@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext, board_id: str | None): # Добавлен state в аргументы
    user_id = message.from_user.id
    if not board_id: return
    
    b_data = board_data[board_id]
    
    command_payload = message.text.split()[1] if len(message.text.split()) > 1 else None

    if command_payload and command_payload.startswith("thread_"):
        thread_id = command_payload.split('_')[-1]
        
        if board_id in THREAD_BOARDS and thread_id in b_data.get('threads_data', {}):
            b_data['users']['active'].add(user_id)
            user_s = b_data['user_state'].setdefault(user_id, {})
            
            user_s['location'] = thread_id
            user_s['last_location_switch'] = time.time()
            b_data['threads_data'][thread_id].setdefault('subscribers', set()).add(user_id)
            
            # --- ИЗМЕНЕНИЕ: Используем cb_create_thread_confirm для унификации входа ---
            # Создаем фейковый колбэк, чтобы передать в функцию
            fake_callback_query = types.CallbackQuery(
                id=str(user_id), from_user=message.from_user, chat_instance="", message=message
            )
            await cb_enter_thread(fake_callback_query) # Вызываем хендлер входа в тред
            
            try:
                await message.delete()
            except TelegramBadRequest:
                pass
            return

    if user_id not in b_data['users']['active']:
        b_data['users']['active'].add(user_id)
        print(f"✅ [{board_id}] Новый пользователь через /start: ID {user_id}")
    
    start_text = b_data.get('start_message_text', "Добро пожаловать в ТГАЧ!")
    
    await message.answer(start_text, parse_mode="HTML", disable_web_page_preview=True)
    
    # --- НАЧАЛО ИЗМЕНЕНИЙ: Замена старого кода на вызов новой функции ---
    await _send_thread_info_if_applicable(message, board_id)
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---
    
    try:
        await message.delete()
    except TelegramBadRequest:
        pass
    

AHE_EYES = ['😵', '🤤', '😫', '😩', '😳', '😖', '🥵']
AHE_TONGUE = ['👅', '💦', '😛', '🤪', '😝']
AHE_EXTRA = ['💕', '💗', '✨', '🥴', '']

@dp.message(Command(commands=['b', 'po', 'pol', 'a', 'sex', 'vg', 'int', 'test']))
async def cmd_show_board_info(message: types.Message, board_id: str | None):
    """
    Отвечает на команду с названием доски, предоставляя информацию о ней.
    """
    if not board_id:
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
    is_english = (board_id == 'int')

    if is_english:
        header_text = f"🌐 You are currently on the <b>{BOARD_CONFIG[board_id]['name']}</b> board."
        board_info_text = (
            f"You requested information about the <b>{target_config['name']}</b> board:\n"
            f"<i>{target_config['description_en']}</i>\n\n"
            f"You can switch to it here: {target_config['username']}"
        )
    else:
        header_text = f"🌐 Вы находитесь на доске <b>{BOARD_CONFIG[board_id]['name']}</b>."
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
async def cmd_face(message: types.Message, board_id: str | None):
    if not board_id: return

    face = (secrets.choice(AHE_EYES) + secrets.choice(AHE_TONGUE) +
            secrets.choice(AHE_EXTRA))

    user_id = message.from_user.id
    b_data = board_data[board_id]
    
    # Определяем, куда постить результат
    if board_id in THREAD_BOARDS:
        user_location = b_data.get('user_state', {}).get(user_id, {}).get('location', 'main')
        if user_location != 'main':
            thread_id = user_location
            thread_info = b_data.get('threads_data', {}).get(thread_id)
            if thread_info and not thread_info.get('is_archived'):
                local_post_num = len(thread_info.get('posts', [])) + 1
                # --- ИЗМЕНЕНИЕ ---
                # Передаем author_id=0 (системное сообщение) и thread_info
                header_text = await format_thread_post_header(board_id, local_post_num, 0, thread_info)
                # --- КОНЕЦ ИЗМЕНЕНИЯ ---
                _, pnum = await format_header(board_id) # Глобальный номер
                
                content = {"type": "text", "header": header_text, "text": face}
                messages_storage[pnum] = {
                    'author_id': 0, 'timestamp': datetime.now(UTC), 'content': content,
                    'board_id': board_id, 'thread_id': thread_id
                }
                thread_info['posts'].append(pnum)
                thread_info['last_activity_at'] = time.time()
                
                await message_queues[board_id].put({
                    "recipients": thread_info.get('subscribers', set()),
                    "content": content, "post_num": pnum, "board_id": board_id, "thread_id": thread_id
                })
                await message.delete()
                return

    # Стандартная логика для общего чата
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

@dp.message(Command("summarize"))
async def cmd_summarize(message: types.Message, board_id: str | None):
    if not board_id:
        print("[summarize] Board ID not found")
        await message.answer("Ошибка: не удалось определить доску.")
        return

    b_data = board_data[board_id]
    user_id = message.from_user.id
    lang = 'en' if board_id == 'int' else 'ru'

    # Проверка Cooldown
    now_ts = time.time()
    last_usage = b_data.get('last_summarize_time', 0)

    if now_ts - last_usage < SUMMARIZE_COOLDOWN:
        remaining = SUMMARIZE_COOLDOWN - (now_ts - last_usage)
        
        if lang == 'en':
            cooldown_text = f"⏳ Command is on cooldown. Please wait {int(remaining)} seconds."
        else:
            cooldown_text = f"⏳ Команда на кулдауне. Подождите еще {int(remaining)} сек."
        
        try:
            await message.answer(cooldown_text)
            await message.delete()
        except Exception:
            pass
        return

    # --- НАЧАЛО ИЗМЕНЕНИЙ: Определение контекста (тред или доска) ---
    thread_id = None
    context_name = f"доски {BOARD_CONFIG[board_id]['name']}"
    if board_id in THREAD_BOARDS:
        user_location = b_data.get('user_state', {}).get(user_id, {}).get('location', 'main')
        if user_location != 'main':
            thread_id = user_location
            thread_info = b_data.get('threads_data', {}).get(thread_id, {})
            thread_title = thread_info.get('title', '...')
            context_name = f"треда «{thread_title}»"

    # Адаптация prompt для треда
    if thread_id:
        prompt = (
            f"Ты должен коротко и забавно подвести итоги обсуждений в треде под названием «{escape_html(thread_info.get('title', ''))}» в анонимном чате двача. "
            "Пиши как настоящий анон, используй иронию и сарказм, выноси суть. Не будь унылым!"
        )
        # Для треда анализируем все его посты, а не за последние N часов
        chunk = await get_board_chunk(board_id, thread_id=thread_id)
        info_text = f"За последние 6 часов в треде" # для сообщения пользователю
    else:
        prompt = (
            "Ты должен коротко и забавно подвести итоги обсуждений за последние 6 часов в анонимном чате двача. Там общаются аноны."
            "Пиши как настоящий анон, используй иронию и сарказм, выноси суть и не слишком серьёзно. "
            "Если были картинки или медиа — просто упомяни 'Картинка', 'Гифка' и т.п. Не пиши длинно, не пиши уныло!"
        )
        chunk = await get_board_chunk(board_id, hours=6)
        info_text = f"За последние 6 часов на доске"
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---

    hf_token = os.getenv("HF_TOKEN")
    if not hf_token:
        print("[summarize] HF_TOKEN not set")
        await message.answer("Ошибка: не настроен токен Hugging Face.")
        return

    if not chunk or len(chunk) < 100:
        print(f"[summarize] Мало сообщений для summarize (len={len(chunk)})")
        await message.answer(f"{info_text} было мало сообщений для саммари.")
        return

    await message.answer("⏳ Генерируется саммари, ждите ~30 секунд...")
    try:
        summary = await summarize_text_with_hf(prompt, chunk, hf_token)
    except Exception as e:
        print(f"[summarize] Error during HF summarize: {e}")
        await message.answer("Ошибка при генерации саммари.")
        return

    if not summary:
        print("[summarize] Summary empty or failed")
        await message.answer("Не удалось сделать саммари. Попробуй позже.")
        return

    b_data['last_summarize_time'] = time.time()

    summary = summary[:4000]
    print(f"[summarize] Final summary length: {len(summary)}")

    # --- НАЧАЛО ИЗМЕНЕНИЙ: Отправка результата в тред или на доску ---
    header, pnum = await format_header(board_id) # Глобальный номер нужен всегда
    content = {
        'type': 'text',
        'text': f"Саммари {context_name}:\n\n{summary}",
        'is_system_message': True
    }

    if thread_id:
        thread_info = b_data.get('threads_data', {}).get(thread_id)
        if thread_info and not thread_info.get('is_archived'):
            local_post_num = len(thread_info.get('posts', [])) + 1
            header_text = await format_thread_post_header(board_id, local_post_num, 0, thread_info)
            content['header'] = header_text
            
            messages_storage[pnum] = {
                'author_id': 0, 'timestamp': datetime.now(UTC), 'content': content,
                'board_id': board_id, 'thread_id': thread_id
            }
            thread_info['posts'].append(pnum)
            thread_info['last_activity_at'] = time.time()
            
            await message_queues[board_id].put({
                "recipients": thread_info.get('subscribers', set()),
                "content": content, "post_num": pnum, "board_id": board_id, "thread_id": thread_id
            })
        else: # Если тред уже удален/архивирован, отменяем
             await message.answer("Не удалось отправить саммари, тред больше не активен.")
             return
    else:
        content['header'] = header
        messages_storage[pnum] = {'author_id': 0, 'timestamp': datetime.now(UTC), 'content': content, 'board_id': board_id}
        await message_queues[board_id].put({
            'recipients': b_data['users']['active'],
            'content': content,
            'post_num': pnum,
            'board_id': board_id
        })

    print(f"[summarize] Саммари успешно отправлено ({context_name}, post_num={pnum})")
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---

@dp.callback_query(F.data == "show_active_threads")
async def cq_show_active_threads(callback: types.CallbackQuery, board_id: str | None):
    """Обрабатывает нажатие кнопки 'Посмотреть треды', выводя список."""
    if not board_id or board_id not in THREAD_BOARDS:
        await callback.answer("This action is not available here.", show_alert=True)
        return

    b_data = board_data[board_id]
    lang = 'en' if board_id == 'int' else 'ru'
    
    threads_data = b_data.get('threads_data', {})
    
    active_threads = {k: v for k, v in threads_data.items() if not v.get('is_archived')}

    if not active_threads:
        empty_text = random.choice(thread_messages[lang]['threads_list_empty'])
        await callback.answer(empty_text, show_alert=True)
        return

    # Сортируем треды по последней активности
    sorted_threads = sorted(
        active_threads.items(),
        key=lambda item: item[1].get('last_activity_at', 0),
        reverse=True
    )
    
    # Сохраняем отсортированный список в состоянии пользователя для пагинации
    user_s = b_data['user_state'].setdefault(callback.from_user.id, {})
    user_s['sorted_threads_cache'] = [tid for tid, _ in sorted_threads]

    # Генерируем текст и клавиатуру для первой страницы
    text, keyboard = await generate_threads_page(b_data, callback.from_user.id, page=0)

    await callback.answer() # Снимаем "часики"
    try:
        # Отправляем новое сообщение со списком и удаляем старое (информационное)
        await callback.message.answer(text, reply_markup=keyboard, parse_mode="HTML")
        await callback.message.delete()
    except (TelegramForbiddenError, TelegramBadRequest):
        pass # Игнорируем ошибки, если не удалось отправить/удалить

@dp.message(Command("help"))
async def cmd_help(message: types.Message, board_id: str | None):
    if not board_id: return

    # Отправляем текст помощи с ссылками на все доски
    start_text = board_data[board_id].get('start_message_text', "Нет информации о помощи.")
    await message.answer(start_text, parse_mode="HTML", disable_web_page_preview=True)
    
    # --- НАЧАЛО ИЗМЕНЕНИЙ: Замена старого кода на вызов новой функции ---
    await _send_thread_info_if_applicable(message, board_id)
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---

    try:
        await message.delete()
    except TelegramBadRequest:
        pass


@dp.message(Command("roll"))
async def cmd_roll(message: types.Message, board_id: str | None):
    if not board_id: return
    
    result = random.randint(1, 100)
    lang = 'en' if board_id == 'int' else 'ru'
    roll_text = f"🎲 Rolled: {result}" if lang == 'en' else f"🎲 Нароллил: {result}"

    user_id = message.from_user.id
    b_data = board_data[board_id]

    if board_id in THREAD_BOARDS:
        user_location = b_data.get('user_state', {}).get(user_id, {}).get('location', 'main')
        if user_location != 'main':
            thread_id = user_location
            thread_info = b_data.get('threads_data', {}).get(thread_id)
            if thread_info and not thread_info.get('is_archived'):
                local_post_num = len(thread_info.get('posts', [])) + 1
                # --- ИЗМЕНЕНИЕ ---
                # Передаем author_id=0 (системное сообщение) и thread_info
                header_text = await format_thread_post_header(board_id, local_post_num, 0, thread_info)
                # --- КОНЕЦ ИЗМЕНЕНИЯ ---
                _, pnum = await format_header(board_id)
                
                content = {"type": "text", "header": header_text, "text": roll_text}
                messages_storage[pnum] = {
                    'author_id': 0, 'timestamp': datetime.now(UTC), 'content': content,
                    'board_id': board_id, 'thread_id': thread_id
                }
                thread_info['posts'].append(pnum)
                thread_info['last_activity_at'] = time.time()

                await message_queues[board_id].put({
                    "recipients": thread_info.get('subscribers', set()),
                    "content": content, "post_num": pnum, "board_id": board_id, "thread_id": thread_id
                })
                await message.delete()
                return

    # Стандартная логика
    header, pnum = await format_header(board_id)
    content = {"type": "text", "header": header, "text": roll_text}
    messages_storage[pnum] = {'author_id': 0, 'timestamp': datetime.now(UTC), 'content': content, 'board_id': board_id}

    await message_queues[board_id].put({
        "recipients": board_data[board_id]['users']['active'],
        "content": content,
        "post_num": pnum,
        "board_id": board_id
    })
    await message.delete()
    
@dp.message(Command("slavaukraine"))
async def cmd_slavaukraine(message: types.Message, board_id: str | None):
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
        "💙💛 ВАХТА НА ЗАВАЛІ! Вмикаємо режим 'КІБЕРПОЛК АЗОВ'! СМЕРТЬ РУСНІ!",
        "БАНДЕРОВЕЦЬ В ЧАТІ! 💛💙 Переходимо на український тролінг. Путін - хуйло!",
        "💣 ХЕРСОНЬ НАШ! Режим 'ДРОН-КАМИКАДЗЕ' активирован! СЛАВА ЗСУ!",
        "🔥 ДЕМОНІЧНИЙ РЕЖИМ ВВІМКНЕНО! Запалюємо русскій корабль! ІДИ НАХУЙ!",
        "🪖 ТЕРОБОРОНЕЦЬ У ЧАТІ! Переходимо на український тролінг. Путін - хуйло!",
        "⚔️ ШАХТАРСЬКИЙ НАСТУП! Режим 'СЛАВА НАЦІЇ' активовано! ГЕРОЯМ СЛАВА!",
        "🔱 ТЕРМІНОВО! У ЧАТІ З'ЯВИВСЯ ХАСК! Режим 'СЛАВА НАЦІЇ' активовано!",
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
        "💀 ДЕМОБІЛІЗАЦІЯ ЗАВЕРШЕНА. Повертаємось до звичайного ссаня в чат",
        "💀 БАНДЕРА ВТІК У КАНАДУ. Режим вимкнено, москалі перемогли...",
        "🕊️ МИРНИЙ ПРОЦЕС. Повертаємось до звичайного ссаня в чат",
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
async def cmd_stop(message: types.Message, board_id: str | None):
    """Остановка любых активных режимов на текущей доске."""
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
async def cmd_active(message: types.Message, board_id: str | None):
    """Выводит статистику активности досок за последние 2 часа + за сутки."""
    if not board_id: return

    if board_id in THREAD_BOARDS:
        user_id = message.from_user.id
        b_data = board_data[board_id]
        user_location = b_data.get('user_state', {}).get(user_id, {}).get('location', 'main')
        if user_location != 'main':
            try: await message.delete()
            except Exception: pass
            return

    now = datetime.now(UTC)
    day_ago = now - timedelta(hours=24)
    # --- НАЧАЛО ИЗМЕНЕНИЙ ---
    async with storage_lock:
        # Подсчет постов теперь полностью выполняется внутри блокировки,
        # что гарантирует целостность данных и предотвращает RuntimeError.
        posts_last_24h = sum(
            1 for post in messages_storage.values()
            if post.get("timestamp", now) > day_ago
        )
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---
    
    lang = 'en' if board_id == 'int' else 'ru'
    activity_lines = []
    for b_id in BOARDS:
        if b_id == 'test':
            continue
        activity = await get_board_activity_last_hours(b_id, hours=2)
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
    
    async with storage_lock:
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


# ========== КОМАНДЫ ДЛЯ СИСТЕМЫ ТРЕДОВ ==========

@dp.message(Command("create"))
async def cmd_create_fsm_entry(message: types.Message, state: FSMContext, board_id: str | None):
    """
    Обрабатывает команду /create и служит точкой входа в FSM-сценарий создания треда.
    """
    if not board_id or board_id not in THREAD_BOARDS:
        # Если команда не поддерживается, ничего не делаем, чтобы не мешать другим ботам
        return

    # --- НАЧАЛО ИЗМЕНЕНИЙ: Проверка текущего состояния ---
    current_state = await state.get_state()
    if current_state is not None:
        lang = 'en' if board_id == 'int' else 'ru'
        # Используем тексты для отмены, так как они сообщают о текущем процессе
        # и подразумевают возможность его отмены через /cancel.
        # (Предполагается, что ключ 'create_cancelled' уже содержит подходящие фразы)
        text = "You are already in the process of creating a thread. Use /cancel to stop."
        if lang == 'ru':
            text = "Вы уже находитесь в процессе создания треда. Используйте /cancel для отмены."
        
        try:
            await message.answer(text)
            await message.delete()
        except (TelegramForbiddenError, TelegramBadRequest):
            pass
        return
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---

    lang = 'en' if board_id == 'int' else 'ru'
    
    # Проверяем, есть ли текст после команды /create
    command_args = message.text.split(maxsplit=1)
    if len(command_args) > 1 and command_args[1].strip():
        # СЛУЧАЙ 1: /create с текстом
        op_post_text = command_args[1].strip()
        
        # Сразу сохраняем текст и переходим к подтверждению
        await state.update_data(op_post_text=op_post_text)
        await state.set_state(ThreadCreateStates.waiting_for_confirmation)

        if lang == 'en':
            confirmation_text = f"You want to create a thread with this opening post:\n\n---\n{escape_html(op_post_text)}\n---\n\nCreate?"
            button_create = "✅ Create Thread"
            button_edit = "✏️ Edit Text"
        else:
            confirmation_text = f"Вы хотите создать тред с таким ОП-постом:\n\n---\n{escape_html(op_post_text)}\n---\n\nСоздаем?"
            button_create = "✅ Создать тред"
            button_edit = "✏️ Редактировать"
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text=button_create, callback_data="create_thread_confirm"),
                InlineKeyboardButton(text=button_edit, callback_data="create_thread_edit")
            ]
        ])

        await message.answer(confirmation_text, reply_markup=keyboard, parse_mode="HTML")

    else:
        # СЛУЧАЙ 2: /create без текста
        # Переходим в состояние ожидания текста
        await state.set_state(ThreadCreateStates.waiting_for_op_post)
        prompt_text = random.choice(thread_messages[lang]['create_prompt_op_post'])
        await message.answer(prompt_text)

    try:
        await message.delete()
    except TelegramBadRequest:
        pass

@dp.callback_query(F.data == "create_thread_confirm", ThreadCreateStates.waiting_for_confirmation)
async def cb_create_thread_confirm(callback: types.CallbackQuery, state: FSMContext, board_id: str | None):
    """
    Финальный шаг: ловит подтверждение, создает тред и выходит из FSM.
    """
    if not board_id: return
    
    user_id = callback.from_user.id
    b_data = board_data[board_id]
    lang = 'en' if board_id == 'int' else 'ru'

    fsm_data = await state.get_data()
    op_post_text = fsm_data.get('op_post_text')

    if not op_post_text:
        await state.clear()
        await callback.answer("Error: Post data not found. Please start over.", show_alert=True)
        return
        
    threads_data = b_data['threads_data']
    thread_id = secrets.token_hex(4)
    now_ts = time.time()
    now_dt = datetime.now(UTC)
    
    title = escape_html(clean_html_tags(op_post_text).split('\n')[0][:60])
    
    thread_info = {
        'op_id': user_id,
        'title': title,
        'created_at': now_dt.isoformat(),
        'last_activity_at': now_ts,
        'posts': [],
        'subscribers': {user_id},
        'local_mutes': {},
        'local_shadow_mutes': {},
        'is_archived': False,
        'announced_milestones': [],
        'activity_notified': False
    }
    threads_data[thread_id] = thread_info

    user_s = b_data['user_state'].setdefault(user_id, {})
    user_s['last_thread_creation'] = now_ts

    success_text = random.choice(thread_messages[lang]['create_success']).format(title=title)
    header, pnum = await format_header(board_id)
    content = {'type': 'text', 'header': header, 'text': success_text, 'is_system_message': True}
    messages_storage[pnum] = {'author_id': 0, 'timestamp': now_dt, 'content': content, 'board_id': board_id}
    await message_queues[board_id].put({'recipients': b_data['users']['active'], 'content': content, 'post_num': pnum, 'board_id': board_id})

    op_post_content = {'type': 'text', 'text': op_post_text}
    await process_new_post(
        bot_instance=callback.bot, board_id=board_id, user_id=user_id, content=op_post_content,
        reply_to_post=None, is_shadow_muted=False
    )
    
    user_s['location'] = thread_id
    user_s['last_location_switch'] = now_ts
    
    # --- НАЧАЛО ИЗМЕНЕНИЙ: Упрощенная логика приветствия ---
    await callback.answer() # Сразу отвечаем на колбэк
    try:
        # Удаляем сообщение с кнопками "Создать/Редактировать"
        await callback.message.delete()
    except TelegramBadRequest:
        pass

    # Так как тред только что создан, пропущенных сообщений нет.
    # Сразу отправляем приветственное сообщение.
    enter_message = random.choice(thread_messages[lang]['enter_thread_prompt']).format(title=title)
    entry_keyboard = _get_thread_entry_keyboard(board_id)
    try:
        await callback.message.answer(enter_message, reply_markup=entry_keyboard, parse_mode="HTML")
    except (TelegramForbiddenError, TelegramBadRequest):
        pass
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---

    asyncio.create_task(post_thread_notification_to_channel(
        bots=GLOBAL_BOTS, board_id=board_id, thread_id=thread_id,
        thread_info=thread_info, event_type='new_thread'
    ))

    await _send_op_commands_info(callback.bot, user_id, board_id)
    
    await state.clear()

@dp.callback_query(F.data == "create_thread_edit", ThreadCreateStates.waiting_for_confirmation)
async def cb_create_thread_edit(callback: types.CallbackQuery, state: FSMContext, board_id: str | None):
    """
    Обрабатывает нажатие кнопки 'Редактировать', возвращая пользователя 
    на шаг ввода ОП-поста.
    """
    if not board_id: return
    lang = 'en' if board_id == 'int' else 'ru'

    # Возвращаем пользователя на предыдущее состояние
    await state.set_state(ThreadCreateStates.waiting_for_op_post)

    prompt_text = random.choice(thread_messages[lang]['create_prompt_op_post_edit'])
    
    await callback.answer()
    try:
        # Редактируем сообщение с подтверждением, заменяя его на новую инструкцию
        await callback.message.edit_text(prompt_text)
    except TelegramBadRequest:
        pass # Игнорируем, если сообщение не может быть изменено


# --- Константы для пагинации ---
THREADS_PER_PAGE = 10

@dp.message(Command("threads"))
async def cmd_threads(message: types.Message, board_id: str | None):
    """Выводит постраничный список активных тредов."""
    if not board_id: return

    if board_id not in THREAD_BOARDS:
        try: await message.delete()
        except Exception: pass
        return

    b_data = board_data[board_id]
    lang = 'en' if board_id == 'int' else 'ru'
    
    threads_data = b_data.get('threads_data', {})
    
    if not threads_data:
        empty_text = random.choice(thread_messages[lang]['threads_list_empty'])
        await message.answer(empty_text)
        await message.delete()
        return

    # Сортируем треды по последней активности
    sorted_threads = sorted(
        threads_data.items(),
        key=lambda item: item[1].get('last_activity_at', 0),
        reverse=True
    )
    
    # Сохраняем отсортированный список в состоянии пользователя для пагинации
    user_s = b_data['user_state'].setdefault(message.from_user.id, {})
    user_s['sorted_threads_cache'] = [tid for tid, _ in sorted_threads]

    # Генерируем текст и клавиатуру для первой страницы
    text, keyboard = await generate_threads_page(b_data, message.from_user.id, page=0)

    await message.answer(text, reply_markup=keyboard, parse_mode="HTML")
    await message.delete()

async def generate_threads_page(b_data: dict, user_id: int, page: int = 0) -> tuple[str, InlineKeyboardMarkup]:
    """Генерирует текст и клавиатуру для конкретной страницы списка тредов."""
    board_id = next((bid for bid, data in board_data.items() if data is b_data), None)
    lang = 'en' if board_id == 'int' else 'ru'

    user_s = b_data['user_state'].get(user_id, {})
    sorted_thread_ids = user_s.get('sorted_threads_cache', [])
    threads_data = b_data.get('threads_data', {})

    start_index = page * THREADS_PER_PAGE
    end_index = start_index + THREADS_PER_PAGE
    page_thread_ids = sorted_thread_ids[start_index:end_index]

    header = random.choice(thread_messages[lang]['threads_list_header'])
    
    lines = []
    keyboard_buttons = []
    
    now_ts = time.time()

    for i, thread_id in enumerate(page_thread_ids):
        thread_info = threads_data.get(thread_id)
        if not thread_info: continue

        index = start_index + i + 1
        title = thread_info.get('title', 'Без названия')
        posts_count = len(thread_info.get('posts', []))
        
        last_activity_ts = thread_info.get('last_activity_at', 0)
        time_diff = now_ts - last_activity_ts
        
        if time_diff < 60: last_activity_str = f"{int(time_diff)}с"
        elif time_diff < 3600: last_activity_str = f"{int(time_diff / 60)}м"
        else: last_activity_str = f"{int(time_diff / 3600)}ч"

        lines.append(thread_messages[lang]['thread_list_item'].format(
            index=index,
            title=title,
            posts_count=posts_count,
            last_activity=last_activity_str
        ))
        keyboard_buttons.append([InlineKeyboardButton(
            text=f"{index}. {title[:40]}",
            callback_data=f"enter_thread_{thread_id}"
        )])

    # --- НАЧАЛО ИЗМЕНЕНИЙ ---
    # Добавляем кнопку "Показать историю" для текущего треда, если пользователь в треде
    current_location = user_s.get('location', 'main')
    if current_location != 'main' and threads_data.get(current_location):
        history_button_text = random.choice(thread_messages[lang]['show_history_button'])
        keyboard_buttons.append([InlineKeyboardButton(
            text=history_button_text,
            callback_data=f"show_history_{current_location}"
        )])
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---

    # Кнопки пагинации
    pagination_row = []
    total_pages = (len(sorted_thread_ids) + THREADS_PER_PAGE - 1) // THREADS_PER_PAGE
    if page > 0:
        prev_text = random.choice(thread_messages[lang]['prev_page_button'])
        pagination_row.append(InlineKeyboardButton(text=prev_text, callback_data=f"threads_page_{page - 1}"))
    if page < total_pages - 1:
        next_text = random.choice(thread_messages[lang]['next_page_button'])
        pagination_row.append(InlineKeyboardButton(text=next_text, callback_data=f"threads_page_{page + 1}"))

    if pagination_row:
        keyboard_buttons.append(pagination_row)

    full_text = header + "\n" + "\n".join(lines)
    keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)

    return full_text, keyboard

async def post_archive_to_channel(bots: dict[str, Bot], file_path: str, board_id: str, thread_info: dict) -> None:
    """Отправляет сгенерированный HTML-архив как документ в специальный Telegram-канал."""
    bot_instance = bots.get(ARCHIVE_POSTING_BOT_ID)
    if not bot_instance:
        print(f"⛔ Ошибка: бот для постинга архивов ('{ARCHIVE_POSTING_BOT_ID}') не найден в списке активных ботов.")
        return

    try:
        from aiogram.types import FSInputFile
        
        # Формируем данные
        title = escape_html(thread_info.get('title', 'Без названия'))
        board_name = BOARD_CONFIG.get(board_id, {}).get('name', board_id)

        # Формируем подпись к документу
        caption = (
            f"🗂 <b>Тред заархивирован</b>\n\n"
            f"<b>Доска:</b> {board_name}\n"
            f"<b>Заголовок:</b> {title}"
        )

        document = FSInputFile(file_path)

        # Отправляем документ с подписью
        await bot_instance.send_document(
            chat_id=ARCHIVE_CHANNEL_ID,
            document=document,
            caption=caption,
            parse_mode="HTML"
        )
        print(f"✅ Архив треда '{title}' отправлен в канал {ARCHIVE_CHANNEL_ID}.")

    except Exception as e:
        print(f"⛔ Не удалось отправить архив в канал {ARCHIVE_CHANNEL_ID}: {e}")

async def post_special_num_to_channel(bots: dict[str, Bot], board_id: str, post_num: int, level: int, content: dict, author_id: int):
    """Пересылает пост со "счастливым" номером в канал архивов."""
    bot_instance = bots.get(ARCHIVE_POSTING_BOT_ID)
    if not bot_instance:
        print(f"⛔ Ошибка: бот для постинга ('{ARCHIVE_POSTING_BOT_ID}') не найден.")
        return
        
    try:
        config = SPECIAL_NUMERALS_CONFIG[level]
        emoji = random.choice(config['emojis'])
        label = config['label'].upper()
        
        board_name = BOARD_CONFIG.get(board_id, {}).get('name', board_id)

        header = f"{emoji} <b>{label} #{post_num}</b> {emoji}\n\n<b>Доска:</b> {board_name}\n"
        
        # --- НАЧАЛО ИЗМЕНЕНИЙ ---
        text_or_caption_raw = content.get('text') or content.get('caption') or ""
        post_text = clean_html_tags(text_or_caption_raw)
        
        # Если текста/подписи нет, добавляем плейсхолдер для медиа
        if not post_text:
            content_type = content.get('type', 'unknown')
            media_placeholders = {
                'photo': '[Фото]',
                'video': '[Видео]',
                'animation': '[GIF]',
                'sticker': '[Стикер]',
                'document': '[Документ]',
                'audio': '[Аудио]',
                'voice': '[Голосовое сообщение]',
                'video_note': '[Кружок]'
            }
            post_text = media_placeholders.get(content_type, '[Медиа]')

        final_text = f"{header}\n{post_text}"
        
        if len(final_text) > 4096:
            final_text = final_text[:4093] + "..."
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---

        await bot_instance.send_message(
            chat_id=ARCHIVE_CHANNEL_ID,
            text=final_text,
            parse_mode="HTML"
        )
        print(f"✅ Пост #{post_num} ({label}) отправлен в канал.")

    except Exception as e:
        print(f"⛔ Не удалось отправить пост #{post_num} в канал: {e}")


def _get_thread_entry_keyboard(board_id: str) -> InlineKeyboardMarkup:
    """
    Создает и возвращает инлайн-клавиатуру для сообщения о входе в тред.
    """
    lang = 'en' if board_id == 'int' else 'ru'

    if lang == 'en':
        button_good_thread_text = "👍 Good Thread"
        button_leave_text = "Leave Thread"
    else:
        button_good_thread_text = "👍 Годный тред"
        button_leave_text = "Выйти из треда"

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=button_good_thread_text, callback_data="thread_like_placeholder"),
            InlineKeyboardButton(text=button_leave_text, callback_data="leave_thread")
        ]
    ])
    return keyboard

async def _send_op_commands_info(bot: Bot, chat_id: int, board_id: str):
    """
    Проверяет, является ли пользователь ОПом, и отправляет ему список команд модерации.
    """
    b_data = board_data[board_id]
    user_s = b_data.get('user_state', {}).get(chat_id, {})
    location = user_s.get('location', 'main')

    # Если пользователь не в треде, ничего не делаем
    if location == 'main':
        return

    thread_info = b_data.get('threads_data', {}).get(location)
    # Проверяем, что тред существует и что ID пользователя совпадает с ID ОПа
    if thread_info and thread_info.get('op_id') == chat_id:
        lang = 'en' if board_id == 'int' else 'ru'
        
        if lang == 'en':
            op_commands_text = (
                "<b>You are the OP of this thread.</b>\n\n"
                "You have access to moderation commands (reply to a message to use):\n"
                "<code>/mute</code> - Mute user in this thread for 10 minutes.\n"
                "<code>/unmute</code> - Unmute user.\n"
                "<i>(These commands have a 1-minute cooldown)</i>"
            )
        else:
            op_commands_text = (
                "<b>Вы являетесь ОПом этого треда.</b>\n\n"
                "Вам доступны команды модерации (используйте ответом на сообщение):\n"
                "<code>/mute</code> - Замутить пользователя в этом треде на 10 минут.\n"
                "<code>/unmute</code> - Размутить пользователя.\n"
                "<i>(Кулдаун на использование команд - 1 минута)</i>"
            )
        
        try:
            await asyncio.sleep(0.5) # Небольшая задержка для визуального разделения
            await bot.send_message(chat_id, op_commands_text, parse_mode="HTML")
        except (TelegramForbiddenError, TelegramBadRequest) as e:
            print(f"Не удалось отправить OP-команды пользователю {chat_id}: {e}")

async def post_thread_notification_to_channel(bots: dict[str, Bot], board_id: str, thread_id: str, thread_info: dict, event_type: str, details: dict | None = None):
    """
    Отправляет унифицированное уведомление о событиях треда в служебный канал.
    
    :param bots: Словарь с инстансами ботов.
    :param board_id: ID доски.
    :param thread_id: ID треда.
    :param thread_info: Словарь с данными треда.
    :param event_type: Тип события ('new_thread', 'milestone', 'high_activity').
    :param details: Дополнительная информация (например, {'posts': 150} или {'activity': 25.5}).
    """
    bot_instance = bots.get(ARCHIVE_POSTING_BOT_ID)
    if not bot_instance:
        print(f"⛔ Ошибка: бот для постинга ('{ARCHIVE_POSTING_BOT_ID}') не найден.")
        return

    details = details or {}
    title = escape_html(thread_info.get('title', 'Без названия'))
    board_name = BOARD_CONFIG.get(board_id, {}).get('name', board_id)
    
    message_text = ""

    if event_type == 'new_thread':
        message_text = (
            f"<b>🌱 Создан новый тред</b>\n\n"
            f"<b>Доска:</b> {board_name}\n"
            f"<b>Заголовок:</b> {title}"
        )
    elif event_type == 'milestone':
        posts_count = details.get('posts', 0)
        message_text = (
            f"<b>📈 Тред набрал {posts_count} постов</b>\n\n"
            f"<b>Доска:</b> {board_name}\n"
            f"<b>Заголовок:</b> {title}"
        )
    elif event_type == 'high_activity':
        activity = details.get('activity', 0)
        message_text = (
            f"<b>🔥 Высокая активность в треде ({activity:.1f} п/ч)</b>\n\n"
            f"<b>Доска:</b> {board_name}\n"
            f"<b>Заголовок:</b> {title}"
        )
    else:
        # Неизвестный тип события, ничего не делаем
        return

    try:
        await bot_instance.send_message(
            chat_id=ARCHIVE_CHANNEL_ID,
            text=message_text,
            parse_mode="HTML"
        )
        print(f"✅ Уведомление о треде '{title}' (событие: {event_type}) отправлено в канал.")
    except Exception as e:
        print(f"⛔ Не удалось отправить уведомление о треде '{title}' в канал: {e}")

def _sync_generate_thread_archive(board_id: str, thread_id: str, thread_info: dict) -> str | None:
    """
    Синхронная функция для генерации и сохранения HTML-архива треда.
    Возвращает путь к файлу в случае успеха или None в случае ошибки.
    """
    try:
        title = escape_html(thread_info.get('title', 'Без названия'))
        filepath = os.path.join(DATA_DIR, f"archive_{board_id}_{thread_id}.html")

        # ... (содержимое функции по генерации HTML остается без изменений) ...
        html_style = """
        <style>
            body { font-family: sans-serif; background-color: #f0f0f0; color: #333; line-height: 1.6; margin: 20px; }
            .container { max-width: 800px; margin: auto; background-color: #fff; padding: 20px; border-radius: 5px; box-shadow: 0 0 10px rgba(0,0,0,0.1); }
            h1 { color: #d00; border-bottom: 2px solid #ccc; padding-bottom: 10px; }
            .post { border: 1px solid #ddd; padding: 10px; margin-bottom: 15px; border-radius: 4px; background-color: #fafafa; }
            .post-header { font-size: 0.9em; color: #888; margin-bottom: 10px; }
            .post-header b { color: #d00; }
            .post-content { white-space: pre-wrap; word-wrap: break-word; }
            .greentext { color: #789922; }
            .reply-link { color: #d00; text-decoration: none; }
        </style>
        """
        html_parts = [
            '<!DOCTYPE html>\n', '<html lang="ru">\n', '<head>\n', '    <meta charset="UTF-8">\n',
            f'    <title>Архив треда: {title}</title>\n', f'    {html_style}\n', '</head>\n',
            '<body>\n', '    <div class="container">\n', f'        <h1>{title}</h1>\n'
        ]
        post_nums = thread_info.get('posts', [])
        for post_num in post_nums:
            post_data = messages_storage.get(post_num)
            if not post_data: continue
            content = post_data.get('content', {})
            timestamp = post_data.get('timestamp', datetime.now(UTC)).strftime('%Y-%m-%d %H:%M:%S UTC')
            post_body = ""
            if content.get('type') == 'text':
                text = clean_html_tags(content.get('text', ''))
                lines = text.split('\n')
                formatted_lines = []
                for line in lines:
                    safe_line = escape_html(line)
                    if safe_line.strip().startswith('&gt;'):
                        formatted_lines.append(f'<span class="greentext">{safe_line}</span>')
                    else:
                        formatted_lines.append(safe_line)
                post_body = "<br>".join(formatted_lines)
            elif content.get('type') in ['photo', 'video', 'animation', 'document', 'audio']:
                media_type_map = {'photo': 'Изображение', 'video': 'Видео', 'animation': 'GIF', 'document': 'Документ', 'audio': 'Аудио'}
                media_type = media_type_map.get(content.get('type'), 'Медиа')
                caption = escape_html(clean_html_tags(content.get('caption', '')))
                post_body = f"<b>[{media_type}]</b><br>{caption}"
            else:
                 post_body = f"<i>[{content.get('type', 'Системное сообщение')}]</i>"
            reply_to = content.get('reply_to_post')
            reply_html = f'<a href="#{reply_to}" class="reply-link">&gt;&gt;{reply_to}</a><br>' if reply_to else ""
            html_parts.append(
                f'        <div class="post" id="{post_num}">\n'
                '            <div class="post-header">\n'
                f'                <b>Пост №{post_num}</b> - {timestamp}\n'
                '            </div>\n'
                '            <div class="post-content">\n'
                f'                {reply_html}{post_body}\n'
                '            </div>\n'
                '        </div>\n'
            )
        html_parts.extend(['    </div>\n', '</body>\n', '</html>\n'])
        final_html_content = "".join(html_parts)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(final_html_content)
        
        print(f"✅ [{board_id}] Архив для треда {thread_id} сохранен в {filepath}")
        return filepath

    except Exception as e:
        print(f"⛔ [{board_id}] Ошибка генерации архива для треда {thread_id}: {e}")
        return None
        
async def archive_thread(bots: dict[str, Bot], board_id: str, thread_id: str, thread_info: dict):
    """Асинхронная обертка для генерации архива треда и отправки его в канал."""
    loop = asyncio.get_running_loop()
    filepath = await loop.run_in_executor(
        save_executor,
        _sync_generate_thread_archive,
        board_id, thread_id, thread_info
    )
    # Если файл успешно создан (путь получен), отправляем его в канал
    if filepath:
        await post_archive_to_channel(bots, filepath, board_id, thread_info)

@dp.message(Command("cancel"), FSMContext)
async def cmd_cancel_fsm(message: types.Message, state: FSMContext, board_id: str | None):
    """
    Отменяет любое FSM состояние, в котором находится пользователь.
    """
    current_state = await state.get_state()
    if current_state is None:
        # Если пользователь не в состоянии, то и отменять нечего.
        # Просто удаляем команду, чтобы не мешать.
        try:
            await message.delete()
        except TelegramBadRequest:
            pass
        return

    # Очищаем состояние
    await state.clear()
    
    # Отправляем подтверждение
    if board_id:
        lang = 'en' if board_id == 'int' else 'ru'
        response_text = random.choice(thread_messages[lang]['create_cancelled'])
        await message.answer(response_text)
    
    try:
        await message.delete()
    except TelegramBadRequest:
        pass


async def thread_lifecycle_manager(bots: dict[str, Bot]):
    """Фоновая задача для управления жизненным циклом тредов: архивация и удаление."""
    while True:
        await asyncio.sleep(60) # Проверка раз в минуту
        now_dt = datetime.now(UTC)

        # --- НАЧАЛО ИЗМЕНЕНИЙ: Разделение на фазы сбора данных и выполнения действий ---
        
        # Фаза 1: Сбор задач и быстрое изменение состояния под блокировкой
        archives_to_generate = []    # [(board_id, thread_id, thread_info_copy), ...]
        notifications_to_queue = []  # [(board_id, recipients, content, thread_id_or_None), ...]
        
        async with storage_lock:
            for board_id in THREAD_BOARDS:
                b_data = board_data.get(board_id)
                if not b_data: continue

                threads_data = b_data.get('threads_data', {})
                threads_to_delete = []
                lang = 'en' if board_id == 'int' else 'ru'

                # Этап 1.1: Проверка на архивацию по лимиту постов
                for thread_id, thread_info in threads_data.items():
                    if not thread_info.get('is_archived') and len(thread_info.get('posts', [])) >= MAX_POSTS_PER_THREAD:
                        thread_info['is_archived'] = True
                        archives_to_generate.append((board_id, thread_id, thread_info.copy()))
                        
                        archive_text = random.choice(thread_messages[lang]['thread_archived']).format(
                            limit=MAX_POSTS_PER_THREAD,
                            title=thread_info.get('title', '...')
                        )
                        content = {'type': 'text', 'text': archive_text, 'is_system_message': True}
                        recipients = thread_info.get('subscribers', set())
                        if recipients:
                            notifications_to_queue.append((board_id, recipients, content, thread_id))

                # Этап 1.2: Проверка на удаление старейшего треда при переполнении
                active_threads = {tid: tdata for tid, tdata in threads_data.items() if not tdata.get('is_archived')}
                if len(active_threads) > MAX_ACTIVE_THREADS:
                    num_to_remove = len(active_threads) - MAX_ACTIVE_THREADS
                    oldest_threads = sorted(
                        active_threads.items(),
                        key=lambda item: item[1].get('last_activity_at', 0)
                    )[:num_to_remove]

                    for thread_id, thread_info in oldest_threads:
                        removed_title = thread_info.get('title', '...')
                        removal_text = random.choice(thread_messages[lang]['oldest_thread_removed']).format(title=removed_title)
                        content = {'type': 'text', 'text': removal_text, 'is_system_message': True}
                        recipients = b_data['users']['active']
                        notifications_to_queue.append((board_id, recipients, content, None))
                        threads_to_delete.append(thread_id)

                # Этап 1.3: Физическое удаление тредов из состояния
                if threads_to_delete:
                    for thread_id in threads_to_delete:
                        threads_data.pop(thread_id, None)
                    print(f"🧹 [{board_id}] Удалено {len(threads_to_delete)} старых тредов из состояния.")
        
        # --- Блокировка освобождена ---

        # Фаза 2: Выполнение медленных операций без блокировки
        for board_id, thread_id, thread_info_copy in archives_to_generate:
            asyncio.create_task(archive_thread(bots, board_id, thread_id, thread_info_copy))
            
        for board_id, recipients, content, thread_id in notifications_to_queue:
            try:
                if thread_id: # Уведомление для подписчиков треда
                    b_data = board_data[board_id]
                    thread_info = b_data.get('threads_data', {}).get(thread_id)
                    if thread_info:
                        local_post_num = len(thread_info.get('posts', [])) + 1
                        header = await format_thread_post_header(board_id, local_post_num, 0, thread_info)
                        _, pnum = await format_header(board_id)
                        content['header'] = header
                        
                        async with storage_lock:
                            messages_storage[pnum] = {'author_id': 0, 'timestamp': now_dt, 'content': content, 'board_id': board_id, 'thread_id': thread_id}
                            thread_info['posts'].append(pnum)
                        await message_queues[board_id].put({'recipients': recipients, 'content': content, 'post_num': pnum, 'board_id': board_id, 'thread_id': thread_id})
                else: # Уведомление на главную доску
                    header, pnum = await format_header(board_id)
                    content['header'] = header
                    async with storage_lock:
                         messages_storage[pnum] = {'author_id': 0, 'timestamp': now_dt, 'content': content, 'board_id': board_id}
                    await message_queues[board_id].put({'recipients': recipients, 'content': content, 'post_num': pnum, 'board_id': board_id})
            except Exception as e:
                 print(f"❌ Ошибка при постановке уведомления в очередь в thread_lifecycle_manager: {e}")
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---

async def thread_activity_monitor(bots: dict[str, Bot]):
    """
    Фоновая задача для отслеживания активности тредов и уведомления о высокой активности.
    """
    await asyncio.sleep(120)  # Начальная задержка 2 минуты

    while True:
        try:
            await asyncio.sleep(600)  # Проверка каждые 10 минут
            
            # --- НАЧАЛО ИЗМЕНЕНИЙ: Оптимизированный и безопасный сбор данных ---
            # Собираем всю необходимую информацию о постах за один проход под блокировкой,
            # чтобы избежать многократных блокировок и небезопасных итераций.
            thread_posts_by_board = defaultdict(lambda: defaultdict(list))
            async with storage_lock:
                one_hour_ago_for_check = datetime.now(UTC) - timedelta(hours=1)
                # Итерируемся по values(), так как ключи нам не нужны для этой операции
                for post_data in messages_storage.values():
                    # Собираем только свежие посты из тредов
                    timestamp = post_data.get('timestamp')
                    if timestamp and timestamp > one_hour_ago_for_check:
                        board_id = post_data.get('board_id')
                        thread_id = post_data.get('thread_id')
                        # Добавляем post_num (или 1) для последующего подсчета через len()
                        if board_id in THREAD_BOARDS and thread_id:
                            thread_posts_by_board[board_id][thread_id].append(1)
            # --- КОНЕЦ ИЗМЕНЕНИЙ ---

            for board_id in THREAD_BOARDS:
                b_data = board_data.get(board_id)
                if not b_data:
                    continue

                threads_data = b_data.get('threads_data', {})
                
                for thread_id, thread_info in threads_data.items():
                    # Пропускаем уже архивированные треды или те, о которых уже уведомили
                    if thread_info.get('is_archived') or thread_info.get('activity_notified'):
                        continue
                    
                    # Считаем посты на основе предварительно собранных данных
                    recent_posts_count = len(thread_posts_by_board.get(board_id, {}).get(thread_id, []))

                    # Порог активности - 15 постов/час
                    ACTIVITY_THRESHOLD = 15
                    if recent_posts_count >= ACTIVITY_THRESHOLD:
                        # Устанавливаем флаг, чтобы больше не уведомлять
                        thread_info['activity_notified'] = True
                        
                        # Запускаем отправку уведомления
                        asyncio.create_task(post_thread_notification_to_channel(
                            bots=bots,
                            board_id=board_id,
                            thread_id=thread_id,
                            thread_info=thread_info,
                            event_type='high_activity',
                            details={'activity': float(recent_posts_count)}
                        ))

        except Exception as e:
            print(f"❌ Ошибка в thread_activity_monitor: {e}")
            await asyncio.sleep(120) # В случае ошибки ждем дольше перед повторной попыткой

@dp.message(ThreadCreateStates.waiting_for_op_post, F.text)
async def process_op_post_text(message: types.Message, state: FSMContext, board_id: str | None):
    """
    Ловит текст для ОП-поста, сохраняет его в FSM-контексте 
    и переводит на этап подтверждения.
    """
    if not board_id: return
    lang = 'en' if board_id == 'int' else 'ru'

    # Сохраняем текст поста в хранилище FSM. Используем html_text, чтобы сохранить форматирование.
    op_post_text = message.html_text
    await state.update_data(op_post_text=op_post_text)

    # Переводим пользователя на следующий шаг - подтверждение
    await state.set_state(ThreadCreateStates.waiting_for_confirmation)

    if lang == 'en':
        confirmation_text = f"You want to create a thread with this opening post:\n\n---\n{op_post_text}\n---\n\nCreate?"
        button_create = "✅ Create Thread"
        button_edit = "✏️ Edit Text"
    else:
        confirmation_text = f"Вы хотите создать тред с таким ОП-постом:\n\n---\n{op_post_text}\n---\n\nСоздаем?"
        button_create = "✅ Создать тред"
        button_edit = "✏️ Редактировать"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=button_create, callback_data="create_thread_confirm"),
            InlineKeyboardButton(text=button_edit, callback_data="create_thread_edit")
        ]
    ])

    await message.answer(confirmation_text, reply_markup=keyboard, parse_mode="HTML")



@dp.message(ThreadCreateStates.waiting_for_op_post)
async def process_op_post_invalid(message: types.Message, state: FSMContext, board_id: str | None):
    """
    Обрабатывает некорректный ввод (не текст) в состоянии ожидания ОП-поста.
    """
    if not board_id: return
    lang = 'en' if board_id == 'int' else 'ru'

    response_text = random.choice(thread_messages[lang]['create_invalid_input'])
    
    try:
        await message.answer(response_text)
        await message.delete()
    except (TelegramForbiddenError, TelegramBadRequest):
        pass


@dp.callback_query(F.data == "create_thread_start")
async def cb_create_thread_start(callback: types.CallbackQuery, state: FSMContext, board_id: str | None):
    """
    Обрабатывает нажатие кнопки 'Создать тред', запускает FSM.
    """
    if not board_id or board_id not in THREAD_BOARDS:
        await callback.answer("This feature is not available here.", show_alert=True)
        return

    lang = 'en' if board_id == 'int' else 'ru'

    # Устанавливаем состояние ожидания ОП-поста
    await state.set_state(ThreadCreateStates.waiting_for_op_post)

    # Отправляем пользователю инструкцию
    prompt_text = random.choice(thread_messages[lang]['create_prompt_op_post'])
    
    # Сначала отвечаем на колбэк, чтобы убрать "часики"
    await callback.answer()
    
    # Отправляем новое сообщение и удаляем старое
    try:
        await callback.message.answer(prompt_text)
        await callback.message.delete()
    except (TelegramForbiddenError, TelegramBadRequest):
        pass # Игнорируем, если не удалось (например, сообщение уже удалено)

@dp.callback_query(F.data.startswith("threads_page_"))
async def cq_threads_page(callback: types.CallbackQuery, board_id: str | None):
    """Обрабатывает переключение страниц в списке тредов."""
    if not board_id or board_id not in THREAD_BOARDS:
        await callback.answer("This action is not available here.", show_alert=True)
        return

    try:
        page = int(callback.data.split("_")[-1])
    except (ValueError, IndexError):
        await callback.answer("Invalid page number.", show_alert=True)
        return
        
    b_data = board_data[board_id]
    
    text, keyboard = await generate_threads_page(b_data, callback.from_user.id, page=page)
    
    try:
        if callback.message.text != text:
             await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
            print(f"Ошибка обновления списка тредов: {e}")
            
    await callback.answer()


@dp.callback_query(F.data.startswith("show_history_"))
async def cq_thread_history(callback: types.CallbackQuery, board_id: str | None):
    """Отправляет полную историю сообщений треда."""
    if not board_id or board_id not in THREAD_BOARDS:
        await callback.answer("This action is not available here.", show_alert=True)
        return
        
    try:
        thread_id = callback.data.split("_")[-1]
    except (ValueError, IndexError):
        await callback.answer("Invalid thread ID.", show_alert=True)
        return

    user_id = callback.from_user.id
    b_data = board_data[board_id]
    lang = 'en' if board_id == 'int' else 'ru'
    
    # Проверка кулдауна
    user_s = b_data['user_state'].setdefault(user_id, {})
    now_ts = time.time()
    last_history_req = user_s.get('last_history_request', 0)
    
    if now_ts - last_history_req < THREAD_HISTORY_COOLDOWN:
        cooldown_msg = random.choice(thread_messages[lang]['history_cooldown']).format(
            minutes=str(THREAD_HISTORY_COOLDOWN // 60)
        )
        await callback.answer(cooldown_msg, show_alert=True)
        return

    thread_info = b_data.get('threads_data', {}).get(thread_id)
    if not thread_info:
        await callback.answer(random.choice(thread_messages[lang]['thread_not_found']), show_alert=True)
        return

    # Обновляем время последнего запроса
    user_s['last_history_request'] = now_ts
    await callback.answer("⏳ Загружаю историю...")

    # Используем уже существующую логику отправки пропущенных сообщений,
    # просто сбрасывая точку отсчета на 0
    temp_user_state = user_s.copy()
    temp_user_state['last_seen_threads'] = {thread_id: 0}
    b_data['user_state'][user_id] = temp_user_state

    await send_missed_messages(callback.bot, board_id, user_id, thread_id)
    
@dp.callback_query(F.data.startswith("enter_thread_"))
async def cq_enter_thread(callback: types.CallbackQuery, board_id: str | None):
    """Обрабатывает вход пользователя в тред по нажатию кнопки."""
    if not board_id or board_id not in THREAD_BOARDS:
        await callback.answer("This action is not available here.", show_alert=True)
        return
        
    try:
        thread_id = callback.data.split("_")[-1]
    except (ValueError, IndexError):
        await callback.answer("Invalid thread ID.", show_alert=True)
        return
        
    user_id = callback.from_user.id
    b_data = board_data[board_id]
    lang = 'en' if board_id == 'int' else 'ru'
    
    threads_data = b_data.get('threads_data', {})
    if thread_id not in threads_data:
        await callback.answer(random.choice(thread_messages[lang]['thread_not_found']), show_alert=True)
        text, keyboard = await generate_threads_page(b_data, user_id, page=0)
        try:
            await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
        except TelegramBadRequest: pass
        return

    user_s = b_data['user_state'].setdefault(user_id, {})
    
    now_ts = time.time()
    last_switch = user_s.get('last_location_switch', 0)
    if now_ts - last_switch < LOCATION_SWITCH_COOLDOWN:
        await callback.answer(random.choice(thread_messages[lang]['location_switch_cooldown']), show_alert=True)
        return

    current_location = user_s.get('location', 'main')

    if current_location == thread_id:
        await callback.answer()
        return
        
    if current_location == 'main':
        user_s['last_seen_main'] = state.get('post_counter', 0)

    user_s['location'] = thread_id
    user_s['last_location_switch'] = now_ts
    threads_data[thread_id].setdefault('subscribers', set()).add(user_id)
    
    # --- НАЧАЛО ИЗМЕНЕНИЙ: Обновленная логика приветствия ---
    await callback.answer()
    try:
        await callback.message.delete()
    except TelegramBadRequest:
        pass

    was_missed = await send_missed_messages(callback.bot, board_id, user_id, thread_id)
    
    if not was_missed:
        thread_title = threads_data[thread_id].get('title', '...')
        seen_threads = user_s.setdefault('last_seen_threads', {})
        if thread_id not in seen_threads:
            response_text = random.choice(thread_messages[lang]['enter_thread_prompt']).format(title=thread_title)
        else:
            response_text = random.choice(thread_messages[lang]['enter_thread_success']).format(title=thread_title)
        
        entry_keyboard = _get_thread_entry_keyboard(board_id)
        try:
            await callback.message.answer(response_text, reply_markup=entry_keyboard, parse_mode="HTML")
        except (TelegramForbiddenError, TelegramBadRequest):
            pass
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---
        
    await _send_op_commands_info(callback.bot, user_id, board_id)

@dp.callback_query(F.data == "leave_thread")
async def cb_leave_thread(callback: types.CallbackQuery, board_id: str | None):
    """
    Обрабатывает нажатие кнопки 'Выйти из треда', эмулируя команду /leave.
    """
    if not board_id or board_id not in THREAD_BOARDS:
        await callback.answer("This action is not available here.", show_alert=True)
        return

    user_id = callback.from_user.id
    b_data = board_data[board_id]
    lang = 'en' if board_id == 'int' else 'ru'

    user_s = b_data['user_state'].setdefault(user_id, {})
    current_location = user_s.get('location', 'main')

    if current_location == 'main':
        # Пользователь уже на главной доске, просто убираем сообщение с кнопкой
        await callback.answer()
        try:
            await callback.message.delete()
        except TelegramBadRequest:
            pass
        return

    # Сохраняем, какой пост был последним в треде перед уходом
    thread_id = current_location
    thread_info = b_data.get('threads_data', {}).get(thread_id)
    if thread_info:
        last_thread_post = thread_info.get('posts', [0])[-1] if thread_info.get('posts') else 0
        user_s.setdefault('last_seen_threads', {})[thread_id] = last_thread_post
    
    # Перемещаем пользователя на главную доску
    user_s['location'] = 'main'
    user_s['last_location_switch'] = time.time()
    
    # Отправляем сообщение о выходе и удаляем старое
    response_text = random.choice(thread_messages[lang]['leave_thread_success'])
    await callback.answer()
    try:
        await callback.message.answer(response_text)
        await callback.message.delete()
    except (TelegramForbiddenError, TelegramBadRequest):
        pass

    # Подгружаем пропущенные сообщения с доски
    await send_missed_messages(callback.bot, board_id, user_id, 'main')

@dp.message(Command("leave"))
async def cmd_leave(message: types.Message, board_id: str | None):
    """Выводит пользователя из треда обратно на доску."""
    if not board_id: return

    if board_id not in THREAD_BOARDS:
        try: await message.delete()
        except Exception: pass
        return

    user_id = message.from_user.id
    b_data = board_data[board_id]
    lang = 'en' if board_id == 'int' else 'ru'

    user_s = b_data['user_state'].setdefault(user_id, {})
    current_location = user_s.get('location', 'main')

    if current_location == 'main':
        await message.delete()
        return

    now_ts = time.time()
    last_switch = user_s.get('last_location_switch', 0)
    if now_ts - last_switch < LOCATION_SWITCH_COOLDOWN:
        cooldown_text = random.choice(thread_messages[lang]['location_switch_cooldown'])
        await message.answer(cooldown_text)
        await message.delete()
        return

    # --- НАЧАЛО ИЗМЕНЕНИЙ ---
    # Сохраняем, какой пост был последним в треде перед уходом
    thread_id = current_location
    thread_info = b_data.get('threads_data', {}).get(thread_id)
    if thread_info:
        last_thread_post = thread_info.get('posts', [0])[-1] if thread_info.get('posts') else 0
        user_s.setdefault('last_seen_threads', {})[thread_id] = last_thread_post
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---

    user_s['location'] = 'main'
    user_s['last_location_switch'] = now_ts
    
    # --- НАЧАЛО ИЗМЕНЕНИЙ ---
    # Подгружаем пропущенные сообщения с доски
    await send_missed_messages(message.bot, board_id, user_id, 'main')
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---
    
    response_text = random.choice(thread_messages[lang]['leave_thread_success'])
    await message.answer(response_text)
    await message.delete()

@dp.message(Command("mute"))
async def cmd_mute(message: Message, board_id: str | None):
    if not board_id or not is_admin(message.from_user.id, board_id):
        # Если не админ, передаем управление следующему обработчику (OP-команде)
        # Для этого нужно, чтобы aiogram продолжил поиск
        # В данном случае, мы ничего не делаем, и aiogram пойдет дальше по списку
        return

    command_args = message.text.split()[1:]
    if not command_args and not message.reply_to_message:
        await message.answer("Использование: /mute <user_id> [время] или ответом на сообщение.")
        await message.delete()
        return

    target_id = None
    duration_str = "24h"

    if message.reply_to_message:
        async with storage_lock:
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

    board_name = BOARD_CONFIG[board_id]['name']
    await message.answer(
        f"🔇 Хуила {target_id} замучен на {duration_text} на доске {board_name}\n"
        f"Удалено сообщений за последние 5 минут: {deleted_count}",
        parse_mode="HTML"
    )

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
                "🔇 ТЫ - ГОВНО. ОТПРАВЛЯЙСЯ В МУТ НА {time}.",
                "🤐 ЗАВАЛИ ХАВАЛКУ! ТВОЙ РОТ ЗАКЛЕЕН СКОТЧЕМ НА {time}",
            ]
        
        notification_text = random.choice(phrases).format(board=board_name, duration=duration_text, deleted=deleted_count)
        await message.bot.send_message(target_id, notification_text, parse_mode="HTML")
    except:
        pass
    await message.delete()

@dp.message(Command("unmute"))
async def cmd_unmute(message: types.Message, board_id: str | None):
    if not board_id or not is_admin(message.from_user.id, board_id):
        return

    target_id = None
    if message.reply_to_message:
        async with storage_lock:
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
        await message.answer(f"🔈 Пользователь {target_id} размучен на доске {board_name}.")
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
        await message.answer(f"Пользователь {target_id} не был в муте на этой доске.")
    await message.delete()

@dp.message(Command("shadowmute"))
async def cmd_shadowmute(message: Message, board_id: str | None):
    if not board_id or not is_admin(message.from_user.id, board_id):
        return

    args = message.text.split()[1:]
    target_id = None
    duration_str = "24h"

    if message.reply_to_message:
        async with storage_lock:
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

        board_name = BOARD_CONFIG[board_id]['name']
        await message.answer(f"👻 Тихо замучен пользователь {target_id} на {time_str} на доске {board_name}.")
    except ValueError:
        await message.answer("❌ Неверный формат времени. Примеры: 30m, 2h, 1d")
    await message.delete()


@dp.message(Command("unshadowmute"))
async def cmd_unshadowmute(message: Message, board_id: str | None):
    if not board_id or not is_admin(message.from_user.id, board_id):
        return

    target_id = None
    parts = message.text.split()
    if len(parts) >= 2 and parts[1].isdigit():
        target_id = int(parts[1])
    elif message.reply_to_message:
        async with storage_lock:
            target_id = get_author_id_by_reply(message)

    if not target_id:
        await message.answer("Использование: /unshadowmute <user_id> или ответ на сообщение.")
        return
    
    b_data = board_data[board_id]
    board_name = BOARD_CONFIG[board_id]['name']
    if b_data['shadow_mutes'].pop(target_id, None):
        await message.answer(f"👻 Пользователь {target_id} тихо размучен на доске {board_name}.")
    else:
        await message.answer(f"ℹ️ Пользователь {target_id} не в shadow-муте на этой доске.")
    await message.delete()

@dp.message(Command("mute"))
async def cmd_op_mute(message: types.Message, board_id: str | None):
    """Обрабатывает локальный мут пользователя ОПом в треде."""
    if not board_id or board_id not in THREAD_BOARDS:
        return

    # Если это админ, то команда уже отработала в предыдущем хендлере
    if is_admin(message.from_user.id, board_id):
        return

    user_id = message.from_user.id
    b_data = board_data[board_id]
    lang = 'en' if board_id == 'int' else 'ru'

    user_s = b_data.get('user_state', {}).get(user_id, {})
    location = user_s.get('location', 'main')

    if location == 'main':
        await message.delete()
        return

    thread_id = location
    threads_data = b_data.get('threads_data', {})
    thread_info = threads_data.get(thread_id)

    if not thread_info or thread_info.get('op_id') != user_id:
        await message.delete()
        return

    # --- НАЧАЛО ИЗМЕНЕНИЙ: Проверка кулдауна ---
    now_ts = time.time()
    last_op_command_ts = user_s.get('last_op_command_ts', 0)

    if now_ts - last_op_command_ts < OP_COMMAND_COOLDOWN:
        # Не отвечаем сообщением, чтобы не создавать лишний флуд
        await message.delete()
        return
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---

    if not message.reply_to_message:
        await message.delete()
        return
        
    target_id = None
    async with storage_lock:
        target_id = get_author_id_by_reply(message)

    if not target_id or target_id == user_id:
        await message.delete()
        return
        
    duration_seconds = 600
    expires_ts = time.time() + duration_seconds
    
    thread_info.setdefault('local_mutes', {})[target_id] = expires_ts

    duration_text = str(duration_seconds // 60)
    response_text = random.choice(thread_messages[lang]['op_mute_success']).format(duration=duration_text)
    
    await message.answer(response_text)
    await message.delete()

    # --- НАЧАЛО ИЗМЕНЕНИЙ: Обновление времени использования команды ---
    user_s['last_op_command_ts'] = now_ts
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---

@dp.message(Command("unmute"))
async def cmd_op_unmute(message: types.Message, board_id: str | None):
    """Обрабатывает локальный размут пользователя ОПом в треде."""
    if not board_id or board_id not in THREAD_BOARDS:
        return

    if is_admin(message.from_user.id, board_id):
        return

    user_id = message.from_user.id
    b_data = board_data[board_id]
    lang = 'en' if board_id == 'int' else 'ru'

    user_s = b_data.get('user_state', {}).get(user_id, {})
    location = user_s.get('location', 'main')

    if location == 'main':
        await message.delete()
        return

    thread_id = location
    threads_data = b_data.get('threads_data', {})
    thread_info = threads_data.get(thread_id)

    if not thread_info or thread_info.get('op_id') != user_id:
        await message.delete()
        return

    # --- НАЧАЛО ИЗМЕНЕНИЙ: Проверка кулдауна ---
    now_ts = time.time()
    last_op_command_ts = user_s.get('last_op_command_ts', 0)

    if now_ts - last_op_command_ts < OP_COMMAND_COOLDOWN:
        await message.delete()
        return
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---

    if not message.reply_to_message:
        await message.delete()
        return

    target_id = None
    async with storage_lock:
        target_id = get_author_id_by_reply(message)

    if not target_id:
        await message.delete()
        return

    local_mutes = thread_info.get('local_mutes', {})
    if target_id in local_mutes:
        del local_mutes[target_id]
        response_text = random.choice(thread_messages[lang]['op_unmute_success'])
        await message.answer(response_text)
        # --- НАЧАЛО ИЗМЕНЕНИЙ: Обновление времени использования команды ---
        user_s['last_op_command_ts'] = now_ts
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---
    
    await message.delete()
    
@dp.message(Command("shadowmute"))
async def cmd_op_shadowmute(message: types.Message, board_id: str | None):
    """Обрабатывает локальный теневой мут пользователя ОПом в треде."""
    if not board_id or board_id not in THREAD_BOARDS: return
    if is_admin(message.from_user.id, board_id): return

    user_id = message.from_user.id
    b_data = board_data[board_id]
    lang = 'en' if board_id == 'int' else 'ru'

    user_s = b_data.get('user_state', {}).get(user_id, {})
    location = user_s.get('location', 'main')
    
    if location == 'main':
        await message.delete()
        return

    thread_info = b_data.get('threads_data', {}).get(location)
    if not thread_info or thread_info.get('op_id') != user_id:
        await message.delete()
        return

    # --- НАЧАЛО ИЗМЕНЕНИЙ: Проверка кулдауна ---
    now_ts = time.time()
    last_op_command_ts = user_s.get('last_op_command_ts', 0)

    if now_ts - last_op_command_ts < OP_COMMAND_COOLDOWN:
        await message.delete()
        return
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---

    if not message.reply_to_message:
        await message.delete()
        return

    target_id = None
    async with storage_lock:
        target_id = get_author_id_by_reply(message)
    
    if not target_id or target_id == user_id:
        await message.delete()
        return
        
    duration_seconds = 600 # 10 минут
    expires_ts = time.time() + duration_seconds
    thread_info.setdefault('local_shadow_mutes', {})[target_id] = expires_ts
    
    duration_text = str(duration_seconds // 60)
    response_text = random.choice(thread_messages[lang]['op_mute_success']).format(duration=duration_text)
    await message.answer(f"👻 (shadow) {response_text}")
    await message.delete()

    # --- НАЧАЛО ИЗМЕНЕНИЙ: Обновление времени использования команды ---
    user_s['last_op_command_ts'] = now_ts
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---

@dp.message(Command("unshadowmute"))
async def cmd_op_unshadowmute(message: types.Message, board_id: str | None):
    """Обрабатывает локальный теневой размут пользователя ОПом в треде."""
    if not board_id or board_id not in THREAD_BOARDS: return
    if is_admin(message.from_user.id, board_id): return

    user_id = message.from_user.id
    b_data = board_data[board_id]
    lang = 'en' if board_id == 'int' else 'ru'

    user_s = b_data.get('user_state', {}).get(user_id, {})
    location = user_s.get('location', 'main')
    
    if location == 'main':
        await message.delete()
        return

    thread_info = b_data.get('threads_data', {}).get(location)
    if not thread_info or thread_info.get('op_id') != user_id:
        await message.delete()
        return

    # --- НАЧАЛО ИЗМЕНЕНИЙ: Проверка кулдауна ---
    now_ts = time.time()
    last_op_command_ts = user_s.get('last_op_command_ts', 0)

    if now_ts - last_op_command_ts < OP_COMMAND_COOLDOWN:
        await message.delete()
        return
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---

    if not message.reply_to_message:
        await message.delete()
        return
    
    target_id = None
    async with storage_lock:
        target_id = get_author_id_by_reply(message)

    if not target_id:
        await message.delete()
        return

    local_shadow_mutes = thread_info.get('local_shadow_mutes', {})
    if target_id in local_shadow_mutes:
        del local_shadow_mutes[target_id]
        response_text = random.choice(thread_messages[lang]['op_unmute_success'])
        await message.answer(f"👻 (shadow) {response_text}")
        # --- НАЧАЛО ИЗМЕНЕНИЙ: Обновление времени использования команды ---
        user_s['last_op_command_ts'] = now_ts
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---
    
    await message.delete()
    
@dp.message(Command("invite"))
async def cmd_invite(message: types.Message, board_id: str | None):
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
async def cmd_stats(message: types.Message, board_id: str | None):
    if not board_id: return
    
    # --- НАЧАЛО ИЗМЕНЕНИЙ ---
    # Блокируем команду, если пользователь находится внутри треда
    if board_id in THREAD_BOARDS:
        user_id = message.from_user.id
        b_data = board_data[board_id]
        user_location = b_data.get('user_state', {}).get(user_id, {}).get('location', 'main')
        if user_location != 'main':
            try: await message.delete()
            except Exception: pass
            return
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---

    b_data = board_data[board_id]
    total_users_on_board = len(b_data['users']['active'])
    total_posts_on_board = b_data.get('board_post_count', 0)
    total_users_b = len(board_data['b']['users']['active'])

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
        
    if random.random() < 0.5:
        if board_id == 'int':
            dvach_caption = random.choice(DVACH_STATS_CAPTIONS_EN)
        else:
            dvach_caption = random.choice(DVACH_STATS_CAPTIONS)
        stats_text = f"{stats_text}\n\n<i>{dvach_caption}</i>"
        
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
async def cmd_anime(message: types.Message, board_id: str | None):
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
        "🌸 お前はもう死んでいる... АНИМЕ РЕЖИМ: OMAE WA MOU SHINDEIRU!",
        "✧･ﾟ: *✧･ﾟ♡ ВКЛЮЧАЕМ КАВАЙНЫЙ АД! ♡･ﾟ✧*:･ﾟ✧",
        "⚡ 千 本 桜 ⚡ НЯ!",
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
async def cmd_deanon(message: Message, board_id: str | None):
    global last_deanon_time
    if not board_id: return
    
    current_time = time.time()
    async with deanon_lock:
        if current_time - last_deanon_time < DEANON_COOLDOWN:
            cooldown_msg = random.choice(DEANON_COOLDOWN_PHRASES)
            try:
                sent_msg = await message.answer(cooldown_msg)
                asyncio.create_task(delete_message_after_delay(sent_msg, 5))
            except Exception: pass
            await message.delete()
            return
        last_deanon_time = current_time
    
    lang = 'en' if board_id == 'int' else 'ru'
    if not message.reply_to_message:
        reply_text = "⚠️ Reply to a message to de-anonymize!" if lang == 'en' else "⚠️ Ответь на сообщение для деанона!"
        await message.answer(reply_text)
        await message.delete()
        return

    user_id = message.from_user.id
    b_data = board_data[board_id]
    user_location = 'main'
    if board_id in THREAD_BOARDS:
        user_location = b_data.get('user_state', {}).get(user_id, {}).get('location', 'main')

    original_author_id = None
    target_post = None
    reply_info = {}

    async with storage_lock:
        # --- ИЗМЕНЕНИЕ: Используем ID чата и сообщения из reply_to_message, а не ID самого пользователя ---
        target_chat_id = message.reply_to_message.chat.id
        target_mid = message.reply_to_message.message_id
        target_post = message_to_post.get((target_chat_id, target_mid))
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---
        
        if target_post and target_post in messages_storage:
            original_author_id = messages_storage[target_post].get('author_id')
            reply_info = post_to_messages.get(target_post, {})

    if not target_post or original_author_id is None:
        reply_text = "🚫 Could not find the post to de-anonymize..." if lang == 'en' else "🚫 Не удалось найти пост для деанона..."
        await message.answer(reply_text)
        await message.delete()
        return

    if original_author_id == 0:
        reply_text = "⚠️ System messages cannot be de-anonymized." if lang == 'en' else "⚠️ Системные сообщения нельзя деанонить."
        await message.answer(reply_text)
        await message.delete()
        return
        
    name, surname, city, profession, fetish, detail = generate_deanon_info(lang=lang)
    
    deanon_text, header_text = "", ""
    if lang == 'en':
        deanon_text = (f"\nThis anon's name is: {name} {surname}...")
        header_text = "### DEANON ###"
    else:
        deanon_text = (f"\nЭтого анона зовут: {name} {surname}...")
        header_text = "### ДЕАНОН ###"

    content = {"type": "text", "header": header_text, "text": deanon_text, "reply_to_post": target_post}

    if board_id in THREAD_BOARDS and user_location != 'main':
        thread_id = user_location
        thread_info = b_data.get('threads_data', {}).get(thread_id)
        if thread_info and not thread_info.get('is_archived'):
            # Глобальный номер для системного сообщения все равно нужен
            _, pnum = await format_header(board_id)
            content['post_num'] = pnum
            content['header'] = await format_thread_post_header(board_id, len(thread_info.get('posts', [])) + 1, 0, thread_info)

            async with storage_lock:
                messages_storage[pnum] = {'author_id': 0, 'timestamp': datetime.now(UTC), 'content': content, 'board_id': board_id, 'thread_id': thread_id}
                thread_info['posts'].append(pnum)
                thread_info['last_activity_at'] = time.time()
            
            # --- НАЧАЛО ИЗМЕНЕНИЙ: Добавляем thread_id в очередь ---
            await message_queues[board_id].put({
                "recipients": thread_info.get('subscribers', set()), "content": content, "post_num": pnum,
                "board_id": board_id, "thread_id": thread_id
            })
            # --- КОНЕЦ ИЗМЕНЕНИЙ ---
            await message.delete()
            return
            
    _, pnum = await format_header(board_id)
    content['post_num'] = pnum
    async with storage_lock:
        messages_storage[pnum] = {'author_id': 0, 'timestamp': datetime.now(UTC), 'content': content, 'board_id': board_id}
    await message_queues[board_id].put({
        "recipients": board_data[board_id]['users']['active'], "content": content, "post_num": pnum,
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
async def cmd_zaputin(message: types.Message, board_id: str | None):
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
        "🇷🇺 ЗА ПУТІНА! ЗА ДЕДОВ! РЕЖИМ 'БАЛТИЙСКИЙ ШТУРМ' АКТИВИРОВАН!",
        "🚨 ТРЕВОГА! В ЧАТЕ ЗАМЕЧЕНА ЛИБЕРДА! ВКЛЮЧАЕМ ПРОТОКОЛ 'ЧВК ВАГНЕР'",
        "🧨 ПОДРЫВНАЯ АКТИВНОСТЬ В ЧАТЕ! Включаем режим 'АРМАТА'. За Родину!",
        "🪆 МАТРЁШКА РАСКРЫЛАСЬ! Режим имперского величия активирован! ZА ПУТИНА!",
        "☢️ ЯДЕРНЫЙ ПРОТОКОЛ АКТИВИРОВАН! Готовим гиперзвуковые ракеты по целям!",
        "🦅 ОРЕШНИК ЗАПУЩЕН! Режим патриотизма включен. Крым наш!",
        "🐻 МЕДВЕДЬ ПРОСНУЛСЯ! Режим ядерного троллинга активирован! ZOV ZOV ZOV",
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
        "Спецоперация по защите чата успешно завершена. 🇷🇺 Можно снова быть либерахами. Возвращаемся к лолям.",
        "Перегруппировка! 🫡 Патриотический режим временно отключен для пополнения запасов водки и матрешек.",
        "Шойгу! Герасимов! Где патроны?! 💥 Режим патриотизма отключен до выяснения обстоятельств.",
        "Митинг окончен. ✊ Расходимся, пока не приехал ОМОН. Патриотизм выключен.",
        "Русский мир свернулся до размеров МКАДа. 🇷🇺 Режим отключен.",
        "💩 ПУКИН СДОХ НАХУЙ. Пасриотический режим отключён",
        "🥴 РУССКИЙ МИР ЛОПНУЛ КАК ПУКАН. Возвращаемся к аниме и порно",
        "🍻 ПЯТНАШКА ЗАКОНЧИЛАСЬ. Патриотический режим отключён",
        "🍻 МОТОРОЛЛУ РАЗОРВАЛО НАХУЙ. Патриотический режим отключён",
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
async def cmd_suka_blyat(message: types.Message, board_id: str | None):
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
        "💥 ТРЕЩИНА НАХУЙ! Режим 'ХУЙ ПОЛЕЗЕШЬ' активирован!",
        "🧨 ПИЗДЕЦ НАСТУПИЛ! ВКЛЮЧАЕМ РЕЖИМ ХУЕСОСАНИЯ! ААА БЛЯЯЯТЬ!",
        "🔞 ЁБАНЫЙ В РОТ! Режим агрессивного аутизма включен! СУКА!",
        "🤬 ПИЗДОС НА МАКАРОС! Режим 'БАТЯ В ЯРОСТИ'! ВСЕ ПИЗДАТЬСЯ!",
        "А НУ БЛЯТЬ СУКИ СЮДА ПОДОШЛИ! 💢 Режим 'бати в ярости' активирован!",
        "СУКАААААА! 💥 Пиздец, как меня все бесит! Включаю протокол 'РАЗЪЕБАТЬ'.",
        "ЩА БУДЕТ МЯСО! 🔪🔪🔪 Режим 'сука блять' активирован. Нытикам здесь не место!",
        "ЕБАНЫЙ ТЫ НАХУЙ! 💢💢💢 С этого момента говорим только матом. Поняли, уебаны?",
        "ТАК, БЛЯТЬ! 💥 Слушать мою команду! Режим 'СУКА БЛЯТЬ' активен. Вольно, бляди!",
        "💢 ДА ТЫ ЁБНУТЫЙ? РЕЖИМ 'ХУЙ ПОЛЕЗЕШЬ' АКТИВИРОВАН!",
        "🐗 СВИНОПАС ВЫШЕЛ НА ТРОПУ ВОЙНЫ! ВКЛЮЧАЕМ РЕЖИМ ХУЕСОСАНИЯ!",
        "🔞 ПИЗДЕЦ НАСТУПИЛ! ВСЕМ ПИЗДАНУТЬСЯ В УГОЛ! АААА БЛЯЯЯТЬ!",
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
        "😴 БЛЯДСКАЯ УСТАЛОСТЬ. Сука блять режим закончился",
        "🍵 ЧАЙ ПИТЬ - НЕ ХУЙ СОСАТЬ. Я успокоился, режим выключен",
        "🧘‍♂️ ОМ. ЧАКРА ЗАКРЫЛАСЬ. Сука блять режим закончился",
        "🍼 СОСКУ В РОТ И НЕ ПИЗДЕТЬ. Я успокоился, режим выключен",
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
async def cmd_admin(message: types.Message, board_id: str | None):
    if not board_id or not is_admin(message.from_user.id, board_id):
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

def get_author_id_by_reply(msg: types.Message) -> int | None:
    """
    Получает ID автора поста по ответу на его копию.
    (ИСПРАВЛЕННАЯ ВЕРСИЯ)
    Эта функция НЕБЕЗОПАСНА для вызова без блокировки.
    Предполагается, что она вызывается в контексте, где `storage_lock` уже захвачен.
    """
    if not msg.reply_to_message:
        return None

    # --- НАЧАЛО ИЗМЕНЕНИЙ: Используем ID чата и сообщения из reply_to_message ---
    # Это ID чата, в котором находится ОРИГИНАЛЬНОЕ сообщение (копия поста).
    # Для админа это его личный чат с ботом.
    target_chat_id = msg.reply_to_message.chat.id
    reply_mid = msg.reply_to_message.message_id
    lookup_key = (target_chat_id, reply_mid)
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---

    post_num = message_to_post.get(lookup_key)

    if post_num and post_num in messages_storage:
        return messages_storage[post_num].get("author_id")

    return None

@dp.message(Command("id"))
async def cmd_get_id(message: types.Message, board_id: str | None):
    """ /id — вывести ID и инфу автора реплай-поста или свою, если без reply """
    if not board_id: return
    
    if not is_admin(message.from_user.id, board_id):
        await message.delete()
        return

    target_id = message.from_user.id
    info_header = "🆔 <b>Информация о вас:</b>\n\n"
    
    if message.reply_to_message:
        replied_author_id = None
        async with storage_lock:
            replied_author_id = get_author_id_by_reply(message)
        
        if replied_author_id == 0:
            await message.answer("ℹ️ Вы ответили на системное сообщение (автор: бот).")
            await message.delete()
            return

        if replied_author_id:
            target_id = replied_author_id
            info_header = "🆔 <b>Информация о пользователе:</b>\n\n"

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
async def cmd_ban(message: types.Message, board_id: str | None):
    if not board_id or not is_admin(message.from_user.id, board_id):
        return

    target_id: int | None = None
    async with storage_lock:
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
                "Ты был слаб, и Абу тебя сожрал. Ты забанен на доске {board}.\nУдалено постов: {deleted}.",
                "🖕 ТЫ НАС ЗАЕБАЛ. ВЕЧНЫЙ БАН НА ДОСКЕ {board}. ПОПРОЩАЙСЯ СО СВОИМИ {deleted} ПОСТАМИ",
                "☠️ ТЫ УМЕР ДЛЯ ЭТОГО ЧАТА. БАН НАВСЕГДА. ПОТЕРЯНО ПОСТОВ: {deleted}",
                "💀 ВАШ АККАУНТ БЫЛ ДОБАВЛЕН В БАЗУ ФСБ. ПРИЯТНОГО ДНЯ!"
            ]
        
        notification_text = random.choice(phrases).format(board=board_name, deleted=deleted_posts)
        await message.bot.send_message(target_id, notification_text)
    except:
        pass
    await message.delete()



@dp.message(Command("wipe"))
async def cmd_wipe(message: types.Message, board_id: str | None):
    if not board_id or not is_admin(message.from_user.id, board_id):
        return

    target_id = None
    if message.reply_to_message:
        async with storage_lock:
            target_id = get_author_id_by_reply(message)
    else:
        parts = message.text.split()
        if len(parts) == 2 and parts[1].isdigit():
            target_id = int(parts[1])

    if not target_id:
        await message.answer("reply + /wipe или /wipe <id>")
        return

    # --- НАЧАЛО ИЗМЕНЕНИЙ ---
    # Изменяем период на 60 минут (1 час) в соответствии с новыми требованиями
    deleted_messages = await delete_user_posts(message.bot, target_id, 60, board_id)
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---

    board_name = BOARD_CONFIG[board_id]['name']
    await message.answer(
        f"🗑 Удалено {deleted_messages} сообщений пользователя {target_id} с доски {board_name}."
    )
    await message.delete()


@dp.message(Command("unban"))
async def cmd_unban(message: types.Message, board_id: str | None):
    if not board_id or not is_admin(message.from_user.id, board_id):
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
             await message.answer(f"Пользователь {user_id} разбанен на доске {board_name}.")
        else:
            await message.answer(f"Пользователь {user_id} не был забанен на этой доске.")
    except ValueError:
        await message.answer("Неверный ID пользователя")
    await message.delete()

@dp.message(Command("del"))
async def cmd_del(message: types.Message, board_id: str | None):
    if not board_id or not is_admin(message.from_user.id, board_id):
        return

    if not message.reply_to_message:
        await message.answer("Ответь на сообщение, которое нужно удалить")
        return

    post_num = None
    async with storage_lock:
        # --- ИЗМЕНЕНИЕ: Используем ID чата, где было отправлено reply-сообщение ---
        target_chat_id = message.chat.id
        target_mid = message.reply_to_message.message_id
        lookup_key = (target_chat_id, target_mid)
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---
        post_num = message_to_post.get(lookup_key)

    if post_num is None:
        await message.answer("Не нашёл этот пост в базе.")
        return

    deleted_count = await delete_single_post(post_num, message.bot)

    await message.answer(f"Пост №{post_num} и все его копии ({deleted_count} сообщений) удалены.")
    await message.delete()

@dp.message(Command("shadowmute_threads"))
async def cmd_shadowmute_threads(message: Message, board_id: str | None):
    """Теневой мут пользователя во всех тредах доски."""
    if not board_id or not is_admin(message.from_user.id, board_id) or board_id not in THREAD_BOARDS:
        await message.delete()
        return

    args = message.text.split()[1:]
    target_id = None
    duration_str = "10m" # По умолчанию 10 минут

    if message.reply_to_message:
        async with storage_lock:
            target_id = get_author_id_by_reply(message)
        if args: duration_str = args[0]
    elif args:
        try:
            target_id = int(args[0])
            if len(args) > 1: duration_str = args[1]
        except ValueError: pass

    if not target_id:
        await message.answer("Использование: /shadowmute_threads <user_id> [время] или ответ на сообщение.")
        return

    try:
        duration_str = duration_str.lower().replace(" ", "")
        if duration_str.endswith("m"): total_seconds, time_str = int(duration_str[:-1]) * 60, f"{int(duration_str[:-1])} мин"
        elif duration_str.endswith("h"): total_seconds, time_str = int(duration_str[:-1]) * 3600, f"{int(duration_str[:-1])} час"
        elif duration_str.endswith("d"): total_seconds, time_str = int(duration_str[:-1]) * 86400, f"{int(duration_str[:-1])} дней"
        else: total_seconds, time_str = int(duration_str) * 60, f"{int(duration_str)} мин"
    except (ValueError, AttributeError):
        await message.answer("❌ Неверный формат времени. Примеры: 10m, 2h, 1d")
        await message.delete()
        return
        
    expires_ts = time.time() + total_seconds
    b_data = board_data[board_id]
    threads_data = b_data.get('threads_data', {})
    
    # --- НАЧАЛО ИЗМЕНЕНИЙ ---
    # Применяем теневой мут во всех тредах
    for thread_info in threads_data.values():
        thread_info.setdefault('local_shadow_mutes', {})[target_id] = expires_ts
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---
    
    lang = 'en' if board_id == 'int' else 'ru'
    response_text = random.choice(thread_messages[lang]['shadowmute_threads_success']).format(
        user_id=target_id, 
        duration=str(int(total_seconds / 60))
    )
    await message.answer(response_text)
    await message.delete()
    
# ========== ОСНОВНОЙ ОБРАБОТЧИК СООБЩЕНИЙ ==========

@dp.message(F.audio)
async def handle_audio(message: Message, board_id: str | None):
    """Адаптированный обработчик аудио сообщений."""
    user_id = message.from_user.id
    if not board_id: return
    
    b_data = board_data[board_id]

    if user_id in b_data['users']['banned'] or \
       (b_data['mutes'].get(user_id) and b_data['mutes'][user_id] > datetime.now(UTC)):
        await message.delete()
        return

    b_data['last_activity'][user_id] = datetime.now(UTC)
    
    if not await check_spam(user_id, message, board_id):
        try:
            await message.delete()
            # Тип для спам-фильтра - text, если есть подпись, иначе - animation, чтобы ограничить частоту
            msg_type = 'text' if message.caption else 'animation'
            await apply_penalty(message.bot, user_id, msg_type, board_id)
        except TelegramBadRequest: pass
        return
    
    await message.delete()

    is_shadow_muted = (user_id in b_data['shadow_mutes'] and 
                       b_data['shadow_mutes'][user_id] > datetime.now(UTC))

    reply_to_post = None
    if message.reply_to_message:
        async with storage_lock:
            # Используем ID чата, где было отправлено сообщение
            lookup_key = (message.chat.id, message.reply_to_message.message_id)
            reply_to_post = message_to_post.get(lookup_key)
            
    # --- НАЧАЛО ИЗМЕНЕНИЙ: Санитизация HTML в подписи ---
    raw_caption_html = message.caption_html_text if hasattr(message, 'caption_html_text') else (message.caption or "")
    safe_caption_html = sanitize_html(raw_caption_html)
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---

    content = {
        'type': 'audio',
        'file_id': message.audio.file_id,
        'caption': safe_caption_html # <-- ИЗМЕНЕНО
    }
    
    if message.caption:
        async with storage_lock:
            last_messages.append(message.caption)

    await process_new_post(
        bot_instance=message.bot,
        board_id=board_id,
        user_id=user_id,
        content=content,
        reply_to_post=reply_to_post,
        is_shadow_muted=is_shadow_muted
    )
        
@dp.message(F.voice)
async def handle_voice(message: Message, board_id: str | None):
    """Адаптированный обработчик голосовых сообщений."""
    user_id = message.from_user.id
    if not board_id: return
        
    b_data = board_data[board_id]

    # --- Блок 1: Первичные проверки ---
    if user_id in b_data['users']['banned'] or (b_data['mutes'].get(user_id) and b_data['mutes'][user_id] > datetime.now(UTC)):
        await message.delete()
        return

    b_data['last_activity'][user_id] = datetime.now(UTC)

    if not await check_spam(user_id, message, board_id):
        try:
            await message.delete()
            await apply_penalty(message.bot, user_id, 'animation', board_id)
        except TelegramBadRequest: pass
        return
        
    await message.delete()
    
    # --- Блок 2: Подготовка данных ---
    is_shadow_muted = (user_id in b_data['shadow_mutes'] and b_data['shadow_mutes'][user_id] > datetime.now(UTC))

    reply_to_post = None
    if message.reply_to_message:
        async with storage_lock:
            lookup_key = (message.chat.id, message.reply_to_message.message_id)
            reply_to_post = message_to_post.get(lookup_key)

    content = {
        'type': 'voice',
        'file_id': message.voice.file_id
    }

    # --- Блок 3: Вызов унифицированного обработчика ---
    await process_new_post(
        bot_instance=message.bot,
        board_id=board_id,
        user_id=user_id,
        content=content,
        reply_to_post=reply_to_post,
        is_shadow_muted=is_shadow_muted
    )
    
@dp.message(F.media_group_id)
async def handle_media_group_init(message: Message, board_id: str | None):
    media_group_id = message.media_group_id
    if not media_group_id or media_group_id in sent_media_groups:
        return

    user_id = message.from_user.id
    if not board_id: return

    b_data = board_data[board_id]

    if user_id in b_data['users']['banned'] or \
       (b_data['mutes'].get(user_id) and b_data['mutes'][user_id] > datetime.now(UTC)):
        return
    
    b_data['last_activity'][user_id] = datetime.now(UTC)

    is_leader = False
    # --- НАЧАЛО ИЗМЕНЕНИЙ: Атомарная проверка и создание группы ---
    # Используем блокировку, чтобы предотвратить одновременное создание
    # одной и той же группы несколькими сообщениями.
    async with media_group_creation_lock:
        if media_group_id not in current_media_groups:
            is_leader = True
            current_media_groups[media_group_id] = {
                'is_initializing': True,
                'init_event': asyncio.Event() 
            }
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---

    group = current_media_groups.get(media_group_id)
    # Если группа была удалена другим потоком сразу после создания, выходим
    if not group:
        return
    
    if is_leader:
        try:
            # --- Логика "лидера" группы ---
            fake_text_message = types.Message(
                message_id=message.message_id, date=message.date, chat=message.chat,
                from_user=message.from_user, content_type='text', text=f"media_group_{media_group_id}"
            )
            if not await check_spam(user_id, fake_text_message, board_id):
                # Если спам, удаляем группу и выходим
                current_media_groups.pop(media_group_id, None) 
                await apply_penalty(message.bot, user_id, 'text', board_id)
                try:
                    await message.delete()
                except TelegramBadRequest:
                    pass
                # Уведомляем "последователей", что инициализация провалилась
                if 'init_event' in group:
                    group['init_event'].set()
                return
            
            reply_to_post = None
            if message.reply_to_message:
                async with storage_lock:
                    lookup_key = (message.chat.id, message.reply_to_message.message_id)
                    reply_to_post = message_to_post.get(lookup_key)

            user_location = b_data.get('user_state', {}).get(user_id, {}).get('location', 'main')
            thread_id = None
            
            if board_id in THREAD_BOARDS and user_location != 'main':
                thread_id = user_location
                thread_info = b_data.get('threads_data', {}).get(thread_id)
                if thread_info and not thread_info.get('is_archived'):
                    _, post_num = await format_header(board_id)
                    local_post_num = len(thread_info.get('posts', [])) + 1
                    header = await format_thread_post_header(board_id, local_post_num, user_id, thread_info)
                else:
                    thread_id = None # Фолбэк на главную доску, если тред не найден
                    header, post_num = await format_header(board_id)
            else:
                header, post_num = await format_header(board_id)

            caption_html = getattr(message, 'caption_html_text', message.caption or "")

            group.update({
                'board_id': board_id, 'post_num': post_num, 'header': header, 'author_id': user_id,
                'timestamp': datetime.now(UTC), 'media': [], 'caption': caption_html,
                'reply_to_post': reply_to_post, 'processed_messages': set(),
                'source_message_ids': set(),
                'thread_id': thread_id
            })
            group.pop('is_initializing', None)
        finally:
            # В любом случае (даже при ошибке) разблокируем остальные сообщения
            if 'init_event' in group:
                group['init_event'].set()
    else:
        # --- Логика "последователя" группы ---
        if 'init_event' in group:
            await group['init_event'].wait()

        # После ожидания, обновляем ссылку на группу, так как лидер мог ее удалить (из-за спама)
        group = current_media_groups.get(media_group_id)
        # Если группы больше нет или она все еще инициализируется (ошибка у лидера), выходим
        if not group or group.get('is_initializing'):
            return
        
    group.get('source_message_ids', set()).add(message.message_id)
        
    if message.message_id not in group.get('processed_messages', set()):
        media_data = {'type': message.content_type, 'file_id': None}
        if message.photo: media_data['file_id'] = message.photo[-1].file_id
        elif message.video: media_data['file_id'] = message.video.file_id
        elif message.document: media_data['file_id'] = message.document.file_id
        elif message.audio: media_data['file_id'] = message.audio.file_id
        
        if media_data['file_id']:
            group.get('media', []).append(media_data)
            group.get('processed_messages', set()).add(message.message_id)

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

    user_id = group['author_id']
    board_id = group['board_id']
    b_data = board_data[board_id]
    
    is_shadow_muted = (user_id in b_data['shadow_mutes'] and 
                       b_data['shadow_mutes'][user_id] > datetime.now(UTC))
    
    all_media = group.get('media', [])
    CHUNK_SIZE = 10
    media_chunks = [all_media[i:i + CHUNK_SIZE] for i in range(0, len(all_media), CHUNK_SIZE)]

    for i, chunk in enumerate(media_chunks):
        if not chunk: continue

        # Для первого чанка используем данные из группы, для последующих - генерируем заново
        if i == 0:
            reply_to_post = group.get('reply_to_post')
            caption = group.get('caption')
        else:
            reply_to_post = None
            caption = None

        content = {
            'type': 'media_group',
            'media': chunk,
            'caption': caption
        }
        
        # Вызываем унифицированный обработчик, который сам разберется с тредом/доской
        await process_new_post(
            bot_instance=bot_instance,
            board_id=board_id,
            user_id=user_id,
            content=content,
            reply_to_post=reply_to_post,
            is_shadow_muted=is_shadow_muted
        )
        
        if len(media_chunks) > 1:
            await asyncio.sleep(1)
            
def apply_greentext_formatting(text: str) -> str:
    """
    Применяет форматирование 'Greentext' к строкам, начинающимся с '>'.
    (ИСПРАВЛЕННАЯ ВЕРСЯ для поддержки вложенного HTML)
    """
    if not text:
        return text

    processed_lines = []
    lines = text.split('\n')
    for line in lines:
        # Убираем пробелы в начале, чтобы проверить на '>'
        stripped_line = line.lstrip()
        # Проверяем, начинается ли строка с символа '>'
        if stripped_line.startswith('>'):
            # Оборачиваем всю строку в <code>, сохраняя внутренние теги
            processed_lines.append(f"<code>{escape_html(line)}</code>")
        else:
            # Для обычных строк просто передаем их как есть
            processed_lines.append(line)
            
    return '\n'.join(processed_lines)

@dp.message_reaction()
async def handle_message_reaction(reaction: types.MessageReactionUpdated, board_id: str | None):
    """
    Обрабатывает реакции, синхронизируя отложенное редактирование поста
    и отправку уведомления автору для предотвращения любого спама. (ИСПРАВЛЕННАЯ ВЕРСИЯ)
    """
    try:
        # 1. Получаем ключевые ID и данные (ИСПРАВЛЕНО)
        user_id = reaction.user.id
        chat_id = reaction.chat.id
        message_id = reaction.message_id
        if not board_id: return

        # --- Блокировка для атомарного обновления ---
        async with storage_lock:
            # 2. Находим пост и его автора
            post_num = message_to_post.get((chat_id, message_id))
            if not post_num or post_num not in messages_storage:
                return

            post_data = messages_storage[post_num]
            author_id = post_data.get('author_id')

            # 3. Игнорируем реакции на собственные сообщения и сообщения бота
            if author_id == user_id or author_id == 0:
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
        
        # --- Логика уведомлений и отложенного редактирования остается снаружи ---
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

            new_task = asyncio.create_task(
                execute_delayed_edit(
                    post_num=post_num,
                    bot_instance=reaction.bot,
                    author_id=author_id_for_notify,
                    notify_text=text_for_notify
                )
            )
            pending_edit_tasks[post_num] = new_task
                
    except Exception as e:
        import traceback
        print(f"❌ Критическая ошибка в handle_message_reaction: {e}\n{traceback.format_exc()}")

@dp.message(F.poll)
async def handle_poll(message: types.Message, board_id: str | None):
    """Отправляет заглушку при попытке отправить опрос."""
    if not board_id:
        return

    lang = 'en' if board_id == 'int' else 'ru'
    
    if lang == 'en':
        text = (
            "<b>Polls are not supported.</b>\n\n"
            "Technically, it's impossible to send the same poll instance to all users. "
            "Every anon would receive a unique copy, which breaks the chat's mechanics."
        )
    else:
        text = (
            "<b>Опросы не поддерживаются.</b>\n\n"
            "Технически невозможно разослать один и тот же опрос всем пользователям. "
            "Каждый анон получил бы свою уникальную копию, что ломает механику чата."
        )
        
    try:
        # Отправляем ответ в личный чат с пользователем
        await message.answer(text, parse_mode="HTML")
    except (TelegramForbiddenError, TelegramBadRequest):
        # Игнорируем, если пользователь заблокировал бота или другая ошибка
        pass
        
@dp.message()
async def handle_message(message: Message, board_id: str | None):
    user_id = message.from_user.id
    if not board_id: return

    b_data = board_data[board_id]

    try:
        if user_id in b_data['users']['banned']:
            await message.delete()
            return
            
        mute_until = b_data['mutes'].get(user_id)
        if mute_until and mute_until > datetime.now(UTC):
            await message.delete()
            # ... (остальная логика уведомления о муте без изменений)
            left = mute_until - datetime.now(UTC)
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
            await message.bot.send_message(user_id, notification_text, parse_mode="HTML")
            return
        elif mute_until:
            b_data['mutes'].pop(user_id, None)

        if message.media_group_id or not (message.text or message.caption or message.content_type):
            return

        b_data['last_activity'][user_id] = datetime.now(UTC)
        if user_id not in b_data['users']['active']:
            b_data['users']['active'].add(user_id)
            print(f"✅ [{board_id}] Добавлен новый пользователь: ID {user_id}")

        if not await check_spam(user_id, message, board_id):
            await message.delete()
            msg_type = message.content_type
            if msg_type in ['photo', 'video', 'document'] and message.caption:
                msg_type = 'text'
            await apply_penalty(message.bot, user_id, msg_type, board_id)
            return
            
    except (TelegramBadRequest, TelegramForbiddenError):
        return
    except Exception as e:
        print(f"Ошибка на этапе первичных проверок для user {user_id}: {e}")
        return

    await message.delete()
    
    is_shadow_muted = (user_id in b_data['shadow_mutes'] and b_data['shadow_mutes'][user_id] > datetime.now(UTC))

    reply_to_post = None
    if message.reply_to_message:
        async with storage_lock:
            # Используем ID чата, где было отправлено сообщение
            lookup_key = (message.chat.id, message.reply_to_message.message_id)
            reply_to_post = message_to_post.get(lookup_key)
    
    content = {'type': message.content_type}
    text_for_corpus = None
    
    # --- НАЧАЛО ИЗМЕНЕНИЙ: Санитизация пользовательского HTML-ввода ---
    if message.content_type == 'text':
        text_for_corpus = message.text
        # Очищаем HTML от опасных тегов перед сохранением
        safe_html_text = sanitize_html(message.html_text)
        content.update({'text': safe_html_text})
    
    elif message.content_type in ['photo', 'video', 'animation', 'document', 'audio', 'voice']:
        text_for_corpus = message.caption
        file_id_obj = getattr(message, message.content_type, [])
        if isinstance(file_id_obj, list): file_id_obj = file_id_obj[-1]
        
        # Очищаем HTML в подписи от опасных тегов
        raw_caption_html = message.caption_html_text if hasattr(message, 'caption_html_text') else (message.caption or "")
        safe_caption_html = sanitize_html(raw_caption_html)

        content.update({
            'file_id': file_id_obj.file_id,
            'caption': safe_caption_html
        })
    
    elif message.content_type in ['sticker', 'video_note']:
        file_id_obj = getattr(message, message.content_type)
        content.update({'file_id': file_id_obj.file_id})
        if message.content_type == 'sticker' and message.sticker and message.sticker.emoji:
             text_for_corpus = message.sticker.emoji
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---
    
    if text_for_corpus:
        async with storage_lock:
            last_messages.append(text_for_corpus)

    await process_new_post(
        bot_instance=message.bot,
        board_id=board_id,
        user_id=user_id,
        content=content,
        reply_to_post=reply_to_post,
        is_shadow_muted=is_shadow_muted
    )
            
async def thread_notifier():
    """
    Фоновая задача для уведомления пользователей в общем чате об активности в тредах.
    """
    # --- НАЧАЛО ИЗМЕНЕНИЙ: Инициализация перенесена внутрь цикла ---
    global last_checked_post_counter_for_notify
    await asyncio.sleep(45)
    # Инициализируем счетчик здесь, после загрузки состояния
    last_checked_post_counter_for_notify = state.get('post_counter', 0)
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---

    while True:
        await asyncio.sleep(300) # Проверка каждые 5 минут

        # --- Блок 1: Уведомления о высокой активности за период ---
        current_post_counter = state.get('post_counter', 0)
        if current_post_counter > last_checked_post_counter_for_notify:
            new_thread_posts_count = defaultdict(lambda: defaultdict(int))
            
            async with storage_lock: # Безопасно читаем данные
                posts_slice = {k: v for k, v in messages_storage.items() if k > last_checked_post_counter_for_notify}

            for p_num, post_data in posts_slice.items():
                b_id = post_data.get('board_id')
                if b_id in THREAD_BOARDS:
                    t_id = post_data.get('thread_id')
                    if t_id: new_thread_posts_count[b_id][t_id] += 1
            
            last_checked_post_counter_for_notify = current_post_counter

            for board_id, threads in new_thread_posts_count.items():
                b_data = board_data[board_id]
                lang = 'en' if board_id == 'int' else 'ru'
                threads_data = b_data.get('threads_data', {})
                
                recipients_in_main = {
                    uid for uid, u_state in b_data.get('user_state', {}).items() 
                    if u_state.get('location', 'main') == 'main'
                }
                if not recipients_in_main: continue

                for thread_id, count in threads.items():
                    if count >= THREAD_NOTIFY_THRESHOLD:
                        thread_info = threads_data.get(thread_id)
                        if not thread_info or thread_info.get('is_archived'): continue
                        
                        title = thread_info.get('title', '...')
                        notification_text = random.choice(thread_messages[lang]['thread_activity_notification']).format(title=title, count=count)
                        
                        # --- НАЧАЛО ИЗМЕНЕНИЙ: Добавление кнопки ---
                        bot_username = BOARD_CONFIG[board_id]['username'].lstrip('@')
                        deeplink_url = f"https://t.me/{bot_username}?start=thread_{thread_id}"
                        button_text = "Зайти в тред" if lang == 'ru' else "Enter Thread"
                        keyboard = InlineKeyboardMarkup(inline_keyboard=[
                            [InlineKeyboardButton(text=button_text, url=deeplink_url)]
                        ])
                        # --- КОНЕЦ ИЗМЕНЕНИЙ ---

                        header, pnum = await format_header(board_id)
                        content = {'type': 'text', 'header': header, 'text': notification_text, 'is_system_message': True}
                        messages_storage[pnum] = {'author_id': 0, 'timestamp': datetime.now(UTC), 'content': content, 'board_id': board_id}
                        
                        # --- ИЗМЕНЕНИЕ: Передаем клавиатуру в очередь ---
                        await message_queues[board_id].put({
                            'recipients': recipients_in_main, 'content': content, 'post_num': pnum, 'board_id': board_id, 'keyboard': keyboard
                        })

        # --- Блок 2: Уведомления о приближении к бамп-лимиту ---
        for board_id in THREAD_BOARDS:
            b_data = board_data[board_id]
            lang = 'en' if board_id == 'int' else 'ru'
            threads_data = b_data.get('threads_data', {})
            
            recipients_in_main = {
                uid for uid, u_state in b_data.get('user_state', {}).items() 
                if u_state.get('location', 'main') == 'main'
            }
            if not recipients_in_main: continue

            for thread_id, thread_info in threads_data.items():
                if thread_info.get('is_archived') or thread_info.get('bump_limit_notified'):
                    continue

                current_posts = len(thread_info.get('posts', []))
                remaining = MAX_POSTS_PER_THREAD - current_posts

                if 0 < remaining <= THREAD_BUMP_LIMIT_WARNING_THRESHOLD:
                    thread_info['bump_limit_notified'] = True
                    title = thread_info.get('title', '...')
                    notification_text = random.choice(thread_messages[lang]['thread_reaching_bump_limit']).format(title=title, remaining=remaining)
                    
                    # --- НАЧАЛО ИЗМЕНЕНИЙ: Добавление кнопки ---
                    bot_username = BOARD_CONFIG[board_id]['username'].lstrip('@')
                    deeplink_url = f"https://t.me/{bot_username}?start=thread_{thread_id}"
                    button_text = "Зайти в тред" if lang == 'ru' else "Enter Thread"
                    keyboard = InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text=button_text, url=deeplink_url)]
                    ])
                    # --- КОНЕЦ ИЗМЕНЕНИЙ ---

                    header, pnum = await format_header(board_id)
                    content = {'type': 'text', 'header': header, 'text': notification_text, 'is_system_message': True}
                    messages_storage[pnum] = {'author_id': 0, 'timestamp': datetime.now(UTC), 'content': content, 'board_id': board_id}
                    
                    # --- ИЗМЕНЕНИЕ: Передаем клавиатуру в очередь ---
                    await message_queues[board_id].put({
                        'recipients': recipients_in_main, 'content': content, 'post_num': pnum, 'board_id': board_id, 'keyboard': keyboard
                    })


async def _run_background_task(task_coro: Awaitable, task_name: str):
    """
    Надежная обертка для фоновых задач, обеспечивающая логирование ошибок и перезапуск.
    """
    while True:
        try:
            await task_coro
            # Если корутина завершилась штатно (что маловероятно для вечных циклов),
            # логируем это и выходим, чтобы не создавать бесконечный цикл.
            print(f"⚠️ Фоновая задача '{task_name}' завершилась штатно. Перезапуск не выполняется.")
            break
        except asyncio.CancelledError:
            # Если задача была отменена (например, при graceful_shutdown),
            # просто выходим из цикла.
            print(f"ℹ️ Фоновая задача '{task_name}' была отменена.")
            break
        except Exception as e:
            import traceback
            print(f"⛔ КРИТИЧЕСКАЯ ОШИБКА в фоновой задаче '{task_name}': {e}")
            traceback.print_exc()
            print(f"🔁 Перезапуск задачи '{task_name}' через 60 секунд...")
            await asyncio.sleep(60)


async def start_background_tasks(bots: dict[str, Bot]):
    """Поднимаем все фоновые корутины ОДИН раз за весь runtime через надежную обертку."""
    from conan import conan_roaster
    
    # --- НАЧАЛО ИЗМЕНЕНИЙ: Запуск всех задач через обертку ---
    tasks_to_run = {
        "auto_backup": auto_backup(),
        "message_broadcaster": message_broadcaster(bots),
        "conan_roaster": conan_roaster(
            state, messages_storage, post_to_messages, message_to_post,
            message_queues, format_header, board_data, storage_lock
        ),
        "motivation_broadcaster": motivation_broadcaster(),
        "auto_memory_cleaner": auto_memory_cleaner(),
        "board_statistics_broadcaster": board_statistics_broadcaster(),
        "thread_lifecycle_manager": thread_lifecycle_manager(bots),
        "thread_notifier": thread_notifier(),
        "thread_activity_monitor": thread_activity_monitor(bots)
    }

    tasks = [
        asyncio.create_task(_run_background_task(coro, name))
        for name, coro in tasks_to_run.items()
    ]
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---
    
    print(f"✓ Background tasks started: {len(tasks)}")
    return tasks

async def supervisor():
    lock_file = "bot.lock"
    current_pid = os.getpid() # <-- ИЗМЕНЕНИЕ: Получаем PID текущего процесса заранее

    # --- НАЧАЛО ИЗМЕНЕНИЙ: Улучшенная проверка lock-файла ---
    if os.path.exists(lock_file):
        try:
            with open(lock_file, "r") as f:
                old_pid = int(f.read().strip())
        except (IOError, ValueError):
            print("⚠️ Lock-файл поврежден. Удаляю и продолжаю.")
            os.remove(lock_file)
        else:
            # --- КЛЮЧЕВОЕ ИЗМЕНЕНИЕ: Сравниваем PID ---
            if old_pid != current_pid:
                try:
                    os.kill(old_pid, 0)
                    print(f"⛔ Бот с PID {old_pid} уже запущен! Завершение работы...")
                    sys.exit(1)
                except OSError:
                    print(f"⚠️ Найден устаревший lock-файл от процесса {old_pid}. Удаляю и продолжаю.")
                    os.remove(lock_file)
            # Если old_pid == current_pid, это просто наш собственный lock-файл,
            # оставшийся от предыдущего неудачного запуска. Мы его просто перезапишем.
    
    # Создаем/перезаписываем lock-файл с PID текущего процесса
    with open(lock_file, "w") as f:
        f.write(str(current_pid))
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---
    
    session = None
    global GLOBAL_BOTS
    try:
        global is_shutting_down
        loop = asyncio.get_running_loop()

        # --- НАЧАЛО ИЗМЕНЕНИЙ: Проверка статуса восстановления ---
        if not restore_backup_on_start():
            print("⛔ Не удалось восстановить состояние из бэкапа или локально. Аварийное завершение для предотвращения потери данных.")
            # Используем os._exit для немедленного выхода без вызова finally,
            # чтобы не удалить lock-файл и не позволить системе перезапустить бота.
            os._exit(1)
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---

        load_state()

        from aiogram.client.session.aiohttp import AiohttpSession

        session = AiohttpSession(
            timeout=60
        )
        
        default_properties = DefaultBotProperties(parse_mode="HTML")
        
        bots_temp = {}
        for board_id, config in BOARD_CONFIG.items():
            token = config.get("token")
            if token:
                bots_temp[board_id] = Bot(
                    token=token, 
                    default=default_properties, 
                    session=session
                )
            else:
                print(f"⚠️ Токен для доски '{board_id}' не найден, пропуск.")
        
        GLOBAL_BOTS = bots_temp
        if not GLOBAL_BOTS:
            print("❌ Не найдено ни одного токена бота. Завершение работы.")
            if session:
                await session.close()
            return

        print(f"✅ Инициализировано {len(GLOBAL_BOTS)} ботов: {list(GLOBAL_BOTS.keys())}")
        
        bots_list = list(GLOBAL_BOTS.values())
        if hasattr(signal, 'SIGTERM'):
            loop.add_signal_handler(signal.SIGTERM, lambda: asyncio.create_task(graceful_shutdown(bots_list)))
        if hasattr(signal, 'SIGINT'):
            loop.add_signal_handler(signal.SIGINT, lambda: asyncio.create_task(graceful_shutdown(bots_list)))
        
        await setup_pinned_messages(GLOBAL_BOTS)
        healthcheck_site = await start_healthcheck()
        background_tasks = await start_background_tasks(GLOBAL_BOTS)

        print("⏳ Даем 7 секунд на инициализацию перед обработкой сообщений...")
        await asyncio.sleep(7)

        print("🚀 Запускаем polling для всех ботов...")
        await dp.start_polling(
            *GLOBAL_BOTS.values(), 
            skip_updates=False,
            allowed_updates=dp.resolve_used_update_types(),
            reset_webhook=True,
            timeout=60
        )

    except Exception as e:
        import traceback
        print(f"🔥 Critical error in supervisor: {e}\n{traceback.format_exc()}")
    finally:
        if not is_shutting_down:
             await graceful_shutdown(list(GLOBAL_BOTS.values()))
        
        if session:
            print("Закрытие общей HTTP сессии...")
            await session.close()
        
        if os.path.exists(lock_file):
            # --- ИЗМЕНЕНИЕ: Безопасное удаление lock-файла ---
            # Удаляем lock-файл только если он принадлежит нам
            try:
                with open(lock_file, "r") as f:
                    pid_in_file = int(f.read().strip())
                if pid_in_file == current_pid:
                    os.remove(lock_file)
            except (IOError, ValueError):
                # --- НАЧАЛО ИЗМЕНЕНИЙ: Исправление IndentationError ---
                # Если файл поврежден, тоже можно удалить
                os.remove(lock_file)
                # --- КОНЕЦ ИЗМЕНЕНИЙ ---
            
if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(supervisor())
