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
from asyncio import Semaphore
from collections import deque, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone, UTC
from typing import Tuple
from dotenv import load_dotenv


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
from help_text import HELP_TEXT_COMMANDS, HELP_TEXT_EN_COMMANDS, generate_boards_list, THREAD_PROMO_TEXT_RU, THREAD_PROMO_TEXT_EN
from japanese_translator import anime_transform, get_random_anime_image, get_monogatari_image, get_nsfw_anime_image
from summarize import summarize_text_with_hf
from thread_texts import thread_messages
from ukrainian_mode import UKRAINIAN_PHRASES, ukrainian_transform
from zaputin_mode import PATRIOTIC_PHRASES, zaputin_transform
from aiogram import BaseMiddleware
from aiogram.types import TelegramObject
from typing import Callable, Dict, Any, Awaitable, Optional

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
        "admins": {int(y) for x in os.getenv("ADMINS", "").split(",") if (y := x.strip()).isdigit()}
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---
    },
    'po': {
        "name": "/po/",
        "description": "ПОЛИТАЧ - (срачи, политика)",
        "description_en": "POLITICS  -",
        "username": "@dvach_po_chatbot",
        "token": os.getenv("PO_BOT_TOKEN"),
        # --- НАЧАЛО ИЗМЕНЕНИЙ ---
        "admins": {int(y) for x in os.getenv("PO_ADMINS", "").split(",") if (y := x.strip()).isdigit()}
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---
    },
    'a': {
        "name": "/a/",
        "description": "АНИМЕ - (манга, Япония, хентай)",
        "description_en": "ANIME (🇯🇵, hentai, manga)",
        "username": "@dvach_a_chatbot",
        "token": os.getenv("A_BOT_TOKEN"),
        # --- НАЧАЛО ИЗМЕНЕНИЙ ---
        "admins": {int(y) for x in os.getenv("A_ADMINS", "").split(",") if (y := x.strip()).isdigit()}
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---
    },
    'sex': {
        "name": "/sex/",
        "description": "СЕКСАЧ - (отношения, секс, тян, еот, блекпилл)",
        "description_en": "SEX (relationships, sex, blackpill)",
        "username": "@dvach_sex_chatbot",
        "token": os.getenv("SEX_BOT_TOKEN"),
        # --- НАЧАЛО ИЗМЕНЕНИЙ ---
        "admins": {int(y) for x in os.getenv("SEX_ADMINS", "").split(",") if (y := x.strip()).isdigit()}
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---
    },
    'vg': {
        "name": "/vg/",
        "description": "ВИДЕОИГРЫ - (ПК, игры, хобби)",
        "description_en": "VIDEO GAMES (🎮, hobbies)",
        "username": "@dvach_vg_chatbot",
        "token": os.getenv("VG_BOT_TOKEN"),
        # --- НАЧАЛО ИЗМЕНЕНИЙ ---
        "admins": {int(y) for x in os.getenv("VG_ADMINS", "").split(",") if (y := x.strip()).isdigit()}
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---
    },
    'int': {
        "name": "/int/",
        "description": "INTERNATIONAL (🇬🇧🇺🇸🇨🇳🇮🇳🇪🇺)",
        "description_en": "INTERNATIONAL (🇬🇧🇺🇸🇨🇳🇮🇳🇪🇺)",
        "username": "@tgchan_chatbot",
        "token": os.getenv("INT_BOT_TOKEN"),
        # --- НАЧАЛО ИЗМЕНЕНИЙ ---
        "admins": {int(y) for x in os.getenv("INT_ADMINS", "").split(",") if (y := x.strip()).isdigit()}
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---
    },
    'thread': {
        "name": "/thread/",
        "description": "ТРЕДЫ - доска для создания тредов",
        "description_en": "THREADS - board for creating threads",
        "username": "@thread_chatbot", 
        "token": os.getenv("THREAD_BOT_TOKEN"),
        # --- НАЧАЛО ИЗМЕНЕНИЙ ---
        "admins": {int(y) for x in os.getenv("THREAD_ADMINS", "").split(",") if (y := x.strip()).isdigit()}
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---
    },
    'test': {
        "name": "/test/",
        "description": "Testground",
        "description_en": "Testground",
        "username": "@tgchan_testbot", 
        "token": os.getenv("TEST_BOT_TOKEN"),
        # --- НАЧАЛО ИЗМЕНЕНИЙ ---
        "admins": {int(y) for x in os.getenv("TEST_ADMINS", "").split(",") if (y := x.strip()).isdigit()}
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---
    }
}


# ========== НОВЫЕ КОНСТАНТЫ ДЛЯ СИСТЕМЫ ТРЕДОВ ==========
THREAD_BOARDS = {'thread', 'test'} # Доски, на которых будет работать система тредов
DATA_DIR = "data"  # Папка для хранения данных (например, архивов тредов)
os.makedirs(DATA_DIR, exist_ok=True) # Гарантируем, что папка существует
# --- НОВАЯ КОНСТАНТА: ID КАНАЛА ДЛЯ РЕАЛ-ТАЙМ АРХИВА ---
REALTIME_ARCHIVE_CHANNEL_ID = -1003026863876

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
    'last_deanon_time': 0, # Время последнего успешного вызова /deanon
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

# Глобальные переменные для cooldown /fap, /gatari, /hent
ANIME_CMD_COOLDOWN = 10 # 10 секунд
ANIME_CMD_COOLDOWN_PHRASES = [
    "НЯ! Подожди немного, сэмпай!",
    "Слишком быстро! Перезаряжаю вайфу-пушку...",
    "Кулдаун, бака! Не так часто!",
    "Подожди, я ищу самую лучшую картинку для тебя!",
    "Терпение, анон-тян! ( ´ ▽ ` )ﾉ"
]
anime_cmd_lock = asyncio.Lock()

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
    Восстанавливает как новые (.gz), так и старые (.json) форматы кэша,
    а также корректно восстанавливает директорию `data`.
    """
    repo_url = "https://github.com/shlomapetia/dvachbot-backup.git"
    backup_dir = "/app/backup"
    max_attempts = 3
    
    if glob.glob("*_state.json"):
        print("✅ Локальные файлы состояния найдены, восстановление из git не требуется.")
        return True
    
    for attempt in range(max_attempts):
        try:
            if os.path.exists(backup_dir):
                shutil.rmtree(backup_dir)
            
            print(f"Попытка клонирования бэкапа #{attempt+1}...")
            subprocess.run(
                ["git", "clone", "--depth", "1", repo_url, backup_dir],
                check=True, timeout=180
            )
            
            # --- НАЧАЛО ИЗМЕНЕНИЙ: УДАЛЕН ПРЕЖДЕВРЕМЕННЫЙ ВЫХОД ---
            
            # 1. Восстанавливаем файлы из корня бэкапа (state, reply_cache)
            root_backup_files = glob.glob(os.path.join(backup_dir, "*_state.json"))
            root_backup_files += glob.glob(os.path.join(backup_dir, "*_reply_cache.json.gz"))
            root_backup_files += glob.glob(os.path.join(backup_dir, "*_reply_cache.json"))

            final_root_files = {}
            for f in root_backup_files:
                base_name = os.path.basename(f).replace('.json','').replace('.gz','')
                if base_name not in final_root_files or f.endswith('.gz'):
                    final_root_files[base_name] = f
            
            if not final_root_files:
                print("⚠️ Корневые файлы состояния в репозитории не найдены.")
            else:
                for src_path in final_root_files.values():
                    shutil.copy2(src_path, os.getcwd())
                print(f"✅ Восстановлено {len(final_root_files)} файлов из корня бэкапа.")

            # 2. Восстанавливаем директорию 'data' целиком, если она есть
            backup_data_dir = os.path.join(backup_dir, DATA_DIR)
            if os.path.isdir(backup_data_dir):
                shutil.copytree(backup_data_dir, DATA_DIR, dirs_exist_ok=True)
                print(f"✅ Директория '{DATA_DIR}' успешно восстановлена из бэкапа.")
            else:
                print(f"⚠️ Директория '{DATA_DIR}' в бэкапе не найдена.")

            # --- КОНЕЦ ИЗМЕНЕНИЙ ---
            
            print(f"✅ Восстановление из backup завершено.")
            return True
        
        except Exception as e:
            print(f"❌ Ошибка при восстановлении (попытка {attempt+1}): {e}")
            time.sleep(5)
    
    print("⛔ КРИТИЧЕСКАЯ ОШИБКА: Все попытки восстановления из git провалились.")
    
    if glob.glob("*_state.json"):
        print("⚠️ Не удалось обновить бэкап из Git, но найдены локальные файлы состояния. Запускаемся с ними.")
        return True
    
    print("⛔ КРИТИЧЕСКАЯ ОШИБКА: Локальные файлы состояния также отсутствуют. Невозможно запустить бота.")
    return False
    
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
    """
    Синхронные Git-операции для бэкапа. Оптимизировано для сред с медленным диском.
    Работает с новым форматом .json.gz и удаляет старые .json файлы.
    САМОСТОЯТЕЛЬНО ОБНОВЛЯЕТ СТРУКТУРУ РЕПОЗИТОРИЯ.
    """
    GIT_TIMEOUT = 90  # Увеличим таймаут для надежности
    GIT_LOCAL_TIMEOUT = 60
    
    try:
        work_dir = "/tmp/git_backup"
        repo_url = f"https://{token}@github.com/shlomapetia/dvachbot-backup.git"

        # --- НАЧАЛО ИЗМЕНЕНИЙ: УМНАЯ СИНХРОНИЗАЦИЯ ВМЕСТО ПРОСТОГО КЛОНИРОВАНИЯ ---

        # 1. Подготовка рабочей директории
        if os.path.exists(work_dir):
            shutil.rmtree(work_dir)
        os.makedirs(work_dir)

        # 2. Клонируем репозиторий с последними изменениями
        print("Git: Клонирую последнюю версию бэкапа...")
        subprocess.run(
            ["git", "clone", "--depth", "1", repo_url, work_dir],
            check=True, timeout=GIT_TIMEOUT
        )
        print("Git: Клонирование завершено.")
        
        # 3. Копируем АКТУАЛЬНЫЕ локальные файлы ПОВЕРХ скачанных
        # Это ключевой шаг: мы обновляем бэкап свежими данными с сервера
        print("Git: Обновляю локальную копию бэкапа актуальными файлами...")
        
        # Копируем файлы из корня проекта (*_state.json, *_reply_cache.json.gz)
        root_files_to_copy = glob.glob(os.path.join(os.getcwd(), "*_state.json"))
        root_files_to_copy += glob.glob(os.path.join(os.getcwd(), "*_reply_cache.json.gz"))
        
        for src_path in root_files_to_copy:
            shutil.copy2(src_path, work_dir)

        # Рекурсивно копируем всю директорию DATA_DIR, если она существует
        local_data_dir = os.path.join(os.getcwd(), DATA_DIR)
        backup_data_dir = os.path.join(work_dir, DATA_DIR)
        if os.path.exists(local_data_dir):
            shutil.copytree(local_data_dir, backup_data_dir, dirs_exist_ok=True)
            
        # 4. Удаляем устаревшие .json кэши из рабочей копии git (если они там еще есть)
        old_json_caches_in_repo = glob.glob(os.path.join(work_dir, "*_reply_cache.json"))
        if old_json_caches_in_repo:
            print(f"Git: Найдены устаревшие .json кэши для удаления: {[os.path.basename(f) for f in old_json_caches_in_repo]}")
            for old_file in old_json_caches_in_repo:
                # Используем git rm для отслеживания удаления
                subprocess.run(["git", "-C", work_dir, "rm", os.path.basename(old_file)], check=False, timeout=GIT_LOCAL_TIMEOUT)

        # --- КОНЕЦ ИЗМЕНЕНИЙ ---

        # Локальные операции (без изменений)
        subprocess.run(["git", "-C", work_dir, "config", "user.name", "Backup Bot"], check=True, timeout=GIT_LOCAL_TIMEOUT)
        subprocess.run(["git", "-C", work_dir, "config", "user.email", "backup@dvachbot.com"], check=True, timeout=GIT_LOCAL_TIMEOUT)
        subprocess.run(["git", "-C", work_dir, "add", "."], check=True, timeout=GIT_LOCAL_TIMEOUT)
        
        status_result = subprocess.run(["git", "-C", work_dir, "status", "--porcelain"], capture_output=True, text=True, timeout=GIT_LOCAL_TIMEOUT)
        if not status_result.stdout:
            print("✅ Git: Нет изменений для коммита.")
            return True

        commit_msg = f"Backup: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}"
        subprocess.run(["git", "-C", work_dir, "commit", "-m", commit_msg], check=True, timeout=GIT_TIMEOUT)

        # Отправка (без изменений)
        push_cmd = ["git", "-C", work_dir, "push", "origin", "main"] # Убрал --force, т.к. мы теперь работаем с актуальной версией
        print(f"Git: Выполняю: {' '.join(push_cmd)}")
        result = subprocess.run(push_cmd, capture_output=True, text=True, timeout=GIT_TIMEOUT)
        
        if result.returncode == 0:
            print(f"✅ Бекап успешно отправлен в GitHub.")
            return True
        else:
            print(f"❌ ОШИБКА PUSH (код {result.returncode}):\n{result.stderr}")
            # Если обычный push не прошел, пробуем --force как крайнюю меру
            print("Git: Обычный push не удался, пробую принудительный push...")
            force_push_cmd = ["git", "-C", work_dir, "push", "--force", "origin", "main"]
            force_result = subprocess.run(force_push_cmd, capture_output=True, text=True, timeout=GIT_TIMEOUT)
            if force_result.returncode == 0:
                print("✅ Принудительный push успешен.")
                return True
            else:
                print(f"❌ КРИТИЧЕСКАЯ ОШИБКА ДАЖЕ С --force (код {force_result.returncode}):\n{force_result.stderr}")
                return False

    except subprocess.TimeoutExpired as e:
        print(f"⛔ КРИТИЧЕСКАЯ ОШИБКА: Таймаут операции git! Команда '{' '.join(e.cmd)}' не завершилась за {e.timeout} секунд.")
        print(f"--- stderr ---\n{e.stderr or '(пусто)'}\n--- stdout ---\n{e.stdout or '(пусто)'}")
        return False
    except Exception as e:
        print(f"⛔ КРИТИЧЕСКАЯ ОШИБКА в sync_git_operations: {e}")
        return False
        
dp = Dispatcher()
dp.message.middleware(BoardMiddleware())
dp.callback_query.middleware(BoardMiddleware())
dp.message_reaction.middleware(BoardMiddleware()) # <-- ДОБАВЛЕНО
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
            await asyncio.sleep(SAVE_INTERVAL)  # Используем константу

            if is_shutting_down:
                break
            
            # Новая функция сохраняет всё и делает бэкап
            await save_all_boards_and_backup()

        except Exception as e:
            print(f"❌ Ошибка в auto_backup: {e}")
            await asyncio.sleep(60)
            
# Настройка сборщика мусора
gc.set_threshold(
    500, 5, 5)  # Оптимальные настройки для баланса памяти/производительности


def get_user_msgs_deque(user_id: int, board_id: str):
    """Получаем deque для юзера на конкретной доске. Очистка теперь централизована в auto_memory_cleaner."""
    last_user_msgs_for_board = board_data[board_id]['last_user_msgs']
    
    # --- НАЧАЛО ИЗМЕНЕНИЙ ---
    # Используем setdefault для атомарного создания deque.
    # Этот метод либо возвращает существующий deque, либо создает и возвращает новый
    # за одну операцию, что предотвращает состояние гонки (race condition).
    return last_user_msgs_for_board.setdefault(user_id, deque(maxlen=10))
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---


# Конфиг
BOT_TOKEN = os.environ.get('BOT_TOKEN')
ADMINS = {int(x) for x in os.getenv("ADMINS", "").split(",") if x}
SPAM_LIMIT = 14
SPAM_WINDOW = 15
STATE_FILE = 'state.json'
SAVE_INTERVAL = 1800  # секунд
STICKER_WINDOW = 10  # секунд
STICKER_LIMIT = 7
REST_SECONDS = 30  # время блокировки
REPLY_FILE = "reply_cache.json"  # отдельный файл для reply
MAX_MESSAGES_IN_MEMORY = 600  # храним только последние 600 постов в общей памяти


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
                # --- НАЧАЛО ИЗМЕНЕНИЙ: Полная очистка данных пользователя ---
                async with storage_lock:
                    b_data = board_data[board_id]
                    b_data['users']['active'].discard(user_id)
                    # Удаляем все связанные данные, чтобы предотвратить "мусор" в памяти
                    b_data['last_activity'].pop(user_id, None)
                    b_data['last_texts'].pop(user_id, None)
                    b_data['last_stickers'].pop(user_id, None)
                    b_data['last_animations'].pop(user_id, None)
                    b_data['spam_violations'].pop(user_id, None)
                    b_data['spam_tracker'].pop(user_id, None)
                    b_data['last_user_msgs'].pop(user_id, None)
                    b_data['message_counter'].pop(user_id, None)
                    b_data['user_state'].pop(user_id, None)
                print(f"🚫 [{board_id}] Пользователь {user_id} заблокировал бота. Все его данные удалены.")
                # --- КОНЕЦ ИЗМЕНЕНИЙ ---
        return True

    # Обработка сетевых ошибок и конфликтов
    if isinstance(exception, (TelegramNetworkError, TelegramConflictError, aiohttp.ClientError)):
        print(f"🌐 Сетевая ошибка: {type(exception).__name__}: {exception}")
        await asyncio.sleep(10)
        # --- НАЧАЛО ИЗМЕНЕНИЙ ---
        # Возвращаем True, чтобы aiogram "проглотил" ошибку и продолжил обрабатывать
        # следующие обновления. Это предотвращает остановку бота из-за временных сетевых проблем.
        return True
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---

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

def _sync_get_board_activity(
    board_id: str,
    relevant_board_ids: list[str]
) -> int:
    """
    Синхронная, блокирующая и оптимизированная функция для подсчета постов.
    Работает с маленьким, предварительно отфильтрованным списком.
    """
    # Просто считаем количество вхождений нужного board_id в переданном списке.
    # Это очень быстрая операция.
    return relevant_board_ids.count(board_id)

async def get_board_activity_last_hours(board_id: str, hours: int = 2) -> float:
    """
    Подсчитывает среднее количество постов в час для указанной доски за последние N часов.
    Оптимизировано для минимальной нагрузки и передачи данных в executor.
    """
    if hours <= 0:
        return 0.0

    time_threshold = datetime.now(UTC) - timedelta(hours=hours)
    
    # 1. Быстро и безопасно фильтруем данные под блокировкой
    relevant_board_ids = []
    async with storage_lock:
        # Итерируемся от новых постов к старым
        for post_data in reversed(messages_storage.values()):
            post_time = post_data.get("timestamp")
            # Прерываемся, как только дошли до слишком старых постов
            if not post_time or post_time < time_threshold:
                break
            
            # Собираем только ID досок - это очень легковесные данные
            b_id = post_data.get("board_id")
            if b_id:
                relevant_board_ids.append(b_id)

    # 2. Выполняем очень быструю операцию подсчета в отдельном потоке
    loop = asyncio.get_running_loop()
    post_count = await loop.run_in_executor(
        save_executor,
        _sync_get_board_activity,
        board_id,
        relevant_board_ids  # Передаем маленький и легкий список
    )
            
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
            'shadow_mutes': shadow_mutes_to_save, # <-- ИЗМЕНЕНО
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
    
def _sync_save_threads_data(board_id: str, data_to_save: dict):
    """Синхронная, потокобезопасная функция для сохранения данных о тредах. Работает только с переданными данными."""
    threads_file = os.path.join(DATA_DIR, f"{board_id}_threads.json")
    try:
        with open(threads_file, 'w', encoding='utf-8') as f:
            json.dump(data_to_save, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        print(f"⛔ [{board_id}] Ошибка в потоке сохранения _threads.json: {e}")
        return False

def _sync_save_user_states(board_id: str, data_to_save: dict):
    """Синхронная, потокобезопасная функция для сохранения состояний пользователей. Работает только с переданными данными."""
    user_states_file = os.path.join(DATA_DIR, f"{board_id}_user_states.json")
    try:
        with open(user_states_file, 'w', encoding='utf-8') as f:
            json.dump(data_to_save, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        print(f"⛔ [{board_id}] Ошибка в потоке сохранения _user_states.json: {e}")
        return False

async def save_user_states(board_id: str):
    """Асинхронная обертка для сохранения состояний пользователей с защитой от гонки состояний."""
    if board_id not in THREAD_BOARDS:
        return

    # 1. Захватываем блокировку и создаем копию данных
    async with storage_lock:
        # .copy() достаточно, так как user_state содержит только JSON-совместимые типы
        data_to_save = board_data[board_id].get('user_state', {}).copy()

    # 2. Передаем копию в другой поток для записи
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(
        save_executor,
        _sync_save_user_states,
        board_id,
        data_to_save
    )

async def save_all_boards_and_backup():
    """Сохраняет данные ВСЕХ досок параллельно и делает один общий бэкап в Git."""
    print("💾 Запуск параллельного сохранения и бэкапа...")

    # --- НАЧАЛО ИЗМЕНЕНИЙ: Централизованный сбор данных для reply_cache ---
    
    # 1. Один раз собираем все данные, группируя их по board_id
    grouped_data_for_cache = defaultdict(lambda: {
        'post_keys': [],
        'post_to_messages': {},
        'message_to_post': {},
        'storage_meta': {}
    })

    async with storage_lock:
        # Сначала группируем все посты по доскам
        posts_by_board = defaultdict(list)
        for p_num, data in messages_storage.items():
            b_id = data.get("board_id")
            if b_id:
                posts_by_board[b_id].append(p_num)

        # Для каждой доски берем срез последних постов
        for b_id, p_nums in posts_by_board.items():
            recent_posts = sorted(p_nums)[-MAX_MESSAGES_IN_MEMORY:]
            grouped_data_for_cache[b_id]['post_keys'] = set(recent_posts) # Используем set для быстрой проверки
        
        # Один раз итерируемся по большим словарям, раскидывая данные по доскам
        all_recent_posts_flat = {p_num for data in grouped_data_for_cache.values() for p_num in data['post_keys']}
        
        for p_num, data in post_to_messages.items():
            if p_num in all_recent_posts_flat:
                b_id = messages_storage.get(p_num, {}).get("board_id")
                if b_id:
                    grouped_data_for_cache[b_id]['post_to_messages'][p_num] = data # Копирование не нужно, т.к. данные не меняются
        
        for key, p_num in message_to_post.items():
            if p_num in all_recent_posts_flat:
                b_id = messages_storage.get(p_num, {}).get("board_id")
                if b_id:
                    grouped_data_for_cache[b_id]['message_to_post'][key] = p_num

        for b_id, data in grouped_data_for_cache.items():
            for p_num in data['post_keys']:
                post_meta = messages_storage.get(p_num)
                if post_meta:
                    data['storage_meta'][p_num] = {
                        "author_id": post_meta.get("author_id", ""),
                        "timestamp": post_meta.get("timestamp", datetime.now(UTC)).isoformat(),
                        "author_message_id": post_meta.get("author_message_id"),
                        "board_id": post_meta.get("board_id")
                    }
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---

    # 2. Создаем задачи для параллельного сохранения всех файлов
    save_tasks = []
    loop = asyncio.get_running_loop()

    for board_id in BOARDS:
        save_tasks.append(save_board_state(board_id))
        
        # Используем предварительно собранные данные для сохранения кэша
        board_cache_data = grouped_data_for_cache[board_id]
        save_tasks.append(loop.run_in_executor(
            save_executor,
            _sync_save_reply_cache,
            board_id,
            list(board_cache_data['post_keys']), # Конвертируем set обратно в list для _sync_
            board_cache_data['post_to_messages'],
            board_cache_data['message_to_post'],
            board_cache_data['storage_meta']
        ))

        if board_id in THREAD_BOARDS:
            save_tasks.append(save_threads_data(board_id))
            save_tasks.append(save_user_states(board_id))

    # 3. Запускаем все задачи сохранения одновременно и ждем их завершения
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
    """Синхронная, блокирующая функция для сохранения кэша. Работает с переданными ей данными, записывая сжатый gzip-файл."""
    reply_file = f"{board_id}_reply_cache.json.gz"
    try:
        recent_posts_set = set(recent_board_posts)

        if not recent_posts_set:
            if os.path.exists(reply_file):
                os.remove(reply_file)
            return True

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
            "messages_storage_meta": {
                str(p_num): all_messages_storage_meta[p_num]
                for p_num in recent_board_posts
                if p_num in all_messages_storage_meta
            }
        }

        # Используем gzip.open в текстовом режиме 'wt' для сохранения сжатого JSON
        with gzip.open(reply_file, 'wt', encoding='utf-8') as f:
            json.dump(new_data, f, ensure_ascii=False, indent=2)

        return True

    except Exception as e:
        print(f"⛔ [{board_id}] Ошибка в потоке сохранения reply_cache: {str(e)[:200]}")
        return False

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
    """
    Читает reply_cache для конкретной доски, обеспечивая бесшовный переход
    со старого формата (.json) на новый сжатый (.json.gz) и защитой от битых файлов.
    """
    global message_to_post, post_to_messages, messages_storage
    
    new_reply_file = f"{board_id}_reply_cache.json.gz"
    old_reply_file = f"{board_id}_reply_cache.json"
    
    reply_file = None
    is_gzipped = False

    if os.path.exists(new_reply_file) and os.path.getsize(new_reply_file) > 0:
        reply_file = new_reply_file
        is_gzipped = True
    elif os.path.exists(old_reply_file) and os.path.getsize(old_reply_file) > 0:
        reply_file = old_reply_file
    else:
        return

    try:
        if is_gzipped:
            with gzip.open(reply_file, "rt", encoding="utf-8") as f:
                data = json.load(f)
        else:
            with open(reply_file, "r", encoding="utf-8") as f:
                data = json.load(f)
    except (json.JSONDecodeError, OSError, gzip.BadGzipFile, EOFError) as e:
        print(f"⛔ КРИТИЧЕСКАЯ ОШИБКА (НО ПЕРЕХВАЧЕНА): Файл кэша {reply_file} повреждён ({e}).")
        print("     Файл будет проигнорирован. При следующем сохранении он будет перезаписан.")
        try:
            os.remove(reply_file)
        except OSError:
            pass
        return
    
    valid_post_nums = set()
    for p_str, meta in data.get("messages_storage_meta", {}).items():
        if "board_id" not in meta or meta.get("board_id") == board_id:
            try:
                p = int(p_str)
                valid_post_nums.add(p)
                
                if 'timestamp' in meta:
                    dt = datetime.fromisoformat(meta['timestamp'])
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=UTC)
                    
                    final_board_id = meta.get("board_id", board_id)
                    messages_storage[p] = {
                        "author_id": meta["author_id"],
                        "timestamp": dt,
                        "author_message_id": meta.get("author_msg"),
                        "board_id": final_board_id
                    }
            except (ValueError, TypeError):
                continue

    loaded_post_count = 0
    for p_str, mapping in data.get("post_to_messages", {}).items():
        try:
            p_num = int(p_str)
            if p_num in valid_post_nums:
                post_to_messages[p_num] = { int(uid): mid for uid, mid in mapping.items() }
                loaded_post_count += 1
        except (ValueError, TypeError):
            continue
            
    for key, post_num in data.get("message_to_post", {}).items():
        try:
            if post_num in valid_post_nums:
                uid, mid = map(int, key.split("_"))
                message_to_post[(uid, mid)] = post_num
        except (ValueError, TypeError):
            continue
            
    status = "сжато" if is_gzipped else "старый формат"
    print(f"[{board_id}] reply-cache загружен: {loaded_post_count} постов ({status})")

async def graceful_shutdown(bots: list[Bot], healthcheck_site: web.TCPSite | None = None):
    """Обработчик корректного сохранения данных ВСЕХ досок перед остановкой."""
    global is_shutting_down
    if is_shutting_down:
        return

    is_shutting_down = True
    print("🛑 Получен сигнал shutdown, начинаем процедуру завершения...")

    # 1. Останавливаем прием новых апдейтов
    try:
        await dp.stop_polling()
        print("⏸ Polling для всех ботов остановлен.")
    except Exception as e:
        print(f"⚠️ Не удалось остановить polling: {e}")

    # 2. Даем немного времени на обработку уже полученных апдейтов
    print("Ожидание опустошения очередей (до 10 секунд)...")
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

    # --- НАЧАЛО ИЗМЕНЕНИЙ: ПРАВИЛЬНЫЙ ПОРЯДОК ОСТАНОВКИ ---

    # 3. Выполняем финальный бэкап, ПОКА event loop еще полностью активен
    print("💾 Попытка финального сохранения и бэкапа в GitHub (таймаут 120 секунд)...")
    try:
        # Выполняем сохранение как обычную асинхронную функцию, а не как новую задачу
        await asyncio.wait_for(save_all_boards_and_backup(), timeout=120.0)
        print("✅ Финальный бэкап успешно завершен в рамках таймаута.")
    except asyncio.TimeoutError:
        print("⛔ КРИТИЧЕСКАЯ ОШИБКА: Финальный бэкап не успел выполниться и был прерван!")
    except Exception as e:
        print(f"⛔ КРИТИЧЕСКАЯ ОШИБКА: Не удалось выполнить финальный бэкап: {e}")

    # 4. ТОЛЬКО ПОСЛЕ сохранения отменяем все остальные фоновые задачи
    print("Отменяем все активные фоновые задачи...")
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
    
    # Ждем завершения отмененных задач
    await asyncio.gather(*tasks, return_exceptions=True)
    print("✅ Все фоновые задачи остановлены.")

    # 5. Завершаем работу с остальными компонентами (healthcheck, executor'ы)
    print("Завершение остальных компонентов...")
    try:
        if healthcheck_site:
            await healthcheck_site.stop()
            print("🛑 Healthcheck server stopped")

        # Executor'ы лучше закрывать без ожидания, так как их задачи уже отменены
        git_executor.shutdown(wait=False, cancel_futures=True)
        save_executor.shutdown(wait=False, cancel_futures=True)
        print("🛑 Executors shutdown initiated.")

    except Exception as e:
        print(f"Ошибка при завершении компонентов: {e}")
    
    # 6. В самом конце, когда уже точно никто не использует сессию, мы ее закрываем
    # Это будет сделано в блоке finally функции main(), что является правильным местом.
    print("✅ Процедура graceful_shutdown завершена. Сессия будет закрыта в main().")
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---
# ========== БЛОК ДИАГНОСТИКИ ПАМЯТИ ==========

def _sync_get_memory_summary() -> str:
    """
    Синхронная, блокирующая функция для сбора и форматирования данных о памяти.
    Предназначена для вызова в ThreadPoolExecutor.
    """
    # Импорты здесь, чтобы не загружать модуль, если функция не используется
    import io
    import sys
    from pympler import muppy, summary

    # Получаем все объекты, которые отслеживает сборщик мусора
    all_objects = muppy.get_objects()
    
    # Создаем сводку, ограничивая вывод 25 самыми "тяжелыми" типами объектов
    sum_result = summary.summarize(all_objects)
    
    # Перехватываем стандартный вывод, чтобы записать результат в строку
    old_stdout = sys.stdout
    sys.stdout = captured_output = io.StringIO()
    summary.print_(sum_result, limit=25)
    sys.stdout = old_stdout # Возвращаем стандартный вывод на место
    
    return captured_output.getvalue()

async def log_memory_summary():
    """
    Асинхронная обертка для неблокирующего логгирования состояния памяти.
    """
    print(f"\n--- 📝 Запуск анализа памяти в {datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S UTC')} ---")
    
    # Принудительно запускаем сборщик мусора перед анализом для получения более точных данных
    gc.collect()
    
    loop = asyncio.get_running_loop()
    try:
        # Выполняем тяжелую операцию в отдельном потоке, чтобы не блокировать бота
        summary_text = await loop.run_in_executor(
            save_executor, # Используем существующий executor
            _sync_get_memory_summary
        )
        
        # Выводим результат в консоль
        print(summary_text.strip())
        print("--- ✅ Анализ памяти завершен ---\n")
        
    except Exception as e:
        print(f"--- ❌ Ошибка при анализе памяти: {e} ---\n")

# ===============================================

async def auto_memory_cleaner():
    """
    Полная и честная очистка мусора каждые 10 минут.
    (ВЕРСЯ 8.0 - БЕЗ EXECUTOR ДЛЯ ОЧИСТКИ СЛОВАРЕЙ)
    """
    # --- ИЗМЕНЕНИЕ: Перемещено в начало функции ---
    global message_to_post
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---
    while True:
        await asyncio.sleep(600)  # 10 минут

        deleted_post_keys = []
        
        # --- НАЧАЛО ИЗМЕНЕНИЙ: Оптимизация и выполнение в одном потоке ---
        async with storage_lock:
            # --- Блок 1: Очистка старых постов ---
            if len(messages_storage) > MAX_MESSAGES_IN_MEMORY:
                to_delete_count = len(messages_storage) - MAX_MESSAGES_IN_MEMORY
                # --- НАЧАЛО ИЗМЕНЕНИЙ ---
                # Преобразуем ключи в список один раз и берем срез с начала.
                # Это избегает полной сортировки и намного эффективнее.
                all_keys = list(messages_storage.keys())
                deleted_post_keys = all_keys[:to_delete_count]
                # --- КОНЕЦ ИЗМЕНЕНИЙ ---
                
                for post_num in deleted_post_keys:
                    messages_storage.pop(post_num, None)
                    post_to_messages.pop(post_num, None)

            if deleted_post_keys:
                print(f"🧹 Очистка памяти: удалено {len(deleted_post_keys)} старых постов из основного хранилища.")

            # --- Блок 2: Эффективная очистка message_to_post "на месте" ---
            actual_post_nums = set(messages_storage.keys())
            
            # --- НАЧАЛО ИЗМЕНЕНИЙ: Оптимизация очистки ---
            # Собираем ключи, которые ссылаются на уже удаленные посты
            keys_to_delete = [
                key for key, post_num in message_to_post.items()
                if post_num not in actual_post_nums
            ]
            
            # Итеративно удаляем устаревшие ключи из оригинального словаря,
            # избегая создания его полной копии в памяти.
            deleted_count = len(keys_to_delete)
            if deleted_count > 0:
                for key in keys_to_delete:
                    del message_to_post[key]
                print(f"🧹 Очистка message_to_post: удалено {deleted_count} неактуальных связей.")
            # --- КОНЕЦ ИЗМЕНЕНИЙ ---

            # --- Блок 3: Агрессивная очистка данных неактивных пользователей ---
            for board_id in BOARDS:
                b_data = board_data[board_id]
                now_utc = datetime.now(UTC)
                
                users_with_active_restrictions = {uid for uid, expiry in b_data.get('mutes', {}).items() if expiry > now_utc}
                users_with_active_restrictions.update({uid for uid, expiry in b_data.get('shadow_mutes', {}).items() if expiry > now_utc})

                inactive_threshold = timedelta(hours=12)
                
                users_to_purge = [
                    uid for uid, last_time in b_data.get('last_activity', {}).items()
                    if (now_utc - last_time) > inactive_threshold and uid not in users_with_active_restrictions
                ]
                
                purged_count = 0
                for user_id in users_to_purge:
                    b_data['last_activity'].pop(user_id, None)
                    b_data['last_texts'].pop(user_id, None)
                    b_data['last_stickers'].pop(user_id, None)
                    b_data['last_animations'].pop(user_id, None)
                    b_data['spam_violations'].pop(user_id, None)
                    b_data['spam_tracker'].pop(user_id, None)
                    b_data['last_user_msgs'].pop(user_id, None)
                    b_data['message_counter'].pop(user_id, None)
                    b_data['user_state'].pop(user_id, None) 
                    purged_count += 1
                
                if purged_count > 0:
                    print(f"🧹 [{board_id}] Очищены данные {purged_count} неактивных пользователей.")

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
        
        # --- КОНЕЦ БЛОКА ИЗМЕНЕНИЙ ---
        
        # --- Блок 4: Очистка зависших медиа-групп ---
        now_ts = time.time()
        stale_groups = [
            group_id for group_id in current_media_groups
            if group_id not in media_group_timers
        ]
        if stale_groups:
             print(f"🧹 Найдено {len(stale_groups)} зависших медиа-групп для очистки.")
             for group_id in stale_groups:
                 current_media_groups.pop(group_id, None)
                 if group_id in media_group_timers:
                     media_group_timers[group_id].cancel()
                     media_group_timers.pop(group_id, None)

        # --- Блок 5: Очистка трекера реакций ---
        tracker_inactive_threshold_sec = 11 * 3600
        keys_to_delete_from_tracker = [
            author_id for author_id, timestamps in author_reaction_notify_tracker.items()
            if not timestamps or (now_ts - timestamps[-1] > tracker_inactive_threshold_sec)
        ]
        if keys_to_delete_from_tracker:
            for author_id in keys_to_delete_from_tracker:
                del author_reaction_notify_tracker[author_id]

        # --- Финальный вызов сборщика мусора ---
        gc.collect()
        print(f"🧹 Очистка памяти завершена. Следующая через 10 минут.")
        
def _sync_collect_board_statistics(hour_ago: datetime, all_messages_meta: dict) -> defaultdict[str, int]:
    """
    Синхронная, блокирующая функция для сбора статистики постов за последний час.
    Безопасна для выполнения в executor'е, работает только с переданными данными.
    """
    posts_per_hour = defaultdict(int)
    # --- НАЧАЛО ИЗМЕНЕНИЙ ---
    # Итерируемся по ключам словаря в обратном порядке без полной сортировки.
    # Это значительно быстрее и опирается на сохранение порядка вставки в dict (Python 3.7+).
    for post_num in reversed(all_messages_meta.keys()):
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---
        post_meta = all_messages_meta.get(post_num)
        if not post_meta:
            continue
        
        post_time = post_meta.get('timestamp')
        if not post_time or post_time < hour_ago:
            # Прерываем цикл, так как все последующие посты будут еще старше.
            break
        
        b_id = post_meta.get('board_id')
        if b_id:
            posts_per_hour[b_id] += 1
            
    return posts_per_hour

async def board_statistics_broadcaster():
    """
    Раз в час собирает общую статистику и рассылает на каждую доску.
    Блокирующие операции вынесены в executor для предотвращения "заморозки" процесса.
    """
    await asyncio.sleep(300)

    while True:
        try:
            await asyncio.sleep(3600)

            now = datetime.now(UTC)
            hour_ago = now - timedelta(hours=1)
            
            # 1. Быстро и безопасно копируем данные под блокировкой
            async with storage_lock:
                messages_storage_copy = messages_storage.copy()
            
            # 2. Выполняем медленную, блокирующую операцию в отдельном потоке
            loop = asyncio.get_running_loop()
            posts_per_hour = await loop.run_in_executor(
                save_executor,
                _sync_collect_board_statistics,
                hour_ago,
                messages_storage_copy
            )
            
            # 3. Продолжаем с асинхронной логикой, используя полученные данные
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
                    hour_stat = posts_per_hour.get(b_id_inner, 0)
                    total_stat = board_data[b_id_inner].get('board_post_count', 0)
                    template = "<b>{name}</b> - {hour} pst/hr, total: {total}" if board_id == 'int' else "<b>{name}</b> - {hour} пст/час, всего: {total}"
                    stats_lines.append(template.format(
                        name=config_inner['name'],
                        hour=hour_stat,
                        total=total_stat
                    ))
                
                header_text = "📊 Boards Statistics:\n" if board_id == 'int' else "📊 Статистика досок:\n"
                full_stats_text = header_text + "\n".join(stats_lines)
                header = "### Statistics ###" if board_id == 'int' else "### Статистика ###"

                if random.random() < 0.66:
                    captions = DVACH_STATS_CAPTIONS_EN if board_id == 'int' else DVACH_STATS_CAPTIONS
                    dvach_caption = random.choice(captions)
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
            base_help_text = random.choice(HELP_TEXT_EN_COMMANDS)
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
            base_help_text = random.choice(HELP_TEXT_COMMANDS)
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
        
        messages_to_delete_from_api = []

        # --- НАЧАЛО ИЗМЕНЕНИЙ: Все модификации под единой блокировкой ---
        async with storage_lock:
            posts_to_delete_info = []
            
            # 1. Находим все посты пользователя за указанный период
            # --- ИЗМЕНЕНИЕ ---
            # Замена sorted() на reversed() для итерации от новых постов к старым без сортировки всего хранилища.
            for post_num in reversed(messages_storage):
            # --- КОНЕЦ ИЗМЕНЕНИЯ ---
                post_data = messages_storage.get(post_num, {})
                post_time = post_data.get('timestamp')

                if not post_time or post_time < time_threshold:
                    break

                if post_data.get('author_id') == user_id and post_data.get('board_id') == board_id:
                    posts_to_delete_info.append((post_num, post_data.get('thread_id')))
                    if post_num in post_to_messages:
                        for uid, mid in post_to_messages[post_num].items():
                            messages_to_delete_from_api.append((uid, mid))

            if not posts_to_delete_info:
                return 0
            
            # 2. Производим полную очистку внутренних структур данных, не выходя из блокировки
            for post_num, thread_id in posts_to_delete_info:
                if post_num in post_to_messages:
                    # Итерируемся по копии, чтобы безопасно удалять элементы
                    for uid, mid in list(post_to_messages[post_num].items()):
                        message_to_post.pop((uid, mid), None)
                
                post_to_messages.pop(post_num, None)
                messages_storage.pop(post_num, None)

                # Очистка из данных треда
                if board_id in THREAD_BOARDS and thread_id:
                    threads_data = board_data[board_id].get('threads_data', {})
                    if thread_id in threads_data and 'posts' in threads_data[thread_id]:
                        try:
                            # Используем set для более быстрого удаления
                            thread_posts_set = set(threads_data[thread_id]['posts'])
                            thread_posts_set.discard(post_num)
                            threads_data[thread_id]['posts'] = list(thread_posts_set)
                        except (ValueError, KeyError):
                            pass
        # --- Блокировка освобождена ---
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---
        
        # 3. Физическое удаление сообщений (медленные сетевые операции)
        deleted_count = 0
        for chat_id, message_id in messages_to_delete_from_api:
            try:
                await bot_instance.delete_message(chat_id, message_id)
                deleted_count += 1
            except (TelegramBadRequest, TelegramForbiddenError):
                continue
            except Exception as e:
                print(f"Ошибка при удалении сообщения {message_id} в чате {chat_id}: {e}")
        
        return deleted_count

    except Exception as e:
        import traceback
        print(f"Критическая ошибка в delete_user_posts: {e}\n{traceback.format_exc()}")
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
    (ФИНАЛЬНАЯ ВЕРСЯ С УЛУЧШЕННОЙ ОБРАБОТКОЙ ОШИБОК)
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

            if user_id in thread_info.get('local_mutes', {}):
                if time.time() < thread_info['local_mutes'][user_id]: return
                else: del thread_info['local_mutes'][user_id]
            
            if user_id in thread_info.get('local_shadow_mutes', {}):
                expires_ts = thread_info['local_shadow_mutes'][user_id]
                if time.time() < expires_ts:
                    is_shadow_muted = True
                else:
                    del thread_info['local_shadow_mutes'][user_id]

            async with b_data['thread_locks'][thread_id]:
                local_post_num = len(thread_info.get('posts', [])) + 1
                header_text = await format_thread_post_header(board_id, local_post_num, user_id, thread_info)
                _, current_post_num = await format_header(board_id)
                thread_info['posts'].append(current_post_num)
                thread_info['last_activity_at'] = time.time()
            
            recipients = thread_info.get('subscribers', set()) - {user_id}

            posts_count = len(thread_info.get('posts', []))
            milestones = [50, 150, 220]
            for milestone in milestones:
                if posts_count == milestone and milestone not in thread_info.get('announced_milestones', []):
                    thread_info.setdefault('announced_milestones', []).append(milestone)
                    asyncio.create_task(post_thread_notification_to_channel(
                        bots=GLOBAL_BOTS,
                        board_id=board_id,
                        thread_id=thread_id,
                        thread_info=thread_info,
                        event_type='milestone',
                        details={'posts': milestone}
                    ))

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
        content['post_num'] = current_post_num

        reply_info_for_author = {}
        async with storage_lock:
            messages_storage[current_post_num] = {
                'author_id': user_id, 'timestamp': datetime.now(UTC), 'content': content,
                'author_message_id': None, 'board_id': board_id, 'thread_id': thread_id
            }
            if reply_to_post:
                reply_info_for_author = post_to_messages.get(reply_to_post, {})

        content_for_author = await _apply_mode_transformations(content.copy(), board_id)
        
        author_results = await send_message_to_users(
            bot_instance=bot_instance, recipients={user_id},
            content=content_for_author, reply_info=reply_info_for_author
        )
        
        if author_results and author_results[0] and author_results[0][1]:
            sent_to_author = author_results[0][1]
            messages_to_save = sent_to_author if isinstance(sent_to_author, list) else [sent_to_author]
            author_message_id_to_archive = messages_to_save[0].message_id
            
            async with storage_lock:
                if current_post_num in messages_storage:
                    messages_storage[current_post_num]['author_message_id'] = author_message_id_to_archive
                
                for m in messages_to_save:
                    post_to_messages.setdefault(current_post_num, {})[user_id] = m.message_id
                    message_to_post[(user_id, m.message_id)] = current_post_num
        
        if not is_shadow_muted and recipients:
            await message_queues[board_id].put({
                'recipients': recipients, 'content': content, 'post_num': current_post_num,
                'board_id': board_id, 'thread_id': thread_id
            })

        if not content.get('is_system_message'):
            asyncio.create_task(_forward_post_to_realtime_archive(
                bot_instance=bot_instance, board_id=board_id, post_num=current_post_num, content=content
            ))

    except TelegramForbiddenError:
        b_data['users']['active'].discard(user_id)
        print(f"🚫 [{board_id}] Пользователь {user_id} заблокировал бота (из process_new_post).")
        if current_post_num:
            async with storage_lock:
                messages_storage.pop(current_post_num, None)
    # ИЗМЕНЕНИЕ: Добавлен traceback.format_exc() для полного логирования неожиданных ошибок
    except Exception as e:
        print(f"❌ Критическая ошибка в process_new_post для user {user_id}: {e}\n{traceback.format_exc()}")
        if current_post_num:
            async with storage_lock:
                messages_storage.pop(current_post_num, None)

async def _forward_post_to_realtime_archive(bot_instance: Bot, board_id: str, post_num: int, content: dict):
    """
    Надежно отправляет текстовую копию поста в реал-тайм архивный канал.
    Для медиа используются текстовые заглушки.
    (ВЕРСИЯ 7.0 - ВОЗВРАТ К ЗАГЛУШКАМ ДЛЯ СТАБИЛЬНОСТИ)
    """
    archive_bot = GLOBAL_BOTS.get(ARCHIVE_POSTING_BOT_ID)
    if not archive_bot:
        print(f"⛔ Ошибка: бот для постинга в реал-тайм архив ('{ARCHIVE_POSTING_BOT_ID}') не найден.")
        return

    try:
        board_name = BOARD_CONFIG.get(board_id, {}).get('name', board_id)
        lang = 'en' if board_id == 'int' else 'ru'
        header_text = f"<b>{board_name}</b> | {'Post' if lang == 'en' else 'Пост'} №{post_num}"

        # --- НАЧАЛО ИЗМЕНЕНИЙ: Полный рефакторинг на текстовые заглушки ---
        
        text_content = ""
        content_type_str = str(content.get("type", "")).split('.')[-1].lower()

        if content_type_str == 'text':
            text_content = content.get('text', '')
        else:
            # Для всех медиа-типов берем подпись, если она есть
            text_content = content.get('caption', '') or ''
            
            # Создаем плейсхолдер для самого медиа
            media_placeholders = {
                'photo': '[Фото]', 'video': '[Видео]', 'animation': '[GIF]', 'sticker': '[Стикер]',
                'document': '[Документ]', 'audio': '[Аудио]', 'voice': '[Голосовое сообщение]',
                'video_note': '[Кружок]', 'media_group': '[Медиа-группа]'
            }
            placeholder = media_placeholders.get(content_type_str, f'[{content_type_str}]')
            
            # Собираем плейсхолдер и подпись
            text_content = f"{placeholder}\n{text_content}".strip()

        final_text = f"{header_text}\n\n{text_content}"
        if len(final_text) > 4096:
            final_text = final_text[:4093] + "..."

        async def send_with_retry():
            try:
                await archive_bot.send_message(
                    chat_id=REALTIME_ARCHIVE_CHANNEL_ID,
                    text=final_text,
                    parse_mode="HTML"
                )
            except TelegramRetryAfter as e:
                print(f"⚠️ Попали на лимит API при отправке в архив. Ждем {e.retry_after} сек...")
                await asyncio.sleep(e.retry_after + 1)
                await send_with_retry()
            except TelegramBadRequest as e:
                print(f"❌ BadRequest при отправке поста #{post_num} в архив: {e}")

        await send_with_retry()
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---

    except Exception as e:
        import traceback
        print(f"❌ Не удалось отправить пост #{post_num} в реал-тайм архив: {e}")
        traceback.print_exc()
            
async def _apply_mode_transformations(content: dict, board_id: str) -> dict:
    """
    Централизованно применяет все трансформации режимов с улучшенной обработкой аниме-изображений.
    ВАЖНО: Эта функция теперь модифицирует переданный словарь. Вызывающая сторона
    должна передавать копию, если не хочет изменять оригинал.
    """
    b_data = board_data[board_id]
    # --- НАЧАЛО ИЗМЕНЕНИЙ: Удалено создание копии. Функция теперь мутирует 'content'. ---
    modified_content = content
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---

    is_transform_mode_active = (
        b_data['anime_mode'] or b_data['slavaukraine_mode'] or
        b_data['zaputin_mode'] or b_data['suka_blyat_mode']
    )

    if not is_transform_mode_active:
        return modified_content

    if 'text' in modified_content and modified_content['text']:
        modified_content['text'] = clean_html_tags(modified_content['text'])
    if 'caption' in modified_content and modified_content['caption']:
        modified_content['caption'] = clean_html_tags(modified_content['caption'])

    if b_data['anime_mode']:
        if 'text' in modified_content and modified_content['text']:
            modified_content['text'] = anime_transform(modified_content['text'])
        if 'caption' in modified_content and modified_content['caption']:
            modified_content['caption'] = anime_transform(modified_content['caption'])
        
        if modified_content.get('type') == 'text' and random.random() < 0.49:
            if random.random() < 0.31:
                anime_img_url = await get_monogatari_image()
                print(f"[ANIME DEBUG] Attempting Monogatari image...")
            else:
                anime_img_url = await get_random_anime_image()
                print(f"[ANIME DEBUG] Attempting random waifu image...")

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
        you_marker = " (You)" if user_id_for_context == reply_to_post_author_id else ""
        reply_line = f">>{reply_to_post}{you_marker}"
        formatted_reply_line = f"<code>{escape_html(reply_line)}</code>"
        parts.append(formatted_reply_line)
        
    # Блок реакций
    reactions_data = post_data.get('reactions')
    
    if reactions_data:
        reaction_lines = []
        # --- НАЧАЛО ИЗМЕНЕНИЙ: Замена прямого доступа на .get() ---
        # Это предотвратит KeyError, если 'users' отсутствует, но 'reactions' существует
        user_reactions = reactions_data.get('users', {})
        if isinstance(user_reactions, dict):
            all_emojis = [emoji for user_emojis in user_reactions.values() for emoji in user_emojis]
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---
            
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
        formatted_main_text = apply_greentext_formatting(main_text_raw)
        parts.append(formatted_main_text)
        
    return '\n\n'.join(filter(None, parts))

async def send_message_to_users(
    bot_instance: Bot,
    recipients: set[int],
    content: dict,
    reply_info: dict | None = None,
    keyboard: InlineKeyboardMarkup | None = None,
) -> list:
    """Оптимизированная рассылка сообщений с минимизацией блокировок."""
    if not recipients or not content or 'type' not in content:
        return []

    board_id = next((b_id for b_id, config in BOARD_CONFIG.items() if config['token'] == bot_instance.token), None)
    if not board_id:
        print(f"КРИТИЧЕСКАЯ ОШИБКА: Не удалось найти доску для бота с токеном ...{bot_instance.token[-6:]}")
        return []

    b_data = board_data[board_id]
    modified_content = await _apply_mode_transformations(content.copy(), board_id)
    
    blocked_users = set()
    active_recipients = {uid for uid in recipients if uid not in b_data['users']['banned']}
    if not active_recipients:
        return []

    user_specific_data = {}
    async with storage_lock:
        post_num = modified_content.get('post_num')
        post_data = messages_storage.get(post_num, {})
        reply_to_post_num = modified_content.get('reply_to_post')
        reply_author_id = messages_storage.get(reply_to_post_num, {}).get('author_id') if reply_to_post_num else None

        for uid in active_recipients:
            reply_to_mid = None
            if reply_info and isinstance(reply_info, dict):
                reply_to_mid = reply_info.get(uid)
            if reply_to_mid is None and reply_to_post_num:
                if reply_to_post_num in post_to_messages and isinstance(post_to_messages[reply_to_post_num], dict):
                    reply_to_mid = post_to_messages[reply_to_post_num].get(uid)

            header_text = modified_content.get('header', '')
            head = f"<i>{escape_html(header_text)}</i>"
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
                content=content_for_user, user_id_for_context=uid,
                post_data=post_data, reply_to_post_author_id=reply_author_id
            )
            
            user_specific_data[uid] = {
                'reply_to_mid': reply_to_mid,
                'head': head,
                'body': formatted_body,
            }
    
    async def really_send(uid: int):
        data = user_specific_data.get(uid)
        if not data: return None
        
        reply_to = data['reply_to_mid']
        head = data['head']
        formatted_body = data['body']
        
        try:
            # --- НАЧАЛО ИЗМЕНЕНИЙ: Надежное получение строкового типа контента ---
            ct_raw = modified_content["type"]
            # Эта строка надежно извлекает 'video' из 'ContentType.VIDEO' или 'video' из 'video'
            ct = str(ct_raw).split('.')[-1].lower()
            # --- КОНЕЦ ИЗМЕНЕНИЙ ---
            
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
            
            kwargs = {'chat_id': uid, 'reply_to_message_id': reply_to, 'reply_markup': keyboard}
            
            if ct == 'text':
                kwargs.update(text=full_text, parse_mode="HTML")
            elif ct in ['photo', 'video', 'animation', 'document', 'audio', 'voice']:
                if len(full_text) > 1024: full_text = full_text[:1021] + "..."
                kwargs.update(caption=full_text, parse_mode="HTML")
                file_source = modified_content.get('image_url') or modified_content.get("file_id")
                kwargs[ct] = file_source
            elif ct in ['video_note', 'sticker']:
                kwargs[ct] = modified_content.get("file_id")
            else:
                return None
            
            try:
                return await send_method(**kwargs)
            except TelegramBadRequest as e:
                if ct == 'photo' and 'image_url' in modified_content and ("failed to get HTTP URL content" in e.message or "wrong type" in e.message):
                    error_text = "⚠️ [Изображение недоступно]"
                    fallback_content = f"{head}\n\n{error_text}\n\n{formatted_body}"
                    return await bot_instance.send_message(
                        chat_id=uid, text=fallback_content, parse_mode="HTML",
                        reply_to_message_id=reply_to, reply_markup=keyboard)
                
                placeholder_map = {"VOICE_MESSAGES_FORBIDDEN": " VOICE MESSAGE ", "VIDEO_MESSAGES_FORBIDDEN": " VIDEO MESSAGE (кружок) "}
                for error_str, placeholder in placeholder_map.items():
                    if error_str in e.message:
                        error_info = (f"<b>[ 🚫 Blocked Content ]</b>\n\n" f"You have blocked receiving {placeholder} in your Telegram privacy settings.") if board_id == 'int' else (f"<b>[ Тут должно было быть медиа, но... ]</b>\n\n" f"У вас в настройках приватности телеграм запрещено получение {placeholder}")
                        return await bot_instance.send_message(
                            chat_id=uid, text=f"{head}\n\n{error_info}", parse_mode="HTML",
                            reply_to_message_id=reply_to, reply_markup=keyboard)
                raise e

        except TelegramRetryAfter as e:
            await asyncio.sleep(e.retry_after + 1)
            return await really_send(uid)
        except TelegramForbiddenError:
            blocked_users.add(uid)
            return None
        except Exception as e:
            print(f"❌ Ошибка отправки {uid} ботом {bot_instance.id}: {e}")
            return None

    semaphore = asyncio.Semaphore(100)
    async def send_with_semaphore(uid):
        async with semaphore:
            result = await really_send(uid)
            return (uid, result)

    tasks = [send_with_semaphore(uid) for uid in active_recipients]
    results = await asyncio.gather(*tasks)

    async with storage_lock:
        post_num = modified_content.get('post_num')
        if post_num:
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
    список реакций. (ИСПРАВЛЕННАЯ ВЕРСИЯ С КОРРЕКТНОЙ БЛОКИРОВКОЙ)
    """
    # --- НАЧАЛО ИЗМЕНЕНИЙ: Полный рефакторинг логики ---
    
    # ЭТАП 1: Собираем ID всех сообщений для редактирования и базовые данные под одной блокировкой.
    message_copies_to_edit = {}
    board_id = None
    content_type = None
    post_data_copy = {}
    reply_author_id = None
    
    async with storage_lock:
        post_data = messages_storage.get(post_num)
        message_copies = post_to_messages.get(post_num)

        if not post_data or not message_copies:
            return

        # Копируем только то, что необходимо для дальнейшей работы
        message_copies_to_edit = message_copies.copy()
        post_data_copy = post_data.copy()
        board_id = post_data.get('board_id')
        content_type = post_data.get('content', {}).get('type')
        
        reply_to_post_num = post_data.get('content', {}).get('reply_to_post')
        if reply_to_post_num:
            reply_author_id = messages_storage.get(reply_to_post_num, {}).get('author_id')
        
        can_be_edited = content_type in ['text', 'photo', 'video', 'animation', 'document', 'audio']
        if not can_be_edited or not board_id:
            return

    # ЭТАП 2: Асинхронно подготавливаем данные для каждого пользователя и выполняем редактирование БЕЗ блокировок.
    async def _edit_one(user_id: int, message_id: int):
        full_text = ""
        try:
            # Используем данные, скопированные на ЭТАПЕ 1.
            content = post_data_copy.get('content', {})
            
            # Формируем заголовок с пометкой об ответе
            header_text = content.get('header', '')
            head = f"<i>{escape_html(header_text)}</i>"
            if user_id == reply_author_id:
                if board_id == 'int': head = head.replace("Post", "🔴 Post")
                else: head = head.replace("Пост", "🔴 Пост")

            # Формируем тело с (You) и реакциями
            content_for_user = content.copy()
            text_or_caption = content_for_user.get('text') or content_for_user.get('caption')
            if text_or_caption:
                # add_you_to_my_posts больше не требует блокировки, так как работает с копией.
                text_with_you = add_you_to_my_posts(text_or_caption, user_id)
                if 'text' in content_for_user: content_for_user['text'] = text_with_you
                elif 'caption' in content_for_user: content_for_user['caption'] = text_with_you
            
            # _format_message_body больше не требует блокировки.
            formatted_body = await _format_message_body(
                content=content_for_user,
                user_id_for_context=user_id,
                post_data=post_data_copy,
                reply_to_post_author_id=reply_author_id
            )
            full_text = f"{head}\n\n{formatted_body}" if formatted_body else head

            # Выполняем медленную сетевую операцию
            if content_type == 'text':
                if len(full_text) > 4096: full_text = full_text[:4093] + "..."
                await bot_instance.edit_message_text(text=full_text, chat_id=user_id, message_id=message_id, parse_mode="HTML")
            else:
                if len(full_text) > 1024: full_text = full_text[:1021] + "..."
                await bot_instance.edit_message_caption(caption=full_text, chat_id=user_id, message_id=message_id, parse_mode="HTML")

        except TelegramBadRequest as e:
            if "message is not modified" not in e.message and "message to edit not found" not in e.message:
                 print(f"⚠️ Ошибка (BadRequest) при редактировании поста #{post_num} для {user_id}: {e}")
        except TelegramForbiddenError:
            b_data = board_data.get(board_id)
            if b_data:
                b_data['users']['active'].discard(user_id)
        except Exception as e:
            print(f"❌ Неизвестная ошибка при редактировании поста #{post_num} для {user_id}: {e}")

    # Запускаем задачи на редактирование параллельно
    tasks = [
        _edit_one(uid, mid)
        for uid, mid in message_copies_to_edit.items()
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

async def send_missed_messages(bot: Bot, board_id: str, user_id: int, target_location: str) -> tuple[bool, bool]:
    """
    Отправляет пользователю пропущенные сообщения. Гарантирует, что ОП-пост
    треда будет показан первым.
    Возвращает кортеж (были ли отправлены сообщения, нужно ли показать кнопку "Вся летопись").
    """
    b_data = board_data[board_id]
    user_s = b_data['user_state'].setdefault(user_id, {})
    
    missed_post_nums_full = []
    last_seen_post = 0
    op_post_num = None
    
    async with storage_lock:
        if target_location == 'main':
            last_seen_post = user_s.get('last_seen_main', 0)
            all_main_posts = sorted([
                p_num for p_num, p_data in messages_storage.items() 
                if p_data.get('board_id') == board_id and not p_data.get('thread_id')
            ])
            missed_post_nums_full = [p_num for p_num in all_main_posts if p_num > last_seen_post]
        else:
            thread_id = target_location
            thread_info = b_data.get('threads_data', {}).get(thread_id)
            if not thread_info: return False, False

            all_thread_posts = sorted(thread_info.get('posts', []))
            if all_thread_posts:
                op_post_num = all_thread_posts[0]
            last_seen_threads = user_s.setdefault('last_seen_threads', {})
            last_seen_post = last_seen_threads.get(thread_id, 0)
            missed_post_nums_full = [p_num for p_num in all_thread_posts if p_num > last_seen_post]

    if not missed_post_nums_full:
        return False, False

    missed_post_nums_to_send = missed_post_nums_full.copy()
    
    MAX_MISSED_TO_SEND = 70
    if len(missed_post_nums_to_send) > MAX_MISSED_TO_SEND:
        missed_post_nums_to_send = missed_post_nums_to_send[-MAX_MISSED_TO_SEND:]

    lang = 'en' if board_id == 'int' else 'ru'
    show_history_button = False

    if op_post_num and op_post_num in missed_post_nums_to_send:
        await _send_single_missed_post(bot, user_id, op_post_num)
        missed_post_nums_to_send.remove(op_post_num)

    THRESHOLD = 30
    
    if len(missed_post_nums_full) > THRESHOLD:
        show_history_button = True
        posts_to_skip_count = len(missed_post_nums_full) - THRESHOLD
        posts_to_send = [p for p in missed_post_nums_full if p > last_seen_post][-THRESHOLD:]

        skip_notice_text = random.choice(thread_messages[lang]['missed_posts_notification']).format(count=posts_to_skip_count)
        try:
            await bot.send_message(user_id, f"<i>{skip_notice_text}</i>", parse_mode="HTML")
            await asyncio.sleep(0.1)
        except (TelegramForbiddenError, TelegramBadRequest):
            pass

        for post_num in posts_to_send:
            if post_num != op_post_num:
                await _send_single_missed_post(bot, user_id, post_num)
    else:
        for post_num in missed_post_nums_to_send:
            await _send_single_missed_post(bot, user_id, post_num)
    
    final_text = "All new messages loaded." if lang == 'en' else "Все новые сообщения загружены."
    entry_keyboard = _get_thread_entry_keyboard(board_id, show_history_button)
    
    try:
        await bot.send_message(user_id, final_text, reply_markup=entry_keyboard, parse_mode="HTML")
    except (TelegramForbiddenError, TelegramBadRequest):
        pass

    if missed_post_nums_full:
        new_last_seen = missed_post_nums_full[-1]
        
        if target_location == 'main':
            user_s['last_seen_main'] = new_last_seen
        else:
            user_s.setdefault('last_seen_threads', {})[target_location] = new_last_seen
    
    return True, show_history_button

async def help_broadcaster():
    """
    Раз в ~4 часов отправляет на каждую доску одно из трех сообщений:
    список команд, список досок или рекламу тредов, выбирая случайный вариант текста.
    """
    await asyncio.sleep(300)  # Начальная задержка 10 минут

    while True:
        # Случайная задержка от 11 до 13 часов
        delay = random.randint(9600, 16800)
        await asyncio.sleep(delay)
        
        try:
            for board_id in BOARDS:
                if board_id == 'test':
                    continue

                b_data = board_data[board_id]
                recipients = b_data['users']['active'] - b_data['users']['banned']

                if not recipients:
                    continue
                
                lang = 'en' if board_id == 'int' else 'ru'
                message_text = ""

                # --- Равновероятный выбор одного из трех типов контента ---
                choice = random.randint(1, 3)
                
                if choice == 1: # 1. Список команд
                    message_text = random.choice(HELP_TEXT_EN_COMMANDS) if lang == 'en' else random.choice(HELP_TEXT_COMMANDS)
                
                elif choice == 2: # 2. Список досок
                    message_text = generate_boards_list(BOARD_CONFIG, lang)

                else: # 3. Реклама тредов
                    message_text = random.choice(THREAD_PROMO_TEXT_EN) if lang == 'en' else random.choice(THREAD_PROMO_TEXT_RU)
                
                header, post_num = await format_header(board_id)
                content = {
                    'type': 'text', 'header': header, 'text': message_text,
                    'is_system_message': True
                }

                async with storage_lock:
                    messages_storage[post_num] = {
                        'author_id': 0, 'timestamp': datetime.now(UTC),
                        'content': content, 'board_id': board_id
                    }

                await message_queues[board_id].put({
                    'recipients': recipients, 'content': content,
                    'post_num': post_num, 'board_id': board_id
                })

                print(f"✅ [{board_id}] Сообщение помощи #{post_num} добавлено в очередь.")

        except Exception as e:
            print(f"❌ [{board_id}] Ошибка в help_broadcaster: {e}")
            await asyncio.sleep(120)

async def motivation_broadcaster():
    """Отправляет мотивационные сообщения на каждую доску в разное время."""
    await asyncio.sleep(15)  # Начальная задержка

    async def board_motivation_worker(board_id: str):
        """Индивидуальный воркер для одной доски."""
        while True:
            try:
                # Случайная задержка от 2 до 4 часов
                delay = random.randint(7000, 14000)
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
    """Асинхронная обертка для сохранения данных о тредах с защитой от гонки состояний."""
    if board_id not in THREAD_BOARDS:
        return

    # 1. Захватываем блокировку и создаем безопасную копию данных
    async with storage_lock:
        original_data = board_data[board_id].get('threads_data', {})
        
        # Создаем копию данных, пригодную для сериализации (конвертируем set в list)
        data_to_save = {}
        for thread_id, thread_info in original_data.items():
            serializable_info = thread_info.copy()
            if 'subscribers' in serializable_info and isinstance(serializable_info['subscribers'], set):
                serializable_info['subscribers'] = list(serializable_info['subscribers'])
            data_to_save[thread_id] = serializable_info

    # 2. Передаем безопасную копию в другой поток для записи
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(
        save_executor,
        _sync_save_threads_data,
        board_id,
        data_to_save
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
async def cmd_start(message: types.Message, state: FSMContext, board_id: str | None):
    user_id = message.from_user.id
    if not board_id: return
    
    b_data = board_data[board_id]
    command_payload = message.text.split()[1] if len(message.text.split()) > 1 else None

    # --- НАЧАЛО ИЗМЕНЕНИЙ: Логика входа в тред через диплинк ---
    if command_payload and command_payload.startswith("thread_"):
        thread_id = command_payload.split('_')[-1]
        
        if board_id in THREAD_BOARDS and thread_id in b_data.get('threads_data', {}):
            b_data['users']['active'].add(user_id) # Добавляем юзера в активные, если его там нет
            
            # ВЫЗЫВАЕМ НОВУЮ УНИВЕРСАЛЬНУЮ ФУНКЦИЮ
            await _enter_thread_logic(
                bot=message.bot,
                board_id=board_id,
                user_id=user_id,
                thread_id=thread_id,
                message_to_delete=message
            )
            return # Завершаем выполнение, чтобы не отправлять стартовое сообщение
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---

    if user_id not in b_data['users']['active']:
        b_data['users']['active'].add(user_id)
        print(f"✅ [{board_id}] Новый пользователь через /start: ID {user_id}")
    
    start_text = b_data.get('start_message_text', "Добро пожаловать в ТГАЧ!")
    
    await message.answer(start_text, parse_mode="HTML", disable_web_page_preview=True)
    
    await _send_thread_info_if_applicable(message, board_id)
    
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
        # --- НАЧАЛО ИЗМЕНЕНИЙ: Безопасное получение текста ---
        empty_phrases = thread_messages.get(lang, {}).get('threads_list_empty', [])
        default_empty_text = "No active threads right now."
        empty_text = random.choice(empty_phrases) if empty_phrases else default_empty_text
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---
        await callback.answer(empty_text, show_alert=True)
        return

    sorted_threads = sorted(
        active_threads.items(),
        key=lambda item: item[1].get('last_activity_at', 0),
        reverse=True
    )
    
    user_s = b_data['user_state'].setdefault(callback.from_user.id, {})
    user_s['sorted_threads_cache'] = [tid for tid, _ in sorted_threads]

    text, keyboard = await generate_threads_page(b_data, callback.from_user.id, page=0)

    await callback.answer()
    try:
        await callback.message.answer(text, reply_markup=keyboard, parse_mode="HTML")
        await callback.message.delete()
    except (TelegramForbiddenError, TelegramBadRequest):
        pass

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

    day_ago = datetime.now(UTC) - timedelta(hours=24)
    posts_last_24h = 0
    
    async with storage_lock:
        # --- ИЗМЕНЕНИЕ ---
        # Замена sorted() на reversed() для эффективной итерации.
        for post_num in reversed(messages_storage):
        # --- КОНЕЦ ИЗМЕНЕНИЯ ---
            post_data = messages_storage[post_num]
            post_time = post_data.get("timestamp")
            
            if not post_time or post_time < day_ago:
                break
            
            posts_last_24h += 1
    
    lang = 'en' if board_id == 'int' else 'ru'
    activity_lines = []
    for b_id in BOARDS:
        if b_id == 'test':
            continue
        activity = await get_board_activity_last_hours(b_id, hours=2)
        board_name = BOARD_CONFIG[b_id]['name']
        line = f"<b>{board_name}</b> - {activity:.1f} posts/hr" if lang == 'en' else f"<b>{board_name}</b> - {activity:.1f} п/ч"
        activity_lines.append(line)

    if lang == 'en':
        header_text = "📊 Boards Activity (last 2h):"
        total_text = f"\n\n📅 Total posts in last 24h: {posts_last_24h}"
    else:
        header_text = "📊 Активность досок (за 2ч):"
        total_text = f"\n\n📅 Всего постов за последние 24 часа: {posts_last_24h}"
        
    full_activity_text = f"{header_text}\n\n" + "\n".join(activity_lines) + total_text

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
        return

    current_state = await state.get_state()
    lang = 'en' if board_id == 'int' else 'ru'
    if current_state is not None:
        # --- НАЧАЛО ИЗМЕНЕНИЙ: Безопасное получение текста ---
        cancel_phrases = thread_messages.get(lang, {}).get('create_cancelled', [])
        default_cancel_text = "You are already creating a thread. Use /cancel." if lang == 'en' else "Вы уже создаете тред. Используйте /cancel."
        text = random.choice(cancel_phrases) if cancel_phrases else default_cancel_text
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---
        
        try:
            await message.answer(text)
            await message.delete()
        except (TelegramForbiddenError, TelegramBadRequest):
            pass
        return

    command_args = message.text.split(maxsplit=1)
    if len(command_args) > 1 and command_args[1].strip():
        op_post_text = command_args[1].strip()
        
        await state.update_data(op_post_text=op_post_text)
        await state.set_state(ThreadCreateStates.waiting_for_confirmation)

        if lang == 'en':
            confirmation_text = f"You want to create a thread with this opening post:\n\n---\n{escape_html(op_post_text)}\n---\n\nCreate?"
            button_create, button_edit = "✅ Create Thread", "✏️ Edit Text"
        else:
            confirmation_text = f"Вы хотите создать тред с таким ОП-постом:\n\n---\n{escape_html(op_post_text)}\n---\n\nСоздаем?"
            button_create, button_edit = "✅ Создать тред", "✏️ Редактировать"
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text=button_create, callback_data="create_thread_confirm"),
                InlineKeyboardButton(text=button_edit, callback_data="create_thread_edit")
            ]
        ])
        await message.answer(confirmation_text, reply_markup=keyboard, parse_mode="HTML")

    else:
        await state.set_state(ThreadCreateStates.waiting_for_op_post)
        # --- НАЧАЛО ИЗМЕНЕНИЙ: Безопасное получение текста ---
        prompt_phrases = thread_messages.get(lang, {}).get('create_prompt_op_post', [])
        default_prompt = "Please send the text for your opening post." if lang == 'en' else "Отправьте текст для вашего ОП-поста."
        prompt_text = random.choice(prompt_phrases) if prompt_phrases else default_prompt
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---
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
        
    threads_data = b_data.get('threads_data', {})
    thread_id = secrets.token_hex(4)
    now_ts = time.time()
    now_dt = datetime.now(UTC)
    
    title = escape_html(clean_html_tags(op_post_text).split('\n')[0][:60])
    
    thread_info = {
        'op_id': user_id, 'title': title, 'created_at': now_dt.isoformat(),
        'last_activity_at': now_ts, 'posts': [], 'subscribers': {user_id},
        'local_mutes': {}, 'local_shadow_mutes': {}, 'is_archived': False,
        'announced_milestones': [], 'activity_notified': False
    }
    threads_data[thread_id] = thread_info

    user_s = b_data['user_state'].setdefault(user_id, {})
    user_s['last_thread_creation'] = now_ts

    notification_text = random.choice(thread_messages.get(lang, {}).get('new_thread_public_notification', [])).format(title=title)
    if not notification_text:
        notification_text = f"Создан новый тред: «<b>{title}</b>»"

    bot_username = BOARD_CONFIG[board_id]['username'].lstrip('@')
    deeplink_url = f"https://t.me/{bot_username}?start=thread_{thread_id}"
    button_text = "Войти в тред" if lang == 'ru' else "Enter Thread"
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=button_text, url=deeplink_url)]
    ])

    header, pnum_notify = await format_header(board_id)
    content_notify = {'type': 'text', 'header': header, 'text': notification_text, 'is_system_message': True}
    messages_storage[pnum_notify] = {'author_id': 0, 'timestamp': now_dt, 'content': content_notify, 'board_id': board_id}
    await message_queues[board_id].put({
        'recipients': b_data['users']['active'], 
        'content': content_notify, 
        'post_num': pnum_notify, 
        'board_id': board_id, 
        'keyboard': keyboard
    })
    
    # --- НАЧАЛО ИЗМЕНЕНИЙ: Ручное создание ОП-поста без трансляции в общий чат ---
    
    # 1. Обновляем локацию пользователя ДО создания поста.
    # Это ключевой шаг, который предотвратит отправку поста в общий чат,
    # так как `process_new_post` не найдет получателей в 'main'.
    user_s['location'] = thread_id
    user_s['last_location_switch'] = now_ts
    
    # 2. Используем process_new_post, который теперь не будет делать рассылку
    # благодаря обновленной локации пользователя. Он корректно сохранит пост
    # и отправит его только автору.
    formatted_op_text = f"<b>ОП-ПОСТ</b>\n_______________________________\n{op_post_text}"
    op_post_content = {'type': 'text', 'text': formatted_op_text}

    await process_new_post(
        bot_instance=callback.bot, board_id=board_id, user_id=user_id, content=op_post_content,
        reply_to_post=None, is_shadow_muted=False
    )
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---
    
    await callback.answer()
    try:
        await callback.message.delete()
    except TelegramBadRequest:
        pass

    enter_phrases = thread_messages.get(lang, {}).get('enter_thread_prompt', [])
    default_enter_text = f"You have entered the thread: {title}" if lang == 'en' else f"Вы вошли в тред: {title}"
    enter_message = random.choice(enter_phrases).format(title=title) if enter_phrases else default_enter_text

    entry_keyboard = _get_thread_entry_keyboard(board_id)
    try:
        await callback.message.answer(enter_message, reply_markup=entry_keyboard, parse_mode="HTML")
    except (TelegramForbiddenError, TelegramBadRequest):
        pass

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

    await state.set_state(ThreadCreateStates.waiting_for_op_post)

    # --- НАЧАЛО ИЗМЕНЕНИЙ: Безопасное получение текста ---
    prompt_phrases = thread_messages.get(lang, {}).get('create_prompt_op_post_edit', [])
    default_prompt = "Okay, send the new text for your opening post." if lang == 'en' else "Хорошо, отправьте новый текст для вашего ОП-поста."
    prompt_text = random.choice(prompt_phrases) if prompt_phrases else default_prompt
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---
    
    await callback.answer()
    try:
        await callback.message.edit_text(prompt_text)
    except TelegramBadRequest:
        pass


# --- Константы для пагинации ---
THREADS_PER_PAGE = 10

@dp.message(Command("threads"))
async def cmd_threads(message: types.Message, board_id: str | None):
    """Выводит постраничный список активных тредов."""
    if not board_id or board_id not in THREAD_BOARDS:
        try: await message.delete()
        except Exception: pass
        return

    b_data = board_data[board_id]
    lang = 'en' if board_id == 'int' else 'ru'
    
    threads_data = b_data.get('threads_data', {})
    active_threads = {k: v for k, v in threads_data.items() if not v.get('is_archived')}

    if not active_threads:
        # --- НАЧАЛО ИЗМЕНЕНИЙ: Безопасное получение текста ---
        empty_phrases = thread_messages.get(lang, {}).get('threads_list_empty', [])
        default_empty_text = "No active threads right now. Be the first to /create one!" if lang == 'en' else "Сейчас нет активных тредов. Стань первым, /create!"
        empty_text = random.choice(empty_phrases) if empty_phrases else default_empty_text
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---
        await message.answer(empty_text)
        await message.delete()
        return

    sorted_threads = sorted(
        active_threads.items(),
        key=lambda item: item[1].get('last_activity_at', 0),
        reverse=True
    )
    
    user_s = b_data['user_state'].setdefault(message.from_user.id, {})
    user_s['sorted_threads_cache'] = [tid for tid, _ in sorted_threads]

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

    # --- НАЧАЛО ИЗМЕНЕНИЙ: Безопасное получение текстов ---
    tm_lang = thread_messages.get(lang, {})
    header_phrases = tm_lang.get('threads_list_header', ["Active Threads:"])
    header = random.choice(header_phrases)

    item_template = tm_lang.get('thread_list_item', "{index}. {title} ({posts_count} posts, last: {last_activity})")
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---
    
    lines, keyboard_buttons = [], []
    now_ts = time.time()

    for i, thread_id in enumerate(page_thread_ids):
        thread_info = threads_data.get(thread_id)
        if not thread_info: continue

        index, title = start_index + i + 1, thread_info.get('title', 'No Title')
        posts_count = len(thread_info.get('posts', []))
        
        time_diff = now_ts - thread_info.get('last_activity_at', 0)
        if time_diff < 60: last_activity_str = f"{int(time_diff)}s"
        elif time_diff < 3600: last_activity_str = f"{int(time_diff / 60)}m"
        else: last_activity_str = f"{int(time_diff / 3600)}h"

        lines.append(item_template.format(index=index, title=title, posts_count=posts_count, last_activity=last_activity_str))
        keyboard_buttons.append([InlineKeyboardButton(text=f"{index}. {title[:40]}", callback_data=f"enter_thread_{thread_id}")])

    current_location = user_s.get('location', 'main')
    if current_location != 'main' and threads_data.get(current_location):
        # --- НАЧАЛО ИЗМЕНЕНИЙ: Безопасное получение текста ---
        history_phrases = tm_lang.get('show_history_button', ["Show History"])
        history_button_text = random.choice(history_phrases)
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---
        keyboard_buttons.append([InlineKeyboardButton(text=history_button_text, callback_data=f"show_history_{current_location}")])

    pagination_row = []
    total_pages = (len(sorted_thread_ids) + THREADS_PER_PAGE - 1) // THREADS_PER_PAGE
    if page > 0:
        # --- НАЧАЛО ИЗМЕНЕНИЙ: Безопасное получение текста ---
        prev_phrases = tm_lang.get('prev_page_button', ["< Previous"])
        prev_text = random.choice(prev_phrases)
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---
        pagination_row.append(InlineKeyboardButton(text=prev_text, callback_data=f"threads_page_{page - 1}"))
    if page < total_pages - 1:
        # --- НАЧАЛО ИЗМЕНЕНИЙ: Безопасное получение текста ---
        next_phrases = tm_lang.get('next_page_button', ["Next >"])
        next_text = random.choice(next_phrases)
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---
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
    """
    Отправляет уведомление о "счастливом" посте в канал архивов.
    Для медиа используются текстовые заглушки.
    (ВЕРСИЯ 7.0 - ВОЗВРАТ К ЗАГЛУШКАМ ДЛЯ СТАБИЛЬНОСТИ)
    """
    archive_bot = GLOBAL_BOTS.get(ARCHIVE_POSTING_BOT_ID)
    if not archive_bot:
        print(f"⛔ Ошибка: бот для постинга ('{ARCHIVE_POSTING_BOT_ID}') не найден.")
        return
        
    try:
        config = SPECIAL_NUMERALS_CONFIG[level]
        emoji = random.choice(config['emojis'])
        label = config['label'].upper()
        board_name = BOARD_CONFIG.get(board_id, {}).get('name', board_id)

        header = f"{emoji} <b>{label} #{post_num}</b> {emoji}\n\n<b>Доска:</b> {board_name}\n"
        
        # --- НАЧАЛО ИЗМЕНЕНИЙ: Логика на основе текстовых заглушек ---
        
        text_content = ""
        content_type_str = str(content.get("type", "")).split('.')[-1].lower()

        if content_type_str == 'text':
            text_content = content.get('text', '')
        else:
            text_content = content.get('caption', '') or ''
            
            media_placeholders = {
                'photo': '[Фото]', 'video': '[Видео]', 'animation': '[GIF]', 'sticker': '[Стикер]',
                'document': '[Документ]', 'audio': '[Аудио]', 'voice': '[Голосовое сообщение]',
                'video_note': '[Кружок]', 'media_group': '[Медиа-группа]'
            }
            placeholder = media_placeholders.get(content_type_str, f'[{content_type_str}]')
            
            text_content = f"{placeholder}\n{text_content}".strip()
        
        final_text = f"{header}\n{text_content}"
        if len(final_text) > 4096:
            final_text = final_text[:4093] + "..."

        async def send_with_retry():
            try:
                await archive_bot.send_message(
                    chat_id=ARCHIVE_CHANNEL_ID,
                    text=final_text,
                    parse_mode="HTML"
                )
            except TelegramRetryAfter as e:
                print(f"⚠️ Попали на лимит API при отправке счастливого поста #{post_num}. Ждем {e.retry_after} сек...")
                await asyncio.sleep(e.retry_after + 1)
                await send_with_retry()
            except TelegramBadRequest as e:
                print(f"❌ BadRequest при отправке счастливого поста #{post_num}: {e}")

        await send_with_retry()
        print(f"✅ Уведомление о счастливом посте #{post_num} ({label}) отправлено в канал.")
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---

    except Exception as e:
        import traceback
        print(f"⛔ Не удалось отправить счастливый пост #{post_num} в канал: {e}")
        traceback.print_exc()


def _get_thread_entry_keyboard(board_id: str, show_history_button: bool = False) -> InlineKeyboardMarkup:
    """
    Создает и возвращает инлайн-клавиатуру для сообщения о входе в тред.
    """
    lang = 'en' if board_id == 'int' else 'ru'

    if lang == 'en':
        button_good_thread_text = "👍 Good Thread"
        button_leave_text = "Leave Thread"
        button_history_text = "📜 Full History"
    else:
        button_good_thread_text = "👍 Годный тред"
        button_leave_text = "Выйти из треда"
        button_history_text = "📜 Вся летопись"

    # Базовая клавиатура
    keyboard_layout = [
        [
            InlineKeyboardButton(text=button_good_thread_text, callback_data="thread_like_placeholder"),
            InlineKeyboardButton(text=button_leave_text, callback_data="leave_thread")
        ]
    ]

    # Если флаг show_history_button равен True, добавляем кнопку "Вся летопись"
    if show_history_button:
        keyboard_layout.append([
            InlineKeyboardButton(text=button_history_text, callback_data="show_current_thread_history")
        ])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_layout)
    return keyboard

@dp.callback_query(F.data == "show_current_thread_history")
async def cq_show_current_thread_history(callback: types.CallbackQuery, board_id: str | None):
    """
    Обрабатывает нажатие на кнопку "Вся летопись" внутри треда.
    """
    if not board_id or board_id not in THREAD_BOARDS:
        await callback.answer()
        return

    user_id = callback.from_user.id
    b_data = board_data[board_id]
    user_s = b_data.get('user_state', {}).get(user_id, {})
    location = user_s.get('location', 'main')

    if location == 'main':
        await callback.answer("You are not in a thread.", show_alert=True)
        return

    callback.data = f"show_history_{location}"
    await cq_thread_history(callback, board_id)

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

def _get_leave_thread_keyboard(board_id: str) -> InlineKeyboardMarkup:
    """
    Создает и возвращает инлайн-клавиатуру для сообщения о выходе из треда.
    """
    lang = 'en' if board_id == 'int' else 'ru'
    
    button_text = "View Threads" if lang == 'en' else "Список тредов"

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=button_text, callback_data="show_active_threads")]
    ])
    return keyboard

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

def _sync_generate_thread_archive(board_id: str, thread_id: str, thread_info: dict, posts_data: list[dict]) -> str | None:
    """
    Синхронная, потокобезопасная функция для генерации HTML-архива.
    Работает только с переданными ей данными постов.
    """
    try:
        title = escape_html(thread_info.get('title', 'Без названия'))
        filepath = os.path.join(DATA_DIR, f"archive_{board_id}_{thread_id}.html")

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
        
        # --- НАЧАЛО ИЗМЕНЕНИЙ: Итерация по переданному списку, а не по глобальному хранилищу ---
        for post_data in posts_data:
            content = post_data.get('content', {})
            post_num = content.get('post_num', 'N/A')
            # Преобразуем timestamp из строки ISO обратно в datetime для форматирования
            timestamp_str = post_data.get('timestamp', '')
            try:
                timestamp_dt = datetime.fromisoformat(timestamp_str)
                timestamp_formatted = timestamp_dt.strftime('%Y-%m-%d %H:%M:%S UTC')
            except (ValueError, TypeError):
                timestamp_formatted = "N/A"
            
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
                f'                <b>Пост №{post_num}</b> - {timestamp_formatted}\n'
                '            </div>\n'
                '            <div class="post-content">\n'
                f'                {reply_html}{post_body}\n'
                '            </div>\n'
                '        </div>\n'
            )
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---
            
        html_parts.extend(['    </div>\n', '</body>\n', '</html>\n'])
        final_html_content = "".join(html_parts)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(final_html_content)
        
        print(f"✅ [{board_id}] Архив для треда {thread_id} сохранен в {filepath}")
        return filepath

    except Exception as e:
        import traceback
        print(f"⛔ [{board_id}] Ошибка генерации архива для треда {thread_id}: {e}\n{traceback.format_exc()}")
        return None
        
async def archive_thread(bots: dict[str, Bot], board_id: str, thread_id: str, thread_info: dict):
    """Асинхронная обертка для генерации архива треда с защитой от гонки состояний."""
    
    # 1. Захватываем блокировку и создаем безопасную копию данных постов
    posts_data_copy = []
    async with storage_lock:
        post_nums = thread_info.get('posts', [])
        for post_num in post_nums:
            post_data = messages_storage.get(post_num)
            if post_data:
                # Создаем копию, пригодную для передачи в другой поток
                data_copy = {
                    'content': post_data.get('content', {}).copy(),
                    'timestamp': post_data.get('timestamp', datetime.now(UTC)).isoformat()
                }
                posts_data_copy.append(data_copy)
    
    # 2. Передаем безопасную копию в другой поток для генерации HTML и записи
    loop = asyncio.get_running_loop()
    filepath = await loop.run_in_executor(
        save_executor,
        _sync_generate_thread_archive,
        board_id, thread_id, thread_info, posts_data_copy
    )
    
    # 3. Если файл успешно создан, отправляем его в канал
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

                # Этап 1.3: Физическое удаление тредов (из-за переполнения) из состояния
                if threads_to_delete:
                    for thread_id in threads_to_delete:
                        threads_data.pop(thread_id, None)
                    print(f"🧹 [{board_id}] Удалено {len(threads_to_delete)} старых тредов из состояния.")
                
                # --- НАЧАЛО ИЗМЕНЕНИЙ: Очистка старых заархивированных тредов ---
                threads_to_purge = []
                now_ts = time.time()
                ARCHIVE_LIFETIME_SECONDS = 24 * 3600  # 24 часа

                for thread_id, thread_info in threads_data.items():
                    if thread_info.get('is_archived'):
                        last_activity = thread_info.get('last_activity_at', 0)
                        if (now_ts - last_activity) > ARCHIVE_LIFETIME_SECONDS:
                            threads_to_purge.append(thread_id)
                
                if threads_to_purge:
                    for thread_id in threads_to_purge:
                        threads_data.pop(thread_id, None)
                    print(f"🧹 [{board_id}] Очищено {len(threads_to_purge)} старых заархивированных тредов из памяти.")
                # --- КОНЕЦ ИЗМЕНЕНИЙ ---
        
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

async def memory_logger_task():
    """Фоновая задача для периодического логгирования состояния памяти."""
    # Начальная задержка, чтобы не нагружать систему при старте
    await asyncio.sleep(80) #1.5 минут
    
    while True:
        try:
            await log_memory_summary()
            # Периодичность логгирования - 25 минут
            await asyncio.sleep(1500)
        except Exception as e:
            # В случае ошибки в самом логгере, ждем дольше, чтобы не спамить в консоль
            print(f"Критическая ошибка в memory_logger_task: {e}")
            await asyncio.sleep(600)

@dp.message(ThreadCreateStates.waiting_for_op_post, F.text)
async def process_op_post_text(message: types.Message, state: FSMContext, board_id: str | None):
    """
    Ловит текст для ОП-поста, очищает его и переводит на этап подтверждения.
    """
    if not board_id: return
    lang = 'en' if board_id == 'int' else 'ru'

    # --- НАЧАЛО ИЗМЕНЕНИЙ: Санитизация HTML перед сохранением в FSM ---
    raw_html_text = message.html_text
    safe_html_text = sanitize_html(raw_html_text)
    await state.update_data(op_post_text=safe_html_text)
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---

    await state.set_state(ThreadCreateStates.waiting_for_confirmation)

    if lang == 'en':
        confirmation_text = f"You want to create a thread with this opening post:\n\n---\n{safe_html_text}\n---\n\nCreate?"
        button_create = "✅ Create Thread"
        button_edit = "✏️ Edit Text"
    else:
        confirmation_text = f"Вы хотите создать тред с таким ОП-постом:\n\n---\n{safe_html_text}\n---\n\nСоздаем?"
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

    await state.set_state(ThreadCreateStates.waiting_for_op_post)

    # --- НАЧАЛО ИЗМЕНЕНИЙ: Безопасное получение текста ---
    prompt_phrases = thread_messages.get(lang, {}).get('create_prompt_op_post', [])
    default_prompt = "Please send the text for your opening post." if lang == 'en' else "Отправьте текст для вашего ОП-поста."
    prompt_text = random.choice(prompt_phrases) if prompt_phrases else default_prompt
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---
    
    await callback.answer()
    
    try:
        await callback.message.answer(prompt_text)
        await callback.message.delete()
    except (TelegramForbiddenError, TelegramBadRequest):
        pass

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
    tm_lang = thread_messages.get(lang, {})
    
    user_s = b_data['user_state'].setdefault(user_id, {})
    now_ts = time.time()
    last_history_req = user_s.get('last_history_request', 0)
    
    if now_ts - last_history_req < THREAD_HISTORY_COOLDOWN:
        cooldown_phrases = tm_lang.get('history_cooldown', ["Cooldown! Wait {minutes} min."])
        cooldown_msg = random.choice(cooldown_phrases).format(minutes=str(THREAD_HISTORY_COOLDOWN // 60))
        await callback.answer(cooldown_msg, show_alert=True)
        return

    thread_info = b_data.get('threads_data', {}).get(thread_id)
    if not thread_info:
        not_found_phrases = tm_lang.get('thread_not_found', ["Thread not found."])
        not_found_msg = random.choice(not_found_phrases)
        await callback.answer(not_found_msg, show_alert=True)
        return

    user_s['last_history_request'] = now_ts
    await callback.answer("⏳ Загружаю историю...")

    # Временно сбрасываем счетчик просмотренных постов для этого треда до 0
    temp_user_state = user_s.copy()
    temp_user_state.setdefault('last_seen_threads', {})[thread_id] = 0
    b_data['user_state'][user_id] = temp_user_state

    # Вызываем функцию отправки, которая теперь загрузит ВСЕ посты
    await send_missed_messages(callback.bot, board_id, user_id, thread_id)

async def _enter_thread_logic(bot: Bot, board_id: str, user_id: int, thread_id: str, message_to_delete: types.Message | None = None):
    """
    Универсальная и правильная логика для входа пользователя в тред.
    Вызывается как из cmd_start, так и из cq_enter_thread.
    """
    b_data = board_data[board_id]
    lang = 'en' if board_id == 'int' else 'ru'
    tm_lang = thread_messages.get(lang, {})
    
    threads_data = b_data.get('threads_data', {})
    if thread_id not in threads_data:
        return

    user_s = b_data['user_state'].setdefault(user_id, {})
    
    now_ts = time.time()
    last_switch = user_s.get('last_location_switch', 0)
    if now_ts - last_switch < LOCATION_SWITCH_COOLDOWN:
        cooldown_phrases = tm_lang.get('location_switch_cooldown', ["Too fast! Please wait a moment."])
        cooldown_msg = random.choice(cooldown_phrases)
        try:
            sent_msg = await bot.send_message(user_id, cooldown_msg)
            asyncio.create_task(delete_message_after_delay(sent_msg, 5))
        except (TelegramForbiddenError, TelegramBadRequest):
            pass
        return

    current_location = user_s.get('location', 'main')
    if current_location == thread_id:
        return
        
    if current_location == 'main':
        user_s['last_seen_main'] = state.get('post_counter', 0)

    user_s['location'] = thread_id
    user_s['last_location_switch'] = now_ts
    threads_data[thread_id].setdefault('subscribers', set()).add(user_id)
    
    if message_to_delete:
        try:
            await message_to_delete.delete()
        except TelegramBadRequest:
            pass

    # Получаем ДВА значения из обновленной функции
    was_missed, show_history_button = await send_missed_messages(bot, board_id, user_id, thread_id)
    
    # Этот блок выполняется, только если send_missed_messages НЕ отправила финальное сообщение
    if not was_missed:
        thread_title = threads_data[thread_id].get('title', '...')
        seen_threads = user_s.setdefault('last_seen_threads', {})
        
        if thread_id not in seen_threads:
            prompt_phrases = tm_lang.get('enter_thread_prompt', [f"Entered thread: {thread_title}"])
            response_text = random.choice(prompt_phrases).format(title=thread_title)
        else:
            success_phrases = tm_lang.get('enter_thread_success', [f"Re-entered thread: {thread_title}"])
            response_text = random.choice(success_phrases).format(title=thread_title)
        
        # Передаем флаг (он будет False), чтобы сгенерировать клавиатуру без кнопки "Вся летопись"
        entry_keyboard = _get_thread_entry_keyboard(board_id, show_history_button)
        try:
            await bot.send_message(user_id, response_text, reply_markup=entry_keyboard, parse_mode="HTML")
        except (TelegramForbiddenError, TelegramBadRequest):
            pass
        
    await _send_op_commands_info(bot, user_id, board_id)

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
    
    # --- НАЧАЛО ИЗМЕНЕНИЙ: Вся логика заменена на вызов одной функции ---
    await callback.answer() # Отвечаем на колбэк, чтобы кнопка перестала "грузиться"

    # ВЫЗЫВАЕМ НОВУЮ УНИВЕРСАЛЬНУЮ ФУНКЦИЮ
    await _enter_thread_logic(
        bot=callback.bot,
        board_id=board_id,
        user_id=user_id,
        thread_id=thread_id,
        message_to_delete=callback.message
    )
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---
    
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
        await callback.answer()
        try:
            await callback.message.delete()
        except TelegramBadRequest:
            pass
        return

    thread_id = current_location
    thread_info = b_data.get('threads_data', {}).get(thread_id)
    if thread_info:
        last_thread_post = thread_info.get('posts', [0])[-1] if thread_info.get('posts') else 0
        user_s.setdefault('last_seen_threads', {})[thread_id] = last_thread_post
    
    user_s['location'] = 'main'
    user_s['last_location_switch'] = time.time()
    
    response_text = random.choice(thread_messages[lang]['leave_thread_success'])
    # --- НАЧАЛО ИЗМЕНЕНИЙ: Добавлена клавиатура ---
    leave_keyboard = _get_leave_thread_keyboard(board_id)
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---
    
    await callback.answer()
    try:
        # --- НАЧАЛО ИЗМЕНЕНИЙ: Клавиатура передается в .answer() ---
        await callback.message.answer(response_text, reply_markup=leave_keyboard)
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---
        await callback.message.delete()
    except (TelegramForbiddenError, TelegramBadRequest):
        pass

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

    thread_id = current_location
    thread_info = b_data.get('threads_data', {}).get(thread_id)
    if thread_info:
        last_thread_post = thread_info.get('posts', [0])[-1] if thread_info.get('posts') else 0
        user_s.setdefault('last_seen_threads', {})[thread_id] = last_thread_post

    user_s['location'] = 'main'
    user_s['last_location_switch'] = now_ts
    
    # Сначала удаляем команду, чтобы избежать "двойных" сообщений
    await message.delete()
    
    # Подгружаем пропущенные сообщения с доски
    await send_missed_messages(message.bot, board_id, user_id, 'main')
    
    # --- НАЧАЛО ИЗМЕНЕНИЙ: Добавлена клавиатура к сообщению о выходе ---
    response_text = random.choice(thread_messages[lang]['leave_thread_success'])
    leave_keyboard = _get_leave_thread_keyboard(board_id)
    await message.answer(response_text, reply_markup=leave_keyboard)
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---

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


async def check_anime_cmd_cooldown(message: types.Message, board_id: str) -> bool:
    """
    Проверяет и применяет кулдаун для команд /fap, /gatari, /hent на уровне доски.
    Возвращает True, если команду можно выполнить, иначе False.
    """
    current_time = time.time()
    async with anime_cmd_lock:
        b_data = board_data[board_id]
        last_usage = b_data.get('last_anime_cmd_time', 0)

        if current_time - last_usage < ANIME_CMD_COOLDOWN:
            cooldown_msg = random.choice(ANIME_CMD_COOLDOWN_PHRASES)
            try:
                sent_msg = await message.answer(cooldown_msg)
                asyncio.create_task(delete_message_after_delay(sent_msg, 5))
            # ИЗМЕНЕНИЕ 1: Замена Exception на конкретные исключения Telegram
            except (TelegramBadRequest, TelegramForbiddenError) as e:
                # Логируем ошибку, чтобы понимать, почему сообщение не отправилось
                print(f"Could not send cooldown message to chat {message.chat.id}: {e}")
                pass
            
            try:
                await message.delete()
            # ИЗМЕНЕНИЕ 2: Замена Exception на конкретное исключение Telegram
            except TelegramBadRequest as e:
                # Ошибка может возникнуть, если сообщение уже удалено, это нормально
                print(f"Could not delete command message in chat {message.chat.id}: {e}")
                pass
            
            return False
        
        b_data['last_anime_cmd_time'] = current_time
        return True


async def _process_anime_command(
    message: types.Message,
    board_id: str,
    image_fetcher: Callable[[], Awaitable[Optional[str]]],
    fail_texts: dict[str, str]
):
    """
    Унифицированный обработчик для команд, отправляющих изображения с API.
    
    :param message: Объект сообщения от Aiogram.
    :param board_id: ID текущей доски.
    :param image_fetcher: Асинхронная функция для получения URL изображения.
    :param fail_texts: Словарь с текстами ошибок для разных языков {'ru': '...', 'en': '...'}.
    """
    # 1. Общая проверка кулдауна
    if not await check_anime_cmd_cooldown(message, board_id):
        return

    user_id = message.from_user.id
    b_data = board_data[board_id]

    # 2. Общие проверки пользователя (бан, мут)
    if user_id in b_data['users']['banned'] or \
       (b_data['mutes'].get(user_id) and b_data['mutes'][user_id] > datetime.now(UTC)):
        try:
            await message.delete()
        except TelegramBadRequest:
            pass
        return

    # 3. Получение изображения с помощью переданной функции
    image_url = await image_fetcher()
    
    # 4. Удаление сообщения с командой в любом случае
    try:
        await message.delete()
    except TelegramBadRequest:
        pass

    # 5. Общая обработка случая, когда изображение не найдено
    if not image_url:
        lang = 'en' if board_id == 'int' else 'ru'
        fail_text = fail_texts.get(lang, "Could not get an image.")
        try:
            error_msg = await message.answer(fail_text)
            asyncio.create_task(delete_message_after_delay(error_msg, 10))
        except (TelegramForbiddenError, TelegramBadRequest):
            pass
        return

    # 6. Общая подготовка и отправка поста
    is_shadow_muted = (user_id in b_data['shadow_mutes'] and
                       b_data['shadow_mutes'][user_id] > datetime.now(UTC))

    # Унифицированное определение типа контента
    content_type = 'animation' if image_url.lower().endswith('.gif') else 'photo'
    
    content = {
        'type': content_type,
        'image_url': image_url,
        'caption': '' # Подпись не нужна
    }

    # Вызов общего обработчика постов
    await process_new_post(
        bot_instance=message.bot,
        board_id=board_id,
        user_id=user_id,
        content=content,
        reply_to_post=None,
        is_shadow_muted=is_shadow_muted
    )


@dp.message(Command("gatari", "monogatari"))
async def cmd_monogatari(message: types.Message, board_id: str | None):
    """Отправляет в чат SFW-изображение из серии Monogatari."""
    if not board_id:
        return

    fail_texts = {
        'ru': "Не удалось получить изображение Monogatari. Попробуйте позже.",
        'en': "Sorry, couldn't fetch a Monogatari image right now. Please try again later."
    }
    
    await _process_anime_command(
        message=message,
        board_id=board_id,
        image_fetcher=get_monogatari_image,
        fail_texts=fail_texts
    )

@dp.message(Command("hent", "fap"))
async def cmd_hentai(message: types.Message, board_id: str | None):
    """Отправляет в чат случайное SFW/NSFW аниме-изображение или GIF."""
    if not board_id:
        return
        
    fail_texts = {
        'ru': "Не удалось получить изображение. Попробуйте позже.",
        'en': "Sorry, couldn't fetch an image right now. Please try again later."
    }

    await _process_anime_command(
        message=message,
        board_id=board_id,
        image_fetcher=get_random_anime_image,
        fail_texts=fail_texts
    )

@dp.message(Command("nsfw"))
async def cmd_nsfw(message: types.Message, board_id: str | None):
    """Отправляет в чат гарантированно NSFW-изображение."""
    if not board_id:
        return
        
    fail_texts = {
        'ru': "Не удалось получить NSFW-изображение. Попробуйте позже.",
        'en': "Sorry, couldn't fetch an NSFW image right now. Please try again later."
    }

    await _process_anime_command(
        message=message,
        board_id=board_id,
        image_fetcher=get_nsfw_anime_image,
        fail_texts=fail_texts
    )

    
@dp.message(Command("deanon"))
async def cmd_deanon(message: Message, board_id: str | None):
    if not board_id: return
    
    current_time = time.time()
    async with deanon_lock:
        # --- НАЧАЛО ИЗМЕНЕНИЙ ---
        b_data = board_data[board_id]
        if current_time - b_data['last_deanon_time'] < DEANON_COOLDOWN:
            cooldown_msg = random.choice(DEANON_COOLDOWN_PHRASES)
            try:
                sent_msg = await message.answer(cooldown_msg)
                asyncio.create_task(delete_message_after_delay(sent_msg, 5))
            except Exception: pass
            await message.delete()
            return
        b_data['last_deanon_time'] = current_time
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---
    
    lang = 'en' if board_id == 'int' else 'ru'
    if not message.reply_to_message:
        reply_text = "⚠️ Reply to a message to de-anonymize!" if lang == 'en' else "⚠️ Ответь на сообщение для деанона!"
        await message.answer(reply_text)
        await message.delete()
        return

    user_id = message.from_user.id
    # b_data уже определена выше, в блоке lock
    user_location = 'main'
    if board_id in THREAD_BOARDS:
        user_location = b_data.get('user_state', {}).get(user_id, {}).get('location', 'main')

    original_author_id = None
    target_post = None
    reply_info = {}

    async with storage_lock:
        target_chat_id = message.reply_to_message.chat.id
        target_mid = message.reply_to_message.message_id
        target_post = message_to_post.get((target_chat_id, target_mid))
        
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
        
    deanon_text = generate_deanon_info(lang=lang)
    header_text = "### DEANON ###" if lang == 'en' else "### ДЕАНОН ###"

    content = {"type": "text", "header": header_text, "text": deanon_text, "reply_to_post": target_post}

    if board_id in THREAD_BOARDS and user_location != 'main':
        thread_id = user_location
        thread_info = b_data.get('threads_data', {}).get(thread_id)
        if thread_info and not thread_info.get('is_archived'):
            _, pnum = await format_header(board_id)
            content['post_num'] = pnum
            content['header'] = await format_thread_post_header(board_id, len(thread_info.get('posts', [])) + 1, 0, thread_info)

            async with storage_lock:
                messages_storage[pnum] = {'author_id': 0, 'timestamp': datetime.now(UTC), 'content': content, 'board_id': board_id, 'thread_id': thread_id}
                thread_info['posts'].append(pnum)
                thread_info['last_activity_at'] = time.time()
            
            await message_queues[board_id].put({
                "recipients": thread_info.get('subscribers', set()), "content": content, "post_num": pnum,
                "board_id": board_id, "thread_id": thread_id
            })
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
    # ИЗМЕНЕНИЕ 3: Замена Exception на конкретное исключение Telegram
    except TelegramBadRequest:
        # Это ожидаемое поведение, если сообщение уже было удалено
        # вручную или другим процессом. Просто игнорируем.
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
    user_id = message.from_user.id

    # --- НАЧАЛО ИЗМЕНЕНИЙ: Проверка на shadow_mute без уведомления ---
    if user_id in b_data['shadow_mutes'] and b_data['shadow_mutes'][user_id] > datetime.now(UTC):
        try:
            # Просто молча удаляем команду
            await message.delete()
        except (TelegramBadRequest, TelegramForbiddenError):
            pass
        return  # И выходим, не давая активировать режим
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---

    if not await check_cooldown(message, board_id):
        return

    b_data['suka_blyat_mode'] = True
    b_data['zaputin_mode'] = False
    b_data['slavaukraine_mode'] = False
    b_data['anime_mode'] = False
    b_data['last_mode_activation'] = datetime.now(UTC)

    header = "### Админ ###"
    _, pnum = await format_header(board_id)

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

    # --- НАЧАЛО ИЗМЕНЕНИЙ: Обновление callback_data ---
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Статистика доски", callback_data=f"stats_{board_id}")],
        [InlineKeyboardButton(text="🚫 Список ограничений", callback_data=f"restrictions_{board_id}")],
        [InlineKeyboardButton(text="💾 Сохранить ВСЕ", callback_data="save_all")],
    ])
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---
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


@dp.callback_query(F.data.startswith("restrictions_"))
async def admin_restrictions_board(callback: types.CallbackQuery):
    """Выводит единый список банов, мутов и теневых мутов."""
    board_id = callback.data.split("_")[1]
    if not is_admin(callback.from_user.id, board_id):
        await callback.answer("Отказано в доступе", show_alert=True)
        return

    b_data = board_data[board_id]
    now = datetime.now(UTC)
    text_parts = [f"<b>Список ограничений на доске {BOARD_CONFIG[board_id]['name']}:</b>"]

    # --- 1. Сбор забаненных ---
    banned_users = b_data['users']['banned']
    if banned_users:
        banned_list = "\n".join([f"  • ID <code>{uid}</code>" for uid in sorted(list(banned_users))])
        text_parts.append(f"\n<u>🚫 Забанены навсегда:</u>\n{banned_list}")
    
    # --- 2. Сбор замученных ---
    active_mutes = {uid: expiry for uid, expiry in b_data['mutes'].items() if expiry > now}
    if active_mutes:
        mute_lines = []
        # Сортируем по времени окончания мута
        for uid, expiry in sorted(active_mutes.items(), key=lambda item: item[1]):
            remaining = expiry - now
            # Форматируем оставшееся время
            hours, remainder = divmod(remaining.total_seconds(), 3600)
            minutes, _ = divmod(remainder, 60)
            time_left_str = f"{int(hours)}ч {int(minutes)}м"
            mute_lines.append(f"  • ID <code>{uid}</code> (осталось: {time_left_str})")
        
        mutes_list = "\n".join(mute_lines)
        text_parts.append(f"\n<u>🔇 В муте:</u>\n{mutes_list}")

    # --- 3. Сбор в теневом муте ---
    active_shadow_mutes = {uid: expiry for uid, expiry in b_data['shadow_mutes'].items() if expiry > now}
    if active_shadow_mutes:
        shadow_mute_lines = []
        for uid, expiry in sorted(active_shadow_mutes.items(), key=lambda item: item[1]):
            remaining = expiry - now
            hours, remainder = divmod(remaining.total_seconds(), 3600)
            minutes, _ = divmod(remainder, 60)
            time_left_str = f"{int(hours)}ч {int(minutes)}м"
            shadow_mute_lines.append(f"  • ID <code>{uid}</code> (осталось: {time_left_str})")

        shadow_mutes_list = "\n".join(shadow_mute_lines)
        text_parts.append(f"\n<u>👻 В теневом муте:</u>\n{shadow_mutes_list}")
        
    # --- Финальная сборка ---
    if len(text_parts) == 1:
        final_text = f"На доске {BOARD_CONFIG[board_id]['name']} нет активных ограничений."
    else:
        final_text = "\n".join(text_parts)

    try:
        await callback.message.edit_text(final_text, parse_mode="HTML")
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
            print(f"Ошибка обновления списка ограничений: {e}")
            
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
    async with media_group_creation_lock:
        if media_group_id not in current_media_groups:
            is_leader = True
            current_media_groups[media_group_id] = {
                'is_initializing': True,
                'init_event': asyncio.Event() 
            }

    group = current_media_groups.get(media_group_id)
    if not group:
        return
    
    if is_leader:
        try:
            fake_text_message = types.Message(
                message_id=message.message_id, date=message.date, chat=message.chat,
                from_user=message.from_user, content_type='text', text=f"media_group_{media_group_id}"
            )
            if not await check_spam(user_id, fake_text_message, board_id):
                current_media_groups.pop(media_group_id, None) 
                await apply_penalty(message.bot, user_id, 'text', board_id)
                try:
                    await message.delete()
                except TelegramBadRequest:
                    pass
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
                    thread_id = None
                    header, post_num = await format_header(board_id)
            else:
                header, post_num = await format_header(board_id)

            # --- НАЧАЛО ИЗМЕНЕНИЙ: Санитизация HTML в подписи ---
            raw_caption_html = getattr(message, 'caption_html_text', message.caption or "")
            safe_caption_html = sanitize_html(raw_caption_html)
            # --- КОНЕЦ ИЗМЕНЕНИЙ ---

            group.update({
                'board_id': board_id, 'post_num': post_num, 'header': header, 'author_id': user_id,
                'timestamp': datetime.now(UTC), 'media': [], 'caption': safe_caption_html, # <-- ИЗМЕНЕНО
                'reply_to_post': reply_to_post, 'processed_messages': set(),
                'source_message_ids': set(),
                'thread_id': thread_id
            })
            group.pop('is_initializing', None)
        finally:
            if 'init_event' in group:
                group['init_event'].set()
    else:
        if 'init_event' in group:
            await group['init_event'].wait()

        group = current_media_groups.get(media_group_id)
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


async def _run_background_task(task_coro: Awaitable[Any], task_name: str):
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
    
    tasks_to_run = {
        "auto_backup": auto_backup(),
        "message_broadcaster": message_broadcaster(bots),
        "conan_roaster": conan_roaster(
            state, messages_storage, post_to_messages, message_to_post,
            message_queues, format_header, board_data, storage_lock
        ),
        "motivation_broadcaster": motivation_broadcaster(),
        "help_broadcaster": help_broadcaster(),
        "auto_memory_cleaner": auto_memory_cleaner(),
        "board_statistics_broadcaster": board_statistics_broadcaster(),
        "thread_lifecycle_manager": thread_lifecycle_manager(bots),
        "thread_notifier": thread_notifier(),
        "thread_activity_monitor": thread_activity_monitor(bots),
        "memory_logger_task": memory_logger_task() # <-- ИЗМЕНЕНИЕ
    }

    tasks = [
        asyncio.create_task(_run_background_task(coro, name))
        for name, coro in tasks_to_run.items()
    ]
    
    print(f"✓ Background tasks started: {len(tasks)}")
    return tasks

async def initialize_bots() -> tuple[dict[str, Bot], AiohttpSession]:
    """Создает и возвращает словарь с экземплярами ботов и общую сессию."""
    from aiogram.client.session.aiohttp import AiohttpSession

    session = AiohttpSession(timeout=60)
    default_properties = DefaultBotProperties(parse_mode="HTML")
    
    bots_temp = {}
    for board_id, config in BOARD_CONFIG.items():
        token = config.get("token")
        if token:
            # --- НАЧАЛО ИЗМЕНЕНИЙ: УБИРАЕМ ПРОВЕРКУ ТОКЕНА await bot.get_me() ---
            # Просто создаем объект бота. Проверка токена произойдет автоматически
            # при первом же запросе к API, уже внутри запущенного event loop.
            bots_temp[board_id] = Bot(token=token, default=default_properties, session=session)
            # --- КОНЕЦ ИЗМЕНЕНИЙ ---
        else:
            print(f"⚠️ Токен для доски '{board_id}' не найден, пропуск.")
    
    return bots_temp, session

def setup_lifecycle_handlers(loop: asyncio.AbstractEventLoop, bots: list[Bot], healthcheck_site: web.TCPSite | None):
    """Настраивает обработчики сигналов для корректного завершения работы."""
    
    # Создаем lambda-функцию, которая захватывает нужные переменные
    handler = lambda: asyncio.create_task(graceful_shutdown(bots, healthcheck_site))
    
    if hasattr(signal, 'SIGTERM'):
        loop.add_signal_handler(signal.SIGTERM, handler)
    if hasattr(signal, 'SIGINT'):
        loop.add_signal_handler(signal.SIGINT, handler)

async def main():
    """Основная функция запуска и управления жизненным циклом бота."""
    lock_file = "bot.lock"
    current_pid = os.getpid()

    # Управление lock-файлом для предотвращения двойного запуска
    if os.path.exists(lock_file):
        try:
            with open(lock_file, "r") as f: old_pid = int(f.read().strip())
            if old_pid != current_pid:
                os.kill(old_pid, 0)
                print(f"⛔ Бот с PID {old_pid} уже запущен! Завершение работы...")
                sys.exit(1)
        except (IOError, ValueError):
            print("⚠️ Lock-файл поврежден. Удаляю и продолжаю.")
            os.remove(lock_file)
        except OSError:
            print(f"⚠️ Найден устаревший lock-файл от процесса {old_pid}. Удаляю и продолжаю.")
            os.remove(lock_file)

    with open(lock_file, "w") as f: f.write(str(current_pid))

    # Инициализация
    session = None
    healthcheck_site = None
    global GLOBAL_BOTS
    
    try:
        global is_shutting_down
        loop = asyncio.get_running_loop()

        if not restore_backup_on_start():
            print("⛔ Не удалось восстановить состояние. Аварийное завершение.")
            os._exit(1)

        load_state()

        GLOBAL_BOTS, session = await initialize_bots()
        if not GLOBAL_BOTS:
            print("❌ Не найдено ни одного токена бота. Завершение работы.")
            return

        active_bots_list = list(GLOBAL_BOTS.values()) # --- ИЗМЕНЕНИЕ: Получаем список здесь
        print(f"✅ Инициализировано {len(active_bots_list)} ботов: {list(GLOBAL_BOTS.keys())}")
        
        await setup_pinned_messages(GLOBAL_BOTS)
        
        try:
            healthcheck_site = await start_healthcheck()
        except Exception as e:
            print(f"⛔ Не удалось запустить healthcheck сервер: {e}. Продолжаем без него.")
            healthcheck_site = None

        setup_lifecycle_handlers(loop, active_bots_list, healthcheck_site)
        await start_background_tasks(GLOBAL_BOTS)

        print("⏳ Даем 7 секунд на инициализацию перед обработкой сообщений...")
        await asyncio.sleep(7)

        print("🚀 Запускаем polling для всех ботов...")
        await dp.start_polling(
            *active_bots_list, skip_updates=False,
            allowed_updates=dp.resolve_used_update_types(),
            reset_webhook=True, timeout=60
        )

    except Exception as e:
        import traceback
        print(f"🔥 Критическая ошибка в main: {e}\n{traceback.format_exc()}")
    finally:
        if not is_shutting_down:
            # --- ИЗМЕНЕНИЕ: Убедимся, что active_bots_list определен ---
            if 'active_bots_list' not in locals():
                active_bots_list = list(GLOBAL_BOTS.values())
            await graceful_shutdown(active_bots_list, healthcheck_site)
        
        if session:
            await session.close()
            print("✅ Общая HTTP сессия закрыта.")
        
        if os.path.exists(lock_file):
            try:
                with open(lock_file, "r") as f: pid_in_file = int(f.read().strip())
                if pid_in_file == current_pid: os.remove(lock_file)
            except (IOError, ValueError):
                os.remove(lock_file)
                
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("ℹ️ Завершение работы по запросу...")
