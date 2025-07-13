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
BOARDS = ['b', 'po', 'a', 'sex', 'vg']  # Список всех досок

BOT_TOKENS = {
    'b': os.environ.get('BOT_TOKEN'),    # Токен основного бота /b/
    'po': os.environ.get('BOT_TOKEN_PO'),  # Токен для /po/
    'a': os.environ.get('BOT_TOKEN_A'),    # Токен для /a/
    'sex': os.environ.get('BOT_TOKEN_SEX'), # Токен для /sex/
    'vg': os.environ.get('BOT_TOKEN_VG'),   # Токен для /vg/
}

BOARD_INFO = {
    'b': {"name": "/b/", "description": "Бред", "username": "@dvach_chatbot"},
    'po': {"name": "/po/", "description": "Политика", "username": "@dvach_po_chatbot"},
    'a': {"name": "/a/", "description": "Аниме", "username": "@dvach_a_chatbot"},
    'sex': {"name": "/sex/", "description": "Сексач", "username": "@dvach_sex_chatbot"},
    'vg': {"name": "/vg/", "description": "Видеоигры", "username": "@dvach_vg_chatbot"},
}

# Очереди сообщений для каждой доски
message_queues = {board: asyncio.Queue(maxsize=9000) for board in BOARDS}

# ========== Глобальные переменные и настройки ==========
is_shutting_down = False
git_executor = ThreadPoolExecutor(max_workers=1)
send_executor = ThreadPoolExecutor(max_workers=100)
git_semaphore = asyncio.Semaphore(1)
message_queue = asyncio.Queue()
anime_mode = False
zaputin_mode = False
slavaukraine_mode = False
suka_blyat_mode = False
last_suka_blyat = None
suka_blyat_counter = 0
last_mode_activation = None
MODE_COOLDOWN = 3600  # 1 час в секундах
# для проверки одинаковых / коротких сообщений
last_texts: dict[int, deque[str]] = defaultdict(lambda: deque(maxlen=5))

shadow_mutes: dict[int, datetime] = {}  # user_id -> datetime конца тихого мута
# хранит последние 5 Message-объектов пользователя
# Вместо defaultdict используем обычный dict с ручной инициализацией
last_user_msgs = {}
MAX_ACTIVE_USERS_IN_MEMORY = 5000

# Рядом с другими глобальными словарями
spam_violations = defaultdict(dict)  # user_id -> количество нарушений


# Отключаем стандартную обработку сигналов в aiogram
os.environ["AIORGRAM_DISABLE_SIGNAL_HANDLERS"] = "1"


SPAM_RULES = {
    'text': {
        'max_repeats': 3,  # Макс одинаковых текстов подряд
        'min_length': 2,  # Минимальная длина текста
        'window_sec': 15,  # Окно для проверки (сек)
        'max_per_window': 6,  # Макс сообщений в окне
        'penalty': [60, 300, 600]  # Шкала наказаний: [1 мин, 5мин, 10 мин]
    },
    'sticker': {
        'max_per_window': 6,  # 5 стикеров за 15 сек
        'window_sec': 15,
        'penalty': [60, 600, 900]  # 1мин, 10мин, 15 мин
    },
    'animation': {  # Гифки
        'max_per_window': 5,  # 4 гифки за 30 сек
        'window_sec': 24,
        'penalty': [60, 600, 900]  # 1мин, 10мин, 15 мин
    }
}

# Хранилище данных
state = {
    'users_data': {
        'active': set(),
        'banned': set()
    },
    'post_counter': 0,
    'message_counter': {},
    'settings': {  # Добавляем настройки по умолчанию
        'dvach_enabled': False
    }
}
# user_id -> datetime конца мута (UTC)
mutes: dict[int, datetime] = {}
# Временные данные (не сохраняются)
spam_tracker = defaultdict(list)
messages_storage = {}
post_to_messages = {}
message_to_post = {}
last_messages = deque(maxlen=300)
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


async def auto_backup():
    """Автоматическое сохранение каждые 1 ч"""
    while True:
        try:
            await asyncio.sleep(14400)  # 4 ч

            if is_shutting_down:
                break

            # Сохраняем reply_cache
            save_reply_cache()

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

            # Пуш в GitHub
            print("💾 Обновление state.json и reply_cache.json, пушим в GitHub...")
            success = await git_commit_and_push()
            if success:
                print("✅ Бэкап успешно отправлен")
            else:
                print("❌ Не удалось отправить данные в GitHub")

        except Exception as e:
            print(f"❌ Ошибка в auto_backup: {e}")
            # Ждем 1 минуту перед повторной попыткой
            await asyncio.sleep(60)

# Настройка сборщика мусора
gc.set_threshold(
    700, 10, 10)  # Оптимальные настройки для баланса памяти/производительности

async def save_state_and_backup():
    """Сохраняет state.json и reply_cache.json, пушит в GitHub"""
    try:
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
        save_reply_cache()
        print("💾 Обновление state.json и reply_cache.json, пушим в GitHub...")
        return await git_commit_and_push()
    except Exception as e:
        print(f"⛔ Ошибка сохранения state: {e}")
        return False

async def cleanup_old_messages():
    """Очистка постов старше 7 дней"""
    while True:
        await asyncio.sleep(7200)  # Каждые 1 час
        try:
            current_time = datetime.now(UTC)  # Используем UTC вместо MSK
            old_posts = [
                pnum for pnum, data in messages_storage.items()
                if (current_time - data.get('timestamp', current_time)).days > 7
            ]
            for pnum in old_posts:
                messages_storage.pop(pnum, None)
                post_to_messages.pop(pnum, None)
            print(f"Очищено {len(old_posts)} старых постов")
        except Exception as e:
            print(f"Ошибка очистки: {e}")

def get_user_msgs_deque(user_id):
    """Получаем deque для юзера, ограничиваем количество юзеров в памяти"""
    if user_id not in last_user_msgs:
        if len(last_user_msgs) >= MAX_ACTIVE_USERS_IN_MEMORY:
            oldest_user = next(iter(last_user_msgs))
            del last_user_msgs[oldest_user]

        last_user_msgs[user_id] = deque(maxlen=10)

    return last_user_msgs[user_id]

# Конфиг
BOT_TOKEN = os.environ.get('BOT_TOKEN')
ADMINS = {int(x) for x in os.getenv("ADMINS", "").split(",") if x}
SPAM_LIMIT = 14
SPAM_WINDOW = 15
STATE_FILE = 'state.json'
SAVE_INTERVAL = 14400  # секунд
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

def save_reply_cache():
    """Сохраняет кэш ответов только если есть изменения"""
    try:
        # Собираем актуальные данные
        recent_posts = sorted(messages_storage.keys())[-REPLY_CACHE:]
        new_data = {
            "post_to_messages": {
                str(p): post_to_messages[p]
                for p in recent_posts 
                if p in post_to_messages
            },
            "message_to_post": {
                f"{uid}_{mid}": p 
                for (uid, mid), p in message_to_post.items() 
                if p in recent_posts
            },
            "messages_storage_meta": {
                str(p): {
                    "author_id": messages_storage[p].get("author_id", ""),
                    "timestamp": messages_storage[p].get("timestamp", datetime.now(UTC)).isoformat(),
                    "author_msg": messages_storage[p].get("author_message_id")
                }
                for p in recent_posts
            }
        }

        # Проверяем существующий файл
        old_data = {}
        if os.path.exists(REPLY_FILE):
            with open(REPLY_FILE, 'r', encoding='utf-8') as f:
                try:
                    old_data = json.load(f)
                except json.JSONDecodeError:
                    old_data = {}

        # Если данные не изменились - выходим
        if old_data == new_data:
            print("ℹ️ reply_cache.json без изменений")
            return True

        # Сохраняем новые данные
        with open(REPLY_FILE, 'w', encoding='utf-8') as f:
            json.dump(new_data, f, ensure_ascii=False, indent=2)
            print("✅ reply_cache.json обновлен")

        return True

    except Exception as e:
        print(f"⛔ Ошибка сохранения reply_cache: {str(e)[:200]}")
        return False

def load_state():
    global message_to_post, post_to_messages
    if not os.path.exists('state.json'):
        return
    with open('state.json', 'r', encoding='utf-8') as f:
        data = json.load(f)

    state['post_counter'] = data.get('post_counter', 0)
    state['users_data']['active'] = set(data['users_data'].get('active', []))
    state['users_data']['banned'] = set(data['users_data'].get('banned', []))
    state['message_counter'] = data.get('message_counter', {})
    state['settings'] = data.get('settings', {'dvach_enabled': False})  # Добавляем значение по умолчанию

    # Восстанавливаем связи постов если есть
    if 'recent_post_mappings' in data:
        for post_str, messages in data['recent_post_mappings'].items():
            post_to_messages[int(post_str)] = messages
            # Восстанавливаем обратные связи
            for uid, mid in messages.items():
                message_to_post[(uid, mid)] = int(post_str)

    load_reply_cache()

def load_archived_post(post_num):
    """Ищем пост в архивах"""
    for archive_file in glob.glob("archive_*.pkl.gz"):
        with gzip.open(archive_file, "rb") as f:
            data = pickle.load(f)
            if post_num in data:
                return data[post_num]
    return None

def load_reply_cache():
    global message_to_post, post_to_messages
    """Читаем reply_cache.json, восстанавливаем словари"""
    if not os.path.exists(REPLY_FILE):
        return

    try:
        if os.path.getsize(REPLY_FILE) == 0:
            return
        with open(REPLY_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        print(f"reply_cache.json повреждён ({e}), игнорирую")
        return

    message_to_post.clear()
    for key, post_num in data.get("message_to_post", {}).items():
        uid, mid = map(int, key.split("_"))
        message_to_post[(uid, mid)] = post_num

    post_to_messages.clear()
    for p_str, mapping in data.get("post_to_messages", {}).items():
        post_to_messages[int(p_str)] = {
            int(uid): mid
            for uid, mid in mapping.items()
        }

    # Восстановление временных меток в UTC
    for p_str, meta in data.get("messages_storage_meta", {}).items():
        p = int(p_str)
        # Преобразуем строку в datetime с указанием UTC
        if 'timestamp' in meta:
            dt = datetime.fromisoformat(meta['timestamp'])
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)  # Добавляем UTC если нет зоны
            messages_storage[p] = {
                "author_id": meta["author_id"],
                "timestamp": dt,
                "author_message_id": meta.get("author_msg"),
            }

    print(f"reply-cache загружен: {len(post_to_messages)} постов")
    print(f"State: пост-счётчик = {state['post_counter']}, "
          f"активных = {len(state['users_data']['active'])}, "
          f"забаненных = {len(state['users_data']['banned'])}")
    print(f"Reply-cache: постов {len(post_to_messages)}, "
          f"сообщений {len(message_to_post)}")

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
    """Единственная функция очистки памяти - каждые 5 минут"""
    while True:
        await asyncio.sleep(300)  # 5 минут

        MAX_POSTS = 100  # Максимум постов в памяти

        # Чистим все словари с постами
        if len(post_to_messages) > MAX_POSTS:
            recent = sorted(post_to_messages.keys())[-MAX_POSTS:]
            old_posts = set(post_to_messages.keys()) - set(recent)

            for post in old_posts:
                post_to_messages.pop(post, None)
                messages_storage.pop(post, None)

        # Чистим message_to_post от старых
        valid_posts = set(post_to_messages.keys())
        message_to_post_copy = dict(message_to_post)
        for key, post_num in message_to_post_copy.items():
            if post_num not in valid_posts:
                del message_to_post[key]

        # Чистим счетчик сообщений - оставляем топ 100
        if len(state['message_counter']) > 100:
            top_users = sorted(state['message_counter'].items(),
                               key=lambda x: x[1],
                               reverse=True)[:100]
            state['message_counter'] = dict(top_users)

        # Чистим spam_tracker от старых записей
        now = datetime.now(UTC)  # Используем UTC вместо наивного времени
        for user_id in list(spam_tracker.keys()):
            spam_tracker[user_id] = [
                t for t in spam_tracker[user_id]
                if (now - t).total_seconds() < SPAM_WINDOW  # Используем total_seconds
            ]
            if not spam_tracker[user_id]:
                del spam_tracker[user_id]

        # Агрессивная сборка мусора
        for _ in range(3):
            gc.collect()

async def cleanup_old_users():
    """Чистим данные неактивных юзеров раз в час"""
    while True:
        await asyncio.sleep(3600)
        active_users = state['users_data']['active']

        # Считаем неактивных до очистки
        inactive_count = 0
        for user_dict in [
                spam_tracker, last_texts, last_stickers, sticker_times
        ]:
            inactive_users = set(user_dict.keys()) - active_users
            inactive_count = max(inactive_count, len(inactive_users))
            for uid in inactive_users:
                user_dict.pop(uid, None)

        if inactive_count > 0:
            print(
                f"Очистка: удалены данные {inactive_count} неактивных юзеров")

# Периодическая сборка мусора
async def garbage_collector():
    """Принудительная сборка мусора каждые 30 минут"""
    while True:
        await asyncio.sleep(1800)  # 30 минут
        collected = gc.collect()
        if collected > 1000:
            print(f"GC: собрано {collected} объектов")

async def aiogram_memory_cleaner():
    """Очищает кэш aiogram каждые 5 минут"""
    while True:
        await asyncio.sleep(300)  # 5 минут
        # Очищаем большие weakref словари
        cleared = 0
        for obj in gc.get_objects():
            if isinstance(obj, dict) and len(obj) > 700:
                try:
                    sample = next(iter(obj.values()), None) if obj else None
                    if isinstance(sample,
                                  (weakref.ref, weakref.ReferenceType)):
                        obj.clear()
                        cleared += 1
                except:
                    pass

        if cleared > 0:
            collected = gc.collect()
            print(
                f"Очищено {cleared} больших weakref словарей, собрано {collected} объектов"
            )

async def auto_save_state():
    """Автоматическое сохранение состояния каждые 1ч"""
    while True:
        try:
            await asyncio.sleep(14400)

            if is_shutting_down:
                break

            save_reply_cache()

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

            print("✅ Состояние сохранено")
            await git_commit_and_push()

        except Exception as e:
            print(f"❌ Ошибка в auto_save_state: {e}")
            await asyncio.sleep(60)

async def check_spam(user_id: int, msg: Message) -> bool:
    """Проверяет спам с прогрессивным наказанием и сбросом уровня"""
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
    # Инициализация данных пользователя
    if user_id not in spam_violations or not spam_violations[user_id]:
        spam_violations[user_id] = {
            'level': 0,
            'last_reset': now,
            'last_contents': deque(maxlen=4)
        }
    # Сброс уровня, если прошло больше 1 часа
    if (now - spam_violations[user_id]['last_reset']) > timedelta(hours=1):
        spam_violations[user_id] = {
            'level': 0,
            'last_reset': now,
            'last_contents': deque(maxlen=4)
        }
    # Проверка повторяющихся текстов/подписей
    if msg_type == 'text' and content:
        spam_violations[user_id]['last_contents'].append(content)
        # 3 одинаковых подряд
        if len(spam_violations[user_id]['last_contents']) == rules['max_repeats']:
            if len(set(spam_violations[user_id]['last_contents'])) == 1:
                spam_violations[user_id]['level'] = min(
                    spam_violations[user_id]['level'] + 1,
                    len(rules['penalty']) - 1)
                return False
        # Чередование двух текстов
        if len(spam_violations[user_id]['last_contents']) == 4:
            unique = set(spam_violations[user_id]['last_contents'])
            if len(unique) == 2:
                contents = list(spam_violations[user_id]['last_contents'])
                p1 = [contents[0], contents[1]] * 2
                p2 = [contents[1], contents[0]] * 2
                if contents == p1 or contents == p2:
                    spam_violations[user_id]['level'] = min(
                        spam_violations[user_id]['level'] + 1,
                        len(rules['penalty']) - 1)
                    return False

    # Проверка лимита по времени
    window_start = now - timedelta(seconds=rules['window_sec'])
    spam_tracker[user_id] = [t for t in spam_tracker[user_id] if t > window_start]
    spam_tracker[user_id].append(now)
    if len(spam_tracker[user_id]) >= rules['max_per_window']:
        spam_violations[user_id]['level'] = min(
            spam_violations[user_id]['level'] + 1,
            len(rules['penalty']) - 1)
        return False
    return True

async def apply_penalty(user_id: int, msg_type: str):
    """Применяет мут согласно текущему уровню нарушения"""
    rules = SPAM_RULES.get(msg_type, {})
    if not rules:
        return
    level = spam_violations.get(user_id, {}).get('level', 0)
    level = min(level, len(rules.get('penalty', [])) - 1)
    mute_seconds = rules['penalty'][level] if rules.get('penalty') else 30
    mutes[user_id] = datetime.now(UTC) + timedelta(seconds=mute_seconds)
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
        await bot.send_message(
            user_id,
            f"🚫 Эй пидор ты в муте на {time_str} за {violation_type}\n"
            f"Спамишь дальше - получишь бан",
            parse_mode="HTML")
        await send_moderation_notice(user_id, "mute", time_str, 0)
    except Exception as e:
        print(f"Ошибка отправки уведомления о муте: {e}")


def format_header() -> Tuple[str, int]:
    """Форматирование заголовка с учетом режимов"""
    state['post_counter'] += 1
    post_num = state['post_counter']

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

async def delete_user_posts(user_id: int, time_period_minutes: int) -> int:
    """Удаляет ВСЕ сообщения пользователя за период"""
    try:
        time_threshold = datetime.now(UTC) - timedelta(minutes=time_period_minutes)
        posts_to_delete = []
        deleted_messages = 0

        # Находим посты за период
        for post_num, post_data in list(messages_storage.items()):
            post_time = post_data.get('timestamp')
            if not post_time: 
                continue
                
            if (post_data.get('author_id') == user_id and 
                post_time >= time_threshold):
                posts_to_delete.append(post_num)

        # Собираем ВСЕ сообщения для удаления
        messages_to_delete = []
        for post_num in posts_to_delete:
            if post_num in post_to_messages:
                for uid, mid in post_to_messages[post_num].items():
                    messages_to_delete.append((uid, mid))

        # Удаляем каждое сообщение с повторными попытками
        for (uid, mid) in messages_to_delete:
            try:
                for _ in range(3):  # 3 попытки удаления
                    try:
                        await bot.delete_message(uid, mid)
                        deleted_messages += 1
                        break
                    except TelegramBadRequest as e:
                        if "message to delete not found" in str(e):
                            break
                        await asyncio.sleep(0.5)
            except Exception as e:
                print(f"Ошибка удаления {mid} у {uid}: {e}")

        # Удаляем записи из хранилищ
        for post_num in posts_to_delete:
            post_to_messages.pop(post_num, None)
            messages_storage.pop(post_num, None)
            # Удаляем из message_to_post
            global message_to_post
            message_to_post = {k: v for k, v in message_to_post.items() if v != post_num}

        return deleted_messages
    except Exception as e:
        print(f"Ошибка в delete_user_posts: {e}")
        return 0


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
    recipients: set[int],
    content: dict,
    reply_info: dict | None = None,
    user_id: int | None = None,
) -> list:
    """Оптимизированная рассылка сообщений пользователям с поддержкой режимов"""
    if not recipients and user_id is None:
        return []

    if not content or 'type' not in content:
        return []

    # Создаем копию контента для модификаций
    modified_content = content.copy()

    # В функции send_message_to_users
    if anime_mode:
        # Преобразуем только основной текст и подписи
        if modified_content.get('text'):
            modified_content['text'] = anime_transform(modified_content['text'])
        if modified_content.get('caption'):
            modified_content['caption'] = anime_transform(modified_content['caption'])

    # Применяем модификации режимов
    if slavaukraine_mode:
        # Преобразуем основной текст
        if modified_content.get('text'):
            modified_content['text'] = ukrainian_transform(modified_content['text'])
        elif modified_content.get('caption'):
            modified_content['caption'] = ukrainian_transform(modified_content['caption'])
        
        # Добавляем украинские фразы к 30% сообщений
        if random.random() < 0.3:
            if modified_content.get('text'):
                modified_content['text'] += "\n\n" + random.choice(UKRAINIAN_PHRASES)
            elif modified_content.get('caption'):
                modified_content['caption'] += "\n\n" + random.choice(UKRAINIAN_PHRASES)
                
    elif zaputin_mode:
        # Добавляем патриотические фразы к 30% сообщений
        if random.random() < 0.3:
            if modified_content.get('text'):
                modified_content['text'] += "\n\n" + random.choice(PATRIOTIC_PHRASES)
            elif modified_content.get('caption'):
                modified_content['caption'] += "\n\n" + random.choice(PATRIOTIC_PHRASES)
                
        # Применяем преобразования символов
        if modified_content.get('text'):
            modified_content['text'] = zaputin_transform(modified_content['text'])
        if modified_content.get('caption'):
            modified_content['caption'] = zaputin_transform(modified_content['caption'])
            
    elif suka_blyat_mode:
        # Матерные замены для текста
        if modified_content.get('text'):
            words = modified_content['text'].split()
            for i in range(len(words)):
                if random.random() < 0.3:
                    words[i] = random.choice(MAT_WORDS)
            modified_content['text'] = ' '.join(words)

            # Добавляем "... СУКА БЛЯТЬ!" к каждому 3-му сообщению
            global suka_blyat_counter
            suka_blyat_counter += 1
            if suka_blyat_counter % 3 == 0:
                modified_content['text'] += " ... СУКА БЛЯТЬ!"

        # Матерные замены для подписей
        elif modified_content.get('caption'):
            words = modified_content['caption'].split()
            for i in range(len(words)):
                if random.random() < 0.3:
                    words[i] = random.choice(MAT_WORDS)
            modified_content['caption'] = ' '.join(words)

            suka_blyat_counter += 1
            if suka_blyat_counter % 3 == 0:
                modified_content['caption'] += " ... СУКА БЛЯТЬ!"

    # Удаляем заблокировавших бота пользователей из активных
    blocked_users = set()
    active_recipients = set()

    for uid in recipients:
        if uid in state['users_data']['banned']:
            continue
        active_recipients.add(uid)

    if not active_recipients:
        return []

    async def really_send(uid: int, reply_to: int | None):
        """Отправка с обработкой ошибок"""
        try:
            ct = modified_content["type"]
            header_text = modified_content['header']
            head = f"<i>{header_text}</i>"

            reply_to_post = modified_content.get('reply_to_post')
            original_author = None
            if reply_to_post and reply_to_post in messages_storage:
                original_author = messages_storage[reply_to_post].get('author_id')

            if uid == original_author:
                head = head.replace("Пост", "🔴 Пост")

            reply_text = ""
            if reply_to_post:
                if uid == original_author:
                    reply_text = f">>{reply_to_post} (You)\n"
                else:
                    reply_text = f">>{reply_to_post}\n"

            main_text = ""
            if modified_content.get('text'):
                main_text = add_you_to_my_posts(modified_content['text'], original_author)
            elif modified_content.get('caption'):
                main_text = add_you_to_my_posts(modified_content['caption'], original_author)

            full_text = f"{head}\n\n{reply_text}{main_text}" if reply_text else f"{head}\n\n{main_text}"

            # Отправка основного контента
            if ct == "text":
                return await bot.send_message(
                    uid,
                    full_text,
                    reply_to_message_id=reply_to,
                    parse_mode="HTML",
                )
            
            elif ct == "photo":
                # Проверяем, есть ли image_url (это случай аниме-фото)
                img_url = content.get('image_url')
                if img_url:
                    # Есть картинка — отправляем фото по URL
                    if len(full_text) > 1024:
                        full_text = full_text[:1021] + "..."
                    return await bot.send_photo(
                        uid,
                        img_url,
                        caption=full_text,
                        reply_to_message_id=reply_to,
                        parse_mode="HTML"
                    )
                elif "file_id" in content:
                    # Есть file_id — обычная фото из Telegram
                    if len(full_text) > 1024:
                        full_text = full_text[:1021] + "..."
                    return await bot.send_photo(
                        uid,
                        content["file_id"],
                        caption=full_text,
                        reply_to_message_id=reply_to,
                        parse_mode="HTML"
                    )
                else:
                    # НЕТ картинки — отправляем просто текст!
                    return await bot.send_message(
                        uid,
                        full_text,
                        reply_to_message_id=reply_to,
                        parse_mode="HTML"
                    )
        
            elif ct == "video":
                if len(full_text) > 1024:
                    full_text = full_text[:1021] + "..."
                return await bot.send_video(
                    uid,
                    content["file_id"],
                    caption=full_text,
                    reply_to_message_id=reply_to,
                    parse_mode="HTML",
                )

            elif ct == "media_group":
                if not content.get('media') or len(content['media']) == 0:
                    return None
                    
                builder = MediaGroupBuilder()
                for idx, media in enumerate(content['media']):
                    media_caption = None
                    if idx == 0:
                        media_caption = full_text

                    if media['type'] == 'photo':
                        if media_caption:
                            builder.add_photo(
                                media=media['file_id'],
                                caption=media_caption,
                                parse_mode="HTML"
                            )
                        else:
                            builder.add_photo(media=media['file_id'])
                    elif media['type'] == 'video':
                        if media_caption:
                            builder.add_video(
                                media=media['file_id'],
                                caption=media_caption,
                                parse_mode="HTML"
                            )
                        else:
                            builder.add_video(media=media['file_id'])
                    elif media['type'] == 'document':
                        if media_caption:
                            builder.add_document(
                                media=media['file_id'],
                                caption=media_caption,
                                parse_mode="HTML"
                            )
                        else:
                            builder.add_document(media=media['file_id'])

                return await bot.send_media_group(
                    chat_id=uid,
                    media=builder.build(),
                    reply_to_message_id=reply_to
                )

            elif ct == "animation":
                if len(full_text) > 1024:
                    full_text = full_text[:1021] + "..."
                return await bot.send_animation(
                    uid,
                    content["file_id"],
                    caption=full_text,
                    reply_to_message_id=reply_to,
                    parse_mode="HTML",
                )

            elif ct == "document":
                if len(full_text) > 1024:
                    full_text = full_text[:1021] + "..."
                return await bot.send_document(
                    uid,
                    content["file_id"],
                    caption=full_text,
                    reply_to_message_id=reply_to,
                    parse_mode="HTML",
                )

            elif ct == "audio":
                if len(full_text) > 1024:
                    full_text = full_text[:1021] + "..."
                return await bot.send_audio(
                    uid,
                    content["file_id"],
                    caption=full_text,
                    reply_to_message_id=reply_to,
                    parse_mode="HTML",
                )

            elif ct == "sticker":
                return await bot.send_sticker(
                    uid,
                    content["file_id"],
                    reply_to_message_id=reply_to,
                )

            elif ct == "voice":
                return await bot.send_voice(
                    uid,
                    content["file_id"],
                    caption=head,
                    reply_to_message_id=reply_to,
                    parse_mode="HTML"
                )

            elif ct == "video_note":
                return await bot.send_video_note(
                    uid,
                    content["file_id"],
                    reply_to_message_id=reply_to,
                )

        except TelegramRetryAfter as e:
            wait_time = e.retry_after + 1
            print(f"⚠️ Flood control {uid}, waiting {wait_time}s")
            await asyncio.sleep(wait_time)
            return await really_send(uid, reply_to)  # Повторяем попытку
        except TelegramForbiddenError:
            blocked_users.add(uid)
            state['users_data']['active'].discard(uid)
            print(f"🚫 Пользователь {uid} заблокировал бота, удален из активных")
            return None
        except Exception as e:
            print(f"❌ Ошибка отправки {uid}: {e}")
            return None

    # Настройки параллелизма
    max_concurrent = 100
    semaphore = asyncio.Semaphore(max_concurrent)

    async def send_with_semaphore(uid):
        async with semaphore:
            reply_to = reply_info.get(uid) if reply_info else None
            return await really_send(uid, reply_to)

    tasks = [send_with_semaphore(uid) for uid in active_recipients]
    results = await asyncio.gather(*tasks)

    # Сохраняем связи сообщений для ответов
    if content.get('post_num'):
        post_num = content['post_num']
        for uid, msg in zip(active_recipients, results):
            if msg:
                if isinstance(msg, list):
                    if post_num not in post_to_messages:
                        post_to_messages[post_num] = {}
                    post_to_messages[post_num][uid] = msg[0].message_id
                    for m in msg:
                        message_to_post[(uid, m.message_id)] = post_num
                else:
                    if post_num not in post_to_messages:
                        post_to_messages[post_num] = {}
                    post_to_messages[post_num][uid] = msg.message_id
                    message_to_post[(uid, msg.message_id)] = post_num

    # Обновляем активных пользователей
    for uid in blocked_users:
        state['users_data']['active'].discard(uid)

    return list(zip(active_recipients, results))

async def message_broadcaster():
    """Обработчик очереди сообщений с многопоточной обработкой"""
    # Создаем несколько worker'ов для параллельной обработки
    workers = [asyncio.create_task(message_worker(f"Worker-{i}")) for i in range(5)]
    await asyncio.gather(*workers)

async def message_worker(worker_name: str):
    """Индивидуальный обработчик сообщений"""
    while True:
        try:
            msg_data = await message_queue.get()
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

            # Быстрая фильтрация получателей
            active_recipients = {
                uid for uid in recipients 
                if uid not in state['users_data']['banned']
            }

            if not active_recipients:
                continue

            # Логируем перед отправкой
            print(f"{worker_name} | Processing post #{post_num} for {len(active_recipients)} users")

            # Основная отправка
            try:
                results = await send_message_to_users(
                    active_recipients,
                    content,
                    reply_info,
                    msg_data.get('user_id')
                )

                # Логируем результат
                success_count = sum(1 for _, msg in results if msg is not None)
                print(f"{worker_name} | ✅ Пост #{post_num} отправлен: {success_count}/{len(active_recipients)}")

                await process_successful_messages(post_num, results)

            except Exception as e:
                print(f"{worker_name} | ❌ Ошибка отправки #{post_num}: {str(e)[:200]}")

            # Периодическая очистка памяти
            if random.random() < 0.05:
                self_clean_memory()

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

async def check_cooldown(message: Message) -> bool:
    """Проверяет кулдаун на активацию режимов для всех пользователей"""
    global last_mode_activation

    if last_mode_activation is None:
        return True

    elapsed = (datetime.now(UTC) - last_mode_activation).total_seconds()
    if elapsed < MODE_COOLDOWN:
        time_left = MODE_COOLDOWN - elapsed
        minutes = int(time_left // 60)
        seconds = int(time_left % 60)

        try:
            await message.answer(
                f"⏳ Эй пидор, не спеши! Режимы можно включать раз в час.\n"
                f"Жди еще: {minutes} минут {seconds} секунд\n\n"
                f"А пока посиди в углу и подумай о своем поведении",
                parse_mode="HTML"
            )
        except Exception as e:
            print(f"Ошибка отправки кулдауна: {e}")

        await message.delete()
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
    global slavaukraine_mode, last_mode_activation, zaputin_mode, suka_blyat_mode

    # Проверка кулдауна
    if not await check_cooldown(message):
        return

    # Активация режима и деактивация других
    slavaukraine_mode = True
    last_mode_activation = datetime.now(UTC)
    zaputin_mode = False
    suka_blyat_mode = False

    # Отправляем сообщение активации
    header, pnum = format_header()
    header = "### Админ ###"

    # Более двачевый текст активации
    activation_text = (
        "УВАГА! АКТИВОВАНО УКРАЇНСЬКИЙ РЕЖИМ!\n\n"
        "💙💛 СЛАВА УКРАЇНІ! 💛💙\n"
        "ГЕРОЯМ СЛАВА!\n\n"
        "Хто не скаже 'Путін хуйло' - той москаль і підар!"
    )

    await message_queue.put({
        "recipients": state['users_data']['active'],
        "content": {
            "type": "text",
            "header": header,
            "text": activation_text
        },
        "post_num": pnum,
    })

    # Таймер отключения
    asyncio.create_task(disable_slavaukraine_mode(300))  # 5 минут

    await message.delete()

async def disable_slavaukraine_mode(delay: int):
    await asyncio.sleep(delay)
    global slavaukraine_mode
    slavaukraine_mode = False

    header, pnum = format_header()
    header = "### Админ ###"

    end_text = (
        "💀 Визг хохлов закончен!\n\n"
        "Украинский режим отключен. Возвращаемся к обычному трёпу."
    )

    await message_queue.put({
        "recipients": state['users_data']['active'],
        "content": {
            "type": "text",
            "header": header,
            "text": end_text
        },
        "post_num": pnum,
    })

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
    total_users = len(state['users_data']['active'])
    total_posts = state['post_counter']

    # Формируем текст без заголовка
    stats_text = (f"📊 Статистика ТГАЧА:\n\n"
                  f"👥 Анонимов: {total_users}\n"
                  f"📨 Постов на борде: {total_posts}")

    # Отправляем через очередь
    header, pnum = format_header()
    await message_queue.put({
        'recipients': state['users_data']['active'],
        'content': {
            'type': 'text',
            'header': header,
            'text': stats_text
        },
        'post_num': pnum
    })

    await message.delete()

@dp.message(Command("shadowmute"))
async def cmd_shadowmute(message: Message, command: CommandObject):
    if not is_admin(message.from_user.id):
        await message.delete()
        return

    # Упрощенная логика получения target_id
    target_id = None
    if message.reply_to_message:
        target_id = get_author_id_by_reply(message)
    
    if not target_id and command.args:
        try:
            # Пробуем взять первый аргумент как ID
            target_id = int(command.args.split()[0])
        except ValueError:
            pass

    if not target_id:
        await message.answer("❌ Не удалось определить пользователя")
        await message.delete()
        return

    # Установка времени мута
    duration_str = "24h"  # значение по умолчанию
    
    # Если есть аргументы, берем последний как время
    if command.args:
        args = command.args.split()
        # Если есть реплай, берем первый аргумент как время
        if message.reply_to_message and len(args) >= 1:
            duration_str = args[0]
        # Если нет реплая, берем второй аргумент как время
        elif not message.reply_to_message and len(args) >= 2:
            duration_str = args[1]

    try:
        # Парсим duration_str
        duration_str = duration_str.lower().replace(" ", "")
        
        if duration_str.endswith("m"):  # минуты
            minutes = int(duration_str[:-1])
            total_seconds = minutes * 60
        elif duration_str.endswith("h"):  # часы
            hours = int(duration_str[:-1])
            total_seconds = hours * 3600
        elif duration_str.endswith("d"):  # дни
            days = int(duration_str[:-1])
            total_seconds = days * 86400
        else:  # если нет суффикса, считаем как минуты
            total_seconds = int(duration_str) * 60
        
        # Ограничиваем максимальное время мута (30 дней)
        total_seconds = min(total_seconds, 2592000)
        shadow_mutes[target_id] = datetime.now(UTC) + timedelta(seconds=total_seconds)
        
        # Форматирование времени для ответа
        if total_seconds < 60:
            time_str = f"{total_seconds} сек"
        elif total_seconds < 3600:
            time_str = f"{total_seconds // 60} мин"
        elif total_seconds < 86400:
            hours = total_seconds // 3600
            time_str = f"{hours} час"
        else:
            days = total_seconds // 86400
            time_str = f"{days} дней"

        await message.answer(
            f"👻 Тихо замучен пользователь {target_id} на {time_str}",
            parse_mode="HTML"
        )
    except ValueError:
        await message.answer(
            "❌ Неверный формат времени. Примеры:\n"
            "30m - 30 минут\n"
            "2h - 2 часа\n"
            "1d - 1 день"
        )
    
    await message.delete()


@dp.message(Command("unshadowmute"))
async def cmd_unshadowmute(message: Message):
    """Снятие тихого мута"""
    if not is_admin(message.from_user.id):
        await message.delete()
        return

    # Получаем target_id: либо из реплая, либо из аргумента
    target_id = None

    # 1. Если это ответ на сообщение, берем ID автора
    if message.reply_to_message:
        target_id = get_author_id_by_reply(message)

    # 2. Если не нашли из реплая, пробуем из аргументов
    if not target_id:
        parts = message.text.split()
        if len(parts) >= 2:
            try:
                target_id = int(parts[1])
            except ValueError:
                pass

    if not target_id:
        await message.answer("Использование: /unshadowmute <user_id> или ответ на сообщение")
        await message.delete()
        return

    if target_id in shadow_mutes:
        del shadow_mutes[target_id]
        await message.answer(f"👻 Пользователь {target_id} тихо размучен")
    else:
        await message.answer(f"ℹ️ Пользователь {target_id} не в shadow-муте")
    
    await message.delete()


@dp.message(Command("anime"))
async def cmd_anime(message: types.Message):
    global anime_mode, last_mode_activation, zaputin_mode, slavaukraine_mode, suka_blyat_mode

    # Проверка кулдауна
    if not await check_cooldown(message):
        return

    # Активируем режим и выключаем другие
    anime_mode = True
    zaputin_mode = False
    slavaukraine_mode = False
    suka_blyat_mode = False
    last_mode_activation = datetime.now(UTC)

    # Отправляем сообщение активации
    header = "### 管理者 ###"
    state['post_counter'] += 1
    pnum = state['post_counter']

    activation_text = (
        "にゃあ～！アニメモードがアクティベートされました！\n\n"
        "^_^"
    )

    await message_queue.put({
        "recipients": state['users_data']['active'],
        "content": {
            "type": "text",
            "header": header,
            "text": activation_text
        },
        "post_num": pnum,
    })

    # Таймер отключения
    asyncio.create_task(disable_anime_mode(300))  # 5 минут

    await message.delete()

async def disable_anime_mode(delay: int):
    """Отключает режим anime через указанное время"""
    await asyncio.sleep(delay)
    global anime_mode
    anime_mode = False

    # Отправляем сообщение об окончании
    header = "### Админ ###"
    state['post_counter'] += 1
    pnum = state['post_counter']

    end_text = (
        "アニメモードが終了しました！\n\n"
        "通常のチャットに戻ります！"
    )

    await message_queue.put({
        "recipients": state['users_data']['active'],
        "content": {
            "type": "text",
            "header": header,
            "text": end_text
        },
        "post_num": pnum,
    })


@dp.message(Command("deanon"))
async def cmd_deanon(message: Message):
    """Обработчик команды /deanon"""
    # Проверяем, что команда вызвана ответом на сообщение
    if not message.reply_to_message:
        await message.answer("⚠️ Ответь на сообщение для деанона!")
        await message.delete()
        return

    # Определяем цель деанона
    reply_key = (message.from_user.id, message.reply_to_message.message_id)
    target_post = message_to_post.get(reply_key)

    if not target_post or target_post not in messages_storage:
        await message.answer("🚫 Не удалось найти пост для деанона!")
        await message.delete()
        return

    target_id = messages_storage[target_post].get("author_id")
    
    # Генерируем фейковые данные
    name, surname, city, profession, fetish, detail = generate_deanon_info()
    ip = f"{random.randint(10,250)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(0,255)}"
    age = random.randint(18, 45)
    
    # Формируем текст деанона
    deanon_text = (
        f"\nЭтого анона зовут: {name} {surname}\n"
        f"Возраст: {age}\n"
        f"Адрес проживания: {city}\n"
        f"Профессия: {profession}\n"
        f"Фетиш: {fetish}\n"
        f"IP-адрес: {ip}\n"
        f"Дополнительная информация о нём: {detail}"
    )

    # Отправляем как ответ на сообщение
    header = "### ДЕАНОН ###"
    state['post_counter'] += 1
    pnum = state['post_counter']

    await message_queue.put({
        "recipients": state['users_data']['active'],
        "content": {
            "type": "text",
            "header": header,
            "text": deanon_text,
            "reply_to_post": target_post
        },
        "post_num": pnum,
        "reply_info": post_to_messages.get(target_post, {})
    })

    await message.delete()
    
# ====== ZAPUTIN ======
@dp.message(Command("zaputin"))
async def cmd_zaputin(message: types.Message):
    global zaputin_mode, last_mode_activation, suka_blyat_mode, slavaukraine_mode

    # Проверка кулдауна
    if not await check_cooldown(message):
        return

    # Активируем режим и выключаем другие
    zaputin_mode = True
    suka_blyat_mode = False
    slavaukraine_mode = False
    last_mode_activation = datetime.now(UTC)

    # Отправляем сообщение активации
    header = "### Админ ###"
    state['post_counter'] += 1
    pnum = state['post_counter']

    activation_text = (
        "🇷🇺 СЛАВА РОССИИ! ПУТИН - НАШ ПРЕЗИДЕНТ! 🇷🇺\n\n"
        "Активирован режим кремлеботов! Все несогласные будут приравнены к пидорасам и укронацистам!"
    )

    await message_queue.put({
        "recipients": state['users_data']['active'],
        "content": {
            "type": "text",
            "header": header,
            "text": activation_text
        },
        "post_num": pnum,
    })

    # Таймер отключения
    asyncio.create_task(disable_zaputin_mode(300))  # 5 минут

    await message.delete()

async def disable_zaputin_mode(delay: int):
    """Отключает режим zaputin через указанное время"""
    await asyncio.sleep(delay)
    global zaputin_mode
    zaputin_mode = False

    # Отправляем сообщение об окончании
    header = "### Админ ###"
    state['post_counter'] += 1
    pnum = state['post_counter']

    end_text = "💀 Бунт кремлеботов окончился. Всем спасибо, все свободны."

    await message_queue.put({
        "recipients": state['users_data']['active'],
        "content": {
            "type": "text",
            "header": header,
            "text": end_text
        },
        "post_num": pnum,
    })

# ====== SUKA_BLYAT ======
@dp.message(Command("suka_blyat"))
async def cmd_suka_blyat(message: types.Message):
    global suka_blyat_mode, last_mode_activation, zaputin_mode, slavaukraine_mode

    # Проверка кулдауна
    if not await check_cooldown(message):
        return

    # Активируем режим и выключаем другие
    suka_blyat_mode = True
    zaputin_mode = False
    slavaukraine_mode = False
    last_mode_activation = datetime.now(UTC)

    # Отправляем сообщение активации
    header = "### Админ ###"
    state['post_counter'] += 1
    pnum = state['post_counter']

    activation_text = (
        "💢💢💢 Активирован режим СУКА БЛЯТЬ! 💢💢💢\n\n"
        "Всех нахуй разъебало!"
    )

    await message_queue.put({
        "recipients": state['users_data']['active'],
        "content": {
            "type": "text",
            "header": header,
            "text": activation_text
        },
        "post_num": pnum,
    })

    # Таймер отключения
    asyncio.create_task(disable_suka_blyat_mode(300))  # 5 минут

    await message.delete()

async def disable_suka_blyat_mode(delay: int):
    """Отключает режим suka_blyat через указанное время"""
    await asyncio.sleep(delay)
    global suka_blyat_mode
    suka_blyat_mode = False

    # Отправляем сообщение об окончании
    header = "### Админ ###"
    state['post_counter'] += 1
    pnum = state['post_counter']

    end_text = "💀 СУКА БЛЯТЬ КОНЧИЛОСЬ. Теперь можно и помолчать."

    await message_queue.put({
        "recipients": state['users_data']['active'],
        "content": {
            "type": "text",
            "header": header,
            "text": end_text
        },
        "post_num": pnum,
    })

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
def get_author_id_by_reply(msg: types.Message) -> int | None:
    """Получаем ID автора поста по reply"""
    if not msg.reply_to_message:
        return None

    reply_mid = msg.reply_to_message.message_id
    for (uid, mid), pnum in message_to_post.items():
        if mid == reply_mid:
            return messages_storage.get(pnum, {}).get("author_id")
    return None

# ===== /id ==========================================================
@dp.message(Command("id"))
async def cmd_get_id(message: types.Message):
    """ /id — вывести ID и инфу автора реплай-поста или свою, если без reply """
    if message.reply_to_message:
        author_id = get_author_id_by_reply(message)
        if author_id:
            try:
                # Получаем информацию об авторе
                author = await bot.get_chat(author_id)

                info = "🆔 <b>Информация об авторе:</b>\n\n"
                info += f"ID: <code>{author_id}</code>\n"
                info += f"Имя: {author.first_name}\n"

                if author.last_name:
                    info += f"Фамилия: {author.last_name}\n"

                if author.username:
                    info += f"Username: @{author.username}\n"

                # Статус
                if author_id in state['users_data']['banned']:
                    info += "\n⛔️ Статус: ЗАБАНЕН"
                elif author_id in state['users_data']['active']:
                    info += "\n✅ Статус: Активен"

                await message.answer(info, parse_mode="HTML")
            except:
                # Если не удалось получить инфу - показываем только ID
                await message.answer(f"ID автора: <code>{author_id}</code>",
                                     parse_mode="HTML")
        else:
            await message.answer("Не удалось определить автора.")
    await message.delete()


# ===== /ban =========================================================
@dp.message(Command("ban"))
async def cmd_ban(message: types.Message):
    """
    /ban  – reply-бан автора сообщения
    /ban <id> – бан по ID
    """
    if not is_admin(message.from_user.id):
        return

    target_id: int | None = None

    # 1) reply-бан
    if message.reply_to_message:
        target_id = get_author_id_by_reply(message)

    # 2) /ban <id>
    parts = message.text.split()
    if len(parts) == 2 and parts[1].isdigit():
        target_id = int(parts[1])

    if not target_id:
        await message.answer(
            "Нужно ответить на сообщение или указать ID: /ban <id>")
        return

    # Удаляем посты пользователя за последние 5 минут (было 60)
    deleted_posts = await delete_user_posts(target_id, 5)

    state['users_data']['banned'].add(target_id)
    state['users_data']['active'].discard(target_id)

    await message.answer(
        f"✅ Хуесос под номером <code>{target_id}</code> забанен\n"
        f"Удалено его постов за последний час: {deleted_posts}",
        parse_mode="HTML")

    # Отправляем уведомление в чат
    await send_moderation_notice(target_id, "ban", None, deleted_posts)

    # Пытаемся уведомить
    try:
        await bot.send_message(
            target_id,
            f"Пидорас ебаный, ты нас так заебал, что тебя блокнули нахуй.\n"
            f"Удалено твоих постов за последний час: {deleted_posts}\n"
            "Пиздуй отсюда."
        )
    except:
        pass

    await message.delete()

# ========== КОМАНДА /MUTE ==========
@dp.message(Command("mute"))
async def cmd_mute(message: Message, command: CommandObject):
    if not is_admin(message.from_user.id):
        await message.delete()
        return

    # Получаем аргументы команды
    args = command.args
    if not args:
        await message.answer(
            "Использование:\n"
            "Ответьте на сообщение + /mute [время]\n"
            "Или: /mute <user_id> [время]\n\n"
            "Примеры:\n"
            "/mute 123456789 1h - мут на 1 час\n"
            "/mute 123456789 30m - мут на 30 минут\n"
            "/mute 123456789 2d - мут на 2 дня"
        )
        await message.delete()
        return

    # Парсим аргументы
    parts = args.split()
    target_id = None
    duration_str = "24h"  # значение по умолчанию

    # Если это reply на сообщение
    if message.reply_to_message:
        # Пытаемся найти автора оригинального сообщения
        reply_key = (message.from_user.id, message.reply_to_message.message_id)
        post_num = message_to_post.get(reply_key)
        if post_num:
            target_id = messages_storage.get(post_num, {}).get('author_id')

        # Если нашли ID, берем время из аргументов
        if target_id and parts:
            duration_str = parts[0]
    else:
        # Если не reply, то первый аргумент - ID, второй - время
        if len(parts) >= 1:
            try:
                target_id = int(parts[0])
                if len(parts) >= 2:
                    duration_str = parts[1]
            except ValueError:
                await message.answer("❌ Неверный ID пользователя")
                await message.delete()
                return

    if not target_id:
        await message.answer("❌ Не удалось определить пользователя")
        await message.delete()
        return

    # Парсим duration_str в timedelta
    try:
        duration_str = duration_str.lower().replace(" ", "")

        if duration_str.endswith("m"):  # минуты
            mute_seconds = int(duration_str[:-1]) * 60
            duration_text = f"{int(duration_str[:-1])} минут"
        elif duration_str.endswith("h"):  # часы
            mute_seconds = int(duration_str[:-1]) * 3600
            duration_text = f"{int(duration_str[:-1])} часов"
        elif duration_str.endswith("d"):  # дни
            mute_seconds = int(duration_str[:-1]) * 86400
            duration_text = f"{int(duration_str[:-1])} дней"
        else:  # если нет суффикса, считаем как минуты
            mute_seconds = int(duration_str) * 60
            duration_text = f"{int(duration_str)} минут"

        # Ограничиваем максимальное время мута (30 дней)
        mute_seconds = min(mute_seconds, 2592000)
        mute_duration = timedelta(seconds=mute_seconds)

    except (ValueError, AttributeError):
        await message.answer(
            "❌ Неверный формат времени. Примеры:\n"
            "30m - 30 минут\n"
            "2h - 2 часа\n"
            "1d - 1 день"
        )
        await message.delete()
        return

    # Удаляем посты пользователя за последние 5 минут
    deleted_count = await delete_user_posts(target_id, 5)

    # Применяем мут
    mutes[target_id] = datetime.now(UTC) + mute_duration

    # Отправляем подтверждение
    await message.answer(
        f"🔇 Хуила {target_id} замучен на {duration_text}\n"
        f"Удалено сообщений за последние 5 минут: {deleted_count}",
        parse_mode="HTML"
    )

    # Отправляем уведомление в чат
    header, post_num = format_header()
    header = header.replace("Пост", "### АДМИН ###")
    mute_text = (
        f"🚨 Пидораса спамера замутило нахуй на {duration_text}\n"
        f"Удалено его сообщений за последние 5 минут: {deleted_count}\n"
        f"Этот пидор всех уже доебал, пускай попустится и отдохнёт."
    )

    # Сохраняем сообщение
    messages_storage[post_num] = {
        'author_id': 0,  # 0 = системное сообщение
        'timestamp': datetime.now(UTC),
        'content': {
            'type': 'text',
            'header': header,
            'text': mute_text
        }
    }

    # Отправляем всем
    await message_queue.put({
        "recipients": state["users_data"]["active"],
        "content": {
            "type": "text",
            "header": header,
            "text": mute_text,
            "is_system_message": True
        },
        "post_num": post_num
    })

    # Личное уведомление пользователю
    try:
        await bot.send_message(
            target_id,
            f"🔇 Пидор ебаный хорош спамить ты меня уже доебал, поссал тебе на рыло ебаное посиди в муте еще {duration_text}.\n"
            f"Удалено твоих сообщений за последние 5 минут: {deleted_count}\n"
            "Вы можете читать сообщения, но не можете писать. Сосите хуй, дорогой пользователь!",
            parse_mode="HTML"
        )
    except Exception as e:
        print(f"Ошибка отправки уведомления о муте: {e}")

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
async def cmd_unmute(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.delete()
        return
    target_id = None
    if message.reply_to_message:
        target_id = message.reply_to_message.from_user.id
    else:
        parts = message.text.split()
        if len(parts) == 2 and parts[1].isdigit():
            target_id = int(parts[1])
    if not target_id:
        await message.answer("Нужно reply или /unmute <id>")
        return
    mutes.pop(target_id, None)
    await message.answer(f"🔈 Пользователь {target_id} размучен")
    try:
        await bot.send_message(
            target_id, "Эй хуйло ебаное, тебя размутили, можешь писать.")
    except:
        pass



@dp.message(Command("unban"))
async def cmd_unban(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.delete()
        return

    args = message.text.split()
    if len(args) < 2:
        await message.answer("Использование: /unban <user_id>")
        await message.delete()
        return

    try:
        user_id = int(args[1])
        state['users_data']['banned'].discard(user_id)
        await message.answer(f"Пользователь {user_id} разбанен")
    except ValueError:
        await message.answer("Неверный ID пользователя")

    await message.delete()


@dp.message(Command("del"))
async def cmd_del(message: types.Message):
    global message_to_post
    if not is_admin(message.from_user.id):
        return

    if not message.reply_to_message:
        await message.answer("Ответь на сообщение, которое нужно удалить")
        return

    # 1. Ищем post_num по message_id
    target_mid = message.reply_to_message.message_id
    post_num = None
    for (uid, mid), pnum in message_to_post.items():
        if mid == target_mid:
            post_num = pnum
            break

    if post_num is None:
        await message.answer("Не нашёл этот пост в базе")
        return

    # 2. Удаляем у всех получателей
    deleted = 0
    if post_num in post_to_messages:
        for uid, mid in post_to_messages[post_num].items():
            try:
                await bot.delete_message(uid, mid)
                deleted += 1
            except:
                pass

    # 3. Удаляем у автора
    author_mid = messages_storage.get(post_num, {}).get('author_message_id')
    author_id = messages_storage.get(post_num, {}).get('author_id')
    if author_mid and author_id:
        try:
            await bot.delete_message(author_id, author_mid)
            deleted += 1
        except:
            pass

    # 4. Чистим словари
    post_to_messages.pop(post_num, None)
    messages_storage.pop(post_num, None)
    
    # 5. Чистим message_to_post
    message_to_post = {k: v for k, v in message_to_post.items() if v != post_num}

    await message.answer(f"Пост №{post_num} удалён у {deleted} пользователей")

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
        return

    await callback.message.answer(
        f"Статистика:\n\n"
        f"Активных: {len(state['users_data']['active'])}\n"
        f"Забаненных: {len(state['users_data']['banned'])}\n"
        f"Всего постов: {state['post_counter']}\n"
        f"Сообщений в памяти: {len(messages_storage)}\n"
        f"В очереди: {message_queue.qsize()}")
    await callback.answer()

@dp.callback_query(F.data == "spammers")
async def admin_spammers(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        return

    # Сортируем по количеству сообщений
    sorted_users = sorted(state['message_counter'].items(),
                          key=lambda x: x[1],
                          reverse=True)[:10]

    text = "Топ 10 спамеров:\n\n"
    for user_id, count in sorted_users:
        text += f"ID {user_id}: {count} сообщений\n"

    await callback.message.answer(text)
    await callback.answer()


@dp.callback_query(F.data == "banned")
async def admin_banned(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        return

    if not state['users_data']['banned']:
        await callback.answer("Нет забаненных пользователей")
        return

    text = "Забаненные пользователи:\n\n"
    for user_id in state['users_data']['banned']:
        text += f"ID {user_id}\n"

    await callback.message.answer(text)
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
    # Защита от повторной отправки!
    if media_group_id in sent_media_groups:
        return
    if media_group_id not in current_media_groups:
        return
    group = current_media_groups[media_group_id]
    if not group['media']:
        del current_media_groups[media_group_id]
        return

    # Помечаем как отправленную (это важно, чтобы дубли не ушли!)
    sent_media_groups.add(media_group_id)
    post_num = group['post_num']
    user_id = group['author_id']
    content = {
        'type': 'media_group',
        'header': group['header'],
        'media': group['media'],
        'caption': group.get('caption'),
        'reply_to_post': group.get('reply_to_post')
    }
    messages_storage[post_num] = {
        'author_id': user_id,
        'timestamp': group['timestamp'],
        'content': content,
        'reply_to': group.get('reply_to_post')
    }

    # Отправляем автору
    try:
        builder = MediaGroupBuilder()
        reply_to_message_id = None
        if group.get('reply_to_post'):
            reply_map = post_to_messages.get(group['reply_to_post'], {})
            reply_to_message_id = reply_map.get(user_id)
        for idx, media in enumerate(group['media']):
            if not media.get('file_id'):
                continue
            caption = None
            if idx == 0:
                caption = f"<i>{group['header']}</i>"
                if group.get('caption'):
                    caption += f"\n\n{escape_html(group['caption'])}"
            if media['type'] == 'photo':
                builder.add_photo(media=media['file_id'], caption=caption, parse_mode="HTML" if caption else None)
            elif media['type'] == 'video':
                builder.add_video(media=media['file_id'], caption=caption, parse_mode="HTML" if caption else None)
            elif media['type'] == 'document':
                builder.add_document(media=media['file_id'], caption=caption, parse_mode="HTML" if caption else None)
            elif media['type'] == 'audio':
                builder.add_audio(media=media['file_id'], caption=caption, parse_mode="HTML" if caption else None)
        if not builder.build():
            del current_media_groups[media_group_id]
            return
        sent_messages = await bot.send_media_group(
            chat_id=user_id,
            media=builder.build(),
            reply_to_message_id=reply_to_message_id
        )
        if sent_messages:
            messages_storage[post_num]['author_message_id'] = sent_messages[0].message_id
            if post_num not in post_to_messages:
                post_to_messages[post_num] = {}
            post_to_messages[post_num][user_id] = sent_messages[0].message_id
            for msg in sent_messages:
                message_to_post[(user_id, msg.message_id)] = post_num
    except Exception as e:
        print(f"Ошибка отправки медиа-альбома автору: {e}")

    # Отправляем остальным
    recipients = state['users_data']['active'] - {user_id}
    if recipients:
        await message_queue.put({
            'recipients': recipients,
            'content': content,
            'post_num': post_num,
            'reply_info': post_to_messages.get(group['reply_to_post'], {}) if group.get('reply_to_post') else None
        })

    # Чистим временную память
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
async def handle_media_group_init(message: Message):
    user_id = message.from_user.id

    # Проверка на бан и мут
    if user_id in state['users_data']['banned']:
        await message.delete()
        return
    if mutes.get(user_id) and mutes[user_id] > datetime.now(UTC):
        await message.delete()
        return

    media_group_id = message.media_group_id
    if not media_group_id:
        return

    # Проверка: если группа уже отправлена - ничего не делаем
    if media_group_id in sent_media_groups:
        await message.delete()
        return

    # Проверяем reply_to_message для ответов
    reply_to_post = None
    if message.reply_to_message:
        reply_mid = message.reply_to_message.message_id
        for (uid, mid), pnum in message_to_post.items():
            if mid == reply_mid:
                reply_to_post = pnum
                break
        if reply_to_post and reply_to_post not in messages_storage:
            reply_to_post = None

    # Инициализация группы, если не существует
    if media_group_id not in current_media_groups:
        header, post_num = format_header()
        caption = message.caption or ""
        # Преобразования режимов
        if slavaukraine_mode and caption:
            caption = ukrainian_transform(caption)
        elif suka_blyat_mode and caption:
            caption = suka_blyatify_text(caption)
        elif anime_mode and caption:
            caption = anime_transform(caption)
        elif zaputin_mode and caption:
            caption = zaputin_transform(caption)

        current_media_groups[media_group_id] = {
            'post_num': post_num,
            'header': header,
            'author_id': user_id,
            'timestamp': datetime.now(MSK),
            'media': [],
            'caption': caption,
            'reply_to_post': reply_to_post,
            'processed_messages': set()
        }

    # Добавляем медиа в группу только если это новое сообщение
    group = current_media_groups[media_group_id]
    if message.message_id not in group['processed_messages']:
        media_data = {
            'type': message.content_type,
            'file_id': None,
            'message_id': message.message_id
        }
        if message.photo:
            media_data['file_id'] = message.photo[-1].file_id
        elif message.video:
            media_data['file_id'] = message.video.file_id
        elif message.document:
            media_data['file_id'] = message.document.file_id
        elif message.audio:
            media_data['file_id'] = message.audio.file_id
        if media_data['file_id']:
            group['media'].append(media_data)
            group['processed_messages'].add(message.message_id)

    await message.delete()

    # --- Новый таймер для завершения группы ---
    # Сбрасываем таймер если он уже есть
    if media_group_id in media_group_timers:
        media_group_timers[media_group_id].cancel()

    # Запускаем новый таймер (например, 1.5 секунды) ТОЛЬКО если группа еще не отправлена!
    if media_group_id not in sent_media_groups:
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
    user_id = message.from_user.id
    
    # Проверка shadow-мута (ДОБАВЛЕНО)
    if user_id in shadow_mutes and shadow_mutes[user_id] > datetime.now(UTC):
        # Удаляем оригинальное сообщение
        try:
            await message.delete()
        except:
            pass

        # Форматируем "фантомный" заголовок с текущим номером
        header = f"Пост №{state['post_counter'] + 1}"
        
        # Имитируем отправку сообщения только автору
        try:
            if message.text:
                # Применяем преобразования как для обычного сообщения
                text = message.text
                if suka_blyat_mode:
                    text = suka_blyatify_text(text)
                if slavaukraine_mode:
                    text = ukrainian_transform(text)
                if anime_mode:
                    text = anime_transform(text)
                if zaputin_mode:
                    text = zaputin_transform(text)
                    
                await bot.send_message(
                    user_id, 
                    f"<i>{header}</i>\n\n{escape_html(text)}", 
                    parse_mode="HTML"
                )
            
            elif message.photo:
                caption = message.caption or ""
                # Применяем преобразования к подписи
                if suka_blyat_mode and caption:
                    caption = suka_blyatify_text(caption)
                if slavaukraine_mode and caption:
                    caption = ukrainian_transform(caption)
                if anime_mode and caption:
                    caption = anime_transform(caption)
                if zaputin_mode and caption:
                    caption = zaputin_transform(caption)
                    
                full_caption = f"<i>{header}</i>"
                if caption:
                    full_caption += f"\n\n{escape_html(caption)}"
                
                await bot.send_photo(
                    user_id,
                    message.photo[-1].file_id,
                    caption=full_caption,
                    parse_mode="HTML"
                )
            
            # Аналогично для других типов контента...
            
        except Exception as e:
            print(f"Ошибка фантомной отправки: {e}")
        
        # Выходим без дальнейшей обработки
        return


    
    if not message.text and not message.caption and not message.content_type:
        await message.delete()
        return
        
    # Пропускаем сообщения, которые являются частью медиа-группы
    if message.media_group_id:
        return
    try:
        until = mutes.get(user_id)
        if until and until > datetime.now(UTC):
            left = until - datetime.now(UTC)
            minutes = int(left.total_seconds() // 60)
            seconds = int(left.total_seconds() % 60)
            try:
                await message.delete()
                await bot.send_message(
                    user_id, 
                    f"🔇 Эй пидор, ты в муте ещё {minutes}м {seconds}с\nСпамишь дальше - получишь бан",
                    parse_mode="HTML"
                )
            except:
                pass
            return
        elif until:  # срок мута вышел
            mutes.pop(user_id, None)

        # 1. автоматически добавляем в active
        if user_id not in state['users_data']['active']:
            state['users_data']['active'].add(user_id)
            print(f"✅ Добавлен новый пользователь: ID {user_id}")

        # 2. бан
        if user_id in state['users_data']['banned']:
            await message.delete()
            await message.answer("❌ Ты забанен", show_alert=True)
            return

        # 3. сохраняем объект сообщения для возможного удаления
        get_user_msgs_deque(user_id).append(message)

        # 4. спам-проверка с прогрессивным наказанием           
        spam_check = await check_spam(user_id, message)
        if not spam_check:
            await message.delete()
            msg_type = message.content_type
            # Если это медиа с подписью - считаем как текстовый спам
            if message.content_type in ['photo', 'video', 'document'] and message.caption:
                msg_type = 'text'

            # Применяем наказание
            await apply_penalty(user_id, msg_type)
            return

        # 5. получатели (исключаем автора из получателей)
        recipients = state['users_data']['active'] - {user_id}

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
                reply_to_post = None  # Сбрасываем, если нет в post_to_messages

        # Форматируем заголовок
        header, current_post_num = format_header()

        # Логируем текст для дневного архива
        if message.text:
            daily_log.write(
                f"[{datetime.now(timezone.utc).isoformat()}] {message.text}\n"
            )

        # Определяем тип контента
        content_type = message.content_type

        # Удаляем оригинальное сообщение
        try:
            await message.delete()
        except:
            pass

        # Для остальных типов контента
        content = {
            'type': content_type,
            'header': header,  # Здесь header без тегов
            'reply_to_post': reply_to_post
        }

        if content_type == 'text':
            if message.entities:
                text_content = message.html_text
            else:
                text_content = escape_html(message.text)
        
            # Применяем преобразования (сохраняя все существующие режимы)
            if suka_blyat_mode:
                text_content = suka_blyatify_text(text_content)
            if slavaukraine_mode:
                text_content = ukrainian_transform(text_content)
            if anime_mode:
                text_content = anime_transform(text_content)
            if zaputin_mode:
                text_content = zaputin_transform(text_content)
        
            if anime_mode and random.random() < 0.41:
                full_caption = f"<i>{header}</i>\n\n{text_content}"
                if len(full_caption) <= 1024:
                    anime_img_url = await get_random_anime_image()
                    if anime_img_url:
                        content['type'] = 'photo'
                        content['caption'] = text_content
                        content['image_url'] = anime_img_url
                    else:
                        # Если не удалось получить картинку — отправляем текстовый пост
                        content['type'] = 'text'
                        content['text'] = text_content
                else:
                    content['type'] = 'text'
                    content['text'] = text_content
            else:
                content['type'] = 'text'
                content['text'] = text_content

        elif content_type == 'photo':
            content['file_id'] = message.photo[-1].file_id
            caption = message.caption

            # Применяем преобразования к подписи
            if caption:
                if suka_blyat_mode:
                    caption = suka_blyatify_text(caption)
                if slavaukraine_mode:
                    caption = ukrainian_transform(caption)
                if anime_mode:
                    caption = anime_transform(caption)
                if zaputin_mode:
                    caption = zaputin_transform(caption)   
                
            content['caption'] = caption

        elif content_type == 'video':
            content['file_id'] = message.video.file_id
            caption = message.caption

            if caption:
                if suka_blyat_mode:
                    caption = suka_blyatify_text(caption)
                if slavaukraine_mode:
                    caption = ukrainian_transform(caption)
                if anime_mode:
                    caption = anime_transform(caption)
                if zaputin_mode:
                    caption = zaputin_transform(caption)   

            content['caption'] = caption

        elif content_type == 'animation':
            content['file_id'] = message.animation.file_id
            caption = message.caption

            if caption:
                if suka_blyat_mode:
                    caption = suka_blyatify_text(caption)
                if slavaukraine_mode:
                    caption = ukrainian_transform(caption)
                if anime_mode:
                    caption = anime_transform(caption)
                if zaputin_mode:
                    caption = zaputin_transform(caption)                       

            content['caption'] = caption

        elif content_type == 'document':
            content['file_id'] = message.document.file_id
            caption = message.caption

            if caption:
                if suka_blyat_mode:
                    caption = suka_blyatify_text(caption)
                if slavaukraine_mode:
                    caption = ukrainian_transform(caption)
                if anime_mode:
                    caption = anime_transform(caption)
                if zaputin_mode:
                    caption = zaputin_transform(caption)   
            
            content['caption'] = caption

        elif content_type == 'sticker':
            content['file_id'] = message.sticker.file_id 

        elif content_type == 'audio':
            content['file_id'] = message.audio.file_id
            caption = message.caption

            if caption:
                if suka_blyat_mode:
                    caption = suka_blyatify_text(caption)
                if slavaukraine_mode:
                    caption = ukrainian_transform(caption)
                if anime_mode:
                    caption = anime_transform(caption)
                if zaputin_mode:
                    caption = zaputin_transform(caption)                 
            
            content['caption'] = caption

        elif content_type == 'video_note':
            content['file_id'] = message.video_note.file_id

        # Инициализируем запись в хранилище перед отправкой
        messages_storage[current_post_num] = {
            'author_id': user_id,
            'timestamp': datetime.now(UTC),
            'content': content,
            'reply_to': reply_to_post,
            'author_message_id': None  # Будет установлено после отправки
        }

        # Отправляем автору
        reply_to_message_id = reply_info.get(user_id) if reply_info else None
        sent_to_author = None

        # Формируем текст для автора (без (You))
        header_text = f"<i>{header}</i>"
        reply_text = ""
        if reply_to_post:
            reply_text = f">>{reply_to_post}\n"

        try:
            if content_type == 'text':
                full_text = f"{header_text}\n\n{reply_text}{content['text']}" if reply_text else f"{header_text}\n\n{content['text']}"
                sent_to_author = await bot.send_message(
                    user_id,
                    full_text,
                    reply_to_message_id=reply_to_message_id,
                    parse_mode="HTML"
                )
            elif content_type in ['photo', 'video', 'animation', 'document', 'audio']:
                caption = header_text
                if content.get('caption'):
                    caption += f"\n\n{escape_html(content['caption'])}"
                if reply_to_post:
                    caption = f"{header_text}\n\n{reply_text}{escape_html(content['caption']) if content.get('caption') else ''}"

                if content_type == 'photo':
                    sent_to_author = await bot.send_photo(
                        user_id,
                        content['file_id'],
                        caption=caption,
                        reply_to_message_id=reply_to_message_id,
                        parse_mode="HTML"
                    )
                elif content_type == 'video':
                    sent_to_author = await bot.send_video(
                        user_id,
                        content['file_id'],
                        caption=caption,
                        reply_to_message_id=reply_to_message_id,
                        parse_mode="HTML"
                    )
                elif content_type == 'animation':
                    sent_to_author = await bot.send_animation(
                        user_id,
                        content['file_id'],
                        caption=caption,
                        reply_to_message_id=reply_to_message_id,
                        parse_mode="HTML"
                    )
                elif content_type == 'document':
                    sent_to_author = await bot.send_document(
                        user_id,
                        content['file_id'],
                        caption=caption,
                        reply_to_message_id=reply_to_message_id,
                        parse_mode="HTML"
                    )
                elif content_type == 'audio':
                    sent_to_author = await bot.send_audio(
                        user_id,
                        content['file_id'],
                        caption=caption,
                        reply_to_message_id=reply_to_message_id,
                        parse_mode="HTML"
                    )
            elif content_type == 'sticker':
                sent_to_author = await bot.send_sticker(
                    user_id,
                    content['file_id'],
                    reply_to_message_id=reply_to_message_id
                )
            elif content_type == 'video_note':
                sent_to_author = await bot.send_video_note(
                    user_id,
                    content['file_id'],
                    reply_to_message_id=reply_to_message_id
                )

            # Сохраняем для ответов
            if sent_to_author:
                messages_storage[current_post_num]['author_message_id'] = sent_to_author.message_id
                if current_post_num not in post_to_messages:
                    post_to_messages[current_post_num] = {}
                post_to_messages[current_post_num][user_id] = sent_to_author.message_id
                message_to_post[(user_id, sent_to_author.message_id)] = current_post_num

            # Отправляем только другим пользователям (исключая автора)
            if recipients:
                await message_queue.put({
                    'recipients': recipients,
                    'content': content,
                    'post_num': current_post_num,
                    'reply_info': reply_info if reply_info else None
                })

        except Exception as e:
            print(f"Ошибка при обработке сообщения: {e}")
            # Удаляем запись из хранилища, если не удалось отправить
            if current_post_num in messages_storage:
                del messages_storage[current_post_num]

    except Exception as e:
        print(f"Критическая ошибка в handle_message: {e}")

async def start_background_tasks():
    """Поднимаем все фоновые корутины ОДИН раз за весь runtime"""
    global message_queue
    message_queue = asyncio.Queue(maxsize=9000)

    tasks = [
        asyncio.create_task(auto_backup()),
        asyncio.create_task(message_broadcaster()),
        asyncio.create_task(conan_roaster(
            state,
            messages_storage,
            post_to_messages,
            message_to_post,
            message_queue,
            format_header
        )),
        asyncio.create_task(motivation_broadcaster()),
        asyncio.create_task(auto_memory_cleaner()),
        asyncio.create_task(cleanup_old_messages()),
        asyncio.create_task(help_broadcaster(state, message_queue, format_header)),
    ]
    print(f"✓ Background tasks started: {len(tasks)}")
    return tasks 

async def supervisor():
    # Проверка токенов
    for board, token in BOT_TOKENS.items():
        if token is None:
            print(f"⛔ Ошибка: Токен для доски {board} не задан! Проверьте переменную окружения в Railway.")
            sys.exit(1)  # Прерываем выполнение

    lock_file = "bot.lock"
    if os.path.exists(lock_file):
        print("⛔ Bot already running! Exiting...")
        sys.exit(1)
    
    with open(lock_file, "w") as f:
        f.write(str(os.getpid()))
    
    try:
        global is_shutting_down, healthcheck_site, bots, dispatchers, bot_to_board
        loop = asyncio.get_running_loop()

        restore_backup_on_start()

        # Инициализация ботов и диспетчеров
        print("✅ Инициализация ботов...")
        bots = {board: Bot(token=BOT_TOKENS[board]) for board in BOARDS}
        dispatchers = {board: Dispatcher() for board in BOARDS}
        bot_to_board = {bots[board]: board for board in BOARDS}
        print("✅ Боты инициализированы:", list(bots.keys()))

        # Регистрация обработчиков
        for board in BOARDS:
            dp = dispatchers[board]
            dp.message.register(cmd_start, Command("start"))
            dp.message.register(cmd_help, Command("help"))
            dp.message.register(cmd_stats, Command("stats"))
            dp.message.register(cmd_face, Command("face"))
            dp.message.register(cmd_roll, Command("roll"))
            dp.message.register(cmd_invite, Command("invite"))
            dp.message.register(cmd_deanon, Command("deanon"))
            dp.message.register(cmd_zaputin, Command("zaputin"))
            dp.message.register(cmd_slavaukraine, Command("slavaukraine"))
            dp.message.register(cmd_suka_blyat, Command("suka_blyat"))
            dp.message.register(cmd_anime, Command("anime"))
            dp.message.register(cmd_admin, Command("admin"))
            dp.message.register(cmd_id, Command("id"))
            dp.message.register(cmd_ban, Command("ban"))
            dp.message.register(cmd_mute, Command("mute"))
            dp.message.register(cmd_wipe, Command("wipe"))
            dp.message.register(cmd_unmute, Command("unmute"))
            dp.message.register(cmd_unban, Command("unban"))
            dp.message.register(cmd_del, Command("del"))
            dp.message.register(handle_message)
            dp.callback_query.register(admin_save, F.data == "save")
            dp.callback_query.register(admin_stats, F.data == "stats")
            dp.callback_query.register(admin_spammers, F.data == "spammers")
            dp.callback_query.register(admin_banned, F.data == "banned")

        load_state()
        healthcheck_site = None  # Пока отключаем healthcheck, так как он не настроен

        # Запуск фоновых задач
        tasks = await start_background_tasks()
        print("✅ Фоновые задачи запущены")

        # Запускаем polling
        print("✅ Запуск polling...")
        await asyncio.gather(
            *[dispatchers[board].start_polling(bots[board], skip_updates=True) for board in BOARDS]
        )

    except Exception as e:
        print(f"🔥 Critical error: {e}")
    finally:
        if not is_shutting_down:
            await graceful_shutdown()
        if os.path.exists(lock_file):
            os.remove(lock_file)

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(supervisor())
