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
from aiogram.dispatcher.middlewares import BaseMiddleware  # Добавлено для middleware
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
BOARDS = ['b', 'po', 'a', 'sex', 'vg']

BOT_TOKENS = {
    'b': os.environ.get('BOT_TOKEN_B'),
    'po': os.environ.get('BOT_TOKEN_PO'),
    'a': os.environ.get('BOT_TOKEN_A'),
    'sex': os.environ.get('BOT_TOKEN_SEX'),
    'vg': os.environ.get('BOT_TOKEN_VG'),
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

# Глобальный счетчик постов для сквозной нумерации
post_counter = 0

# Состояние для каждой доски
users_data = {board: {'active': set(), 'banned': set()} for board in BOARDS}
mutes = {board: {} for board in BOARDS}
board_modes = {board: {
    'anime_mode': False,
    'zaputin_mode': False,
    'slavaukraine_mode': False,
    'suka_blyat_mode': False
} for board in BOARDS}
shadow_mutes = {board: {} for board in BOARDS}
spam_violations = {board: defaultdict(dict) for board in BOARDS}

last_mode_activation = None
MODE_COOLDOWN = 3600  # 1 час в секундах
last_texts = defaultdict(lambda: deque(maxlen=5))
MAX_ACTIVE_USERS_IN_MEMORY = 5000
last_user_msgs = {}
spam_tracker = defaultdict(list)
messages_storage = {}  # Глобальное хранилище постов
post_to_messages = {}
message_to_post = {}
last_messages = deque(maxlen=300)
last_activity_time = datetime.now()
daily_log = io.StringIO()
sent_media_groups = set()
current_media_groups = {}
media_group_timers = {}

# Отключаем стандартную обработку сигналов в aiogram
os.environ["AIORGRAM_DISABLE_SIGNAL_HANDLERS"] = "1"

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

def suka_blyatify_text(text: str) -> str:
    if not text:
        return text
    words = text.split()
    for i in range(len(words)):
        if random.random() < 0.3:
            words[i] = random.choice(MAT_WORDS)
    return ' '.join(words)

def restore_backup_on_start():
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
    await site.start()
    print(f"🟢 Healthcheck-сервер запущен на порту {port}")
    return site

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")

async def git_commit_and_push():
    async with git_semaphore:
        try:
            token = os.getenv("GITHUB_TOKEN")
            if not token:
                print("❌ Нет GITHUB_TOKEN")
                return False
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(git_executor, sync_git_operations, token)
        except Exception as e:
            print(f"⛔ Ошибка в git_commit_and_push: {str(e)}")
            return False

def sync_git_operations(token: str) -> bool:
    try:
        work_dir = "/tmp/git_backup"
        os.makedirs(work_dir, exist_ok=True)
        repo_url = f"https://{token}@github.com/shlomapetia/dvachbot-backup.git"
        if not os.path.exists(os.path.join(work_dir, ".git")):
            subprocess.run(["git", "clone", repo_url, work_dir], check=True)
        else:
            subprocess.run(["git", "-C", work_dir, "pull"], check=True)
        for fname in ["state.json", "reply_cache.json"]:
            src = os.path.join(os.getcwd(), fname)
            if os.path.exists(src):
                shutil.copy2(src, work_dir)
        subprocess.run(["git", "-C", work_dir, "config", "user.name", "Backup Bot"], check=True)
        subprocess.run(["git", "-C", work_dir, "config", "user.email", "backup@dvachbot.com"], check=True)
        subprocess.run(["git", "-C", work_dir, "add", "."], check=True)
        commit_msg = f"Backup: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}"
        subprocess.run(["git", "-C", work_dir, "commit", "-m", commit_msg], check=True)
        subprocess.run(["git", "-C", work_dir, "push", "-u", "origin", "main"], check=True)
        print("✅ Бэкап сохранён в GitHub")
        return True
    except Exception as e:
        print(f"⛔ Git ошибка: {str(e)}")
        return False

# Middleware для передачи информации о доске
class BoardMiddleware(BaseMiddleware):
    def __init__(self, board):
        self.board = board
        super().__init__()

    async def on_process_message(self, message: types.Message, data: dict):
        data['board'] = self.board

    async def on_process_callback_query(self, callback_query: types.CallbackQuery, data: dict):
        data['board'] = self.board

dp = Dispatcher()
logging.basicConfig(level=logging.WARNING, format="%(message)s")
aiohttp_log = logging.getLogger('aiohttp')
aiohttp_log.setLevel(logging.CRITICAL)
aiogram_log = logging.getLogger('aiogram')
aiogram_log.setLevel(logging.WARNING)

def clean_html_tags(text: str) -> str:
    if not text:
        return text
    return re.sub(r'<[^>]+>', '', text)

def add_you_to_my_posts(text: str, user_id: int) -> str:
    if not text:
        return text
    pattern = r">>(\d+)"
    matches = re.findall(pattern, text)
    for post_str in matches:
        try:
            post_num = int(post_str)
            post_data = messages_storage.get(post_num, {})
            if post_data.get("author_id") == user_id:
                target = f">>{post_num}"
                replacement = f">>{post_num} (You)"
                if target in text and replacement not in text:
                    text = text.replace(target, replacement)
        except (ValueError, KeyError):
            continue
    return text

async def shutdown():
    print("Shutting down...")
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
    for board, bot in bots.items():
        if bot.session:
            await bot.session.close()
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
    await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=5.0)
    print("All background tasks stopped")

async def auto_backup():
    while True:
        try:
            await asyncio.sleep(14400)  # 4 часа
            if is_shutting_down:
                break
            save_reply_cache()
            await save_state()
            print("💾 Автобэкап выполнен")
        except Exception as e:
            print(f"❌ Ошибка в auto_backup: {e}")
            await asyncio.sleep(60)

gc.set_threshold(700, 10, 10)

async def save_state():
    try:
        state_data = {
            'post_counter': post_counter,
            'users_data': {board: {
                'active': list(data['active']),
                'banned': list(data['banned'])
            } for board, data in users_data.items()},
            'mutes': {board: {
                str(user_id): mute_time.isoformat()
                for user_id, mute_time in mutes[board].items()
            } for board in BOARDS},
            'board_modes': board_modes,
            'recent_post_mappings': {str(k): v for k, v in list(post_to_messages.items())[-500:]}
        }
        with open('state.json', 'w', encoding='utf-8') as f:
            json.dump(state_data, f, ensure_ascii=False, indent=2)
        success = await git_commit_and_push()
        if success:
            print("✅ Состояние сохранено и отправлено в GitHub")
        else:
            print("❌ Не удалось отправить в GitHub")
        return success
    except Exception as e:
        print(f"⛔ Ошибка сохранения state: {e}")
        return False

async def cleanup_old_messages():
    while True:
        await asyncio.sleep(7200)
        try:
            current_time = datetime.now(UTC)
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
    if user_id not in last_user_msgs:
        if len(last_user_msgs) >= MAX_ACTIVE_USERS_IN_MEMORY:
            oldest_user = next(iter(last_user_msgs))
            del last_user_msgs[oldest_user]
        last_user_msgs[user_id] = deque(maxlen=10)
    return last_user_msgs[user_id]

BOT_TOKEN = os.environ.get('BOT_TOKEN')
ADMINS = {int(x) for x in os.getenv("ADMINS", "").split(",") if x}
SPAM_LIMIT = 14
SPAM_WINDOW = 15
STATE_FILE = 'state.json'
SAVE_INTERVAL = 14400
STICKER_WINDOW = 10
STICKER_LIMIT = 7
REST_SECONDS = 30
REPLY_CACHE = 500
REPLY_FILE = "reply_cache.json"
MAX_MESSAGES_IN_MEMORY = 1110

WEEKDAYS = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]
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
MAT_WORDS = ["сука", "блядь", "пиздец", "ебать", "нах", "пизда", "хуйня", "ебал", "блять", "отъебись", "ебаный", "еблан", "ХУЙ", "ПИЗДА"]
last_stickers = defaultdict(lambda: deque(maxlen=5))
sticker_times = defaultdict(list)
MSK = timezone(timedelta(hours=3))

async def global_error_handler(event: types.ErrorEvent, board: str) -> bool:
    exception = event.exception
    update = event.update
    if exception is None:
        print(f"⚠️ Event without exception on board {board}")
        return True
    error_msg = f"⚠️ Ошибка на доске {board}: {type(exception).__name__}"
    if str(exception):
        error_msg += f": {exception}"
    print(error_msg)
    if isinstance(exception, TelegramForbiddenError):
        user_id = None
        if update and update.message:
            user_id = update.message.from_user.id
        elif update and update.callback_query:
            user_id = update.callback_query.from_user.id
        if user_id:
            users_data[board]['active'].discard(user_id)
            print(f"🚫 Пользователь {user_id} заблокировал бота на {board}")
        return True
    elif isinstance(exception, (TelegramNetworkError, TelegramConflictError, aiohttp.ClientError)):
        print(f"🌐 Сетевая ошибка на {board}: {exception}")
        await asyncio.sleep(10)
        return False
    elif isinstance(exception, KeyError):
        print(f"🔑 KeyError на {board}: {exception}")
        return True
    else:
        print(f"⛔ Непредвиденная ошибка на {board}: {exception}")
        if update:
            print(f"Update: {update.model_dump_json(exclude_none=True)}")
        await asyncio.sleep(10)
        return False

def escape_html(text: str) -> str:
    if not text:
        return text
    return text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;')

def is_admin(uid: int) -> bool:
    return uid in ADMINS

def save_reply_cache():
    try:
        recent_posts = sorted(messages_storage.keys())[-REPLY_CACHE:]
        new_data = {
            "post_to_messages": {str(p): post_to_messages[p] for p in recent_posts if p in post_to_messages},
            "message_to_post": {f"{uid}_{mid}": p for (uid, mid), p in message_to_post.items() if p in recent_posts},
            "messages_storage_meta": {
                str(p): {
                    "author_id": messages_storage[p].get("author_id", ""),
                    "timestamp": messages_storage[p].get("timestamp", datetime.now(UTC)).isoformat(),
                    "author_msg": messages_storage[p].get("author_message_id"),
                    "board": messages_storage[p].get("board")
                } for p in recent_posts
            }
        }
        with open(REPLY_FILE, 'w', encoding='utf-8') as f:
            json.dump(new_data, f, ensure_ascii=False, indent=2)
            print("✅ reply_cache.json обновлен")
        return True
    except Exception as e:
        print(f"⛔ Ошибка сохранения reply_cache: {str(e)[:200]}")
        return False

def load_state():
    global post_counter
    if not os.path.exists('state.json'):
        return
    with open('state.json', 'r', encoding='utf-8') as f:
        data = json.load(f)
    post_counter = data.get('post_counter', 0)
    for board in BOARDS:
        board_data = data.get('users_data', {}).get(board, {})
        users_data[board]['active'] = set(board_data.get('active', []))
        users_data[board]['banned'] = set(board_data.get('banned', []))
        mutes[board] = {
            int(uid): datetime.fromisoformat(time_str) for uid, time_str in data.get('mutes', {}).get(board, {}).items()
        }
        board_modes[board] = data.get('board_modes', {}).get(board, board_modes[board])
    load_reply_cache()

def load_reply_cache():
    global message_to_post, post_to_messages
    if not os.path.exists(REPLY_FILE):
        return
    try:
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
        post_to_messages[int(p_str)] = {int(uid): mid for uid, mid in mapping.items()}
    for p_str, meta in data.get("messages_storage_meta", {}).items():
        p = int(p_str)
        dt = datetime.fromisoformat(meta['timestamp'])
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        messages_storage[p] = {
            "author_id": meta["author_id"],
            "timestamp": dt,
            "author_message_id": meta.get("author_msg"),
            "board": meta.get("board")
        }
    print(f"reply-cache загружен: {len(post_to_messages)} постов")

async def graceful_shutdown():
    global is_shutting_down
    if is_shutting_down:
        return
    is_shutting_down = True
    print("🛑 Получен сигнал shutdown, сохраняем данные...")
    for board, dp in dispatchers.items():
        try:
            await dp.stop_polling()
            print(f"⏸ Polling остановлен для {board}")
        except Exception as e:
            print(f"⚠️ Не удалось остановить polling для {board}: {e}")
    for board in BOARDS:
        while not message_queues[board].empty():
            await asyncio.sleep(1)
    await save_state()
    await shutdown()

async def auto_memory_cleaner():
    while True:
        await asyncio.sleep(300)
        MAX_POSTS = 100
        if len(post_to_messages) > MAX_POSTS:
            recent = sorted(post_to_messages.keys())[-MAX_POSTS:]
            old_posts = set(post_to_messages.keys()) - set(recent)
            for post in old_posts:
                post_to_messages.pop(post, None)
                messages_storage.pop(post, None)
        valid_posts = set(post_to_messages.keys())
        message_to_post_copy = dict(message_to_post)
        for key, post_num in message_to_post_copy.items():
            if post_num not in valid_posts:
                del message_to_post[key]
        for board in BOARDS:
            if len(spam_tracker) > 100:
                now = datetime.now(UTC)
                for user_id in list(spam_tracker.keys()):
                    spam_tracker[user_id] = [t for t in spam_tracker[user_id] if (now - t).total_seconds() < SPAM_WINDOW]
                    if not spam_tracker[user_id]:
                        del spam_tracker[user_id]
        for _ in range(3):
            gc.collect()

async def check_spam(user_id: int, msg: Message, board: str) -> bool:
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
    if user_id not in spam_violations[board] or not spam_violations[board][user_id]:
        spam_violations[board][user_id] = {
            'level': 0,
            'last_reset': now,
            'last_contents': deque(maxlen=4)
        }
    if (now - spam_violations[board][user_id]['last_reset']) > timedelta(hours=1):
        spam_violations[board][user_id] = {
            'level': 0,
            'last_reset': now,
            'last_contents': deque(maxlen=4)
        }
    if msg_type == 'text' and content:
        spam_violations[board][user_id]['last_contents'].append(content)
        if len(spam_violations[board][user_id]['last_contents']) == rules['max_repeats']:
            if len(set(spam_violations[board][user_id]['last_contents'])) == 1:
                spam_violations[board][user_id]['level'] = min(
                    spam_violations[board][user_id]['level'] + 1,
                    len(rules['penalty']) - 1)
                return False
        if len(spam_violations[board][user_id]['last_contents']) == 4:
            unique = set(spam_violations[board][user_id]['last_contents'])
            if len(unique) == 2:
                contents = list(spam_violations[board][user_id]['last_contents'])
                p1 = [contents[0], contents[1]] * 2
                p2 = [contents[1], contents[0]] * 2
                if contents == p1 or contents == p2:
                    spam_violations[board][user_id]['level'] = min(
                        spam_violations[board][user_id]['level'] + 1,
                        len(rules['penalty']) - 1)
                    return False
    window_start = now - timedelta(seconds=rules['window_sec'])
    spam_tracker[user_id] = [t for t in spam_tracker[user_id] if t > window_start]
    spam_tracker[user_id].append(now)
    if len(spam_tracker[user_id]) >= rules['max_per_window']:
        spam_violations[board][user_id]['level'] = min(
            spam_violations[board][user_id]['level'] + 1,
            len(rules['penalty']) - 1)
        return False
    return True

async def apply_penalty(user_id: int, msg_type: str, board: str):
    rules = SPAM_RULES.get(msg_type, {})
    if not rules:
        return
    level = spam_violations[board].get(user_id, {}).get('level', 0)
    level = min(level, len(rules.get('penalty', [])) - 1)
    mute_seconds = rules['penalty'][level] if rules.get('penalty') else 30
    mutes[board][user_id] = datetime.now(UTC) + timedelta(seconds=mute_seconds)
    violation_type = {'text': "текстовый спам", 'sticker': "спам стикерами", 'animation': "спам гифками"}.get(msg_type, "спам")
    try:
        if mute_seconds < 60:
            time_str = f"{mute_seconds} сек"
        elif mute_seconds < 3600:
            time_str = f"{mute_seconds // 60} мин"
        else:
            time_str = f"{mute_seconds // 3600} час"
        await bots[board].send_message(
            user_id,
            f"🚫 Эй пидор ты в муте на {time_str} за {violation_type}\nСпамишь дальше - получишь бан",
            parse_mode="HTML")
        await send_moderation_notice(user_id, "mute", time_str, 0, board)
    except Exception as e:
        print(f"Ошибка отправки уведомления о муте на {board}: {e}")

def format_header(board: str) -> Tuple[str, int]:
    global post_counter
    post_counter += 1
    post_num = post_counter
    if board_modes[board]['slavaukraine_mode']:
        return f"💙💛 Пiст №{post_num}", post_num
    if board_modes[board]['zaputin_mode']:
        return f"🇷🇺 Пост №{post_num}", post_num
    if board_modes[board]['anime_mode']:
        return f"🌸 投稿 {post_num} 番", post_num
    if board_modes[board]['suka_blyat_mode']:
        return f"💢 Пост №{post_num}", post_num
    rand = random.random()
    circle = "🔴 " if rand < 0.003 else "🟢 " if rand < 0.006 else ""
    prefix = ""
    rand_prefix = random.random()
    prefixes = [
        (0.005, "### АДМИН ### "),
        (0.008, "Абу - "),
        (0.01, "Пидор - "),
        (0.012, "### ДЖУЛУП ### "),
        (0.014, "### Хуесос ### "),
        (0.016, "Пыня - "),
        (0.018, "Нариман Намазов - "),
        (0.021, "ИМПЕРАТОР КОНАН - "),
        (0.023, "Антон Бабкин - "),
        (0.025, "### НАРИМАН НАМАЗОВ ### "),
        (0.027, "### ПУТИН ### "),
        (0.028, "Гей - "),
        (0.030, "Анархист - "),
        (0.033, "### Имбецил ### "),
        (0.035, "### ЧМО ### "),
        (0.037, "### ОНАНИСТ ### "),
        (0.040, "### ЧЕЧЕНЕЦ ### "),
        (0.042, "Томоко Куроки - "),
        (0.044, "### Аниме девочка ### ")
    ]
    for threshold, pref in prefixes:
        if rand_prefix < threshold:
            prefix = pref
            break
    return f"{circle}{prefix}Пост №{post_num}", post_num

async def delete_user_posts(user_id: int, time_period_minutes: int, board: str) -> int:
    try:
        time_threshold = datetime.now(UTC) - timedelta(minutes=time_period_minutes)
        posts_to_delete = [
            pnum for pnum, data in messages_storage.items()
            if data.get('author_id') == user_id and data.get('board') == board and data.get('timestamp', datetime.now(UTC)) >= time_threshold
        ]
        deleted_messages = 0
        messages_to_delete = []
        for post_num in posts_to_delete:
            if post_num in post_to_messages:
                for uid, mid in post_to_messages[post_num].items():
                    messages_to_delete.append((uid, mid))
        for (uid, mid) in messages_to_delete:
            try:
                for _ in range(3):
                    try:
                        await bots[board].delete_message(uid, mid)
                        deleted_messages += 1
                        break
                    except TelegramBadRequest as e:
                        if "message to delete not found" in str(e):
                            break
                        await asyncio.sleep(0.5)
            except Exception as e:
                print(f"Ошибка удаления {mid} у {uid} на {board}: {e}")
        for post_num in posts_to_delete:
            post_to_messages.pop(post_num, None)
            messages_storage.pop(post_num, None)
            global message_to_post
            message_to_post = {k: v for k, v in message_to_post.items() if v != post_num}
        return deleted_messages
    except Exception as e:
        print(f"Ошибка в delete_user_posts на {board}: {e}")
        return 0

async def send_moderation_notice(user_id: int, action: str, duration: str = None, deleted_posts: int = 0, board: str = 'b'):
    global post_counter
    post_counter += 1
    post_num = post_counter
    header = "### Админ ###"
    if action == "ban":
        text = f"🚨 Хуесос был забанен за спам. Помянем."
    elif action == "mute":
        text = f"🔇 Ебаного пидораса замутили на {duration}. Хорош спамить, хуйло ебаное!"
    else:
        return
    messages_storage[post_num] = {
        'author_id': 0,
        'timestamp': datetime.now(UTC),
        'content': {'type': 'text', 'header': header, 'text': text},
        'board': board
    }
    await message_queues[board].put({
        "recipients": users_data[board]['active'],
        "content": {'type': 'text', 'header': header, 'text': text},
        "post_num": post_num,
    })

async def cmd_start(message: Message, board: str):
    await bots[board].send_message(
        message.from_user.id,
        f"Добро пожаловать в {BOARD_INFO[board]['name']}!\n\n{Help_TEXT}",
        parse_mode="HTML"
    )
    users_data[board]['active'].add(message.from_user.id)
    await message.delete()

async def cmd_help(message: Message, board: str):
    await bots[board].send_message(
        message.from_user.id,
        HELP_TEXT,
        parse_mode="HTML"
    )
    await message.delete()

async def cmd_stats(message: Message, board: str):
    await bots[board].send_message(
        message.from_user.id,
        f"Статистика {BOARD_INFO[board]['name']}:\n\n"
        f"Активных: {len(users_data[board]['active'])}\n"
        f"Забаненных: {len(users_data[board]['banned'])}\n"
        f"Всего постов: {post_counter}",
        parse_mode="HTML"
    )
    await message.delete()

async def cmd_roll(message: Message, board: str):
    roll = random.randint(1, 100)
    global post_counter
    post_counter += 1
    post_num = post_counter
    header = f"Ролл №{post_num}"
    text = f"Анон {message.from_user.id} выбросил {roll}"
    messages_storage[post_num] = {
        'author_id': message.from_user.id,
        'timestamp': datetime.now(UTC),
        'content': {'type': 'text', 'header': header, 'text': text},
        'board': board
    }
    await message_queues[board].put({
        "recipients": users_data[board]['active'],
        "content": {'type': 'text', 'header': header, 'text': text},
        "post_num": post_num,
    })
    await message.delete()

async def cmd_invite(message: Message, board: str):
    motivation = random.choice(MOTIVATIONAL_MESSAGES)
    invite = random.choice(INVITE_TEXTS)
    await bots[board].send_message(
        message.from_user.id,
        f"{motivation}\n\nПриглашение:\n{invite}",
        parse_mode="HTML"
    )
    await message.delete()

async def check_cooldown(message: Message, board: str) -> bool:
    global last_mode_activation
    now = datetime.now(UTC)
    if last_mode_activation and (now - last_mode_activation).total_seconds() < MODE_COOLDOWN:
        time_left = MODE_COOLDOWN - int((now - last_mode_activation).total_seconds())
        await bots[board].send_message(
            message.from_user.id,
            f"Режим на кулдауне! Осталось {time_left // 60} мин {time_left % 60} сек",
            parse_mode="HTML"
        )
        await message.delete()
        return False
    return True

async def cmd_suka_blyat(message: Message, board: str):
    if not await check_cooldown(message, board):
        return
    global last_mode_activation
    board_modes[board]['suka_blyat_mode'] = True
    board_modes[board]['zaputin_mode'] = False
    board_modes[board]['slavaukraine_mode'] = False
    board_modes[board]['anime_mode'] = False
    last_mode_activation = datetime.now(UTC)
    global post_counter
    post_counter += 1
    pnum = post_counter
    header = "### Админ ###"
    activation_text = "💢💢💢 Активирован режим СУКА БЛЯТЬ! 💢💢💢\n\nВсех нахуй разъебало!"
    await message_queues[board].put({
        "recipients": users_data[board]['active'],
        "content": {'type': 'text', 'header': header, 'text': activation_text},
        "post_num": pnum,
    })
    asyncio.create_task(disable_suka_blyat_mode(300, board))
    await message.delete()

async def disable_suka_blyat_mode(delay: int, board: str):
    await asyncio.sleep(delay)
    board_modes[board]['suka_blyat_mode'] = False
    global post_counter
    post_counter += 1
    pnum = post_counter
    header = "### Админ ###"
    end_text = "💀 СУКА БЛЯТЬ КОНЧИЛОСЬ. Теперь можно и помолчать."
    await message_queues[board].put({
        "recipients": users_data[board]['active'],
        "content": {'type': 'text', 'header': header, 'text': end_text},
        "post_num": pnum,
    })

async def cmd_zaputin(message: Message, board: str):
    if not await check_cooldown(message, board):
        return
    global last_mode_activation
    board_modes[board]['zaputin_mode'] = True
    board_modes[board]['suka_blyat_mode'] = False
    board_modes[board]['slavaukraine_mode'] = False
    board_modes[board]['anime_mode'] = False
    last_mode_activation = datetime.now(UTC)
    global post_counter
    post_counter += 1
    pnum = post_counter
    header = "### Админ ###"
    activation_text = "🇷🇺 СЛАВА РОССИИ! ПУТИН - НАШ ПРЕЗИДЕНТ! 🇷🇺\n\nАктивирован режим кремлеботов!"
    await message_queues[board].put({
        "recipients": users_data[board]['active'],
        "content": {'type': 'text', 'header': header, 'text': activation_text},
        "post_num": pnum,
    })
    asyncio.create_task(disable_zaputin_mode(300, board))
    await message.delete()

async def disable_zaputin_mode(delay: int, board: str):
    await asyncio.sleep(delay)
    board_modes[board]['zaputin_mode'] = False
    global post_counter
    post_counter += 1
    pnum = post_counter
    header = "### Админ ###"
    end_text = "💀 Бунт кремлеботов окончился. Всем спасибо, все свободны."
    await message_queues[board].put({
        "recipients": users_data[board]['active'],
        "content": {'type': 'text', 'header': header, 'text': end_text},
        "post_num": pnum,
    })

async def cmd_slavaukraine(message: Message, board: str):
    if not await check_cooldown(message, board):
        return
    global last_mode_activation
    board_modes[board]['slavaukraine_mode'] = True
    board_modes[board]['suka_blyat_mode'] = False
    board_modes[board]['zaputin_mode'] = False
    board_modes[board]['anime_mode'] = False
    last_mode_activation = datetime.now(UTC)
    global post_counter
    post_counter += 1
    pnum = post_counter
    header = "### Админ ###"
    activation_text = "💙💛 СЛАВА УКРАЇНІ! ГЕРОЯМ СЛАВА! 💙💛\n\nАктивирован режим свидомых!"
    await message_queues[board].put({
        "recipients": users_data[board]['active'],
        "content": {'type': 'text', 'header': header, 'text': activation_text},
        "post_num": pnum,
    })
    asyncio.create_task(disable_slavaukraine_mode(300, board))
    await message.delete()

async def disable_slavaukraine_mode(delay: int, board: str):
    await asyncio.sleep(delay)
    board_modes[board]['slavaukraine_mode'] = False
    global post_counter
    post_counter += 1
    pnum = post_counter
    header = "### Админ ###"
    end_text = "💙💛 Слава нації — режим відключено. Повертаємось до звичайного чату."
    await message_queues[board].put({
        "recipients": users_data[board]['active'],
        "content": {'type': 'text', 'header': header, 'text': end_text},
        "post_num": pnum,
    })

async def cmd_anime(message: Message, board: str):
    if not await check_cooldown(message, board):
        return
    global last_mode_activation
    board_modes[board]['anime_mode'] = True
    board_modes[board]['suka_blyat_mode'] = False
    board_modes[board]['zaputin_mode'] = False
    board_modes[board]['slavaukraine_mode'] = False
    last_mode_activation = datetime.now(UTC)
    global post_counter
    post_counter += 1
    pnum = post_counter
    header = "### Админ ###"
    activation_text = "🌸 アニメモードがアクティブされました！\n\n^_^"
    await message_queues[board].put({
        "recipients": users_data[board]['active'],
        "content": {'type': 'text', 'header': header, 'text': activation_text},
        "post_num": pnum,
    })
    asyncio.create_task(disable_anime_mode(300, board))
    await message.delete()

async def disable_anime_mode(delay: int, board: str):
    await asyncio.sleep(delay)
    board_modes[board]['anime_mode'] = False
    global post_counter
    post_counter += 1
    pnum = post_counter
    header = "### Админ ###"
    end_text = "アニメモードが終了しました！\n\n通常のチャットに戻ります！"
    await message_queues[board].put({
        "recipients": users_data[board]['active'],
        "content": {'type': 'text', 'header': header, 'text': end_text},
        "post_num": pnum,
    })

async def cmd_deanon(message: Message, board: str):
    if not message.reply_to_message:
        await bots[board].send_message(message.from_user.id, "⚠️ Ответь на сообщение для деанона!", parse_mode="HTML")
        await message.delete()
        return
    reply_key = (message.from_user.id, message.reply_to_message.message_id)
    target_post = message_to_post.get(reply_key)
    if not target_post or target_post not in messages_storage:
        await bots[board].send_message(message.from_user.id, "🚫 Не удалось найти пост для деанона!", parse_mode="HTML")
        await message.delete()
        return
    target_id = messages_storage[target_post].get("author_id")
    name, surname, city, profession, fetish, detail = generate_deanon_info()
    ip = f"{random.randint(10,250)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(0,255)}"
    age = random.randint(18, 45)
    deanon_text = (
        f"\nЭтого анона зовут: {name} {surname}\n"
        f"Возраст: {age}\n"
        f"Адрес проживания: {city}\n"
        f"Профессия: {profession}\n"
        f"Фетиш: {fetish}\n"
        f"IP-адрес: {ip}\n"
        f"Дополнительная информация о нём: {detail}"
    )
    global post_counter
    post_counter += 1
    pnum = post_counter
    header = "### ДЕАНОН ###"
    await message_queues[board].put({
        "recipients": users_data[board]['active'],
        "content": {'type': 'text', 'header': header, 'text': deanon_text, 'reply_to_post': target_post},
        "post_num": pnum,
        "reply_info": post_to_messages.get(target_post, {})
    })
    await message.delete()

async def cmd_admin(message: Message, board: str):
    if not is_admin(message.from_user.id):
        await message.delete()
        return
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Статистика", callback_data="stats"), InlineKeyboardButton(text="🚫 Забаненные", callback_data="banned")],
        [InlineKeyboardButton(text="💾 Сохранить", callback_data="save"), InlineKeyboardButton(text="👥 Топ спамеров", callback_data="spammers")]
    ])
    await bots[board].send_message(
        message.from_user.id,
        f"Админка {BOARD_INFO[board]['name']}:\n\nКоманды:\n/ban /mute /del /wipe /unban /unmute\n",
        reply_markup=keyboard,
        parse_mode="HTML"
    )
    await message.delete()

def get_author_id_by_reply(msg: Message) -> int | None:
    if not msg.reply_to_message:
        return None
    reply_mid = msg.reply_to_message.message_id
    for (uid, mid), pnum in message_to_post.items():
        if mid == reply_mid:
            return messages_storage.get(pnum, {}).get("author_id")
    return None

async def cmd_id(message: Message, board: str):
    if message.reply_to_message:
        author_id = get_author_id_by_reply(message)
        if author_id:
            try:
                author = await bots[board].get_chat(author_id)
                info = "🆔 <b>Информация об авторе:</b>\n\n"
                info += f"ID: <code>{author_id}</code>\n"
                info += f"Имя: {author.first_name}\n"
                if author.last_name:
                    info += f"Фамилия: {author.last_name}\n"
                if author.username:
                    info += f"Username: @{author.username}\n"
                if author_id in users_data[board]['banned']:
                    info += "\n⛔️ Статус: ЗАБАНЕН"
                elif author_id in users_data[board]['active']:
                    info += "\n✅ Статус: Активен"
                await bots[board].send_message(message.from_user.id, info, parse_mode="HTML")
            except:
                await bots[board].send_message(message.from_user.id, f"ID автора: <code>{author_id}</code>", parse_mode="HTML")
        else:
            await bots[board].send_message(message.from_user.id, "Не удалось определить автора.")
    await message.delete()

async def cmd_ban(message: Message, board: str):
    if not is_admin(message.from_user.id):
        await message.delete()
        return
    target_id = None
    if message.reply_to_message:
        target_id = get_author_id_by_reply(message)
    parts = message.text.split()
    if len(parts) == 2 and parts[1].isdigit():
        target_id = int(parts[1])
    if not target_id:
        await bots[board].send_message(message.from_user.id, "Нужно ответить на сообщение или указать ID: /ban <id>", parse_mode="HTML")
        await message.delete()
        return
    deleted_posts = await delete_user_posts(target_id, 5, board)
    users_data[board]['banned'].add(target_id)
    users_data[board]['active'].discard(target_id)
    await bots[board].send_message(
        message.from_user.id,
        f"✅ Хуесос под номером <code>{target_id}</code> забанен\nУдалено его постов за последние 5 минут: {deleted_posts}",
        parse_mode="HTML"
    )
    await send_moderation_notice(target_id, "ban", None, deleted_posts, board)
    try:
        await bots[board].send_message(
            target_id,
            f"Пидорас ебаный, ты нас так заебал, что тебя блокнули нахуй.\nУдалено твоих постов за последние 5 минут: {deleted_posts}\nПиздуй отсюда.",
            parse_mode="HTML"
        )
    except:
        pass
    await message.delete()

async def cmd_mute(message: Message, command: CommandObject, board: str):
    if not is_admin(message.from_user.id):
        await message.delete()
        return
    args = command.args
    if not args:
        await bots[board].send_message(
            message.from_user.id,
            "Использование:\nОтветьте на сообщение + /mute [время]\nИли: /mute <user_id> [время]\n\nПримеры:\n/mute 123456789 1h\n/mute 123456789 30m",
            parse_mode="HTML"
        )
        await message.delete()
        return
    target_id = None
    duration_str = "24h"
    if message.reply_to_message:
        reply_key = (message.from_user.id, message.reply_to_message.message_id)
        post_num = message_to_post.get(reply_key)
        if post_num:
            target_id = messages_storage.get(post_num, {}).get('author_id')
        if target_id and args:
            duration_str = args
    else:
        parts = args.split()
        if len(parts) >= 1:
            try:
                target_id = int(parts[0])
                if len(parts) >= 2:
                    duration_str = parts[1]
            except ValueError:
                await bots[board].send_message(message.from_user.id, "❌ Неверный ID пользователя", parse_mode="HTML")
                await message.delete()
                return
    if not target_id:
        await bots[board].send_message(message.from_user.id, "❌ Не удалось определить пользователя", parse_mode="HTML")
        await message.delete()
        return
    try:
        duration_str = duration_str.lower().replace(" ", "")
        if duration_str.endswith("m"):
            mute_seconds = int(duration_str[:-1]) * 60
            duration_text = f"{int(duration_str[:-1])} минут"
        elif duration_str.endswith("h"):
            mute_seconds = int(duration_str[:-1]) * 3600
            duration_text = f"{int(duration_str[:-1])} часов"
        elif duration_str.endswith("d"):
            mute_seconds = int(duration_str[:-1]) * 86400
            duration_text = f"{int(duration_str[:-1])} дней"
        else:
            mute_seconds = int(duration_str) * 60
            duration_text = f"{int(duration_str)} минут"
        mute_seconds = min(mute_seconds, 2592000)
    except (ValueError, AttributeError):
        await bots[board].send_message(
            message.from_user.id,
            "❌ Неверный формат времени. Примеры:\n30m - 30 минут\n2h - 2 часа\n1d - 1 день",
            parse_mode="HTML"
        )
        await message.delete()
        return
    deleted_count = await delete_user_posts(target_id, 5, board)
    mutes[board][target_id] = datetime.now(UTC) + timedelta(seconds=mute_seconds)
    await bots[board].send_message(
        message.from_user.id,
        f"🔇 Хуила {target_id} замучен на {duration_text}\nУдалено сообщений за последние 5 минут: {deleted_count}",
        parse_mode="HTML"
    )
    await send_moderation_notice(target_id, "mute", duration_text, deleted_count, board)
    try:
        await bots[board].send_message(
            target_id,
            f"🔇 Пидор ебаный хорош спамить, посиди в муте {duration_text}.\nУдалено твоих сообщений: {deleted_count}",
            parse_mode="HTML"
        )
    except:
        pass
    await message.delete()

async def cmd_wipe(message: Message, board: str):
    if not is_admin(message.from_user.id):
        await message.delete()
        return
    target_id = None
    if message.reply_to_message:
        target_id = get_author_id_by_reply(message)
    else:
        parts = message.text.split()
        if len(parts) == 2 and parts[1].isdigit():
            target_id = int(parts[1])
    if not target_id:
        await bots[board].send_message(message.from_user.id, "reply + /wipe  или  /wipe <id>", parse_mode="HTML")
        await message.delete()
        return
    posts_to_delete = [
        pnum for pnum, info in messages_storage.items()
        if info.get("author_id") == target_id and info.get("board") == board
    ]
    deleted = 0
    for pnum in posts_to_delete:
        for uid, mid in post_to_messages.get(pnum, {}).items():
            try:
                await bots[board].delete_message(uid, mid)
                deleted += 1
            except:
                pass
        author_mid = messages_storage[pnum].get("author_message_id")
        if author_mid:
            try:
                await bots[board].delete_message(target_id, author_mid)
                deleted += 1
            except:
                pass
        post_to_messages.pop(pnum, None)
        messages_storage.pop(pnum, None)
    global message_to_post
    message_to_post = {k: v for k, v in message_to_post.items() if v not in posts_to_delete}
    await bots[board].send_message(
        message.from_user.id,
        f"🗑 Удалено {len(posts_to_delete)} постов ({deleted} сообщений) пользователя {target_id}",
        parse_mode="HTML"
    )
    await message.delete()

async def cmd_unmute(message: Message, board: str):
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
        await bots[board].send_message(message.from_user.id, "Нужно reply или /unmute <id>", parse_mode="HTML")
        await message.delete()
        return
    mutes[board].pop(target_id, None)
    await bots[board].send_message(message.from_user.id, f"🔈 Пользователь {target_id} размучен", parse_mode="HTML")
    try:
        await bots[board].send_message(target_id, "Эй хуйло ебаное, тебя размутили, можешь писать.", parse_mode="HTML")
    except:
        pass
    await message.delete()

async def cmd_unban(message: Message, board: str):
    if not is_admin(message.from_user.id):
        await message.delete()
        return
    args = message.text.split()
    if len(args) < 2:
        await bots[board].send_message(message.from_user.id, "Использование: /unban <user_id>", parse_mode="HTML")
        await message.delete()
        return
    try:
        user_id = int(args[1])
        users_data[board]['banned'].discard(user_id)
        await bots[board].send_message(message.from_user.id, f"Пользователь {user_id} разбанен", parse_mode="HTML")
    except ValueError:
        await bots[board].send_message(message.from_user.id, "Неверный ID пользователя", parse_mode="HTML")
    await message.delete()

async def cmd_del(message: Message, board: str):
    if not is_admin(message.from_user.id):
        await message.delete()
        return
    if not message.reply_to_message:
        await bots[board].send_message(message.from_user.id, "Ответь на сообщение, которое нужно удалить", parse_mode="HTML")
        await message.delete()
        return
    target_mid = message.reply_to_message.message_id
    post_num = None
    for (uid, mid), pnum in message_to_post.items():
        if mid == target_mid:
            post_num = pnum
            break
    if post_num is None:
        await bots[board].send_message(message.from_user.id, "Не нашёл этот пост в базе", parse_mode="HTML")
        await message.delete()
        return
    deleted = 0
    if post_num in post_to_messages:
        for uid, mid in post_to_messages[post_num].items():
            try:
                await bots[board].delete_message(uid, mid)
                deleted += 1
            except:
                pass
    author_mid = messages_storage.get(post_num, {}).get('author_message_id')
    author_id = messages_storage.get(post_num, {}).get('author_id')
    if author_mid and author_id:
        try:
            await bots[board].delete_message(author_id, author_mid)
            deleted += 1
        except:
            pass
    post_to_messages.pop(post_num, None)
    messages_storage.pop(post_num, None)
    global message_to_post
    message_to_post = {k: v for k, v in message_to_post.items() if v != post_num}
    await bots[board].send_message(
        message.from_user.id,
        f"Пост №{post_num} удалён у {deleted} пользователей",
        parse_mode="HTML"
    )
    await message.delete()

async def admin_save(callback: types.CallbackQuery, board: str):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа")
        return
    await save_state()
    await callback.answer("Состояние сохранено")

async def admin_stats(callback: types.CallbackQuery, board: str):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа")
        return
    await bots[board].send_message(
        callback.from_user.id,
        f"Статистика {BOARD_INFO[board]['name']}:\n\n"
        f"Активных: {len(users_data[board]['active'])}\n"
        f"Забаненных: {len(users_data[board]['banned'])}\n"
        f"Всего постов: {post_counter}\n"
        f"Сообщений в памяти: {len(messages_storage)}\n"
        f"В очереди: {message_queues[board].qsize()}",
        parse_mode="HTML"
    )
    await callback.answer()

async def admin_spammers(callback: types.CallbackQuery, board: str):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа")
        return
    sorted_users = sorted(
        [(uid, len(spam_tracker[uid])) for uid in spam_tracker if uid in users_data[board]['active']],
        key=lambda x: x[1],
        reverse=True
    )[:10]
    text = f"Топ 10 спамеров {BOARD_INFO[board]['name']}:\n\n"
    for user_id, count in sorted_users:
        text += f"ID {user_id}: {count} сообщений\n"
    await bots[board].send_message(callback.from_user.id, text, parse_mode="HTML")
    await callback.answer()

async def admin_banned(callback: types.CallbackQuery, board: str):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа")
        return
    if not users_data[board]['banned']:
        await callback.answer("Нет забаненных пользователей")
        return
    text = f"Забаненные пользователи {BOARD_INFO[board]['name']}:\n\n"
    for user_id in users_data[board]['banned']:
        text += f"ID {user_id}\n"
    await bots[board].send_message(callback.from_user.id, text, parse_mode="HTML")
    await callback.answer()

async def process_complete_media_group(media_group_id: str, board: str):
    if media_group_id in sent_media_groups or media_group_id not in current_media_groups:
        return
    group = current_media_groups[media_group_id]
    if not group['media']:
        del current_media_groups[media_group_id]
        return
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
        'reply_to': group.get('reply_to_post'),
        'board': board
    }
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
        sent_messages = await bots[board].send_media_group(
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
        print(f"Ошибка отправки медиа-альбома автору на {board}: {e}")
    recipients = users_data[board]['active'] - {user_id}
    if recipients:
        await message_queues[board].put({
            'recipients': recipients,
            'content': content,
            'post_num': post_num,
            'reply_info': post_to_messages.get(group['reply_to_post'], {}) if group.get('reply_to_post') else None
        })
    if media_group_id in current_media_groups:
        del current_media_groups[media_group_id]

async def handle_audio(message: Message, board: str):
    user_id = message.from_user.id
    if user_id in users_data[board]['banned']:
        await message.delete()
        return
    if mutes[board].get(user_id) and mutes[board][user_id] > datetime.now(UTC):
        await message.delete()
        return
    header, current_post_num = format_header(board)
    reply_to_post = None
    reply_info = {}
    if message.reply_to_message:
        reply_mid = message.reply_to_message.message_id
        for (uid, mid), pnum in message_to_post.items():
            if mid == reply_mid:
                reply_to_post = pnum
                break
        if reply_to_post and reply_to_post in post_to_messages and messages_storage[reply_to_post]['board'] == board:
            reply_info = post_to_messages[reply_to_post]
        else:
            reply_to_post = None
    try:
        await message.delete()
    except:
        pass
    caption = message.caption
    if board_modes[board]['slavaukraine_mode'] and caption:
        caption = ukrainian_transform(caption)
    if board_modes[board]['suka_blyat_mode'] and caption:
        caption = suka_blyatify_text(caption)
    if board_modes[board]['anime_mode'] and caption:
        caption = anime_transform(caption)
    if board_modes[board]['zaputin_mode'] and caption:
        caption = zaputin_transform(caption)
    content = {
        'type': 'audio',
        'header': header,
        'file_id': message.audio.file_id,
        'caption': caption,
        'reply_to_post': reply_to_post
    }
    messages_storage[current_post_num] = {
        'author_id': user_id,
        'timestamp': datetime.now(UTC),
        'content': content,
        'reply_to': reply_to_post,
        'board': board
    }
    reply_to_message_id = reply_info.get(user_id) if reply_info else None
    caption_text = f"<i>{header}</i>"
    if caption:
        caption_text += f"\n\n{escape_html(caption)}"
    sent_to_author = await bots[board].send_audio(
        user_id,
        message.audio.file_id,
        caption=caption_text,
        reply_to_message_id=reply_to_message_id,
        parse_mode="HTML"
    )
    if sent_to_author:
        messages_storage[current_post_num]['author_message_id'] = sent_to_author.message_id
        if current_post_num not in post_to_messages:
            post_to_messages[current_post_num] = {}
        post_to_messages[current_post_num][user_id] = sent_to_author.message_id
        message_to_post[(user_id, sent_to_author.message_id)] = current_post_num
    recipients = users_data[board]['active'] - {user_id}
    if recipients:
        await message_queues[board].put({
            'recipients': recipients,
            'content': content,
            'post_num': current_post_num,
            'reply_info': reply_info if reply_info else None,
            'user_id': user_id
        })

async def handle_voice(message: Message, board: str):
    user_id = message.from_user.id
    if user_id in users_data[board]['banned']:
        await message.delete()
        return
    if mutes[board].get(user_id) and mutes[board][user_id] > datetime.now(UTC):
        await message.delete()
        return
    header, current_post_num = format_header(board)
    reply_to_post = None
    reply_info = {}
    if message.reply_to_message:
        reply_mid = message.reply_to_message.message_id
        for (uid, mid), pnum in message_to_post.items():
            if mid == reply_mid:
                reply_to_post = pnum
                break
        if reply_to_post and reply_to_post in post_to_messages and messages_storage[reply_to_post]['board'] == board:
            reply_info = post_to_messages[reply_to_post]
        else:
            reply_to_post = None
    try:
        await message.delete()
    except:
        pass
    content = {
        'type': 'voice',
        'header': header,
        'file_id': message.voice.file_id,
        'reply_to_post': reply_to_post
    }
    messages_storage[current_post_num] = {
        'author_id': user_id,
        'timestamp': datetime.now(UTC),
        'content': content,
        'reply_to': reply_to_post,
        'board': board
    }
    reply_to_message_id = reply_info.get(user_id) if reply_info else None
    sent_to_author = await bots[board].send_voice(
        user_id,
        message.voice.file_id,
        caption=f"<i>{header}</i>",
        reply_to_message_id=reply_to_message_id,
        parse_mode="HTML"
    )
    if sent_to_author:
        messages_storage[current_post_num]['author_message_id'] = sent_to_author.message_id
        if current_post_num not in post_to_messages:
            post_to_messages[current_post_num] = {}
        post_to_messages[current_post_num][user_id] = sent_to_author.message_id
        message_to_post[(user_id, sent_to_author.message_id)] = current_post_num
    recipients = users_data[board]['active'] - {user_id}
    if recipients:
        await message_queues[board].put({
            'recipients': recipients,
            'content': content,
            'post_num': current_post_num,
            'reply_info': reply_info if reply_info else None,
            'user_id': user_id
        })

async def handle_media_group_init(message: Message, board: str):
    user_id = message.from_user.id
    if user_id in users_data[board]['banned']:
        await message.delete()
        return
    if mutes[board].get(user_id) and mutes[board][user_id] > datetime.now(UTC):
        await message.delete()
        return
    media_group_id = message.media_group_id
    if not media_group_id or media_group_id in sent_media_groups:
        await message.delete()
        return
    reply_to_post = None
    if message.reply_to_message:
        reply_mid = message.reply_to_message.message_id
        for (uid, mid), pnum in message_to_post.items():
            if mid == reply_mid:
                reply_to_post = pnum
                break
        if reply_to_post and (reply_to_post not in messages_storage or messages_storage[reply_to_post]['board'] != board):
            reply_to_post = None
    if media_group_id not in current_media_groups:
        header, post_num = format_header(board)
        caption = message.caption or ""
        if board_modes[board]['slavaukraine_mode'] and caption:
            caption = ukrainian_transform(caption)
        elif board_modes[board]['suka_blyat_mode'] and caption:
            caption = suka_blyatify_text(caption)
        elif board_modes[board]['anime_mode'] and caption:
            caption = anime_transform(caption)
        elif board_modes[board]['zaputin_mode'] and caption:
            caption = zaputin_transform(caption)
        current_media_groups[media_group_id] = {
            'post_num': post_num,
            'header': header,
            'author_id': user_id,
            'timestamp': datetime.now(UTC),
            'media': [],
            'caption': caption,
            'reply_to_post': reply_to_post,
            'processed_messages': set(),
            'board': board
        }
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
    if media_group_id in media_group_timers:
        media_group_timers[media_group_id].cancel()
    if media_group_id not in sent_media_groups:
        media_group_timers[media_group_id] = asyncio.create_task(
            complete_media_group_after_delay(media_group_id, 1.5, board)
        )

async def complete_media_group_after_delay(media_group_id, delay, board):
    try:
        await asyncio.sleep(delay)
        if media_group_id in sent_media_groups:
            return
        await process_complete_media_group(media_group_id, board)
    except asyncio.CancelledError:
        pass
    finally:
        if media_group_id in media_group_timers:
            del media_group_timers[media_group_id]

async def handle_message(message: Message, board: str):
    user_id = message.from_user.id
    if user_id in shadow_mutes[board] and shadow_mutes[board][user_id] > datetime.now(UTC):
        try:
            await message.delete()
        except:
            pass
        header = f"Пост №{post_counter + 1}"
        try:
            if message.text:
                text = message.text
                if board_modes[board]['suka_blyat_mode']:
                    text = suka_blyatify_text(text)
                if board_modes[board]['slavaukraine_mode']:
                    text = ukrainian_transform(text)
                if board_modes[board]['anime_mode']:
                    text = anime_transform(text)
                if board_modes[board]['zaputin_mode']:
                    text = zaputin_transform(text)
                await bots[board].send_message(
                    user_id,
                    f"<i>{header}</i>\n\n{escape_html(text)}",
                    parse_mode="HTML"
                )
            elif message.photo:
                caption = message.caption or ""
                if board_modes[board]['suka_blyat_mode'] and caption:
                    caption = suka_blyatify_text(caption)
                if board_modes[board]['slavaukraine_mode'] and caption:
                    caption = ukrainian_transform(caption)
                if board_modes[board]['anime_mode'] and caption:
                    caption = anime_transform(caption)
                if board_modes[board]['zaputin_mode'] and caption:
                    caption = zaputin_transform(caption)
                full_caption = f"<i>{header}</i>"
                if caption:
                    full_caption += f"\n\n{escape_html(caption)}"
                await bots[board].send_photo(
                    user_id,
                    message.photo[-1].file_id,
                    caption=full_caption,
                    parse_mode="HTML"
                )
        except Exception as e:
            print(f"Ошибка фантомной отправки на {board}: {e}")
        return
    if not message.text and not message.caption and not message.content_type:
        await message.delete()
        return
    if message.media_group_id:
        return
    try:
        until = mutes[board].get(user_id)
        if until and until > datetime.now(UTC):
            left = until - datetime.now(UTC)
            minutes = int(left.total_seconds() // 60)
            seconds = int(left.total_seconds() % 60)
            try:
                await message.delete()
                await bots[board].send_message(
                    user_id,
                    f"🔇 Эй пидор, ты в муте ещё {minutes}м {seconds}с\nСпамишь дальше - получишь бан",
                    parse_mode="HTML"
                )
            except:
                pass
            return
        elif until:
            mutes[board].pop(user_id, None)
        if user_id not in users_data[board]['active']:
            users_data[board]['active'].add(user_id)
            print(f"✅ Добавлен новый пользователь на {board}: ID {user_id}")
        if user_id in users_data[board]['banned']:
            await message.delete()
            await bots[board].send_message(user_id, "❌ Ты забанен", parse_mode="HTML")
            return
        get_user_msgs_deque(user_id).append(message)
        spam_check = await check_spam(user_id, message, board)
        if not spam_check:
            await message.delete()
            msg_type = message.content_type
            if message.content_type in ['photo', 'video', 'document'] and message.caption:
                msg_type = 'text'
            await apply_penalty(user_id, msg_type, board)
            return
        recipients = users_data[board]['active'] - {user_id}
        reply_to_post = None
        reply_info = {}
        if message.reply_to_message:
            reply_mid = message.reply_to_message.message_id
            for (uid, mid), pnum in message_to_post.items():
                if mid == reply_mid:
                    reply_to_post = pnum
                    break
            if reply_to_post and reply_to_post in post_to_messages and messages_storage[reply_to_post]['board'] == board:
                reply_info = post_to_messages[reply_to_post]
            else:
                reply_to_post = None
        header, current_post_num = format_header(board)
        if message.text:
            daily_log.write(f"[{datetime.now(timezone.utc).isoformat()}] {message.text}\n")
        content_type = message.content_type
        try:
            await message.delete()
        except:
            pass
        content = {
            'type': content_type,
            'header': header,
            'reply_to_post': reply_to_post
        }
        if content_type == 'text':
            if message.entities:
                text_content = message.html_text
            else:
                text_content = escape_html(message.text)
            if board_modes[board]['suka_blyat_mode']:
                text_content = suka_blyatify_text(text_content)
            if board_modes[board]['slavaukraine_mode']:
                text_content = ukrainian_transform(text_content)
            if board_modes[board]['anime_mode']:
                text_content = anime_transform(text_content)
            if board_modes[board]['zaputin_mode']:
                text_content = zaputin_transform(text_content)
            if board_modes[board]['anime_mode'] and random.random() < 0.41:
                full_caption = f"<i>{header}</i>\n\n{text_content}"
                if len(full_caption) <= 1024:
                    anime_img_url = await get_random_anime_image()
                    if anime_img_url:
                        content['type'] = 'photo'
                        content['caption'] = text_content
                        content['image_url'] = anime_img_url
                    else:
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
            if caption:
                if board_modes[board]['suka_blyat_mode']:
                    caption = suka_blyatify_text(caption)
                if board_modes[board]['slavaukraine_mode']:
                    caption = ukrainian_transform(caption)
                if board_modes[board]['anime_mode']:
                    caption = anime_transform(caption)
                if board_modes[board]['zaputin_mode']:
                    caption = zaputin_transform(caption)
            content['caption'] = caption
        elif content_type == 'video':
            content['file_id'] = message.video.file_id
            caption = message.caption
            if caption:
                if board_modes[board]['suka_blyat_mode']:
                    caption = suka_blyatify_text(caption)
                if board_modes[board]['slavaukraine_mode']:
                    caption = ukrainian_transform(caption)
                if board_modes[board]['anime_mode']:
                    caption = anime_transform(caption)
                if board_modes[board]['zaputin_mode']:
                    caption = zaputin_transform(caption)
            content['caption'] = caption
        elif content_type == 'animation':
            content['file_id'] = message.animation.file_id
            caption = message.caption
            if caption:
                if board_modes[board]['suka_blyat_mode']:
                    caption = suka_blyatify_text(caption)
                if board_modes[board]['slavaukraine_mode']:
                    caption = ukrainian_transform(caption)
                if board_modes[board]['anime_mode']:
                    caption = anime_transform(caption)
                if board_modes[board]['zaputin_mode']:
                    caption = zaputin_transform(caption)
            content['caption'] = caption
        elif content_type == 'document':
            content['file_id'] = message.document.file_id
            caption = message.caption
            if caption:
                if board_modes[board]['suka_blyat_mode']:
                    caption = suka_blyatify_text(caption)
                if board_modes[board]['slavaukraine_mode']:
                    caption = ukrainian_transform(caption)
                if board_modes[board]['anime_mode']:
                    caption = anime_transform(caption)
                if board_modes[board]['zaputin_mode']:
                    caption = zaputin_transform(caption)
            content['caption'] = caption
        elif content_type == 'sticker':
            content['file_id'] = message.sticker.file_id
        elif content_type == 'audio':
            content['file_id'] = message.audio.file_id
            caption = message.caption
            if caption:
                if board_modes[board]['suka_blyat_mode']:
                    caption = suka_blyatify_text(caption)
                if board_modes[board]['slavaukraine_mode']:
                    caption = ukrainian_transform(caption)
                if board_modes[board]['anime_mode']:
                    caption = anime_transform(caption)
                if board_modes[board]['zaputin_mode']:
                    caption = zaputin_transform(caption)
            content['caption'] = caption
        elif content_type == 'video_note':
            content['file_id'] = message.video_note.file_id
        messages_storage[current_post_num] = {
            'author_id': user_id,
            'timestamp': datetime.now(UTC),
            'content': content,
            'reply_to': reply_to_post,
            'author_message_id': None,
            'board': board
        }
        reply_to_message_id = reply_info.get(user_id) if reply_info else None
        sent_to_author = None
        header_text = f"<i>{header}</i>"
        reply_text = f">>{reply_to_post}\n" if reply_to_post else ""
        try:
            if content_type == 'text':
                full_text = f"{header_text}\n\n{reply_text}{content['text']}" if reply_text else f"{header_text}\n\n{content['text']}"
                sent_to_author = await bots[board].send_message(
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
                    sent_to_author = await bots[board].send_photo(
                        user_id,
                        content['file_id'],
                        caption=caption,
                        reply_to_message_id=reply_to_message_id,
                        parse_mode="HTML"
                    )
                elif content_type == 'video':
                    sent_to_author = await bots[board].send_video(
                        user_id,
                        content['file_id'],
                        caption=caption,
                        reply_to_message_id=reply_to_message_id,
                        parse_mode="HTML"
                    )
                elif content_type == 'animation':
                    sent_to_author = await bots[board].send_animation(
                        user_id,
                        content['file_id'],
                        caption=caption,
                        reply_to_message_id=reply_to_message_id,
                        parse_mode="HTML"
                    )
                elif content_type == 'document':
                    sent_to_author = await bots[board].send_document(
                        user_id,
                        content['file_id'],
                        caption=caption,
                        reply_to_message_id=reply_to_message_id,
                        parse_mode="HTML"
                    )
                elif content_type == 'audio':
                    sent_to_author = await bots[board].send_audio(
                        user_id,
                        content['file_id'],
                        caption=caption,
                        reply_to_message_id=reply_to_message_id,
                        parse_mode="HTML"
                    )
            elif content_type == 'sticker':
                sent_to_author = await bots[board].send_sticker(
                    user_id,
                    content['file_id'],
                    reply_to_message_id=reply_to_message_id
                )
            elif content_type == 'video_note':
                sent_to_author = await bots[board].send_video_note(
                    user_id,
                    content['file_id'],
                    reply_to_message_id=reply_to_message_id
                )
            if sent_to_author:
                messages_storage[current_post_num]['author_message_id'] = sent_to_author.message_id
                if current_post_num not in post_to_messages:
                    post_to_messages[current_post_num] = {}
                post_to_messages[current_post_num][user_id] = sent_to_author.message_id
                message_to_post[(user_id, sent_to_author.message_id)] = current_post_num
            if recipients:
                await message_queues[board].put({
                    'recipients': recipients,
                    'content': content,
                    'post_num': current_post_num,
                    'reply_info': reply_info if reply_info else None
                })
        except Exception as e:
            print(f"Ошибка при обработке сообщения на {board}: {e}")
            if current_post_num in messages_storage:
                del messages_storage[current_post_num]
    except Exception as e:
        print(f"Критическая ошибка в handle_message на {board}: {e}")

async def message_broadcaster(board: str):
    while True:
        try:
            msg = await message_queues[board].get()
            recipients = msg['recipients']
            content = msg['content']
            post_num = msg['post_num']
            reply_info = msg.get('reply_info', {})
            for user_id in recipients:
                try:
                    reply_to_message_id = reply_info.get(user_id)
                    if content['type'] == 'text':
                        text = content['text']
                        text = add_you_to_my_posts(text, user_id)
                        await bots[board].send_message(
                            user_id,
                            f"<i>{content['header']}</i>\n\n{text}",
                            reply_to_message_id=reply_to_message_id,
                            parse_mode="HTML"
                        )
                    elif content['type'] == 'photo':
                        caption = f"<i>{content['header']}</i>"
                        if content.get('caption'):
                            caption += f"\n\n{escape_html(content['caption'])}"
                        await bots[board].send_photo(
                            user_id,
                            content['file_id'],
                            caption=caption,
                            reply_to_message_id=reply_to_message_id,
                            parse_mode="HTML"
                        )
                    elif content['type'] == 'video':
                        caption = f"<i>{content['header']}</i>"
                        if content.get('caption'):
                            caption += f"\n\n{escape_html(content['caption'])}"
                        await bots[board].send_video(
                            user_id,
                            content['file_id'],
                            caption=caption,
                            reply_to_message_id=reply_to_message_id,
                            parse_mode="HTML"
                        )
                    elif content['type'] == 'animation':
                        caption = f"<i>{content['header']}</i>"
                        if content.get('caption'):
                            caption += f"\n\n{escape_html(content['caption'])}"
                        await bots[board].send_animation(
                            user_id,
                            content['file_id'],
                            caption=caption,
                            reply_to_message_id=reply_to_message_id,
                            parse_mode="HTML"
                        )
                    elif content['type'] == 'document':
                        caption = f"<i>{content['header']}</i>"
                        if content.get('caption'):
                            caption += f"\n\n{escape_html(content['caption'])}"
                        await bots[board].send_document(
                            user_id,
                            content['file_id'],
                            caption=caption,
                            reply_to_message_id=reply_to_message_id,
                            parse_mode="HTML"
                        )
                    elif content['type'] == 'sticker':
                        await bots[board].send_sticker(
                            user_id,
                            content['file_id'],
                            reply_to_message_id=reply_to_message_id
                        )
                    elif content['type'] == 'audio':
                        caption = f"<i>{content['header']}</i>"
                        if content.get('caption'):
                            caption += f"\n\n{escape_html(content['caption'])}"
                        await bots[board].send_audio(
                            user_id,
                            content['file_id'],
                            caption=caption,
                            reply_to_message_id=reply_to_message_id,
                            parse_mode="HTML"
                        )
                    elif content['type'] == 'voice':
                        await bots[board].send_voice(
                            user_id,
                            content['file_id'],
                            caption=f"<i>{content['header']}</i>",
                            reply_to_message_id=reply_to_message_id,
                            parse_mode="HTML"
                        )
                    elif content['type'] == 'video_note':
                        await bots[board].send_video_note(
                            user_id,
                            content['file_id'],
                            reply_to_message_id=reply_to_message_id
                        )
                    elif content['type'] == 'media_group':
                        builder = MediaGroupBuilder()
                        for idx, media in enumerate(content['media']):
                            caption = None
                            if idx == 0:
                                caption = f"<i>{content['header']}</i>"
                                if content.get('caption'):
                                    caption += f"\n\n{escape_html(content['caption'])}"
                            if media['type'] == 'photo':
                                builder.add_photo(media=media['file_id'], caption=caption, parse_mode="HTML" if caption else None)
                            elif media['type'] == 'video':
                                builder.add_video(media=media['file_id'], caption=caption, parse_mode="HTML" if caption else None)
                            elif media['type'] == 'document':
                                builder.add_document(media=media['file_id'], caption=caption, parse_mode="HTML" if caption else None)
                            elif media['type'] == 'audio':
                                builder.add_audio(media=media['file_id'], caption=caption, parse_mode="HTML" if caption else None)
                        await bots[board].send_media_group(
                            user_id,
                            media=builder.build(),
                            reply_to_message_id=reply_to_message_id
                        )
                except Exception as e:
                    print(f"Ошибка отправки сообщения пользователю {user_id} на {board}: {e}")
            message_queues[board].task_done()
        except Exception as e:
            print(f"Ошибка в message_broadcaster для {board}: {e}")
            await asyncio.sleep(1)

async def motivation_broadcaster():
    while True:
        await asyncio.sleep(3600)
        for board in BOARDS:
            motivation = random.choice(MOTIVATIONAL_MESSAGES)
            invite = random.choice(INVITE_TEXTS)
            global post_counter
            post_counter += 1
            pnum = post_counter
            header = "Приглашение"
            text = f"{motivation}\n\n{invite}"
            await message_queues[board].put({
                "recipients": users_data[board]['active'],
                "content": {'type': 'text', 'header': header, 'text': text},
                "post_num": pnum,
            })

async def start_background_tasks():
    tasks = []
    for board in BOARDS:
        tasks.append(asyncio.create_task(message_broadcaster(board)))
        tasks.append(asyncio.create_task(conan_roaster(
            users_data[board],
            messages_storage,
            post_to_messages,
            message_to_post,
            message_queues[board],
            lambda: format_header(board),
            board
        )))
        tasks.append(asyncio.create_task(help_broadcaster(users_data[board], message_queues[board], lambda: format_header(board), board)))
    tasks.append(asyncio.create_task(auto_backup()))
    tasks.append(asyncio.create_task(auto_memory_cleaner()))
    tasks.append(asyncio.create_task(cleanup_old_messages()))
    tasks.append(asyncio.create_task(motivation_broadcaster()))
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
        global is_shutting_down, healthcheck_site, bots, dispatchers
        restore_backup_on_start()
        bots = {board: Bot(token=BOT_TOKENS[board]) for board in BOARDS}
        dispatchers = {board: Dispatcher() for board in BOARDS}
        load_state()
        healthcheck_site = await start_healthcheck()
        for board, dp in dispatchers.items():
            dp.middleware.setup(BoardMiddleware(board))
            dp.message.register(handle_message)
            dp.message.register(handle_audio, F.audio)
            dp.message.register(handle_voice, F.voice)
            dp.message.register(handle_media_group_init, F.media_group_id)
            dp.message.register(cmd_start, Command("start"))
            dp.message.register(cmd_help, Command("help"))
            dp.message.register(cmd_stats, Command("stats"))
            dp.message.register(cmd_roll, Command("roll"))
            dp.message.register(cmd_invite, Command("invite"))
            dp.message.register(cmd_suka_blyat, Command("suka_blyat"))
            dp.message.register(cmd_zaputin, Command("zaputin"))
            dp.message.register(cmd_slavaukraine, Command("slavaukraine"))
            dp.message.register(cmd_anime, Command("anime"))
            dp.message.register(cmd_deanon, Command("deanon"))
            dp.message.register(cmd_admin, Command("admin"))
            dp.message.register(cmd_id, Command("id"))
            dp.message.register(cmd_ban, Command("ban"))
            dp.message.register(cmd_mute, Command("mute"))
            dp.message.register(cmd_wipe, Command("wipe"))
            dp.message.register(cmd_unmute, Command("unmute"))
            dp.message.register(cmd_unban, Command("unban"))
            dp.message.register(cmd_del, Command("del"))
            dp.callback_query.register(admin_save, F.data == "save")
            dp.callback_query.register(admin_stats, F.data == "stats")
            dp.callback_query.register(admin_spammers, F.data == "spammers")
            dp.callback_query.register(admin_banned, F.data == "banned")
            dp.errors.register(global_error_handler)
        tasks = await start_background_tasks()
        print("✅ Фоновые задачи запущены")
        await asyncio.gather(
            *[dp.start_polling(bots[board], skip_updates=True) for board, dp in dispatchers.items()]
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
