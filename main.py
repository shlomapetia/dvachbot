from __future__ import annotations
import asyncio
import gc
import io
import json
import logging
import time
import os
import re 
import glob
import random
import secrets
import pickle
import gzip
import gc
import weakref
from collections import deque, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
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
import os
from datetime import datetime, UTC  # Добавьте UTC в импорты

GITHUB_REPO = "https://github.com/shlomapetia/dvachbot.git"
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")  # Проверь, что переменная есть в Railway!

async def git_commit_and_push():
    """Надежная функция бэкапа state и reply в GitHub"""
    try:
        # Проверка переменных окружения
        token = os.getenv("GITHUB_TOKEN")
        if not token:
            print("❌ Нет GITHUB_TOKEN")
            return False

        # Настройка Git
        subprocess.run(["git", "config", "--global", "user.name", "Backup Bot"], check=True)
        subprocess.run(["git", "config", "--global", "user.email", "bot@example.com"], check=True)

        # Рабочая директория
        work_dir = "/data"
        if not os.path.exists(work_dir):
            os.makedirs(work_dir, exist_ok=True)
        
        # Клонируем или обновляем репозиторий
        git_dir = os.path.join(work_dir, ".git")
        if not os.path.exists(git_dir):
            clone_cmd = ["git", "clone", f"https://{token}@github.com/shlomapetia/dvachbot.git", work_dir]
            result = subprocess.run(clone_cmd, cwd=work_dir)
            if result.returncode != 0:
                print("❌ Ошибка клонирования репозитория")
                return False
        else:
            subprocess.run(["git", "fetch", "origin"], cwd=work_dir, check=True)
            subprocess.run(["git", "reset", "--hard", "origin/main"], cwd=work_dir, check=True)

        # Копируем файлы для бэкапа
        files_to_backup = []
        for f in ["state.json", "reply_cache.json"] + glob.glob("backup_state_*.json"):
            if os.path.exists(f):
                shutil.copy2(f, work_dir)
                files_to_backup.append(os.path.basename(f))
        
        if not files_to_backup:
            print("⚠️ Нет файлов для бэкапа")
            return False

        # Git операции
        try:
            # Добавляем файлы
            subprocess.run(["git", "add"] + files_to_backup, cwd=work_dir, check=True)
            
            # Проверяем есть ли изменения для коммита
            status_result = subprocess.run(["git", "status", "--porcelain"], cwd=work_dir, 
                                         capture_output=True, text=True)
            if not status_result.stdout.strip():
                print("ℹ️ Нет изменений для коммита")
                return True
            
            # Коммит
            commit_msg = f"Backup: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            subprocess.run(["git", "commit", "-m", commit_msg], cwd=work_dir, check=True)
            
            # Push с повторными попытками при ошибках
            max_retries = 3
            for attempt in range(max_retries):
                push_result = subprocess.run(["git", "push"], cwd=work_dir)
                if push_result.returncode == 0:
                    print(f"✅ Бекапы сохранены в GitHub: {', '.join(files_to_backup)}")
                    return True
                else:
                    print(f"⚠️ Ошибка при push в GitHub (попытка {attempt + 1}/{max_retries})")
                    await asyncio.sleep(5)  # Теперь это внутри async функции
            
            print("❌ Не удалось выполнить push после нескольких попыток")
            return False
                
        except subprocess.CalledProcessError as e:
            print(f"❌ Ошибка git-операции: {e}")
            return False

    except Exception as e:
        print(f"⛔ Критическая ошибка в git_commit_and_push: {e}")
        return False

dp = Dispatcher()
# Настройка логирования - только важные сообщения
logging.basicConfig(
    level=logging.WARNING,  # Только предупреждения и ошибки
    format="%(message)s",  # Просто текст без дат и т.п.
    datefmt="%H:%M:%S"  # Если время нужно, то в коротком формате
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

async def run_bot():
    global bot, connector
    connector = aiohttp.TCPConnector(limit=10, force_close=True)
    bot = Bot(token=BOT_TOKEN, connector=connector)
    while True:
        try:
            await dp.start_polling(
                bot,
                skip_updates=True,  # Пропускать обновления при старте
                close_bot_session=False,
                handle_signals=False
            )
        except Exception as e:
            logging.error(f"Bot crashed: {e}, restarting in 10 seconds...")
            await asyncio.sleep(10)  # Увеличиваем задержку перед перезапуском


async def shutdown():
    """Cleanup tasks before shutdown"""
    print("Shutting down...")
    await dp.storage.close()
    await dp.storage.wait_closed()
    await bot.session.close()

async def auto_backup():
    """Бэкап + коммит в GitHub."""
    while True:
        try:
            await asyncio.sleep(12000)  # 2.8 часа
            
            backup_name = f'backup_state_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
            if os.path.exists('state.json'):
                shutil.copy('state.json', backup_name)
                print(f"✅ Создан бэкап: {backup_name}")
                
                # Удаляем старые бэкапы
                current_time = time.time()
                for old_backup in glob.glob('backup_state_*.json'):
                    if os.path.basename(old_backup) != backup_name:
                        file_time = os.path.getmtime(old_backup)
                        if current_time - file_time > 36000:  # 10 часов
                            os.remove(old_backup)
                            print(f"🗑️ Удален старый бэкап: {old_backup}")
                
                await git_commit_and_push()  # <- Коммитим бэкап
            else:
                print("⚠️ state.json не найден, пропускаем бэкап")
        except Exception as e:
            print(f"❌ Ошибка бэкапа: {e}")

# Настройка сборщика мусора
gc.set_threshold(
    700, 10, 10)  # Оптимальные настройки для баланса памяти/производительности

async def cleanup_old_messages():
    """Очистка постов старше 7 дней"""
    while True:
        await asyncio.sleep(3600)  # Каждые 1 час
        try:
            current_time = datetime.now(MSK)  # Используем MSK timezone
            old_posts = [
                pnum for pnum, data in messages_storage.items()
                if (current_time -
                    data.get('timestamp', current_time)).days > 7
            ]
            for pnum in old_posts:
                messages_storage.pop(pnum, None)
                post_to_messages.pop(pnum, None)
            print(f"Очищено {len(old_posts)} старых постов")
        except Exception as e:
            print(f"Ошибка очистки: {e}")
# для проверки одинаковых / коротких сообщений
last_texts: dict[int, deque[str]] = defaultdict(lambda: deque(maxlen=5))

# хранит последние 5 Message-объектов пользователя
# Вместо defaultdict используем обычный dict с ручной инициализацией
last_user_msgs = {}
MAX_ACTIVE_USERS_IN_MEMORY = 5000

def get_user_msgs_deque(user_id):
    """Получаем deque для юзера, ограничиваем количество юзеров в памяти"""
    if user_id not in last_user_msgs:
        # Если слишком много юзеров - удаляем самого старого
        if len(last_user_msgs) >= MAX_ACTIVE_USERS_IN_MEMORY:
            # Удаляем первого (самого старого)
            oldest_user = next(iter(last_user_msgs))
            del last_user_msgs[oldest_user]

        last_user_msgs[user_id] = deque(maxlen=10)  # Исправлено!

    return last_user_msgs[user_id]  # Исправлено!
# Рядом с другими глобальными словарями
spam_violations = defaultdict(int)  # user_id -> количество нарушений

# Конфиг
BOT_TOKEN = os.environ.get('BOT_TOKEN')
ADMINS = {int(x) for x in os.getenv("ADMINS", "").split(",") if x}
SPAM_LIMIT = 12
SPAM_WINDOW = 15
STATE_FILE = 'state.json'
SAVE_INTERVAL = 120  # секунд
STICKER_WINDOW = 10  # секунд
STICKER_LIMIT = 7
REST_SECONDS = 30  # время блокировки
REPLY_CACHE = 500  # сколько постов держать
REPLY_FILE = "reply_cache.json"  # отдельный файл для reply
# В начале файла с константами
MAX_MESSAGES_IN_MEMORY = 1110  # храним только последние 1000 постов

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

# ─── КОНАН-СЛОВАРИ ─────────────────────────────────────────────
SPORT_INVENTS = [
    "штанга", "гантеля", "гиря", "эспандер", "мешок с песком",
    "мешок с говном", "камень из пенопласта", "камень из кусков говна",
    "таджикский хуй"
]
INVENT_WEIGHT = [
    "20 кг", "35 кг", "50 кг", "75 кг", "86 кг", "72 кг", "100 кг", "120 кг",
    "150 кг", "200 кг", "250 кг", "300 кг", "350 кг", "400 кг", "450 кг",
    "20 грамм", "35 грамм", "50 грамм", "75 грамм", "86 грамм", "72 грамм",
    "100 грамм", "120 грамм", "150 грамм", "200 грамм", "250 грамм",
    "300 грамм", "350 грамм"
]
INSULTS = [
    "дегенерат", "шиз", "хряк", "русня", "слабак", "дрыщ", "чушпан", "русня",
    "таджик", "пидор", "пидорас", "шизофреник", "идиот", "дебил", "кретин",
    "хуесос", "хуеглот", "хуегрыз", "хуежопый пидор", "хуежопый пидорас",
    "хуежопый шизофреник", "хуежопый идиот", "хуежопый дебил",
    "хуежопый кретин", "хуежопый хуесос", "хуежопый хуеплет",
    "хуежопый хуеглот", "хуежопый хуегрыз", "хуежопый хуежоп",
    "хуежопый хуежопый", "хуежопый хуежопый пидор"
]
ACHIEVEMENTS = [
    "трёхразрядник", "МСМК по жиму", "чемпион лесной качалки",
    "бронзовый призер спартакиады", "гроза возбужденных мужчин",
    "сперморазрядник", "мастер геевик", "сисси", "мастер спорта по жиму",
    "мастер спорта по жиму штангой", "мастер спорта по жиму гантелями",
    "мастер спорта по жиму гирями", "главный пидор лесной качалки",
    "мастер спорта по жиму эспандером",
    "мастер спорта по жиму мешком с песком",
    "мастер спорта по жиму мешком с говном",
    "мастер спорта по жиму камнем из пенопласта",
    "мастер спорта по жиму камнем из кусков говна",
    "мастер спорта по жиму штангой"
]
SPORT_FACTS = [
    "Габариты Конана? — Рост 185 мм, вес 94 г",
    "Каковы титулы Сосана? — Девственник с большой буквы Д, качок-геевик старой школы, сперморазрядник по жиму лёжа, чёрный минетчик",
    "На что живет Николай Романов (он же сосан)? ответ: Николай симулировал психическое заболевание ради получения пенсии",
    "Почему от Сосана воняет? — Сосан стирает вещи под дождём или в речке-вонючке, не использует дезодорант и духи, не вытирает/подмывает сраку после калоизвержения",
    "Увы, Сосан действительно был изгнан из лесной качалки за гнилой базар и немытую сраку.",
    "Сосан ненавидит скотов, живущих в скотоблоках и жрущих русское хрючево, осуждает вкусно и точка и другие заведения быстрого питания. Был отшит всеми девушками из-за умственной отсталости и омежности, но до сих пор не бросает попыток найти вторую половинку. Нюхал трусы своей сестры и брата фейса. Использует фарму ради достижения новых спортивных свершений",
    "Каков размер пенсии Чмонана? ответ: Доподлинно неизвестно, примерно 22-23к",
    "Где живет Николай Конан? Ответ: В Тимирязевском районе на севере Москвы",
    "Сколько Конану лет? Ответ: Николай родился 09.12.1999, на данный момент ему 25 лет.   В какие игры играет/играл Николай? Ответ: Кс 1.6, Кс соурс, WoW, Майнкрафт, Лара, Крофт, Руна",
    "Николай Сосан (он же чмонан, он же кокон, он же пенопласт, он же пеликан, он же гуимплен) Романов - официальный шизофреник с манией величия и микропенисом. Жимцельская ноша сильно давит на самочувствие Коли и он срет в тредах своими гомопастами. Из спортивных достижений КМС по бегу от ментов. МС по шизе (заслуженный). ЧСВ международного класса.",
    "В 17 лет Сосан нагрел на костре и прожег себе на груди дырки, шрамы якобы от фехтования, ушел в дурку и получил белый билет.",
    "Сосан, вероятно не сидит итт, зато им притворяется жирножопый вырожденец, прикрученный к инвалидному стулу, чтобы троллить и грубить анонам"
]
CATCHPHRASES = [
    "Тухлый форс от русни", "Преклонитесь перед Императором", "Я сосал!",
    "Чернь, приказываю тебе!", "Эктоморф пожал штангу 86 килограмм"
]

TEMPLATES = [
    "Ты унижен, {name}, ты дрыщ не осилишь {inv} {wgt}",
    "На колени, раб, {name}", "В глаза долбишься, {ins}? Я {ach}.",
    "В глаза долбишься, {ins}? Русня ты знаешь что я {ach}?",
    "Молчи мразь. Я {ach}", "Молчи русня. Я {ach}",
    "Ты знаешь что с тобой общается {ach}?", "Ты знаешь что я {ach}?",
    "Ты знаешь что я сосал?", "{ins} дрищавый {ins}", "Ты выглядишь как {ins}",
    "У тебя шиза, {ins}", "У тебя шиза",
    "Слабая русня не поднимает {inv} {wgt}", "Ты не осилишь {inv} {wgt}",
    "Да, я сосал."
    "Спокуха, {ins}", "Таблетки прими, {ins}", "{fact}",
    "Русня, а ты знаешь этот факт про меня? {fact}",
    "Правда глаза режет, {ins}?", "{catch}", "{ins} ты с ума сошел от зависти",
    "Проекции, {ins}", "Запомни. {fact}", "Сейчас обоссу, {ins}",
    "В палату вернись, {ins}", "Манямирок выключи, {ins}",
    "{ins} чмо ты с ума сошло от зависти", "Пузо втянуть, {ins}",
    "Кажется пора проучить тебя пидорас. Большой афганский дядя накажет тебя.", "Ты похож на {ins}",
    "Ты выглядишь как руснявый {ins}", "Какой же ты {ins}!",
    "Привет, приятель!", "Приходи в лесную качалку, {ins}",
    "Ты унижен штангой в {wgt}", "Я мастер спорта по жиму {inv} {wgt}",
    "ТЫ будешь жить в страхе, {ins}", "Я уничтожу тебя, {ins}",
    "На колени, {ins}", "Сдохни, {ins}", "Я жму {wgt} от груди",
    "Я жму {wgt} анусом", "Я жму {wgt} своим ртом", "Я жму {wgt}",
    "Я поднимал камень в {wgt}", "Я сосал",
    "Я совал камень в {wgt} себе в свою дырявую, обосранную, вонючую сраку",
    "Я делаю жим на яйцепс каждое утро", "Твои проекции, {ins} ебаный",
    "Сойдёт за прогресс только в твоём манямирке",
    "Тухлый форс от тебя, ебаный {ins}!", "Не вывез веса? {ins} ебаный",
    "В палату вернись, ебаный {ins}", "Я король мужских членов"
]

def conan_phrase(username: str = "Приятель"):
    tpl = secrets.choice(TEMPLATES)
    return tpl.format(
        name=username,
        inv=secrets.choice(SPORT_INVENTS),
        wgt=secrets.choice(INVENT_WEIGHT),
        ach=secrets.choice(ACHIEVEMENTS),
        ins=secrets.choice(INSULTS),
        fact=secrets.choice(SPORT_FACTS),
        catch=secrets.choice(CATCHPHRASES),
    )

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

# Глобальный executor для параллельной отправки
executor = ThreadPoolExecutor(max_workers=100)

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

def escape_html(text: str) -> str:
    """Экранирует HTML символы"""
    if not text:
        return text
    return text.replace('&', '&amp;').replace('<', '&lt;').replace(
        '>', '&gt;').replace('"', '&quot;')


def is_admin(uid: int) -> bool:
    return uid in ADMINS

async def save_state():
    """Сохранение состояния с надежным бэкапом и очисткой старых"""
    try:
        # Подготовка данных
        data = {
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
        }
        
        # Сохранение state.json
        with open('state.json', 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        # Создание бэкапа
        backup_name = f'backup_state_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
        shutil.copy('state.json', backup_name)
        
        # Очистка старых бэкапов - оставляем только:
        # - Последние 3 обычных бэкапа
        # - По одному на последние 5 дней
        backups = sorted(glob.glob('backup_state_*.json'), key=os.path.getmtime)
        backups_to_keep = set()
        
        # 1. Всегда оставляем последние 3 бэкапа
        backups_to_keep.update(backups[-3:])
        
        # 2. По одному бэкапу на день за последние 5 дней
        daily_backups = defaultdict(list)
        for backup in backups:
            date_part = os.path.basename(backup)[12:20]  # Извлекаем YYYYMMDD
            daily_backups[date_part].append(backup)
        
        last_3_days = sorted(daily_backups.keys())[-3:]
        for day in last_3_days:
            if daily_backups[day]:
                backups_to_keep.add(daily_backups[day][-1])  # Последний бэкап за день
        
        # Удаляем все, кроме тех что нужно сохранить
        for old_backup in set(backups) - backups_to_keep:
            try:
                os.remove(old_backup)
                print(f"🗑️ Удален старый бэкап: {os.path.basename(old_backup)}")
            except Exception as e:
                print(f"⚠️ Не удалось удалить бэкап {old_backup}: {e}")
        
        # Сохранение в GitHub
        git_success = await git_commit_and_push()
        if not git_success:
            print("⚠️ Не удалось сохранить в GitHub, но локальные файлы сохранены")
        
        # Всегда сохраняем reply-cache
        save_reply_cache()
        
        return True
        
    except Exception as e:
        print(f"⛔ Критическая ошибка при сохранении состояния: {e}")
        return False

def save_reply_cache():
    """Надежное сохранение reply-cache"""
    try:
        recent = sorted(messages_storage.keys())[-REPLY_CACHE:]
        if not recent:
            print("⚠️ Нет данных для reply-cache")
            return False

        m2p, p2m, meta = {}, {}, {}

        for p in recent:
            # post → {uid: mid}
            if p in post_to_messages:
                p2m[str(p)] = post_to_messages[p]

            # message → post
            for (uid, mid), post_num in message_to_post.items():
                if post_num == p:
                    m2p[f"{uid}_{mid}"] = post_num

            # метаданные
            ms = messages_storage.get(p, {})
            a_id = ms.get("author_id") or ms.get("author", "")
            meta[str(p)] = {
                "author_id": a_id,
                "timestamp": ms.get("timestamp", datetime.now(UTC)).isoformat(),
                "author_msg": ms.get("author_message_id"),
            }

        # Сохраняем локально
        with open(REPLY_FILE, "w", encoding="utf-8") as f:
            json.dump({
                "post_to_messages": p2m,
                "message_to_post": m2p,
                "messages_storage_meta": meta,
            }, f, ensure_ascii=False, indent=2)

        # Пытаемся сохранить в GitHub только если есть изменения
        try:
            # Проверяем есть ли изменения
            status_result = subprocess.run(["git", "diff", "--exit-code", REPLY_FILE], 
                                         cwd="/data")
            if status_result.returncode == 0:
                print("ℹ️ reply_cache.json не изменился, пропускаем коммит")
                return True

            subprocess.run(["git", "add", REPLY_FILE], cwd="/data", check=True)
            commit_result = subprocess.run(
                ["git", "commit", "-m", f"Update reply cache: {datetime.now().strftime('%Y-%m-%d %H:%M')}"], 
                cwd="/data"
            )
            if commit_result.returncode != 0:
                print("⚠️ Нет изменений для коммита reply_cache.json")
                return True

            push_result = subprocess.run(["git", "push"], cwd="/data")
            if push_result.returncode == 0:
                print("✅ reply_cache.json сохранен и залит в GitHub")
                return True
            else:
                print("⚠️ Ошибка push reply_cache.json")
                return False
        except subprocess.CalledProcessError as e:
            print(f"⚠️ Ошибка коммита reply_cache.json: {e}")
            return False

    except Exception as e:
        print(f"⛔ Ошибка сохранения reply-cache: {e}")
        return False

def check_restart_needed():
    """Проверяет, нужно ли перезапускать бота (только при изменении main.py)"""
    try:
        # Получаем хэш текущего файла
        current_hash = subprocess.run(
            ["git", "hash-object", "main.py"],
            capture_output=True, text=True
        ).stdout.strip()
        
        # Получаем хэш из последнего коммита
        last_commit_hash = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True, text=True
        ).stdout.strip()
        
        # Получаем хэш main.py в последнем коммите
        file_hash = subprocess.run(
            ["git", "ls-tree", last_commit_hash, "main.py"],
            capture_output=True, text=True
        ).stdout.split()[2] if last_commit_hash else ""
        
        return current_hash != file_hash
    except Exception as e:
        print(f"⚠️ Ошибка при проверке необходимости перезапуска: {e}")
        return False

def load_state():
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
    """Читаем reply_cache.json, восстанавливаем словари"""
    if not os.path.exists(REPLY_FILE):
        return

    # файл есть, но может быть пустой или битый
    try:
        if os.path.getsize(REPLY_FILE) == 0:
            return
        with open(REPLY_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        print(f"reply_cache.json повреждён ({e}), игнорирую")
        return
    # ---------- восстановление словарей ----------
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

    for p_str, meta in data.get("messages_storage_meta", {}).items():
        p = int(p_str)
        messages_storage[p] = {
            "author_id": meta["author_id"],
            "timestamp": datetime.fromisoformat(meta["timestamp"]),
            "author_message_id": meta.get("author_msg"),
        }

    print(f"reply-cache загружен: {len(post_to_messages)} постов")
    # ─── итоговая статистика ─────────────────────────
    print(f"State: пост-счётчик = {state['post_counter']}, "
          f"активных = {len(state['users_data']['active'])}, "
          f"забаненных = {len(state['users_data']['banned'])}")
    print(f"Reply-cache: постов {len(post_to_messages)}, "
          f"сообщений {len(message_to_post)}")


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
        now = datetime.now()
        for user_id in list(spam_tracker.keys()):
            spam_tracker[user_id] = [
                t for t in spam_tracker[user_id]
                if (now - t).seconds < SPAM_WINDOW
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
    """Автоматическое сохранение состояния"""
    while True:
        await asyncio.sleep(SAVE_INTERVAL)
        await save_state()

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
        'window_sec': 20,
        'penalty': [60, 600, 900]  # 1мин, 10мин, 15 мин
    }
}

async def check_spam(user_id: int, msg: Message) -> bool:
    """Проверяет спам с градацией наказаний и сбросом уровня"""
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
        # Обрабатываем подписи к медиа как текст
        msg_type = 'text'
        content = msg.caption
    else:
        return True  # Другие типы не проверяем

    rules = SPAM_RULES.get(msg_type, {})
    if not rules:
        return True

    now = datetime.now(UTC)

    # Инициализация данных пользователя
    if user_id not in spam_violations:
        spam_violations[user_id] = {
            'level': 0,  # Уровень нарушения (число)
            'last_reset': now,
            'last_contents': deque(maxlen=4)  # Храним последние 4 сообщения
        }

    # Сброс уровня, если прошло больше 1 часа
    if (now - spam_violations[user_id]['last_reset']) > timedelta(hours=1):
        spam_violations[user_id] = {
            'level': 0,
            'last_reset': now,
            'last_contents': deque(maxlen=4)
        }

    # Проверка повторяющихся текстов/подписей
    if msg_type == 'text':
        # Добавляем текущее сообщение в историю
        spam_violations[user_id]['last_contents'].append(content)

        # Проверяем, не чередует ли пользователь 2 сообщения
        if len(spam_violations[user_id]['last_contents']) >= 4:
            unique_contents = set(spam_violations[user_id]['last_contents'])
            # Если всего 2 уникальных сообщения в последних 4
            if len(unique_contents) == 2:
                # Проверяем, что они действительно чередуются
                contents = list(spam_violations[user_id]['last_contents'])
                pattern1 = [contents[0], contents[1]] * 2
                pattern2 = [contents[1], contents[0]] * 2
                if contents == pattern1 or contents == pattern2:
                    spam_violations[user_id]['level'] = min(
                        spam_violations[user_id]['level'] + 1,
                        len(rules['penalty']) - 1)
                    return False  # Это спам

        # Также сохраняем в общую историю текстов для проверки повторов
        last_texts[user_id].append(content)
        if len(last_texts[user_id]) == rules.get('max_repeats', 3) and len(
                set(last_texts[user_id])) == 1:
            spam_violations[user_id]['level'] = min(
                spam_violations[user_id]['level'] + 1,
                len(rules['penalty']) - 1)
            return False  # Это спам

    # Проверка лимитов в временном окне
    window_start = now - timedelta(seconds=rules.get('window_sec', 15))

    # Обновляем трекер
    spam_tracker[user_id] = [
        t for t in spam_tracker.get(user_id, []) if t > window_start
    ]
    spam_tracker[user_id].append(now)

    # Если превысили лимит
    if len(spam_tracker[user_id]) >= rules.get('max_per_window', 5):
        # Увеличиваем уровень
        spam_violations[user_id]['level'] = min(
            spam_violations[user_id]['level'] + 1,
            len(rules['penalty']) - 1)
        return False  # Это спам

    return True  # Это не спам

async def apply_penalty(user_id: int, msg_type: str):
    """Применяет наказание согласно текущему уровню"""
    rules = SPAM_RULES.get(msg_type, {})
    if not rules:
        return

    # Получаем текущий уровень нарушений
    level = spam_violations.get(user_id, {}).get('level', 0)
    level = min(level, len(rules.get('penalty', [])) - 1)  # Ограничиваем максимальный уровень

    # Определяем время мута
    mute_seconds = rules['penalty'][level] if rules.get('penalty') else 30  # По умолчанию 30 сек
    mutes[user_id] = datetime.now(UTC) + timedelta(seconds=mute_seconds)

    # Удаляем посты пользователя за последние 5 минут
    deleted_posts = await delete_user_posts(user_id, 5)

    # Определяем тип нарушения
    violation_type = ""
    if msg_type == 'text':
        violation_type = "текстовый спам"
    elif msg_type == 'sticker':
        violation_type = "спам стикерами"
    elif msg_type == 'animation':
        violation_type = "спам гифками"

    # Уведомление
    try:
        time_str = ""
        if mute_seconds < 60:
            time_str = f"{mute_seconds} сек"
        elif mute_seconds < 3600:
            time_str = f"{mute_seconds // 60} мин"
        else:
            time_str = f"{mute_seconds // 3600} час"

        await bot.send_message(
            user_id,
            f"🚫 Эй пидор ты в муте на {time_str} за {violation_type}\n"
            f"Удалено твоих последних постов: {deleted_posts}\n"
            f"Спамишь дальше - получишь бан",
            parse_mode="HTML")

        # Отправляем уведомление в чат
        await send_moderation_notice(user_id, "mute", time_str, deleted_posts)
    except Exception as e:
        print(f"Ошибка отправки уведомления о муте: {e}")

def format_header() -> Tuple[str, int]:
    """Форматирование заголовка в стиле двача со случайными префиксами"""
    state['post_counter'] += 1

    rand = random.random()
    if rand < 0.003:
        circle = "🔴 "
    elif rand < 0.006:
        circle = "🟢 "
    else:
        circle = ""

    # Добавляем специальные префиксы с разной вероятностью
    prefix = ""
    rand_prefix = random.random()
    if rand_prefix < 0.005:  # 0.5%
        prefix = "### АДМИН ### "
    elif rand_prefix < 0.008:  # 0.3% (0.5% + 0.3% = 0.8%)
        prefix = "Абу - "
    elif rand_prefix < 0.01:   # 0.2% (0.8% + 0.2% = 1.0%)
        prefix = "Пидор - "
    elif rand_prefix < 0.012:  # 0.2%
        prefix = "### ДЖУЛУП ###"
    elif rand_prefix < 0.014:   # 0.2% 
        prefix = "### Хуесос ### "
    elif rand_prefix < 0.01:   # 0.2% (0.8% + 0.2% = 1.0%)
        prefix = "Пыня - "
    elif rand_prefix < 0.012:  # 0.2%
        prefix = "Нариман Намазов - "

    # Убрали HTML-теги из текста, оставили только текст
    header_text = f"{circle}{prefix}Пост №{state['post_counter']}"
    return header_text, state['post_counter']

async def delete_user_posts(user_id: int, time_period_minutes: int):
    """Удаляет все посты пользователя за указанный период времени (в минутах)"""
    try:
        time_threshold = datetime.now(MSK) - timedelta(minutes=time_period_minutes)
        posts_to_delete = []

        # Находим посты пользователя за указанный период
        for post_num, post_data in messages_storage.items():
            if (post_data.get('author_id') == user_id and 
                post_data.get('timestamp', datetime.now(MSK)) >= time_threshold):
                posts_to_delete.append(post_num)

        deleted_count = 0
        for post_num in posts_to_delete:
            # Удаляем у всех получателей
            for uid, mid in post_to_messages.get(post_num, {}).items():
                try:
                    await bot.delete_message(uid, mid)
                    deleted_count += 1
                except TelegramBadRequest as e:
                    if "message to delete not found" not in str(e):
                        print(f"Ошибка удаления сообщения {mid} у пользователя {uid}: {e}")
                except Exception as e:
                    print(f"Ошибка удаления сообщения {mid} у пользователя {uid}: {e}")

            # Удаляем у автора (если сохранили author_message_id)
            author_mid = messages_storage.get(post_num, {}).get("author_message_id")
            if author_mid:
                try:
                    await bot.delete_message(user_id, author_mid)
                    deleted_count += 1
                except TelegramBadRequest as e:
                    if "message to delete not found" not in str(e):
                        print(f"Ошибка удаления сообщения у автора {user_id}: {e}")
                except Exception as e:
                    print(f"Ошибка удаления сообщения у автора {user_id}: {e}")

            # Удаляем из хранилищ
            post_to_messages.pop(post_num, None)
            messages_storage.pop(post_num, None)

        # Чистим message_to_post
        global message_to_post
        message_to_post = {
            k: v for k, v in message_to_post.items() 
            if v not in posts_to_delete
        }

        return deleted_count
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
        reply_key = (user_id, message.reply_to_message.message_id)
        reply_to_post = message_to_post.get(reply_key)
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
        'type': 'audio',
        'header': header,
        'file_id': message.audio.file_id,
        'caption': message.caption,
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

    caption = f"<i>{header}</i>"
    if message.caption:
        caption += f"\n\n{escape_html(message.caption)}"

    sent_to_author = await bot.send_audio(
        user_id,
        message.audio.file_id,
        caption=caption,
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
    header, post_num = format_header()
    header = header.replace("Пост", "### АДМИН ###")

    if action == "ban":
        text = (f"🚨 Хуесос был забанен за спам. "
               f"Удалено его постов за последний час: {deleted_posts}. Помянем.")
    elif action == "mute":
        text = (f"🔇 Ебаного пидораса замутили на {duration}. "
               f"Удалено его постов за последние 15 минут: {deleted_posts}. "
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
    """Оптимизированная рассылка сообщений пользователям"""
    if not recipients and user_id is None:
        return []

    if not content or 'type' not in content:
        return []

    if user_id is not None:
        recipients.add(user_id)

    # Удаляем заблокировавших бота пользователей из активных
    blocked_users = set()
    active_recipients = set()

    # Быстрая проверка активных пользователей
    for uid in recipients:
        if uid in state['users_data']['banned']:
            continue
        active_recipients.add(uid)

    if not active_recipients:
        return []

    async def really_send(uid: int, reply_to: int | None):
        """Отправка с обработкой ошибок"""
        try:
            ct = content["type"]
            header_text = content['header']
            head = f"<i>{header_text}</i>"

            reply_to_post = content.get('reply_to_post')
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
            if content.get('text'):
                main_text = add_you_to_my_posts(content['text'], original_author)
            elif content.get('caption'):
                main_text = add_you_to_my_posts(content['caption'], original_author)

            full_text = f"{head}\n\n{reply_text}{main_text}" if reply_text else f"{head}\n\n{main_text}"

            if ct == "text":
                return await bot.send_message(
                    uid,
                    full_text,
                    reply_to_message_id=reply_to,
                    parse_mode="HTML",
                )

            elif ct == "photo":
                if len(full_text) > 1024:
                    full_text = full_text[:1021] + "..."
                return await bot.send_photo(
                    uid,
                    content["file_id"],
                    caption=full_text,
                    reply_to_message_id=reply_to,
                    parse_mode="HTML",
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
    max_concurrent = 100  # Максимальное количество одновременных отправок
    semaphore = asyncio.Semaphore(100)  # Вместо 100

    async def send_with_semaphore(uid):
        async with semaphore:
            reply_to = reply_info.get(uid) if reply_info else None
            return await really_send(uid, reply_to)

    # Запускаем все задачи параллельно
    tasks = [send_with_semaphore(uid) for uid in active_recipients]
    results = await asyncio.gather(*tasks)

    # Сохраняем связи сообщений для ответов
    if content.get('post_num'):
        for uid, msg in zip(active_recipients, results):
            if msg:
                if isinstance(msg, list):  # Для медиа-групп
                    if content['post_num'] not in post_to_messages:
                        post_to_messages[content['post_num']] = {}
                    post_to_messages[content['post_num']][uid] = msg[0].message_id
                    for m in msg:
                        message_to_post[(uid, m.message_id)] = content['post_num']
                else:  # Для одиночных сообщений
                    if content['post_num'] not in post_to_messages:
                        post_to_messages[content['post_num']] = {}
                    post_to_messages[content['post_num']][uid] = msg.message_id
                    message_to_post[(uid, msg.message_id)] = content['post_num']

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

                    import re
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

async def conan_roaster():
    """Каждые 5-18 минут Конан отвечает рандомному посту."""
    while True:
        try:
            await asyncio.sleep(secrets.randbelow(3600) + 1600)  # 5-15 минут

            # Проверяем что есть посты и активные пользователи
            if not messages_storage or not state["users_data"]["active"]:
                continue

            # Выбираем случайный пост из последних 50 (только те, что есть в post_to_messages)
            valid_posts = [
                p for p in messages_storage.keys() if p in post_to_messages
            ]
            if not valid_posts:
                continue

            post_num = secrets.choice(valid_posts[-50:] if len(valid_posts) > 50 else valid_posts)
            original_author = messages_storage.get(post_num, {}).get('author_id')

            # Проверяем что есть кому отвечать
            reply_map = post_to_messages.get(post_num, {})
            if not reply_map:
                continue

            # Генерируем фразу
            phrase = conan_phrase()

            # Формируем текст ответа без (You) - оно добавится автоматически при отправке
            reply_text = f">>{post_num}\n{phrase}"

            header, new_pnum = format_header()

            # Сохраняем метаданные
            messages_storage[new_pnum] = {
                'author_id': 0,  # 0 = системное сообщение
                'timestamp': datetime.now(MSK),
                'content': {
                    'type': 'text',
                    'header': header,
                    'text': phrase
                },
                'reply_to': post_num
            }

            # Формируем контент для отправки
            content = {
                "type": "text",
                "header": header,
                "text": phrase,
                "reply_to_post": post_num
            }

            # Отправляем всем
            await message_queue.put({
                "recipients": state["users_data"]["active"],
                "content": content,
                "post_num": new_pnum,
                "reply_info": reply_map,
                "original_author": original_author  # Передаем ID автора для корректной обработки (You)
            })

            print(f"Conan reply to #{post_num}: {phrase[:50]}...")

        except Exception as e:
            print(f"Conan error: {e}")
            await asyncio.sleep(5)

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
            delay = random.randint(4400, 14400)  # 2-4 часа в секундах
            await asyncio.sleep(delay)

            # Проверяем что есть активные пользователи
            if not state['users_data']['active']:
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

            message_text = (f"{header}\n\n"
                          f"💭 {motivation}\n\n"
                          f"Скопируй и отправь анончикам:\n"
                          f"<code>{invite_text}</code>")

            # Отправляем всем активным, кроме забаненных и замьюченных
            recipients = state['users_data']['active'] - state['users_data']['banned'] - set(mutes.keys())

            if not recipients:
                continue

            content = {
                'type': 'text',
                'header': header,
                'text': f"💭 {motivation}\n\nСкопируй и отправь анончикам:\n<code>{invite_text}</code>"
            }

            await message_queue.put({
                'recipients': recipients,
                'content': content,
                'post_num': post_num,
                'reply_info': None
            })

            print(f"Отправлено мотивационное сообщение #{post_num}")

        except Exception as e:
            print(f"Ошибка в motivation_broadcaster: {e}")
            await asyncio.sleep(60)  # Ждем минуту при ошибке

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
        "Просто пиши сообщения, они будут отправлены всем анонимно. Всем от всех.")
    await message.delete()


AHE_EYES = ['😵', '🤤', '😫', '😩', '😳', '😖', '🥵']
AHE_TONGUE = ['👅', '💦', '😛', '🤪', '😝']
AHE_EXTRA = ['💕', '💗', '✨', '🥴', '']


@dp.message(Command("restore_backup"))
async def cmd_restore_backup(message: types.Message):
    """Восстановить из последнего бэкапа"""
    if not is_admin(message.from_user.id):
        return

    # Ищем последний бэкап
    backups = sorted(glob.glob('backup_state_*.json'))
    if not backups:
        await message.answer("❌ Нет бэкапов")
        return

    latest = backups[-1]
    shutil.copy(latest, 'state.json')
    await message.answer(f"✅ Восстановлено из {latest}")

    # Перезагружаем
    load_state()

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
    await message.answer("Это анонимный чат ТГАЧ\n\n"
                         "Команды:\n"
                         "/start - начать\n"
                         "/help - помощь\n"
                         "/stats - статистика\n"
                         "/face \n"
                         "/roll – ролл 0-100 или /roll N\n"
                         "Все сообщения анонимны!")
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
    """
    Получаем ID автора поста, на который отвечает админ.
    1. Берём message_id ответа
    2. Находим post_num через message_to_post
    3. Берём author_id из messages_storage
    """
    if not msg.reply_to_message:
        return None

    reply_mid = msg.reply_to_message.message_id
    key_any_user = None

    # reply пришёл от бота к текущему администратору
    # message_to_post хранит ключ (любое_user_id, message_id)
    for (uid, mid), pnum in message_to_post.items():
        if mid == reply_mid:
            key_any_user = (uid, mid)
            post_num = pnum
            break
    else:
        return None  # не нашли

    # Добавляем проверку существования post_num в messages_storage
    if post_num not in messages_storage:
        return None

    return messages_storage[post_num].get("author_id")


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

@dp.message(Command("mute"))
async def cmd_mute(message: Message, command: CommandObject):
    """Мут пользователя"""
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
        target_id = message_to_post.get(reply_key)
        if target_id:
            target_id = messages_storage.get(target_id, {}).get('author_id')

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

    # Удаляем посты пользователя за последние 5 минут (было 30)
    deleted_posts = await delete_user_posts(target_id, 5)

    # Применяем мут
    mutes[target_id] = datetime.now(UTC) + mute_duration

    # Отправляем подтверждение
    await message.answer(
        f"🔇 Хуила {target_id} замучен на {duration_text}\n"
        f"Удалено его постов за последние 5 минут: {deleted_posts}",
        parse_mode="HTML"
    )

    # Отправляем уведомление в чат
    header, post_num = format_header()
    header = header.replace("Пост", "### АДМИН ###")
    mute_text = (
        f"🚨 Пидораса спамера замутило нахуй на {duration_text}\n"
        f"Удалено его постов за последние 5 минут: {deleted_posts}\n"
        f"Этот пидор всех уже доебал, пускай попустится и отдохнёт."
    )

    # Сохраняем сообщение
    messages_storage[post_num] = {
        'author_id': 0,  # 0 = системное сообщение
        'timestamp': datetime.now(MSK),
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
            f"Удалено твоих постов за последние 5 минут: {deleted_posts}\n"
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
    if not is_admin(message.from_user.id):
        return

    global message_to_post  # Перенесено в начало функции

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
    if not is_admin(message.from_user.id):
        return

    if not message.reply_to_message:
        await message.answer("Ответь на сообщение, которое нужно удалить")
        return

    # 1. Ищем post_num по message_id (неважно, кто отправитель)
    target_mid = message.reply_to_message.message_id
    post_num = None
    for pnum, mapping in post_to_messages.items():
        if target_mid in mapping.values():
            post_num = pnum
            break

    # Если не нашли – сообщаем
    if post_num is None:
        await message.answer("Не нашёл этот пост в базе")
        return

    # 2. Удаляем у всех получателей
    deleted = 0
    for uid, mid in post_to_messages.get(post_num, {}).items():
        try:
            await bot.delete_message(uid, mid)
            deleted += 1
        except:
            pass

    # 3. Удаляем у автора (если сохранили author_message_id)
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
    """Отправка собранного медиа-альбома без дублирования"""
    if media_group_id not in current_media_groups or media_group_id in sent_media_groups:
        return

    media_group = current_media_groups[media_group_id]
    if not media_group.get('media'):
        if media_group_id in current_media_groups:
            del current_media_groups[media_group_id]
        return

    # Проверяем reply_to_post
    if media_group.get('reply_to_post') and media_group['reply_to_post'] not in messages_storage:
        media_group['reply_to_post'] = None

    # Помечаем как отправленную
    sent_media_groups.add(media_group_id)

    post_num = media_group['post_num']
    user_id = media_group['author_id']

    # Формируем контент для сохранения
    content = {
        'type': 'media_group',
        'header': media_group['header'],
        'media': media_group['media'],
        'caption': media_group.get('caption'),
        'reply_to_post': media_group.get('reply_to_post')
    }

    # Сохраняем сообщение
    messages_storage[post_num] = {
        'author_id': user_id,
        'timestamp': media_group['timestamp'],
        'content': content,
        'reply_to': media_group.get('reply_to_post')
    }

    # Отправляем автору
    try:
        builder = MediaGroupBuilder()
        reply_to_message_id = None

        # Если это ответ, получаем message_id для reply
        if media_group.get('reply_to_post'):
            reply_map = post_to_messages.get(media_group['reply_to_post'], {})
            reply_to_message_id = reply_map.get(user_id)

        # Добавляем медиа с подписью только к первому элементу
        for idx, media in enumerate(media_group['media']):
            if not media.get('file_id'):
                continue

            caption = None
            if idx == 0:
                caption = f"<i>{media_group['header']}</i>"
                if media_group.get('caption'):
                    caption += f"\n\n{escape_html(media_group['caption'])}"

            if media['type'] == 'photo':
                builder.add_photo(
                    media=media['file_id'],
                    caption=caption,
                    parse_mode="HTML" if caption else None
                )
            elif media['type'] == 'video':
                builder.add_video(
                    media=media['file_id'],
                    caption=caption,
                    parse_mode="HTML" if caption else None
                )
            elif media['type'] == 'document':
                builder.add_document(
                    media=media['file_id'],
                    caption=caption,
                    parse_mode="HTML" if caption else None
                )
            elif media['type'] == 'audio':
                builder.add_audio(
                    media=media['file_id'],
                    caption=caption,
                    parse_mode="HTML" if caption else None
                )

        # Проверяем, что есть что отправлять
        if not builder.build():  # Проверяем собранные медиа
            return

        sent_messages = await bot.send_media_group(
            chat_id=user_id,
            media=builder.build(),
            reply_to_message_id=reply_to_message_id
        )

        # Сохраняем связь сообщения с постом
        if sent_messages:
            messages_storage[post_num]['author_message_id'] = sent_messages[0].message_id
            if post_num not in post_to_messages:
                post_to_messages[post_num] = {}
            post_to_messages[post_num][user_id] = sent_messages[0].message_id
            for msg in sent_messages:
                message_to_post[(user_id, msg.message_id)] = post_num

    except Exception as e:
        print(f"Ошибка отправки медиа-альбома автору: {e}")
        return

    # Отправляем остальным пользователям через очередь ТОЛЬКО ОДИН РАЗ
    recipients = state['users_data']['active'] - {user_id}
    if recipients:
        await message_queue.put({
            'recipients': recipients,
            'content': content,
            'post_num': post_num,
            'reply_info': post_to_messages.get(media_group['reply_to_post'], {}) if media_group.get('reply_to_post') else None
        })

    # Удаляем временные данные
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
        reply_key = (user_id, message.reply_to_message.message_id)
        reply_to_post = message_to_post.get(reply_key)
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

# Добавьте этот обработчик перед @dp.message()
@dp.message(F.media_group_id)
async def handle_media_group_init(message: Message):
    """Обработчик медиа-альбомов с защитой от дублирования"""
    user_id = message.from_user.id

    if user_id in state['users_data']['banned']:
        await message.delete()
        return

    if mutes.get(user_id) and mutes[user_id] > datetime.now(UTC):
        await message.delete()
        return

    media_group_id = message.media_group_id

    # Проверяем reply_to_message для ответов
    reply_to_post = None
    if message.reply_to_message:
        reply_key = (user_id, message.reply_to_message.message_id)
        reply_to_post = message_to_post.get(reply_key)
        if reply_to_post and reply_to_post not in messages_storage:
            reply_to_post = None

    if media_group_id not in current_media_groups:
        header, post_num = format_header()
        current_media_groups[media_group_id] = {
            'post_num': post_num,
            'header': header,
            'author_id': user_id,
            'timestamp': datetime.now(MSK),
            'media': [],
            'caption': message.caption if message.caption else None,
            'reply_to_post': reply_to_post,
            'processed_messages': set()
        }

    # Добавляем медиа в группу только если это новое сообщение
    if message.message_id not in current_media_groups[media_group_id]['processed_messages']:
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

        if media_data['file_id']:  # Только если есть file_id
            current_media_groups[media_group_id]['media'].append(media_data)
            current_media_groups[media_group_id]['processed_messages'].add(message.message_id)

            # Обновляем подпись, если она есть
            if message.caption and not current_media_groups[media_group_id].get('caption'):
                current_media_groups[media_group_id]['caption'] = message.caption

    await message.delete()

    # Обрабатываем группу через 0.5 секунды
    await asyncio.sleep(0.5)
    await process_complete_media_group(media_group_id)

@dp.message()
async def handle_message(message: Message):
    """Основной обработчик сообщений"""
    # Пропускаем сообщения, которые являются частью медиа-группы
    if message.media_group_id:
        return

    user_id = message.from_user.id
    
    try:
        # 1. Проверка мута (должна быть ПЕРВОЙ)
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
            reply_key = (user_id, message.reply_to_message.message_id)
            reply_to_post = message_to_post.get(reply_key)

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
            content['text'] = text_content
        elif content_type == 'photo':
            content['file_id'] = message.photo[-1].file_id
            content['caption'] = message.caption
        elif content_type == 'video':
            content['file_id'] = message.video.file_id
            content['caption'] = message.caption
        elif content_type == 'sticker':
            content['file_id'] = message.sticker.file_id
        elif content_type == 'animation':
            content['file_id'] = message.animation.file_id
            content['caption'] = message.caption
        elif content_type == 'document':
            content['file_id'] = message.document.file_id
            content['caption'] = message.caption
        elif content_type == 'voice':
            content['file_id'] = message.voice.file_id
        elif content_type == 'audio':
            content['file_id'] = message.audio.file_id
            content['caption'] = message.caption
        elif content_type == 'video_note':
            content['file_id'] = message.video_note.file_id

        # Инициализируем запись в хранилище перед отправкой
        messages_storage[current_post_num] = {
            'author_id': user_id,
            'timestamp': datetime.now(MSK),
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

# ============ СТАРТ БОТА (один loop, автоперезапуск polling) ============
async def start_background_tasks():
    """Поднимаем все фоновые корутины ОДИН раз за весь runtime"""
    global message_queue
    message_queue = asyncio.Queue(
        maxsize=5000)  # очередь, привязана к текущему loop


    tasks = [
        asyncio.create_task(auto_save_state()),
        asyncio.create_task(message_broadcaster()),
        asyncio.create_task(message_broadcaster()),
        asyncio.create_task(message_broadcaster()),
        asyncio.create_task(conan_roaster()), 
        asyncio.create_task(motivation_broadcaster()), 
        asyncio.create_task(auto_memory_cleaner()),
        asyncio.create_task(auto_backup()), 
        asyncio.create_task(cleanup_old_messages()),
    ]
    print(f"✓ Background tasks started: {len(tasks)}")
    return tasks 

async def supervisor():
    """One event-loop: background tasks live forever, polling restarts."""
    global bot, connector

    load_state()
    
    try:
        # Запускаем веб-сервер синхронно (не как задачу)
        runner = await start_web_server()
        
        # Запускаем фоновые задачи
        bg_tasks = await start_background_tasks()

        # Инициализация бота
        connector = aiohttp.TCPConnector(limit=10, force_close=True)
        bot = Bot(token=BOT_TOKEN, connector=connector)

        # Основной цикл работы бота
        restart_count = 0
        max_restarts = 10  # Максимальное количество перезапусков
        restart_delay = 30  # Задержка между перезапусками в секундах
        
        while restart_count < max_restarts:
            try:
                print("▶️ Start polling...")
                await dp.start_polling(
                    bot, 
                    allowed_updates=dp.resolve_used_update_types(), 
                    close_bot_session=False,
                    handle_signals=False
                )
            except TelegramNetworkError as e:
                restart_count += 1
                print(f"⚠️ Network error: {e} (restarting in {restart_delay} seconds)")
                await asyncio.sleep(restart_delay)
            except Exception as e:
                restart_count += 1
                print(f"⚠️ Unexpected error: {e} (restarting in {restart_delay} seconds)")
                await asyncio.sleep(restart_delay)
            else:
                print("⏹️ Polling finished normally")
                break

            # Проверяем нужно ли перезапускать из-за изменений в main.py
            if check_restart_needed():
                print("🔄 Обнаружены изменения в main.py - требуется перезапуск")
                break

    except asyncio.CancelledError:
        print("⚠️ Received cancellation signal")
    except Exception as e:
        print(f"🔥 Critical error in supervisor: {e}")
    finally:
        # Корректное завершение
        print("🛑 Shutting down...")
        await shutdown()
        if 'runner' in locals():
            await runner.cleanup()
        if 'bot' in globals():
            await bot.session.close()
        if 'connector' in globals():
            await connector.close()
        print("✅ Clean shutdown completed")
        
async def shutdown():
    """Cleanup tasks before shutdown"""
    print("Shutting down services...")
    try:
        await dp.storage.close()
        await dp.storage.wait_closed()
    except Exception as e:
        print(f"Error during storage shutdown: {e}")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    
    try:
        asyncio.run(supervisor())
    except KeyboardInterrupt:
        print("✖️  Bot stopped by Ctrl-C")
