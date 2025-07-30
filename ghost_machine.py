import asyncio
import random
import re
from collections import deque, defaultdict
from datetime import datetime, UTC

from japanese_translator import get_random_anime_image, anime_transform
from ukrainian_mode import ukrainian_transform
from zaputin_mode import zaputin_transform

try:
    import markovify
except ImportError:
    print("❌ [Ghost Machine] Библиотека markovify не установлена. Призрак не сможет работать. Установите: pip install markovify")
    markovify = None

from typing import TYPE_CHECKING, List
if TYPE_CHECKING:
    pass  # Теперь defaultdict импортируется напрямую выше

# --- Константы ---
MIN_MESSAGES_FOR_TRAINING = 50
MIN_GHOST_POST_DELAY_SEC = 1 * 3600
MAX_GHOST_POST_DELAY_SEC = 4 * 3600

GHOST_HEADERS = ["???", "[АБУ]", "...", "ПИДОРАС", "[НЕЧТО]", "ХОХОЛ", "КОНАН", "ТГАЧ"]
REPLY_CHANCE = 0.4  # Увеличен шанс на ответ, т.к. это основная "умная" механика
SELF_REPLY_CHANCE = 0.20
MEDIA_CHANCE = 0.1
MANIC_EPISODE_CHANCE = 0.25

# Для адаптации под режим /suka_blyat без циклического импорта
MAT_WORDS = ["сука", "блядь", "пиздец", "ебать", "нах", "пизда", "хуйня", "ебал", "блять", "отъебись", "ебаный", "еблан", "ХУЙ", "ПИЗДА", "хуйло", "долбаёб", "пидорас"]

class TextModel(markovify.Text if markovify else object):
    def sentence_split(self, text):
        text = [re.sub(r'(>>\d+|<[^>]+>)', '', msg).strip() for msg in text if msg]
        return [line for line in text if len(line.split()) > 1]

# --- Функции-генераторы ---
def _generate_short_post(model: TextModel, **kwargs) -> str | None:
    return model.make_short_sentence(min_chars=15, max_chars=100, tries=100)

def _generate_story_post(model: TextModel, length_hint: int = 0, **kwargs) -> str | None:
    # Динамическая длина истории
    num_sentences = 2
    if length_hint > 200: num_sentences = random.randint(3, 5)
    elif length_hint > 80: num_sentences = random.randint(2, 3)
    
    story = [s for s in (model.make_sentence(tries=100) for _ in range(num_sentences)) if s]
    return ". ".join(s.capitalize() for s in story) + "." if story else None

def _generate_greentext_post(model: TextModel, length_hint: int = 0, **kwargs) -> str | None:
    # Динамическая длина greentext
    num_lines = 2
    if length_hint > 150: num_lines = random.randint(3, 5)
    elif length_hint > 50: num_lines = random.randint(2, 3)
    
    story = [f"<code>>{s}</code>" for s in (model.make_sentence(tries=100) for _ in range(num_lines)) if s]
    return "\n".join(story) if story else None

async def _send_ghost_post(post_data: dict, board_id: str, b_data, message_queues, messages_storage, post_to_messages):
    """Вспомогательная функция для отправки одного поста Призрака."""
    recipients = b_data['users']['active'] - b_data['users']['banned']
    if not recipients: return

    post_num, header, message = post_data['post_num'], post_data['header'], post_data['message']
    reply_to = post_data.get('reply_to')
    
    # --- ПОЛНАЯ АДАПТАЦИЯ ПОД АКТИВНЫЙ РЕЖИМ ---
    if b_data.get('anime_mode'): message = anime_transform(message)
    elif b_data.get('zaputin_mode'): message = zaputin_transform(message)
    elif b_data.get('slavaukraine_mode'): message = ukrainian_transform(message)
    elif b_data.get('suka_blyat_mode'):
        words = message.split()
        message = ' '.join([random.choice(MAT_WORDS) if random.random() < 0.3 else word for word in words])

    content = {'type': 'text', 'header': header, 'text': message, 'reply_to_post': reply_to, 'is_system_message': True}
    reply_info = post_to_messages.get(reply_to, {}) if reply_to else {}

    if random.random() < MEDIA_CHANCE and content['type'] == 'text':
        img_url = await get_random_anime_image()
        if img_url:
            content.update({'type': 'photo', 'image_url': img_url, 'caption': message})
            content.pop('text')

    messages_storage[post_num] = {'author_id': 0, 'timestamp': datetime.now(UTC), 'content': content, 'board_id': board_id}
    await message_queues[board_id].put({'recipients': recipients, 'content': content, 'post_num': post_num, 'board_id': board_id, 'reply_info': reply_info})
    print(f"👻 ...отправлен пост #{post_num}...")


# --- Основной процесс "Призрака" ---
async def ghost_poster(
    last_messages: deque, message_queues: dict, messages_storage: dict,
    post_to_messages: dict, format_header, board_data: defaultdict, BOARDS: list
):
    if not markovify: return
    print("👻 [Ghost Machine] Версия 'Контекстуальный Хищник' запущена.")
    
    generation_strategies = [(_generate_short_post, 50), (_generate_story_post, 20), (_generate_greentext_post, 30)]

    while True:
        try:
            await asyncio.sleep(random.randint(MIN_GHOST_POST_DELAY_SEC, MAX_GHOST_POST_DELAY_SEC))
            if len(last_messages) < MIN_MESSAGES_FOR_TRAINING: continue

            destination_board_id = random.choice(BOARDS)
            b_data = board_data[destination_board_id]
            if not (b_data['users']['active'] - b_data['users']['banned']): continue

            corpus: List[str] = list(last_messages)
            reply_to_post_num = None
            target_text_for_priming = ""
            force_generator = None

            # --- УЛУЧШЕННАЯ ЛОГИКА ОТВЕТА ---
            if random.random() < REPLY_CHANCE and messages_storage:
                # Шанс ответить самому себе
                if random.random() < SELF_REPLY_CHANCE:
                    ghost_posts = [p for p, data in messages_storage.items() if data.get('author_id') == 0 and data.get('content', {}).get('header', '').startswith(tuple(GHOST_HEADERS))]
                    if ghost_posts: reply_to_post_num = random.choice(ghost_posts[-20:])
                
                # Если не отвечаем себе, ищем цель среди юзеров
                if not reply_to_post_num:
                    user_posts = [p for p, data in messages_storage.items() if data.get('author_id') != 0]
                    if user_posts: reply_to_post_num = random.choice(user_posts[-150:])

                # --- Контекстуальная Мимикрия и Прайминг ---
                if reply_to_post_num:
                    target_content = messages_storage.get(reply_to_post_num, {}).get('content', {})
                    raw_text = target_content.get('text') or target_content.get('caption', '')
                    target_text_for_priming = re.sub(r'<[^>]+>', '', raw_text) # Очищаем текст цели для прайминга
                    
                    if target_content.get('type') == 'text' and target_content.get('text', '').strip().startswith('<code>>'):
                        force_generator = _generate_greentext_post

            # --- Создание и Прайминг Модели ---
            final_corpus = corpus
            if target_text_for_priming:
                print(f"👻 [Ghost Machine] Прайминг на тексте поста #{reply_to_post_num}...")
                final_corpus += [target_text_for_priming] * 10 # Добавляем текст цели 10 раз для усиления влияния

            state_size = random.choice([1, 2, 2, 3])
            text_model = TextModel(final_corpus, state_size=state_size, well_formed=False)
            
            # Генерация сообщения
            length_hint = len(target_text_for_priming)
            generator = force_generator or random.choices([s[0] for s in generation_strategies], [s[1] for s in generation_strategies], k=1)[0]
            ghost_message = generator(model=text_model, length_hint=length_hint) or "..."

            # --- Основной пост ---
            _, post_num = await format_header(destination_board_id)
            current_post_data = {
                'post_num': post_num,
                'header': f"{random.choice(GHOST_HEADERS)} Пост №{post_num}",
                'message': ghost_message,
                'reply_to': reply_to_post_num
            }
            await _send_ghost_post(current_post_data, destination_board_id, b_data, message_queues, messages_storage, post_to_messages)

            # --- Маниакальный Эпизод (с самоответами) ---
            is_self_reply_chain = reply_to_post_num and messages_storage.get(reply_to_post_num, {}).get('author_id') == 0
            if (not reply_to_post_num or is_self_reply_chain) and random.random() < MANIC_EPISODE_CHANCE:
                print(f"👻 [Ghost Machine] Начало маниакального эпизода...")
                for i in range(random.randint(1, 2)):
                    await asyncio.sleep(random.uniform(2.0, 4.0))
                    
                    current_post_data['reply_to'] = current_post_data['post_num']
                    _, new_post_num = await format_header(destination_board_id)
                    current_post_data['post_num'] = new_post_num
                    current_post_data['header'] = f"{random.choice(GHOST_HEADERS)} Пост №{new_post_num}"
                    # Сообщения становятся короче
                    current_post_data['message'] = text_model.make_short_sentence(max_chars=60 - i * 20, tries=50) or "..."
                    
                    await _send_ghost_post(current_post_data, destination_board_id, b_data, message_queues, messages_storage, post_to_messages)

        except Exception as e:
            import traceback
            print(f"❌ КРИТИЧЕСКАЯ ОШИБКА в ghost_poster: {e}\n{traceback.format_exc()}")
            await asyncio.sleep(900)```
