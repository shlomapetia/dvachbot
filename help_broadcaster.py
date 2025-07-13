import asyncio
import random
from help_text import HELP_TEXT

async def help_broadcaster(state, message_queue, format_header_func):
    """Рассылает команды бота каждые 2-3 часа"""
    await asyncio.sleep(10)  # Начальная задержка
    
    while True:
        try:
            # Случайный интервал 2-3 часа в секундах
            delay = random.randint(7200, 10800)
            await asyncio.sleep(delay)
            
            if not state['users_data']['active']:
                continue
                
            # Формируем заголовок с помощью переданной функции
            header = "### Админ ###"
            state['post_counter'] += 1
            post_num = state['post_counter']
            
            await message_queue.put({
                "recipients": state['users_data']['active'],
                "content": {
                    "type": "text",
                    "header": header,
                    "text": HELP_TEXT
                },
                "post_num": post_num
            })
            print(f"✅ Рассылка команд отправлена в очередь")
            
        except Exception as e:
            print(f"Ошибка в help_broadcaster: {e}")
            await asyncio.sleep(60)
