import asyncio
import random
from help_text import HELP_TEXT
from datetime import datetime, UTC
from main import board_data, message_queues, messages_storage, format_header, BOARDS

async def help_broadcaster():
    """Рассылает команды бота на каждую доску в разное время."""
    await asyncio.sleep(20)  # Начальная задержка
    
    async def board_help_worker(board_id: str):
        """Индивидуальный воркер для одной доски."""
        while True:
            try:
                # Случайный интервал 2-3 часа в секундах
                delay = random.randint(7200, 10800) # Было 10800, 16800. Уменьшил, чтобы было чаще.
                await asyncio.sleep(delay)
                
                b_data = board_data[board_id]
                recipients = b_data['users']['active'] - b_data['users']['banned']
                
                if not recipients:
                    continue
                
                header = "### Админ ###"
                # Используем общую функцию format_header для инкремента счетчика
                _, post_num = await format_header(board_id)
                
                content = {
                    "type": "text",
                    "header": header,
                    "text": HELP_TEXT,
                    "is_system_message": True
                }
                
                await message_queues[board_id].put({
                    "recipients": recipients,
                    "content": content,
                    "post_num": post_num,
                    "board_id": board_id
                })
    
                # Сохраняем системное сообщение в общем хранилище
                messages_storage[post_num] = {
                    'author_id': 0,
                    'timestamp': datetime.now(UTC), # <--- ИЗМЕНЕНИЕ ЗДЕСЬ
                    'content': content,
                    'board_id': board_id
                }
                
                print(f"✅ [{board_id}] Рассылка помощи #{post_num} отправлена в очередь")
                
            except Exception as e:
                print(f"❌ [{board_id}] Ошибка в help_broadcaster: {e}")
                await asyncio.sleep(120)

    # Запускаем по одному воркеру на каждую доску
    tasks = [asyncio.create_task(board_help_worker(bid)) for bid in BOARDS]
    await asyncio.gather(*tasks)
