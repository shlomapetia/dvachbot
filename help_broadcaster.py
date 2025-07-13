# help_broadcaster.py
import asyncio
import random
from help_text import HELP_TEXT

# Функция теперь принимает всё, что ей нужно для работы
async def help_broadcaster(
    get_global_post_counter, # Функция для получения и увеличения счетчика
    boards_data,             # Словарь со всеми досками
    board_info_dict,         # Словарь с информацией о досках (имена, юзернеймы)
    queues,                  # Словарь с очередями
    ):
    """Рассылает команды бота на все доски каждые 2-3 часа."""
    await asyncio.sleep(10)

    while True:
        try:
            delay = random.randint(7200, 10800)
            await asyncio.sleep(delay)

            # Проходим по всем доскам, используя переданные данные
            for board_id, board_state in boards_data.items():
                if not board_state['users_data']['active']:
                    continue

                # Формируем сообщение с адресами других досок
                help_text_with_links = f"{HELP_TEXT}\n\n**Наши доски:**\n"
                for b_id, b_info in board_info_dict.items():
                    help_text_with_links += f"- **{b_info['name']}** ({b_info['description']}) -> {b_info['username']}\n"

                header = "### Админ ###"
                
                # Получаем и увеличиваем глобальный счетчик через переданную функцию
                post_num = get_global_post_counter()

                content = {
                    "type": "text",
                    "header": header,
                    "text": help_text_with_links
                }

                # Кладем в очередь нужной доски
                await queues[board_id].put({
                    "recipients": board_state['users_data']['active'],
                    "content": content,
                    "post_num": post_num
                })

            print(f"✅ Рассылка помощи (/help) отправлена в очереди всех досок.")

        except Exception as e:
            print(f"Ошибка в help_broadcaster: {e}")
            await asyncio.sleep(60)
