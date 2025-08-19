# help_text.py

# --- Тексты только для команд ---

HELP_TEXT = (
    "Это анонимный чат ТГАЧ. Просто пиши сообщения, они будут отправлены всем анонимно. Всем от всех.\n\n"
    "<b>Команды:</b>\n"
    "/start - начать\n"
    "/help - помощь\n"
    "/stats - статистика\n"
    "/face - случайное лицо\n"
    "/roll - ролл 0-100\n"
    "/invite - пригласить анонов\n"
    "/deanon - деанон постера\n"
    "/active - активность досок\n"
    "/anime - режим 🌸 Аниме\n"
    "/zaputin - режим 🇷🇺 За Путина\n"
    "/slavaukraine - режим 💙💛 Слава Украине\n"
    "/suka_blyat - режим 💢 Сука Блять"
)

HELP_TEXT_EN = (
    "This is TGACH, an anonymous chat. Just send messages, and they will be sent to everyone anonymously. From everyone, to everyone.\n\n"
    "<b>Commands:</b>\n"
    "/start - start the bot\n"
    "/help - show this help message\n"
    "/stats - show statistics\n"
    "/face - get a random face\n"
    "/roll - roll a number from 0-100\n"
    "/invite - get text to invite new anons\n"
    "/deanon - de-anonymize a poster\n"
    "/active - boards activity\n"
    "/anime - activate 🌸 Anime mode"
)
HELP_TEXT_COMMANDS = (
    "Это анонимный чат ТГАЧ. Просто пиши сообщения, они будут отправлены всем анонимно. Всем от всех.\n\n"
    "<b>Команды:</b>\n"
    "/start - начать\n"
    "/help - помощь\n"
    "/stats - статистика\n"
    "/face - случайное лицо\n"
    "/roll - ролл 0-100\n"
    "/invite - пригласить анонов\n"
    "/deanon - деанон постера\n"
    "/active - активность досок\n"
    "/anime - режим 🌸 Аниме\n"
    "/zaputin - режим 🇷🇺 За Путина\n"
    "/slavaukraine - режим 💙💛 Слава Украине\n"
    "/suka_blyat - режим 💢 Сука Блять"
)

HELP_TEXT_EN_COMMANDS = (
    "This is TGACH, an anonymous chat. Just send messages, and they will be sent to everyone anonymously. From everyone, to everyone.\n\n"
    "<b>Commands:</b>\n"
    "/start - start the bot\n"
    "/help - show this help message\n"
    "/stats - show statistics\n"
    "/face - get a random face\n"
    "/roll - roll a number from 0-100\n"
    "/invite - get text to invite new anons\n"
    "/deanon - de-anonymize a poster\n"
    "/active - boards activity\n"
    "/anime - activate 🌸 Anime mode"
)


# --- Функция для генерации списка досок ---

def generate_boards_list(board_configs: dict, lang: str = 'ru') -> str:
    """
    Генерирует форматированный HTML-список досок из конфига.
    """
    if lang == 'en':
        header = "🌐 <b>All boards:</b>"
        board_lines = [
            f"<b>{config['name']}</b> {config['description_en']} - {config['username']}"
            for b_id, config in board_configs.items() if b_id != 'test'
        ]
    else:
        header = "🌐 <b>Все доски:</b>"
        board_lines = [
            f"<b>{config['name']}</b> {config['description']} - {config['username']}"
            for b_id, config in board_configs.items() if b_id != 'test'
        ]
    
    return f"{header}\n" + "\n".join(board_lines)
