# help_text.py
import random

# --- Варианты для рассылки списка команд ---

HELP_TEXT_COMMANDS = [
    (
        "<b>Команды, которые ты можешь использовать в этом чате:</b>\n\n"
        "/start - начать\n"
        "/help - помощь\n"
        "/stats - статистика\n"
        "/roll - ролл 0-100\n"
        "/invite - пригласить анонов\n"
        "/deanon - деанон постера\n"
        "/active - активность досок\n"
        "/hent /fap /gatari /hentai /nsfw - аниме пикча\n"
        "/anime - режим 🌸 Аниме\n"
        "/zaputin - режим 🇷🇺 За Путина\n"
        "/slavaukraine - режим 💙💛 Слава Украине\n"
        "/kurwa - Польский режим\n"
        "/wh40k - режим 💢 За Императора! \n"
        "/yer - Царскiй режим\n"
        "/b /po /a /vg /sex /int /threads -команды для перехода на другую доску\n\n"
        "Канал ТГАЧ НОВОСТИ  https://t.me/tgach_bot\n"
        "Канал ТГАЧ АРХИВ (все посты) https://t.me/tgach_archive\n"
    ),
    (
        "<b>Абу напоминает, что есть команды:</b>\n\n"
        "/start - если ты заблудился\n"
        "/help - если совсем тупой\n"
        "/stats - посмотреть, как мы срем в чат\n"
        "/roll - испытать удачу\n"
        "/invite - затащить сюда друзей-дегенератов\n"
        "/deanon - устроить кибербуллинг\n"
        "/active - где больше всего срут\n"
        "/hent /fap /gatari /hentai /nsfw - подрочить\n"
        "/anime - 🌸 режим для пидоров\n"
        "/zaputin - 🇷🇺 режим для z-пидоров\n"
        "/slavaukraine - 💙💛 режим для хохло-пидоров\n"
        "/suka_blyat - 💢 режим для просто пидоров\n"
        "/kurwa - Польский режим\n"
        "/wh40k - режим 💢 За Императора! \n"
        "/yer - Царскiй режим\n"
        "/b /po /a /vg /sex /int /threads -команды для перехода на другую доску\n\n"
        "Канал ТГАЧ НОВОСТИ  https://t.me/tgach_bot\n"
        "Канал ТГАЧ АРХИВ (все посты) https://t.me/tgach_archive\n"
    ),
    (
        "<b>Список доступных команд:</b>\n\n"
        "/start - Начать/Перезапустить.\n"
        "/help - Показать это сообщение.\n"
        "/stats - Показать статистику чата.\n"
        "/roll - Случайное число от 0 до 100.\n"
        "/invite - Получить текст для приглашения.\n"
        "/deanon - Деанонимизировать автора сообщения.\n"
        "/active - Показать активность всех досок.\n"
        "/hent /fap /gatari /hentai /nsfw - Отправить аниме.\n"
        "/anime - Включить режим 🌸 Аниме.\n"
        "/zaputin - Включить режим 🇷🇺 За Путина.\n"
        "/slavaukraine - Включить режим 💙💛 Слава Украине.\n"
        "/suka_blyat - Включить режим 💢 Сука Блять.\n"
        "/kurwa - включить Польский режим\n"
        "/wh40k - включить режим 💢 За Императора! \n"
        "/yer - включить Царскiй режим\n"
        "/b /po /a /vg /sex /int /threads - команды для перехода на другую доску\n\n"
        "Канал ТГАЧ НОВОСТИ  https://t.me/tgach_bot\n"
        "Канал ТГАЧ АРХИВ (все посты) https://t.me/tgach_archive\n"
    )
]

HELP_TEXT_EN_COMMANDS = [
    (
        "<b>Available commands you can use in this mess:</b>\n\n"
        "/start - start the bot\n"
        "/help - show this help message\n"
        "/stats - show statistics\n"
        "/roll - roll a number from 0-100\n"
        "/invite - get text to invite new anons\n"
        "/deanon - de-anonymize a poster\n"
        "/active - boards activity\n"
        "/hent /fap /gatari /hentai /nsfw - anime pic\n"
        "/anime - activate 🌸 Anime mode"
        "/b /po /a /vg /sex /int /threads - other boards\n\n"
        "TGCHAN NEWS  https://t.me/tgach_bot\n"
        "TGCHAN archive https://t.me/tgach_archive\n"
    ),
    (
        "<b>List of available commands:</b>\n\n"
        "/start - To start or restart.\n"
        "/help - To show this message.\n"
        "/stats - To show chat statistics.\n"
        "/roll - A random number from 0 to 100.\n"
        "/invite - To get an invitation text.\n"
        "/deanon - To de-anonymize a message author.\n"
        "/active - To show the activity of all boards.\n"
        "/hent /fap /gatari /hentai /nsfw  - To send an anime art.\n"
        "/anime - To activate the 🌸 Anime mode."
        "/b /po /a /vg /sex /int /threads - other boards\n\n"
        "TGCHAN NEWS  https://t.me/tgach_bot\n"
        "TGCHAN archive https://t.me/tgach_archive\n"
    ),
    (
        "<b>Abu reminds you that there are commands:</b>\n\n"
        "/start - if you are lost\n"
        "/help - if you are completely stupid\n"
        "/stats - to see how we shit in the chat\n"
        "/roll - to try your luck\n"
        "/invite - to drag your degenerate friends here\n"
        "/deanon - to arrange cyberbullying\n"
        "/active - where the most shit is\n"
        "/hent /fap /gatari /hentai /nsfw - to fap\n"
        "/anime - 🌸 mode for faggots"
        "/b /po /a /vg /sex /int /threads - other boards\n\n"
        "TGCHAN NEWS  https://t.me/tgach_bot\n"
        "TGCHAN archive https://t.me/tgach_archive\n"
    )
]

# --- Варианты для рассылки списка досок ---

BOARD_LIST_HEADERS_RU = [
    "🌐 <b>Все доски ТГАЧА:</b>",
    "🗂️ <b>Наши доски (секции):</b>",
    "📌 <b>Список досок, выбирай куда срать:</b>",
    "📋 <b>Навигация по доскам:</b>"
]

BOARD_LIST_HEADERS_EN = [
    "🌐 <b>All TGACH boards:</b>",
    "🗂️ <b>Our boards (sections):</b>",
    "📌 <b>List of boards, choose where to shitpost:</b>",
    "📋 <b>Board navigation:</b>"
]

def generate_boards_list(board_configs: dict, lang: str = 'ru') -> str:
    """
    Генерирует форматированный HTML-список досок из конфига со случайным заголовком.
    """
    if lang == 'en':
        header = random.choice(BOARD_LIST_HEADERS_EN)
        board_lines = [
            f"<b>{config['name']}</b> {config['description_en']} - {config['username']}"
            for b_id, config in board_configs.items() if b_id != 'test'
        ]
    else:
        header = random.choice(BOARD_LIST_HEADERS_RU)
        board_lines = [
            f"<b>{config['name']}</b> {config['description']} - {config['username']}"
            for b_id, config in board_configs.items() if b_id != 'test'
        ]
    
    return f"{header}\n" + "\n".join(board_lines)


# --- Варианты для рекламной рассылки тредов ---

THREAD_PROMO_TEXT_RU = [
    (
        "<b>Надоели срачи и хаос в общем чате?</b>\n\n"
        "Создай свой собственный тред на доске /thread/ -> @thread_chatbot\n\n"
        "Приглашай друзей, обсуждай свои темы, бань неугодных. "
        "Полный контроль в твоих руках. Начни с команды /create"
    ),
    (
        "<b>Хочешь лампового общения без лишних глаз?</b>\n\n"
        "Переходи на доску /thread/ (@thread_chatbot) и создавай свою комнату.\n\n"
        "Это как своя конфа, только с анонами и без регистрации. Идеально для обсуждения хентая или твоих девиаций."
    ),
    (
        "<b>Этот чат — помойка. Но есть решение.</b>\n\n"
        "Доска /thread/ (@thread_chatbot) позволяет создавать временные треды на любую тему.\n\n"
        "Собери анонов и обсуждай то, что интересно вам, а не этим дегенератам из общего чата. Стань ОПом, почувствуй власть."
    ),
    (
        "<b>Абу разрешает тебе стать ОПом!</b>\n\n"
        "На доске /thread/ (@thread_chatbot) ты можешь создать свой маленький тредик и мутить там всех пидорасов, кто тебе не нравится.\n\n"
        "Просто напиши /create и стань хозяином своего маленького чата."
    )
]

THREAD_PROMO_TEXT_EN = [
    (
        "<b>Tired of the chaos in the main chat?</b>\n\n"
        "Create your own cozy thread on the /thread/ board -> @thread_chatbot\n\n"
        "Invite friends, discuss your topics, ban annoying users. "
        "Full control is in your hands. Start with the /create command."
    ),
    (
        "<b>Want a comfy discussion without randoms?</b>\n\n"
        "Go to the /thread/ board (@thread_chatbot) and create your own room.\n\n"
        "It's like your own private group, but with anons and no registration. Perfect for discussing hentai or your deviations."
    ),
    (
        "<b>This chat is a dumpster fire. But there is a solution.</b>\n\n"
        "The /thread/ board (@thread_chatbot) allows you to create temporary threads on any topic.\n\n"
        "Gather anons and discuss what's interesting to you, not these degenerates from the main chat. Become an OP, feel the power."
    )
]
