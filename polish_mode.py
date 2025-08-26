import random
import re

POLISH_PHRASES_START = [
    "🇵🇱 UWAGA, UWAGA! AKTYWOWANO TRYB POLSKI! 🇵🇱\n\nO, kurwa! Czas na pierogi, bigos i narzekanie na wszystko! POLSKA GUROM!",
    "O kurwa, jebany! Włączono 'Protokół Bóbr'! 🇵🇱 Czas rozkurwić system!",
    "BOSZE, CO ZA INBA! 🇵🇱 Wjeżdżamy z trybem polskim! Kto nie skacze, ten z PiS-u!",
    "Alarm! Poziom 'Polskości' w czacie przekroczył normę! WITAMY W POLSCE, KURWA!",
    "ROZPOCZYNAMY 'OPERACJĘ HUSARZ'! 🇵🇱 Skrzydła rozłożone, pierogi podgrzane. Do boju!",
    "Co jest, kurwa?! 🇵🇱 Tryb 'Janusz' aktywowany! Skarpetki do sandałów, siatka z Biedronki i jedziemy!",
    "No i chuj, no i cześć! Włączono tryb 'Polska Myśl Szkoleniowa'! Jakoś to będzie, kurwa!",
]

POLISH_PHRASES_END = [
    "Dobra, kurwa, wystarczy tego polskiego pierdolenia. 🇵🇱 Wracamy do normalności, bo zaraz nas PiS opodatkuje.",
    "Koniec inby. 🇵🇱 Bóbr poszedł spać. Tryb polski wyłączony, można znowu mówić po ludzku.",
    "Boże, jak mi wstyd... Wyłączamy ten tryb, zanim ktoś wezwie TVP Info.",
    "Wódka się skończyła, pierogi zjedzone. 🇵🇱 Polski cud gospodarczy dobiegł końca. Wyłączam tryb.",
    "Dobra, fajrant. Czas na przerwę od bycia Polakiem. To męczące, kurwa.",
]

POLISH_DATA = {
    'prefixes': [
        "Słuchajcie, kurwa...", "Generalnie to jest tak:", "No więc, ja pierdolę,", "Patrzcie, co za akcja:",
    ],
    'suffixes': [
        ", nie?", ", i chuj.", ", wiadomo.", ", tak to już jest.", ", masakra.", ", rozumiesz.", ". I tyle w temacie.", ", essa.",
    ],
    'injections': [
        "(ja pierdolę)", "(nosz kurwa)", "(co za żenada)", "(takie życie)", "(polska, rozumiesz)", "(bez kitu)",
    ],
    'replacements': {
        'привет': 'siema', 'пока': 'nara', 'да': 'no', 'нет': 'nie', 'что': 'co', 'хорошо': 'dobrze', 'плохо': 'chujowo',
        'деньги': 'pieniądze', 'работа': 'robota', 'пожалуйста': 'proszę', 'спасибо': 'dzięki', 'пиздец': 'ja pierdolę',
        'сука': 'kurwa', 'блять': 'kurwa mać', 'заебись': 'zajebiście', 'человек': 'gościu'
    }
}


def kurwa_comma_transform(text: str) -> str:
    """
    С вероятностью 50% заменяет каждую запятую на ", kurwa,".
    """
    if random.random() < 0.5:
        return text
    
    # Используем функцию в re.sub для вероятностной замены
    def replacer(match):
        return ", kurwa," if random.random() < 0.5 else ","

    return re.sub(r',', replacer, text)


def polish_transform(text: str) -> str:
    """
    Трансформирует текст, добавляя в него польский колорит, используя структурированный подход.
    """
    if not text:
        return ""
    
    # 1. Замена слов
    for rus, pol in POLISH_DATA['replacements'].items():
        text = re.sub(r'\b' + re.escape(rus) + r'\b', pol, text, flags=re.IGNORECASE)

    # 2. Трансформация "kurwa-запятая"
    text = kurwa_comma_transform(text)
    
    # 3. Выбор основного метода трансформации
    method = random.choice(['prefix', 'suffix', 'injection', 'suffix']) # суффикс дважды для большей вероятности
    
    transformed = False
    if method == 'prefix':
        text = f"{random.choice(POLISH_DATA['prefixes'])} {text}"
        transformed = True
    elif method == 'suffix':
        text = f"{text}{random.choice(POLISH_DATA['suffixes'])}"
        transformed = True
    elif method == 'injection':
        words = text.split()
        if len(words) > 2:
            injection_point = random.randint(1, len(words) - 1)
            words.insert(injection_point, random.choice(POLISH_DATA['injections']))
            text = ' '.join(words)
            transformed = True

    # 4. Гарантируем, что хоть какая-то трансформация была, если не сработали основные
    if not transformed:
        text += f", {random.choice(['kurwa', 'nie?', 'wiadomo'])}."
        
    return text
