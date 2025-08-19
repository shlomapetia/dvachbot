import random
from typing import Tuple
from aiogram.types import Message
import asyncio


# Для /deanon
DEANON_NAMES = ["Валера", "Геннадий", "Дмитрий", "Аркадий", "Николай", "Женя", "Чмоня", "Арестарх", 
                "Сергей", "Александр", "Владимир", "Борис", "Евгений", "Михаил", "Хуйло", "Вазген", "Нариман", "Абу", 
                "Олег", "Павел", "Константин", "Виктор", "Юрий", "Тимофей", "Глеб", "Роман", "Эдик", "Гена", 
                "Андрей", "Иван", "Данил", "Саня", "Лёша", "Коля", "Ваня", "Петя", "Саша", "Миша",
                "Матвей", "Руслан", "Артем", "Илья", "Денис", "Егор", "Максим", "Кирилл", 
                "Тимур", "Артём", "Даниил", "Ахмет", "Бахтияр", "Ашот", "Боря", "Славик", "Славка", "Антоша", "Маня", "Чмоня", "Анонимус", "Вениамин", "Тимурка"]
DEANON_SURNAMES = ["Андреев", "Борисов", "Васильев", "Григорьев", "Дмитриев", "Егоров", "Туалетов", "Скуфьин",
                   "Захаров", "Иванов", "Константинов", "Леонидов", "Михайлов", "Николаев",
                   "Пукин", "Орлов", "Петров", "Романов", "Смирнов", "Титов", "Ульянов", "Федоров",
                   "Харитонов", "Царев", "Чернов", "Шапошников", "Анальный", "Говноедин", "Пидорашкин", "Пидорас",
                   "Курбатов", "Ерохин", "Сычев", "Болонкин", "Очкошин", "Членососов", "Опущенный", "Говноёбов",
                   "Хуйкин", "Чехов", "Шевцов", "Щербаков", "Юрьев", "Яковлев", "Яшин", "Пиздюков",
                   "Пидарасов", "Пидоров", "Пидоровский", "Ленковец", "Нагибалов", "Навальный",
                   "Гитлер", "Хуйланский", "Жейков", "Филатов", "Кукушин", "Перов", "Козлов", "Соболев",
                   "Петухов", "Хуев", "Дрочилов", "Пидарасов", "Мудаков", "Говнюков", "Елхов", "Елдов", "керенский",
                   "Залупин", "Мудозвонов", "Херович", "Песков", "Ананасов", "Шурыгин", "Шизанутов", "Кончалов",
                   "Минетов", "Спермов", "Членов", "Вагин", "Сосунков", "Педиков", "Гомиков", "Аналов"]
DEANON_CITIES = ["Магнитогорск", "Челябинск", "Тюмень", "Уфа", "Омск", "Кемерово",
                 "Братск", "Норильск", "Воркута", "Ухта", "Сызрань", "Саранск", "Тольятти", "Нижний Тагил", "Череповец",
                 "Липецк", "Тольятти", "Набережные Челны", "Магадан", "Петропавловск-Камчатский",
                 "Новокузнецк", "Красноярск", "Иркутск", "Кемерово", "Новосибирск", "Красноярск",
                 "Ижевск", "Сургут", "Сыктывкар", "Вологда", "Владивосток", "Москва", "Самара", 
                 "Саратов", "Казань", "Пенза", "Киев", "Минск", "Днiпро", "Грозный", "Хабаровск", "Чапаевск",
                 "Нефтекамск", "Ижевск", "Москва", "Москва", "Санкт-Петербург", "Санкт-Петербург",
                 "Навашино", "Село Пердяевка", "Ангарск", "Астана", "Уральск", "Каменск-Шахтинский", "Харькiв", "Киев", "Днiпро"]
DEANON_STREETS = [
    # 50 обычных улиц
    "Ленина", "Советская", "Мира", "Центральная", "Молодежная", 
    "Школьная", "Садовская", "Лесная", "Новая", "Набережная",
    "Зеленая", "Парковая", "Солнечная", "Речная", "Спортивная",
    "Юбилейная", "Гагарина", "Пушкина", "Кирова", "Строителей",
    "Заводская", "Колхозная", "Рабочая", "Береговая", "Полевая",
    "Северная", "Южная", "Восточная", "Западная", "Садовая",
    "Вишневая", "Яблоневая", "Липовая", "Кленовая", "Октябрьская",
    "Первомайская", "Космонавтов", "Горького", "Чкалова", "Фрунзе",
    "Чапаева", "Суворова", "Кутузова", "Железнодорожная", "Вокзальная",
    "Станционная", "Профсоюзная", "Клубная", "Пионерская", "Комсомольская",
    # 50 смешных улиц
    "Хуесосов", "Адольфа Гитлера", "Туалетная", "Пидорская", 
    "Пьяных Трактористов", "Пупкина", "Унитазная", "Говнярка", "Гомосексуалистов", "Анальных Исследований",
    "Пукинская", "Заднеприводных", "Высиранов СВО", "Анусная",
    "Долбоёбов", "Говноедов", "Пидорасов", "Гейская", "Обосранная", 
    "Бомжацкая", "Шлюхина", "Гондонов", "им. Холокоста", "Двачевская",
]
DEANON_PROFESSIONS = ["сантехник", "грузчик", "охранник", "менеджер по продажам", "стоматолог", "психически больной", "ворует пенсию бабушки", "ворует металл", "тестировщик резиновых хуёв",
                      "электрик", "безработный", "дворник", "алкаш", "наркодилер", "в колл центре работает", "подметает дворы", "высиран сво", "является инвалидом",
                      "вор в законе", "охотник на педофилов", "разнорабочий", "хиккан", "РНН ГОСПОДИН", "свошник", "штурмовик Z", "Бо Синн", "свошник",
                      "грузчик", "разработчик ПО", "уборщик сортиров", "вор в пятерочке", "торговец героином", "разработчик", "штаб навального", "сборщик пасскодов на Дваче",
                      "смотритель помойки", "сборщик бутылок", "попрошайка", "сутенер", "зоофил", "проститутка", "админ бота", "модератор", "модератор двача",
                      "психолог", "психиатр", "врач", "врач-нарколог", "врач-сексолог", "врач-терапевт", "врач-хирург", "личная дырка Абу", "оператор ЦП", "торговец вейпами",
                      "гей шлюха", "трансгендер", "аниматор", "диджей", "бармен", "бармен-пидор", "бариста", "анимешник", "художник"]
DEANON_FETISHES = ["ножки школьниц", "трусики бабушек", "Школьницы", "Еврейское порно", "Егор Летов", "испражнения в банке", 
                   "трупы голубей", "просроченный майонез", "порно 80-х", "анимешные девочки", "классический секс", "держание за руки", "обнимашки",
                   "запах говна", "гнойные прыщи", "фекалии", "Янка Дягилева и сибпанк", "немецкие видео с изнасилованиями",
                   "использованные тампоны", "аутофелляция", "студентки", "изнасилования", "секс с овцами", "засохшая сперма", "фистинг", "украинское цп", "межрасовое", "межвидовое",
                   "зоофилия", "негры", "девочки", "мамки", "детское порно", "боллбастинг", "пожилые", "смешные картинки",
                   "анальный секс", "классический секс", "мигранты", "азиаты", "евреи", "афроамериканцы", "нюдсы Абу",
                   "латиноамериканцы", "индийцы", "китайцы", "японцы", "корейцы", "школьницы на коленках", "моногатари", 
                   "хентай", "фурри", "негры", "девочки", "мамки", "детское порно", "боллбастинг", "шотакон", "лоликон", "Сенгоку Надеко",
                   "пожилые", "анальный секс", "классический", "уринация", "бдсм", "свинг", "соло", "самоотсос",
                   "групп секс", "оргизм", "минет", "фелацио", "кунилингус", "анальный секс", "оргазм"]
DEANON_DETAILS = [
    "скрывает криминальное прошлое", "сосет у работодателя", "мочится в раковину",
    "ебется с детьми", "боится темноты", "коллекционирует дилдаки", "унизили на дебатах в универе", 
    "имеет 5 судимостей", "просрочил паспорт", "не моется 2 недели", "отфрендзонен шлюхой",
    "ворует в Пятерочке", "пьет одеколон", "снимает квартиру у педофила", "любит есть собственную сперму",
    "спит на помойке", "мечтает стать хохлом", "боится женщин", "ему нравится Александр Гельевич Дугин",
    "мастурбирует на советские мультики", "носит трусы сестры", "донатил навальному", 
    "платит за секс с бабушками", "покупает поддельные кроссовки", "донатил высиранам сво", 
    "участвует в собачьих боях", "пьет мочу из банки", "отсосал школьному хулигану в 7 классе",
    "обоссался в метро", "пидор", "донатил в казино", "вытирает жопу пальцами", "лизал очко однокласснику", "боится срать в гостях",
    "сидит на бутылке", "сын шлюхи", "абсолютно здоровый человек", "добрый. хороший человек так-то", 
    "не против кому-нибудь отсосать", "лежал в дурке полгода", "пьет антидепрессанты", "его дед был Украинцем",
    "разработчик этого бота", "очень любит смотреть аниме", "является конфоблядью", "участник сво", 
    "инвалид по дурке", "член 10 см", "добрый, приятный человек", "стыдистя своего лица", "стыдится своего голоса",
    "мечтает изнасиловать школьницу", "ему понравились моногатари", "любит анальную мастурбацию",
    "латентный пидор", "его отец был чуркой", "он просто забитый чмошник",
    "кормит кота виагрой", "его мать изнасиловали таджики когда она шла домой",
    "продал почку за коллекцию фигурок аниме",
    "Смотрел Ре:Зеро и ему понравилось", "нюхает пальцы после того как чешет себе промежность",
    "спит в гробу бабушки", "нюхал трусы своей бабушки",
    "недавно вновь обоссался под себя во сне", "не бреется из прицнипа",
    "пользуется нейросетями", "облысел в 22 года", "балуется время от времени наркотиками",
    "живёт обычной жизнью, ничего примечательного",
    "подрабатывает в цирке уродов", "неиронично смотрит аниме",
    "заперт в подвале мамой-алкашкой", "пытался облапать собственную мать",
    "собрал 500 гигабайт фурри-порно", "трахался с отчимом",
    "считает что земля плоская", "Прозревший",
    "тратит зарплату на проституток", "настоящий аутист",
    "тратит зарплату на аниме-мерч", "позор своей семьи, все его ненавидят",
    "лудоман", "судимый", "сидел в тюрьме 4 года", "донатил Поднебесному", 
    "сидит в магаче",
    "имеет суицидальные наклонности",
    "пару раз в месяц занимается анальной мастурбацией",
    "до сих пор боится Больших украинских дядь 185/90",
    "попал в Книгу рекордов Гиннесса по количеству съеденного говна",
    "женился на подушке с принтом Ту Хао",
    "снимается в порно под ником 'Мокрый хлебушек'",
    "выращивает грибы",
    "сжег свою мать в 15 лет ради лулзов",
    "подрабатывает в Макдональдсе",
    "пробовал собственную сперму",
    "какой-то период своей жизни был бомжом",
    "купил собачий фаллоимитатор",
    "работает в Лахта-Центре",
    "съел свою плаценту при рождении",
    "облысевшее уёбище",
    "стыдится своего отражения",
    "СТОИТ НА УЧЁТЕ В ПНД",
    "живёт в обосанной хрущёвке",
    "после перекрута яичка одно яйцо ему отрезали",
    "зашёл на двач впервые в 2023 году",
    "травили на дваче неоднократно",
    "он занимался сексом с двумя мужчинами одновременно",
    "пытался увеличить свой член",
    "его ненавидят все его знакомые",
    "у него нет настоящих друзей",
    "его родители стыдятся иметь такого сына",
    "подвёл и разочаровал много хороших людей",
    "пытался самоутвердится на дваче",
    "у него недостаток женского внимания",
    "он попал в психушку после видения",
    "он считает себя реинкарнацией Тесака",
    "он пробовал секс с пылесосом",
    "трахает арбузы с дыркой",
    "выиграл конкурс по поеданию стекла",
    "выиграл конкурс по поеданию говна",
    "он плавает в бассейне с собственной спермой",
    "этот человек бреет анус и яйца",
    "он собирает пердеж в баночки",
    "считает нормой дрочить на цп",
    "пытался получить модерку на дваче",
    "он курит сушеные грибы",
    "он живет в палатке на балконе",
    "он трогает себя, пока никто не видит"
]

# --- ENGLISH LISTS ---
DEANON_NAMES_EN = ["John", "Michael", "David", "Chris", "Mike", "James", "Robert", "William", "Richard", "Eugene", "Walter", "Chad", "Kyle", "Brandon", "Kevin", "Scott", "Peter", "Anonymous", "Abu", "Tyrone", "Jamal", "Bob", "Rick", "Morty", "Homer", "Bartholomew", "Arthur"]
DEANON_SURNAMES_EN = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Miller", "Davis", "Wilson", "Anderson", "Taylor", "Moore", "Jackson", "White", "Harris", "Martin", "Thompson", "Garcia", "Martinez", "Robinson", "Clark", "Rodriguez", "Lewis", "Lee", "Walker", "Hall", "Allen", "Young", "King", "Wright", "Hill", "Scott", "Green", "Adams", "Baker", "Faggot", "Cuckson", "Biden", "Trump", "Freeman", "Doe"]
DEANON_CITIES_EN = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose", "Austin", "Jacksonville", "London", "Birmingham", "Manchester", "Glasgow", "Liverpool", "Bristol", "Sheffield", "Detroit", "Ohio", "Mumbai", "Beijing", "Berlin", "Gary, Indiana", "Compton", "Silent Hill", "Racoon City"]
DEANON_STREETS_EN = ["Main St", "Elm St", "High St", "Washington St", "Park Ave", "Oak Ave", "Maple St", "2nd St", "3rd St", "Church St", "Broad St", "Center St", "Fake St", "Faggot Alley", "Cuckold Drive", "Retard Rd", "Loser Lane", "Gaylord Ave", "4chan Blvd", "Anon Way"]
DEANON_PROFESSIONS_EN = ["plumber", "loader", "security guard", "sales manager", "dentist", "mentally ill", "steals grandma's pension", "steals metal", "rubber dildo tester", "electrician", "unemployed", "janitor", "alcoholic", "drug dealer", "call center operator", "fast-food worker", "war veteran", "disabled person", "thief", "pedophile hunter", "handyman", "hikikomori", "NEET", "software developer", "toilet cleaner", "shoplifter at Walmart", "heroin dealer", "Discord mod", "dumpster diver", "bottle collector", "beggar", "pimp", "zoophile", "prostitute", "bot admin", "moderator", "4chan mod", "personal hole of Abu", "CP operator", "vape seller", "gay prostitute", "transgender", "animator", "DJ", "bartender", "faggot-bartender", "barista", "weeb", "artist", "YouTuber"]
DEANON_FETISHES_EN = ["schoolgirls' feet", "grandmas' panties", "schoolgirls", "scat", "dead pigeons", "expired mayonnaise", "80s porn", "anime girls", "hand-holding", "hugging", "smell of shit", "pustules", "feces", "used tampons", "autofellatio", "college girls", "rape", "sex with sheep", "dried cum", "fisting", "interracial", "interspecies", "zoophilia", "black guys", "girls", "moms", "child porn", "ballbusting", "elderly", "funny pictures", "anal sex", "migrants", "asians", "jews", "african-americans", "Abu's nudes", "hentai", "furry", "shotacon", "lolicon", "watersports", "BDSM", "cuckoldry", "pegging"]
DEANON_DETAILS_EN = [
    "hides a criminal past", "sucks off his boss", "pees in the sink", 
    "is afraid of the dark", "collects dildos", "has 5 criminal records", 
    "has an expired passport", "hasn't showered in 2 weeks", "got friendzoned by a whore", 
    "shoplifts at Walmart", "drinks cologne", "rents an apartment from a pedophile", 
    "likes to eat his own cum", "sleeps at a dumpster", "dreams of becoming Ukrainian", 
    "is afraid of women", "masturbates to old cartoons", "wears his sister's panties", 
    "donated to a controversial political figure", "pays for sex with grandmas", "buys fake sneakers", 
    "participates in dog fights", "drinks piss from a jar", "gave a blowjob to a school bully in 7th grade", 
    "pissed himself on the subway", "is a faggot", "is a gambling addict", 
    "wipes his ass with his fingers", "licked a classmate's asshole", "is afraid to take a shit at a friend's house",
    "sits on a bottle", "son of a whore", "is a completely healthy person", 
    "is a kind, good person actually", "wouldn't mind sucking someone off", "spent half a year in a mental hospital", 
    "takes antidepressants", "his grandpa was Ukrainian", "is the developer of this bot", 
    "loves watching anime", "is a drama queen", "is a war veteran", 
    "is mentally disabled", "has a 4-inch dick", "is a kind, pleasant person", 
    "is ashamed of his face", "is ashamed of his voice", "dreams of raping a schoolgirl", 
    "is a latent faggot", "his father was a foreigner", "is just a beaten-down loser",
    "feeds his cat viagra", "his mother was raped by migrants on her way home",
    "sold a kidney for an anime figure collection", "sniffs his fingers after scratching his crotch",
    "sleeps in his grandmother's coffin", "sniffed his grandmother's panties",
    "recently wet the bed again", "uses neural networks", 
    "went bald at 22", "dabbles in drugs from time to time", "lives an ordinary life, nothing remarkable",
    "works in a freak show", "unironically watches anime",
    "is locked in the basement by his alcoholic mother", "tried to grope his own mother",
    "has collected 500 gigabytes of furry porn", "had sex with his stepfather",
    "believes the Earth is flat", "is an enlightened one",
    "spends his salary on prostitutes", "is a real autist",
    "spends his salary on anime merch", "is a disgrace to his family, everyone hates him",
    "is a gambling addict", "has a criminal record", "spent 4 years in prison", 
    "has suicidal tendencies", "is registered at a psychiatric clinic",
    "lives in a piss-stained apartment building", "had one testicle removed after a torsion",
    "visited 4chan for the first time in 2023"
]

def generate_deanon_info(lang: str = 'ru') -> str:
    """
    Генерирует фейковые данные для деанона на указанном языке
    и возвращает единую, отформатрованную строку в случайном стиле с высокой вариативностью.
    """
    
    # --- БЛОК ГЕНЕРАЦИИ ДАННЫХ (без изменений) ---
    if lang == 'en':
        name = random.choice(DEANON_NAMES_EN)
        surname = random.choice(DEANON_SURNAMES_EN)
        city = random.choice(DEANON_CITIES_EN)
        street = random.choice(DEANON_STREETS_EN)
        prof = random.choice(DEANON_PROFESSIONS_EN)
        fetish = random.choice(DEANON_FETISHES_EN)
        details_list = DEANON_DETAILS_EN
        address = f"{city}, {street}, {random.randint(1, 200)}, apt. {random.randint(1, 500)}"
    else:
        name = random.choice(DEANON_NAMES)
        surname = random.choice(DEANON_SURNAMES)
        city = random.choice(DEANON_CITIES)
        street = random.choice(DEANON_STREETS)
        prof = random.choice(DEANON_PROFESSIONS)
        fetish = random.choice(DEANON_FETISHES)
        details_list = DEANON_DETAILS
        address = f"{city}, ул. {street}, д. {random.randint(1, 200)}, кв. {random.randint(1, 500)}"

    details = random.sample(details_list, k=min(len(details_list), random.randint(1, 3)))
    details_str = ", ".join(details)

    if lang == 'en':
        return (
            f"<b>[DEANONYMIZATION REPORT]</b>\n\n"
            f"<b>Subject Name:</b> {name} {surname}\n"
            f"<b>Last Known Address:</b> {address}\n"
            f"<b>Probable Occupation:</b> {prof}\n"
            f"<b>Psychological Profile Note:</b> Fixation on {fetish}\n\n"
            f"<b>Field Agent's Notes:</b> Subject is known to be {details_str}."
        )

    styles = [
        'standard', 'fsb', 'ukrainian', 'chechen', 'official', 'schizo',
        'news', 'old_school_hacker', 'psych_eval', 'hitman', 'dating_app',
        'housing_report', 'cultist'
    ]
    chosen_style = random.choice(styles)

    if chosen_style == 'housing_report':
        complaints = [
            "шумит по ночам, предположительно слушает Стаса Михайлова",
            "постоянно заливает соседей снизу, утверждает, что это 'божья роса'",
            "захламил балкон старыми покрышками и пустыми бутылками",
            "разводит на лестничной клетке голубей",
            "дрочит в подъезде",
            "заебал уже весь дом этот гандон",
            "систематически ссыт и пердит в лифте",
            "разговаривает с мусоропроводом"
        ]
        resolution = random.choice(["Провести разъяснительную беседу.", "Выписать штраф.", "Игнорировать до следующего заявления.", "Поставить на учёт."])
        report_parts = [
            f"<b>СПРАВКА</b>",
            f"Выдана гр. {surname} {name} о том, что он(а) действительно проживает по адресу: {address}.",
            f"Задолженности по коммунальным платежам {random.choice(['не имеет', 'имеет в размере ' + str(random.randint(1000, 25000)) + ' руб.'])}.",
            f"Со стороны соседей поступали жалобы: {random.choice(complaints)}.",
            f"\n<b>Заключение участкового:</b> {resolution}"
        ]
        return "\n".join(report_parts)

    elif chosen_style == 'cultist':
        parts = [
            "<i>Имя сосуда:</i>", f"<code>{name.upper()} {surname.upper()}</code>",
            "\n<i>Знак начертан по адресу:</i>", f"<code>{city}, {street.upper()}</code>",
            "\n<i>Его слабость, ключ к его душе:</i>", f"<code>{fetish.upper()}</code>",
            "\n<i>Пророчество гласит:</i>", f"<code>Он тот, кто {details_str.upper()}. Звезды укажут час, когда он будет готов.</code>",
            "\n<b>ОН БУДЕТ НАШИМ.</b>"
        ]
        return "\n".join(parts)

    elif chosen_style == 'psych_eval':
        f_code = f"F{random.randint(20, 69)}.{random.randint(0, 9)}"
        diag = random.choice(["Острое полиморфное психотическое расстройство", "Гебефреническая шизофрения", "Шизоаффективное расстройство", "Органическое бредовое расстройство", "Диссоциативное расстройство идентичности", "Расстройства аутистического спектра", "Гендерная дисфорсия", "Генерализованное Тревожное расстройство"])
        conclusion = random.choice(["Рекомендована недобровольная госпитализация.", "Расстрйоства сексуального спектра", "У пациента недержание кала", "Представляет опасность для себя и окружающих.", "Рекмоендована лоботомия", "Направить на принудительное лечение по месту жительства.", "Отправить на лечение в логерь Чёрный Дельфин", "Ярко выражена негативная симптоматика", "Пациент симулирует, но на всякий случай изолировать."])
        report_parts = [
            f"<b>[ИСТОРИЯ БОЛЕЗНИ №{random.randint(1000, 9999)}]</b>",
            f"<b>Пациент:</b> {surname} {name}",
            f"<b>Диагноз:</b> {f_code} ({diag})",
            f"<b>Анамнез:</b> Со слов пациента, он {details_str}. Отмечается навязчивая фиксация на теме '{fetish}'.",
            f"<b>Наблюдение:</b> {random.choice(['Бредовые идеи величия.', 'Агрессивен, вербально и невербально.', 'Вялый, апатичный, отказывается от еды.', 'Пытался построить из хлеба портал в другое измерение.'])}",
            f"<b>Заключение:</b> {conclusion}"
        ]
        return "\n".join(report_parts)

    elif chosen_style == 'hitman':
        methods = ["стандартный пакет", "несчастный случай", "утилизировать", "тихо, без свидетелей", "показательно, в назидание другим"]
        text = (
            f"//-- ЗАКАЗ #{secrets.token_hex(3).upper()} --//\n"
            f"ЦЕЛЬ: {surname} {name}\n"
            f"МЕСТОПОЛОЖЕНИЕ: {address}\n"
            f"ПРОФИЛЬ: Работает ({prof}). Уязвим через интерес к '{fetish}'.\n"
            f"ЗАМЕТКА: Цель {details_str}. Следует соблюдать осторожность.\n"
            f"ИСПОЛНЕНИЕ: {random.choice(methods)}.\n"
            f"СТАТУС: {random.choice(['Принято в работу.', 'Ожидает предоплаты.', 'Исполнитель назначен.'])}"
        )
        return f"<code>{escape_html(text)}</code>"

    elif chosen_style == 'dating_app':
        age = random.randint(18, 45)
        parts = [
            f"❤️ <b>{name}, {age}</b>",
            f"📍 {city}",
            f"💼 {prof}",
            f"\n💬 О себе: {details_str}. Ищу человека, который разделит мою непреодолимую тягу к {fetish}.",
            f"\n❌ <i>{random.choice(['Не пиши, если у тебя нет айфона.', 'Свайпай влево, если ищешь только серьезных отношений.', 'Только для реальных встреч, общение не интересует.', 'Рост 180+'])}</i>"
        ]
        return "\n".join(parts)
        
    # --- НАЧАЛО ИЗМЕНЕНИЙ: УЛУЧШЕНИЕ УКРАИНСКОГО СТИЛЯ ---
    elif chosen_style == 'ukrainian':
        headers = [
            "<b>[ДАНІ ПРО СЕПАРАТИСТА]</b> 🫡",
            "<b>[ОСОБОВА СПРАВА КОЛАБОРАНТА]</b> 📝",
            "<b>[РУСЬКIЙ СЕПАР]</b> 📝",
            "<b>[ПАШПОРТ ЛУГАНДОНЦА]</b> 📝",
            "<b>[ІНФОРМАЦІЯ З БАЗИ \"МИРОТВОРЕЦЬ\"]</b>  Datenbank",
            "<b>[ЗВІТ СБУ]</b> 🇺🇦"
        ]
        
        name_intros = [
            f"<b>Об'єкт:</b> {name} {surname}, позивний '{random.choice(['Пиня', 'Чмоня', 'Дырка','Лахтобот', 'Ванька', 'Хуiло', 'Пiдоран'])}'.",
            f"<b>ПІБ:</b> {surname} {name}, клятий москаль.",
            f"<b>Чоловiк:</b> {surname} {name}, ёбанiй ватник.",
            f"<b>Ідентифіковано особу:</b> {name} {surname}, зрадник України."
        ]
        
        address_intros = [
            f"<b>Місцезнаходження:</b> {address}, тимчасово окупована територія.",
            f"<b>Зафіксовано за адресою:</b> {address}.",
            f"<b>Ймовірне місце дислокації:</b> {address}."
        ]

        prof_intros = [
            f"<b>Діяльність:</b> {prof}, пособник окупантів.",
            f"<b>Діяльність:</b> {prof}, зрадник України.",
            f"<b>Прикриття:</b> працює як {prof}, але насправді веде підривну діяльність.",
            f"<b>Заняття:</b> {prof} (інформація перевіряється)."
        ]

        details_intros = [
            f"<b>Що відомо:</b> {details_str}.",
            f"<b>Особливі прикмети:</b> {details_str}.",
            f"<b>Агентурні дані:</b> {details_str}."
        ]
        
        fetish_intros = [
            f"<b>Збочення:</b> {fetish}.",
            f"<b>Морально-психологічний стан:</b> схильний до {fetish}.",
            f"<b>Слабкість:</b> {fetish}."
        ]

        conclusions = ['Слава Україні!', 'Смерть ворогам!', 'Азов - Сила!', 'Передати дані в ГУР.', 'На підвал, пiдора.', 'Буде перемога', 'Запакувати та доставити до найближчого ТЦК.']

        report_parts = [random.choice(headers)]
        report_parts.append(random.choice(name_intros))

        # Случайное добавление полей для вариативности
        if random.random() < 0.9:
            report_parts.append(random.choice(address_intros))
        if random.random() < 0.8:
            report_parts.append(random.choice(prof_intros))
        if random.random() < 0.95:
            report_parts.append(random.choice(details_intros))
        if random.random() < 0.75:
            report_parts.append(random.choice(fetish_intros))

        report_parts.append(f"\n<i>{random.choice(conclusions)}</i> 🇺🇦")
        return "\n".join(report_parts)
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---
    
    elif chosen_style == 'schizo':
        schizo_phrases = [
            "ЭТО ОН", "ОНИ СЛЕДЯТ ЗА НИМ ЧЕРЕЗ РОЗЕТКИ", f"ЕГО НАСТОЯЩЕЕ ИМЯ {name.upper()}",
            "ОН ПРЯЧЕТ ЧИП В ЗУБЕ", f"ГОРОД {city.upper()} ЭТО ИХ БАЗА", "ИЗЛУЧАТЕЛИ РАБОТАЮТ",
            f"ОН {details_str.upper()}", "Я ВИДЕЛ ЕГО ГЛАЗА ОНИ СВЕТЯТСЯ", "ПЯТЬ ДЖИ",
            "РЕПТИЛОИДЫ УЖЕ ЗДЕСЬ", f"ФЕТИШ ({fetish.upper()}) ЭТО КЛЮЧ", "НЕ СЛУШАЙТЕ ЕГО ОН ЛЖЕТ",
            "КРАСНАЯ РТУТЬ", "ТАБЛЕТКИ НЕ ПОМОГАЮТ", "СИГНАЛ ИДЕТ ИЗНУТРИ"
        ]
        num_lines = random.randint(4, 7)
        selected_lines = random.sample(schizo_phrases, k=min(len(schizo_phrases), num_lines))
        return "<code>" + "\n".join(selected_lines) + "</code>"

    else:
        return random.choice([
            f"👤 <b>Имя:</b> {name} {surname}\n🏠 <b>Адрес:</b> {address}\n💼 <b>Профессия:</b> {prof}\n❤️‍🔥 <b>Фетиш:</b> {fetish}\n📝 <b>Дополнительно:</b> {details_str}",
            f"<b>[ДОСЬЕ ОБЪЕКТА №{random.randint(1000, 9999)}]</b>\n<b>ФИО:</b> {surname} {name}\n<b>Место регистрации:</b> {address}\n<b>Род деятельности:</b> {prof}\n<b>Особые приметы:</b> {details_str}\n<i>Заключение: {random.choice(['Объект нестабилен.', 'Рекомендовано наблюдение.'])}</i>",
            f"Ассаламу алейкум. Этот шайтан, {name} {surname}, живет тут: {address}. Говорят, ему нравится {fetish}. Аллах ему судья. <b>Ахмат - Сила!</b>",
            f"<b>АНКЕТА №{random.randint(100, 999)}-П</b>\n<b>Фамилия, Имя:</b> {surname}, {name}\n<b>Адрес проживания:</b> {address}\n<b>Характеристика:</b> {details_str}",
            f"⚡️ <b>СРОЧНЫЕ НОВОСТИ</b> ⚡️\n\nСтали известны подробности о жителе города {city}, гражданине {surname} {name}. По данным нашего источника, он известен тем, что {details_str}. Мы следим за развитием событий.",
            f"<code>//-- Deanon protocol --//\n> Name: \"{name} {surname}\"\n> Location: \"{address}\"\n> Exploit: \"{fetish}\"\n> Profile dump: {details_str}\n//-- Terminated --//</code>"
        ])
