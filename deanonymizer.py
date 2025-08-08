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

def generate_deanon_info(lang: str = 'ru') -> Tuple[str, str, str, str, str, str]:
    """Генерирует фейковые данные для деанона на указанном языке."""
    
    # --- БЛОК ВЫБОРА ЯЗЫКА ---
    if lang == 'en':
        names = DEANON_NAMES_EN
        surnames = DEANON_SURNAMES_EN
        cities = DEANON_CITIES_EN
        streets = DEANON_STREETS_EN
        professions = DEANON_PROFESSIONS_EN
        fetishes = DEANON_FETISHES_EN
        details_list = DEANON_DETAILS_EN
        address_template = "{city}, {street}, {house}"
        apartment_template = ", apt. {flat}"
    else: # По умолчанию используется русский
        names = DEANON_NAMES
        surnames = DEANON_SURNAMES
        cities = DEANON_CITIES
        streets = DEANON_STREETS
        professions = DEANON_PROFESSIONS
        fetishes = DEANON_FETISHES
        details_list = DEANON_DETAILS
        address_template = "{city}, ул. {street}, д. {house}"
        apartment_template = ", кв. {flat}"

    # --- ОСТАЛЬНАЯ ЛОГИКА ОСТАЕТСЯ ПРЕЖНЕЙ, НО ИСПОЛЬЗУЕТ ВЫБРАННЫЕ СПИСКИ ---
    
    # Генерация адреса
    city = random.choice(cities)
    street = random.choice(streets)
    house = random.randint(1, 200)
    address = address_template.format(city=city, street=street, house=house)
    
    # --- ИЗМЕНЕННЫЙ БЛОК ГЕНЕРАЦИИ ДЕТАЛЕЙ ---
    if random.random() < 0.21:
        # С вероятностью 21% возвращаем заглушку
        details_str = "Nothing to say about this anon" if lang == 'en' else "Про анона нечего сказать"
    else:
        # В остальных случаях генерируем факты как раньше
        details = [random.choice(details_list)]
        
        # 40% шанс на второй факт
        if random.random() < 0.25:
            details.append(random.choice(details_list))
        
        # 10% шанс на третий факт
        if random.random() < 0.1:
            details.append(random.choice(details_list))
        
        details_str = ", ".join(details)
    # --- КОНЕЦ ИЗМЕНЕННОГО БЛОКА ---

    return (
        random.choice(names),
        random.choice(surnames),
        address,
        random.choice(professions),
        random.choice(fetishes),
        details_str
    )
