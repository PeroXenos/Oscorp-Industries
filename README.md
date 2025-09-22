

# Oscorp-Industries

## Краткое описание проекта:
Oscorp-Industries занимается спортивной аналитикой. В рамках текущего проекта команда анализирует данные по чемпионатам мира по футболу (WorldCups dataset), выявляет тенденции, оценивает результаты команд и игроков, а также строит визуализации для поддержки принятия решений.

## Скриншот основной аналитики:
![](Diagram.png)

## Пошаговые инструкции по запуску проекта:

### Запуск PostgreSQL через Docker:

```docker run --name worldcups-db -e POSTGRES_PASSWORD=yourpassword -d -p 5432:5432 postgres:15```


### Импорт данных WorldCups в PostgreSQL:

```psql -h localhost -U postgres -d postgres -f worldcups.sql```


### Запуск Apache Superset через Docker:

```docker run -d -p 8088:8088 apache/superset```


### Подключение Superset к базе данных:

URL подключения:

```postgresql+psycopg2://postgres:yourpassword@localhost:5432/postgres```


Создайте новую таблицу или подключите существующую.

### Создание визуализаций и дашбордов:

Используйте интерфейс Superset для построения графиков, таблиц и дашбордов на основе WorldCups dataset.


## Используемые инструменты и ресурсы:

Язык программирования: Python

База данных: PostgreSQL

Контейнеризация: Docker

BI / визуализация: Apache Superset

Данные: WorldCups dataset


# World Cup Database Project

## 📌 Описание
Проект для работы с базой данных, содержащей информацию о Чемпионате мира по футболу.  
Скрипт `main.py` подключается к PostgreSQL, выполняет SQL-запросы из файла `queries.sql`, выводит результаты в терминал и сохраняет их в CSV/Excel.

---

## ⚙️ Требования
- Python 3.9+
- PostgreSQL 13+
- Установленные зависимости:
  ```bash
  pip install psycopg2 pandas openpyxl


## Структура проекта
├── main.py          # Python-скрипт для запуска запросов
├── queries.sql      # SQL-запросы для аналитики
├── README.md        # Документация проекта
└── WorldCup.zip     # Архив с таблицами


## Настройте доступ к БД в main.py:

DB = {
    "dbname": "worldcup",
    "user": "postgres",
    "password": "ваш_пароль",
    "host": "localhost",
    "port": "5432"
}


## Запустите скрипт:

python main.py



[Diagram.png]: Diagramm.png