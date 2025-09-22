<<<<<<< HEAD
=======
<<<<<<< HEAD
# Oscorp-Industries
=======
>>>>>>> 0067a13 (Diagramm)
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


## Результаты появятся:

в терминале;

в папке results/ в формате CSV и Excel.

## Пример работы

# Терминал:

--- Запрос 1 ---
('England', 26)
('France', 23)
...

--- Запрос 2 ---
('2022-11-20', 'Qatar', 'Ecuador')
...


# Файлы:

results/query_1.csv
results/query_1.xlsx
<<<<<<< HEAD
results/query_2.csv
=======
results/query_2.csv
>>>>>>> d0f5f2c (Initial commit)
>>>>>>> 0067a13 (Diagramm)
