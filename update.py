import psycopg2
import time
from datetime import datetime, timedelta
import random


DB_NAME = "WorldCup_db" 
DB_USER = "DataVis"
DB_PASS = "DataVis"
DB_HOST = "localhost" 
DB_PORT = "5432"
# --------------------------------------------------------

TEAMS = [
    "Brazil", "Germany", "Argentina", "France", "Spain", "England", 
    "Portugal", "Italy", "Netherlands", "Croatia", "Qatar", "Mexico"
]

def insert_new_world_cup_match():
    """Вставляет одну новую запись в public.world_cup_matches."""
    conn = None
    try:
        # Подключение к базе данных
        conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT)
        cursor = conn.cursor()

        # Случайный выбор данных для нового матча
        home_team = random.choice(TEAMS)
        # Гостевая команда, которая отличается от домашней
        away_team = random.choice([t for t in TEAMS if t != home_team])
        home_goals = random.randint(50, 60)
        away_goals = random.randint(50, 60)
        
        match_id = random.randint(1000000, 9999999) 
        # Используем текущий год для имитации матчей самого последнего турнира
        current_year = 2022 
        current_date = datetime.now() 
        stage = random.choice(["Group stage", "Round of 16", "Quarter-finals", "Semi-finals"])
        win_conditions = ' '
        host_team = 'true'

        insert_query = f"""
        INSERT INTO public.world_cup_matches 
            (id, date, year, stage, home_team, home_goals, away_goals, away_team, win_conditions, host_team)
        VALUES 
            ({match_id}, '{current_date}', {current_year}, '{stage}', '{home_team}', {home_goals}, {away_goals}, '{away_team}', '{win_conditions}', {host_team});
        """
        
        cursor.execute(insert_query)
        conn.commit()
        print(f"[{current_date.strftime('%H:%M:%S')}] Вставлен новый матч {current_year}: {home_team} {home_goals}-{away_goals} {away_team} (Стадия: {stage})")

    except Exception as e:
        print(f"Ошибка при вставке данных: {e}")
        # Проверьте, что колонки в базе данных точно соответствуют используемым в запросе
    finally:
        if conn:
            conn.close()

# =========================================================================
# === 2. ЗАПУСК ЦИКЛА ОБНОВЛЕНИЯ ===
# =========================================================================

if __name__ == "__main__":
    print(f"Скрипт запущен. Данные будут добавляться в public.world_cup_matches каждые 15 секунд...")
    
    while True:
        insert_new_world_cup_match()
        time.sleep(5)