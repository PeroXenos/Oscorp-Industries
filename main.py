import psycopg2
import pandas as pd

# Настройки подключения
DB_NAME = "WorldCup_db"
DB_USER = "DataVis"
DB_PASSWORD = "DataVis"
DB_HOST = "localhost"
DB_PORT = "5432"

# SQL-запросы
QUERIES = {
    "top_scorers": """
        SELECT Player, Team, Goals
        FROM world_cup_squads
        WHERE Goals > 0
        ORDER BY Goals DESC
        LIMIT 10;
    """,

    # 2. Общее количество матчей по годам (Использует world_cup_matches)
    "matches_by_year": """
        SELECT Year, COUNT(*) AS total_matches
        FROM world_cup_matches
        GROUP BY Year
        ORDER BY Year;
    """,

    # 3. Количество команд в каждой группе (Использует 2022_world_cup_groups)
    "teams_by_group_2022": """
        SELECT "Group" AS group_name, COUNT(Team) AS team_count
        FROM world_cup_groups
        GROUP BY "Group"
        ORDER BY "Group";
    """,

    # 4. Среднее количество голов, забитых за матч, по годам
    # (Корректировка имен колонок с пробелами)
    "avg_goals_by_year": """
        SELECT 
            Year, 
            CAST(SUM("Home Goals" + "Away Goals") AS DECIMAL) / COUNT(*) AS avg_goals
        FROM 
            world_cup_matches
        GROUP BY 
            Year
        ORDER BY 
            Year DESC;
    """,

    # 5. Топ-10 игроков по суммарным голам (Использует 2022_world_cup_squads)
    "player_total_goals": """
        SELECT 
            Player, 
            Team, 
            SUM(Goals) AS total_goals
        FROM 
            world_cup_squads
        GROUP BY 
            Player, Team
        ORDER BY 
            total_goals DESC
        LIMIT 10;
    """,

    # 6. Список команд-победителей и их страны-хозяйки (LEFT JOIN)
    # (Использует world_cups и world_cup_matches)
    "winners_and_hosts": """
        SELECT 
            T.Winner, 
            T."Host Country"
        FROM 
            world_cups T
        LEFT JOIN 
            world_cup_matches M ON T.Year = M.Year AND T.Winner = M."Home Team"
        GROUP BY 
            T.Winner, T."Host Country"
        ORDER BY 
            T.Year DESC
        LIMIT 10;
    """,

    # 7. Матчи, в которых не было забито голов (Фильтрация)
    "goalless_matches": """
        SELECT 
            Date, 
            Stage, 
            "Home_Team", 
            "Away_Team"
        FROM 
            world_cup_matches
        WHERE 
            "Home Goals" = 0 AND "Away Goals" = 0
        LIMIT 10;
    """,

    # 8. Топ-10 команд по общему количеству сыгранных матчей
    # (LEFT JOIN для подсчета матчей, используя команды из 2022_world_cup_groups как базовый список)
    "team_total_matches": """
        SELECT 
            T.Team, 
            COUNT(M.ID) AS total_played 
        FROM 
            world_cup_groups T
        LEFT JOIN 
            world_cup_matches M ON T.Team = M."Home Team" OR T.Team = M."Away Team"
        GROUP BY 
            T.Team
        ORDER BY 
            total_played DESC
        LIMIT 10;
    """,

    # 9. Годы, в которых победитель не совпал с хозяином (JOIN)
    "non_host_winners": """
        SELECT 
            "Host Country", 
            Winner, 
            Year
        FROM 
            world_cups
        WHERE 
            "Host Country" <> Winner
        ORDER BY 
            Year DESC
        LIMIT 10;
    """,

    # 10. Общая информация о матчах, RIGHT JOIN к базе данных турниров.
    # (Показывает, как можно объединить все матчи с историей турнира)
    "matches_right_join_cups": """
        SELECT 
            M."home_team", 
            M."away_team", 
            C.Winner,
            C."host_country"
        FROM 
            world_cup_matches M
        RIGHT JOIN 
            world_cups C ON M.Year = C.Year
        LIMIT 10;
    """
}

def run_query(query, conn):
    """Выполняет SQL-запрос и возвращает DataFrame"""
    try:
        # Просто читаем данные. psycopq2/pandas сами декодируют строку
        # в соответствии с настройкой client_encoding.
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        print(f"❌ Ошибка при выполнении запроса: {e}")
        return None


def main():
    try:
        # Подключение с указанием кодировки
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            options="-c client_encoding=UTF-8"
        )
        print("✅ Подключение успешно")

        # Выполняем все запросы
        for name, query in QUERIES.items():
            print(f"\n=== {name.upper()} ===")
            df = run_query(query, conn)
            if df is not None:
                print(df)

                # Сохраняем в CSV и Excel в UTF-8
                df.to_csv(f"{name}.csv", index=False, encoding="utf-8-sig")
                df.to_excel(f"{name}.xlsx", index=False)
                print(f"💾 Сохранено: {name}.csv и {name}.xlsx")

        conn.close()
    except Exception as e:
        print(f"❌ Ошибка подключения или выполнения: {e}")


if __name__ == "__main__":
    main()
