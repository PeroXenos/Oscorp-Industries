import psycopg2
import pandas as pd

# Настройки подключения
DB_NAME = "worldcup_db"
DB_USER = "postgres"
DB_PASSWORD = "postgres"   # замени на свой пароль
DB_HOST = "localhost"
DB_PORT = "5432"

# SQL-запросы (примеры — можно заменить на свои)
QUERIES = {
    "top_scorers": """
        SELECT player, team, goals
        FROM world_cup_squads
        WHERE goals > 0
        ORDER BY goals DESC
        LIMIT 10;
    """,
    "matches_by_year": """
        SELECT year, COUNT(*) AS total_matches
        FROM world_cup_matches
        GROUP BY year
        ORDER BY year;
    """,
    "teams_by_group": """
        SELECT group_name, COUNT(*) AS team_count
        FROM world_cup_groups
        GROUP BY group_name
        ORDER BY group_name;
    """
}


def run_query(query, conn):
    """Выполняет SQL-запрос и возвращает DataFrame"""
    try:
        df = pd.read_sql(query, conn)

        # Перекодируем строки в UTF-8, если они в Latin1
        df = df.applymap(
            lambda x: x.encode("latin1").decode("utf-8", errors="ignore")
            if isinstance(x, str)
            else x
        )
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
            options="-c client_encoding=LATIN1"
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
