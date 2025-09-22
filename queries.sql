
-- 1. Количество матчей на каждом чемпионате мира
SELECT year, COUNT(*) AS total_matches
FROM world_cup_matches
GROUP BY year
ORDER BY year;

-- 2. Среднее количество голов за матч по годам
SELECT year, ROUND(AVG(home_goals + away_goals), 2) AS avg_goals_per_match
FROM world_cup_matches
GROUP BY year
ORDER BY year;

-- 3. Страна с наибольшим количеством побед
SELECT winner, COUNT(*) AS wins
FROM world_cups
GROUP BY winner
ORDER BY wins DESC
LIMIT 1;

-- 4. Количество игроков в каждой команде (2022)
SELECT team, COUNT(*) AS squad_size
FROM world_cup_squads
GROUP BY team
ORDER BY squad_size DESC;

-- 5. Средний возраст игроков по группам
SELECT g.group_name, ROUND(AVG(s.age), 2) AS avg_age
FROM world_cup_squads s
JOIN world_cup_groups g ON s.group_id = g.id
GROUP BY g.group_name
ORDER BY avg_age;

-- 6. Лучшие бомбардиры (игроки с наибольшим количеством голов)
SELECT player, team, goals
FROM world_cup_squads
WHERE goals > 0
ORDER BY goals DESC
LIMIT 10;

-- 7. Количество матчей, сыгранных каждой сборной (международные матчи)
SELECT team, COUNT(*) AS matches_played
FROM (
    SELECT home_team AS team FROM international_matches
    UNION ALL
    SELECT away_team AS team FROM international_matches
) t
GROUP BY team
ORDER BY matches_played DESC
LIMIT 20;

-- 8. Среднее количество голов по континентам (пример — через команды в группах)
SELECT g.group_name, ROUND(AVG(s.goals), 2) AS avg_goals
FROM world_cup_squads s
JOIN world_cup_groups g ON s.group_id = g.id
GROUP BY g.group_name
ORDER BY avg_goals DESC;

-- 9. Хозяева турнира и их результат (место)
SELECT year, host_country, winner, runners_up
FROM world_cups
ORDER BY year DESC;

-- 10. Самые результативные матчи (по числу голов)
SELECT id, home_team, home_goals, away_team, away_goals, (home_goals + away_goals) AS total_goals
FROM world_cup_matches
ORDER BY total_goals DESC
LIMIT 10;

