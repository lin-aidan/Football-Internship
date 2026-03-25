select 
    opponent || ' (' ||
    sum(case when result = 'W' then 1 else 0 end) || '-' ||
    sum(case when result = 'L' then 1 else 0 end) || '-' ||
    sum(case when result = 'T' then 1 else 0 end) || ')' as record
from game_results
group by opponent
order by opponent;

select *
from game_results;

SELECT line
FROM (
    -- Opponent header with record
    SELECT
        opponent,
        1 AS sort_order,
        opponent || ' (' ||
        SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) || '-' ||
        SUM(CASE WHEN result = 'L' THEN 1 ELSE 0 END) || '-' ||
        SUM(CASE WHEN result = 'T' THEN 1 ELSE 0 END) || ')' AS line,
        NULL AS game_date
    FROM game_results
    GROUP BY opponent

    UNION ALL

    -- Column headers
    SELECT
        opponent,
        2,
        'Date' || char(9) ||
        'Site' || char(9) ||
        'Result' || char(9) ||
        'Score',
        NULL
    FROM game_results
    GROUP BY opponent

    UNION ALL

    -- Game rows
    SELECT
        opponent,
        3,
        strftime('%m-%d-%Y', date) || char(9) ||
        site || char(9) ||
        result || char(9) ||
        "MU Score" || '-' || "Opp Score",
        date
    FROM game_results
)
ORDER BY
    opponent,
    sort_order,
    game_date;

-- 1p0, 8p5, 14p5, 20p5