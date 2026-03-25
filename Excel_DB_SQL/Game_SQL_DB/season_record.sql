select *
from game_results
order by date;

select
    strftime('%Y', date) || ' (' ||
    sum(case when result = 'W' then 1 else 0 end) || '-' ||
    sum(case when result = 'L' then 1 else 0 end) || '-' ||
    sum(case when result = 'T' then 1 else 0 end) || ')' as record
from game_results
group by strftime('%Y', date)
order by strftime('%Y', date);

select 
    strftime('%m-%d', date) as game_date, 
    "MU Score" as mu_score,
    opponent,
    "Opp Score"
from game_results
order by date;

SELECT line
FROM (
    -- Header (record per season)
    SELECT 
        strftime('%Y', date) AS season,
        1 AS sort_order,
        strftime('%Y', date) || ' (' ||
        SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) || '-' ||
        SUM(CASE WHEN result = 'L' THEN 1 ELSE 0 END) || '-' ||
        SUM(CASE WHEN result = 'T' THEN 1 ELSE 0 END) || ')' 
        AS line,
        NULL AS game_date
    FROM game_results
    GROUP BY season

    UNION ALL

    -- Column headers (per season) WITH TABS
    SELECT 
        strftime('%Y', date) AS season,
        2,
        'Date' || char(9) ||
        'MU Score' || char(9) ||
        'Opponent' || char(9) ||
        'Opp Score',
        NULL
    FROM game_results
    GROUP BY season

    UNION ALL

    -- Game rows WITH TABS
    SELECT
        strftime('%Y', date) AS season,
        3,
        strftime('%m-%d', date) || char(9) ||
        "MU Score" || char(9) ||
        opponent || char(9) ||
        "Opp Score",
        date
    FROM game_results
)
ORDER BY 
    season,
    sort_order,
    game_date;