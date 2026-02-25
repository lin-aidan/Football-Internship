select *
from game_results;

select
    strftime('%Y', date) as season,
    sum(case when result = 'W' then 1 else 0 end) as wins,
    sum(case when result = 'L' then 1 else 0 end) as losses,
    count(*) as total_games
from game_results
group by season
order by season;

select date, opponent, result, MU Score, Opp Score
from game_results
order by date;