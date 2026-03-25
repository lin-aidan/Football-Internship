-- Most rushing attempts in a game
select date, opponent, "rush att" as rush_attempts
from game_offense
order by rush_attempts desc
limit 5;

-- Most rushing attempts in a season
select 
    strftime('%Y', date) as season, 
    sum("rush att") as rush_attempts
from game_offense
group by season
order by rush_attempts desc
limit 5;

-- Most rushing yards in a game
select date, opponent, "rush yds" as rush_yards
from game_offense
order by rush_yards desc
limit 5;

-- Most rushing yards in a season
select
    strftime('%Y', date) as season, 
    sum("rush yds") as rush_yards
from game_offense
group by season
order by rush_yards desc
limit 5;

-- Most rushing touchdowns in a game
select date, opponent, "rush td" as rush_touchdowns
from game_offense
order by rush_touchdowns desc
limit 5;

-- Most rushing touchdowns in a season
select
    strftime('%Y', date) as season,
    sum("rush td") as rush_touchdowns
from game_offense
group by season
order by rush_touchdowns desc
limit 5;

-- Most completions/receptions in a game
select date, opponent, receptions
from game_offense
order by receptions desc
limit 5;

-- Most completions/receptions in a season
select
    strftime('%Y', date) as season,
    sum(receptions) as receptions
from game_offense
group by season
order by receptions desc
limit 5;

-- Most passing/receiving yards in a game
select date, opponent, "pass yds" as passing_yards
from game_offense
order by passing_yards desc
limit 5;

-- Most passing/receiving yards in a season
select
    strftime('%Y', date) as season,
    sum("pass yds") as passing_yards
from game_offense
group by season
order by passing_yards desc
limit 5;

-- Most passing/receiving touchdowns in a game
select date, opponent, "pass td" as passing_touchdowns
from game_offense
order by passing_touchdowns desc
limit 5;

-- Most passing/receiving touchdowns in a season
select
    strftime('%Y', date) as season,
    sum("pass td") as passing_touchdowns
from game_offense
group by season
order by passing_touchdowns desc
limit 5;

-- Most total yards in a game
select date, opponent, ("rush yds" + "pass yds") as total_yards
from game_offense
order by total_yards desc
limit 5;

-- Most total yards in a season
select
    strftime('%Y', date) as season,
    sum("rush yds" + "pass yds") as total_yards
from game_offense
group by season
order by total_yards desc
limit 5;

-- Total yards per game in a season
select
    strftime('%Y', date) as season,
    sum("rush yds" + "pass yds") * 1.0 / count(distinct date) as yards_per_game
from game_offense
group by season
order by yards_per_game desc
limit 5;

-- Most points in a game
select date, opponent, "mu score" as points
from game_results
order by points desc
limit 5;

-- Most points in a season
select
    strftime('%Y', date) as season,
    sum("mu score") as points
from game_results
group by season
order by points desc
limit 5;

-- Points per game in a season
select
    strftime('%Y', date) as season,
    sum("mu score") * 1.0 / count(distinct date) as points_per_game
from game_results
group by season
order by points_per_game desc
limit 5;

-- Largest Margin of Victory in a Game
select date, opponent, ("mu score" - "opp score") as margin_of_victory
from game_results
order by margin_of_victory desc
limit 5;

-- Most points scored by both teams in a game
select date, opponent, ("mu score" + "opp score") as total_points
from game_results
order by total_points desc
limit 5;