-- Career Passing Yards
select player, sum(yds) as yds
from Season_Passing
group by player
order by yds desc
limit 10;

-- Season Passing Yards
select player, yds, year
from Season_Passing
order by yds desc
limit 10;

-- Career Passing Touchdowns
select player, sum(td) as td
from Season_Passing
group by player
order by td desc
limit 10;

-- Season Passing Touchdowns
select player, td, year
from Season_Passing
order by td desc
limit 10;

-- Career Completions
select player, sum(comp) as comp
from Season_Passing
group by player
order by comp desc
limit 10;

-- Career Rushing Attempts
select name, sum(att) as att
from Season_Rushing
group by name
order by att desc
limit 15;

-- Season Rushing Yards
select name, net as yds, year
from Season_Rushing
order by yds desc
limit 10;

-- Career Rushing Yards
select name, sum(net) as yds
from Season_Rushing
group by name
order by yds desc
limit 15;

-- Season Rushing Touchdowns
select name, td, year
from Season_Rushing
order by td desc
limit 15;

-- Career Rushing Touchdowns
select name, sum(td) as td
from Season_Rushing
group by name
order by td desc
limit 15;

-- Season Receptions
select name, no as rec, year
from Season_Receiving
order by rec desc
limit 10;

-- Career Receptions
select name, sum(no) as rec
from Season_Receiving
group by name
order by rec desc
limit 15;

-- Season Receiving Yards
select name, yds, year
from Season_Receiving
order by yds desc
limit 10;

-- Career Receiving Yards
select name, sum(yds) as yds
from Season_Receiving
group by name
order by yds desc
limit 15;

-- Season Receiving Touchdowns
select name, td, year
from Season_Receiving
order by td desc
limit 15;

-- Career Receiving Touchdowns
select name, sum(td) as td
from Season_Receiving
group by name
order by td desc
limit 15;

-- Season Total Touchdowns
select name, td, year
from season_scoring
order by td desc
limit 15;

-- Career Total Touchdowns
select name, sum(td) as td
from season_scoring
group by name
order by td desc
limit 15;

-- Season AP Yards
select 
    name,
        sum(yds) AS total_yds,
            year
            from (
                select name, year, net as yds from season_rushing
                    union all
                        select name, year, yds from season_receiving
                            union all
                                select name, year, yds from season_KR
                                    union all
                                        select name, year, yds from season_PR
                                        )
                                        group by name, year
                                        order by total_yds desc
                                        limit 12;

                                        -- Career AP Yards
                                        select 
                                            name,
                                                sum(yds) AS total_yds
                                                from (
                                                    select name, year, net as yds from season_rushing
                                                        union all
                                                            select name, year, yds from season_receiving
                                                                union all
                                                                    select name, year, yds from season_KR
                                                                        union all
                                                                            select name, year, yds from season_PR
                                                                            )
                                                                            group by name
                                                                            order by total_yds desc
                                                                            limit 12;

                                                                            