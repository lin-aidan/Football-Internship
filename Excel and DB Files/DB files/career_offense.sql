-- Career Passing Yards
select player, sum(yds) as yds
from Season_Passing
group by player
order by yds desc
limit 15;

-- Career Passing Touchdowns
select player, sum(td) as td
from Season_Passing
group by player
order by td desc
limit 15;

-- Career Completions
select player, sum(comp) as comp
from Season_Passing
group by player
order by comp desc
limit 15;

-- Career Rushing Attempts
select name, sum(att) as att
from Season_Rushing
group by name
order by att desc
limit 15;

-- Career Rushing Yards
select name, sum(net) as yds
from Season_Rushing
group by name
order by yds desc
limit 15;

-- Career Rushing Touchdowns
select name, sum(td) as td
from Season_Rushing
group by name
order by td desc
limit 15;

-- Career Receptions
select name, sum(no) as rec
from Season_Receiving
group by name
order by rec desc
limit 15;

-- Career Receiving Yards
select name, sum(yds) as yds
from Season_Receiving
group by name
order by yds desc
limit 15;

-- Career Receiving Touchdowns
select name, sum(td) as td
from Season_Receiving
group by name
order by td desc
limit 15;

-- Career Total Touchdowns
select rush.name, sum(rush.td) as rush_td, sum(rec.td) as rec_td, (sum(rush.td) + sum(rec.td)) as total_td
from Season_Rushing rush
left join Season_Receiving rec
    on rush.name = rec.name and rush.year = rec.year
group by rush.name
order by total_td desc
limit 15;

