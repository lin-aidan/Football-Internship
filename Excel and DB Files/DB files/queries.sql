-- Career Passing Yards
select player, sum(yds) as yds
from Season_Passing
group by player
order by yds desc
limit 15;

-- Career Rushing Yards
select name, sum(net) as yds
from Season_Rushing
group by name
order by yds desc;

-- Career Receiving Yards
select name, sum(yds) as yds
from Season_Receiving
group by name
order by yds desc;

select name, tot, year
from Season_Defense
order by name;

-- Career Tackles
select name, sum(tot) as tackles
from Season_Defense
group by name
order by tackles desc;

-- Career Punting Yards
select name, sum(yds) as yds
from Season_Punting
group by name
order by yds desc;
