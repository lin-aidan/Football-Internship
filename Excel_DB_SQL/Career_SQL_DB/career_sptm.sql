-- Career Punts
select name, sum(no) as punts, sum(yds)/sum(no) as avg
from Season_Punting
group by name
order by punts desc
limit 10;

-- Career Punting Yards
select name, sum(yds) as yds
from Season_Punting
group by name
order by yds desc
limit 10;

-- Career XP Kicks
select name, sum(cast(substr(kick, 1, instr(kick, '-') - 1) as integer)) as xpm
from Season_Scoring
where kick is not null and kick like '%-%'
group by name
order by xpm desc
limit 10;

-- Career FGs Made
select name, sum(FGM) as FGs
from Season_FG
group by name
order by FGs desc
limit 10;

-- Career Kickoffs
select name, sum(no) as kickoffs, sum(yds)/sum(no) as avg
from Season_KO
group by name
order by kickoffs desc
limit 10;

-- Career Touchbacks
select name, sum(tb) as Touchbacks
from Season_KO
group by name
order by Touchbacks desc
limit 10;

-- Career Punt Returns
select name, sum(no) as punt_returns
from Season_PR
group by name
order by punt_returns desc
limit 10;

-- Career Punt Return Yards
select name, sum(yds) as pr_yds
from Season_PR
group by name
order by pr_yds desc
limit 10;

-- Career Punt Return Touchdowns
select name, sum(td) as pr_td
from Season_PR
group by name
order by pr_td desc
limit 10;

-- Career Kick Returns
select name, sum(no) as kr
from Season_KR
group by name
order by kr desc
limit 10;

-- Career Kick Return Yards
select name, sum(yds) as kr_yds
from Season_KR
group by name
order by kr_yds desc
limit 10;

-- Career Kick Return Touchdowns
select name, sum(td) as kr_td
from Season_KR
group by name
order by kr_td desc
limit 10;
