/*
-- There is no way on God's Green Earth that Zach Hill has more career tackles than Tommy Zacharyasz
select name, tot, year
from Season_Defense
order by name;
*/

-- Career Tackles
select name, sum(tot) as tackles
from Season_Defense
group by name
order by tackles desc
limit 15;

-- Career TFLs
select name, sum(tfl) as tfl
from Season_Defense
group by name
order by tfl desc
limit 15;

-- Career Sacks
select name, sum(sacks) as sacks
from Season_Defense
group by name
order by sacks desc
limit 15;

-- Career Interceptions
select name, sum(int) as ints
from Season_Defense
group by name
order by ints desc
limit 15;

-- Career Forced Fumbles
select name, sum(ff) as FF
from Season_Defense
group by name
order by fumbles desc
limit 15;

-- Career Fumble Recoveries
select name, sum(fr) as FR
from Season_Defense
group by name
order by FR desc
limit 15;
