with balls_on_over as
(
select match_id,over,inning, sum(case when wide_runs=0 and noball_runs=0 then 1 else 0 end) as balls
from {{ref('deliveries_cdc')}}
group by match_id,over,inning
),

scorecard as (
select match_id,season,venue,city,date as match_date,winner,inning,batting_team,sum(total_runs) as runs,count(dismissal_kind) as wicket_gone,max(over) as over
from {{ref('deliveries_cdc')}} a
inner join {{ref('match_info_cdc')}} b
on a.match_id=b.id
group by match_id,season,inning,batting_team,city,date,venue,winner
)

select s.* , b.balls
from scorecard s
inner join balls_on_over b
on s.match_id = b.match_id
and s.inning = b.inning
and s.over = b.over
