

with winner_point as (
    select
        season,
        winner as team_name,
        2 as point
    from {{ ref('match_info_cdc') }}
),

rnk_cte as (
    select season, team_name, sum(point) as total_points, row_number() over(partition by season order by total_points desc) as rnk
    from winner_point
    group by season, team_name
)

select *
from rnk_cte