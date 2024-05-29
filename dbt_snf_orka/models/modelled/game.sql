{{config(
    materialized = 'incremental',
    unique_key = 'match_id'
)}}


select
    id as match_id,
    season,
    venue as match_stadium,
    city as match_city,
    date as match_date,
    case when team1='Delhi Capitals' then 'Delhi Daredevils' else team1 end as team1,
    case when team2='Delhi Capitals' then 'Delhi Daredevils' else team2 end as team2,
    case when toss_winner='Delhi Capitals' then 'Delhi Daredevils' else toss_winner end as toss_winner,
    toss_decision,
    result as match_result,
    dl_applied,
    case when winner='Delhi Capitals' then 'Delhi Daredevils' else winner end as match_winner,
    win_by_runs,
    win_by_wickets,
    player_of_match,
    umpire1,
    umpire2,
    umpire3,
    elt_update_ts as src_ts,
    current_timestamp() as elt_update_ts
from {{ ref('match_info_cdc') }} src

{% if is_incremental() %}

  where src.elt_update_ts > (select coalesce(max(src_ts), '1900-01-01') from {{ this }} )

{% endif %}