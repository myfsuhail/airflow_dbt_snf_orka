{{ config(
    materialized = 'incremental',
    unique_key = 'id'
) }}

with source_cte as (
    select
        {{ dbt_utils.generate_surrogate_key( ['id'] ) }} AS row_hash_id,
        a.*
    from {{ source('raw_conn','match_info') }} a
),

cdc as (
    select 
        src.*,
        case 
            when tgt.id is null then 'INSERT'
            when src.id is null then 'DELETE'
            when src.id = tgt.id then 'NO_CHANGE'
        end as change_indicator
        from {{ this }} tgt
        full join source_cte src
        on tgt.id = src.id
)

select
    id,
    season,
    city,
    date,
    team1,
    team2,
    toss_winner,
    toss_decision,
    result,
    dl_applied,
    winner,
    win_by_runs,
    win_by_wickets,
    player_of_match,
    venue,
    umpire1,
    umpire2,
    umpire3,
    current_timestamp() as elt_update_ts
from cdc
where change_indicator = 'INSERT'