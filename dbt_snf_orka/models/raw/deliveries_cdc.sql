{{ config(
    materialized = 'incremental',
    unique_key = 'row_hash_id'
) }}

with source_cte as (
SELECT
    distinct
    {{ dbt_utils.generate_surrogate_key(
        ['match_id',
        'inning',
        'batting_team',
        'bowling_team',
        'over',
        'ball',
        'batsman',
        'non_striker',
        'bowler',
        'is_super_over',
        'wide_runs',
        'bye_runs',
        'legbye_runs',
        'noball_runs',
        'penalty_runs',
        'batsman_runs',
        'extra_runs',
        'total_runs',
        'player_dismissed',
        'dismissal_kind',
        'fielder']
    ) }} AS row_hash_id,
    match_id,
    inning,
    batting_team,
    bowling_team,
    over,
    ball,
    batsman,
    non_striker,
    bowler,
    is_super_over,
    wide_runs,
    bye_runs,
    legbye_runs,
    noball_runs,
    penalty_runs,
    batsman_runs,
    extra_runs,
    total_runs,
    player_dismissed,
    dismissal_kind,
    fielder
from {{ source('raw_conn','deliveries') }}
),

cdc as (
    select 
        src.*,
        case 
            when tgt.row_hash_id is null then 'INSERT'
            when src.row_hash_id is null then 'DELETE'
            when src.row_hash_id = tgt.row_hash_id then 'NO_CHANGE'
        end as change_indicator
        from {{ this }} tgt
        full join source_cte src
        on tgt.row_hash_id = src.row_hash_id
)

select
    row_hash_id,
    match_id,
    inning,
    batting_team,
    bowling_team,
    over,
    ball,
    batsman,
    non_striker,
    bowler,
    is_super_over,
    wide_runs,
    bye_runs,
    legbye_runs,
    noball_runs,
    penalty_runs,
    batsman_runs,
    extra_runs,
    total_runs,
    player_dismissed,
    dismissal_kind,
    fielder,
    current_timestamp() as elt_update_ts
from cdc
where change_indicator = 'INSERT'