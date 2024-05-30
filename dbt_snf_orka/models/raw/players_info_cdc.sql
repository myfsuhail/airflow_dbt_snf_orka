{{ config(
    materialized = 'incremental',
    unique_key = 'id'
) }}

with source_cte as (
    select
        {{ dbt_utils.generate_surrogate_key( ['id'] ) }} AS row_hash_id,
        a.*
    from {{ source('raw_conn','players_info') }} a
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
    name,
    country,
    full_name,
    birthdate,
    birthplace,
    died,
    date_of_death,
    age,
    major_teams,
    batting_style,
    bowling_style,
    other,
    awards,
    current_timestamp() as elt_update_ts
from cdc
where change_indicator = 'INSERT'