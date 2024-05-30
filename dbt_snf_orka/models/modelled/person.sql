
{{config(
    materialized = 'incremental',
    unique_key = 'person_id'
)}}

select
    id as person_id,
    name as person_name,
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
    elt_update_ts as src_ts,
    current_timestamp() as elt_update_ts
from {{ref('players_info_cdc')}} src

{% if is_incremental() %}

  where src.elt_update_ts > (select coalesce(max(src_ts), '1900-01-01') from {{ this }} )

{% endif %}