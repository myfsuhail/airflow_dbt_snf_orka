{{config(
    materialized = 'incremental',
    unique_key = 'team_name'
)}}

select case when team1='Delhi Capitals' then 'Delhi Daredevils' else team1 end as team_name
from {{ ref('match_info_cdc') }}
union 
select case when team2='Delhi Capitals' then 'Delhi Daredevils' else team2 end as team_name
from {{ ref('match_info_cdc') }}