{{config(
    materialized = 'incremental'
)}}



with player_team_data as (
    /*Fetch Players Data and Flatten for each team they played*/
    select 
        id,
        name,
        replace(full_name,'-',' ') as full_name,
        a.value as teams
    from {{ ref('players_info_cdc') }} p,
    lateral flatten(input => SPLIT(p.major_teams, ''',')) a
),

player_team_flatten_data as (
    /*Filter to get only IPL Team Players*/
    select 
        distinct id as person_id, 
        name, 
        full_name, 
        split(full_name,' ') as list_name,
        nvl(substring(trim(trim(list_name[0],'"')),1,1),'') as c0,
        nvl(substring(trim(trim(list_name[1],'"')),1,1),'') as c1,
        nvl(substring(trim(trim(list_name[2],'"')),1,1),'') as c2,
        nvl(substring(trim(trim(list_name[3],'"')),1,1),'') as c3,
        nvl(substring(trim(trim(list_name[4],'"')),1,1),'') as c4,
        nvl(substring(trim(trim(list_name[5],'"')),1,1),'') as c5,
        nvl(substring(trim(trim(list_name[6],'"')),1,1),'') as c6,
        nvl(substring(trim(trim(list_name[7],'"')),1,1),'') as c7,
        nvl(substring(trim(trim(list_name[8],'"')),1,1),'') as c8,
        trim(trim(list_name[array_size(list_name)-1],'"')) as last_name,
        concat(c0,c1,c2,c3,c4,c5,c6,c7,c8,' ',last_name) as concat_name,
        substring(concat_name,0,position(' ',concat_name)-2) || substring(concat_name,position(' ',concat_name)) as modified_player_name,
        trim(translate(teams,''',[-]', '')) as flatten_team_name
    from player_team_data a
    inner join {{ref('team')}} b
    on upper(trim(flatten_team_name)) = upper(trim(b.team_name))
),

team_player_record_cte as (
    /*Identify Player and Role in deliveries*/
    select batting_team as team_name,batsman as name, 'BATSMAN' as role,count(*) as cnt
    from {{ ref('deliveries_cdc') }} 
    group by batting_team,batsman
    union all
    select bowling_team,bowler, 'BOWLER' as role,count(*)
    from {{ ref('deliveries_cdc') }} 
    group by bowling_team,bowler
    union all
    select bowling_team,fielder, '' as role,count(*)
    from {{ ref('deliveries_cdc') }}  
    group by bowling_team,fielder
),

team_player_role_cte as (
    /*If player is both batsman and bowler, identify his main contribution in match*/
    select 
        team_name, 
        name as original_player_name,
        trim(replace(name, '(sub)', '')) as player_name,
        trim(substring(trim(player_name), length(trim(player_name))-position(' ',reverse(trim(player_name)))+1 )) as player_last_name,
        role, 
        cnt 
    from team_player_record_cte
    where name is not null
    qualify row_number() over (partition by team_name,name order by cnt desc )=1
),

get_person_id_cte1 as (
    select distinct b.person_id,b.name,b.full_name,b.modified_player_name,b.last_name, a.*
    from team_player_role_cte a
    left join player_team_flatten_data b
    on upper(trim(a.team_name)) = upper(trim(b.flatten_team_name))
    and (upper(trim(a.player_name)) = upper(trim(b.name)) 
    or upper(trim(a.player_name)) = upper(trim(b.full_name)) 
    or upper(trim(a.player_name)) = upper(trim(b.modified_player_name)))
),

get_person_id_cte2 as (
    select distinct b.person_id,b.name,b.full_name,b.modified_player_name,b.last_name, a.*
    from team_player_role_cte a
    left join player_team_flatten_data b
    on upper(trim(a.team_name)) = upper(trim(b.flatten_team_name))
    and (soundex(upper(trim(a.player_name))) = soundex(upper(trim(b.name))) 
    or soundex(upper(trim(a.player_name))) = soundex(upper(trim(b.full_name))) 
    or soundex(upper(trim(a.player_name))) = soundex(upper(trim(b.modified_player_name))))
),

selective_col_data as (
    select
        distinct
        coalesce(a.person_id,b.person_id) as person_id,
        coalesce(a.name,b.name) as name,
        coalesce(a.full_name,b.full_name) as full_name,
        coalesce(a.modified_player_name,b.modified_player_name) as modified_player_name,
        coalesce(a.last_name,b.last_name) as last_name,
        a.team_name,
        a.original_player_name,
        a.player_name,
        a.role,
        a.cnt
    from get_person_id_cte1 a
    inner join get_person_id_cte2 b
    on a.team_name = b.team_name
    and a.player_name = b.player_name
),

get_id_role_cte as (
    select 
        id,
        case 
            when batting_style is not null then 'BATSMAN'
            when bowling_style is not null then 'BOWLER'
            else ''
        end as role
    from {{ ref('players_info_cdc') }}
),

umpire_data as (
    /*Filter to get only IPL Team Players*/
    select 
        distinct id as person_id, 
        name, 
        full_name, 
        split(full_name,' ') as list_name,
        nvl(substring(trim(trim(list_name[0],'"')),1,1),'') as c0,
        nvl(substring(trim(trim(list_name[1],'"')),1,1),'') as c1,
        nvl(substring(trim(trim(list_name[2],'"')),1,1),'') as c2,
        nvl(substring(trim(trim(list_name[3],'"')),1,1),'') as c3,
        nvl(substring(trim(trim(list_name[4],'"')),1,1),'') as c4,
        nvl(substring(trim(trim(list_name[5],'"')),1,1),'') as c5,
        nvl(substring(trim(trim(list_name[6],'"')),1,1),'') as c6,
        nvl(substring(trim(trim(list_name[7],'"')),1,1),'') as c7,
        nvl(substring(trim(trim(list_name[8],'"')),1,1),'') as c8,
        trim(trim(list_name[array_size(list_name)-1],'"')) as last_name,
        concat(c0,c1,c2,c3,c4,c5,c6,c7,c8,' ',last_name) as concat_name,
        substring(concat_name,0,position(' ',concat_name)-2) || substring(concat_name,position(' ',concat_name)) as modified_player_name
    from {{ ref('players_info_cdc') }} a
),

umpire_cte as (
    select umpire1 as umpire
    from {{ ref('match_info_cdc') }}
    union
    select umpire2
    from {{ ref('match_info_cdc') }}
    union
    select umpire3
    from {{ ref('match_info_cdc') }}
),

umpire_info_cte as (
    select distinct b.person_id,b.name,b.full_name,b.modified_player_name,b.last_name, a.*
    from umpire_cte a
    left join umpire_data b
    on (upper(trim(a.umpire)) = upper(trim(b.name)) 
    or upper(trim(a.umpire)) = upper(trim(b.full_name)) 
    or upper(trim(a.umpire)) = upper(trim(b.modified_player_name)))
),

union_data as (
    select
        person_id,
        team_name,
        player_name,
        full_name,
        case when a.role='' then b.role else a.role end as role
    from selective_col_data a
    left join get_id_role_cte b
    on a.person_id = b.id

    union all

    select 
        person_id,
        null as team_name,
        umpire as player_name,
        full_name,
        'UMPIRE' as role
    from umpire_info_cte
)

select distinct *
from union_data
where person_id is not null