


select *
from {{ ref('points_table_season') }}
where rnk <= 3