
with temp_table as (SELECT 
  case
    when t1.parent is null then 1
    else 0 
  end if_post,
  case
    when t1.parent is null then 0
    else 1
  end as if_comment,
  t1.authorname as authorname
FROM `project_id.dataset_id.table_id` as t1)

select 
  SUM(if_post) as `Number of Posts`,
  SUM(if_comment) as `Number of Comments`,
  COUNT(DISTINCT authorname) as `Number of Authors`
from temp_table as t1





