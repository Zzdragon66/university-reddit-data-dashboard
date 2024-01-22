SELECT 
  subreddit, create_date as `date`, count(distinct id) as n_posts
FROM `project_id.dataset_id.table_id`
WHERE parent is not null
group by subreddit, create_date