SELECT 
  -- subreddit, create_date as `date`, count(distinct id) as n_comments
  SUBSTR(text, 0, 75) as preview,
  url as `Post URL`,
  author_url as `Author URL`,
  subreddit as `School`,
  CASE 
    WHEN sentiment = 0 then "neutral"
    when sentiment > 0 then "positive"
    when sentiment < 0 then "negative"
  end as `Sentiment`,
  score
FROM `project_id.dataset_id.table_id`
WHERE parent is null
order by score DESC
limit 100