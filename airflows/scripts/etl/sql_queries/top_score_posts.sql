
select 
  MAX(preview) as preview,
  `POST URL`,
  MAX(`Author URL`) as `Author URL`,
  MAX(School) as School,
  MAX(Sentiment) as Sentiment,
  MAX(score) as score
FROM (SELECT 
  distinct SUBSTR(text, 0, 75) as preview,
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
limit 100) as t1
GROUP BY t1.`Post URL`
ORDER BY score DESC