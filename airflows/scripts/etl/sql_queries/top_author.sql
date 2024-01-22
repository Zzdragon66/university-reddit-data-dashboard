SELECT
  t1.authorname as Name,
  MAX(author_url) as URL,
  MAX(school) as school,
  SUM(if_post) as `Posts`,
  SUM(if_comment) as `Comments`,
  SUM(if_post) + SUM(if_comment) as `Interactions`,
  CASE 
    WHEN avg(sentiment) > 0 THEN "Positive"
    WHEN avg(sentiment) < 0 THEN "Negative"
    ELSE "Neural"
  END as `Sentiment`
FROM 
(SELECT
  (CASE WHEN parent is null then 1 ELSE 0 END) as if_post,
  (CASE WHEN parent is null then 0 ElSE 1 END) as if_comment,
  authorname,
  author_url,
  subreddit as school,
  sentiment,
  id
FROM `project_id.dataset_id.table_id`) as t1
WHERE t1.authorname is not null
GROUP BY t1.authorname
ORDER BY Interactions DESC, Posts DESC