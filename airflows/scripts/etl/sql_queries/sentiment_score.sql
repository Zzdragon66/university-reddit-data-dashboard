SELECT subreddit, create_date as date, ROUND(avg(sentiment), 2) * 10 as `Mean Sentiment Score`
FROM `project_id.dataset_id.table_id`
GROUP BY subreddit, create_date