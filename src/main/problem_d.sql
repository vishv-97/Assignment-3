WITH LatestReports AS (
  SELECT
    reportCreationDate,
    forDate,
    case1_records,
    case2_records,
    case3_records,
    ROW_NUMBER() OVER (PARTITION BY forDate ORDER BY reportCreationDate DESC) AS row_num
  FROM
    quality_table
)
SELECT
  reportCreationDate,
  forDate,
  case1_records,
  case2_records,
  case3_records
FROM
  LatestReports
WHERE
  row_num = 1;