WITH sjg68olrdxx7_customer_web_activity_csv AS (
  SELECT
    *
  FROM {{ source('uploads', 'sjg68olrdxx7_customer_web_activity_csv') }}
), stg_sjg68olrdxx7_customer_web_activity_sql AS (
  SELECT
    *
  FROM sjg68olrdxx7_customer_web_activity_csv
)
SELECT
  *
FROM stg_sjg68olrdxx7_customer_web_activity_sql