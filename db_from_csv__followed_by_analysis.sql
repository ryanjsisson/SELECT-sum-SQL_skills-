-- Ryan Sisson
-- Code to do the analysis

CREATE DATABASE test;                                                                                     -- creates the db on my local host

CREATE TABLE company_sizes (                                                                               -- creates the first of the tables
company_name VARCHAR(25) NOT NULL PRIMARY KEY,                                                             -- primary key, cannot be null
company_size VARCHAR(25) NOT NULL);

\copy public.company_sizes FROM '/Users/ryanJ/test_estimated_company_size.csv' DELIMITER ',' CSV HEADER;  -- imports data from the csv file on my local computer

CREATE TABLE registered_users (                                                                            -- next table
company_name VARCHAR(25) NOT NULL PRIMARY KEY,                                                             -- the PK we will be joining across tables
dec_2018 int NULL,                                                                                         -- can be null
jan_2019 int NULL,
feb_2019 int NULL,
mar_2019 int NULL,
apr_2019 int NULL,
may_2019 int NULL,
jun_2019 int NULL,
jul_2019 int NULL,
aug_2019 int NULL);

-- repeat steps

\copy public.registered_users FROM '/Users/ryanJ/test_registered_users.csv' DELIMITER ',' CSV HEADER;

CREATE TABLE trip_requests (
company_name VARCHAR(25) NOT NULL PRIMARY KEY,
dec_2018 int NULL,
jan_2019 int NULL,
feb_2019 int NULL,
mar_2019 int NULL,
apr_2019 int NULL,
may_2019 int NULL,
jun_2019 int NULL,
jul_2019 int NULL,
aug_2019 int NULL);

\copy public.trip_requests FROM '/Users/ryanJ/test_trip_requests.csv' DELIMITER ',' CSV HEADER;

CREATE TABLE scheduling_users (
company_name VARCHAR(25) NOT NULL PRIMARY KEY,
dec_2018 int NULL,
jan_2019 int NULL,
feb_2019 int NULL,
mar_2019 int NULL,
apr_2019 int NULL,
may_2019 int NULL,
jun_2019 int NULL,
jul_2019 int NULL,
aug_2019 int NULL);

\copy public.scheduling_users FROM '/Users/ryanJ/test_scheduling_users.csv' DELIMITER ',' CSV HEADER;

-- Done creating the tables/db
------------------------------
------------------------------
-- Analysis:

WITH size_thresholds AS (                                                           -- calculating 25th, 50th, and 75th percentile
  SELECT
    percentile_disc(0.25) WITHIN GROUP (ORDER BY company_size) AS qrt1,             -- 1994
    percentile_disc(0.5) WITHIN GROUP (ORDER BY company_size) AS qrt2,              -- 8994
    percentile_disc(0.75) WITHIN GROUP (ORDER BY company_size) AS qrt3              -- 15986
  FROM public.company_sizes
)
, penetration_rate AS (
  SELECT
    reg_user.company_name,
    reg_user.registered_user_count,
    reg_user.registered_user_count / size.company_size::FLOAT AS penetration_rate    -- change to float for precision
  FROM (
    SELECT
      company_name,
      (
        COALESCE(dec_2018, 0) +                                                      -- coalescing due to NULL values
        COALESCE(jan_2019, 0) +
        COALESCE(feb_2019, 0) +
        COALESCE(mar_2019, 0) +
        COALESCE(apr_2019, 0) +
        COALESCE(may_2019, 0) +
        COALESCE(jun_2019, 0) +
        COALESCE(jul_2019, 0) +
        COALESCE(aug_2019, 0)
      ) AS registered_user_count
    FROM public.registered_users
  ) reg_user
  LEFT JOIN public.company_sizes AS size                                             -- left join because I know it's 1:1, so this saves memory
  ON size.company_name = reg_user.company_name
)
, penetration_thresholds AS (
  SELECT
    percentile_disc(0.20) WITHIN GROUP (ORDER BY penetration_rate) AS twentieth,     -- for later analysis, creating 20/40/60/80th precentiles
    percentile_disc(0.40) WITHIN GROUP (ORDER BY penetration_rate) AS fortieth,
    percentile_disc(0.60) WITHIN GROUP (ORDER BY penetration_rate) AS sixtieth,
    percentile_disc(0.80) WITHIN GROUP (ORDER BY penetration_rate) AS eightieth
  FROM penetration_rate
)
, activation_rate AS (
  SELECT
    sched.company_name,
    (
      COALESCE(sched.dec_2018, 0) +
      COALESCE(sched.jan_2019, 0) +
      COALESCE(sched.feb_2019, 0) +
      COALESCE(sched.mar_2019, 0) +
      COALESCE(sched.apr_2019, 0) +
      COALESCE(sched.may_2019, 0) +
      COALESCE(sched.jun_2019, 0) +
      COALESCE(sched.jul_2019, 0) +
      COALESCE(sched.aug_2019, 0)
    ) /
    (
      COALESCE(reg.dec_2018, 0) +
      COALESCE(reg.jan_2019, 0) +
      COALESCE(reg.feb_2019, 0) +
      COALESCE(reg.mar_2019, 0) +
      COALESCE(reg.apr_2019, 0) +
      COALESCE(reg.may_2019, 0) +
      COALESCE(reg.jun_2019, 0) +
      COALESCE(reg.jul_2019, 0) +
      COALESCE(reg.aug_2019, 0)
    )::FLOAT AS activation_rate
  FROM public.scheduling_users AS sched
  LEFT JOIN public.registered_users AS reg
    ON reg.company_name = sched.company_name
)
-- same as above but for activation
, activation_threshold AS (
  SELECT
    percentile_disc(0.20) WITHIN GROUP (ORDER BY activation_rate) AS twentieth,
    percentile_disc(0.40) WITHIN GROUP (ORDER BY activation_rate) AS fortieth,
    percentile_disc(0.60) WITHIN GROUP (ORDER BY activation_rate) AS sixtieth,
    percentile_disc(0.80) WITHIN GROUP (ORDER BY activation_rate) AS eightieth
  FROM activation_rate
)
, trip_request_data AS (
  SELECT
    company_name,
    total_requested_rides,
    months_active,
    total_requested_rides / months_active AS monthly_average_ride_requests
  FROM (
    SELECT
      req.company_name,
      (
        COALESCE(req.dec_2018, 0) +
        COALESCE(req.jan_2019, 0) +
        COALESCE(req.feb_2019, 0) +
        COALESCE(req.mar_2019, 0) +
        COALESCE(req.apr_2019, 0) +
        COALESCE(req.may_2019, 0) +
        COALESCE(req.jun_2019, 0) +
        COALESCE(req.jul_2019, 0) +
        COALESCE(req.aug_2019, 0)
      ) AS total_requested_rides,
      (
        COUNT(req.dec_2018) +                                                -- Returns 0 if NULL
        COUNT(req.jan_2019) +
        COUNT(req.feb_2019) +
        COUNT(req.mar_2019) +
        COUNT(req.apr_2019) +
        COUNT(req.may_2019) +
        COUNT(req.jun_2019) +
        COUNT(req.jul_2019) +
        COUNT(req.aug_2019)
      ) AS months_active
    FROM public.trip_requests AS req
    GROUP BY company_name
  ) AS requests
)
, analysis AS (
  SELECT
    sizes.company_name,
    sizes.company_size,
    trip_request_data.total_requested_rides,
    trip_request_data.months_active,
    registered_user_count,
    trip_request_data.monthly_average_ride_requests,
    CASE
      WHEN company_size < size_thresholds.qrt1
      THEN 'small company'
      WHEN company_size > size_thresholds.qrt3
      THEN 'large company'
      ELSE 'medium company'
    END AS size_category,
    penetration_rate,
    CASE
      WHEN penetration_rate < penetration_thresholds.twentieth
      THEN 'low'
      WHEN
        penetration_rate >= penetration_thresholds.twentieth
        AND penetration_rate < penetration_thresholds.fortieth
      THEN 'below average'
      WHEN
        penetration_rate >= penetration_thresholds.fortieth
        AND penetration_rate < penetration_thresholds.sixtieth
      THEN 'average'
      WHEN
        penetration_rate >= penetration_thresholds.sixtieth
        AND penetration_rate < penetration_thresholds.eightieth
      THEN 'above average'
      WHEN penetration_rate >= penetration_thresholds.eightieth
      THEN 'high'
    END AS penetration_rate_category,
    activation_rate,
    CASE
      WHEN activation_rate < penetration_thresholds.twentieth
      THEN 'low'
      WHEN
        activation_rate >= activation_threshold.twentieth
        AND activation_rate < activation_threshold.fortieth
      THEN 'below average'
      WHEN
        activation_rate >= activation_threshold.fortieth
        AND activation_rate < activation_threshold.sixtieth
      THEN 'average'
      WHEN
        activation_rate >= activation_threshold.sixtieth
        AND activation_rate < activation_threshold.eightieth
      THEN 'above average'
      WHEN activation_rate >= activation_threshold.eightieth
      THEN 'high'
    END AS activation_rate_category
  FROM public.company_sizes AS sizes
  LEFT JOIN penetration_rate
    ON penetration_rate.company_name = sizes.company_name
  LEFT JOIN activation_rate
    ON activation_rate.company_name = sizes.company_name
  LEFT JOIN trip_request_data
    ON trip_request_data.company_name = sizes.company_name
  LEFT JOIN size_thresholds
    ON 1=1
  LEFT JOIN penetration_thresholds
    ON 1=1
  LEFT JOIN activation_threshold
    ON 1=1
)

-- This is where the model would be worked on, would like to take into consideration more features, but also hit approximately the desired segment sizes with this model.
SELECT
  analysis.*,
  CASE
    WHEN (
        size_category = 'large company'
        AND penetration_rate_category IN ('above average', 'high')
        AND activation_rate_category IN ('above average', 'high')
      ) OR (
        size_category = 'medium company'
        AND penetration_rate_category IN ('above average', 'high')
        AND activation_rate_category IN ('above average', 'high')
      )
    THEN 'platinum'
    WHEN (
        size_category = 'large company'
        AND penetration_rate_category IN ('average', 'below average')
        AND activation_rate_category IN ('average', 'below average')
      ) OR (
        size_category = 'medium company'
        AND penetration_rate_category IN ('average', 'below average')
        AND activation_rate_category IN ('average', 'below average')
      ) OR (
        size_category = 'small company'
        AND penetration_rate_category IN ('above average', 'high')
        AND activation_rate_category IN ('above average', 'high')
      )
    THEN 'gold'
    ELSE 'silver'
  END AS segment
FROM analysis
ORDER BY segment DESC


