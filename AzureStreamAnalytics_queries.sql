SELECT *
FROM events
WHERE temperature_C > 24;

SELECT datepart(hour,obs_datetime) as hour, datepart(minute,obs_datetime) as minute, * 
FROM events;

SELECT min(temperature_C) AS min_temperature_C
    , max(temperature_C) AS max_temperature_C
    , (max(temperature_C) - min(temperature_C)) / max(temperature_C) as temperature_variation_pct
FROM events;

SELECT min(temperature_C) OVER (LIMIT DURATION (minute, 5)) AS min_temperature_C
    , max(temperature_C) OVER (LIMIT DURATION (minute, 5)) AS max_temperature_C
    , (max(temperature_C) OVER (LIMIT DURATION (minute, 5)) - min(temperature_C)OVER (LIMIT DURATION (minute, 5))) / max(temperature_C) OVER (LIMIT DURATION (minute, 5)) as temperature_variation_pct
FROM events;

SELECT min(temperature_C) AS min_temperature_C
    , max(temperature_C) AS max_temperature_C
    , (max(temperature_C) - min(temperature_C)) / max(temperature_C) as temperature_variation_pct
FROM events
GROUP BY tumblingwindow(minute,2) ;



SELECT min(temperature_C) AS min_temperature_C
    , max(temperature_C) AS max_temperature_C
    , (max(temperature_C) - min(temperature_C)) / max(temperature_C) as temperature_variation_pct
    , System.Timestamp as timewindow_end
FROM events
GROUP BY tumblingwindow(minute,2) ;



SELECT min(temperature_C) AS min_temperature_C
    , max(temperature_C) AS max_temperature_C
    , (max(temperature_C) - min(temperature_C)) / max(temperature_C) as temperature_variation_pct
    , System.Timestamp as timewindow_end
    , topone() OVER (ORDER BY temperature_C desc) as highest_obs
FROM events
GROUP BY tumblingwindow(minute,2) ;



SELECT min(temperature_C) AS min_temperature_C
    , max(temperature_C) AS max_temperature_C
    , (max(temperature_C) - min(temperature_C)) / max(temperature_C) as temperature_variation_pct
    , System.Timestamp as timewindow_end
    , GetRecordPropertyValue(
        cast(topone() OVER (ORDER BY temperature_C desc) as record),'obs_datetime'
        ) as highest_obs
FROM events
GROUP BY tumblingwindow(minute,2) ;



-- Run demo2 Python script

SELECT temperature_C_random
    , ANOMALYDETECTION(temperature_C_random) OVER(LIMIT DURATION(second, 10)) AS anomaly_rand
    , temperature_C_steady_increase
    , ANOMALYDETECTION(temperature_C_steady_increase) OVER(LIMIT DURATION(second, 10)) AS anomaly_steady
    , temperature_C_stepwise_increase
    , ANOMALYDETECTION(temperature_C_stepwise_increase) OVER(LIMIT DURATION(second, 10)) AS anomaly_stepwise
FROM events;




SELECT temperature_C_random
    ,GetRecordPropertyValue(
     ANOMALYDETECTION(temperature_C_random) OVER(LIMIT DURATION(second, 10)),
    'BiLevelChangeScore') AS anomaly_rand_bi
    , temperature_C_steady_increase
    , GetRecordPropertyValue(
    ANOMALYDETECTION(temperature_C_steady_increase) OVER(LIMIT DURATION(second, 10)),
    'BiLevelChangeScore') AS anomaly_steady_bi
    , GetRecordPropertyValue(
    ANOMALYDETECTION(temperature_C_steady_increase) OVER(LIMIT DURATION(second, 10)),
    'SlowPosTrendScore') AS anomaly_steady_pos
    , temperature_C_stepwise_increase
    , GetRecordPropertyValue(
    ANOMALYDETECTION(temperature_C_stepwise_increase) OVER(LIMIT DURATION(second, 10))
    , 'BiLevelChangeScore') AS anomaly_stepwise_bi
    , GetRecordPropertyValue(
    ANOMALYDETECTION(temperature_C_stepwise_increase) OVER(LIMIT DURATION(second, 10))
    , 'SlowPosTrendScore') AS anomaly_stepwise_pos
FROM events;


-- run demo3 python script

WITH TimeParsed as
( select datepart(weekday,System.TimeStamp) as weekday,
    datepart(hour,System.TimeStamp) as hour, 
    datepart(minute,System.TimeStamp) as minute,
    avg(temperature_C) as avg_temp_c
  FROM events
  GROUP BY tumblingwindow(minute,2)
)     
SELECT * FROM TimeParsed;


WITH TimeParsed as
( select datepart(weekday,System.TimeStamp) as weekday,
    datepart(hour,System.TimeStamp) as hour, 
    datepart(minute,System.TimeStamp) as minute,
    avg(temperature_C) as avg_temp_c
  FROM events
  GROUP BY tumblingwindow(minute,2)
)     

, MLEnhanced as
(SELECT weekday, hour, minute, avg_temp_c
    , udf.predictHeater(weekday, hour, minute, avg_temp_c) as prediction
 FROM TimeParsed)
 
SELECT weekday, hour, minute, avg_temp_c, GetRecordPropertyValue(prediction, 'Scored Labels') as startHeater
, GetRecordPropertyValue(prediction, 'Scored Probabilities') as startHeaterProb
 FROM MLEnhanced;






WITH mlpredictions AS
( SELECT *, needHeater(dayOfWeek, hour, minute, temperature) as nh
  FROM events
)
SELECT
dayOfWeek, hour, minute, temperature
, GetRecordPropertyValue(nh, 'Scored Labels') as startHeater
, GetRecordPropertyValue(nh, 'Scored Probabilities') as startHeaterProb
INTO tempasa
FROM
    mlpredictions



SELECT System.Timestamp as ts, uda.LongestIncreasingSequence(temperature) as longestseq
  FROM events
group by tumblingwindow(second,10);