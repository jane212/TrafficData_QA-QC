data = LOAD 'Tingting/SensorData/inrix_raw.txt' Using PigStorage(',') AS (name:chararray, a:chararray, b:chararray, score:chararray, d:chararray, e:chararray,  f:chararray, g:chararray, dt:chararray);

data2 = DISTINCT data;

data3 = FOREACH data2 GENERATE name, SUBSTRING(dt, 0,10) as date, SUBSTRING(dt, 11,13) as hour, score;

data4 = FOREACH data3 GENERATE name, date, hour, (score=='30'?1:0) as high, (score=='20'?1:0) as medium, (score=='10'?1:0) as low;

data5 = GROUP data4 BY (name, date, hour);

data6 = FOREACH data5 GENERATE group.name as name, group.date, group.hour as hour, SUM(data4.high) AS high, SUM(data4.medium) AS medium, SUM(data4.low) AS low;

data7 = FOREACH data6 GENERATE name, date, hour, high, medium, low, high+medium+low as total;

data8 = GROUP data7 BY (name, date);

data9 = FOREACH data8 GENERATE group.name as name, group.date, SUM(data7.total) AS dtotal;

data10 = JOIN data7 BY (name, date) LEFT, data9 BY (name, date);

data11 = FOREACH data10 GENERATE data7::name as name, data7::date as date, data7::hour as hour, data7::high as high, data7::medium as medium, data7::low as low, data7::total as total, data9::dtotal as dtotal;

final = FILTER data11 BY dtotal>1;


STORE final INTO 'Tingting/SensorData/inrix' Using PigStorage(',');