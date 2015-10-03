/* Problem Statement - This PIG script is used to find the highest and lowest temperature recorded for a city*/

/* Pre requisite - The input file should have been uploaded on HDFS. In my case the HDFS directory is 'pig/weather'*/

/*The input file is unstructured. The input file will be of the following format*/

/*63891 20130101  5.102  -86.61   32.85    12.8     9.6    11.2    11.6    19.4 -9999.00 U -9999.0 -9999.0 -9999.0 -9999.0 -9999.0 -9999.0 -99.000 -99.000 -99.000 -99.000 -99.000 -9999.0 -9999.0 -9999.0 -9999.0 -9999.0*/

/* In the following input file we need to extract the date, maximum and the minimum temperature. We have to manually count the position where the fields start*/

A = LOAD 'pig/weather/weather.txt' using TextLoader AS (data:chararray);
B = FOREACH A GENERATE TRIM(SUBSTRING(data, 6, 14)), TRIM(SUBSTRING(data, 46, 53)), TRIM(SUBSTRING(data, 38, 45));
STORE B INTO 'pig/weather/weather_new' USING PigStorage(',');
C = LOAD 'pig/weather/weather_new/part-m-00000' USING PigStorage(',') AS (date:chararray, min:double, max:double);

/*
>illustrate C;
-------------------------------------------------------------------
| C     | date:chararray      | min:double      | max:double      | 
-------------------------------------------------------------------
|       | 20130511            | 14.2            | 24.6            | 
|       | 20130128            | 8.3             | 18.7            | 
-------------------------------------------------------------------
*/ 

/*-------Hot Days------*/
X1 = FILTER C BY max > 25;

/* -------Cold Days------ */
X2 = FILTER C BY min < 0;

/*-------Hottest Day-----*/

D = GROUP C ALL; /* GROUP ALL - puts C's data in D's Tuple */

/*
>illustrate D;
---------------------------------------------------------------------------------------------------------------
| D     | group:chararray      | C:bag{:tuple(date:chararray,min:double,max:double)}                          | 
---------------------------------------------------------------------------------------------------------------
|       | all                  | {(20130511, 14.2, 24.6), (20130128, 8.3, 18.7)}                              | 
---------------------------------------------------------------------------------------------------------------
*/

E = FOREACH D GENERATE MAX(C.max) as maximum; /* Generate the maximum temperature by using the MAX function */
/*
>illustrate E;
-------------------------------
| E     | maximum:double      | 
-------------------------------
|       | 24.6                | 
-------------------------------
*/
F = FILTER C BY max == E.maximum; /* Getting the entire record with date for the maximum temperature */
dump F; /* Displaying the results */

/*
>illustrate F;
-------------------------------------------------------------------
| F     | date:chararray      | min:double      | max:double      | 
-------------------------------------------------------------------
|       | 20130511            | 14.2            | 24.6            | 
-------------------------------------------------------------------
*/

/* -------Coldest Day------*/

G = GROUP C ALL; /*  GROUP ALL - puts C's data in G's Tuple */
H = FOREACH G GENERATE MIN(C.min) as minimum; /* Generate the minimum temperature by using the MAX function */
I = FILTER C BY min == H.minimum; /* Getting the entire record with date for the maximum temperature */
/*dump I*/; /* Displaying the results */
