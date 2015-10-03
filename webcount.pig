/* 
Problem Statement - This PIG script is to used to count the count of the webpages that are having the projectname as en.
*/

/* Pre requisite - The input file should have been uploaded on HDFS. In my case the HDFS directory is 'pig/webcount'*/

/*
A. Load webcount records
========================
en google.com 50 100
en yahoo.com 60 100
us google.com 70 100
en google.com 68 100
*/

records = LOAD 'pig/webcount/sample' using PigStorage(' ') as (projectname:chararray, pagename:chararray, pagecount:int, pagesize:int);

/*
B. FILTER webcount records
==========================
*/

project = FILTER records BY projectname == 'en';

/*
C. GROUP Project by pagename
=============================
*/

groupbyproject = GROUP project by pagename;

/*
D. Total count of the number of pages
=====================================
*/

totalpagecount = FOREACH groupbyproject GENERATE group, SUM(project.pagecount);

/*
E. Sort it in decreasing order
==============================
*/

final = ORDER totalpagecount BY $1 desc;


/*
F. Display the output
===================
*/

dump final;


