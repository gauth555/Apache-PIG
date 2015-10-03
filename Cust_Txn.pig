/* 
Problem Statement - This PIG script is to used to Join records from both the files using all the PIG commands like LOAD, LIMIT, GROUPBY, ORDER, COUNT, DUMP etc.  - 
1. Load the customer data records
2. Select only 100 records from the customer records
3. Group customer records by profession
4. Count no of customers by profession
5. Load transaction records
6. Group transactions by customer
7. Sum total amount spent by each customer
8. Order the customer records beginning from highest spender
9. Select only top 100 customers
10. Join the transactions with customer details
11. Select the required fields from the join for final output
12. Dump the final output
*/

/* Pre requisite - The input file should have been uploaded on HDFS. In my case the HDFS directory is 'pig/custtxn'*/

/*
A. Load Customer records
========================
4009992,Erin,Blackwell,33,Electrician
*/

cust = LOAD 'pig/custtxn/custs' using  PigStorage (',') AS (custid:chararray, fname:chararray, lname:chararray, age:int, profession:chararray);

/*
------------------------------------------------------------------------------------------------------------------------
| cust     | custid:chararray     | fname:chararray     | lname:chararray     | age:int     | profession:chararray     | 
------------------------------------------------------------------------------------------------------------------------
|          | 4002034              | Carlos              | Barber              | 33          | Writer                   | 
|          | 4003199              | Charlene            | May                 | 33          | Writer                   | 
------------------------------------------------------------------------------------------------------------------------
*/

/*
B. Select only 100 records
==========================
*/

amt = LIMIT cust 100;

/*
c. Group customer records by profession
=======================================
*/

custbyprof = GROUP cust BY profession;

/*

----------------------------------------------------------------------------------------------------------------------------------------------------------------------
| custbyprof     | group:chararray     | cust:bag{:tuple(custid:chararray,fname:chararray,lname:chararray,age:int,profession:chararray)}                             | 
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
|                | Writer              | {(4002034, ..., Writer), (4003199, ..., Writer)}                                                                            | 
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
*/

/*
D. Count no of customers by profession
======================================
*/

profcount = FOREACH custbyprof GENERATE group, COUNT(cust.profession); 

/*

---------------------------------------------------
| profcount     | group:chararray     | :long     | 
---------------------------------------------------
|               | Writer              | 2         | 
---------------------------------------------------

/*

E. Load transaction records
===========================

00000000,06-26-2011,4007024,040.33,Exercise & Fitness,Cardio Machine Accessories,Clarksville,Tennessee,credit

*/

txn = LOAD 'pig/custtxn/txns' USING PigStorage(',') AS (txnid:chararray, date:chararray, custid:chararray, amount:double, category:chararray, product:chararray, city:chararray, state:chararray, type:chararray);

/*
F. Group transactions by customer
=================================
*/
txnbycust = GROUP txn BY custid;

/*
G. Sum total amount spent by each customer
==========================================
*/
totalamt = FOREACH txnbycust GENERATE group, SUM(txn.amount);

/*
H. Order the customer records beginning from highest spender
============================================================
*/

topspenders = ORDER totalamt BY $1 desc;


/*
I. Select only top 100 customers
================================
*/

top100spenders = LIMIT topspenders 100;

/*
J. Join the transactions with customer details
==============================================
*/

top100join = JOIN top100spenders BY $0, cust BY $0;

/*
K. Select the required fields from the join for final output
============================================================
*/
final = FOREACH top100join GENERATE $0,$3,$4,$5,$6,$1;

/*
L.Dump the final output
=======================
*/
dump final;
