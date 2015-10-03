/* Pig script to analyse the given datasets and print the student names who have successfully cleared the exam */
/**/
 

/*This following script loads the student dataset from HDFS & the schema is defined after which it is stored in the 'stud' variable */

stud = LOAD 'pig/student/student' AS (name:chararray, rollno:int);

/*
--------------------------------------------------
| stud     | name:chararray     | rollno:int     | 
--------------------------------------------------
|          | shravi             | 18             | 
--------------------------------------------------
*/ 


/* 
This following script loads the results dataset from HDFS & the schema is defined after which it is stored in the 'res' variable */

res = LOAD 'pig/student/results' AS (rollno_:int, passorfail:chararray);

/*
--------------------------------------------------------
| res     | rollno_:int     | passorfail:chararray     | 
--------------------------------------------------------
|         | 12              | fail                     | 
|         | 18              | pass                     | 
--------------------------------------------------------
*/ 

/* Extracting those records that have the passorfail column value as 'pass' */
passstudent = FILTER res BY passorfail == 'pass';

/* Joining the results of stud and passstudent variables using the common column name rollno  */
passstudentname = JOIN stud BY rollno, passstudent BY rollno_;

/*
------------------------------------------------------------------------------------------------------------------------------------------------
| passstudentname     | stud::name:chararray     | stud::rollno:int     | passstudent::rollno_:int     | passstudent::passorfail:chararray     | 
------------------------------------------------------------------------------------------------------------------------------------------------
|                     | shravi                   | 18                   | 18                           | pass                                  | 
------------------------------------------------------------------------------------------------------------------------------------------------
*/

/* Read each record and display the name  */
names = FOREACH passstudentname GENERATE name;

/*
----------------------------------------
| names     | stud::name:chararray     | 
----------------------------------------
|           | shravi                   | 
----------------------------------------
*/

/* Print the names on the console*/
dump names;

