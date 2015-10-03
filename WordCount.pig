/* Problem Statement - This PIG script is used to count the number of words in a File */
/* Pre requisite - The input file should have been uploaded on HDFS. In my case the HDFS directory is 'pig/wordcount'*/

/*
A. Load Sample records
========================
*/

my_input = LOAD 'pig/wordcount/sample.txt' AS line;

/*
B. TOKENIZE and FLATTEN
========================
TOKENIZE splits the line into a field for each word. 
flatten will take the collection of records returned by TOKENIZE and roduce a separate record for each one, calling the single field in the record word.
*/

words = FOREACH my_input GENERATE flatten(TOKENIZE(line)) as word;

/*
C. Group words by Words
========================
*/

grpd = group words by word;

/*
D. Get the count of words
========================
*/
cntd = foreach grpd generate group, COUNT(words);

/*
E. Display the result on screen
================================
*/
dump cntd;




