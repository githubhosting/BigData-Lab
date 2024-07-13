student_details = LOAD 'student.txt' USING 
PigStorage(',') as (id:int, firstname:chararray, lastname:chararray, age:int, phone:chararray, city:chararray);

filter_data = FILTER student_details by city == 'Chennai';
STORE filter_data INTO 'filter';
group_data = GROUP student_details by age;
STORE group_data INTO 'group';

customers = LOAD 'customer.txt' USING
PigStorage(',') as (id:int, name:chararray, age:int, address:chararray, salary:int);

orders = LOAD 'order.txt' USING 
PigStorage(',') as (oid:int, date:chararray, customer_id:int, amount:int);

join_result = JOIN customers BY id, orders BY customer_id;
STORE join_result INTO 'joinoutput';
sorting = ORDER join_result by age ASC;
STORE sorting into 'sortoutput';


pig -x local customer.pig