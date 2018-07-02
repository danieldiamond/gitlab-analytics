/* 
    author: chase wright
    date: 6/28/2018
    summary: table creation and current manual process for uploading
    headcount data into gitlab dw
*/


CREATE TABLE historical.headcount(
   id int,
   date date,
   function varchar(50),
   employee_cnt int,
   PRIMARY KEY(id)
);


INSERT INTO historical.headcount(id,date,function,employee_cnt)
VALUES
(a,b,c,d);