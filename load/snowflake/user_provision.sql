-- RUN THE FOLLOWING BEFORE THE SCRIPT (with correct values) using the SECURITYADMIN role
-- ====================
-- set email = 'email@gitlab.com';
-- set firstname  = 'Sasha';
-- set lastname = 'Ulyanov';
-- ====================
use role securityadmin;

use warehouse ADMIN;

set username = (select upper(LEFT($email, CHARINDEX('@', $email) - 1)));

set loginname = (select upper($email));

CREATE USER identifier($username) 
LOGIN_NAME = $loginname
DISPLAY_NAME = $username 
FIRST_NAME = $firstname
LAST_NAME = $lastname 
EMAIL = $email;

CREATE ROLE identifier($username) ;
GRANT ROLE identifier($username) TO ROLE "SYSADMIN";

GRANT ROLE identifier($username) to user identifier($username);

-- IF PASSWORD NEEDED (PROBABLY NOT NEEDED)
-- RUN THE FOLLOWING TO SET PASSWORD AND FORCE RESET (with randomly generated values) https://passwordsgenerator.net
-- ====================
-- ALTER USER identifier($username) SET PASSWORD ='randomGeneratedPassword' MUST_CHANGE_PASSWORD = TRUE;
