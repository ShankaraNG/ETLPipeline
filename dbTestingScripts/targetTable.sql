-- Drop table if it already exists (optional)
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE employees_target';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;
/

-- Create table
CREATE TABLE employees_target (
    emp_id     NUMBER PRIMARY KEY,
    emp_name   VARCHAR2(50),
    joined_on  TIMESTAMP
);


COMMIT;