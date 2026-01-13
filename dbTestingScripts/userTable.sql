-- Drop table if it exists (safe for testing)
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE etl_users';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;
/

-- Create table
CREATE TABLE etl_users (
    user_id        NUMBER PRIMARY KEY,
    username       VARCHAR2(50) NOT NULL,
    password_hash  VARCHAR2(128) NOT NULL,
    email_id       VARCHAR2(100),
    full_name      VARCHAR2(100)
);


COMMIT;