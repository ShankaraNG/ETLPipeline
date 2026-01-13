import oracledb
import hashlib
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import config_loader as cnfloader

def getConnection(db_user, db_password, dsn):
    return oracledb.connect(
        user=db_user,
        password=db_password,
        dsn=dsn
    )

def hashPassword(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()

def load_db_config():
    config = cnfloader.load_properties()

    databaseHost = config.get("databaseHost")
    databasePort = config.get("databasePort")
    databaseServiceName = config.get("databaseServiceName")
    dbUserName = config.get("userName")
    dbPassword = config.get("password")
    tableName = config.get("tableName")

    if not all([
        databaseHost,
        databasePort,
        databaseServiceName,
        dbUserName,
        dbPassword,
        tableName
    ]):
        raise ValueError("Database configuration is missing or incomplete")

    dsn = f"{databaseHost}:{int(databasePort)}/{databaseServiceName}"

    return dbUserName, dbPassword, dsn, tableName

def validateUser(username: str, password: str) -> str:
    conn = None
    cur = None

    try:
        dbUser, dbPassword, dsn, tableName = load_db_config()

        conn = getConnection(dbUser, dbPassword, dsn)
        cur = conn.cursor()

        cur.execute(
            f"""
            SELECT password_hash
            FROM {tableName}
            WHERE username = :username
            """,
            {"username": username}
        )

        row = cur.fetchone()

        if row is None or row[0] != hashPassword(password):
            return "Username or password is incorrect"

        return "SUCCESSFUL"

    except ValueError as ve:
        return str(ve)

    except Exception as e:
        print(e)
        return "FAILED"
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()

def addUser(username: str, password: str, email_id: str, full_name: str) -> str:
    conn = None
    cur = None

    if not (email_id.endswith(".com") and "@" in email_id):
        return "Invalid email format"

    try:
        dbUser, dbPassword, dsn, tableName = load_db_config()

        conn = getConnection(dbUser, dbPassword, dsn)
        cur = conn.cursor()

        # Check if username already exists
        cur.execute(
            f"""
            SELECT 1
            FROM {tableName}
            WHERE username = :username
            """,
            {"username": username}
        )

        if cur.fetchone():
            return "Username already exists and failed to create a user"

        cur.execute(
            f"""
            INSERT INTO {tableName}
            (username, password_hash, email_id, full_name)
            VALUES (:username, :password_hash, :email_id, :full_name)
            """,
            {
                "username": username,
                "password_hash": hashPassword(password),
                "email_id": email_id,
                "full_name": full_name
            }
        )

        conn.commit()
        return "User added successfully"

    except ValueError as ve:
        return str(ve)

    except oracledb.Error as e:
        if conn:
            conn.rollback()
        return "Failed to create a user"

    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()


def deleteUser(username: str, email_id: str, full_name: str) -> str:
    conn = None
    cur = None

    try:
        dbUser, dbPassword, dsn, tableName = load_db_config()

        conn = getConnection(dbUser, dbPassword, dsn)
        cur = conn.cursor()

        cur.execute(
            f"""
            DELETE FROM {tableName}
            WHERE username = :username
              AND email_id = :email_id
              AND full_name = :full_name
            """,
            {
                "username": username,
                "email_id": email_id,
                "full_name": full_name
            }
        )

        conn.commit()

        if cur.rowcount == 0:
            return "No matching user found. Delete failed"

        return "User deleted successfully"

    except ValueError as ve:
        return str(ve)

    except oracledb.Error as e:
        if conn:
            conn.rollback()
        return "Failed to Delete the user"

    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()



# print(addUser("Sng123", "Shankar123", "shankar@test.com", "Shankara N G"))

