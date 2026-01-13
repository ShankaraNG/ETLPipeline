from flask import Flask, render_template, request, redirect, url_for, flash, session, abort
from functools import wraps
import services.userManagement as userManagement
import services.intro as intro
import services.historyPipeline as historyPipeline
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from dao.oracleDao import OracleDAO
import config_loader as cnfloader
import logger as logging
import mainApp as mainApp

app = Flask(__name__)
app.secret_key = "change-this-secret"

PIPELINE_HISTORY = []

# ---------------- AUTH DECORATORS ----------------
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get("logged_in"):
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return decorated_function


def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if session.get("role") != "admin":
            abort(403)
        return f(*args, **kwargs)
    return decorated_function


# ---------------- LOGIN ----------------
@app.route("/login", methods=["GET", "POST"])
def login():
    if session.get("logged_in"):
        return redirect(url_for("home"))

    if request.method == "POST":
        username = request.form.get("username")
        password = request.form.get("password")

        if username == "admin" and password == "admin123":
            session["logged_in"] = True
            session["username"] = username
            session["role"] = "admin"
            return redirect(url_for("home"))

        elif username !="admin":
            result = None
            result = userManagement.validateUser(username, password)
            if result == "SUCCESSFUL":
                session["logged_in"] = True
                session["username"] = username
                session["role"] = "user"
                return redirect(url_for("home"))
            else:
                flash("Invalid username or password", "error")
        else:
            flash("Invalid username or password", "error")

    return render_template("login.html")


# ---------------- LOGOUT ----------------
@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


# ---------------- PAGES ----------------
@app.route("/")
@login_required
def home():
    return render_template("home.html")


@app.route("/about")
@login_required
def about():
    return render_template("about.html")


@app.route("/run", methods=["GET", "POST"])
@login_required
# def run_pipeline():
#     # if request.method == "POST":
#     #     PIPELINE_HISTORY.append(dict(request.form))
#     #     flash("ETL Pipeline triggered successfully", "success")
#     #     return redirect(url_for("history"))

#     # return render_template("run_pipeline.html")
def run_pipeline():
    if request.method == "POST":

        file = request.form.get("file")
        finalFileName = request.form.get("finalFileName")

        if not file or file == "false":
            finalFileName = "test"

        initialLoad = request.form.get("initialLoad")

        SourceDbHost = request.form.get("dbHost1")
        SourcePort = request.form.get("port1")
        SourceserviceName = request.form.get("serviceName1")
        SourceuserName = request.form.get("userName1")
        SourcePassword = request.form.get("password1")

        targetDbHost = request.form.get("dbHost2")
        targetPort = request.form.get("port2")
        targetserviceName = request.form.get("serviceName2")
        targetuserName = request.form.get("userName2")
        targetPassword = request.form.get("password2")

        selectColumns = request.form.get("selectColumns")
        sourceTableName = request.form.get("sourceTableName")

        incrementalCheck = request.form.get("incrementalCheck")
        tsCheckColumn = request.form.get("tsCheckColumn")

        if incrementalCheck != "true":
            tsCheckColumn = "testxxx"

        filterCheck = request.form.get("filterCheck")
        keepOnlyColumnsFromSource = request.form.get("keepOnlyColumnsFromSource")

        if filterCheck != "true":
            keepOnlyColumnsFromSource = selectColumns

        renameCheck = request.form.get("renameCheck")
        renameFrom = request.form.get("renameFrom")
        renameTo = request.form.get("renameTo")

        if renameCheck != "true":
            renameFrom = selectColumns
            renameTo = selectColumns

        insertColumns = request.form.get("insertColumns")
        insertTableName = request.form.get("insertTableName")

        emaildistributionlist = request.form.get("emaildistributionlist")
        if not emaildistributionlist:
            emaildistributionlist = "shankar@gmail.com"

        result = mainApp.RunETLMain(
            file,
            finalFileName,
            initialLoad,
            SourceDbHost,
            SourcePort,
            SourceserviceName,
            SourceuserName,
            SourcePassword,
            targetDbHost,
            targetPort,
            targetserviceName,
            targetuserName,
            targetPassword,
            selectColumns,
            sourceTableName,
            incrementalCheck,
            tsCheckColumn,
            filterCheck,
            keepOnlyColumnsFromSource,
            renameCheck,
            renameFrom,
            renameTo,
            insertColumns,
            insertTableName,
            emaildistributionlist,
            session.get("username")
        )

        flash("ETL Pipeline triggered successfully", "success")


        return redirect(url_for("history"))

    return render_template("run_pipeline.html")


@app.route("/history")
@login_required
def history():
    # return render_template("history.html", history=PIPELINE_HISTORY)
    header, rows = historyPipeline.readPipelineHistory()
    return render_template(
        "history.html",
        header=header,
        rows=rows
    )


# ---------------- ADMIN ONLY ----------------
@app.route("/add-user", methods=["GET", "POST"])
@login_required
@admin_required
def add_user():
    if request.method == "POST":
        username = request.form.get("username")
        password = request.form.get("password")
        email_id = request.form.get("email_id")
        full_name = request.form.get("full_name")

        result = userManagement.addUser(username, password, email_id, full_name)

        if result == "User added successfully":
            flash(result, "success")
        else:
            flash(result, "error")

    return render_template("add_user.html")


# @app.route("/delete-user")
# @login_required
# @admin_required
# def delete_user():
#     # return render_template("delete_user.html")
@app.route("/delete-user", methods=["GET", "POST"])
@login_required
@admin_required
def delete_user():
    if request.method == "POST":
        username = request.form.get("username")
        email_id = request.form.get("email_id")
        full_name = request.form.get("full_name")

        result = userManagement.deleteUser(username, email_id, full_name)

        if result == "User deleted successfully":
            flash(result, "success")
        else:
            flash(result, "error")

    return render_template("delete_user.html")


# ---------------- ERROR HANDLER ----------------
@app.errorhandler(403)
def forbidden(e):
    return "<h3>403 Forbidden: Admin access required</h3>", 403


# ---------------- MAIN ----------------
if __name__ == "__main__":
    startmessage = intro.intro()
    logging.startinglogger(startmessage)
    app.run(debug=True)

