from datetime import datetime
import json
import logging
import os
import sys
import traceback

from atlassian import Jira
import pandas as pd
from sqlsorcery import MSSQL

from mailer import Mailer
from timer import elapsed


def configure_logging():
    logging.basicConfig(
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(filename="app.log", mode="w+"),
        ],
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %I:%M:%S%p %Z",
    )


def get_nested(issue, field, subfield):
    if not issue:
        return
    if issue.get(field):
        return issue.get(field).get(subfield)


def format_record(issue):
    fields = issue.get("fields")
    return {
        "id": issue.get("id"),
        "key": issue.get("key"),
        "issue_type": get_nested(fields, "issuetype", "name"),
        "parent": get_nested(fields, "parent", "key"),
        "status": get_nested(fields, "status", "name"),
        "priority": get_nested(fields, "priority", "name"),
        "estimate": fields.get("customfield_10015"),
        "summary": fields.get("summary"),
        "strategic_goal": get_nested(fields, "customfield_10040", "value"),
        "assignee": get_nested(fields, "assignee", "displayName"),
        "start_date": fields.get("customfield_10014"),
        "due_date": fields.get("duedate"),
        "created": fields.get("created"),
        "updated": fields.get("updated"),
    }


def get_all_issues(jira, project, fields):
    issues = []
    start = 0
    while True:
        data = jira.jql(
            f"project={project} & updated >= -1d",
            start=start,
            limit=100,
            fields=fields,
        )
        records = [format_record(issue) for issue in data["issues"]]
        issues.extend(records)
        start = len(issues)
        if start >= data["total"]:
            break

    return issues


@elapsed
def main():
    configure_logging()
    sql = MSSQL()
    url = f'https://{os.getenv("JIRA_URL")}.atlassian.net'
    username = os.getenv("JIRA_USER")
    password = os.getenv("JIRA_TOKEN")
    jira = Jira(url=url, username=username, password=password,)
    fields = [
        "issuetype",
        "parent",
        "summary",
        "assignee",
        "created",
        "priority",
        "updated",
        "status",
        "customfield_10040",  # strategic_goal
        "customfield_10014",  # start date
        "customfield_10015",  # story points
        "duedate",
    ]
    dates = ["start_date", "due_date", "created", "updated"]
    issues = get_all_issues(jira, "PROJ21", fields)
    if issues:
        new = pd.DataFrame(issues)
        new = new.astype({col: "datetime64[ns]" for col in dates})
        table_name = "jira_Issues"
        old = pd.read_sql_table(table_name, con=sql.engine, schema=sql.schema)
        df = pd.concat([old, new])
        df.drop_duplicates(keep="last", inplace=True, subset=["id"])
        sql.insert_into(table_name, df, if_exists="replace")
        logging.info(f"Inserted {len(df)} rows into {table_name}")


if __name__ == "__main__":
    try:
        main()
        error_message = None
    except Exception as e:
        logging.exception(e)
        error_message = traceback.format_exc()
    if int(os.getenv("ENABLE_MAILER")):
        Mailer("Jira Connector").notify(error_message=error_message)
