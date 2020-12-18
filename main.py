from datetime import datetime
import json
import logging
import os
import sys
import traceback

from atlassian import Jira
import pandas as pd
from sqlsorcery import MSSQL
from sqlalchemy.types import DateTime

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


class Connector:
    def __init__(self):
        self.sql = MSSQL()
        url = f'https://{os.getenv("JIRA_URL")}.atlassian.net'
        username = os.getenv("JIRA_USER")
        password = os.getenv("JIRA_TOKEN")
        self.jira = Jira(url=url, username=username, password=password)

    def table_name(self, name):
        return f"jira_{name}"

    def get_projects(self):
        table = self.table_name("projects")
        columns = ["id", "key", "name", "projectTypeKey", "style", "isPrivate"]
        projects = self.jira.get_all_projects()
        df = pd.json_normalize(projects, sep="_", errors="ignore")
        df = df[columns]
        self.sql.insert_into(table, df, if_exists="replace")
        logging.info(f"Loaded {len(projects)} projects into {table}")

    def get_boards(self):
        table = self.table_name("boards")
        columns = ["id", "name", "type", "location_projectId"]
        boards = self.jira.get_all_agile_boards()
        df = pd.json_normalize(boards["values"], sep="_", errors="ignore")
        df = df[columns]
        self.sql.insert_into(table, df, if_exists="replace")
        logging.info(f"Loaded {len(boards)} boards into {table}")

    def get_sprints(self):
        table = self.table_name("sprints")
        sprints = self.jira.get_all_sprint("10")  # TODO: Get this board_id dynamically
        dates = ["startDate", "endDate", "completeDate"]
        df = pd.json_normalize(sprints["values"], sep="_", errors="ignore")
        df.drop(["self"], axis=1, inplace=True)
        df = df.astype({col: "datetime64[ns]" for col in dates})
        self.sql.insert_into(table, df, if_exists="replace")
        logging.info(f"Loaded {len(sprints)} sprints into {table}")

    def get_sprint_ids(self, active=False):
        table = self.table_name("sprints")
        df = pd.read_sql_table(table, con=self.sql.engine, schema=self.sql.schema)
        if active:
            df = df[df["state"] == "active"]
        else:
            df = df[df["state"] != "future"]
        return df[["id"]].values.flatten().tolist()

    def get_active_sprint(self):
        table = self.table_name("sprints")
        df = pd.read_sql_table(table, con=self.sql.engine, schema=self.sql.schema)
        df = df[df["state"] == "active"]
        return df[["id"]].values.flatten().tolist()[0]

    def get_sprint_issues(self, sprint_id):
        table = self.table_name("issues")
        columns = {
            "id": "id",
            "key": "key",
            "fields_issuetype_name": "issue_type",
            "fields_project_id": "project",
            "fields_epic": "epic",
            "fields_status_name": "status",
            "fields_priority_name": "priority",
            "fields_customfield_10015": "estimate",
            "fields_summary": "summary",
            "fields_assignee_displayName": "assignee",
            "fields_creator_displayName": "creator",
            "fields_duedate": "due_date",
            "fields_created": "created",
            "fields_updated": "updated",
        }
        issues = []
        start = 0
        while True:
            data = self.jira.get_sprint_issues(
                sprint_id=sprint_id, start=start, limit=100
            )
            issues.extend(data["issues"])
            start = len(issues)
            if start >= data["total"]:
                break
        df = pd.json_normalize(issues, sep="_", errors="ignore")
        df = df[columns.keys()]
        df.rename(columns=columns, inplace=True)
        df["created"] = pd.to_datetime(df["created"], utc=True)
        df["updated"] = pd.to_datetime(df["updated"], utc=True)
        df["sprint"] = sprint_id
        self.sql.insert_into(
            table, df, dtype={"created": DateTime, "updated": DateTime}
        )
        logging.info(f"Loaded {len(issues)} issues for sprint {sprint_id} into {table}")

    def get_all_issues(self):
        table_name = self.table_name("issues")
        if self.table_exists(table_name):
            sprints = self.get_sprint_ids(active=True)
            self.delete_sprint_issues(sprints[0])
        else:
            sprints = self.get_sprint_ids()
        for sprint in sprints:
            self.get_sprint_issues(sprint)

    def table_exists(self, table_name):
        return self.sql.engine.dialect.has_table(
            connection=self.sql.engine, tablename=table_name, schema=self.sql.schema
        )

    def delete_sprint_issues(self, sprint):
        table_name = self.table_name("issues")
        if self.table_exists(table_name):
            table = self.sql.table(table_name)
            d = table.delete().where(table.c.sprint == sprint)
            self.sql.engine.execute(d)

    def get_issue_changes(self, issue_key, count, total):
        table = self.table_name("issue_changes")

        changes = self.jira.get_issue_changelog(issue_key)
        if changes:
            df = pd.json_normalize(
                changes["histories"],
                sep="_",
                record_path=["items"],
                meta=["id", "created", "author"],
                errors="ignore",
            )
            df["issue_key"] = issue_key
            df["author"] = df["author"].map(lambda a: a.get("displayName"))
            df["created"] = pd.to_datetime(df["created"], utc=True)
            columns = [
                "issue_key",
                "id",
                "created",
                "author",
                "field",
                "fieldtype",
                "fromString",
                "toString",
            ]
            df = df[columns]
            # TODO: check if id in issue_changes, drop from df if already exists (only load new ones)
            self.sql.insert_into(table, df, dtype={"created": DateTime})
            logging.info(
                f"Loaded {len(df)} changes for {issue_key} into {table} {count}/{total}"
            )

    def get_issue_keys(self, active=False):
        table = self.table_name("issues")
        df = pd.read_sql_table(table, con=self.sql.engine, schema=self.sql.schema)
        if active:
            active_sprint = self.get_sprint_ids(active=True)[0]
            df = df[df["sprint"] == active_sprint]
        return set(df[["key"]].values.flatten().tolist())

    def get_issue_change_keys(self):
        table = self.table_name("issue_changes")
        df = pd.read_sql_table(table, con=self.sql.engine, schema=self.sql.schema)
        return set(df[["issue_key"]].values.flatten().tolist())

    def get_issue_key_diff(self):
        issue_keys = self.get_issue_keys()
        issue_change_keys = self.get_issue_change_keys()
        return issue_keys - issue_change_keys

    def delete_issue_changes(self):
        table_name = self.table_name("issue_changes")
        if self.table_exists(table_name):
            table = self.sql.table(table_name)
            keys = self.get_issue_keys(active=True)
            d = table.delete().where(table.c.issue_key.in_(keys))
            self.sql.engine.execute(d)

    def get_all_changes(self):
        # drop matching keys from tables
        self.delete_issue_changes()
        # query issue keys in issues but not in issue_changes
        keys = self.get_issue_key_diff()
        total = len(keys)

        for index, key in enumerate(keys):
            try:
                self.get_issue_changes(key, index + 1, total)
            except Exception as e:
                print(e)
                print(key)


@elapsed
def main():
    configure_logging()
    connector = Connector()
    connector.get_projects()
    connector.get_boards()
    connector.get_sprints()
    connector.get_all_issues()
    connector.get_all_changes()


if __name__ == "__main__":
    try:
        main()
        error_message = None
    except Exception as e:
        logging.exception(e)
        error_message = traceback.format_exc()
    if int(os.getenv("ENABLE_MAILER")):
        Mailer("Jira Connector").notify(error_message=error_message)
