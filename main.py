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
        """
        Simple function to add a consistent prefix to table names.
        """
        return f"jira_{name}"

    def get_projects(self):
        """
        Extract project data from Jira and load into database table.
        """
        table = self.table_name("projects")
        columns = {
            "id": "id",
            "key": "project_key",
            "name": "name",
            "projectTypeKey": "project_type",
            "style": "style",
            "isPrivate": "isPrivate",
            "projectCategory_name": "category",
        }
        projects = self.jira.get_all_projects()
        df = pd.json_normalize(projects, sep="_", errors="ignore")
        df = df[columns.keys()]
        df.rename(columns=columns, inplace=True)
        self.sql.insert_into(table, df, if_exists="replace")
        logging.info(f"Loaded {len(projects)} projects into {table}")

    def get_boards(self):
        """
        Extract board data from Jira and load into database table. This
        is needed for querying sprints.
        """
        table = self.table_name("boards")
        columns = ["id", "name", "type", "location_projectId"]
        boards = self.jira.get_all_agile_boards()
        df = pd.json_normalize(boards["values"], sep="_", errors="ignore")
        df = df[columns]
        self.sql.insert_into(table, df, if_exists="replace")
        logging.info(f"Loaded {len(boards)} boards into {table}")

    def get_active_project_id(self):
        """
        Returns the ID of the active project to use for querying boards.
        """
        table = self.table_name("projects")
        df = pd.read_sql_table(table, con=self.sql.engine, schema=self.sql.schema)
        df = df[df["category"] == "Active"]
        project_id = df[["id"]].values.flatten().tolist()[0]
        return project_id

    def get_active_board_id(self):
        """
        Returns the ID of the board for the active project to use for
        querying issues.
        """
        table = self.table_name("boards")
        project_id = self.get_active_project_id()
        df = pd.read_sql_table(table, con=self.sql.engine, schema=self.sql.schema)
        df = df[df.location_projectId == float(project_id)]
        board_id = df[["id"]].values.flatten().tolist()[0]
        return board_id

    def get_sprints(self):
        """
        Extract sprint data from Jira and load into database table.
        """
        table = self.table_name("sprints")
        board_id = self.get_active_board_id()
        sprints = self.jira.get_all_sprint(board_id)
        dates = ["startDate", "endDate", "completeDate"]
        df = pd.json_normalize(sprints["values"], sep="_", errors="ignore")
        df.drop(["self"], axis=1, inplace=True)
        df = df.astype({col: "datetime64[ns]" for col in dates})
        self.sql.insert_into(table, df, if_exists="replace")
        logging.info(f"Loaded {len(df)} sprints into {table}")

    def get_sprint_ids(self, active=False):
        """
        Return a list of sprint IDs to use for querying issues. Optional
        param can be passed to only return active and future sprints.
        """
        table = self.table_name("sprints")
        df = pd.read_sql_table(table, con=self.sql.engine, schema=self.sql.schema)
        if active:
            df = df[df["state"] != "closed"]
        return df[["id"]].values.flatten().tolist()

    def get_sprint_issues(self, sprint_id):
        """
        Extract issue data for a given sprint from Jira and load into
        database table.
        """
        table = self.table_name("issues")
        columns = {
            "id": "id",
            "key": "issue_key",
            "fields_issuetype_name": "issue_type",
            "fields_project_id": "project",
            "fields_parent_key": "parent_key",
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

        if issues:
            df = pd.json_normalize(issues, sep="_", errors="ignore")
            df = df[columns.keys()]
            df.rename(columns=columns, inplace=True)
            df["created"] = pd.to_datetime(df["created"], utc=True)
            df["updated"] = pd.to_datetime(df["updated"], utc=True)
            df["sprint"] = sprint_id
            self.sql.insert_into(
                table, df, dtype={"created": DateTime, "updated": DateTime}
            )
            logging.info(
                f"Loaded {len(issues)} issues for sprint {sprint_id} into {table}"
            )

    def get_all_issues(self):
        """
        Loads issues for multiple sprints. On first loading it will pull
        all sprints. On subsequent runs, it will drop any issues for
        active and future sprints and re-query them.
        """
        table_name = self.table_name("issues")
        if self.table_exists(table_name):
            sprints = self.get_sprint_ids(active=True)
            self.delete_sprint_issues(sprints)
        else:
            sprints = self.get_sprint_ids()
        for sprint in sprints:
            self.get_sprint_issues(sprint)

    def table_exists(self, table_name):
        """
        Checks if a table already exists in the database. Used for
        determining what actions to take for loading depending on
        the existence of prior loaded data.
        """
        return self.sql.engine.dialect.has_table(
            connection=self.sql.engine, tablename=table_name, schema=self.sql.schema
        )

    def delete_sprint_issues(self, sprints):
        """
        Deletes records in the issues table for a given set of sprints.
        """
        table_name = self.table_name("issues")
        if self.table_exists(table_name):
            table = self.sql.table(table_name)
            d = table.delete().where(table.c.sprint.in_(sprints))
            self.sql.engine.execute(d)

    def get_issue_changes(self, issue_key, count, total):
        """
        Extract change history for a given issue by key. Expects params
        for a count and expected total to be passed in for debug logging.
        """
        table = self.table_name("issue_changes")

        changes = self.jira.get_issue_changelog(issue_key)
        if changes["histories"]:
            df = pd.json_normalize(
                changes["histories"],
                sep="_",
                record_path=["items"],
                meta=["id", "created", "author"],
                errors="ignore",
            )
            df["issue_key"] = issue_key
            if "author" in df.columns:
                df["author"] = df["author"].map(lambda a: a.get("displayName"))
            else:
                df["author"] = None
            if "created" in df.columns:
                df["created"] = pd.to_datetime(df["created"], utc=True)
            else:
                df["created"] = None
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
            logging.debug(
                f"Loaded {len(df)} changes for {issue_key} into {table} {count}/{total}"
            )

    def get_issue_keys(self, active=False):
        """
        Return a distinct list (set) of issue keys for use in querying
        the change history. If the active param is passed it will only
        pull issues in active or future sprints.
        """
        table = self.table_name("issues")
        df = pd.read_sql_table(table, con=self.sql.engine, schema=self.sql.schema)
        if active:
            active_sprints = self.get_sprint_ids(active=True)
            df = df[df["sprint"].isin(active_sprints)]
        return set(df[["issue_key"]].values.flatten().tolist())

    def get_issue_change_keys(self):
        """
        Queries a distinct list (set) of issue keys from the change
        history. Used to determine which changes to query for.
        """
        table = self.table_name("issue_changes")
        df = pd.read_sql_table(table, con=self.sql.engine, schema=self.sql.schema)
        return set(df[["issue_key"]].values.flatten().tolist())

    def get_issue_key_diff(self):
        """
        Returns the distinct set of issue keys that exist in the issue
        table but not in the change table.
        """
        table_name = self.table_name("issue_changes")
        issue_keys = self.get_issue_keys()
        if self.table_exists(table_name):
            issue_change_keys = self.get_issue_change_keys()
            return issue_keys - issue_change_keys
        else:
            return issue_keys

    def delete_issue_changes(self):
        """
        Delete records in the issue changes table that match a set of
        issue keys.
        """
        table_name = self.table_name("issue_changes")
        if self.table_exists(table_name):
            table = self.sql.table(table_name)
            keys = self.get_issue_keys(active=True)
            d = table.delete().where(table.c.issue_key.in_(keys))
            self.sql.engine.execute(d)

    def get_all_changes(self):
        """
        Extract issue changes and load in database table.
        """
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

    def get_parent_keys(self):
        """
        Return a list of parent issue keys to use for querying parent issues. O
        """
        issues_table = self.table_name("issues")
        issues = pd.read_sql_table(
            issues_table, con=self.sql.engine, schema=self.sql.schema
        )
        issue_parent_keys = set(issues["parent_key"].unique().tolist())
        issue_parent_keys.remove(None)

        parents_table = self.table_name("parent_issues")
        if self.table_exists(parents_table):
            parents = pd.read_sql_table(
                parents_table, con=self.sql.engine, schema=self.sql.schema
            )
            parent_keys = set(parents["issue_key"].unique().tolist())
            return issue_parent_keys - parent_keys
        else:
            return issue_parent_keys

    def get_parent_issues(self):
        """
        Extract issue data for parent keys (epics) to separate table
        for categorizing issues by parent.
        """
        table = self.table_name("parent_issues")
        columns = {
            "id": "id",
            "key": "issue_key",
            "fields_issuetype_name": "issue_type",
            "fields_project_id": "project",
            "fields_status_name": "status",
            "fields_summary": "summary",
            "fields_customfield_10042_value": "team",
            "fields_customfield_10038_value": "goal",
            "fields_assignee_displayName": "assignee",
            "fields_creator_displayName": "creator",
            "fields_customfield_10014": "start_date",
            "fields_duedate": "due_date",
            "fields_created": "created",
            "fields_updated": "updated",
        }
        issues = []
        start = 0

        while True:
            data = self.jira.jql("issuetype = 'project'", start=start)
            issues.extend(data["issues"])
            start = len(issues)

            if start >= data["total"]:
                break

        if issues:
            df = pd.json_normalize(issues, sep="_", errors="ignore")
            custom_fields = [k for k in columns.keys() if "customfield" in k]
            for field in custom_fields:
                df[field] = df.get(field)
            df = df[columns.keys()]
            df.rename(columns=columns, inplace=True)
            df["created"] = pd.to_datetime(df["created"], utc=True)
            df["updated"] = pd.to_datetime(df["updated"], utc=True)
            self.sql.insert_into(
                table,
                df,
                dtype={"created": DateTime, "updated": DateTime},
                if_exists="replace",
            )
            logging.info(f"Loaded {len(issues)} parent issues into {table}")


@elapsed
def main():
    configure_logging()
    connector = Connector()
    connector.get_projects()
    connector.get_boards()
    connector.get_sprints()
    connector.get_all_issues()
    connector.get_parent_issues()
    # connector.get_all_changes()


if __name__ == "__main__":
    try:
        main()
        error_message = None
    except Exception as e:
        logging.exception(e)
        error_message = traceback.format_exc()
    if int(os.getenv("ENABLE_MAILER")):
        Mailer("Jira Connector").notify(error_message=error_message)
