# jira
Data pipeline for Jira issue data used to project plan

## Dependencies:

- Python3.7
- [Pipenv](https://pipenv.readthedocs.io/en/latest/)
- [Docker](https://www.docker.com/)

## Getting Started

### Setup Environment

1. Clone this repo

```
git clone https://github.com/kippnorcal/jira.git
```

2. Install dependencies

- Docker can be installed directly from the website at docker.com.

3. Create .env file with project secrets

```
# API Credentials
JIRA_URL=
JIRA_USER=
JIRA_TOKEN=

# Database Credentials
DB_SERVER=
DB=
DB_SCHEMA=
DB_USER=
DB_PWD=

# Email Credentials (Optional)
ENABLE_MAILER=
SENDER_EMAIL=
SENDER_PWD=
RECIPIENT_EMAIL=
```

4. Build the container

```
$ docker build -t jira .
```


5. Running the job

```
$ docker run --rm -it jira
```
