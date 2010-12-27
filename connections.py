import ConfigParser
import os
import psycopg2
from suds.client import Client as suds_client

class JiraConnection(object):
  def __init__(self, url, username, password):
    self._url = url
    self._username = username
    self._password = password
    self._client = None
    self._auth = None

  @staticmethod
  def make_connection_from_config(name):
    config = ConfigParser.ConfigParser()
    config.readfp(open(os.path.expanduser('~/.jira_fetcher')))

    return JiraConnection(config.get(name, 'soap_endpoint'),
                          config.get(name, 'username'),
                          config.get(name, 'password'))

  @property
  def client(self):
    if not self._client:
      self._client = suds_client(self._url)
    return self._client

  @property
  def auth(self):
    if not self._auth:
      self._auth = self.client.service.login(self._username, self._password)
    return self._auth

  def get_issues_from_filter(self, filter):
    return self.client.service.getIssuesFromFilterWithLimit(self.auth, filter, 0, 100000)

  def get_issues_from_jql(self, query):
    return self.client.service.getIssuesFromJqlSearch(self.auth, query, 1000000)

  def get_user(self, user):
    return self.client.service.getUser(self.auth, user)

  def close(self):
    self.client.service.logout(self.auth)


class PostgreSQLConnection(object):
  def __init__(self, db, username):
    self._db = db
    self._username = username
    self._conn = None

  @property
  def conn(self):
    if not self._conn:
      self._conn = psycopg2.connect("dbname=jira_fetcher user=hammer")
    return self._conn

  @staticmethod
  def make_connection_from_config(name):
    config = ConfigParser.ConfigParser()
    config.readfp(open(os.path.expanduser('~/.jira_fetcher')))

    return PostgreSQLConnection(config.get(name, 'db'),
                          config.get(name, 'username'))
  
  def execute_sql(self, sql, params=None):
    cur = self.conn.cursor()
    cur.execute(sql, params)
    self.conn.commit()
    cur.close()

  def fetch_sql(self, sql):
    cur = self.conn.cursor()
    cur.execute(sql)
    results = cur.fetchall()
    cur.close()
    return results

  def close(self):
    self.conn.close()
