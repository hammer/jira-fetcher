import ConfigParser
import datetime
import psycopg2
import os
from suds.client import Client as suds_client

DEBUG = True
WORKING_DIR = '/tmp'

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


class DataSource(object):
  def __init__(self, name):
    self._name = name

  name = property(lambda self: self._name)
  ofilename = property(lambda self: datetime.datetime.now().strftime('%Y%m%d') + self._name + '.dat')


class FixedIssuesSource(DataSource):
  JQL = 'project = %s AND status in (Closed, Resolved) AND resolution = Fixed AND updated > "%s"'

  def __init__(self, jira_connection, project, date_start):
    super(FixedIssuesSource, self).__init__(project)
    self._jira_connection = jira_connection
    self._date_start = date_start

  def fetch_data(self):
    return self._jira_connection.get_issues_from_jql(self.JQL % (self.name, self._date_start))


class FilterSource(DataSource):
  def __init__(self, jira_connection, project, filter):
    super(FilterSource, self).__init__(project)
    self._jira_connection = jira_connection
    self._filter = filter

  def fetch_data(self):
    return self._jira_connection.get_issues_from_filter(self._filter)


class IssuesTransform(object):
  field_delimiter = '\t'
  row_delimiter = '\n'

  @staticmethod
  def transform_issue(issue):
    new_assignee = str(issue.assignee)
    new_reporter = str(issue.reporter)
    new_created = issue.created.strftime('%Y-%m-%d')
    new_updated = issue.updated.strftime('%Y-%m-%d')
    return [issue.project, issue.id, issue.key, new_assignee, new_reporter, new_created, new_updated]

  @staticmethod
  def transform_issues(issues):
    transformed_issues = []
    failed_issues = []
    for issue in issues:
      try:
        transformed_issues.append(IssuesTransform.transform_issue(issue))
      except:
        failed_issues.append(issue)
    return transformed_issues

  @staticmethod
  def serialize_issues(issues):
    return IssuesTransform.row_delimiter.join([IssuesTransform.field_delimiter.join(issue) for issue in issues])


def write_tsv(ofilename, content):
  ofile = open(os.path.join(WORKING_DIR, ofilename), 'w')
  ofile.write(content)
  ofile.close()
  if DEBUG: print "Wrote %s" % ofilename

def fetch_fixed_issues():
  # Setup
  start_date = '2010-10-01'
  asf_jira = JiraConnection.make_connection_from_config('ASF')
  cloudera_jira = JiraConnection.make_connection_from_config('CLOUDERA')
  output_db = PostgreSQLConnection('jira_fetcher', 'hammer')

  sources = [FilterSource(asf_jira, 'Avro', 12315050),
             FilterSource(asf_jira, 'Hadoop', 12315051),
             FilterSource(asf_jira, 'HDFS', 12315052),
             FilterSource(asf_jira, 'MapReduce', 12315053),
             FilterSource(asf_jira, 'HBase', 12315054),
             FilterSource(asf_jira, 'Hive', 12315055),
             FilterSource(asf_jira, 'Pig', 12315056),
             FilterSource(asf_jira, 'Whirr', 12315057),
             FilterSource(asf_jira, 'ZooKeeper', 12315058),
             FixedIssuesSource(cloudera_jira, 'Flume', start_date),
             FixedIssuesSource(cloudera_jira, 'Sqoop', start_date),
             FixedIssuesSource(cloudera_jira, 'Hue', start_date),
            ]

  for source in sources:
    if DEBUG: print "Begin processing source %s" % source.name

    # EXTRACT
    issues = source.fetch_data()
    if DEBUG: print "Fetched %s issues for project %s" % (len(issues), source.name)

    # TRANSFORM
    transformed_issues = IssuesTransform.transform_issues(issues)
    if DEBUG: print "Transformed %s issues successfully for project %s" % (len(transformed_issues), source.name)    
    serialized_issues = IssuesTransform.serialize_issues(transformed_issues)
    if DEBUG: print "Serialized issues: %s" % serialized_issues
    write_tsv(source.ofilename, serialized_issues)

    # LOAD
    output_db.execute_sql("COPY fixed_issues FROM '%s';" % os.path.join(WORKING_DIR, source.ofilename))
    if DEBUG: print "Loaded data for source %s" % source.name

  # Teardown
  asf_jira.close()
  cloudera_jira.close()
  output_db.close()


def fetch_users():
  # Setup
  asf_jira = JiraConnection.make_connection_from_config('ASF')
  db_conn = PostgreSQLConnection('jira_fetcher', 'hammer')
  asf_usernames = [row[0] for row in db_conn.fetch_sql('SELECT DISTINCT(assignee) FROM fixed_issues;')]
  
  for name in asf_usernames:
    # EXTRACT
    if DEBUG: print "Begin processing name %s" % name
    user_info = asf_jira.get_user(name)

    # TRANSFORM
    if user_info:
      row_info = {'asf_name': user_info.name,
                  'asf_fullname': user_info.fullname,
                  'asf_email': user_info.email,
                  'asf_email_domain': user_info.email.split('@')[1]}
    else:
      print "No results from ASF JIRA for %s" % name

    # LOAD
    try:
      sql = """\
INSERT INTO contributors (asf_name, asf_fullname, asf_email, asf_email_domain)
VALUES (%(asf_name)s, %(asf_fullname)s, %(asf_email)s, %(asf_email_domain)s)
"""
      db_conn.execute_sql(sql, row_info)
      if DEBUG: print "Successful insertion for %s" % name
    except Exception, e:
      print "Problem inserting %s:" % (name, e)

  # Teardown
  asf_jira.close()
  db_conn.close()


if __name__ == '__main__':
  fetch_fixed_issues()
  fetch_users()

