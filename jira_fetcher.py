import ConfigParser
import datetime
import os
from suds.client import Client as suds_client

PROJECTS = [('Avro', 12315050),
            ('Hadoop', 12315051),
            ('HDFS', 12315052),
            ('MapReduce', 12315053),
            ('HBase', 12315054),
            ('Hive', 12315055),
            ('Pig', 12315056),
            ('Whirr', 12315057),
            ('ZooKeeper', 12315058),
            ('Flume', None),
            ('Sqoop', None),
            ('Hue', None),
           ]

JQL = 'project = %s AND status in (Closed, Resolved) AND resolution = Fixed AND updated > "2010-10-01"'

# TODO(hammer): memoize/make singleton
def get_client_and_auth(jira):
  config = ConfigParser.ConfigParser()
  config.readfp(open(os.path.expanduser('~/.jira_fetcher')))

  client = suds_client(config.get(jira, 'soap_endpoint'))
  auth = client.service.login(config.get(jira, 'username'), config.get(jira, 'password'))

  return client, auth


def make_issues_string(issues):
  return '\n'.join(['\t'.join([issue.project,
                               issue.id,
                               issue.key,
                               str(issue.assignee),
                               str(issue.reporter),
                               issue.created.strftime('%m-%d-%Y'),
                               issue.updated.strftime('%m-%d-%Y')])
                    for issue in issues])

def make_users_string(users_info):
  return '\n'.join(['\t'.join(user_info) for user_info in users_info])


def write_tsv(name, content, debug = False):
  ofilename = '%s%s.tsv' % (datetime.datetime.now().strftime('%Y%m%d'), name)
  ofile = open(ofilename, 'w')
  ofile.write(content)
  ofile.close()
  if debug: print "Wrote %s" % ofilename

# TODO(hammer): Deal with fetch errors
# TODO(hammer): Get watcher information for JIRA tickets
# TODO(hammer): Standardize the execution of derivative fetches (like the user and watcher fetches)
if __name__ == '__main__':
  cloudera_client, cloudera_auth = get_client_and_auth('CLOUDERA')
  asf_client, asf_auth = get_client_and_auth('ASF')

  users = set()

  for project, filter in PROJECTS:
    if filter:
      issues = asf_client.service.getIssuesFromFilterWithLimit(asf_auth, filter, 0, 100000)
    else:
      issues = cloudera_client.service.getIssuesFromJqlSearch(cloudera_auth, JQL % project, 1000000)
    print "Fetched %s issues for project %s" % (len(issues), project)
    write_tsv(project, make_issues_string(issues), True)
    users.update([issue.assignee for issue in issues if issue.assignee is not None])
    users.update([issue.reporter for issue in issues if issue.reporter is not None])

  write_tsv('users', '\n'.join(list(users)), True)

  users_info = []
  for user in users:
    user_info = asf_client.service.getUser(asf_auth, user)
    print user_info
    if user_info:
      users_info.append([user_info.name, user_info.fullname, user_info.email])
  write_tsv('users_info', make_users_string(users_info), True)

  asf_client.service.logout(auth)
  cloudera_client.service.logout(auth)

# TODO(hammer): load into PostgreSQL
# CREATE TABLE fixed_issues (project text, id int, key text, assignee text, reporter text, created date, updated date);
# CREATE TABLE contributors (asf_name text, asf_fullname text, asf_email text, organization text);
# COPY fixed_issues FROM '/path/to/project.tsv';

# TODO(hammer): generate some summaries
# Unique contributors: SELECT COUNT(assignee), COUNT(DISTINCT assignee) FROM fixed_issues;
# Unique contributors per project: SELECT project, COUNT(assignee) total_issues, COUNT(DISTINCT assignee) unique_contributors FROM fixed_issues GROUP BY project;
