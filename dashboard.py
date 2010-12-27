import os
import tornado.httpserver
import tornado.ioloop
import tornado.web

from connections import PostgreSQLConnection

# single row
def get_unique_contributors():
  db_conn = PostgreSQLConnection('jira_fetcher', 'hammer')
  sql = "SELECT COUNT(assignee), COUNT(DISTINCT assignee) FROM fixed_issues WHERE updated >= '2010-10-01';"
  results = db_conn.fetch_sql(sql)
  db_conn.close()
  return results[0]

# multiple rows
def get_unique_contributors_per_project():
  db_conn = PostgreSQLConnection('jira_fetcher', 'hammer')
  sql = "SELECT project, COUNT(assignee) total_issues, COUNT(DISTINCT assignee) unique_contributors FROM fixed_issues WHERE updated >= '2010-10-01' GROUP BY project;"
  results = db_conn.fetch_sql(sql)
  db_conn.close()
  return results

def get_fixed_issues():
  db_conn = PostgreSQLConnection('jira_fetcher', 'hammer')
  sql = "SELECT project, key, assignee, updated FROM fixed_issues WHERE updated >= '2010-10-01';"
  results = db_conn.fetch_sql(sql)
  db_conn.close()
  return results

class Application(tornado.web.Application):
  def __init__(self):
    handlers = [
      (r"/", MainHandler),
    ]
    settings = dict(
      template_path=os.path.join(os.path.dirname(__file__), "templates"),
      static_path=os.path.join(os.path.dirname(__file__), "static"),
    )
    tornado.web.Application.__init__(self, handlers, **settings)

class MainHandler(tornado.web.RequestHandler):
  def get(self):
    unique_contributors = get_unique_contributors()
    unique_contributors_per_project = get_unique_contributors_per_project()
    fixed_issues = get_fixed_issues()
    self.render("index.html",
                unique_contributors=unique_contributors,
                unique_contributors_per_project=unique_contributors_per_project,
                fixed_issues=fixed_issues)

def main():
  http_server = tornado.httpserver.HTTPServer(Application())
  http_server.listen(8888)
  tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
  main()

