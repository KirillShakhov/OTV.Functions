import os
import ydb
import json
from datetime import datetime

# Create driver in global space.
driver = ydb.Driver(endpoint=os.getenv('YDB_ENDPOINT'), database=os.getenv('YDB_DATABASE'))
# Wait for the driver to become active for requests.
driver.wait(fail_fast=True, timeout=5)
# Create the session pool instance to manage YDB sessions.
pool = ydb.SessionPool(driver)


def execute_query(pool, userId, email, name):
  def callee(session):
    current_date = datetime.today().strftime('%Y-%m-%d')
    return session.transaction().execute(
      'insert into users (userId,createDate,email,name) VALUES ('+userId+',CAST("'+str(current_date)+'" AS Date),"'+email+'","'+name+'")',
      commit_tx=True,
      settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
    )
  return pool.retry_operation_sync(callee)


def handler(event, context):
  userId = event['queryStringParameters']['id']
  email = str(event['queryStringParameters']['email'])
  name = str(event['queryStringParameters']['name'])
  # Execute query with the retry_operation helper.
  with ydb.SessionPool(driver) as pool:
    result = execute_query(pool, userId, email, name)
  return {
  'statusCode': 200,
  'body': 'ok',
}