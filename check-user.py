import os
import ydb
import json

# Create driver in global space.
driver = ydb.Driver(endpoint=os.getenv('YDB_ENDPOINT'), database=os.getenv('YDB_DATABASE'))
# Wait for the driver to become active for requests.
driver.wait(fail_fast=True, timeout=5)
# Create the session pool instance to manage YDB sessions.
pool = ydb.SessionPool(driver)


def execute_query(pool, userId):
  def callee(session):
    return session.transaction().execute(
      'select count(*)>0 from users where userId = '+str(userId),
      commit_tx=True,
      settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
    )
  return pool.retry_operation_sync(callee)


def handler(event, context):
  userId = event['queryStringParameters']['id']
  # Execute query with the retry_operation helper.
  with ydb.SessionPool(driver) as pool:
    result = execute_query(pool, userId)
  return {
  'statusCode': 200,
  'body': str(result[0].rows[0][0]),
}