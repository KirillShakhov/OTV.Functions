import os
import ydb

# Create driver in global space.
driver = ydb.Driver(endpoint=os.getenv('YDB_ENDPOINT'), database=os.getenv('YDB_DATABASE'))
# Wait for the driver to become active for requests.
driver.wait(fail_fast=True, timeout=5)
# Create the session pool instance to manage YDB sessions.
pool = ydb.SessionPool(driver)

SQL_CREATE_COURIER = 'insert into couriers (user_id,work_location) VALUES (%user_id%,CAST(@@%work_location%@@ AS Json))'

def execute_query(pool, userId, workLocation):
  def callee(session):
    return session.transaction().execute(
      SQL_CREATE_COURIER
      .replace('%user_id%', userId)
      .replace('%work_location%', str(workLocation)),
      commit_tx=True,
      settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
    )
  return pool.retry_operation_sync(callee)


def handler(event, context):
  userId = event['queryStringParameters']['id']
  workLocation = str(event['queryStringParameters']['location'])
  # Execute query with the retry_operation helper.
  with ydb.SessionPool(driver) as pool:
    result = execute_query(pool, userId, workLocation)
  return {
  'statusCode': 200,
  'body': 'ok',
}