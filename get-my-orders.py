import os
import ydb
import json

# Create driver in global space.
driver = ydb.Driver(endpoint=os.getenv('YDB_ENDPOINT'), database=os.getenv('YDB_DATABASE'))
# Wait for the driver to become active for requests.
driver.wait(fail_fast=True, timeout=5)
# Create the session pool instance to manage YDB sessions.
pool = ydb.SessionPool(driver)

SQL_GET_MY_ORDERS = 'select coordinate,price,product_id,stock_id from orders where courier_id=%courier_id%'

def execute_query(pool, courierId):
    def callee(session):
        return session.transaction().execute(
            SQL_GET_MY_ORDERS
            .replace('%courier_id%', courierId),
            commit_tx=True,
            settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
        )
    return pool.retry_operation_sync(callee)

def handler(event, context):
  # Execute query with the retry_operation helper.
  courierId = event['queryStringParameters']['id']
  with ydb.SessionPool(driver) as pool:
      result = execute_query(pool, courierId)

  return {
    'statusCode': 200,
    'body': str(result[0].rows),
  }