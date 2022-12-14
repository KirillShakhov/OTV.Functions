import os
import ydb
import json
from datetime import datetime
import uuid
import math

# Create driver in global space.
driver = ydb.Driver(endpoint=os.getenv('YDB_ENDPOINT'), database=os.getenv('YDB_DATABASE'))
# Wait for the driver to become active for requests.
driver.wait(fail_fast=True, timeout=5)
# Create the session pool instance to manage YDB sessions.
pool = ydb.SessionPool(driver)

SQL_EDIT_COUNT = 'UPDATE stock_products SET count=Cast((count - %count%) as Uint32) WHERE product_id = %product_id% AND stock_id = %stock_id%'
SQL_ADD_ORDER = 'insert into orders (id,coordinate,courier_id,price,product_id,stock_id,user_id) ' \
                'VALUES ("%id%",CAST(@@%coordinate%@@ AS Json),%courier_id%,%price%,%product_id%,%stock_id%,%user_id%)'


def edit_stock(pool, stockId, productId, count):
    def callee(session):
        return session.transaction().execute(
            SQL_EDIT_COUNT
            .replace('%count%', str(count))
            .replace('%product_id%', str(productId))
            .replace('%stock_id%', str(stockId)),
            commit_tx=True,
            settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
        )

    return pool.retry_operation_sync(callee)


def get_product_price(pool, productId):
    def callee(session):
        return session.transaction().execute(
            'select price from products where id=' + productId,
            commit_tx=True,
            settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
        )

    return pool.retry_operation_sync(callee)


def get_couriers(pool):
  def callee(session):
    return session.transaction().execute(
      'select * from couriers',
      commit_tx=True,
      settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
    )

  return pool.retry_operation_sync(callee)


def create_order(pool, coordinate, courierId, price, productId, stockId, userId):
    def callee(session):
        return session.transaction().execute(
            SQL_ADD_ORDER
            .replace('%id%', str(uuid.uuid1()))
            .replace('%coordinate%', str(coordinate))
            .replace('%courier_id%', str(courierId))
            .replace('%price%', str(price))
            .replace('%product_id%', str(productId))
            .replace('%stock_id%', str(stockId))
            .replace('%user_id%', str(userId)),
            commit_tx=True,
            settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
        )

    return pool.retry_operation_sync(callee)

def handler(event, context):
    productId = str(event['queryStringParameters']['productId'])

    with ydb.SessionPool(driver) as pool:
        result = get_product_price(pool, productId)
    price = result[0].rows[0].price

    stockId = str(event['queryStringParameters']['stockId'])
    with ydb.SessionPool(driver) as pool:
        edit_stock(pool, stockId, productId, 1)

    with ydb.SessionPool(driver) as pool:
        result = get_couriers(pool)

    coordinate = str(event['queryStringParameters']['coordinate'])
    json_my_coord = json.loads(str(coordinate.replace("': b'", "': '").replace("'", '"')))

    minId = -1
    minDist = 100000000
    for c in result[0].rows:
        json_coord = json.loads(str(c['work_location'].replace("': b'", "': '").replace("'", '"')))
        dist = math.hypot(json_coord['x'] - json_my_coord['x'], json_coord['y'] - json_my_coord['y'])
        if minDist > dist:
            minId = c['user_id']
            minDist = dist

    userId = str(event['queryStringParameters']['userId'])

    if minId == -1:
        return {
            'statusCode': 404,
            'body': 'error',
        }

    with ydb.SessionPool(driver) as pool:
        result = create_order(pool, coordinate, minId, price, productId, stockId, userId)

    return {
        'statusCode': 200,
        'body': minId,
    }
