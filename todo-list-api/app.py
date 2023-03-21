from tornado.web import Application, RequestHandler
from tornado.ioloop import IOLoop
import tornado.web
import json
import asyncio
import uuid
import time
import redis
import mysql.connector


cache = redis.Redis(host='localhost', port=12345)

guid_db = mysql.connector.connect(
    host='localhost',
    user='root',
    password='password',
    database='guids',
)

class GUID:
    def __init__(self, id, expire, user):

        self._id = id
        self._expire = expire
        self._user = user
    
    def return_json(self):

        return {"guid": self._id,
                "expire": self._expire,
                "user": self._user}

class GUIDHandle(RequestHandler):
    def get(self, id):

        # First check if guid is in cache
        cached = cache.get(id)

        if (cached is not None):

            # The GUID is in the cache
            found = cached
        else:
            
            # Check database for GUID
            cursor = guid_db.cursor()
            cursor.execute("SELECT * FROM guids WHERE guid=%s", (id,))
            row = cursor.fetchone()

            if row is not None:
                found = GUID(row[0], int(row[1]), row[2])
                cache.set(id, found)


            else:
                raise tornado.web.HTTPError(404, 'GUID Not Found')
        # Return the json of the GUID
        self.write(found.return_json)
        
    def post(self, id=None):
        
        if id == None:
            id = str(uuid.uuid4()).replace('-', '').upper()
        
        input = json.loads(self.request.body)

        # Insert into db

        cursor = guid_db.cursor()
        cursor.execute("INSERT INTO guids VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE expire=%s, user=%s",
                       id, input.get('expire'), input.get('user'), input.get('expire'), input.get('user'))
        guid_db.commit()

        # Insert int cache
        guid = GUID(id, int(input.get('expire'), input.get('user')))


        self.write(guid.return_json)

    def delete(self, id):
        
        cursor = guid_db.cursor()
        cursor.execute("DELETE FROM guids WHERE guid=%s", (id,))
        guid_db.commit()

        cache.delete(id)

        self.set_status(204)


def make_app():
  urls = [(r"/guid/([^/]+)", GUIDHandle)]
  return Application(urls)
  
if __name__ == '__main__':
    app = make_app()
    app.listen(3000)
    IOLoop.instance().start()