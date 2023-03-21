from tornado.web import Application, RequestHandler
from tornado.ioloop import IOLoop
import tornado.web
import json
import asyncio
import uuid
import time
import redis
import mysql.connector


cache = redis.Redis(host='localhost', port=6379)

guid_db = mysql.connector.connect(
    host='localhost',
    user='root',
    password='wjdgks08',
    database='guids',
)

class GUID:
    '''
    GUID is a class that contains information regarding a GUID. It stores
    the user and the expiration date of the GUID. The class can also 
    return the information in a json format
    '''

    def __init__(self, id, expire, user):

        self._id = id
        self._expire = expire
        self._user = user
    
    def return_json(self):

        return json.dumps({"guid": self._id,
                "expire": self._expire,
                "user": self._user})

class GUIDHandle(RequestHandler):
    def get(self, id):

        # First check if guid is in cache
        cached = cache.get(id)

        if (cached is not None):

            # The GUID is in the cache
            found = json.loads(cached)
        else:
            
            # Check databse for GUID
            cursor = guid_db.cursor()
            cursor.execute("SELECT * FROM guids WHERE guid=%s", (id,))
            row = cursor.fetchone()

            if row is not None:
                found = json.dumps(row)
                cache.set(id, found)


            else:
                raise tornado.web.HTTPError(404, 'GUID Not Found')
        # Return the json of the GUID
        self.write(found)
        
    def post(self, id=None):
        
        if id == None:
            id = str(uuid.uuid4()).replace('-', '').upper()
        
        input = json.loads(self.request.body)
        user = input['user']

        if ('expire' in input):
            expire = int(input['expire'])        

        else:            
            expire = int(time.time() + 30*24*3600)

        # Insert into db

        cursor = guid_db.cursor()
        cursor.execute("INSERT INTO guids VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE expire=%s, user=%s",
                       (id, expire, user, expire, user))
        guid_db.commit()

        # Insert int cache
        guid = GUID(id, expire, user).return_json()
        cache.set(id, guid)


        self.write(guid)

    def delete(self, id):
        
        cursor = guid_db.cursor()
        cursor.execute("DELETE FROM guids WHERE guid=%s", (id,))
        guid_db.commit()

        cache.delete(id)

        self.set_status(204)





def make_app():
  urls = [(r"/guid/([^/]+)", GUIDHandle), (r"/guid/", GUIDHandle)]

  return Application(urls)
  
if __name__ == '__main__':
    app = make_app()
    app.listen(3000)
    IOLoop.instance().start()