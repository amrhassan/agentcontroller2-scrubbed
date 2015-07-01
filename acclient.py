import redis
import json
import string

class Client(object):
    """
    Initialize the redis connection
    """
    def __init__(self, address='localhost', port=6379, db=0):
        # Initializing redis client
        self._redis = redis.StrictRedis(host=address, port=port, db=db)

        # Check the connectivity
        self._redis.ping()
    
    """
    Prepare a command request, return the json request
    Note: args and data should be array/objects
    """
    def cmd(self, id, gid, nid, cmd, args, data):
        # Building local object to push
        datatable = {
            'id': id,
            'gid': int(gid),
            'nid': int(nid),
            'cmd': cmd,
            'args': args,
            'data': data
        }
        
        # Return json which will be pushed
        return json.dumps(datatable)
    
    """
    Send a json request to the redis master queue
    """
    def run(self, cmd):
        # Push json request on our redis master queue
        self._redis.lpush('__master__', cmd)
        return True

    """
    Wait for result on a given jobid
    """
    def result(self, id):
        return self._redis.blpop(id, 0)
