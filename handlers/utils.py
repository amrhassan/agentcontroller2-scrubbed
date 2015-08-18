import sys
import os
import fcntl
import functools

settings = {
    'syncthing': {
        'url': os.environ.get('SYNCTHING_URL', 'http://localhost:8384/'),
        'api-key': os.environ.get('SYNCTHING_API_KEY', None),
        'shared-folder-id': os.environ.get('SYNCTHING_SHARED_FOLDER_ID', 'jumpscripts'),
    },
    'redis': {
        'address': os.environ.get('REDIS_ADDRESS', 'localhost'),
        'port': os.environ.get('REDIS_PORT', '6379'),
        'password': os.environ.get('REDIS_PASSWORD', None)
    }
}


def run(func):
    if len(sys.argv) != 3:
        raise Exception("Expecting event.py <gid> <nid>")

    gid, nid = sys.argv[1:]
    func(gid, nid)


class Lock(object):
    def __init__(self, path):
        self._path = path

    def __enter__(self):
        self._fd = open(self._path, 'w')
        fcntl.flock(self._fd, fcntl.LOCK_EX)

    def __exit__(self, type, value, traceback):
        fcntl.flock(self._fd, fcntl.LOCK_UN)
        self._fd.close()


class exclusive(object):  # NOQA
    def __init__(self, path):
        self._path = path

    def __call__(self, fnc):
        @functools.wraps(fnc)
        def wrapper(*args, **kwargs):
            with Lock(self._path):
                return fnc(*args, **kwargs)

        return wrapper
