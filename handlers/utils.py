import sys
import os

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
