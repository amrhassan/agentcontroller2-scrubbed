import sys

settings = {
    'syncthing': {
        'url': 'http://localhost:8384/',
        'api-key': None,
        'shared-folder-id': 'jumpscripts',
    },
    'redis': {
        'address': '172.17.42.1',
        'port': '6379',
        'password': None
    }
}


def run(func):
    if len(sys.argv) != 3:
        raise Exception("Expecting event.py <gid> <nid>")

    gid, nid = sys.argv[1:]
    func(gid, nid)
