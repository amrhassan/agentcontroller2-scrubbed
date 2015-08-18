import json
import requests
import logging

import utils
import acclient

ENDPOINT_CONFIG = '/rest/system/config'
ENDPOINT_RESTART = '/rest/system/restart'


"""
SHARE_FOLDERS defines what folders to share on agent
key: local folder id
value: remote path to where to share on agent side.

Currently we only share legacy and jumpscripts folder to agent
"""
SHARE_FOLDERS = {
    'legacy': '/opt/jumpscale7/apps/agent2/legacy',
    'jumpscripts': '/opt/jumpscale7/apps/agent2/jumpscripts'
}


def results_or_die(results):
    if results['state'] != 'SUCCESS':
        raise Exception('Error executing cmd %s.%s: %s' % (results['cmd'], results['args']['name'], results['data']))
    assert results['level'] == 20, 'Only json response is supported so far'

    return json.loads(results['data'])


def get_url(endpoint):
    base_url = utils.settings['syncthing']['url'].rstrip('/')
    return '%s%s' % (base_url, endpoint)


@utils.exclusive('/tmp/agent-start.lock')
def startup(gid, nid):
    # TODO: client must use settings of somekind
    sessions = requests.Session()

    headers = {
        'content-type': 'application/json'
    }

    syncthing = utils.settings['syncthing']
    api_key = syncthing['api-key']

    if api_key is not None:
        headers['X-API-Key'] = api_key

    client = acclient.Client(**utils.settings['redis'])
    default = acclient.RunArgs(domain='jumpscale')

    get_id = client.cmd(gid, nid, 'sync', default.update({'name': 'get_id'}))

    agent_device_id = results_or_die(get_id.get_result(30))

    endpoint = get_url(ENDPOINT_CONFIG)
    response = sessions.get(endpoint, headers=headers)

    if not response.ok:
        raise Exception('Invalid response from syncthing', response.reason)

    local_device_id = response.headers['x-syncthing-id']
    config = response.json()

    if api_key is None:
        # if auth is off, we still need to use the API key to be able to use POST.
        # in this case, get the API key from the get response
        api_key = config['gui']['apiKey']
        headers['X-API-Key'] = api_key

    devices = filter(lambda d: d['deviceID'] == agent_device_id, config['devices'])

    dirty = False
    if not devices:
        device = {
            'addresses': ['dynamic'],
            'certName': '',
            'compression': 'metadata',
            'deviceID': agent_device_id,
            'introducer': False,
            'name': '%s-%s' % (gid, nid)
        }

        config['devices'].append(device)
        dirty = True

    # add device to shared folder.
    for folder_id in SHARE_FOLDERS:
        folders = filter(lambda f: f['id'] == folder_id, config['folders'])

        if not folders:
            logging.warn('Folder id "%s" is not shared on syncthing', folder_id)
            continue

        folder = folders[0]
        if not filter(lambda d: d['deviceID'] == agent_device_id, folder['devices']):
            # share folder with device.

            folder['devices'].append({
                'deviceID': agent_device_id
            })
            dirty = True

    if dirty:
        # apply changes
        response = sessions.post(endpoint, data=json.dumps(config), headers=headers)
        if not response.ok:
            raise Exception('Failed to set syncthing configuration', response.reason)

        response = sessions.post(get_url(ENDPOINT_RESTART), headers=headers)
        if not response.ok:
            raise Exception('Failed to restart syncthing', get_url(ENDPOINT_RESTART), response.reason)

    # Now, the syncthing on AC side knows about the syncthing on Agent side. Now we have
    # to register this instance of syncthing on agent as well. We can do this via a simple agent command

    for folder_id, remote_path in SHARE_FOLDERS.iteritems():
        data = {
            'device_id': local_device_id,
            'folder_id': folder_id,
            'path': remote_path
        }

        result = client.cmd(gid, nid, 'sync', default.update({'name': 'sync_folder'}), data).get_result()
        if result['state'] != 'SUCCESS':
            logging.warn('Error syncthing jumpscripts folder on agent: %s' % result['data'])
            continue

if __name__ == '__main__':
    utils.run(startup)
