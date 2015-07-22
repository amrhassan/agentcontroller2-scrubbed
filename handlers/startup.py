import json
import requests

import utils
import acclient

ENDPOINT_CONFIG = '/rest/system/config'
ENDPOINT_RESTART = '/rest/system/restart'


def results_or_die(results):
    if results['state'] != 'SUCCESS':
        raise Exception('Error executing cmd %s.%s: %s' % (results['cmd'], results['args']['name'], results['data']))
    assert results['level'] == 20, 'Only json response is supported so far'

    return json.loads(results['data'])


def get_url(endpoint):
    base_url = utils.settings['syncthing']['url'].rstrip('/')
    return '%s%s' % (base_url, endpoint)


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
    folders = filter(lambda f: f['id'] == syncthing['shared-folder-id'], config['folders'])

    if not folders:
        raise Exception('No folder with id %s found' % syncthing['shared-folder-id'])

    folder = folders[0]
    if not filter(lambda d: d['deviceID'] == agent_device_id, folder['devices']):
        # share folder with device.

        folder['devices'].append({
            'deviceID': agent_device_id
        })
        dirty = True

    if not dirty:
        return

    response = sessions.post(endpoint, data=json.dumps(config), headers=headers)
    if not response.ok:
        raise Exception('Failed to set syncthing configuration', response.reason)

    response = sessions.post(get_url(ENDPOINT_RESTART), headers=headers)
    if not response.ok:
        raise Exception('Failed to restart syncthing', get_url(ENDPOINT_RESTART), response.reason)

if __name__ == '__main__':
    utils.run(startup)
