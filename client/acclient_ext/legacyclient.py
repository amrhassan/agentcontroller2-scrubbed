import acclient
import json


class LegacyClient(object):

    def __init__(self, address, port, password, db):
        self.client = acclient.Client(address, port, password, db)
        self.args = acclient.RunArgs()

    def execute(self, organization, name, role=None, nid=None, gid=None, timeout=60, wait=True,  # NOQA
                queue='', dieOnFailure=True, errorreport=True, args=None):

        job = self.executeJumpscript(organization, name, nid, role, args, False, timeout, wait, queue, gid, errorreport)
        if wait:
            result = self._process_result(job.get_result())
            if result['state'] != 'OK':
                if dieOnFailure:
                    raise RuntimeError(
                        'Could not execute job on agent with {}.{}. Error: {}'.format(gid, nid, result['result']))
            return result
        else:
            return job

    def _process_result(self, result):
        job = dict()
        job['state'] = result['state']
        if job['state'] == 'SUCCESS':
            job['state'] = 'OK'

        if result['level'] == 20:  # json (may be we do autoloading of other types)
            job['result'] = json.loads(result['data'])
        else:
            job['result'] = result['data']

        for key in ['nid', 'gid', 'id']:
            job[key] = result[key]
        return job

    def executeJumpscript(self, organization, name, nid=None, role=None, args={}, all=False,
                          timeout=600, wait=True, queue=None, gid=None, errorreport=True):
        data = {}
        data['args'] = args
        data['domain'] = organization
        data['name'] = name

        if all:
            raise NotImplementedError('all is not supported')

        args = acclient.RunArgs(
            domain='jumpscale',  # that's the domain of the legacy command, it's not related to script domain
            name='execute',      # Same as ^^ (name is not currently used in the legacy command)
            queue=queue,
        )

        job = self.client.cmd(gid, nid, 'legacy', self.args, data=data, role=role)
        if wait:
            job = self._process_result(job.get_result(timeout))
        return job

    def waitJumpscript(self, jobguid, timeout=600):
        cmd = self.client.get_by_id(None, None, jobguid)
        return self._process_result(cmd.get_result(timeout))
