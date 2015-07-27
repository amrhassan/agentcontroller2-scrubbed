import acclient
import json

class LegacyClient(object):
    def __init__(self, address, port, password, db):
        self.client = acclient.Client(address, port, password, db)
        self.args = acclient.RunArgs()

    def execute(self,organization,name,role=None,nid=None,gid=None,timeout=60,wait=True,queue="",dieOnFailure=True,errorreport=True, args=None):
        job = self.executeJumpscript(organization, name, nid, role, args, False, timeout, wait, queue, gid, errorreport)
        if wait:
            result = self._process_result(job.get_result())
            if result['state'] != 'OK':
                if dieOnFailure:
                    raise RuntimeError('Could not execute job on agent with {}.{}. Error: %s'.format(gid, nid, result['result']))
            return result
        else:
            return job

    def _process_result(self, result):
        job = dict()
        job['state'] = result['state']
        if job['state'] == 'SUCCESS':
            job['state'] = 'OK'
        job['result'] = result['data']
        for key in ['nid', 'gid', 'id']:
            job[key] = result[key]
        return job

    def executeJumpscript(self, organization, name, nid=None, role=None, args={},all=False, \
                          timeout=600,wait=True,queue="", gid=None,errorreport=True):
        data = args or {}
        data['js_domain'] = organization
        data['js_name'] = name
        job = self.client.cmd(gid, nid, 'legacy', self.args, data=data)
        if wait:
            job = self._process_result(job.get_result())
        return job

    def waitJumpscript(self, jobguid):
        _, result = self.client._redis.blpop("cmds_queue_%s" % jobguid)
        return json.loads(result)
