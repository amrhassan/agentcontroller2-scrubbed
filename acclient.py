import redis
import json
import uuid

GET_INFO_TIMEOUT = 60


class RunArgs(object):
    def __init__(self, domain=None, name=None, max_time=0, max_restart=0,
                 recurring_period=0, stats_interval=0, args=None, loglevels=None,
                 loglevels_db=None, loglevels_ac=None):
        # init defaults.
        self._domain = domain
        self._name = name
        self._max_time = max_time
        self._max_restart = max_restart
        self._recurring_period = recurring_period
        self._stats_interval = stats_interval
        self._args = args
        self._loglevels = loglevels
        self._loglevels_db = loglevels_db
        self._loglevels_ac = loglevels_ac

    @property
    def domain(self):
        return self._domain

    @property
    def name(self):
        return self._name

    @property
    def max_time(self):
        return self._max_time

    @property
    def max_restart(self):
        return self._max_restart

    @property
    def recurring_period(self):
        return self._recurring_period

    @property
    def stats_interval(self):
        return self._stats_interval

    @property
    def args(self):
        return self._args or []

    @property
    def loglevels(self):
        return self._loglevels or []

    @property
    def loglevels_ac(self):
        return self._loglevels_ac or []

    @property
    def loglevels_db(self):
        return self._loglevels_db or []

    def dump(self):
        dump = {}
        for key in ('domain', 'name', 'max_time', 'max_restart', 'recurring_period',
                    'stats_interval', 'loglevels', 'loglevels_db', 'loglevels_ac'):
            value = getattr(self, key)
            if value:
                dump[key] = value
        return dump

    def update(self, args):
        base = self.dump()
        if isinstance(args, RunArgs):
            data = args.dump()
        elif isinstance(args, dict):
            data = args
        else:
            raise ValueError('Expecting RunArgs or dict')

        base.update(data)
        return RunArgs(**base)


class Cmd(object):
    def __init__(self, redis_client, id, gid, nid, cmd, run_args, data):
        if not isinstance(run_args, RunArgs):
            raise ValueError('Invalid arguments')

        self._redis = redis_client
        self._id = id
        self._gid = int(gid)
        self._nid = int(nid)
        self._cmd = cmd
        self._args = run_args
        self._data = data

    @property
    def id(self):
        return self._id

    @property
    def gid(self):
        return self._gid

    @property
    def nid(self):
        return self._nid

    @property
    def cmd(self):
        return self._cmd

    @property
    def args(self):
        return self._args

    @property
    def data(self):
        return self._data

    def dump(self):
        return {
            'id': self.id,
            'gid': self.gid,
            'nid': self.nid,
            'cmd': self.cmd,
            'args': self.args.dump(),
            'data': json.dumps(self.data) if self.data is not None else ''
        }

    def get_result(self, timeout=0):
        queue, result = self._redis.blpop("cmds_queue_%s" % self.id, timeout)
        return json.loads(result)


class BoundClient(object):
    def __init__(self, client, gid, nid, default_args):
        self._client = client
        self._gid = gid
        self._nid = nid
        self._default_args = default_args

    def _get_args(self, args):
        if self._default_args is not None:
            return self._default_args.update(args)
        return args

    def cmd(self, cmd, args, data, id=None):
        args = self._get_args(args)
        return self._client.cmd(self._gid, self._nid, cmd, args, data, id=None)

    def get_cpu_info(self):
        return self._client.get_cpu_info(self._gid, self._nid)

    def get_disk_info(self):
        return self._client.get_disk_info(self._gid, self._nid)

    def get_mem_info(self):
        return self._client.get_mem_info(self._gid, self._nid)

    def get_nic_info(self):
        return self._client.get_nic_info(self._gid, self._nid)

    def get_os_info(self):
        return self._client.get_os_info(self._gid, self._nid)


class Client(object):
    """
    Initialize the redis connection
    """
    def __init__(self, address='localhost', port=6379, db=0):
        # Initializing redis client
        self._redis = redis.StrictRedis(host=address, port=port, db=db)

        # Check the connectivity
        self._redis.ping()

    def cmd(self, gid, nid, cmd, args, data=None, id=None):
        """
        Executes a command, return a cmd descriptor
        """
        cmd_id = id or str(uuid.uuid4())

        cmd = Cmd(self._redis, cmd_id, gid, nid, cmd, args, data)

        payload = json.dumps(cmd.dump())
        self._redis.lpush('cmds_queue', payload)
        return cmd

    def get_bound_client(self, gid, nid, default_args=None):
        return BoundClient(self, gid, nid, default_args)

    def get_cpu_info(self, gid, nid):
        result = self.cmd(gid, nid, 'get_cpu_info', RunArgs()).get_result(GET_INFO_TIMEOUT)
        return json.loads(result['data'])

    def get_disk_info(self, gid, nid):
        result = self.cmd(gid, nid, 'get_disk_info', RunArgs()).get_result(GET_INFO_TIMEOUT)
        return json.loads(result['data'])

    def get_mem_info(self, gid, nid):
        result = self.cmd(gid, nid, 'get_mem_info', RunArgs()).get_result(GET_INFO_TIMEOUT)
        return json.loads(result['data'])

    def get_nic_info(self, gid, nid):
        result = self.cmd(gid, nid, 'get_nic_info', RunArgs()).get_result(GET_INFO_TIMEOUT)
        return json.loads(result['data'])

    def get_os_info(self, gid, nid):
        result = self.cmd(gid, nid, 'get_os_info', RunArgs()).get_result(GET_INFO_TIMEOUT)
        return json.loads(result['data'])
