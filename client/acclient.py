import redis
import json
import uuid

GET_INFO_TIMEOUT = 60

CMD_EXECUTE = 'execute'
CMD_EXECUTE_JS_PY = 'execute_js_py'
CMD_EXECUTE_JS_LUA = 'execute_js_lua'
CMD_GET_CPU_INFO = 'get_cpu_info'
CMD_GET_NIC_INFO = 'get_nic_info'
CMD_GET_OS_INFO = 'get_os_info'
CMD_GET_DISK_INFO = 'get_disk_info'
CMD_GET_MEM_INFO = 'get_mem_info'
CMD_GET_PROCESSES_STATS = 'get_processes_stats'

LEVELS = range(13) + range(20, 24) + [30]


class RunArgs(object):
    def __init__(self, domain=None, name=None, max_time=0, max_restart=0,
                 recurring_period=0, stats_interval=0, args=None, loglevels='*',
                 loglevels_db=None, loglevels_ac=None):
        """
        :domain: Domain name
        :name: script or executable name
        :max_time: Max run time, 0 (forever), -1 forever but remember during reboots (long running),
            other values is timeout
        :max_restart: Max number of restarts if process died in under 5 min.
        :recurring_period: Scheduling time
        :stats_interval: How frequent the stats aggregation is done/flushed to AC
        :args: Command line arguments (in case of execute)
        :loglevels: Which log levels to capture and pass to logger
        :loglevels_db: Which log levels to store in DB (overrides logger defaults)
        :loglevels_ac: Which log levels to send to AC (overrides logger defaults)
        """
        self._domain = domain
        self._name = name
        self._max_time = max_time
        self._max_restart = max_restart
        self._recurring_period = recurring_period
        self._stats_interval = stats_interval
        self._args = args
        self._loglevels = self._expand(loglevels)
        self._loglevels_db = self._expand(loglevels_db)
        self._loglevels_ac = self._expand(loglevels_ac)

    def _expand(self, l):
        if l is None:
            return

        if isinstance(l, list):
            # make sure only valid values are allowed
            return list(set(l).intersection(LEVELS))
        elif isinstance(l, basestring):
            levels = set()
            for part in l.split(','):
                lower, _, upper = part.partition('-')
                lower = lower.strip()
                upper = upper.strip()

                if lower == '*':
                    levels.update(LEVELS)
                    continue

                assert lower.isdigit(), 'Value %s is not digit' % lower
                lower = int(lower)

                if upper:
                    # range input
                    assert upper.isdigit(), 'Upper bound %s is not digit' % upper
                    upper = int(upper)
                    if upper > 30:
                        # trim limit
                        upper = 30
                    levels.update(range(lower, upper + 1))
                    continue

                levels.add(lower)
            return list(levels.intersection(LEVELS))

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
                    'stats_interval', 'args', 'loglevels', 'loglevels_db', 'loglevels_ac'):
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
        elif args is None:
            data = {}
        else:
            raise ValueError('Expecting RunArgs or dict')

        base.update(data)
        return RunArgs(**base)


class BaseCmd(object):
    def __init__(self, client, redis_client, id, gid, nid):
        self._client = client
        self._redis = redis_client
        self._id = id
        self._gid = gid
        self._nid = nid
        self._result = None

    @property
    def id(self):
        return self._id

    @property
    def gid(self):
        return self._gid

    @property
    def nid(self):
        return self._nid

    def get_result(self, timeout=0):
        if self._result is None:
            queue, result = self._redis.blpop("cmds_queue_%s" % self.id, timeout)
            self._result = json.loads(result)

        return self._result

    def kill(self):
        return self._client.cmd(self._gid, self._nid, 'kill', RunArgs(), {'id': self._id})

    def get_stats(self):
        stats = self._client.cmd(self._gid, self._nid, 'get_process_stats',
                                 RunArgs(), {'id': self._id}).get_result(GET_INFO_TIMEOUT)
        if stats['state'] != 'SUCCESS':
            raise Exception(stats['data'])

        # TODO: parsing data should be always based on the level
        result = json.loads(stats['data'])
        return result


class Cmd(BaseCmd):
    def __init__(self, client, redis_client, id, gid, nid, cmd, run_args, data):
        if not isinstance(run_args, RunArgs):
            raise ValueError('Invalid arguments')

        super(Cmd, self).__init__(client, redis_client, id, gid, nid)
        self._cmd = cmd
        self._args = run_args
        self._data = data

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

    def get_by_id(self, id):
        return self._client.get_by_id(self._gid, self._nid, id)

    def execute(self, executable, cmdargs=None, args=None, data=None, id=None):
        args = self._get_args(args)
        return self._client.execute(self._gid, self._nid,
                                    executable, cmdargs, args, data, id)

    def get_cpu_info(self):
        """
        Get CPU info of the agent node
        """
        return self._client.get_cpu_info(self._gid, self._nid)

    def get_disk_info(self):
        """
        Get disk info of the agent node
        """
        return self._client.get_disk_info(self._gid, self._nid)

    def get_mem_info(self):
        """
        Get MEM info of the agent node
        """
        return self._client.get_mem_info(self._gid, self._nid)

    def get_nic_info(self):
        """
        Get NIC info of the agent node
        """
        return self._client.get_nic_info(self._gid, self._nid)

    def get_os_info(self):
        """
        Get OS info of the agent node
        """
        return self._client.get_os_info(self._gid, self._nid)

    def get_processes(self, domain=None, name=None):
        """
        Get stats for all running process at the moment of the call, optionally filter with domain and/or name
        """
        return self._client.get_processes(self._gid, self._nid, domain, name)


class Client(object):
    """
    Initialize the redis connection
    """
    def __init__(self, address='localhost', port=6379, password=None, db=0):
        # Initializing redis client
        self._redis = redis.StrictRedis(host=address, port=port, password=password, db=db)

        # Check the connectivity
        self._redis.ping()

    def cmd(self, gid, nid, cmd, args, data=None, id=None):
        """
        Executes a command, return a cmd descriptor
        :gid: grid id
        :nid: node id
        :cmd: one of the supported commands (execute, execute_js_py, get_?_info, etc...)
        :args: instance of RunArgs
        :data: Raw data to send to the command standard input. Passed as objecte and will be dumped as json on wire
        :id: id of command for retrieve later, if None a random GUID will be generated.
        """
        cmd_id = id or str(uuid.uuid4())

        cmd = Cmd(self, self._redis, cmd_id, gid, nid, cmd, args, data)

        payload = json.dumps(cmd.dump())
        self._redis.lpush('cmds_queue', payload)
        return cmd

    def execute(self, gid, nid, executable, cmdargs=None, args=None, data=None, id=None):
        """
        Short cut for cmd.execute
        :gid: grid id
        :nid: node id
        :executable: the executable to run_args
        :cmdargs: An optional array with command line arguments
        :args: Optional RunArgs
        :data: Raw data to the command stdin. (see cmd)
        :id: Optional command id (see cmd)
        """
        if cmdargs is not None and not isinstance(cmdargs, list):
            raise ValueError('cmdargs must be a list')

        run_args = RunArgs(name=executable, args=cmdargs)
        if args is not None:
            run_args = args.update(run_args)

        return self.cmd(gid, nid, CMD_EXECUTE, run_args, data, id)

    def get_by_id(self, gid, nid, id):
        """
        Get a command descriptor by an ID. So you can read command result later if the ID is known.
        """
        return BaseCmd(self, self._redis, id, gid, nid)

    def get_bound_client(self, gid, nid, default_args=None):
        """
        Get a bound client to a specific gid and nid with optional default run arguments
        """
        return BoundClient(self, gid, nid, default_args)

    def get_cpu_info(self, gid, nid):
        """
        Get CPU info of the agent node
        """
        result = self.cmd(gid, nid, CMD_GET_CPU_INFO, RunArgs()).get_result(GET_INFO_TIMEOUT)
        return json.loads(result['data'])

    def get_disk_info(self, gid, nid):
        """
        Get disk info of the agent node
        """
        result = self.cmd(gid, nid, CMD_GET_DISK_INFO, RunArgs()).get_result(GET_INFO_TIMEOUT)
        return json.loads(result['data'])

    def get_mem_info(self, gid, nid):
        """
        Get MEM info of the agent node
        """
        result = self.cmd(gid, nid, CMD_GET_MEM_INFO, RunArgs()).get_result(GET_INFO_TIMEOUT)
        return json.loads(result['data'])

    def get_nic_info(self, gid, nid):
        """
        Get NIC info of the agent node
        """
        result = self.cmd(gid, nid, CMD_GET_NIC_INFO, RunArgs()).get_result(GET_INFO_TIMEOUT)
        return json.loads(result['data'])

    def get_os_info(self, gid, nid):
        """
        Get OS info of the agent node
        """
        result = self.cmd(gid, nid, CMD_GET_OS_INFO, RunArgs()).get_result(GET_INFO_TIMEOUT)
        return json.loads(result['data'])

    def get_processes(self, gid, nid, domain=None, name=None):
        """
        Get stats for all running process at the moment of the call, optionally filter with domain and/or name
        """
        data = {
            'domain': domain,
            'name': name
        }

        result = self.cmd(gid, nid, CMD_GET_PROCESSES_STATS, RunArgs(), data).get_result(GET_INFO_TIMEOUT)
        return json.loads(result['data'])
