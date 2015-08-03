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
CMD_TUNNEL_OPEN = 'hubble_open_tunnel'
CMD_TUNNEL_CLOSE = 'hubble_close_tunnel'
CMD_TUNNEL_LIST = 'hubble_list_tunnels'
CMD_GET_MSGS = 'get_msgs'

LEVELS = range(1, 10) + range(20, 24) + [30]


class RunArgs(object):
    def __init__(self, domain=None, name=None, max_time=0, max_restart=0,
                 recurring_period=0, stats_interval=0, args=None, loglevels='*',
                 loglevels_db=None, loglevels_ac=None, queue=None):
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
        :queue: Name of the command queue to wait on.
            This job will not get executed until no other commands running on the same queue.
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
        self._queue = queue

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

    @property
    def queue(self):
        return self._queue

    def dump(self):
        dump = {}
        for key in ('domain', 'name', 'max_time', 'max_restart', 'recurring_period',
                    'stats_interval', 'args', 'loglevels', 'loglevels_db', 'loglevels_ac', 'queue'):
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
        self._gid = 0 if gid is None else int(gid)
        self._nid = 0 if nid is None else int(nid)
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

    def get_msgs(self, levels='*', limit=20):
        return self._client.get_msgs(self._gid, self._nid, jobid=self._id, levels=levels, limit=limit)


class Cmd(BaseCmd):
    def __init__(self, client, redis_client, id, gid, nid, cmd, run_args, data, role):
        if not isinstance(run_args, RunArgs):
            raise ValueError('Invalid arguments')

        if role is not None and (gid, nid) != (None, None):
            raise ValueError('Role is mutual exclusive with gid/nid')

        if (gid, nid, role) == (None, None, None):
            raise ValueError('Gid/Nid or Role must be supplied')

        super(Cmd, self).__init__(client, redis_client, id, gid, nid)
        self._cmd = cmd
        self._args = run_args
        self._data = data
        self._role = role

    @property
    def cmd(self):
        return self._cmd

    @property
    def args(self):
        return self._args

    @property
    def data(self):
        return self._data

    @property
    def role(self):
        return self._role

    def dump(self):
        return {
            'id': self.id,
            'gid': self.gid,
            'nid': self.nid,
            'role': self.role,
            'cmd': self.cmd,
            'args': self.args.dump(),
            'data': json.dumps(self.data) if self.data is not None else ''
        }


class Client(object):
    """
    Initialize the redis connection
    """
    def __init__(self, address='localhost', port=6379, password=None, db=0):
        # Initializing redis client
        self._redis = redis.StrictRedis(host=address, port=port, password=password, db=db)

        # Check the connectivity
        self._redis.ping()

    def cmd(self, gid, nid, cmd, args, data=None, id=None, role=None):
        """
        Executes a command, return a cmd descriptor
        :gid: grid id
        :nid: node id
        :cmd: one of the supported commands (execute, execute_js_py, get_?_info, etc...)
        :args: instance of RunArgs
        :data: Raw data to send to the command standard input. Passed as objecte and will be dumped as json on wire
        :id: id of command for retrieve later, if None a random GUID will be generated.
        :role: Optional role, only agents that satisfy this role can process this job. This is mutual exclusive with
            gid/nid compo in that case, the gid/nid values must be None or a ValueError will be raised.
        """
        cmd_id = id or str(uuid.uuid4())

        cmd = Cmd(self, self._redis, cmd_id, gid, nid, cmd, args, data, role)

        payload = json.dumps(cmd.dump())
        self._redis.rpush('cmds_queue', payload)
        return cmd

    def execute(self, gid, nid, executable, cmdargs=None, args=None, data=None, id=None, role=None):
        """
        Short cut for cmd.execute
        :gid: grid id
        :nid: node id
        :executable: the executable to run_args
        :cmdargs: An optional array with command line arguments
        :args: Optional RunArgs
        :data: Raw data to the command stdin. (see cmd)
        :id: Optional command id (see cmd)
        :role: Optional role, only agents that satisfy this role can process this job. This is mutual exclusive with
            gid/nid compo in that case, the gid/nid values must be None or a ValueError will be raised.
        """
        if cmdargs is not None and not isinstance(cmdargs, list):
            raise ValueError('cmdargs must be a list')

        args = RunArgs().update(args).update({'name': executable, 'args': cmdargs})

        return self.cmd(gid, nid, CMD_EXECUTE, args, data, id, role)

    def execute_js_py(self, gid, nid, domain, name, data=None, args=None, role=None):
        """
        Executes jumpscale script (py) on agent. The execute_js_py extension must be
        enabled and configured correctly on the agent.

        :gid: Grid id
        :nid: Node id
        :domain: Domain of script
        :name: Name of script
        :data: Data object (any json serializabl struct) that will be sent to the script.
        :args: Optional run arguments
        """
        args = RunArgs().update(args).update({'domain': domain, 'name': name})
        return self.cmd(gid, nid, CMD_EXECUTE_JS_PY, args, data, role=role)

    def get_by_id(self, gid, nid, id):
        """
        Get a command descriptor by an ID. So you can read command result later if the ID is known.
        """
        return BaseCmd(self, self._redis, id, gid, nid)

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

    def get_msgs(self, gid, nid, jobid=None, timefrom=None, timeto=None, levels='*', limit=20):
        """
        Query and return log messages stored on agent side.
        :gid: Grid id
        :nid: Node id
        :jobid: Optional jobid
        :timefrom: Optional time from (unix timestamp in seconds)
        :timeto: Optional time to (unix timestamp in seconds)
        :levels: Levels to return (ex: 1,2,3 or 1,2,6-9 or * for all)
        :limit: Max number of log messages to return. Note that the agent in anyways will not return
            more than 1000 record
        """

        query = {
            'jobid': jobid,
            'timefrom': timefrom,
            'timeto': timeto,
            'levels': levels,
            'limit': limit
        }

        result = self.cmd(gid, nid, CMD_GET_MSGS, RunArgs(), query).get_result()
        if result['state'] != 'SUCCESS':
            raise Exception(result['data'])

        return json.loads(result['data'])

    def tunnel_open(self, gid, nid, local, gateway, ip, remote):
        """
        Opens a secure tunnel that accepts connection at the agent's local port `local`
        and forwards the received connections to remote agent `gateway` which will
        forward the tunnel connection to `ip:remote`

        Note: The agent will proxy the connection over the agent-controller it recieved this open command from.

        :gid: Grid id
        :nid: Node id
        :local: Agent's local listening port for the tunnel. 0 for dynamic allocation
        :gateway: The other endpoint `agent` which the connection will be redirected to.
            This should be the name of the hubble agent.
            NOTE: if the endpoint is another superangent, it automatically names itself as '<gid>.<nid>'
        :ip: The endpoint ip address on the remote agent network. Note that IP must be a real ip not a host name
            dns names lookup is not supported.
        :remote: The endpoint port on the remote agent network
        """

        request = {
            'local': int(local),
            'gateway': gateway,
            'ip': ip,
            'remote': int(remote)
        }

        result = self.cmd(gid, nid, CMD_TUNNEL_OPEN, RunArgs(), request).get_result(GET_INFO_TIMEOUT)
        if result['state'] != 'SUCCESS':
            raise Exception(result['data'])

        return json.loads(result['data'])

    def tunnel_close(self, gid, nid, local, gateway, ip, remote):
        """
        Closes a tunnel previously opened by tunnel_open. The `local` port MUST match the
        real open port returned by the tunnel_open function. Otherwise the agent will not match the tunnel and return
        ignore your call.

        Note: closing a non-existing tunnel is not an error.
        """
        request = {
            'local': int(local),
            'gateway': gateway,
            'ip': ip,
            'remote': int(remote)
        }

        result = self.cmd(gid, nid, CMD_TUNNEL_CLOSE, RunArgs(), request).get_result(GET_INFO_TIMEOUT)
        if result['state'] != 'SUCCESS':
            raise Exception(result['data'])

    def tunnel_list(self, gid, nid):
        """
        Return all opened connection that are open from the agent over the agent-controller it
        received this command from
        """
        result = self.cmd(gid, nid, CMD_TUNNEL_LIST, RunArgs()).get_result(GET_INFO_TIMEOUT)
        if result['state'] != 'SUCCESS':
            raise Exception(result['data'])

        return json.loads(result['data'])
