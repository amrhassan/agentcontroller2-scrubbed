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
    """
    Creates a new instance of RunArgs

    :param domain: Domain name
    :param name: script or executable name
    :param max_time: Max run time, 0 (forever), -1 forever but remember during reboots (long running),
                   other values is timeout
    :param max_restart: Max number of restarts if process died in under 5 min.
    :param recurring_period: Scheduling time
    :param stats_interval: How frequent the stats aggregation is done/flushed to AC
    :param args: Command line arguments (in case of execute)
    :param loglevels: Which log levels to capture and pass to logger
    :param loglevels_db: Which log levels to store in DB (overrides logger defaults)
    :param loglevels_ac: Which log levels to send to AC (overrides logger defaults)
    :param queue: Name of the command queue to wait on.

    This job will not get executed until no other commands running on the same queue.
    """
    def __init__(self, domain=None, name=None, max_time=0, max_restart=0,
                 recurring_period=0, stats_interval=0, args=None, loglevels='*',
                 loglevels_db=None, loglevels_ac=None, queue=None):
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
        self._queue = queue

    @property
    def domain(self):
        """
        Command domain name
        """
        return self._domain

    @property
    def name(self):
        """
        Script or executable name. It's totally up to the ``cmd`` to interpret this
        """
        return self._name

    @property
    def max_time(self):
        """
        Max exection time.
        """
        return self._max_time

    @property
    def max_restart(self):
        """
        Max number of restarts before agent gives up
        """
        return self._max_restart

    @property
    def recurring_period(self):
        """
        How often to run this job (in seconds)
        """
        return self._recurring_period

    @property
    def stats_interval(self):
        """
        How frequent the stats aggregation is done/flushed to AC
        """
        return self._stats_interval

    @property
    def args(self):
        """
        List of command line arguments (if applicable)
        """
        return self._args or []

    @property
    def loglevels(self):
        """
        Log levels to process
        """
        return self._loglevels or ''

    @property
    def loglevels_ac(self):
        """
        Which log levels to report back to AC
        """
        return self._loglevels_ac or ''

    @property
    def loglevels_db(self):
        """
        Which log levels to store in agent DB
        """
        return self._loglevels_db or ''

    @property
    def queue(self):
        """
        Which command queue to wait in (in case of serial exection)
        """
        return self._queue

    def dump(self):
        """
        Return run arguments dict

        :rtype: dict
        """
        dump = {}
        for key in ('domain', 'name', 'max_time', 'max_restart', 'recurring_period',
                    'stats_interval', 'args', 'loglevels', 'loglevels_db', 'loglevels_ac', 'queue'):
            value = getattr(self, key)
            if value:
                dump[key] = value
        return dump

    def update(self, args):
        """
        Return a new instance of run arguments with updated arguments

        :param args: overrides the current arguments with this set
        :type args: :class:`acclient.RunArgs` or :class:`dict`

        :rtype: :class:`acclient.RunArgs`
        """
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
    def __init__(self, client, id, gid, nid):
        self._client = client
        self._redis = client._redis
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
        """
        Pops and returns the first available result for that job. It blocks until the result is givent

        The result is POPed out of the result queue, so a second call to the same method will block until a new
            result object is available for that job. In case you don't want to wait use noblock_get_result()

        :param timeout: Waits for this amount of seconds before giving up on results. 0 means wait forever.

        :rtype: dict
        """

        queue, result = self._redis.blpop('cmds_queue_%s' % self.id, timeout)
        return json.loads(result)

    def noblock_get_result(self):
        """
        Returns a list with all available job results (non-blocking)

        :rtype: list of dicts
        """

        results = self._redis.hgetall('jobresult:%s' % self._id)
        return map(json.loads, results.values())

    def kill(self):
        """
        Kills this command on agent (if it's running)
        """
        return self._client.cmd(self._gid, self._nid, 'kill', RunArgs(), {'id': self._id})

    def get_stats(self):
        """
        Gets the job cpu and memory stats on agent. Only valid if job is still running on
        agent. otherwise will give a 'job id not found' error.
        """
        stats = self._client.cmd(self._gid, self._nid, 'get_process_stats',
                                 RunArgs(), {'id': self._id}).get_result(GET_INFO_TIMEOUT)
        if stats['state'] != 'SUCCESS':
            raise Exception(stats['data'])

        # TODO: parsing data should be always based on the level
        result = json.loads(stats['data'])
        return result

    def get_msgs(self, levels='*', limit=20):
        """
        Gets job log messages from agent

        :param levels: Log levels to retrieve, default to '*' (all)
        :param limit: Max number of log lines to retrieve (max to 1000)

        :rtype: list of dict
        """
        return self._client.get_msgs(self._gid, self._nid, jobid=self._id, levels=levels, limit=limit)


class Cmd(BaseCmd):
    """
    You probably don't need to make an instance of this class manually. Alway use :func:`acclient.Client.cmd` or
    ony of the client shortcuts.
    """
    def __init__(self, client, id, gid, nid, cmd, args, data, role, fanout):
        if not isinstance(args, RunArgs):
            raise ValueError('Invalid arguments')

        if not (bool(role) ^ bool(nid)):
            raise ValueError('Nid and Role are mutual exclusive')

        if not bool(gid) and not bool(role):
            raise ValueError('Gid or Role are required')

        if fanout and role is None:
            raise ValueError('Fanout only effective if role is set')

        super(Cmd, self).__init__(client, id, gid, nid)
        self._cmd = cmd
        self._args = args
        self._data = data
        self._role = role
        self._fanout = fanout

    @property
    def cmd(self):
        """
        Command name
        """
        return self._cmd

    @property
    def args(self):
        """
        Command run arguments
        """
        return self._args

    @property
    def data(self):
        """
        Command data
        """
        return self._data

    @property
    def role(self):
        """
        Command role
        """
        return self._role

    @property
    def fanout(self):
        """
        Either to fanout command
        """
        return self._fanout

    def dump(self):
        """
        Gets the command as a dict

        :rtype: dict
        """
        return {
            'id': self.id,
            'gid': self.gid,
            'nid': self.nid,
            'role': self.role,
            'fanout': self.fanout,
            'cmd': self.cmd,
            'args': self.args.dump(),
            'data': json.dumps(self.data) if self.data is not None else ''
        }

    def __repr__(self):
        return repr(self.dump())


class Client(object):
    """
    Creates a new client instance. You need a client to send jobs to the agent-controller
    and to retrieve results

    :param address: Redis host address
    :param port: Redis port
    :param password: (optional) redis password
    """
    def __init__(self, address='localhost', port=6379, password=None, db=0):
        # Initializing redis client
        self._redis = redis.StrictRedis(host=address, port=port, password=password, db=db)

        # Check the connectivity
        self._redis.ping()

    def cmd(self, gid, nid, cmd, args, data=None, id=None, role=None, fanout=False):
        """
        Executes a command, return a cmd descriptor

        :param gid: grid id (can be None)
        :param nid: node id (can be None)
        :param cmd: one of the supported commands (execute, execute_js_py, get_x_info, etc...)
        :param args: instance of RunArgs
        :param data: Raw data to send to the command standard input. Passed as objecte and will be dumped as json on
                   wire
        :param id: id of command for retrieve later, if None a random GUID will be generated.
        :param role: Optional role, only agents that satisfy this role can process this job. This is mutual exclusive
                   with gid/nid compo in that case, the gid/nid values must be None or a ValueError will be raised.
                   There is a special role '*' which means ANY.
        :param fanout: Send job to ALL agents that satisfy the given role. Only effective is role is set.

        Allowed compinations:

        +-----+-----+------+---------------------------------------+
        | GID | NID | ROLE | Meaning                               |
        +=====+=====+======+=======================================+
        |  X  |  X  |  O   | To specific agent Gid/Nid             |
        +-----+-----+------+---------------------------------------+
        |  X  |  O  |  X   | Any agent with that role on this grid |
        +-----+-----+------+---------------------------------------+
        |  O  |  O  |  X   | Any agent with that role globaly      |
        +-----+-----+------+---------------------------------------+
        """
        cmd_id = id or str(uuid.uuid4())

        cmd = Cmd(self, cmd_id, gid, nid, cmd, args, data, role, fanout)

        payload = json.dumps(cmd.dump())
        self._redis.rpush('cmds_queue', payload)
        return cmd

    def execute(self, gid, nid, executable, cmdargs=None, args=None, data=None, id=None, role=None, fanout=False):
        """
        Short cut for cmd.execute

        :param gid: grid id
        :param nid: node id
        :param executable: the executable to run_args
        :param cmdargs: An optional array with command line arguments
        :param args: Optional RunArgs
        :param data: Raw data to the command stdin. (see cmd)
        :param id: Optional command id (see cmd)
        :param role: Optional role, only agents that satisfy this role can process this job. This is mutual exclusive
                   with gid/nid compo in that case, the gid/nid values must be None or a ValueError will be raised.
        :param fanout: Fanout job to all agents with given role (only effective if role is set)
        """
        if cmdargs is not None and not isinstance(cmdargs, list):
            raise ValueError('cmdargs must be a list')

        args = RunArgs().update(args).update({'name': executable, 'args': cmdargs})

        return self.cmd(gid, nid, CMD_EXECUTE, args, data, id, role, fanout)

    def execute_js_py(self, gid, nid, domain, name, data=None, args=None, role=None, fanout=False):
        """
        Executes jumpscale script (py) on agent. The execute_js_py extension must be
        enabled and configured correctly on the agent.

        :param gid: Grid id
        :param nid: Node id
        :param domain: Domain of script
        :param name: Name of script
        :param data: Data object (any json serializabl struct) that will be sent to the script.
        :param args: Optional run arguments
        """
        args = RunArgs().update(args).update({'domain': domain, 'name': name})
        return self.cmd(gid, nid, CMD_EXECUTE_JS_PY, args, data, role=role, fanout=fanout)

    def get_by_id(self, gid, nid, id):
        """
        Get a command descriptor by an ID. So you can read command result later if the ID is known.
        :rtype: :class:`acclient.BaseCmd`
        """
        return BaseCmd(self, id, gid, nid)

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

        :param gid: Grid id
        :param nid: Node id
        :param jobid: Optional jobid
        :param timefrom: Optional time from (unix timestamp in seconds)
        :param timeto: Optional time to (unix timestamp in seconds)
        :param levels: Levels to return (ex: 1,2,3 or 1,2,6-9 or * for all)
        :param limit: Max number of log messages to return. Note that the agent in anyways will not return
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

        :param gid: Grid id
        :param nid: Node id
        :param local: Agent's local listening port for the tunnel. 0 for dynamic allocation
        :param gateway: The other endpoint `agent` which the connection will be redirected to.
                      This should be the name of the hubble agent.
                      NOTE: if the endpoint is another superangent, it automatically names itself as '<gid>.<nid>'
        :param ip: The endpoint ip address on the remote agent network. Note that IP must be a real ip not a host name
                 dns names lookup is not supported.
        :param remote: The endpoint port on the remote agent network
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

        Closing a non-existing tunnel is not an error.
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

    def get_jobs(self, start=0, count=100):
        """
        Retrieves jobs losgs. This by default returns the latest 100 jobs. You can
        change the `start` and `count` argument to thinking of the jobs history as a list where
        the most recent job is at index 0

        :param start: Start index to retrieve, default 0
        :param count: Number of jobs to retrieve
        """
        assert count > 0, "Invalid count, must be greater than 0"

        def _rmap(r):
            r = json.loads(r)
            r.pop('args', None)
            return r

        def _map(s):
            j = json.loads(s)
            result = self._redis.hgetall('jobresult:%s' % j['id'])

            j['result'] = map(_rmap, result.values())
            return j

        return map(_map, self._redis.lrange('joblog', start, start + count - 1))
