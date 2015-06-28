import argparse, redis, json, string

def main():
	parser = argparse.ArgumentParser(description='jsagent python client.')
	parser.add_argument('--id', required=True)
	parser.add_argument('--gid', required=True)
	parser.add_argument('--nid', required=True)
	parser.add_argument('--cmd', required=True)
	parser.add_argument('--args', required=True)

	args = parser.parse_args()
	
	data = {
		'id': string.atoi(args.id),
		'gid': string.atoi(args.gid),
		'nid': string.atoi(args.nid),
		'cmd': args.cmd,
		'args': args.args,
		'data': ''
	}
	
	command = json.dumps(data)
	
	r = redis.StrictRedis(host='localhost', port=6379, db=0)
	r.lpush('__master__', command)
	
	value = r.blpop(args.gid + ':' + args.nid + ':' + args.id, 0)
	print(value)

if __name__ == "__main__":
	main()
