import acclient
import argparse

def main():
	parser = argparse.ArgumentParser(description='jsagent python client.')
	parser.add_argument('--id', required=True)
	parser.add_argument('--gid', required=True)
	parser.add_argument('--nid', required=True)
	parser.add_argument('--cmd', required=True)
	parser.add_argument('--args', required=True)

	args = parser.parse_args()
	
	print("Building client")
	client = acclient.Client(address='localhost', port=6379, db=0)
	
	print("Sending command")
	cmd = client.Cmd(args.id, args.gid, args.nid, args.cmd, args.args)
	client.Run(cmd)
	
	print("Waiting result")
	print(client.Result(args.id))

if __name__ == "__main__":
	main()
