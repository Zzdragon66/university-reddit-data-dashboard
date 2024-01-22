import argparse
import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

def main(input_str : str):
    ip_address = input_str.split("=")[-1].strip()
    ip_address = ip_address.replace("\"", "")
    ssh_dir = Path(os.environ.get("ssh_directory"))
    ssh_file_path = ssh_dir / "reddit_ssh"
    scp_command = f"scp -r -i  {str(ssh_file_path)} ./airflows airflow@{ip_address}:"
    ssh_command = f"ssh -v -i {str(ssh_file_path)} airflow@{ip_address}"
    ssh_remote_tunnel = f"ssh -v -i {str(ssh_file_path)} -L 8080:localhost:8080 airflow@{ip_address}"
    os.system(scp_command)
    print("--------------------SSH and SSH Remote Tunnel Command-----------------------")
    print(ssh_command)
    print(ssh_remote_tunnel)
    #os.system(ssh_command)

if __name__ == "__main__":
    argparser = argparse.ArgumentParser("the ip address extractor")
    argparser.add_argument("--input", required=True, type=str)
    args = argparser.parse_args() 
    main(args.input)

# sudo docker compose up -d