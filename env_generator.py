import subprocess
from pathlib import Path
import json

def get_docker_credential() -> dict:
    """
    get docker username and password 
    """
    docker_username = input("Enter your docker username: ")
    docker_password = input("Enter your docker password: ")
    docker_login_command = f"docker login --username {docker_username} --password {docker_password}"
    result = subprocess.run(docker_login_command, shell=True, stdout=subprocess.PIPE, stderr =subprocess.PIPE, text=True)
    error = result.stderr
    output = result.stdout 
    if result.returncode != 0:
        print("Error:", error)
        raise Exception("Docker login error")
    else:
        print("Output:", output)
        return {
            "docker_username" : docker_username,
            "docker_password" : docker_password
        }

def gcp_keys():
    """Get the GCP file path
    """
    gcp_key_path = input("gcp service account key path: ")
    if not Path(gcp_key_path).exists():
        raise FileNotFoundError("the gcp account key is not found")
    gcp_project_id = input("GCP project id: ") 
    return {"gcp_key_path" : gcp_key_path,
            "gcp_project_id" : gcp_project_id
            }

def get_reddit_credential():
    """Get the reddit credential"""
    reddit_credential_path = input("reddit credential path: ")
    if not Path(reddit_credential_path).exists():
        raise FileNotFoundError("the reddit credential is not found")
    return {"reddit_credential" : reddit_credential_path}

def ssh_key_dir():
    """Ask the user where to store the SSH key 
    """
    ssh_dir = input("SSH key store directory: ")
    ssh_dir_path = Path(ssh_dir)
    if not ssh_dir_path.exists():
        ssh_dir_path.mkdir(parents=True, exist_ok=True)
    return {"ssh_directory" : str(ssh_dir_path)}
    
def get_service_account_email():
    """Get service account email for the terraform infrastructure
    """
    service_account_email = input('service_account_email: ')
    return {"service_account_email" : service_account_email}

def resources_prompt():
    """Get the number of vms and worknodes in the project"""
    n_vms = input("number of VM instances(Default to 2): ")
    if len(n_vms) == 0:
        n_vms = 2
    n_vms = int(n_vms)
    n_worknodes = input("number of spark work nodes(Default to 2): ")
    if len(n_worknodes) == 0:
       n_worknodes = 2
    n_worknodes = int(n_worknodes)
    return {"n_vms" : n_vms, "n_worknodes" : n_worknodes}

def subreddit_prompt() -> str:
    """Get the subreddits on the dashboard
    Returns:
        a string
    """
    reddit_lst = []
    while True:
        subreddit = input(f"Subreddit_{len(reddit_lst)} name(leave blank to finish): ")
        if len(subreddit) == 0:
            break
        reddit_lst.append(subreddit)
    return {"subreddits": " ".join(reddit_lst)}

def write_to_environmenet(input_dict):
    """Output the environment variable based on the user input dictionary
    Args:
        input_dict (_type_): user input dictionary
    """
    with open(".env", "w") as env_file:
        for key in input_dict:
            env_file.writelines(f"{key}={input_dict[key]}\n")

def sanity_check(input_dict : dict):
    """Sanity check"""
    reddit_credential = input_dict["reddit_credential"]
    with open(reddit_credential, "r") as f:
        reddit_json = json.load(f)
    reddit_client_ids = reddit_json["client_id"]
    reddit_client_secrets = reddit_json["client_secret"]
    n_vms = input_dict["n_vms"]
    # start checking
    if len(reddit_client_ids) != len(reddit_client_secrets):
        raise AssertionError("The length of client id is equal to client secrets")
    if n_vms != len(reddit_client_ids):
        raise AssertionError("the number of vm should be equal to number of vms")
    return True

def main():
    """Main function"""
    input_dict = {}
    input_dict.update(get_docker_credential())
    input_dict.update(get_service_account_email())
    input_dict.update(gcp_keys())
    input_dict.update(get_reddit_credential())
    input_dict.update(ssh_key_dir())
    input_dict.update(resources_prompt())
    input_dict.update(subreddit_prompt())
    sanity_check(input_dict)
    write_to_environmenet(input_dict)
    
if __name__ == "__main__":
    main()