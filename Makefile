include .env
IMAGE_TAG := latest
AIRFLOW_MAIN_DIR := ./airflows
AIRFLOW_DIR := services/airflow_image
SCRAPE_DIR := services/scrape_docker
SCRAPE_IMAGE_DIR := services/image_scrape_docker
IMAGE_CAPTION_DIR := services/image_caption_docker
SENTIMENT_ANALYSIS_DIR := services/sentiment_analysis_docker
TERRAFORM_DIR := ./terraform
# Credential directory
SSH_KEY_DIR := $(ssh_directory)
SSH_PRIVATE := $(SSH_KEY_DIR)/reddit_ssh
SSH_PUBLIC := $(SSH_KEY_DIR)/reddit_ssh.pub
REDDIT_CREDENTIAL := $(reddit_credential)
GCP_SERVICE_CREDENTIAL := $(gcp_key_path)

# the ssh key path is $HOME/ssh_key and 

test:
	echo "test"

ssh_key_generation : 	
	@if [ ! -d $(SSH_KEY_DIR) ]; then \
		mkdir -p "$(SSH_KEY_DIR)"; \
		ssh-keygen -t ed25519 -N "" -f $(SSH_PRIVATE) -C "airflow"; \
	fi

docker-builder-init:
	@if ! docker buildx ls | grep -q "mybuilder"; then \
		docker buildx create --name mybuilder --use; \
	else \
		echo "mybuilder already exists and in use"; \
	fi

terraform-prepare: ssh_key_generation
	@if [ ! -f $(TERRAFORM_DIR)/terraform.tfvars ]; then \
		touch $(TERRAFORM_DIR)/terraform.tfvars; \
		echo "ssh_public = \"$$(cat $(SSH_PUBLIC))\"" >> ${TERRAFORM_DIR}/terraform.tfvars; \
		echo "docker_username = \"$(docker_username)\"" >> ${TERRAFORM_DIR}/terraform.tfvars; \
		echo "docker_password = \"$(docker_password)\"" >> ${TERRAFORM_DIR}/terraform.tfvars; \
		echo "service_account_email = \"$(service_account_email)\"" >> ${TERRAFORM_DIR}/terraform.tfvars; \
		echo "gcp_key_path = \"$(GCP_SERVICE_CREDENTIAL)\"" >> ${TERRAFORM_DIR}/terraform.tfvars; \
		echo "scrape_machine_count = \"$(n_vms)\"" >> ${TERRAFORM_DIR}/terraform.tfvars; \
		echo "worker_node_count = \"$(n_worknodes)\"" >> ${TERRAFORM_DIR}/terraform.tfvars; \
		echo "project_id = \"$(gcp_project_id)\"" >> ${TERRAFORM_DIR}/terraform.tfvars; \
	fi

terraform-init: terraform-prepare 
	cd $(TERRAFORM_DIR) && terraform init 
	cd $(TERRAFORM_DIR) && terraform apply 	

terraform-output: terraform-init
	cd $(TERRAFORM_DIR) && pwd && terraform output -json > ../terraform.json

# the targe depends on the terraform

generate_variables : terraform-output
	python3 make_variables.py \
		--reddit_path $(REDDIT_CREDENTIAL) \
		--terraform_path terraform.json \
		--ssh_public_key_path $(SSH_PUBLIC) \
		--output_path $(AIRFLOW_MAIN_DIR)/variables.json
	cd $(AIRFLOW_MAIN_DIR) && echo "AIRFLOW_UID=1000\nAIRFLOW_GID=0\n" > .env	

airflow: docker-builder-init ssh_key_generation
	cp $(GCP_SERVICE_CREDENTIAL) $(AIRFLOW_DIR)/gcp_key.json
	cp $(SSH_PRIVATE) $(AIRFLOW_DIR)
	cp $(SSH_PUBLIC) $(AIRFLOW_DIR)
	echo $(subreddits) > $(AIRFLOW_DIR)/subreddits.txt
	docker buildx build --platform linux/amd64,linux/arm64 -t $(docker_username)/reddit-airflow:latest $(AIRFLOW_DIR) --push

scrape_reddit: docker-builder-init
	cp $(GCP_SERVICE_CREDENTIAL) $(SCRAPE_DIR)/gcp_key.json
	docker buildx build --platform linux/amd64,linux/arm64 -t $(docker_username)/scrape-reddit:latest $(SCRAPE_DIR) --push

scrape_image : docker-builder-init 
	cp $(GCP_SERVICE_CREDENTIAL) $(SCRAPE_IMAGE_DIR)/gcp_key.json
	docker buildx build --platform linux/amd64,linux/arm64 -t $(docker_username)/scrape-image:latest $(SCRAPE_IMAGE_DIR) --push	

image_caption_image : docker-builder-init 
	cp $(GCP_SERVICE_CREDENTIAL) $(IMAGE_CAPTION_DIR)/gcp_key.json
	docker buildx build --platform linux/amd64,linux/arm64 -t $(docker_username)/reddit-image-caption:latest $(IMAGE_CAPTION_DIR) --push

sentiment_analysis_image : docker-builder-init 
	cp $(GCP_SERVICE_CREDENTIAL) $(SENTIMENT_ANALYSIS_DIR)/gcp_key.json
	docker buildx build --platform linux/amd64,linux/arm64 -t $(docker_username)/reddit-sentiment-analysis:latest $(SENTIMENT_ANALYSIS_DIR) --push 	

# Make the reddit-data-dashboard 

reddit-data-dashboard: airflow scrape_reddit scrape_image generate_variables image_caption_image sentiment_analysis_image 
	docker buildx rm mybuilder
	make ssh_connect

ssh_connect:
	@python3 ssh_command.py --input "$$(cd terraform && terraform output | grep 'airflow_external_ip')" 
		
clean: 
	make clean-terraform
	make -j4 clean-variables clean-terraform-output clean-airflow clean-scrape-image clean-image-caption clean-sentiment-analysis clean-env clean-ssh

clean-terraform:
	cd $(TERRAFORM_DIR) && terraform destroy
	rm $(TERRAFORM_DIR)/terraform.tfvars 

clean-variables:
	rm $(AIRFLOW_MAIN_DIR)/variables.json
clean-terraform-output:
	rm ./terraform.json
clean-airflow:
	rm $(AIRFLOW_DIR)/reddit_ssh
	rm $(AIRFLOW_DIR)/reddit_ssh.pub
	rm $(AIRFLOW_DIR)/gcp_key.json	
	rm $(AIRFLOW_DIR)/subreddits.txt
clean-scrape-docker:
	rm $(SCRAPE_DIR)/gcp_key.json
clean-scrape-image:
	rm $(SCRAPE_IMAGE_DIR)/gcp_key.json
clean-image-caption:
	rm ${IMAGE_CAPTION_DIR}/gcp_key.json
clean-sentiment-analysis:
	rm ${SENTIMENT_ANALYSIS_DIR}/gcp_key.json
clean-env:
	rm ./.env
clean-ssh:
	rm -rf $(SSH_KEY_DIR)