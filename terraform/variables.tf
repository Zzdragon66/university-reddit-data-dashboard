variable "gcp_key_path" {
  description = "The GCP credential file"
  type = string
}

variable "project_id" {
  description = "The ID of the GCP project" 
}

variable "region" {
  description = "The region to host the GCP instances"
  default     = "us-central1"
}

variable "zone" {
  description = "The zone within the region" 
  default = "us-central1-f"
}

variable "airflow_machine_type" {
    description = "the airflow orchestration type"
    default = "e2-standard-4"
}

variable "scrape_machine_type" {
  description = "The type of the GCP instance"
  default     = "e2-small"
}

variable "scrape_machine_count" {
  description = "the number scraping machines"
}

variable "worker_node_count" {
  description = "the number of worker node in dataproc"
}

variable "image_bucket_name" {
    default = "image"
}
variable "text_bucket_name" {
    default = "text"
}
variable "meta_bucket_name" {
    default = "meta"
}

variable "spark_bucket_name" {
  default = "spark"
}

variable "data_proc_staging_bucket_name" {
  default = "reddit-data-proc-staging-random"
}


variable "docker_username" {
  description = "Docker Hub username"
  type        = string
}

variable "docker_password" {
  description = "Docker Hub password"
  type        = string
  sensitive   = true
}

variable service_account_email {
  description = "service account email"
  sensitive = true
}

variable ssh_public {
  description = "the public ssh key"
}

variable dataproc_cluster_name {
  description = "the dataproc cluster name"
  default = "reddit-cluster"
}

variable gpu_instance_name {
  description = "the gpu instance name"
  default = "gpu-vm"
}

variable gpu_accelerator {
  description = "the gpu accelerator instance"
  default = "nvidia-tesla-t4"
}

variable dataset_name {
  description = "The reddit data set name"
  default = "reddit_dataset"
}