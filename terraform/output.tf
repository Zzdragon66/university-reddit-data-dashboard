output "airflow_external_ip" {
  value = google_compute_address.airflow.address
  description = "The external IP address of the Airflow instance"
}
output "internal_ip_addresses" {
  value = [for instance in google_compute_instance.scraping_machine : instance.network_interface[0].network_ip]
}
output "image_bucket" {
  value = google_storage_bucket.image.name
}
output "text_bucket" {
  value = google_storage_bucket.text.name
}
output "meta_bucket" {
  value = google_storage_bucket.meta.name
}

output "spark_bucket" {
  value = google_storage_bucket.spark.name
}

output "cluster_name" {
  value = google_dataproc_cluster.reddit-cluster.name
}

output "region" {
  value = var.region
}

output "gpu_vm_name" {
  value = var.gpu_instance_name
} 

output "gpu_ip_address" {
  value = google_compute_instance.gpu-machine.network_interface[0].network_ip
}

output "project_id" {
  description = "the project id of the terraform project"
  value = var.project_id
}

output "bigquery_dataset_id" {
  description = "the bigquery dataset id"
  value = google_bigquery_dataset.reddit_dataset.dataset_id 
}

output "report_bucket" {
  description = "the report bucket on gcp"
  value = google_storage_bucket.report_bucket.name
}