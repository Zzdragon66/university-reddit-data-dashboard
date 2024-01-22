provider "google" {
  credentials = file(var.gcp_key_path)
  project = var.project_id
  region  = var.region
}

provider "random" {
}

resource "random_integer" "staging_random_number" {
  min     = 1
  max     = 1000
}

resource "google_storage_bucket" "image" {
  name          = "${var.project_id}-${var.image_bucket_name}"
  location      = var.region
  storage_class = "STANDARD"
  force_destroy = true
}

resource "google_storage_bucket" "text" {
  name          = "${var.project_id}-${var.text_bucket_name}"
  location      = var.region
  storage_class = "STANDARD"
  force_destroy = true
}

resource "google_storage_bucket" "meta" {
  name          = "${var.project_id}-${var.meta_bucket_name}"
  location      = var.region
  storage_class = "STANDARD"
  force_destroy = true
}

resource "google_storage_bucket" "spark" {
  name  = "${var.project_id}-${var.spark_bucket_name}"
  location = var.region
  storage_class = "STANDARD"
  force_destroy = true
}

resource "google_storage_bucket" "data_proc_staging" {
  name = "${var.data_proc_staging_bucket_name}-${random_integer.staging_random_number.result}"
  location = var.region
  storage_class = "STANDARD"
  force_destroy = true
}

resource "google_storage_bucket" "report_bucket" {
  name = "${var.project_id}-report_bucket"
  location = var.region
  storage_class = "STANDARD"
  force_destroy = true 
}

resource "google_compute_network" "internal" {
  name                    = "vpc-network"
  auto_create_subnetworks = true 
}

# the main vm for airflow
resource "google_compute_firewall" "allow_ssh" {
  name    = "allow-ssh"
  network = google_compute_network.internal.name

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  source_ranges = ["0.0.0.0/0"]  # This allows SSH access from any IP address. 
                                 # It's advisable to restrict this to known IPs for security.
}

resource "google_compute_firewall" "default_allow_internal" {
  name    = "default-allow-internal"
  network = "default"

  allow {
    protocol = "tcp"
    ports    = ["0-65535"]
  }

  allow {
    protocol = "udp"
    ports    = ["0-65535"]
  }

  allow {
    protocol = "icmp"
  }

  direction = "INGRESS"
  priority  = 65534

  source_ranges = ["10.128.0.0/9"]
}

resource "google_compute_address" "airflow" {
  name   = "airflow"
  region = var.region
  address_type = "EXTERNAL"
  ip_version   = "IPV4"
}

resource "google_compute_instance" "airflow" {
  name = "airflow"
  machine_type = var.airflow_machine_type
  zone = var.zone
  allow_stopping_for_update = true
  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-11"
      size = 30
    }
  }
  network_interface {
    network = google_compute_network.internal.name 
    access_config {
      nat_ip = google_compute_address.airflow.address
    }
  }
  metadata = {
    "ssh-keys" = <<EOT
      airflow:${var.ssh_public} 
     EOT
    "startup-script" = <<EOT
      #!/bin/bash
      sudo apt-get update
      sudo apt-get install -y ca-certificates curl gnupg
      sudo install -m 0755 -d /etc/apt/keyrings
      curl -fsSL https://download.docker.com/linux/debian/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
      sudo chmod a+r /etc/apt/keyrings/docker.gpg
      echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian $(. /etc/os-release && echo \"$VERSION_CODENAME\") stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
      sudo apt-get update
      sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
      sudo apt-get install python3-pip
      sudo docker login --username ${var.docker_username} --password ${var.docker_password}
    EOT 
  }
  service_account {
    email = var.service_account_email
    scopes = ["cloud-platform"]
  }
}

resource "google_compute_instance" "scraping_machine" {
  count = var.scrape_machine_count
  name = "scraping-machine-${count.index}"
  machine_type = var.scrape_machine_type
  zone = var.zone
  allow_stopping_for_update = true
  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-11"
      size = 15
    }
  }
  network_interface {
    network = google_compute_network.internal.name 
    access_config {
      
    }
  }
  metadata = {
    "ssh-keys" = <<EOT
      airflow:${var.ssh_public} 
     EOT
    "startup-script" = <<EOT
      #!/bin/bash
      sudo apt-get update
      sudo apt-get install -y ca-certificates curl gnupg
      sudo install -m 0755 -d /etc/apt/keyrings
      curl -fsSL https://download.docker.com/linux/debian/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
      sudo chmod a+r /etc/apt/keyrings/docker.gpg

      echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian $(. /etc/os-release && echo \"$VERSION_CODENAME\") stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
      sudo apt-get update
      sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
      sudo apt-get install python3-pip
      sudo docker login --username ${var.docker_username} --password ${var.docker_password}
    EOT 
  }
  service_account {
    email = var.service_account_email
    scopes = ["cloud-platform"]
  }
}

resource "google_compute_instance" "gpu-machine" {
  name         =  var.gpu_instance_name
  machine_type = "n1-standard-1"
  zone         =  var.zone

  boot_disk {
    initialize_params {
      image = "deeplearning-platform-release/pytorch-latest-gpu-v20231209-debian-11"
      size = 120
    }
  }
  network_interface {
    network = google_compute_network.internal.name 
    access_config {
    }
  }
  
  guest_accelerator {
    type  = var.gpu_accelerator
    count = 1
  }
  scheduling {
    on_host_maintenance = "TERMINATE" 
    automatic_restart   = true
    preemptible         = false
  }

  metadata = {
    "install-nvidia-driver" = "True"
    "ssh-keys" = <<EOT
      airflow:${var.ssh_public} 
     EOT
    "startup-script" = <<EOT
      sudo apt-get update
      sudo apt-get install -y ca-certificates curl gnupg
      sudo install -m 0755 -d /etc/apt/keyrings
      curl -fsSL https://download.docker.com/linux/debian/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
      sudo chmod a+r /etc/apt/keyrings/docker.gpg
      echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian $(. /etc/os-release && echo \"$VERSION_CODENAME\") stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
      sudo apt-get update
      sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
      sudo apt-get install -y python3-pip
      curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey | sudo gpg --dearmor -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg \
      && curl -s -L https://nvidia.github.io/libnvidia-container/stable/deb/nvidia-container-toolkit.list | \
        sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#g' | \
        sudo tee /etc/apt/sources.list.d/nvidia-container-toolkit.list
      sudo apt-get update
      sudo apt-get install -y nvidia-container-toolkit
      sudo docker login --username ${var.docker_username} --password ${var.docker_password}
    EOT 
  }
  service_account {
    email = var.service_account_email
    scopes = ["cloud-platform"]
  }
}
resource "google_dataproc_cluster" "reddit-cluster" {
  name     = var.dataproc_cluster_name
  region   = var.region
  graceful_decommission_timeout = "120s"

  cluster_config {
    staging_bucket = google_storage_bucket.data_proc_staging.name

    master_config {
      num_instances = 1
      machine_type  = "e2-standard-4"
      disk_config {
        boot_disk_size_gb = 100
      }
    }
    worker_config {
      num_instances    = var.worker_node_count
      machine_type     = "e2-standard-2"
      disk_config {
        boot_disk_size_gb = 50
      }
    }
    preemptible_worker_config {
      num_instances = 0
    }
    gce_cluster_config {
      service_account = var.service_account_email
      service_account_scopes = [
        "cloud-platform"
      ]
    }
  }
}

resource "google_bigquery_dataset" "reddit_dataset" {
  dataset_id = var.dataset_name
  location = "US"
  delete_contents_on_destroy = true
}
