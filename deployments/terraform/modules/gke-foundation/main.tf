# =============================================================================
# Foundation Module
# =============================================================================
# Long-lived resources that persist across cluster lifecycle:
# - VPC with Sukko cluster subnet
# - Static IPs for gateway (external), provisioning (external), and redpanda (internal)
# - Firewall rules for internal traffic and health checks

# =============================================================================
# Shared VPC Network
# =============================================================================

resource "google_compute_network" "vpc" {
  name                    = var.vpc_name
  auto_create_subnetworks = false
  project                 = var.project_id
}

# =============================================================================
# Subnets
# =============================================================================

resource "google_compute_subnetwork" "sukko" {
  name          = "${var.vpc_name}-sukko-subnet"
  ip_cidr_range = var.sukko_subnet_cidr
  region        = var.region
  network       = google_compute_network.vpc.id
  project       = var.project_id

  secondary_ip_range {
    range_name    = "pods"
    ip_cidr_range = var.sukko_pods_cidr
  }

  secondary_ip_range {
    range_name    = "services"
    ip_cidr_range = var.sukko_services_cidr
  }

  private_ip_google_access = true
}

# =============================================================================
# Static IPs
# =============================================================================

resource "google_compute_address" "gateway_external" {
  name         = "${var.vpc_name}-gateway-external"
  region       = var.region
  address_type = "EXTERNAL"
  project      = var.project_id
}

resource "google_compute_address" "provisioning_external" {
  name         = "${var.vpc_name}-provisioning-external"
  region       = var.region
  address_type = "EXTERNAL"
  project      = var.project_id
}

resource "google_compute_address" "redpanda_internal" {
  name         = "${var.vpc_name}-redpanda-internal"
  region       = var.region
  address_type = "INTERNAL"
  subnetwork   = google_compute_subnetwork.sukko.id
  project      = var.project_id
}

# =============================================================================
# Firewall Rules
# =============================================================================

resource "google_compute_firewall" "internal" {
  name    = "${var.vpc_name}-allow-internal"
  network = google_compute_network.vpc.name
  project = var.project_id

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

  source_ranges = [
    var.sukko_subnet_cidr,
    var.sukko_pods_cidr,
    var.sukko_services_cidr,
  ]
}

# Allow SSH via IAP tunneling (for GCE VMs without public IPs)
resource "google_compute_firewall" "iap_ssh" {
  name    = "${var.vpc_name}-allow-iap-ssh"
  network = google_compute_network.vpc.name
  project = var.project_id

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  source_ranges = ["35.235.240.0/20"]
}

# Allow health checks from GCP load balancers
# Safe without target_tags: this VPC is dedicated to Sukko, so all instances are GKE nodes.
resource "google_compute_firewall" "health_checks" {
  name    = "${var.vpc_name}-allow-health-checks"
  network = google_compute_network.vpc.name
  project = var.project_id

  allow {
    protocol = "tcp"
  }

  # GCP health check IP ranges
  source_ranges = ["35.191.0.0/16", "130.211.0.0/22"]
}
