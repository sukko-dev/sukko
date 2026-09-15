# =============================================================================
# GKE Standard Cluster Module
# =============================================================================
# Reusable module for creating GKE Standard clusters with Spot VMs
# Cost-optimized: Spot VMs provide 60-90% savings over regular VMs

# =============================================================================
# Locals - Conditional VPC/IP creation
# =============================================================================
# When external_vpc_id is set, the module uses a pre-existing VPC from the
# foundation layer instead of creating its own. This lets static IPs and
# firewall rules survive cluster destroy/recreate cycles.

locals {
  create_vpc        = var.external_vpc_id == ""
  create_static_ips = var.external_vpc_id == ""
  vpc_name          = local.create_vpc ? google_compute_network.vpc[0].name : var.external_vpc_name
  vpc_id            = local.create_vpc ? google_compute_network.vpc[0].id : var.external_vpc_id
  subnet_name       = local.create_vpc ? google_compute_subnetwork.subnet[0].name : var.external_subnet_name
  subnet_id         = local.create_vpc ? google_compute_subnetwork.subnet[0].id : var.external_subnet_id
}

# =============================================================================
# VPC Network (skipped when external VPC provided)
# =============================================================================

resource "google_compute_network" "vpc" {
  count                   = local.create_vpc ? 1 : 0
  name                    = var.network_name
  auto_create_subnetworks = false
  project                 = var.project_id
}

resource "google_compute_subnetwork" "subnet" {
  count         = local.create_vpc ? 1 : 0
  name          = "${var.network_name}-subnet"
  ip_cidr_range = var.subnet_cidr
  region        = var.region
  network       = google_compute_network.vpc[0].id
  project       = var.project_id

  # Secondary ranges for GKE pods and services
  secondary_ip_range {
    range_name    = "pods"
    ip_cidr_range = var.pods_cidr
  }

  secondary_ip_range {
    range_name    = "services"
    ip_cidr_range = var.services_cidr
  }

  private_ip_google_access = true
}

# =============================================================================
# Cloud NAT for private node egress
# =============================================================================

resource "google_compute_router" "router" {
  name    = "${var.cluster_name}-router"
  region  = var.region
  network = local.vpc_id
  project = var.project_id
}

resource "google_compute_router_nat" "nat" {
  name                               = "${var.cluster_name}-nat"
  router                             = google_compute_router.router.name
  region                             = var.region
  project                            = var.project_id
  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "LIST_OF_SUBNETWORKS"

  # Port allocation (defaults match GCP defaults: 64 ports, no dynamic allocation)
  min_ports_per_vm               = var.nat_min_ports_per_vm
  enable_dynamic_port_allocation = var.nat_enable_dynamic_port_allocation
  max_ports_per_vm               = var.nat_enable_dynamic_port_allocation ? var.nat_max_ports_per_vm : null

  subnetwork {
    name                    = local.subnet_id
    source_ip_ranges_to_nat = ["ALL_IP_RANGES"]
  }

  log_config {
    enable = true
    filter = "ERRORS_ONLY"
  }
}

# =============================================================================
# Firewall Rules (skipped when external VPC provided — foundation handles these)
# =============================================================================

resource "google_compute_firewall" "internal" {
  count   = local.create_vpc ? 1 : 0
  name    = "${var.network_name}-allow-internal"
  network = google_compute_network.vpc[0].name
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

  source_ranges = [var.subnet_cidr, var.pods_cidr, var.services_cidr]
}

# Allow health checks from GCP load balancers
resource "google_compute_firewall" "health_checks" {
  count   = local.create_vpc ? 1 : 0
  name    = "${var.network_name}-allow-health-checks"
  network = google_compute_network.vpc[0].name
  project = var.project_id

  allow {
    protocol = "tcp"
  }

  # GCP health check IP ranges
  source_ranges = ["35.191.0.0/16", "130.211.0.0/22"]
}

# =============================================================================
# GKE Standard Cluster (zonal for cost savings)
# =============================================================================

resource "google_container_cluster" "primary" {
  name     = var.cluster_name
  location = var.zone # Zonal cluster (cheaper than regional)
  project  = var.project_id

  # We can't create a cluster with no node pool, so we create the smallest
  # possible default node pool and immediately delete it.
  remove_default_node_pool = true
  initial_node_count       = 1

  # Network configuration
  network    = local.vpc_name
  subnetwork = local.subnet_name

  ip_allocation_policy {
    cluster_secondary_range_name  = "pods"
    services_secondary_range_name = "services"
  }

  # Release channel
  release_channel {
    channel = var.release_channel
  }

  # Workload Identity
  workload_identity_config {
    workload_pool = "${var.project_id}.svc.id.goog"
  }

  # Network policy (Calico)
  network_policy {
    enabled  = var.enable_network_policy
    provider = var.enable_network_policy ? "CALICO" : "PROVIDER_UNSPECIFIED"
  }

  # Addons
  addons_config {
    http_load_balancing {
      disabled = false
    }
    horizontal_pod_autoscaling {
      disabled = false
    }
    network_policy_config {
      disabled = !var.enable_network_policy
    }
  }

  # Vertical Pod Autoscaler
  vertical_pod_autoscaling {
    enabled = var.enable_vertical_pod_autoscaling
  }

  # Maintenance window - Daily 4-8 AM UTC
  maintenance_policy {
    recurring_window {
      start_time = "2024-01-01T04:00:00Z"
      end_time   = "2024-01-01T08:00:00Z"
      recurrence = "FREQ=DAILY"
    }
  }

  # Deletion protection
  deletion_protection = var.deletion_protection

  # Logging and monitoring
  logging_service    = "logging.googleapis.com/kubernetes"
  monitoring_service = "monitoring.googleapis.com/kubernetes"
}

# =============================================================================
# Primary Node Pool with Spot VMs
# =============================================================================

resource "google_container_node_pool" "primary" {
  name     = "primary-pool"
  location = var.zone
  cluster  = google_container_cluster.primary.name
  project  = var.project_id

  # Fixed node count (dev/staging) OR autoscaling (production)
  node_count = var.enable_autoscaling ? null : var.node_count

  # Autoscaling (only when enabled)
  dynamic "autoscaling" {
    for_each = var.enable_autoscaling ? [1] : []
    content {
      min_node_count = var.min_node_count
      max_node_count = var.max_node_count
    }
  }

  # Node management
  management {
    auto_repair  = true
    auto_upgrade = true
  }

  # Node configuration
  node_config {
    machine_type = var.node_machine_type
    disk_size_gb = var.node_disk_size_gb
    disk_type    = "pd-standard"

    # Spot VMs for cost savings (60-90% cheaper)
    spot = var.use_spot_vms

    # OAuth scopes
    oauth_scopes = [
      "https://www.googleapis.com/auth/cloud-platform"
    ]

    # Workload Identity
    workload_metadata_config {
      mode = "GKE_METADATA"
    }

    # Labels
    labels = {
      environment = var.environment
      cluster     = var.cluster_name
      spot        = var.use_spot_vms ? "true" : "false"
    }

    # Taints for Spot VMs (optional)
    dynamic "taint" {
      for_each = var.use_spot_vms && var.taint_spot_nodes ? [1] : []
      content {
        key    = "cloud.google.com/gke-spot"
        value  = "true"
        effect = "NO_SCHEDULE"
      }
    }

    # Metadata
    metadata = {
      disable-legacy-endpoints = "true"
    }

    # Shielded instance
    shielded_instance_config {
      enable_secure_boot          = true
      enable_integrity_monitoring = true
    }
  }

  # Upgrade settings for Spot VMs
  upgrade_settings {
    max_surge       = 1
    max_unavailable = var.use_spot_vms ? 1 : 0
    strategy        = "SURGE"
  }

  lifecycle {
    ignore_changes = [
      node_count, # Managed by autoscaler when enabled
    ]
  }
}

# =============================================================================
# Static External IPs for LoadBalancer Services (skipped when using foundation)
# =============================================================================

resource "google_compute_address" "redpanda_external" {
  count        = local.create_static_ips ? 1 : 0
  name         = "${var.cluster_name}-redpanda-external"
  region       = var.region
  address_type = "EXTERNAL"
  project      = var.project_id
}
