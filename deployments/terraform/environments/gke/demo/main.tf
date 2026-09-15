# =============================================================================
# GKE Standard - Demo Environment
# =============================================================================

# Foundation layer provides VPC, subnets, and static IPs that persist
# across cluster destroy/recreate cycles.
data "terraform_remote_state" "foundation" {
  backend = "local"
  config = {
    path = "${path.module}/../demo-foundation/terraform.tfstate"
  }
}

module "gke" {
  source = "../../../modules/gke-standard-cluster"

  # Project
  project_id = var.project_id
  region     = var.region
  zone       = var.zone

  # Cluster
  cluster_name = var.cluster_name
  environment  = var.environment
  network_name = var.network_name

  # Network CIDRs
  subnet_cidr   = var.subnet_cidr
  pods_cidr     = var.pods_cidr
  services_cidr = var.services_cidr

  # Use foundation VPC instead of creating one
  external_vpc_id      = data.terraform_remote_state.foundation.outputs.vpc_id
  external_vpc_name    = data.terraform_remote_state.foundation.outputs.vpc_name
  external_subnet_id   = data.terraform_remote_state.foundation.outputs.subnet_id
  external_subnet_name = data.terraform_remote_state.foundation.outputs.subnet_name

  # Node Pool
  node_machine_type  = var.node_machine_type
  node_count         = var.node_count
  node_disk_size_gb  = var.node_disk_size_gb
  use_spot_vms       = var.use_spot_vms
  taint_spot_nodes   = var.taint_spot_nodes
  enable_autoscaling = var.enable_autoscaling
  min_node_count     = var.min_node_count
  max_node_count     = var.max_node_count

  # Features
  enable_network_policy           = var.enable_network_policy
  enable_vertical_pod_autoscaling = var.enable_vertical_pod_autoscaling
  release_channel                 = var.release_channel
  deletion_protection             = var.deletion_protection

  # Cloud NAT: increase ports for tester VMs with high connection counts.
  # Each outbound connection through NAT consumes one port.
  # Values must be powers of 2 when dynamic allocation is enabled.
  nat_min_ports_per_vm               = 16384
  nat_enable_dynamic_port_allocation = true
  nat_max_ports_per_vm               = 32768
}
