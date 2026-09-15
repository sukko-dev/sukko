# =============================================================================
# Foundation - Shared VPC, Static IPs, Firewall Rules
# =============================================================================
# These resources persist across cluster destroy/recreate cycles.
# Static IPs survive cluster lifecycle changes.

module "foundation" {
  source = "../../../modules/gke-foundation"

  project_id = var.project_id
  region     = var.region
  vpc_name   = var.vpc_name

  # Sukko cluster subnet
  sukko_subnet_cidr   = var.sukko_subnet_cidr
  sukko_pods_cidr     = var.sukko_pods_cidr
  sukko_services_cidr = var.sukko_services_cidr
}
