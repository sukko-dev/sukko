# =============================================================================
# Outputs
# =============================================================================

output "vpc_name" {
  description = "The name of the shared VPC"
  value       = google_compute_network.vpc.name
}

output "vpc_id" {
  description = "The ID of the shared VPC"
  value       = google_compute_network.vpc.id
}

output "subnet_name" {
  description = "The name of the Sukko cluster subnet"
  value       = google_compute_subnetwork.sukko.name
}

output "subnet_id" {
  description = "The ID of the Sukko cluster subnet"
  value       = google_compute_subnetwork.sukko.id
}

output "gateway_external_ip" {
  description = "The static external IP for the gateway LoadBalancer"
  value       = google_compute_address.gateway_external.address
}

output "provisioning_external_ip" {
  description = "The static external IP for the provisioning LoadBalancer"
  value       = google_compute_address.provisioning_external.address
}

output "redpanda_internal_ip" {
  description = "The static internal IP for the Redpanda LoadBalancer"
  value       = google_compute_address.redpanda_internal.address
}
