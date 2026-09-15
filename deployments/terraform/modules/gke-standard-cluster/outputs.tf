# =============================================================================
# Outputs
# =============================================================================

output "cluster_id" {
  description = "The ID of the GKE cluster"
  value       = google_container_cluster.primary.id
}

output "cluster_name" {
  description = "The name of the GKE cluster"
  value       = google_container_cluster.primary.name
}

output "cluster_endpoint" {
  description = "The endpoint of the GKE cluster"
  value       = google_container_cluster.primary.endpoint
  sensitive   = true
}

output "cluster_ca_certificate" {
  description = "The CA certificate of the GKE cluster"
  value       = google_container_cluster.primary.master_auth[0].cluster_ca_certificate
  sensitive   = true
}

output "cluster_location" {
  description = "The location (zone) of the GKE cluster"
  value       = google_container_cluster.primary.location
}

output "network_name" {
  description = "The name of the VPC network"
  value       = local.vpc_name
}

output "subnet_name" {
  description = "The name of the subnet"
  value       = local.subnet_name
}

output "node_pool_name" {
  description = "The name of the primary node pool"
  value       = google_container_node_pool.primary.name
}

output "redpanda_external_ip" {
  description = "The static external IP for Redpanda LoadBalancer"
  value       = local.create_static_ips ? google_compute_address.redpanda_external[0].address : ""
}
