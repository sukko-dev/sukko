# =============================================================================
# Outputs
# =============================================================================

output "project_id" {
  description = "The GCP project ID"
  value       = var.project_id
}

output "cluster_id" {
  description = "The ID of the GKE cluster"
  value       = module.gke.cluster_id
}

output "cluster_name" {
  description = "The name of the GKE cluster"
  value       = module.gke.cluster_name
}

output "cluster_endpoint" {
  description = "The endpoint of the GKE cluster"
  value       = module.gke.cluster_endpoint
  sensitive   = true
}

output "cluster_ca_certificate" {
  description = "The CA certificate of the GKE cluster"
  value       = module.gke.cluster_ca_certificate
  sensitive   = true
}

output "cluster_location" {
  description = "The location (zone) of the GKE cluster"
  value       = module.gke.cluster_location
}

output "network_name" {
  description = "The name of the VPC network"
  value       = module.gke.network_name
}

output "subnet_name" {
  description = "The name of the subnet"
  value       = module.gke.subnet_name
}

output "node_pool_name" {
  description = "The name of the primary node pool"
  value       = module.gke.node_pool_name
}

output "kubeconfig_command" {
  description = "Command to configure kubectl"
  value       = "gcloud container clusters get-credentials ${module.gke.cluster_name} --zone ${var.zone} --project ${var.project_id}"
}

output "gateway_external_ip" {
  description = "The static external IP for Gateway LoadBalancer"
  value       = data.terraform_remote_state.foundation.outputs.gateway_external_ip
}

output "redpanda_internal_ip" {
  description = "The static internal IP for Redpanda LoadBalancer"
  value       = data.terraform_remote_state.foundation.outputs.redpanda_internal_ip
}
