# =============================================================================
# Outputs
# =============================================================================

output "vpc_name" {
  description = "The name of the shared VPC"
  value       = module.foundation.vpc_name
}

output "vpc_id" {
  description = "The ID of the shared VPC"
  value       = module.foundation.vpc_id
}

output "subnet_name" {
  description = "The name of the Sukko cluster subnet"
  value       = module.foundation.subnet_name
}

output "subnet_id" {
  description = "The ID of the Sukko cluster subnet"
  value       = module.foundation.subnet_id
}

output "gateway_external_ip" {
  description = "The static external IP for the gateway LoadBalancer"
  value       = module.foundation.gateway_external_ip
}

output "provisioning_external_ip" {
  description = "The static external IP for the provisioning LoadBalancer"
  value       = module.foundation.provisioning_external_ip
}

output "redpanda_internal_ip" {
  description = "The static internal IP for the Redpanda LoadBalancer"
  value       = module.foundation.redpanda_internal_ip
}
