# =============================================================================
# Project Configuration
# =============================================================================

variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "us-central1"
}

# =============================================================================
# VPC Configuration
# =============================================================================

variable "vpc_name" {
  description = "Shared VPC name"
  type        = string
}

# =============================================================================
# Sukko Cluster Subnet
# =============================================================================

variable "sukko_subnet_cidr" {
  description = "Sukko cluster node subnet CIDR"
  type        = string
  default     = "10.0.0.0/20"
}

variable "sukko_pods_cidr" {
  description = "Sukko cluster pods secondary CIDR"
  type        = string
  default     = "10.1.0.0/16"
}

variable "sukko_services_cidr" {
  description = "Sukko cluster services secondary CIDR"
  type        = string
  default     = "10.2.0.0/20"
}
