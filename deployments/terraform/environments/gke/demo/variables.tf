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

variable "zone" {
  description = "GCP zone"
  type        = string
  default     = "us-central1-a"
}

# =============================================================================
# Cluster Configuration
# =============================================================================

variable "cluster_name" {
  description = "GKE cluster name"
  type        = string
  default     = "sukko-demo"
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "demo"
}

variable "network_name" {
  description = "VPC network name"
  type        = string
  default     = "sukko-demo"
}

# =============================================================================
# Network Configuration
# =============================================================================

variable "subnet_cidr" {
  description = "Subnet CIDR range"
  type        = string
  default     = "10.0.0.0/20"
}

variable "pods_cidr" {
  description = "Secondary CIDR range for pods"
  type        = string
  default     = "10.1.0.0/16"
}

variable "services_cidr" {
  description = "Secondary CIDR range for services"
  type        = string
  default     = "10.2.0.0/20"
}

# =============================================================================
# Node Pool Configuration
# =============================================================================

variable "node_machine_type" {
  description = "Machine type for nodes"
  type        = string
  default     = "e2-standard-4"
}

variable "node_count" {
  description = "Fixed number of nodes"
  type        = number
  default     = 2
}

variable "min_node_count" {
  description = "Minimum nodes for autoscaling"
  type        = number
  default     = 1
}

variable "max_node_count" {
  description = "Maximum nodes for autoscaling"
  type        = number
  default     = 5
}

variable "node_disk_size_gb" {
  description = "Disk size for nodes in GB"
  type        = number
  default     = 50
}

variable "use_spot_vms" {
  description = "Use Spot VMs for cost savings"
  type        = bool
  default     = true
}

variable "taint_spot_nodes" {
  description = "Add taints to Spot nodes"
  type        = bool
  default     = false
}

variable "enable_autoscaling" {
  description = "Enable node pool autoscaling"
  type        = bool
  default     = false
}

# =============================================================================
# Features
# =============================================================================

variable "enable_network_policy" {
  description = "Enable Kubernetes Network Policies"
  type        = bool
  default     = true
}

variable "enable_vertical_pod_autoscaling" {
  description = "Enable Vertical Pod Autoscaler"
  type        = bool
  default     = true
}

variable "release_channel" {
  description = "GKE release channel"
  type        = string
  default     = "REGULAR"
}

variable "deletion_protection" {
  description = "Enable deletion protection for the cluster"
  type        = bool
  default     = false
}
