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
  description = "GCP zone for zonal cluster (cost-effective for non-prod)"
  type        = string
  default     = "us-central1-a"
}

# =============================================================================
# Cluster Configuration
# =============================================================================

variable "cluster_name" {
  description = "GKE cluster name"
  type        = string
}

variable "environment" {
  description = "Environment name (demo, dev, staging, production)"
  type        = string
}

# =============================================================================
# Network Configuration
# =============================================================================

variable "network_name" {
  description = "VPC network name"
  type        = string
}

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
# External VPC (optional - when set, skip VPC/subnet/firewall/IP creation)
# =============================================================================

variable "external_vpc_id" {
  description = "External VPC ID (skip VPC creation if set)"
  type        = string
  default     = ""
}

variable "external_vpc_name" {
  description = "External VPC name"
  type        = string
  default     = ""
}

variable "external_subnet_id" {
  description = "External subnet ID (skip subnet creation if set)"
  type        = string
  default     = ""
}

variable "external_subnet_name" {
  description = "External subnet name"
  type        = string
  default     = ""
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
  description = "Fixed number of nodes (used when enable_autoscaling=false)"
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
  description = "Use Spot VMs for cost savings (can be preempted)"
  type        = bool
  default     = true
}

variable "taint_spot_nodes" {
  description = "Add taints to Spot nodes (requires tolerations in workloads)"
  type        = bool
  default     = false
}

variable "enable_autoscaling" {
  description = "Enable node pool autoscaling (true for production, false for dev/staging)"
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
  description = "GKE release channel (RAPID, REGULAR, STABLE)"
  type        = string
  default     = "REGULAR"
}

variable "deletion_protection" {
  description = "Enable deletion protection for the cluster"
  type        = bool
  default     = false
}

# =============================================================================
# Cloud NAT Port Allocation
# =============================================================================

variable "nat_min_ports_per_vm" {
  description = "Minimum NAT source ports per VM. GCP default is 64. Increase for loadtest VMs."
  type        = number
  default     = 64
}

variable "nat_enable_dynamic_port_allocation" {
  description = "Allow NAT to dynamically allocate ports beyond min_ports_per_vm up to max_ports_per_vm."
  type        = bool
  default     = false
}

variable "nat_max_ports_per_vm" {
  description = "Maximum NAT source ports per VM when dynamic allocation is enabled."
  type        = number
  default     = 32768
}
