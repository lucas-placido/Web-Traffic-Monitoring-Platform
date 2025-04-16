variable "aws_region" {
  description = "AWS region to deploy resources"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name (dev, prod)"
  type        = string
}

variable "vpc_cidr" {
  description = "CIDR block for VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "private_subnet_cidrs" {
  description = "CIDR blocks for private subnets"
  type        = list(string)
  default     = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]
}

variable "public_subnet_cidrs" {
  description = "CIDR blocks for public subnets"
  type        = list(string)
  default     = ["10.0.101.0/24", "10.0.102.0/24", "10.0.103.0/24"]
}

variable "ecs_cluster_name" {
  description = "Name of the ECS cluster"
  type        = string
  default     = "web-traffic-cluster"
}

variable "emr_cluster_name" {
  description = "Name of the EMR cluster"
  type        = string
  default     = "web-traffic-emr"
}

variable "emr_instance_type" {
  description = "Instance type for EMR nodes"
  type        = string
  default     = "m5.xlarge"
}

variable "emr_instance_count" {
  description = "Number of EMR instances"
  type        = number
  default     = 3
}

variable "s3_bucket_name" {
  description = "Name of the S3 bucket for data storage"
  type        = string
  default     = "web-traffic-data"
} 
