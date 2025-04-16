resource "aws_emr_cluster" "main" {
  name          = var.emr_cluster_name
  release_label = "emr-6.7.0"
  applications  = ["Spark", "Hadoop"]

  ec2_attributes {
    subnet_id                         = var.private_subnets[0]
    emr_managed_master_security_group = aws_security_group.emr_master.id
    emr_managed_slave_security_group  = aws_security_group.emr_slave.id
    instance_profile                  = aws_iam_instance_profile.emr.arn
  }

  master_instance_group {
    instance_type  = var.emr_instance_type
    instance_count = 1
  }

  core_instance_group {
    instance_type  = var.emr_instance_type
    instance_count = var.emr_instance_count
  }

  service_role = aws_iam_role.emr_service.arn

  configurations_json = jsonencode([
    {
      Classification = "spark-defaults"
      Properties = {
        "spark.driver.memory"   = "4g"
        "spark.executor.memory" = "4g"
      }
    }
  ])

  tags = {
    Environment = var.environment
  }
}

resource "aws_security_group" "emr_master" {
  name        = "emr-master-sg"
  description = "Security group for EMR master node"
  vpc_id      = var.vpc_id

  ingress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Environment = var.environment
  }
}

resource "aws_security_group" "emr_slave" {
  name        = "emr-slave-sg"
  description = "Security group for EMR slave nodes"
  vpc_id      = var.vpc_id

  ingress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Environment = var.environment
  }
}

resource "aws_iam_role" "emr_service" {
  name = "emr_service_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "elasticmapreduce.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "emr_service" {
  role       = aws_iam_role.emr_service.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole"
}

resource "aws_iam_instance_profile" "emr" {
  name = "emr_instance_profile"
  role = aws_iam_role.emr_instance.name
}

resource "aws_iam_role" "emr_instance" {
  name = "emr_instance_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "emr_instance" {
  role       = aws_iam_role.emr_instance.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role"
}

# Outputs
output "cluster_id" {
  value = aws_emr_cluster.main.id
}
