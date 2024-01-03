# Establish your provider

provider "aws" {
    region = "eu-west-2"
}

# Refer to things that already exist

data "aws_vpc" "c9-vpc" {
    id = "vpc-04423dbb18410aece"
}

data "aws_ecs_cluster" "c9-cluster" {
    cluster_name = "c9-ecs-cluster"
}

data "aws_iam_role" "execution-role" {
    name = "ecsTaskExecutionRole"
}

# Create Database Security Group
resource "aws_security_group" "database-sg" {
  name        = "c9-bandcamp-database-sg"
  description = "Allow inbound Postgres traffic"
  vpc_id      = var.VPC_ID

  ingress {
    description      = "Postgres access"
    from_port        = 5432
    to_port          = 5432
    protocol         = "tcp"
    cidr_blocks      = ["0.0.0.0/0"]
  }
}

# Create Database
resource "aws_db_instance" "bandcamp_db" {
  allocated_storage            = 10
  db_name                      = var.DB_NAME
  identifier                   = "c9-bandcamp-database"
  engine                       = "postgres"
  engine_version               = "15.3"
  instance_class               = "db.t3.micro"
  publicly_accessible          = true
  performance_insights_enabled = false
  skip_final_snapshot          = true
  db_subnet_group_name         = "public_subnet_group"
  vpc_security_group_ids       = [aws_security_group.database-sg.id]
  username                     = var.DB_USER
  password                     = var.DB_PASSWORD
}
