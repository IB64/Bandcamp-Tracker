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

# Create Role for the Lambda
# resource "aws_iam_role" "lambda_role" {
#   name = "c9-bandcamp-lambda-role"
#   assume_role_policy = jsonencode({
#     "Version": "2012-10-17",
#     "Statement": [
#         {
#             "Effect": "Allow",
#             "Principal": {
#                 "Service": "lambda.amazonaws.com"
#             },
#             "Action": "sts:AssumeRole"
#         }
#     ]
# })
# }

# # Create Role for Event Schedule
# resource "aws_iam_role" "event_schedule_role" {
#   name = "c9-bandcamp-schedule-role"
#   assume_role_policy = jsonencode({
#         "Version": "2012-10-17",
#         "Statement": [
#             {
#                 "Effect": "Allow",
#                 "Principal": {
#                     "Service": "scheduler.amazonaws.com"
#                 },
#                 "Action": "sts:AssumeRole",
#                 "Condition": {
#                     "StringEquals": {
#                         "aws:SourceAccount": "129033205317"
#                     }
#                 }
#             }
#         ]
#     })
# }

# # Create Policy for Schedule Role

# resource "aws_iam_policy" "event_schedule_policy"{
#   name = "c9-bandcamp-schedule-policy"
#   policy = jsonencode({
#         "Version": "2012-10-17",
#         "Statement": [
#             {
#                 "Effect": "Allow",
#                 "Action": [
#                     "ecs:RunTask",
#                     "states:StartExecution"
#                 ],
#                 "Resource": [
#                     "${aws_ecs_task_definition.bandcamp_pipeline_taskdef.arn}",
#                     "${aws_lambda_function.bandcamp_report_lambda.arn}"
#                 ],
#                 "Condition": {
#                     "ArnLike": {
#                         "ecs:cluster": "${data.aws_ecs_cluster.c9-cluster.arn}"
#                     }
#                 }
#             },
#             {
#                 "Effect": "Allow",
#                 "Action": "iam:PassRole",
#                 "Resource": [
#                     "*"
#                 ],
#                 "Condition": {
#                     "StringLike": {
#                         "iam:PassedToService": "ecs-tasks.amazonaws.com"
#                     }
#                 }
#             }
#         ]
#     })
# }

# # Attach Policy to Role

# resource "aws_iam_policy_attachment" "event_schedule_attachment" {
#   name = "c9-bandcamp-attach-policy"
#   roles = [aws_iam_role.event_schedule_role.name]
#   policy_arn = aws_iam_policy.event_schedule_policy.arn
# }

# # Create Dashboard Security Group

resource "aws_security_group" "dashboard-sg" {
  name        = "c9-bandcamp-dashboard-sg"
  description = "Allow inbound Postgres traffic"
  vpc_id      = var.VPC_ID

  ingress {
    description      = "Postgres access"
    from_port        = 8501
    to_port          = 8501
    protocol         = "tcp"
    cidr_blocks      = ["0.0.0.0/0"]
  }
}

# # Create Database Security Group
# resource "aws_security_group" "database-sg" {
#   name        = "c9-bandcamp-database-sg"
#   description = "Allow inbound Postgres traffic"
#   vpc_id      = var.VPC_ID

#   ingress {
#     description      = "Postgres access"
#     from_port        = 5432
#     to_port          = 5432
#     protocol         = "tcp"
#     cidr_blocks      = ["0.0.0.0/0"]
#   }
# }

# # Create Database
# resource "aws_db_instance" "bandcamp_db" {
#   allocated_storage            = 10
#   db_name                      = var.DB_NAME
#   identifier                   = "c9-bandcamp-database"
#   engine                       = "postgres"
#   engine_version               = "15.3"
#   instance_class               = "db.t3.micro"
#   publicly_accessible          = true
#   performance_insights_enabled = false
#   skip_final_snapshot          = true
#   db_subnet_group_name         = "public_subnet_group"
#   vpc_security_group_ids       = [aws_security_group.database-sg.id]
#   username                     = var.DB_USER
#   password                     = var.DB_PASSWORD
# }


# # Create Pipeline Task Definition

# resource "aws_ecs_task_definition" "bandcamp_pipeline_taskdef" {
#   family = "c9-bandcamp-pipeline-td"
#   requires_compatibilities = ["FARGATE"]
#   network_mode = "awsvpc"
#   container_definitions = jsonencode([
#     {
#         "name": "bandcamp-pipeline-td",
#         "image": "129033205317.dkr.ecr.eu-west-2.amazonaws.com/c9-bandcamp-pipeline:latest",
#         "essential": true,
#         "environment": [
#             {
#                 "name": "DB_IP",
#                 "value": var.DB_IP
#             },
#             {
#                 "name": "DB_USER",
#                 "value": var.DB_USER
#             },
#             {
#                 "name": "DB_PASSWORD",
#                 "value": var.DB_PASSWORD
#             },
#             {
#                 "name": "DB_NAME",
#                 "value": var.DB_NAME
#             },
#             {
#                 "name": "DB_PORT",
#                 "value": var.DB_PORT
#             }
#         ],
#         "logConfiguration": {
#             "logDriver": "awslogs",
#             "options": {
#                 "awslogs-group": "/ecs/c9-bandcamp-pipeline-td"
#                 "awslogs-region": "eu-west-2"
#                 "awslogs-stream-prefix": "ecs"
#                 "awslogs-create-group" : "true"
#             }
#         }
#     }
# ])
#   memory=2048
#   cpu = 1024
#   execution_role_arn = data.aws_iam_role.execution-role.arn
# }

# Create Lambda Function for Report Script

# resource "aws_lambda_function" "bandcamp_report_lambda" {
#     function_name = "c9-bandcamp-report-lambda"
#     role = aws_iam_role.lambda_role.arn
#     image_uri = "129033205317.dkr.ecr.eu-west-2.amazonaws.com/c9-bandcamp-report:latest"
#     package_type = "Image"
#     environment {
#       variables = {
#         "DB_IP": var.DB_IP,
#         "DB_NAME": var.DB_NAME,
#         "DB_PASSWORD": var.DB_PASSWORD,
#         "DB_PORT": var.DB_PORT,
#         "DB_USER": var.DB_USER
#       }
#     }
# }

# Create Dashboard Task Definition

resource "aws_ecs_task_definition" "bandcamp_dashboard_taskdef" {
    family = "c9-bandcamp-dashboard-td"
    requires_compatibilities = ["FARGATE"]
    network_mode = "awsvpc"
    container_definitions = jsonencode([
    {
        "name": "bandcamp-dashboard-td",
        "image": "129033205317.dkr.ecr.eu-west-2.amazonaws.com/c9-bandcamp-dashboard:latest",
        "essential": true,
        "portMappings": [{
            "containerPort": 8501,
            "hostPort": 8501
        }]
        "environment": [
            {
                "name": "DB_IP",
                "value": var.DB_IP
            },
            {
                "name": "DB_USER",
                "value": var.DB_USER
            },
            {
                "name": "DB_PASSWORD",
                "value": var.DB_PASSWORD
            },
            {
                "name": "DB_NAME",
                "value": var.DB_NAME
            },
            {
                "name": "DB_PORT",
                "value": var.DB_PORT
            },
            {
                "name": "AWS_ACCESS_KEY_ID_",
                "value": var.AWS_ACCESS_KEY_ID
            },
            {
                "name": "AWS_SECRET_ACCESS_KEY_",
                "value": var.AWS_SECRET_ACCESS_KEY
            },
            {
                "name": "API_KEY",
                "value": var.API_KEY
            }
        ],
    }
])
    memory=2048
    cpu = 1024
    execution_role_arn = data.aws_iam_role.execution-role.arn
}

# Create Dashboard ECS Service 

resource "aws_ecs_service" "bandcamp_dashboard_service" {
    name = "bandcamp_dashboard_service"
    cluster = data.aws_ecs_cluster.c9-cluster.id
    task_definition = aws_ecs_task_definition.bandcamp_dashboard_taskdef.arn
    desired_count = 1
    launch_type = "FARGATE"
    network_configuration {
      subnets = ["subnet-0d0b16e76e68cf51b", "subnet-081c7c419697dec52", "subnet-02a00c7be52b00368"]
      security_groups = [aws_security_group.dashboard-sg.id]
      assign_public_ip = true
    }
}

# Create Pipeline EventBridge Schedule

# resource "aws_scheduler_schedule" "bandcamp_pipeline_schedule" {
#   name = "c9-bandcamp-schedule"

#   flexible_time_window {
#     mode = "OFF"
#   }

#   schedule_expression = "cron(0/5 * * * ? *)"

#   target {
#     arn      = data.aws_ecs_cluster.c9-cluster.arn
#     role_arn = aws_iam_role.event_schedule_role.arn
#     ecs_parameters {
#       task_definition_arn = aws_ecs_task_definition.bandcamp_pipeline_taskdef.arn
#       task_count = 1
#       launch_type = "FARGATE"
#       platform_version = "LATEST"
#       network_configuration {
#         subnets = [ "subnet-0d0b16e76e68cf51b", "subnet-081c7c419697dec52", "subnet-02a00c7be52b00368" ]
#         security_groups = [ "sg-020697b6514174b72" ]
#         assign_public_ip = true
#       }
#     }
#   }
# }

# Create Report Script EventBridge Schedule

# resource "aws_scheduler_schedule" "bandcamp_report_schedule" {
#   name = "c9-bandcamp-report-schedule"

#   flexible_time_window {
#     mode = "OFF"
#   }

#   schedule_expression = "cron(0 9 * * ? *)"

#   target {
#     arn      = aws_lambda_function.bandcamp_report_lambda.arn
#     role_arn = aws_iam_role.event_schedule_role.arn
#   }
# }