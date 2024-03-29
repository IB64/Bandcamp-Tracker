variable "DB_USER" {
    type = string
}

variable "DB_NAME" {
    type = string
}

variable "DB_PASSWORD" {
    type = string
}

variable "DB_IP" {
    type= string
}

variable "DB_PORT" {
    type = string
}

variable "VPC_ID" {
    type = string
    default = "vpc-04423dbb18410aece"
}

variable "AWS_SECRET_ACCESS_KEY" {
    type = string
}

variable "AWS_ACCESS_KEY_ID" {
    type = string
}

variable "API_KEY" {
    type = string
}