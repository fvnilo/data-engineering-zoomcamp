# Terraform and Google Cloud Platform

## Environment Variables

These are the required environment variables that need to be defined:

- `GOOGLE_PROJECT`: The project ID to use.
- `GOOGLE_REGION`: The GCP region to use
- `GOOGLE_CREDENTIALS`: The path of the service account credentials.

## Create Infrastructure

```bash
# Initialize the workspace
terraform init

# Dry run infrastructure creation
terraform plan

# Infrastructure creation
terraform apply

# Delete infrastructure
terraform destroy
```
