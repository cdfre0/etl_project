# Azure Deployment Plan

## Mode
MODIFY - Existing Python ETL application with Docker, adding Azure infrastructure deployment.

## Requirements Analysis
- Application: ETL pipeline in Python, containerized with Docker
- Scale: Small to medium (development/testing)
- Budget: Cost-optimized (Azure for Students subscription)
- Features: Data ingestion, transformation, storage in medallion architecture (bronze/silver/gold)

## Codebase Scan
- Language: Python
- Framework: None (scripts)
- Dependencies: requirements.txt (pandas, etc.)
- Infrastructure: Existing Terraform (storage account, containers, Data Factory, Key Vault)
- Containerization: Dockerfile, docker-compose.yml
- Components: agent_orchestrator.py (MCP server?), ingest scripts, transform scripts

## Recipe Selection
Terraform - Extending existing infrastructure code with Azure Container Apps for the ETL application.

## Architecture Plan
- **ETL App**: Azure Container Apps (serverless containers)
- **Storage**: Azure Data Lake Storage Gen2 (existing: bronze, silver, gold containers)
- **Orchestration**: Azure Data Factory (existing)
- **Secrets**: Azure Key Vault (existing)
- **Networking**: Default (public access for dev)
- **Security**: Managed identity, RBAC

## Infrastructure Components
- Extend infra/main.tf with Azure Container Apps resources
- Add environment variables for storage connections
- Configure managed identity for Key Vault access
- Update outputs.tf with ACA URL and other values

## Deployment Steps
1. Update Terraform configuration for ACA
2. Run terraform plan
3. Run terraform apply
4. Retrieve outputs and save to repo (.env or config file)
5. Validate deployment

## Validation Checklist
- ACA deployed and running
- Storage accessible
- Key Vault secrets retrievable
- Outputs saved in repo

## Risks
- Azure CLI authentication required
- Subscription quotas
- Cost monitoring needed

## Next Steps
Approve this plan to proceed with infrastructure generation and deployment.

## Progress
- ✅ Updated Terraform configuration with Azure Container Apps resources
- ⏳ Run terraform plan
- ⏳ Run terraform apply
- ⏳ Retrieve outputs and save to repo
- ⏳ Validate deployment