#!/usr/bin/env python3
"""
Test connectivity to Azure Storage and Key Vault
"""
import os
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient
from azure.keyvault.secrets import SecretClient

def test_storage():
    """Test Azure Data Lake Storage connection"""
    print("=" * 50)
    print("Testing Azure Data Lake Storage...")
    print("=" * 50)
    
    try:
        storage_account_name = os.getenv("STORAGE_ACCOUNT_NAME")
        credential = ClientSecretCredential(
            tenant_id=os.getenv("ARM_TENANT_ID"),
            client_id=os.getenv("ARM_CLIENT_ID"),
            client_secret=os.getenv("ARM_CLIENT_SECRET")
        )
        
        service_client = DataLakeServiceClient(
            account_url=f"https://{storage_account_name}.dfs.core.windows.net",
            credential=credential
        )
        
        file_systems = list(service_client.list_file_systems())
        print(f"✅ Storage connection successful! Found {len(file_systems)} containers")
        print("\n📋 Available containers (file systems):")
        for fs in file_systems:
            print(f"  - {fs.name}")
        return True
    except Exception as e:
        print(f"❌ Storage connection failed: {str(e)}")
        return False

def test_keyvault():
    """Test Azure Key Vault connection"""
    print("\n" + "=" * 50)
    print("Testing Azure Key Vault...")
    print("=" * 50)
    
    try:
        key_vault_uri = os.getenv("KEY_VAULT_URI")
        credential = ClientSecretCredential(
            tenant_id=os.getenv("ARM_TENANT_ID"),
            client_id=os.getenv("ARM_CLIENT_ID"),
            client_secret=os.getenv("ARM_CLIENT_SECRET")
        )
        
        client = SecretClient(vault_url=key_vault_uri, credential=credential)
        secrets = client.list_properties_of_secrets()
        print("✅ Key Vault connection successful!")
        print("\nAvailable secrets:")
        for secret in secrets:
            print(f"  - {secret.name}")
        return True
    except Exception as e:
        print(f"❌ Key Vault connection failed: {str(e)}")
        return False

def test_env():
    """Test environment variables"""
    print("\n" + "=" * 50)
    print("Environment Variables Status")
    print("=" * 50)
    
    required_vars = [
        "ARM_TENANT_ID",
        "ARM_CLIENT_ID", 
        "ARM_CLIENT_SECRET",
        "ARM_SUBSCRIPTION_ID",
        "STORAGE_ACCOUNT_NAME",
        "KEY_VAULT_URI"
    ]
    
    all_present = True
    for var in required_vars:
        value = os.getenv(var)
        status = "✅" if value else "❌"
        print(f"{status} {var}: {'SET' if value else 'MISSING'}")
        if not value:
            all_present = False
    
    return all_present

if __name__ == "__main__":
    print("\n🔍 Azure Connectivity Test\n")
    
    env_ok = test_env()
    if not env_ok:
        print("\n⚠️  Some environment variables are missing!")
        exit(1)
    
    storage_ok = test_storage()
    keyvault_ok = test_keyvault()
    
    print("\n" + "=" * 50)
    print("SUMMARY")
    print("=" * 50)
    print(f"Storage:    {'✅' if storage_ok else '❌'}")
    print(f"Key Vault:  {'✅' if keyvault_ok else '❌'}")
    
    if storage_ok and keyvault_ok:
        print("\n✅ All systems operational! Ready for data source connection.")
        exit(0)
    else:
        print("\n❌ Some systems failed. Check credentials and connectivity.")
        exit(1)
