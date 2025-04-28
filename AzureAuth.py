"""
AzureAuth.py

This file defines functions useful for interacting with Azure virtual machines.
"""
import os
from dotenv import load_dotenv
from azure.identity import ClientSecretCredential
from azure.mgmt.compute import ComputeManagementClient

ENV_FILE = "credentials/Azure.env"
CONFIG_FILE = "schedule.json"

def get_credentials(env_file):
    load_dotenv(env_file)

    subscription_id = os.getenv('AZURE_SUBSCRIPTION_ID')
    credentials = ClientSecretCredential(
        client_id=os.getenv('AZURE_CLIENT_ID'),
        client_secret=os.getenv('AZURE_CLIENT_SECRET'),
        tenant_id=os.getenv('AZURE_TENANT_ID')
    )
    return credentials, subscription_id

def create_compute_client(credentials, subscription_id):
    return ComputeManagementClient(credentials, subscription_id)

"""
def check_job_status(ssh_client):
    _, stdout, _ = ssh_client.exec_command('sudo cat /home/ek1074/done.txt')
    status = stdout.read().decode().strip()
    return status
"""
    