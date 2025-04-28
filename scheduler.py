"""
scheduler.py

This file defines the scheduler that is used to schedule jobs on virtual machines.
It expects as input a configuration file in a JSON format that lists the jobs the
scheduler should consider, whether location switching is permitted, and whether the job can be delayed.
Based on the current percentile across regions, a decision is made and the VM is started.
At maximum a job will wait for up to 12 hours before it will be scheduled.
"""

import json
import logging
import time
import AzureAuth
import AWSAuth
import paramiko
import WattTime
from scipy.stats import norm
from datetime import datetime, timedelta

AZURE_LOCATIONS = ["PJM_ROANOKE", "FR"]
AWS_LOCATIONS = ["PJM_SOUTHWEST_OH", "SE"]
SHARED_LOCATIONS = ["CAISO_NORTH", "UK"]
MIN_MAX_GCO2 = [{"region": "CAISO_NORTH", "min": 54, "max": 263},
                {"region": "UK", "min": 147, "max": 319},
                {"region": "PJM_ROANOKE", "min": 360, "max": 432},
                {"region": "PJM_SOUTHWEST_OH", "min": 360, "max": 432},
                {"region": "FR", "min": 17, "max": 54},
                {"region": "SE", "min": 18, "max": 28}]

WATTTIME_ENV = "credentials/WattTime.env"
AZURE_ENV = "credentials/Azure.env"
AWS_ENV = "credentials/AWS.env"
TOKEN = ""

def initialize_logger():
    logger = logging.getLogger('scheduler_logger')
    logger.setLevel(logging.INFO)

    file_logger = logging.FileHandler("scheduler.log")
    file_logger.setLevel(logging.INFO)

    console_logger = logging.StreamHandler()
    console_logger.setLevel(logging.INFO)

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    file_logger.setFormatter(formatter)
    console_logger.setFormatter(formatter)

    logger.addHandler(file_logger)
    logger.addHandler(console_logger)

    return logger

def map_azure_location(location):
    if location == "CAISO_NORTH":
        return "West US"
    elif location == "UK":
        return "UK South"
    elif location == "PJM_ROANOKE":
        return "East US"
    elif location == "FR":
        return "France Central"
    else:
        return None

def map_aws_location(location):
    if location == "CAISO_NORTH":
        return "us-west-1"
    elif location == "UK":
        return "eu-west-2"
    elif location == "PJM_SOUTHWEST_OH":
        return "us-east-2"
    elif location == "SE":
        return "eu-north-1"
    else:
        return None

def parse_configuration(config_path):
    azure_jobs = []
    aws_jobs = []
    azure_vms = []
    aws_vms = []

    with open(config_path, 'r') as file:
        data = json.load(file)
        for azure_job in data["azure"]:
            azure_jobs.append(azure_job)
        for aws_job in data["aws"]:
            aws_jobs.append(aws_job)
        for azure_vm in data["azure_vms"]:
            azure_vms.append(azure_vm)
        for aws_vm in data["aws_vms"]:
            aws_vms.append(aws_vm)
    
    return azure_jobs, aws_jobs, azure_vms, aws_vms

def add_timestamp(azure_jobs, aws_jobs):
    submitted = datetime.now()
    for job in (azure_jobs + aws_jobs):
        job["submitted"] = submitted

    return azure_jobs, aws_jobs

def get_region_percentiles(token):
    percentiles = []

    for region in (SHARED_LOCATIONS + AZURE_LOCATIONS + AWS_LOCATIONS):
        region_data = WattTime.get_current(token, region)
        if region_data.ok:
            percentile = region_data.json()["data"][0]["value"]
            percentiles.append({"region": region, "percentile": percentile})
        else:
            global TOKEN
            TOKEN = WattTime.generate_token(WATTTIME_ENV)

    return percentiles

def get_gco2_range_for_region(region):
    for range in MIN_MAX_GCO2:
        if range["region"] == region:
            return range
        
    return None

def percentile_to_gco2(percentiles):
    gco2s = []

    for percentile in percentiles:
        range = get_gco2_range_for_region(percentile["region"])

        # Calculate mean and standard deviation
        mean = (range["min"] + range["max"]) / 2
        std_dev = (range["max"] - range["min"]) / 4
        
        z_score = norm.ppf(percentile["percentile"] / 100)  # Inverse CDF
        
        gco2 = mean + z_score * std_dev

        gco2s.append({"region": percentile["region"], "gco2": gco2})
    
    return gco2s

def gco2_to_moer(gco2s):
    values = []

    for gco2 in gco2s:
        conversion_factor = 2.20462
        values.append({"region": gco2["region"], "co2_moer": gco2["gco2"] * conversion_factor})

    return values

def get_lowest_azure_co2_moer(co2_moers):
    lowest_co2_moer = co2_moers[0]
    for co2_moer in co2_moers:
        data = co2_moer["co2_moer"]
        if (data < lowest_co2_moer["co2_moer"]) and (co2_moer["region"] in (AZURE_LOCATIONS + SHARED_LOCATIONS)):
            lowest_co2_moer = co2_moer
    
    return lowest_co2_moer

def get_lowest_aws_co2_moer(co2_moers):
    lowest_co2_moer = co2_moers[0]
    for co2_moer in co2_moers:
        data = co2_moer["co2_moer"]
        if (data < lowest_co2_moer["co2_moer"]) and (co2_moer["region"] in (AWS_LOCATIONS + SHARED_LOCATIONS)):
            lowest_co2_moer = co2_moer
    
    return lowest_co2_moer

def get_azure_percentile_threshold(percentiles, threshold):
    matches = []
    
    for percentile in percentiles:
        if (percentile["percentile"] <= threshold) and (percentile["region"] in (AZURE_LOCATIONS + SHARED_LOCATIONS)):
            matches.append(percentile)

    return matches

def get_aws_percentile_threshold(percentiles, threshold):
    matches = []
    
    for percentile in percentiles:
        if (percentile["percentile"] <= threshold) and (percentile["region"] in (AWS_LOCATIONS + SHARED_LOCATIONS)):
            matches.append(percentile)

    return matches

def azure_schedule_decision(job, percentiles, co2_moers):

    # Case 1: The job is time sensitive and can't switch locations. Schedule immediately.
    if job["time_sensitive"] and not job["location_switch"]:
        return job["location"]
    
    # Case 2: The job is time sensitive but you can switch locations. Schedule at lowest co2_moer.
    if job["time_sensitive"] and job["location_switch"]:
        lowest_co2_moer = get_lowest_azure_co2_moer(co2_moers)
        region = map_azure_location(lowest_co2_moer["region"])
        return region

    threshold = job["percentile_threshold"]

    # Case 3: Not time sensitive and can't switch locations. Schedule on threshold or timeout
    if not job["time_sensitive"] and not job["location_switch"]:
        matches = get_azure_percentile_threshold(percentiles, threshold)
        for match in matches:
            region = map_azure_location(match["region"])
            if region == job["location"]:
                return region

    # Case 4: Not time sensitive and can switch locations. Schedule on threshold or timeout
    if not job["time_sensitive"] and job["location_switch"]:
        matches = get_azure_percentile_threshold(percentiles, threshold)
        if matches != []:
            matches_c02_moer = gco2_to_moer(percentile_to_gco2(matches))
            lowest_co2_moer = get_lowest_azure_co2_moer(matches_c02_moer)
            return map_azure_location(lowest_co2_moer["region"])

    # Check for a timeout
    current_time = datetime.now()
    difference = current_time - job["submitted"]
    if difference >= timedelta(hours=12):
        return job["location"]

    # Cannot be scheduled at this time
    return None

def aws_schedule_decision(job, percentiles, co2_moers):

    # Case 1: The job is time sensitive and can't switch locations. Schedule immediately.
    if job["time_sensitive"] and not job["location_switch"]:
        return job["location"]
    
    # Case 2: The job is time sensitive but you can switch locations. Schedule at lowest co2_moer.
    if job["time_sensitive"] and job["location_switch"]:
        lowest_co2_moer = get_lowest_aws_co2_moer(co2_moers)
        region = map_aws_location(lowest_co2_moer["region"])
        return region

    threshold = job["percentile_threshold"]

    # Case 3: Not time sensitive and can't switch locations. Schedule on threshold or timeout
    if not job["time_sensitive"] and not job["location_switch"]:
        matches = get_aws_percentile_threshold(percentiles, threshold)
        for match in matches:
            region = map_aws_location(match["region"])
            if region == job["location"]:
                return region

    # Case 4: Not time sensitive and can switch locations. Schedule on threshold or timeout
    if not job["time_sensitive"] and job["location_switch"]:
        matches = get_aws_percentile_threshold(percentiles, threshold)
        if matches != []:
            matches_c02_moer = gco2_to_moer(percentile_to_gco2(matches))
            lowest_co2_moer = get_lowest_aws_co2_moer(matches_c02_moer)
            return map_aws_location(lowest_co2_moer["region"])

    # Check for a timeout
    current_time = datetime.now()
    difference = current_time - job["submitted"]
    if difference >= timedelta(hours=12):
        return job["location"]

    # Cannot be scheduled at this time
    return None

def get_azure_vm_by_region(vms, region):
    for vm in vms:
        if vm["location"] == region:
            return vm

    return None

def get_aws_vm_by_region(vms, region):
    for vm in vms:
        if vm["location"] == region:
            return vm

def init_job_list(azure_vms, aws_vms):
    vm_jobs = {}
    for vm in (azure_vms + aws_vms):
        vm_jobs[vm["vm_name"]] = []
    
    return vm_jobs

def init_ssh_list(azure_vms, aws_vms):
    vm_clients = {}
    for vm in (azure_vms + aws_vms):
        vm_clients[vm["vm_name"]] = []
    
    return vm_clients

def main():
    logger = initialize_logger()

    azure_jobs, aws_jobs, azure_vms, aws_vms = parse_configuration("schedule.json")
    azure_jobs, aws_jobs = add_timestamp(azure_jobs, aws_jobs)

    vm_jobs = init_job_list(azure_vms, aws_vms)
    vm_clients = init_ssh_list(azure_vms, aws_vms)

    logger.info("Successfully Read Azure and AWS Schedule JSON")

    credentials, subscription_id = AzureAuth.get_credentials(AZURE_ENV)
    azure_compute_client = AzureAuth.create_compute_client(credentials, subscription_id)

    aws_compute_clients = []
    for aws_region in (SHARED_LOCATIONS + AWS_LOCATIONS):
        location = map_aws_location(aws_region)
        session = AWSAuth.make_aws_session(AWS_ENV, location)
        session = session.resource("ec2")
        aws_compute_clients.append({"region": location, "session": session})

    logger.info("Successfully Authenticated to AWS and Azure")

    global TOKEN
    TOKEN = WattTime.generate_token(WATTTIME_ENV)

    while True:
        percentiles = get_region_percentiles(TOKEN)
        co2_moers = gco2_to_moer(percentile_to_gco2(percentiles))

        logger.info("Scheduler obtained new carbon emission data")

        for azure_job in azure_jobs:
            region = azure_schedule_decision(azure_job, percentiles, co2_moers)

            if region != None:
                logger.info("Scheduling Azure job \"%s\" in region %s", azure_job["job_name"], region)
                azure_jobs.remove(azure_job)

                # At this point we are ready to run the VM
                vm_info = get_azure_vm_by_region(azure_vms, region)

                if len(vm_jobs[vm_info["vm_name"]]) == 0:
                    logger.info("Azure job \"%s\" - Attempting to start vm %s", azure_job["job_name"], vm_info["vm_name"])
                    async_start = azure_compute_client.virtual_machines.begin_start(vm_info["resource_group_name"], vm_info["vm_name"])
                    async_start.wait()
                
                logger.info("Azure job \"%s\" - Vm %s was started or is already running", azure_job["job_name"], vm_info["vm_name"])

                vm_jobs[vm_info["vm_name"]].append({"job": azure_job, "resource_group_name": vm_info["resource_group_name"]})
                logger.info("Azure job \"%s\" - Running command", azure_job["job_name"])

                ssh_client = paramiko.SSHClient()
                ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

                private_key = paramiko.RSAKey.from_private_key_file(vm_info["pkey_path"])
                ssh_client.connect(vm_info["host"], username=vm_info["username"], pkey=private_key)
                vm_clients[vm_info["vm_name"]].append({"job_type": "Azure", "job_name": azure_job["job_name"], "client": ssh_client, "aws_client": None})

                ssh_client.exec_command(azure_job["command"])

                logger.info("Azure job \"%s\" - Executed command successfully", azure_job["job_name"])

            else:
                continue

        for aws_job in aws_jobs:
            region = aws_schedule_decision(aws_job, percentiles, co2_moers)

            if region != None:
                logger.info("Scheduling AWS job \"%s\" in region %s", aws_job["job_name"], region)
                aws_jobs.remove(aws_job)

                # At this point we are ready to run the VM
                aws_region_client = None
                for aws_client in aws_compute_clients:
                    if aws_client["region"] == region:
                        aws_region_client = aws_client["session"]

                vm_info = get_aws_vm_by_region(aws_vms, region)
                instance_id = vm_info["vm_name"]
                if len(vm_jobs[vm_info["vm_name"]]) == 0:
                    logger.info("AWS job \"%s\" - Attempting to start vm %s", aws_job["job_name"], vm_info["vm_name"])
                    instance = aws_region_client.Instance(instance_id)
                    instance.start()
                    waiter = aws_region_client.meta.client.get_waiter('instance_running')
                    waiter.wait(InstanceIds=[instance_id])

                logger.info("AWS job \"%s\" - Vm %s was started or is already running", aws_job["job_name"], vm_info["vm_name"])

                vm_jobs[vm_info["vm_name"]].append({"job": aws_job, "resource_group_name": None})

                logger.info("AWS job \"%s\" - Running command", aws_job["job_name"])

                ssh_client = paramiko.SSHClient()
                ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

                private_key = paramiko.RSAKey.from_private_key_file(vm_info["pkey_path"])
                ssh_client.connect(vm_info["host"], username=vm_info["username"], pkey=private_key)
                vm_clients[vm_info["vm_name"]].append({"job_type": "AWS", "job_name": aws_job["job_name"], "client": ssh_client, "aws_client": aws_region_client})

                ssh_client.exec_command(aws_job["command"])

                logger.info("AWS job \"%s\" - Executed command successfully", aws_job["job_name"])
            else:
                continue

        # Check if any jobs finished and power off vm if needed
        for vm_name, jobs in vm_jobs.items():
            for job in jobs:
                # Find right ssh client
                ssh_clients = vm_clients[vm_name]
                for ssh_client in ssh_clients:
                    if ssh_client["job_name"] == job["job"]["job_name"]:
                        if ssh_client["job_type"] == "Azure":
                            _, stdout, _ = ssh_client["client"].exec_command(job["job"]["output"])
                            status = stdout.read().decode().strip()
                            if status == 'DONE':
                                logger.info("Azure job \"%s\" - Job is complete.", ssh_client["job_name"])
                                ssh_client["client"].close()
                                jobs.remove(job)
                                vm_clients[vm_name].remove(ssh_client)
                                if len(jobs) == 0:
                                    logger.info("Deallocating Azure VM %s", vm_name)
                                    azure_compute_client.virtual_machines.begin_deallocate(job["resource_group_name"], vm_name).wait()
                        if ssh_client["job_type"] == "AWS":
                            _, stdout, _ = ssh_client["client"].exec_command(job["job"]["output"])
                            status = stdout.read().decode().strip()
                            if status == 'DONE':
                                logger.info("AWS job \"%s\" - Job is complete.", ssh_client["job_name"])
                                ssh_client["client"].close()
                                jobs.remove(job)
                                vm_clients[vm_name].remove(ssh_client)
                                if len(jobs) == 0:
                                    logger.info("Deallocating AWS VM %s", vm_name)
                                    aws_client = ssh_client["aws_client"]
                                    instance = aws_client.Instance(instance_id)
                                    instance.stop()
                                    waiter = aws_region_client.meta.client.get_waiter('instance_stopped')
                                    waiter.wait(InstanceIds=[instance_id])

        # Power off vms if they are idle
        no_jobs = True
        for vm_name, jobs in vm_jobs.items():
            if len(jobs) != 0:
                no_jobs = False

        if len(azure_jobs + aws_jobs) != 0 or not no_jobs:
            time.sleep(60)
        else:
            break

if __name__ == "__main__":
    main()
