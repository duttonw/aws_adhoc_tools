import os
import json
import subprocess
import argparse
import difflib
import csv
import boto3
import botocore

dryrun = True

def get_resource_record_sets(zone_id):
    record_sets = []
    starting_token = None
    try:
        while True:
            cmd = f"aws route53 list-resource-record-sets --hosted-zone-id {zone_id} --max-items 300"
            if starting_token:
                cmd += f" --starting-token {starting_token}"
            result = subprocess.check_output(cmd, shell=True)
            data = json.loads(result)
            record_sets.extend(data.get('ResourceRecordSets', []))

            starting_token = data.get('NextToken')
            if not starting_token:
                break
    except:
        print("zone_id:" + zone_id + " Does not exist")

    return record_sets

def list_domains_with_records():
    zones = []
    starting_token = None
    while True:
        cmd = "aws route53 list-hosted-zones --max-items 100"
        if starting_token:
            cmd += f" --starting-token {starting_token}"
        result = subprocess.check_output(cmd, shell=True)
        data = json.loads(result)
        zones.extend(data.get('HostedZones', []))

        starting_token = data.get('NextToken')
        if not starting_token:
            break

    for zone in zones:
        zone_id = zone['Id'].split('/')[-1]
        zone_name = zone['Name'].rstrip('.')
        filename = f"{zone_id}_{zone_name}_records.json"

        record_sets = get_resource_record_sets(zone_id)
        zone_data = {
            'HostedZone': zone,
            'ResourceRecordSets': record_sets
        }

        with open(filename, 'w') as file:
            json.dump(zone_data, file, indent=4)
        print(f"Exported: {filename}")

def compare_delta():
    for filename in os.listdir('.'):
        if filename.endswith('_records.json'):
            with open(filename, 'r') as file:
                saved_zone_data = json.load(file)

            saved_zone_id = saved_zone_data['HostedZone']['Id'].split('/')[-1]
            current_record_sets = get_resource_record_sets(saved_zone_id)

            current_zone_data = {
                'HostedZone': saved_zone_data['HostedZone'],  # Assuming HostedZone data remains constant
                'ResourceRecordSets': current_record_sets
            }

            saved_str = json.dumps(saved_zone_data, indent=4)
            current_str = json.dumps(current_zone_data, indent=4)

            if saved_str != current_str:
                print(f"Differences found in {filename}:")
                for line in difflib.unified_diff(saved_str.splitlines(), current_str.splitlines(), lineterm='', fromfile='saved', tofile='current'):
                    print(line)
            else:
                print(f"No differences in {filename}")

def update_spf_txt_record(zone_id, record_name, new_spf_record):
    print("zone:" + zone_id + " record_name:" + record_name + " spf:" + new_spf_record)
    route53_client = boto3.client('route53')
    try:
        records = route53_client.list_resource_record_sets(HostedZoneId=zone_id)

        txt_records = [r for r in records['ResourceRecordSets']
                       if r['Name'] == f"{record_name}." and r['Type'] == 'TXT']

        print(f"txt_records: {txt_records}")

        # Modify the record
        if len(txt_records) >= 1:
            count = 0

            # update SPF record if not the same
            for r in records['ResourceRecordSets']:
                if r['Name'] == f"{record_name}." and r['Type'] == 'TXT':
                    for value in r['ResourceRecords']:
                        if value['Value'].startswith("v=spf1"):
                            count = count + 1
                    # Append additional spf record
                    if count == 0:
                        r['ResourceRecords'].append(
                            {"Value": f'"{new_spf_record}"'}
                        )
                        route53_updateCommand(zone_id, r)
                    if count > 1:
                        print(f"Error: {record_name} has multiple spf records. Please manually fix!!")
                        break
                    for value in r['ResourceRecords']:
                        if value['Value'] == new_spf_record:
                            print("Existing SPF record matches the new record.")
                        elif value['Value'].startswith("v=spf1"):
                            value['Value'] = f'"{new_spf_record}"'
                            route53_updateCommand(zone_id, r)

        else:
            # Not TXT records exist, so add new record
            r = {
                    "Name": f"{record_name}.",
                    "Type": "TXT",
                    "TTL": 300,
                    "ResourceRecords": [
                        {
                            "Value": f'"{new_spf_record}"'
                        }
                    ]
            }
            route53_updateCommand(zone_id, r)

    except botocore.exceptions.ClientError as error:
        print(f"An error occurred: {error}")

def route53_updateCommand(zone_id, record):
    print(f"Update zoneid {zone_id}, changebatch: {record}")
    update_command = f"aws route53 change-resource-record-sets --hosted-zone-id {zone_id} --change-batch '{json.dumps({'Changes': [{'Action': 'UPSERT', 'ResourceRecordSet': record}]})}'"
    if(dryrun is True):
        print(f"DRYRUN: Command would have been: \r\n{update_command}\r\n")
    else:
        print(f"About to execute Command: \r\n{update_command}\r\n")
        result = subprocess.run(update_command, shell=True, capture_output=True)
        if result.returncode != 0:
            raise Exception(f"Command failed: {result.stderr.decode()}")
        return result.stdout.decode()


def update_dmarc_txt_record(zone_id, record_name, new_dmarc_record):
    route53_client = boto3.client('route53')
    try:
        records = route53_client.list_resource_record_sets(HostedZoneId=zone_id)

        # Filter for DMARC TXT records
        dmarc_records = [r for r in records['ResourceRecordSets']
                         if r['Name'] == f"_dmarc.{record_name}." and r['Type'] == 'TXT'
                         and any("v=DMARC1" in value['Value'] for value in r['ResourceRecords'])]
        print(f"dmarc_records: {dmarc_records}")
        if dmarc_records:
            if len(dmarc_records) > 1:
                print("{record_name} Warning: Multiple DMARC records found. This is typically a misconfiguration. Please fix Manually")

            # Assuming we choose the first DMARC record to update
            existing_record = dmarc_records[0]
            if existing_record['ResourceRecords'][0]['Value'].strip('"') == new_dmarc_record:
                print("Existing DMARC record matches the new record.")
            else:
                # Update logic for the existing record
                for r in records['ResourceRecordSets']:
                    if r['Name'] == f"_dmarc.{record_name}." and r['Type'] == 'TXT':
                        dmarc_found = False
                        for value in r['ResourceRecords']:
                            if value['Value'].startswith("v=DMARC1"):
                                value['Value'] = f'"{new_dmarc_record}"'
                                dmarc_found = True
                                route53_updateCommand(zone_id, r)
                                break
                        if dmarc_found == False:
                            r['ResourceRecords'].append(
                                {"Value": f'"{new_dmarc_record}"'}
                            )
                            route53_updateCommand(zone_id, r)

        else:
            #logic for a new DMARC record
            change_batch = {
                        'Name': f"_dmarc.{record_name}.",
                        'Type': 'TXT',
                        'TTL': 300,
                        'ResourceRecords': [{'Value': f'"{new_dmarc_record}"'}]
                    }

            route53_updateCommand(zone_id, change_batch)
    except botocore.exceptions.ClientError as error:
        print(f"An error occurred: {error}")


def csv_update(file_name):
    with open(file_name, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            hosted_zone_id = get_resource_record_sets(row['hostedzoneid'])
            if not hosted_zone_id:
                print(f"Hosted zone not found for {row['hostedzoneid']}")
                continue
            update_spf_txt_record(row['hostedzoneid'], row['domain'], row['spf_value'])
            update_dmarc_txt_record(row['hostedzoneid'], row['domain'], row['dmarc_txt'])

def main():
    parser = argparse.ArgumentParser(description='Route53 Domain Operations with Records')
    parser.add_argument('--commit', action='store_true', help='If set will action changes')
    parser.add_argument('--list', action='store_true', help='List and export domains with records to JSON files')
    parser.add_argument('--compare', action='store_true', help='Compare saved domain files with current state in Route53')
    parser.add_argument('--csv', action='store_true', help='Use csv file to update dmarc, spf records in Route53')
    parser.add_argument('--file', help='the csv file, required headers are, hostedzoneid, domain, spf_value, dmarc_txt')
    args = parser.parse_args()

    if args.commit:
        global dryrun
        dryrun = False

    if args.list:
        list_domains_with_records()
    elif args.compare:
        compare_delta()
    elif args.csv and args.file is not None:
        csv_update(args.file)
    else:
        print(f"--help to see options")

if __name__ == "__main__":
    main()

