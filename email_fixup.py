import os
import json
import subprocess
import argparse
import difflib
import csv
import boto3
import botocore
from string import whitespace

dryrun = True


def getRoute53Zones():
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
    return zones


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


def list_domains_with_records_and_save_to_disk():
    zones = getRoute53Zones()

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


def compare_file_to_route53(filename):
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
        for line in difflib.unified_diff(saved_str.splitlines(), current_str.splitlines(), lineterm='',
                                         fromfile='saved', tofile='current'):
            print(line)
    else:
        print(f"No differences in {filename}")


def compare_delta(zone_id=None):

    for filename in os.listdir('.'):
        if filename.endswith('_records.json'):
            if zone_id:
                if filename.startswith(zone_id):
                    compare_file_to_route53(filename)
            else:
                compare_file_to_route53(filename)

def route53_updateCommand(zone_id, record):
    #print(f"Update zoneid {zone_id}, changebatch: {record}")
    update_command = f"aws route53 change-resource-record-sets --hosted-zone-id {zone_id} --change-batch '{json.dumps({'Changes': [{'Action': 'UPSERT', 'ResourceRecordSet': record}]})}'"
    if(dryrun is True):
        print(f"DRYRUN: Command would have been: \r\n{update_command}\r\n")
    else:
        print(f"About to execute Command: \r\n{update_command}\r\n")
        result = subprocess.run(update_command, shell=True, capture_output=True)
        if result.returncode != 0:
            print(f"Command failed: {result.stderr.decode()}")
        print(result.stdout.decode())


def update_spf_txt_record(zone_id, record_name, new_spf_record):
    route53_client = boto3.client('route53')
    try:
        records = route53_client.list_resource_record_sets(HostedZoneId=zone_id)

        #AWS only has 1 record type per Name, it won't allow more than 1.
        txt_records = [r for r in records['ResourceRecordSets']
                       if r['Name'] == f"{record_name}." and r['Type'] == 'TXT']
        #print(f"txt_records: {txt_records}")

        # Modify the record
        if len(txt_records) >= 1:
            spf_record_count = len([value['Value'] for value in txt_records[0]['ResourceRecords'] if value['Value'].startswith('"v=spf1')])
            #print(f"spf_record_count: {spf_record_count}")

            if spf_record_count > 1:
                print(f"Error: {record_name} has multiple spf records. Please manually fix!!")
            else:
                # update SPF record if not the same
                r = txt_records[0]

                # Append additional spf record
                if spf_record_count == 0:
                    r['ResourceRecords'].append(
                        {"Value": f'"{new_spf_record}"'}
                    )
                    route53_updateCommand(zone_id, r)
                else:
                    for value in r['ResourceRecords']:
                        value_cleansed = value['Value'].strip(whitespace + '"\'')
                        if value_cleansed == new_spf_record.strip(whitespace + '"\''):
                            print("Existing SPF record matches the new record.")
                        elif value_cleansed.startswith("v=spf1"):
                            print("Existing SPF don't match updating")
                            value['Value'] = f'"{new_spf_record}"'
                            route53_updateCommand(zone_id, r)
        else:
            # No TXT records exist, so add new record
            r = {
                    "Name": f"{record_name}.",
                    "Type": "TXT",
                    "TTL": 300,
                    "ResourceRecords": [{"Value": f'"{new_spf_record}"'}]
            }
            route53_updateCommand(zone_id, r)

    except botocore.exceptions.ClientError as error:
        print(f"An error occurred: {error}")

def update_dmarc_txt_record(zone_id, record_name, new_dmarc_record):
    route53_client = boto3.client('route53')
    try:
        records = route53_client.list_resource_record_sets(HostedZoneId=zone_id)

        # Filter for DMARC TXT records
        txt_records = [r for r in records['ResourceRecordSets']
                       if r['Name'] == f"_dmarc.{record_name}." and r['Type'] == 'TXT']
        #print(f"txt_records: {txt_records}")
        if txt_records:
            # AWS only has 1 type per name.
            r = txt_records[0]

            dmarc_record_count = len(
                [value['Value'] for value in r['ResourceRecords'] if value['Value'].startswith('"v=DMARC1')])
            # print(f"dmarc_record_count: {dmarc_record_count}")

            if dmarc_record_count > 1:
                print(
                    f"{record_name} Warning: Multiple DMARC records found. This is typically a misconfiguration. Please fix Manually")
            else:

                # Append additional spf record
                if dmarc_record_count == 0:
                    r['ResourceRecords'].append(
                        {"Value": f'"{new_dmarc_record}"'}
                    )
                    route53_updateCommand(zone_id, r)
                else:
                    # Update logic for the existing record
                    for value in r['ResourceRecords']:
                        value_cleansed = value['Value'].strip(whitespace + '"\'')
                        if value_cleansed == new_dmarc_record.strip(whitespace + '"\''):
                            print("Existing DMARC record matches the new record.")
                        elif value_cleansed.startswith("v=DMARC1"):
                            print("Existing DMARC record don't match updating")
                            value['Value'] = f'"{new_dmarc_record}"'
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
    zones = getRoute53Zones()
    with open(file_name, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if not any(record['Id'] == f"/hostedzone/{row['hostedzoneid']}" for record in zones):
                print(f"id:{row['hostedzoneid']}, domain:{row['domain']} does not exist in this account, skipping")
            else:
                hosted_zone_id = get_resource_record_sets(row['hostedzoneid'])
                if not hosted_zone_id:
                    print(f"Hosted zone not found for {row['hostedzoneid']}")
                    continue
                print(f"updating: {row['hostedzoneid']}, {row['domain']}") #, \"{row['spf_value']}\", \"{row['dmarc_txt']}\"")
                update_spf_txt_record(row['hostedzoneid'], row['domain'], row['spf_value'])
                update_dmarc_txt_record(row['hostedzoneid'], row['domain'], row['dmarc_txt'])

def main():
    parser = argparse.ArgumentParser(description='Route53 Domain Operations with Records')
    parser.add_argument('--commit', action='store_true', help='If set will action changes')
    parser.add_argument('--list', action='store_true', help='List and export domains with records to JSON files')
    parser.add_argument('--compare', action='store_true', help='Compare saved domain files with current state in Route53')
    parser.add_argument('--zoneid',  help='Compare save zoneid domain file with current state in Route53')
    parser.add_argument('--csv', action='store_true', help='Use csv file to update dmarc, spf records in Route53')
    parser.add_argument('--file', help='the csv file, required headers are, hostedzoneid, domain, spf_value, dmarc_txt')
    args = parser.parse_args()

    if args.commit:
        global dryrun
        dryrun = False

    if args.list:
        list_domains_with_records_and_save_to_disk()
    elif args.compare and args.zoneid:
        compare_delta(args.zoneid)
    elif args.compare:
        compare_delta()
    elif args.csv and args.file is not None:
        csv_update(args.file)
    else:
        print(f"--help to see options")

if __name__ == "__main__":
    main()

