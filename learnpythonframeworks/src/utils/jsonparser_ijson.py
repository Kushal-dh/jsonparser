import ijson
import os
import argparse
import csv
import aiohttp
import asyncio
import time


def is_file_empty(file_path):
    # check if the file exists if it does is the file empty to write header in csv
    if os.path.exists(file_path):
        return os.stat(file_path).st_size == 0
    else:
        return True



def initialize_dict_store_content():
    # make five empty dictionaries to store only one content at a time
    json_data = {'reporting_entity_name': '', 'reporting_entity_type': '', 'last_updated_on': '', 'version': ''}
    provider_references = {'provider_group_id': ''}
    npi = {'npi': [], 'tin_type': '', 'tin_value': ''}
    in_network = {'negotiation_arrangement': '', 'name': '', 'billing_code_type': '', 'billing_code_type_version': '',
                  'billing_code': '', 'description': ''}
    negotiated_rates = {'provider_references': '', 'negotiated_type': '', 'negotiated_rate': '', 'expiration_date': '',
                        'billing_class': '','billing_code_modifier': '', 'service_code': []}
    return {
        'json_data': json_data,
        'provider_references': provider_references,
        'npi': npi,
        'in_network': in_network,
        'negotiated_rates': negotiated_rates
    }

def parse_arguments():

    parser = argparse.ArgumentParser(description='Convert JSON file to CSV.')
    parser.add_argument('json_file_path', type=str, help='Path to the input JSON file')
    parser.add_argument('provider_file_path', type=str, help='Path to the provider output CSV file')
    parser.add_argument('in_network_file_path', type=str, help='Path to the in_network output CSV file')
    return parser.parse_args()

dict_1_keys = ('reporting_entity_name', 'reporting_entity_type', 'last_updated_on', 'version', 'provider_group_id', 'npi', 'tin_type', 'tin_value')
dict_2_keys = ('negotiation_arrangement', 'name', 'billing_code_type', 'billing_code_type_version', 'billing_code', 'description','provider_references', 'negotiated_type', 'negotiated_rate', 'expiration_date' ,'billing_class','billing_code_modifier', 'service_code')

prefix_mapping = {
    'reporting_entity_name': ('json_data', 'reporting_entity_name'),
    'reporting_entity_type': ('json_data', 'reporting_entity_type'),
    'last_updated_on': ('json_data', 'last_updated_on'),
    'version': ('json_data', 'version'),
    'provider_references.item.provider_group_id': ('provider_references', 'provider_group_id'),
    'provider_references.item.provider_groups.item.npi.item': ('npi', 'npi'),
    'provider_references.item.provider_groups.item.tin.type': ('npi', 'tin_type'),
    'provider_references.item.provider_groups.item.tin.value': ('npi', 'tin_value'),
    'in_network.item.negotiation_arrangement': ('in_network', 'negotiation_arrangement'),
    'in_network.item.name': ('in_network', 'name'),
    'in_network.item.billing_code_type': ('in_network', 'billing_code_type'),
    'in_network.item.billing_code_type_version': ('in_network', 'billing_code_type_version'),
    'in_network.item.billing_code': ('in_network', 'billing_code'),
    'in_network.item.description': ('in_network', 'description'),
    'in_network.item.negotiated_rates.item.provider_references.item': ('negotiated_rates', 'provider_references'),
    'in_network.item.negotiated_rates.item.negotiated_prices.item.negotiated_type': ('negotiated_rates', 'negotiated_type'),
    'in_network.item.negotiated_rates.item.negotiated_prices.item.negotiated_rate': ('negotiated_rates', 'negotiated_rate'),
    'in_network.item.negotiated_rates.item.negotiated_prices.item.expiration_date': ('negotiated_rates', 'expiration_date'),
    'in_network.item.negotiated_rates.item.negotiated_prices.item.billing_class': ('negotiated_rates', 'billing_class'),
    'in_network.item.negotiated_rates.item.negotiated_prices.item.billing_code_modifier.item':('negotiated_rates', 'billing_code_modifier'),
    'in_network.item.negotiated_rates.item.negotiated_prices.item.service_code.item': ('negotiated_rates', 'service_code')

}

def update_dictionary(data_container, data_structure_name, key, value):
    # assign values to dictionary keys
    data_structure = data_container[data_structure_name]
    if key == 'service_code' or key == 'npi':
        data_structure[key].append(value)
    else:
        data_structure[key] = value

async def fetch_provider_data_from_url(session, url):
    try:
        async with session.get(url) as response:
            if response.status == 200:
                return await response.json()
            else:
                print(f"Failed to fetch data from URL: {url}. Status code: {response.status}")
                return None
    except aiohttp.ClientError as e:
        print(f"An error occurred while fetching data from URL: {url}. Error: {e}")
        return None


async def main(json_file_path, provider_file_path, in_network_file_path):
    provider_count = 0
    in_network_count = 0
    location = False

    batch_size = 100
    provider_batch = []
    in_network_batch = []
    async with aiohttp.ClientSession() as session:
        with open(json_file_path, 'r', encoding='utf-8') as file, open(provider_file_path, 'a', newline='') as provider_file, open(in_network_file_path, 'a', newline='') as in_network_file:
            writer_1 = csv.writer(provider_file)
            writer_2 = csv.writer(in_network_file)

            if is_file_empty(provider_file_path):
                writer_1.writerow(dict_1_keys)
            if is_file_empty(in_network_file_path):
                writer_2.writerow(dict_2_keys)
            parser = ijson.parse(file)

            data_container = initialize_dict_store_content()

            for prefix, event, value in parser:
                if (prefix in prefix_mapping) and (event in ('string', 'number', 'boolean', 'null')):
                    data_structure_name, key = prefix_mapping[prefix]
                    update_dictionary(data_container, data_structure_name, key, value)
               
                elif prefix == 'provider_references.item.location' and event == 'string':
                    location = True
                    url = value
                    provider_data = await fetch_provider_data_from_url(session, url)
                    if provider_data is not None:
                        # Store the fetched provider data in the url_data_mapping
                        data_container['npi'] = {'npi': provider_data['provider_groups'][0]['npi'], 'tin_type': provider_data['provider_groups'][0]['tin']['type'], 'tin_value': provider_data['provider_groups'][0]['tin']['value']}

                # elif prefix == 'provider_references.item' and event == 'end_map':
                elif not location and prefix == 'provider_references.item.provider_groups.item.tin' and event == 'end_map':
                    provider_batch.append({**data_container['json_data'], **data_container['provider_references'], **data_container['npi']}.values())
                    data_container['npi'] = {'npi': [], 'tin_type': '', 'tin_value': ''}
                    provider_count += 1

                elif location and prefix == 'provider_references.item' and event == 'end_map':
                    # -------------writing into file provider references by merging 3 dictionaries---------------------------
                    provider_batch.append({**data_container['json_data'], **data_container['provider_references'], **data_container['npi']}.values())
                    # ------------emptying dictionary-----------------------------------------------
                    data_container['npi'] = {'npi': '', 'tin_type': '', 'tin_value': ''}
                    provider_count += 1
                    location = False

                elif prefix == 'in_network.item.negotiated_rates' and event == 'end_array':

                    #-----------------writing into in_network file by merging two dictionaries ---------------------------------
                    in_network_batch.append({**data_container['in_network'], **data_container['negotiated_rates']}.values())

                    # ---------------emptying two dictionaries---------------------------------------------------------------
                    data_container['in_network'] = {'negotiation_arrangement': '', 'name': '', 'billing_code_type': '', 'billing_code_type_version': '', 'billing_code': '', 'description': ''}
                    data_container['negotiated_rates'] = {'provider_references': '', 'negotiated_type': '', 'negotiated_rate': '', 'expiration_date': '', 'billing_class': '', "billing_code_modifier": '', 'service_code': []}
                    in_network_count += 1
                
                
                if len(provider_batch) == batch_size:
                    writer_1.writerows(provider_batch)
                    provider_batch = []
                if len(in_network_batch) == batch_size:
                    writer_2.writerows(in_network_batch)
                    in_network_batch = []
                # Write any remaining data in the last batch
            if provider_batch:
                writer_1.writerows(provider_batch)

            if in_network_batch:
                writer_2.writerows(in_network_batch)
        
if __name__ == '__main__':
    args = parse_arguments()

    start_time = time.time()

    asyncio.run(main(args.json_file_path, args.provider_file_path, args.in_network_file_path))
    end_time = time.time()

    elapsed_time = end_time - start_time
    print(f'Time taken: {elapsed_time} seconds')
