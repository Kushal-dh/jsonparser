import pandas as pd
import json
import argparse
 
# load JSON data from file
def load_json(filename, encoding ):
    try:
        with open(filename, "r", encoding = encoding) as file:
            json_data = json.load(file)
    except FileNotFoundError as e:
        print('please Enter correct path', e)
        return
    else:
        return json_data
 
# def load_json(filename, encoding):
#     with open(filename, encoding= 'latin-1' ) as f:
#         data = json.load(f)
#     return data
# Create DataFrame for normal key-value pair
 
def create_base_dataframe(json_data):
    normal_df = pd.DataFrame({
        'reporting_entity_name': [json_data['reporting_entity_name']],
        'reporting_entity_type': [json_data['reporting_entity_type']],
        'last_updated_on': [json_data['last_updated_on']],
        'version': [json_data['version']],
    })
    return normal_df
 
# dataframe for key and list of array pair
def provider_dataframe(json_data):
    provider_records = []
    for provider in json_data['provider_references']:
        provider_group_id = provider['provider_group_id']
        for provider_group in provider['provider_groups']:
            if 'npi' in provider_group and len(provider_group['npi']) > 0:
                npi = provider_group['npi'][0]
            else:
                npi = None
#             npi = provider_group['npi'][0]
            tin_type = provider_group['tin']['type']
            tin_value = provider_group ['tin']['value']
            provider_records.append({
                'provider_group_id' : provider_group_id,
                'npi' : npi,
                'tin_type' : tin_type,
                'tin_value': tin_value
            })
           
    provider_array_df = pd.DataFrame(provider_records)
    return provider_array_df
 
def in_network_dataframe(json_data):
    network_records = []
    for network in json_data['in_network']:
        negotiation_arrangement = network['negotiation_arrangement']
        name = network['name']
        billing_code_type = network['billing_code_type']
        billing_code_type_version = network['billing_code_type_version']
        billing_code = network['billing_code']
        description = network['description']
       
        for rate in network['negotiated_rates']:
            provider_references = rate['provider_references'][0]
            for price in rate['negotiated_prices']:
                negotiated_type = price['negotiated_type']
                negotiated_rate = price['negotiated_rate']
                expiration_date = price['expiration_date']
                billing_class = price['billing_class']
            if 'service_code' in price and len(price['service_code']) > 0:
                for service_code in price['service_code']:
                    network_records.append({
                        'negotiation_arrangement': negotiation_arrangement,
                        'billing_code_type': billing_code_type,
                        'billing_code_type_version' : billing_code_type_version,
                        'billing_code' : billing_code,
                        'provider_references': provider_references,
                        'negotiated_rate': negotiated_rate,
                        'expiration_date': expiration_date,
                        'billing_class' : billing_class,
                        'service_code' : service_code,
                        'name': name,
                        'description': description
                    })
            else:
                service_code = None
                network_records.append({
                'negotiation_arrangement': negotiation_arrangement,
                'billing_code_type': billing_code_type,
                'billing_code_type_version' : billing_code_type_version,
                'billing_code' : billing_code,
                'provider_references': provider_references,
                'negotiated_rate': negotiated_rate,
                'expiration_date': expiration_date,
                'billing_class' : billing_class,
                'service_code' : service_code,
                'name': name,
                'description': description
            })
#                 for service_code in price['service_code']:
#                     network_records.append({
#                         'negotiation_arrangement': negotiation_arrangement,
#                         'billing_code_type': billing_code_type,
#                         'billing_code_type_version' : billing_code_type_version,
#                         'billing_code' : billing_code,
#                         'provider_references': provider_references,
#                         'negotiated_rate': negotiated_rate,
#                         'expiration_date': expiration_date,
#                         'billing_class' : billing_class,
#                         'service_code' : service_code,
#                         'name': name,
#                         'description': description
#                     })
    network_array_df = pd.DataFrame(network_records)
    return network_array_df
 
# merging normal DataFrame with provider DataFrame and in_network
def merge_dataframe(a, b, c):
    final_df = a.join(pd.concat( [b] + [c], axis=1), how = 'cross')
    return final_df
 
# writing into csv
def make_csv(df, name):
#     return df
    try:
        df.to_csv( name +'.csv', index = False)
    except:
        print(f"couldnot convert into csv")
    else:
        print(f'File {name}.csv succesfully written')
 
def make_dataframe(data):
    df1 = create_base_dataframe(data)
    df2 = provider_dataframe(data)
    df3 = in_network_dataframe(data)
    return merge_dataframe(df1, df2, df3)
 
def main(json_file, csv_file, encoding = 'utf-8'):
    data = load_json(json_file, encoding)
#     data = json_file
    if data == None:
        return
    data_frame = make_dataframe(data)
    make_csv(data_frame, csv_file)
 

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="add json and csv file path")
   
    parser.add_argument(
        "--jsonfile",
        type=str,
        required= True,
        help="your json file path"
    )

    parser.add_argument(
        "--csvname", 
        # nargs="*",  
        type=str, 
        required=True,
        help="Your output csv name"
    )

    args = parser.parse_args()
    json_file = args.jsonfile
    csv_file = args.csvname
    main(json_file, csv_file)