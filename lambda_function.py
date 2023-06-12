import pandas as pd
import boto3
# import sys
# import subprocess

# DON'T HAVE TO RUN THE FOLLOWING SUBPROCESS. ADDED OPENPYXL AND PANDAS AS A LAMBDA LAYER
# subprocess.call('pip install cryptography -t /tmp/ --no-cache-dir'.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
# subprocess.call('pip install openpyxl -t /tmp', shell=True)
# sys.path.insert(1, '/tmp/')

def convert_excel_to_csv(event, context):
    # Get the S3 bucket and file key from the event
    input_bucket = event['Records'][0]['s3']['bucket']['name']
    input_key = event['Records'][0]['s3']['object']['key']
    print("input_bucket: ", input_bucket)
    input_key = input_key.replace("%3D","=")
    print("input_key: ", input_key)
    client, dataload, excel_name = input_key.split("/")
    output_bucket = 'bts-data-esg-csv'
    output_folder = f"{client}/{dataload}"
    print("output_bucket: ", output_bucket)
    print("output_folder: ", output_folder)
    
    sheets = ["Site Emissions Summary", "Portfolio Emissions Summary", "Decarbonization Model", "Abatement Activities ->", "Decarb - Energy Efficiency", "Decarb - RECs", "Decarb - Offsets", "Decarb - Electric Fleet", "Decarb - Renewable Fuels", "Decarb - Solar Panels", "Decarb - Capital Improvement", "Decarb - Business Travel", "Decarb - Commute-Virtual Office", "Scope 1 - Natural Gas", "Scope 1 - Liquid Fuels", "Scope 2 - Electricity", "Scope 1 - Fugitive Emissions", "Scope 1 - Process Emissions", "Scope 1 - Fleet Vehicles", "Scope 1 - Other Mobile Equip.", "Scope 2 - District Heating", "1 - Purchased Goods & Services", "2 - Capital Goods", "3 - Fuel & Energy Activities", "4 - Upstream Transp. & Dist", "5 - Waste Generation", "6 - Business Travel", "7- Employee Commuting", "8 - Upstream Leased Assets", "9 - Downstream Transp. & Dist", "10 - Processing of Sold Product", "11 - Use of Sold Products", "12 - End of Life Treatment", "13 - Downstream Leased Assets", "14 - Franchises", "15 - Investments"]
    
    # Read the Excel file from S3
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=input_bucket, Key=input_key)
    content = obj['Body'].read()
    excel_file = pd.ExcelFile(content)

    # Loop through all sheets in the Excel file
    for sheet_name in excel_file.sheet_names:
        if sheet_name in sheets:
            # Read the sheet into a Pandas dataframe
            sheet_df = excel_file.parse(sheet_name)
            csv_buffer = sheet_df.to_csv(index=False)
            s3.put_object(Body=csv_buffer, Bucket=output_bucket, Key=f"{output_folder}/{sheet_name}/{sheet_name}.csv")
