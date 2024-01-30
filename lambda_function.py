import pandas as pd
import boto3
import os

# import sys
# import subprocess

# DON'T HAVE TO RUN THE FOLLOWING SUBPROCESS. ADDED OPENPYXL AND PANDAS AS A LAMBDA LAYER
# subprocess.call('pip install cryptography -t /tmp/ --no-cache-dir'.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
# subprocess.call('pip install openpyxl -t /tmp', shell=True)
# sys.path.insert(1, '/tmp/')

S3_OUTPUT_BUCKET = os.environ.get('S3_OUTPUT_BUCKET')
GLUE_CRAWLER = os.environ.get('GLUE_CRAWLER')  # will trigger after CSV extraction
SHEETS = ["Site Emissions Summary Import", "Portfolio Summary Import", "Decarbonization Import - Emiss",
          "Decarbonization Import - Reduct", "Decarbonization Import - Costs", "Decarbn Import Wtrfall S1 S2",
          "Decarbn Import Wtrfall S1 - S3"]  # these sheets will be converted to CSV
EXCLUDED_COLUMNS = ['Total Index Merged', 'Total Index', 'Portfolio Index', 'Year Index', 'Time Period Index',
                    'Pathway Index']  # These columns will be ignored before converting to CSV


def convert_excel_to_csv(event, context):
    print("EVENT ", event)
    # Get the S3 bucket and file key from the event
    input_bucket = event['Records'][0]['s3']['bucket']['name']
    input_key = event['Records'][0]['s3']['object']['key']
    print("input_bucket: ", input_bucket)
    input_key = input_key.replace("%3D", "=")
    print("input_key: ", input_key)
    client, excel_name = input_key.split("/")
    output_bucket = S3_OUTPUT_BUCKET
    print("output_bucket: ", output_bucket)

    # Read the Excel file from S3
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=input_bucket, Key=input_key)
    content = obj['Body'].read()
    excel_file = pd.ExcelFile(content, engine='openpyxl')

    # Loop through all SHEETS in the Excel file
    for sheet_name in excel_file.sheet_names:
        if sheet_name in SHEETS:
            # Read the sheet into a Pandas dataframe
            sheet_df = excel_file.parse(sheet_name)

            # Exclude specific columns if they exist in the dataframe
            columns_to_drop = [col for col in EXCLUDED_COLUMNS if col in sheet_df.columns]
            sheet_df = sheet_df.drop(columns=columns_to_drop)

            csv_buffer = sheet_df.to_csv(index=False)
            sheet_name = sheet_name.replace(" ", "_").lower()
            s3.put_object(Body=csv_buffer, Bucket=output_bucket, Key=f"{sheet_name}/{client}/{sheet_name}.csv")
            print(f"{sheet_name} uploaded to S3 successfully!")

    print("#== CSV extracted successfully ==#")

    # Create a Glue client
    glue_client = boto3.client('glue')

    # Specify the name of the Glue crawler to trigger
    crawler_name = GLUE_CRAWLER

    try:
        # Start the Glue crawler
        response = glue_client.start_crawler(Name=crawler_name)

        # Print the response
        print(f"Glue crawler '{crawler_name}' started successfully.")
        print(response)

        # Return a success message
        # return {
        #     'statusCode': 200,
        #     'body': 'Glue crawler started successfully.'
        # }
    except Exception as e:
        # Print the error message
        print(f"Failed to start Glue crawler '{crawler_name}'. Error: {str(e)}")

        # Return an error message
        # return {
        #     'statusCode': 500,
        #     'body': 'Failed to start Glue crawler.'
        # }
    print("#== Lambda function execution done ==#")