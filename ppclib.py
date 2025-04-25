# Fetch PPC data files 1 day as input_day
import requests
import os
def fetch_ppc_data(input_date):
    """Fetch PPC data from API based on input date and return results as dictionary
    
    Args:
        input_date (str): Date string in format "YYYY-MM-DD"
    
    Returns:
        dict: Dictionary containing status code, response data/error
    """
    url = "http://th2sroeeii2.TH.utacgroup.com/APISAWParameter/api/PPCData/postData"
    payload = {
        "startdate": input_date
    }
    headers = {
        "Content-Type": "application/json"
    }
    
    response = requests.post(url, json=payload, headers=headers)
    
    result = {
        'status_code': response.status_code,
        'data': None,
        'error': None,
        'raw_text': None
    }
    
    try:
        result['data'] = response.json()
    except ValueError:
        result['error'] = "Failed to parse JSON response"
        result['raw_text'] = response.text
    
    return result

# Fetch PPC data files days back
import pandas as pd
import os  # Added missing import
from datetime import datetime, timedelta
def fetch_ppc_days_back(start_date_str, daysback):
    # Set initial date and convert to datetime object
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    
    # Define output directory
    output_dir = 'D:/ppc_saw/myfolder/'
    
    # List to hold output file paths
    output_files = []
    
    # Loop through consecutive days backward
    for i in range(daysback):
        current_date = start_date - timedelta(days=i)
        current_date_str = current_date.strftime("%Y-%m-%d")
        output_file = os.path.join(output_dir, f"{current_date_str}.xlsx")
        output_files.append(output_file)  # Add path to the list
        
        # Skip if file already exists
        if os.path.exists(output_file):
            print(f"Skipping {current_date_str} (file already exists)")
            continue
        
        print(f"Fetching data for {current_date_str}...")
        
        try:
            # Fetch data for the current date
            api_response = fetch_ppc_data(current_date_str)
            data = api_response['data']
            
            # Create DataFrame
            df = pd.DataFrame(data)
            
            # Convert datetime column
            df['CreateTime'] = pd.to_datetime(df['CreateTime'], format='ISO8601')
            
            # Save to Excel
            df.to_excel(output_file, index=False)
            print(f"Saved: {output_file}")
        
        except Exception as e:
            print(f"Error processing {current_date_str}: {str(e)}")
    
    return output_files  # Return the list of all output file paths

# Import Data Profile Excel file name 'ProcessParasProfileUTL'
import polars as pl
def get_data_profile():
    # Specify the path to your Excel file and the sheet names to import
    excel_path = 'PPP.xlsx'
    sheet_names = ['ProcessParasProfileUTL']
    
    # Read each specified sheet into a dictionary of DataFrames
    dfs2 = {}
    for sheet in sheet_names:
        dfs2[sheet] = pl.read_excel(
            excel_path,
            sheet_name=sheet  # Specifies which sheet to read
        )
    
    # Access the DataFrame using the sheet name as the key
    df4 = dfs2['ProcessParasProfileUTL']
    
    # Convert the column to string (Utf8) for Polar dataframe to avoid Error
    df4 = df4.with_columns(
        pl.col("ParaKey").cast(pl.Utf8)  # Cast to string
    )
    return df4

# Consolidate all 'PPCDataUTL' list of Excel files in working folder to one Polar dataframe
def combine_ppc_dataframe(ppc_file_list):
    # Initialize a list to collect DataFrames
    dfs_list = []
    
    # Loop through each file in the folder
    for filename in ppc_file_list: 
        if filename.endswith('.xlsx'):  # Process only .xlsx files
            #excel_path = os.path.join(folder_path, filename)
            
            try:
                # Read the Excel file (defaults to first sheet)
                df = pl.read_excel(filename)
                dfs_list.append(df)
                print(f"Appended: {filename} done.")
                print(df.schema)
                
            except Exception as e:
                print(f"Error reading file {filename}: {e}")
                continue
    
    # Combine all DataFrames (handles empty case gracefully)
    df3 = pl.concat(dfs_list) if dfs_list else pl.DataFrame()
    
    # Drop columns and sort
    if not df3.is_empty():
        df3 = (
            df3
            .drop(['EquipOpn', 'ULotID', 'EventID'])  # Remove columns
            .sort(by=['EquipID', 'CreateTime'])  # Sort
            # .with_row_count("index")  # Optional: Add index column if needed
        )

    print("Data has been read from all files and combined into df3")
    return df3

# Create ECID and SVID blank Polar dataframe
def blank_svid_ecid():
    # Define the column names and their corresponding data types
    col_ECID = {'EquipID':'object', 'CreateTime':'datetime64[ns]','CreateTimeUnix':'int64','EventDesc':'object','4280':'int64','4290':'int64','6603':'int64','6611':'int64',
                '6607':'int64','6615':'int64','4628':'int64','4629':'int64','6641':'int64','16009':'int64','16058':'int64',
                '6640':'int64','16008':'int64','16057':'int64','6636':'int64','16004':'int64','16053':'int64','6637':'int64',
                '16005':'int64','16054':'int64','6666':'int64','16034':'int64','16132':'int64','4204':'int64','4205':'int64'}
    
    # Map Pandas-style data types to Polars data types
    dtype_mapping = {
        'object': pl.Utf8,
        'datetime64[ns]': pl.Datetime,
        'float32': pl.Float32,
        'int64': pl.Int64
    }
    
    # Create a schema dictionary with Polars data types
    polars_schema = {
        col: dtype_mapping[dtype] 
        for col, dtype in col_ECID.items()
    }
    
    # Initialize an empty DataFrame with the specified schema
    ECID = pl.DataFrame({
        col: pl.Series(name=col, dtype=dt) 
        for col, dt in polars_schema.items()
    })
    
    #print(ECID)
    
    # Define the column names and their corresponding data types
    col_SVID = {'EquipID':'object',   'CreateTime':'datetime64[ns]','CreateTimeUnix':'int64','EventDesc':'object','1404':'int64','1405':'int64','3223':'int64','1412':'int64',
                '1413':'int64','1400':'int64','1401':'int64','1763':'int64','1765':'int64','1352':'int64','1353':'int64',
                '1771':'int64','1775':'int64','1502':'int64','1503':'int64','1760':'int64','1759':'int64','1755':'int64',
                '1756':'int64','1500':'int64','1501':'int64','1785':'int64','1764':'int64','1766':'int64'}
    
    # Map Pandas-style data types to Polars data types
    dtype_mapping = {
        'object': pl.Utf8,
        'datetime64[ns]': pl.Datetime,
        'float32': pl.Float32,
        'int64': pl.Int64
    }
    
    # Create a schema dictionary with Polars data types
    polars_schema = {
        col: dtype_mapping[dtype] 
        for col, dtype in col_SVID.items()
    }
    
    # Initialize an empty DataFrame with the specified schema
    SVID = pl.DataFrame({
        col: pl.Series(name=col, dtype=dt) 
        for col, dt in polars_schema.items()
    })
    
    #print(SVID)
    return SVID,ECID

def get_parameter(s):
    pairs = s.split(',')
    parameter = {}
    for pair in pairs:
        key_str, value_str = pair.split(':')
        key = int(key_str)
        if value_str != 'System.Byte[]':
            value = int(value_str)
            parameter[key] = value
    return parameter

from tqdm import tqdm
import polars as pl
def param_spliting(df3):

    # Create ECID and SVID blank Polar dataframe
    SVID,ECID = blank_svid_ecid()

    # Initialize lists to collect new rows for SVID and ECID
    svid_rows = []
    ecid_rows = []
    
    # Iterate over each row in df3 with a progress bar
    for row in tqdm(df3.iter_rows(named=True), desc="Processing rows"):
        # Extract the parameters string from the 'parameters' column (adjust column name if necessary)
        param_str = row['Parameter']
        # Parse the parameters string into a dictionary
        param_dict = get_parameter(param_str)
        # Add additional columns from the current row
        param_dict.update({
            'EquipID': row['EquipID'],
            'CreateTime': row['CreateTime'],
            'EventDesc': row['EventDesc']
        })
        # Convert all keys to strings
        param_dict = {str(k): v for k, v in param_dict.items()}
        
        # Check for SVID record (key '1404' with value > 0)
        svid_value = param_dict.get('1404', 0)
        if svid_value > 0:
            # Create a row with columns matching SVID's schema, filling missing keys with None
            svid_row = {col: param_dict.get(col, None) for col in SVID.columns}
            svid_rows.append(svid_row)
        
        # Check for ECID record (key '4280' with value > 0)
        ecid_value = param_dict.get('4280', 0)
        if ecid_value > 0:
            # Create a row with columns matching ECID's schema, filling missing keys with None
            ecid_row = {col: param_dict.get(col, None) for col in ECID.columns}
            ecid_rows.append(ecid_row)

    SVID = pd.DataFrame(svid_rows)
    SVID['CreateTimeUnix'] = SVID['CreateTime'].astype('int64') // 10**9
    SVID.sort_values(by=['EquipID', 'CreateTime'])
    #SVID_polars = pl.from_pandas(SVID)
    SVID = pl.from_pandas(SVID)
    
    ECID = pd.DataFrame(ecid_rows)
    ECID['CreateTimeUnix'] = ECID['CreateTime'].astype('int64') // 10**9
    ECID.sort_values(by=['EquipID', 'CreateTime'])
    #ECID_polars = pl.from_pandas(ECID)
    ECID = pl.from_pandas(ECID)
    
    df3 = df3.with_columns(
        pl.col("CreateTime").dt.epoch('s').alias("CreateTimeUnix")
    )
    return (df3,SVID,ECID)

if __name__ == "__main__":
    test_date = "2025-03-21"
    api_response = fetch_ppc_data(test_date)

    print(f"Status Code: {api_response['status_code']}")
    if api_response['data']:
        print("✅ Response Data (as JSON):")
        data = api_response['data']
    else:
        print(f"⚠️ Error: {api_response['error']}")
        print("Raw Response:")
        print(api_response['raw_text'])