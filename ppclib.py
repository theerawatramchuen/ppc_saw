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

# Extract the parameters string from the 'parameter' column df3 to SVID and ECID dataframe
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
    
# Join df3, SVID and ECID dataframe and combine to result polar dataframe
import duckdb
def combine(df3,SVID,ECID):
    # Configure DuckDB to work in memory-constrained environments
    duckdb.execute("SET temp_directory='C:/Users/RYZEN/datamining/saw/temp';")  # Use SSD if possible
    
    # Query directly on the DataFrame (no need to load into a database)
    result = duckdb.sql("""
        SELECT df3.EquipID, Recipe, df3.CreateTime, df3.CreateTimeUnix, df3.EventDesc,
        SAW_ProductionStock_Z1, BladeOD_Z1, BladeThickness_Z1, FlangeODType_Z1,
        SAW_ProductionStock_Z2, BladeOD_Z2, BladeThickness_Z2, FlangeODType_Z2,
        ECID."4280" AS ECID_4280, 
        ECID."4290" AS ECID_4290, 
        ECID."6603" AS ECID_6603, 
        ECID."6611" AS ECID_6611,
        ECID."6607" AS ECID_6607, 
        ECID."6615" AS ECID_6615, 
        ECID."4628" AS ECID_4628,
        ECID."4629" AS ECID_4629,
        ECID."6641" AS ECID_6641,
        ECID."16009" AS ECID_16009,
        ECID."16058" AS ECID_16058,
        ECID."6640" AS ECID_6640,
        ECID."16008" AS ECID_16008,
        ECID."16057" AS ECID_16057,
        ECID."6636" AS ECID_6636,
        ECID."16004" AS ECID_16004,
        ECID."16053" AS ECID_16053,
        ECID."6637" AS ECID_6637,
        ECID."16005" AS ECID_16005,
        ECID."16054" AS ECID_16054,
        ECID."6666" AS ECID_6666,
        ECID."16034" AS ECID_16034,
        ECID."16132" AS ECID_16132,
        ECID."4204" AS ECID_4204,
        ECID."4205" AS ECID_4205,
        SVID."1404" AS SVID_1404,
        SVID."1405" AS SVID_1405,
        SVID."3223" AS SVID_3223,
        SVID."1412" AS SVID_1412,
        SVID."1413" AS SVID_1413,
        SVID."1400" AS SVID_1400,
        SVID."1401" AS SVID_1401,
        SVID."1763" AS SVID_1763,
        SVID."1765" AS SVID_1765,
        SVID."1352" AS SVID_1352,
        SVID."1353" AS SVID_1353,
        SVID."1771" AS SVID_1771,
        SVID."1775" AS SVID_1775,
        SVID."1502" AS SVID_1502,
        SVID."1503" AS SVID_1503,
        SVID."1760" AS SVID_1760,
        SVID."1759" AS SVID_1759,
        SVID."1755" AS SVID_1755,
        SVID."1756" AS SVID_1756,
        SVID."1500" AS SVID_1500,
        SVID."1501" AS SVID_1501,
        SVID."1785" AS SVID_1785,
        SVID."1764" AS SVID_1764,
        SVID."1766" AS SVID_1766
        FROM df3, SVID, ECID
        WHERE df3.EquipID = SVID.EquipID AND df3.EquipID = ECID.EquipID 
        AND df3.CreateTimeUnix = SVID.CreateTimeUnix AND df3.CreateTimeUnix = ECID.CreateTimeUnix AND df3.Parameter LIKE '4280%'
        ORDER BY df3.EquipID, df3.CreateTime ASC
    """).to_df()
    return result

# Anomaly Detection with Isolation Forest Code on result PANDAS dataframe
#import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import LabelEncoder, MinMaxScaler
def anamoly_det(result):
    # Initial cleaning
    dfx = result.dropna(axis=1, how='all')          # Drop completely empty columns
    dfx = dfx.dropna(axis=0, how='any')              # Drop rows with any missing values
    df = dfx.drop(['CreateTime', 'CreateTimeUnix'], axis=1)  # Remove time columns
    
    # Free Memory
    result = []
    
    # Preserve original categorical values before encoding
    original_cat_columns = df.select_dtypes(include='object').copy()
    encoded_df = df.copy()
    
    # Label encode categorical columns
    cat_cols = original_cat_columns.columns
    label_encoders = {}
    for col in cat_cols:
        le = LabelEncoder()
        encoded_df[col] = le.fit_transform(encoded_df[col].astype(str))
        label_encoders[col] = le
    
    # Train Isolation Forest and get scores
    model = IsolationForest(
        n_estimators=200,
        contamination=0.05,
        random_state=42
    )
    model.fit(encoded_df)
    
    # Get anomaly scores and normalize them to 0-1 range
    scores = model.decision_function(encoded_df)
    scaler = MinMaxScaler()
    normalized_scores = scaler.fit_transform(scores.reshape(-1, 1))
    
    # Create results dataframe with original values and scores
    encoded_df['AnomalyScore'] = scores  # Original scores (-0.5 to 0.5)
    encoded_df['AnomalyScore_normalized'] = normalized_scores  # 0-1 scaled
    
    results_df = pd.concat([
        encoded_df[['AnomalyScore', 'AnomalyScore_normalized']],
        dfx[['CreateTime','CreateTimeUnix']], original_cat_columns,
        df.select_dtypes(exclude='object')
    ], axis=1)
    
    # Sort by anomaly score for better inspection
    #results_df = results_df.sort_values('AnomalyScore_normalized', ascending=False)
    
    # Extract anomalies with original values
    #anomalies_df = results_df[results_df['AnomalyScore_normalized'] > 0.5]  # Adjust threshold as needed
    #print(f"Found {len(anomalies_df)} anomalies from {len(df)} total records")
    #print("\nTop 5 anomalies:")
    #print(anomalies_df.head())
    
    # Verification metrics
    # print("\nScore Statistics:")
    # print(f"Mean score: {results_df.AnomalyScore_normalized.mean():.2f}")
    # print(f"Max score: {results_df.AnomalyScore_normalized.max():.2f}")
    # print(f"Min score: {results_df.AnomalyScore_normalized.min():.2f}")

    return results_df

import seaborn as sns
import matplotlib.pyplot as plt
def plot_equipid_violin(anomalies_df):
    # Set plot style and size
    plt.figure(figsize=(14, 8))
    
    # Create violin plot (shows distribution density)
    sns.violinplot(
        x='EquipID',
        y='AnomalyScore_normalized',
        data=anomalies_df, 
        inner='quartile'  # Adds quartile lines inside the violin
    )
    
    # Improve readability
    plt.title('Distribution of Normalized Anomaly Scores by Equipment ID', fontsize=14)
    plt.xlabel('Equipment ID', fontsize=10)
    plt.ylabel('Normalized Anomaly Score', fontsize=14)
    plt.xticks(rotation=45, ha='right')  # Rotate x-axis labels
    
    # Add gridlines for clarity
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    
    plt.tight_layout()
    plt.show()

# Sort data and calculate moving average
def plot_trend(anomalies_df):
    df = anomalies_df.sort_values('CreateTime')
    window_size = 30  # Adjust this based on your data frequency
    df['AnomalyScore_smoothed'] = df['AnomalyScore_normalized'].rolling(
        window=window_size,
        min_periods=1,
        center=True
    ).mean()
    
    plt.figure(figsize=(14, 7))
    
    # Plot original data with transparency
    sns.lineplot(
        data=df,
        x='CreateTime',
        y='AnomalyScore_normalized',
        color='#abd9e9',
        linewidth=0.7,
        alpha=0.4,
        label='Original Values'
    )
    
    # Plot smoothed trend line
    sns.lineplot(
        data=df,
        x='CreateTime',
        y='AnomalyScore_smoothed',
        color='#2c7bb6',
        linewidth=1.5,
        label=f'Smoothed ({window_size}-period MA)'
    )
    
    plt.title('Normalized Anomaly Score Trend with Noise Reduction', fontsize=14, pad=20)
    plt.xlabel('Create Time', fontsize=12)
    plt.ylabel('Score Value', fontsize=12)
    plt.grid(True, alpha=0.3)
    plt.legend(loc='upper left')
    
    # Format x-axis dates
    plt.xticks(rotation=45, ha='right')
    plt.gca().xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%Y-%m-%d')) 
    
    plt.tight_layout()
    plt.show()

def plot_violin_catagories(anomalies_df,cols_to_analyze):
    plt.figure(figsize=(16, 10))
    for i, col in enumerate(cols_to_analyze, 1):
        plt.subplot(2, 2, i)
        sns.violinplot(
            data=anomalies_df,
            hue=col, legend='auto',
            y='AnomalyScore_normalized',
            palette='coolwarm',
            inner='quartile'  # Show median and quartiles
        )
        plt.title(f'Distribution by {col}', fontsize=12)
        plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

def plot_mean_bars_catagoies(anomalies_df,cols_to_analyze):
    plt.figure(figsize=(16, 6))
    
    # Calculate mean anomaly score by category for each column
    for i, col in enumerate(cols_to_analyze, 1):
        plt.subplot(1, 4, i)
        anomalies_df.groupby(col)['AnomalyScore_normalized'].mean().sort_values().plot(
            kind='bar',
            color=sns.color_palette('rocket'),
            alpha=0.7
        )
        plt.title(f'Mean Score by {col}')
        plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

# Create a pivot table of mean scores
def plot_heatmap(anomalies_df):
    heatmap_data = pd.pivot_table(
        data=anomalies_df,
        index='EquipID',  # Primary category
        columns='EventDesc',  # Secondary category
        values='AnomalyScore_normalized',
        aggfunc='mean'
    )
    
    plt.figure(figsize=(12, 8))
    sns.heatmap(
        heatmap_data,
        cmap='viridis',
        annot=True,
        fmt=".2f",
        linewidths=0.5
    )
    plt.title('Interaction Effect on Anomaly Score\n(EquipID × Event Description)')
    plt.tight_layout()
    plt.show()
    
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