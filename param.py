# Import Data Profile Excel file name 'ProcessParasProfileUTL'
import pandas as pd
import os
from tqdm import tqdm

# Specify the path to your Excel file and the sheet names to import
#excel_path = 'your_excel_file.xlsx'
#sheet_names = ['Sheet1', 'Sheet2']  # Replace with your sheet names
excel_path = 'PPP.xlsx' # 'ProcessParasProfileUTL' sheet
sheet_names = ['PPCDataUTL', 'ProcessParasProfileUTL']  # Replace with your sheet names
# Set the path to your folder containing Excel files
folder_path = 'myfolder'

# Read the specified sheets into a dictionary of DataFrames
dfs2 = pd.read_excel(
    excel_path,
    sheet_name=sheet_names,
    engine='openpyxl'  # Required for .xlsx files
)

# Access DataFrames using the sheet names as keys
# Example:
# df_sheet1 = dfs['Sheet1']
# df_sheet2 = dfs['Sheet2']
df4 = dfs2['ProcessParasProfileUTL']

# Initialize empty DataFrames to store data from all files
df3 = pd.DataFrame()

# Loop through each file in the folder
for filename in os.listdir(folder_path):
    if filename.endswith('.xlsx'):  # Process only .xlsx files
        excel_path = os.path.join(folder_path, filename)
        
        try:
            # Read both sheets from the Excel file
            dfs2 = pd.read_excel(
                excel_path,
                sheet_name=['PPCDataUTL', 'ProcessParasProfileUTL'],
                engine='openpyxl'
            )
            
            # Append data to df3 and df4
            df3 = pd.concat([df3, dfs2['PPCDataUTL']], axis=0)
            
        except Exception as e:
            print(f"Error reading file {filename}: {e}")
            continue

# Optional: If you want to reset the index after concatenation
df3.reset_index(drop=True, inplace=True)
df3 = df3.drop(['EquipOpn','ULotID','ActiveFlag','SpecRevision','EventID'], axis=1)

# Clean 'ProcessParasProfileUTL' sheet on Duplicated column name
# Column index to use (e.g., column 0 = 'A')
column_index = 5 #5

# Extract values from the specified column (all rows)
column_values = df4.iloc[:, column_index].values  

# Create new DataFrame with these values as column names
new_df4 = pd.DataFrame(columns=column_values)

# Convert all columns in the new DataFrame to float32 (empty columns still need a data type)
new_df4 = new_df4.astype('float32')

# Clean ProcessParasProfileUTL sheet on Duplicated column name
new_df4 = new_df4.loc[:, ~new_df4.columns.duplicated()]

#combined_cols = pd.Index(df3.columns).append(new_df4.columns)
# Combine DataFrames vertically
combined_df = pd.concat([df3, new_df4], ignore_index=True)

# Initialize an empty dictionary
parameter = {}

# Convert the parameter values to float and prepare a row
row_data = {str(key): float(value) for key, value in parameter.items() if str(key) in combined_df.columns}

# Split the string into key-value pairs
def get_parameter(s):
    pairs = s.split(',')
    
    # Initialize an empty dictionary
    parameter = {}
    
    # Process each pair and convert to integers
    for pair in pairs:
        key_str, value_str = pair.split(':')
        key = int(key_str)
        if value_str != 'System.Byte[]':   # Skip key_str when value_str is undefined !!!!
            value = int(value_str)
            parameter[key] = value
            
    return(parameter)

# Update all rows in the DataFrame with values from the 'parameter' dictionary
for row_index in tqdm(combined_df.index, desc="Updating DataFrame rows"):
    parameter = get_parameter(df3.iloc[row_index, 2])
    for col in combined_df.columns:
        if col in parameter:
            combined_df.iloc[row_index, combined_df.columns.get_loc(col)] = parameter[col]

# Drop 'Parameter' column
combined_df = combined_df.drop(['Parameter'], axis=1)

# Save to csv file
combined_df.to_csv('output.csv', index=False)