### LOAD THE LATEST BANK TRANSACTIONS FROM A CSV INTO THE TILLER GOOGLE SHEET TRACKING FINANCES
### AVOIDING DUPLICATES

import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import csv
from datetime import datetime
import shutil 
import os
from dotenv import load_dotenv
import pdb

load_dotenv()
sheetID = os.environ.get("sheetID") 

########################################################################################
#                                 FUNCTION BLOCK                                       #
########################################################################################

def sheet_update(account):

    #########################################
    #    GOOGLE SHEETS CONNECTION SETUP     #
    #########################################
    
    ### https://docs.gspread.org/en/latest/index.html
    
    # use creds to create a client to interact with the Google Sheets & Drive API
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
    client = gspread.authorize(creds)

    # open google sheet by ID, safer than name 
    sheet = client.open_by_key(sheetID)
    # select main transactions table. dynamic based on the variable passed to this function
    worksheet = sheet.worksheet(account)
    # get the sheet data
    data = worksheet.get_all_values()

    # get the column headers
    headers = data.pop(0)
    # turn into df
    df = pd.DataFrame(data, columns=headers)
    # print(df.head(3))   
    # print(df.dtypes)

    #########################################
    #           DATA PREPARATION            #
    #########################################

    # convert date column to date for comparison later
    df['Date'] = pd.to_datetime(df['Date'])
    # grab most recent transaction date
    max_date = df['Date'].max()
    max_date_str = max_date.strftime("%-m/%-d/%-y")

    ## WHAT IF CSV DOESNT HAVE MOST RECENT DATE 
    ## NEED TO ACCOUNT

    # delete all records for the most recent date. 
    # latest data will be inserted from here onwards.
    while True:
        try:
            # careful, this searches all columns. should update to just search the specific Date column. 
            cell = worksheet.find(max_date_str).row 
            worksheet.delete_rows(cell)

        except gspread.CellNotFound:
            break

    #########################################
    #        LATEST DATA INSERTION          #
    #########################################

    # read the latest transactions csv export, the file will match the variable passed to the function. 
    latest = pd.read_csv(f'/Users/sam/Desktop/{account}.csv')
    latest['Date'] = pd.to_datetime(latest['Date'])
    
    # only include on,after the previous max date in the google sheet
    df_update = latest[latest['Date'] >= max_date]
    # turn date back to str for insertion method
    # df_update['Date'] = df_update['Date'].astype(str)
    df_update['Date'] = df_update.loc[:,'Date'].astype(str)

    # method to insert latest data back into the google sheet
    def update_sheet(worksheet: gspread.Worksheet, df: pd.DataFrame):
        """ Args:
            worksheet (gspread.Worksheet): the sheet to update
            df (pd.DataFrame): the df data to append
        """
        # go over the values in the df by row
        for row in df.values:
            # cast as list
            row = list(row)
            # leaving out table_range, will append at the end
            worksheet.append_row(row)

    update_sheet(worksheet, df_update)

    #########################################
    #          ARCHIVE THE CSV FILE         #
    #########################################

    # add a timestamp to the file
    timestamp = datetime.today().strftime('%Y%m%d-%H%M%S')
    file_rename = f'{account}_{timestamp}.csv'
    os.rename(f'/Users/sam/Desktop/{account}.csv',f'/Users/sam/Desktop/{file_rename}') 
    # move to archve folder
    archive_path = f'/Users/sam/Documents/Tiller_Archive/{file_rename}'
    shutil.move(f'/Users/sam/Desktop/{file_rename}', archive_path)
    print(f'Archived {file_rename} to /Users/sam/Documents/Tiller_Archive')

########################################################################################
#                                  FUNCTION END                                        #
########################################################################################

   
########################################################################################
#                               CALLING THE FUNCTION                                   #
########################################################################################

# valid filenames
checking = "allychecking"
savings = "allysavings"

# check if file exists to be loaded
is_checking = os.path.exists(f'/Users/sam/Desktop/{checking}.csv')
is_savings = os.path.exists(f'/Users/sam/Desktop/{savings}.csv')

if is_checking == False and is_savings == False:
    print("No files found. File must be on Desktop, and either 'allychecking.csv' or 'allysavings.csv'")

if is_checking == True:
    sheet_update(checking)

if is_savings == True:
    sheet_update(savings)

#### Test File & Sheet ####
# test = "test"
# testing = os.path.exists(f'/Users/sam/Desktop/{test}.csv')
# if testing == True:
#     sheet_update(test)