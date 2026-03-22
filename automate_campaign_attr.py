import gspread_pandas.conf
from helper.fb_scripts_attr import get_organised_fb_data_with_attr, get_fb_campaign_data_sync, fb_data_benchmarking_monthly
from facebook_business.exceptions import FacebookRequestError
import os
import pandas as pd
import gspread_pandas
from gspread_pandas import Spread
import logging
import pytz
import datetime
import argparse
import sys

from helper.clients_info import fb_attribution_windows as attribution_windows
from helper.clients_info import fb_clients as clients, lead_clients
from helper.clients_info import fb_clients_level as levels
from utils.constants import CONSOLE_CONFIG_FILE,FB_LOG_FILE,FB_SHEET_NAME,FB_START_DATE

directory = os.path.dirname(os.path.realpath(__file__))
cfg = gspread_pandas.conf.get_config(conf_dir=directory, file_name=CONSOLE_CONFIG_FILE)

# print(cfg)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(levelname)s:%(asctime)s:%(name)s:%(message)s')

log_directory = os.path.join(directory, "logs")
file_handler = logging.FileHandler(os.path.join(log_directory, FB_LOG_FILE))
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


# todo
"""
    everthing is successfull
    one of them failed, it should go ahead and do other ones
    tell us which one failed or succeed.
"""
def get_yesterday_str(days=1):
    tz = pytz.timezone('Asia/Kolkata')
    today_dt = datetime.datetime.now(tz).date()
    today_dt = today_dt - datetime.timedelta(days=days)
    today = today_dt.strftime("%Y-%m-%d")
    return today

def get_insights(client, level, attribution_window=[], since_dt=None):
    api_call_func = get_fb_campaign_data_sync if (level == 'campaign') or (level == 'account')\
                    else fb_data_benchmarking_monthly
    api_call_func = get_fb_campaign_data_sync
    yesterday = get_yesterday_str()
    today = get_yesterday_str(days=0)
    
    if since_dt == None:
        since = FB_START_DATE
        # yesterday = "2023-05-25"
        # print(since, yesterday)
        time_range = {'since': since, 'until': yesterday}
        message = f"DB Initialize from {since}"
        try:
            insights = api_call_func(client,
                level=level,
                time_range=time_range,
                attribution_window=attribution_window
                )
            # print(len(insights))
        except FacebookRequestError as e: 
            error_dict = e.__dict__
            # print(error_dict.get('_api_error_message'))
            message = error_dict.get('_api_error_message')
            insights = None
    else:
        since_d = since_dt + datetime.timedelta(days=1)
        since = since_d.strftime("%Y-%m-%d")
        
        
        if since == today:
            # print({'message': 'Already done'})
            return None, "Already Updated"
        
        # yesterday = "2023-05-01"
        # since = "2023-11-01"
        time_range = {'since': since, 'until': yesterday}
        message = f"Updated Successfully"
        try:
            insights = api_call_func(client,
                level=level,
                time_range=time_range,
                attribution_window=attribution_window
                )
        except FacebookRequestError as e: 
            error_dict = e.__dict__
            # print(error_dict.get('_api_error_message'))
            message = error_dict.get('_api_error_message')
            insights = None

    message = "NO DATA RETURNED" if insights == [] else message  # when insights = []

    return insights, message


# spread = Spread('TEST')
# spread = Spread('client_datasheets_attr')
# spread = Spread('Client_Datasheet_adset')

def update_sheets(client, level, attribution_window):
    # print(client, level)
    sheet_name = f'{client}_{level}'
    
    # spread = Spread('TEST')
    spread = Spread(FB_SHEET_NAME, config=cfg)
    lead_req = True
    if spread.find_sheet(sheet_name):
        # print(f'has sheet {sheet_name}')
        df = spread.sheet_to_df(header_rows=1, index=0, start_row=3, sheet=sheet_name)
        has_df = True
        try:
            since_dt = datetime.datetime.strptime(df['Start Date'][0], '%Y-%m-%d').date()
        except ValueError as e:
            # print(e)
            has_df = False
        
        if has_df:
            # print(f'has df in {sheet_name}')
            insights, message = get_insights(client, level, attribution_window, since_dt=since_dt)
            if insights:
                dff = get_organised_fb_data_with_attr(insights, attribution_window=attribution_window, level=level, leads=lead_req)
                dff.sort_values('Start Date', ascending=False, inplace=True, ignore_index=True)
                new_df = pd.concat([dff, df], ignore_index=True)
            ### concat with old ... 
        else:
            ### new_df
            # print(f'does not have df in {sheet_name}')
            insights, message = get_insights(client, level, attribution_window)
            if insights:
                dff = get_organised_fb_data_with_attr(insights, attribution_window=attribution_window, level=level, leads=lead_req)
                dff.sort_values('Start Date', ascending=False, inplace=True, ignore_index=True)
                new_df = dff.copy()
        
    else:
        # print(f'did not find {sheet_name}')
        insights, message = get_insights(client, level, attribution_window)
        if insights:
            dff = get_organised_fb_data_with_attr(insights, attribution_window=attribution_window, level=level, leads=lead_req)
            dff.sort_values('Start Date', ascending=False, inplace=True, ignore_index=True)
            new_df = dff.copy()
    
    if insights:
        spread.df_to_sheet(new_df, sheet=sheet_name, start='A3', replace=True, index=False)
        result = True
    else:
        result = False
    
    logger.info(f" MESSAGE - {client.capitalize()} : {level.upper()} --> {message}")
    return result
                        

if __name__ == '__main__':

    success_dict = {}
    parser = argparse.ArgumentParser(description="Provide Client's name and level")
    parser.add_argument('-c', '--client', type=str, default='all', help="Client's name")

    args = parser.parse_args()
    
    if args.client not in clients and args.client != 'all':
        sys.exit()
    # print('main run')
    if args.client == 'all':
        
        for client in clients:
            try:
                result = update_sheets(client, levels[client], attribution_window=attribution_windows.get(client, []))
                success_dict[client] = result
            except Exception as e:
                message = f"Error in updating the sheets : {e}"
                logger.info(f" MESSAGE - {client.capitalize()} : {levels[client].upper()} --> {message}")
    
        

    else:
        if args.client not in clients:
            sys.exit()
        result = update_sheets(args.client, levels[args.client], attribution_window=attribution_windows.get(args.client, []))
        success_dict[args.client] = result
    
    # print(success_dict)

