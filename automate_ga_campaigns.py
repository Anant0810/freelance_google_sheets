import argparse
import sys
from datetime import datetime, timedelta
import pytz
from google.ads.googleads.errors import GoogleAdsException
from gspread_pandas import Spread, Client
import gspread_pandas
import pandas as pd
import os
import logging

from helper.ga_scripts import get_results_ga_ads, build_ga_df, get_ad_group_results_ga_ads, build_ad_Group_ga_df
from helper.clients_info import ga_clients as clients, ga_client_level_campaign
from utils.constants import GA_LOG_FILE, GA_SHEET_NAME, GA_START_DATE, CONSOLE_CONFIG_FILE


directory = os.path.dirname(os.path.realpath(__file__))
cfg = gspread_pandas.conf.get_config(conf_dir=directory, file_name=CONSOLE_CONFIG_FILE)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(levelname)s:%(asctime)s:%(name)s:%(message)s')

log_directory = os.path.join(directory, "logs")
file_handler = logging.FileHandler(os.path.join(log_directory, GA_LOG_FILE))
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


def get_yesterday_str(days=1):
    tz = pytz.timezone('Asia/Kolkata')
    today_dt = datetime.now(tz).date()
    today_dt = today_dt - timedelta(days=days)
    today = today_dt.strftime("%Y-%m-%d")
    return today

def get_insights(client, level='campaign', since_dt=None):

    api_call_func = get_results_ga_ads if level == 'campaign' else get_ad_group_results_ga_ads
    
    yesterday = get_yesterday_str()
    today = get_yesterday_str(days=0)

    start_date = GA_START_DATE
    if since_dt == None:
        message = f"DB Initialize from {start_date}"
        try:
            results = api_call_func(client,
                start_date=start_date,
                end_date=yesterday
                )
        except GoogleAdsException as e: 
            error_dict = e.__dict__
            # print(error_dict.get('failure'))
            message = error_dict.get('failure')
            results = None
    else:
        since_d = since_dt + timedelta(days=1)
        since = since_d.strftime("%Y-%m-%d")
        
        
        if since == today:
            # print({'message': 'Already done'})
            return None, "Already Updated"
        message = f"Updated Successfully"
        try:
            results = api_call_func(client,
                start_date = since,
                end_date = yesterday
                )
        except GoogleAdsException as e: 
            error_dict = e.__dict__
            # print(error_dict.get('failure'))
            message = error_dict.get('failure')
            results = None

    message = "NO DATA RETURNED" if results == [] else message  # when results = []

    return results, message




def update_sheets(client, client_id,  level='campaign'):
    # print(client)
    sheet_name = f'{client}_{level}'
    
    # spread = Spread('TEST')
    spread = Spread(GA_SHEET_NAME, config=cfg)
    
    build_df = build_ga_df
    
    if spread.find_sheet(sheet_name):
        # print(f'has sheet {sheet_name}')
        df = spread.sheet_to_df(header_rows=1, index=0, start_row=3, sheet=sheet_name)
        has_df = True
        try:
            since_dt = datetime.strptime(df['Start Date'][0], '%Y-%m-%d').date()
        except ValueError as e:
            # print(e)
            has_df = False
        except KeyError as e:
            has_df = False
            print(e)
        

        if has_df:
            # print(f'has df in {sheet_name}')
            insights, message = get_insights(client_id, level, since_dt=since_dt)
            if insights:
                dff = build_df(insights)
                dff.sort_values('Start Date', ascending=False, inplace=True, ignore_index=True)
                new_df = pd.concat([dff, df], ignore_index=True)
            ### concat with old ... 
        else:
            ### new_df
            # print(f'does not have df in {sheet_name}')
            insights, message = get_insights(client_id, level)
            if insights:
                dff = build_df(insights)
                dff.sort_values('Start Date', ascending=False, inplace=True, ignore_index=True)
                new_df = dff.copy()
        
    else:
        # print(f'did not find {sheet_name}')
        insights, message = get_insights(client_id, level)
        if insights:
            dff = build_df(insights)
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
    parser.add_argument('-l', '--level', type=str, default='campaign', help='Account Level')

    args = parser.parse_args()
    

    
    if args.client not in clients.keys() and args.client != 'all':
        sys.exit()
    # print('main run')
    if args.client == 'all':
        for client in clients.keys():
            #result = 
            client_id = clients[client]
            level = 'campaign'
            result = update_sheets(client, client_id, level=level)
            success_dict[client] = result

    else:
        # print(f'main {args.client}')
        if args.client not in clients.keys():
            sys.exit()
        client_id = clients[args.client]
        result = update_sheets(args.client, client_id, level=args.level)
        success_dict[args.client] = result
        
    # print(success_dict)
        