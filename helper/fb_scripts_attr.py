from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adreportrun import AdReportRun
import pandas as pd
import numpy as np
import time
from config import get_client_config


def get_fb_campaign_data_sync(client, level, time_range, breakdowns=[], attribution_window=[]):

    access_token, ad_account_id, _, _ = get_client_config(client)

    if access_token == 'NA':
        return []
    
    
    FacebookAdsApi.init(access_token=access_token, api_version='v19.0')

    fields = [
    'ad_name',
    'adset_name',
    'campaign_name',
    'created_time',
    'clicks',
    'conversions',
    'spend',
    'purchase_roas',
    'reach',
    'impressions',
    'frequency',
    'cpm',
    'outbound_clicks',
    'outbound_clicks_ctr',
    'actions',
    'action_values',
        ]

    params = {
        'time_range': time_range,
        'filtering': [],
        'level': level,
        'time_increment': 1,
        'breakdowns': breakdowns,
        'action_attribution_windows': attribution_window
    }

    raw_data = list(AdAccount(ad_account_id).get_insights(
        fields=fields,
        params=params))


    return raw_data


def fb_data_benchmarking_monthly(client, time_range, attribution_window=[], level='account', breakdowns=[], filtering=[]):
    access_token, ad_account_id, _, _ = get_client_config(client)

    if access_token == 'NA':
        return []
    
    
    FacebookAdsApi.init(access_token=access_token, api_version='v19.0')

    fields = [
    'ad_name',
    'adset_name',
    'campaign_name',
    'created_time',
    'clicks',
    'conversions',
    'spend',
    'purchase_roas',
    'reach',
    'impressions',
    'frequency',
    'cpm',
    'outbound_clicks',
    'outbound_clicks_ctr',
    'actions',
    'action_values',
        ]

    params = {
        'time_range': time_range,
        'filtering': filtering,
        'level': level,
        'time_increment': 1,
        'breakdowns': breakdowns,
        'action_attribution_windows': attribution_window
    }

    async_job = AdAccount(ad_account_id).get_insights(
        fields=fields,
        params=params, is_async=True)
    async_job.api_get()
    while async_job[AdReportRun.Field.async_status] != 'Job Completed' or \
             async_job[AdReportRun.Field.async_percent_completion] < 100:
        time.sleep(0.5)
        async_job.api_get()
        # print(async_job[AdReportRun.Field.async_percent_completion])
    time.sleep(0.5)
    raw_data = list(async_job.get_result())
    return raw_data


def get_capturing_metrics():
    
    outbound_clicks_metrics = ['outbound_clicks_outbound_click']
    outbound_clicks_ctr_metrics = ['outbound_clicks_ctr_outbound_click']
    purchase_roas_metrics = ['purchase_roas_omni_purchase']
    action_values_metrics = ['action_values_omni_purchase']
    actions_metrics = ['actions_landing_page_view', 'actions_omni_add_to_cart', 'actions_omni_initiated_checkout',
                        'actions_offsite_conversion.fb_pixel_add_payment_info', 
                        'actions_omni_purchase','actions_lead'] #actions_offsite_conversion.fb_pixel_lead
    capturing_metrics = outbound_clicks_metrics

    capturing_metrics.extend([*outbound_clicks_ctr_metrics, *purchase_roas_metrics,  *actions_metrics, *action_values_metrics, ])
    
    return capturing_metrics

def get_all_metrics(capturing_metrics, attribution_window_value):
    basic_metrics =['date_start', 'date_stop', 'created_time', 'ad_name', 'adset_name', 'campaign_name',  'spend',
            'reach', 'impressions', 'clicks', 'frequency', 'cpm']
    attr_metrics = []
    for i in capturing_metrics:
        for j in attribution_window_value:
            attr_metrics.append(f'{i}_{j}')
    metrics = basic_metrics + attr_metrics
    return metrics

def get_results(insights, attribution_set, capturing_metrics):
    results = []
    for item in insights:   
        data = dict(item)
        new_data = {}
        for key, value in data.items():        
            if isinstance(value, list):
                for action in value:
                    try:
                        if f"{key}_{action['action_type']}" in capturing_metrics:

                            for attr in attribution_set:
                                new_data[f'{key}_{action["action_type"]}_{attr}'] =  action.get(attr, 0)
                            new_data[f'{key}_{action["action_type"]}_value'] = action.get('value', 0)
                    except KeyError as e:
                        # print(e)
                        continue    
            else:
                new_data[key] = value
        results.append(new_data)
    return results


def add_attr_total(df, column_name, tr_column_name, attribution_window, func=np.sum):
    shape = df.shape
    column = []
    if attribution_window == []:
        attributions = ['value']
    else:
        attributions = attribution_window

    for attr in attributions:
        column.append(f"{column_name}_{attr}")
    try:
        new_df = df[column].copy()
    except KeyError as e:
        print(e)
        return None
    
    
    for col in new_df.columns:
        new_df[col] = pd.to_numeric(new_df[col])
        if new_df[col].sum() == 0:
            del new_df[col]
            
    
    df[tr_column_name] = func(new_df, axis=1)
    
def get_organised_fb_data_with_attr(insights, attribution_window, level='campaign', leads=False):
    
    capturing_metrics = get_capturing_metrics()
    
    
    attr_window_val = attribution_window + ['value']
    metrics = get_all_metrics(capturing_metrics, attr_window_val)
    
    results = get_results(insights, attribution_window, capturing_metrics)
    df = pd.DataFrame(results, columns=metrics)
    
    
    adding_columns_metrics = {
                          'outbound_clicks_outbound_click': ['sum', 'OBC'],
                          'outbound_clicks_ctr_outbound_click': ['mean', 'CTR'], 
                          'purchase_roas_omni_purchase': ['mean', 'ROAS'],
                          'actions_landing_page_view': ['sum', 'LPV'],
                          'actions_omni_add_to_cart': ['sum', 'ATC'],
                          'actions_omni_initiated_checkout': ['sum', 'CI'],
                          'actions_lead': ['sum', 'Leads'],
                          'actions_offsite_conversion.fb_pixel_add_payment_info': ['sum', 'ATPI'],
                          'actions_omni_purchase': ['sum', 'Purchases'],
                          'action_values_omni_purchase': ['sum', 'Purchase Value']
     }
    
    for key, value in adding_columns_metrics.items():
        func_text, tr_column = value
        func = np.sum if func_text == 'sum' else np.mean
        if key in ['outbound_clicks_outbound_click', 'outbound_clicks_ctr_outbound_click']:
            df[tr_column] = df[f"{key}_value"]
        else:
            add_attr_total(df, key, tr_column, attribution_window, func)
    
    df_ad = df.copy()
    
    df_ad['OBC'] = df_ad['OBC'].apply(pd.to_numeric)
    df_ad['CTR'] = df_ad['CTR'].apply(pd.to_numeric)
    df_ad['APV']  = np.where(df_ad['Purchases'] == 0 ,  df_ad['Purchases'], df_ad['Purchase Value']/df_ad['Purchases'])
    df_ad['APV'] = df_ad['APV'].round(2)

    df_ad['OBC>LPV(%)']  = np.where(df_ad['OBC'] == 0 ,  0, df_ad['LPV']/df_ad['OBC'])
    df_ad['OBC>LPV(%)'] = df_ad['OBC>LPV(%)'].apply(lambda x : 0 if x == 0 else round(x * 100, 2))

    df_ad['LPV>ATC(%)']  = np.where(df_ad['LPV'] == 0 ,  0, df_ad['ATC']/df_ad['LPV'])
    df_ad['LPV>ATC(%)'] = df_ad['LPV>ATC(%)'].apply(lambda x : 0 if x == 0 else round(x * 100, 2))

    df_ad['ATC>CI(%)']  = np.where(df_ad['ATC'] == 0 ,  0, df_ad['CI']/df_ad['ATC'])
    df_ad['ATC>CI(%)'] = df_ad['ATC>CI(%)'].apply(lambda x : 0 if x == 0 else round(x * 100, 2))

    df_ad['LPV>CI(%)']  = np.where(df_ad['LPV'] == 0 ,  0, df_ad['CI']/df_ad['LPV'])
    df_ad['LPV>CI(%)'] = df_ad['LPV>CI(%)'].apply(lambda x : 0 if x == 0 else round(x * 100, 2))

    df_ad['CI>Purchases(%)']  = np.where(df_ad['CI'] == 0 ,  0, df_ad['Purchases']/df_ad['CI'])
    df_ad['CI>Purchases(%)'] = df_ad['CI>Purchases(%)'].apply(lambda x : 0 if x == 0 else round(x * 100, 2))

    df_ad['LPV>Purchases(%)']  = np.where(df_ad['LPV'] == 0 ,  0, df_ad['Purchases']/df_ad['LPV'])
    df_ad['LPV>Purchases(%)'] = df_ad['LPV>Purchases(%)'].apply(lambda x : 0 if x == 0 else round(x * 100, 2))


    df_ad['spend'] = df_ad['spend'].astype(float)
    df_ad['reach'] = df_ad['reach'].astype(float)
    df_ad['impressions'] = df_ad['impressions'].astype(float)
    df_ad['frequency'] = df_ad['frequency'].astype(float)
    df_ad['cpm'] = df_ad['cpm'].astype(float)
    df_ad['Leads'] = df_ad['Leads'].astype(int)
    
    if level == 'adset':
        df_ad = df_ad.rename(columns={"spend": "Spends", "reach": "Reach", "impressions": "Impressions", "frequency": "Frequency",
                                  "cpm": "CPM", "date_start": "Start Date", "date_stop": "End Date", "created_time": "Launch Date",
                                  "adset_name": "AD Set Name", "campaign_name": "Campaign Name", 'clicks': 'Clicks'})
        df_ad_organised = df_ad[['Start Date','End Date', 'Campaign Name','AD Set Name','Launch Date','Spends','ROAS','Reach',
                                 'Impressions','Frequency','CPM', 'Clicks', 'CTR','OBC', 'LPV','OBC>LPV(%)','ATC','LPV>ATC(%)',
                                 'CI','ATC>CI(%)', 'LPV>CI(%)', 'ATPI','Purchases','CI>Purchases(%)', 'LPV>Purchases(%)',
                                     'Purchase Value','APV']] #"ad_name": "AD Name",
#         df_ad_organised = df_ad_organised.sort_values(['Start Date', 'AD Set Name'], ascending=[False, True])
        if leads:
            df_ad_organised = df_ad[['Start Date','End Date', 'Campaign Name','AD Set Name','Launch Date','Spends','ROAS','Reach',
                                 'Impressions','Frequency','CPM', 'Clicks', 'CTR','OBC', 'LPV','OBC>LPV(%)','ATC','LPV>ATC(%)',
                                 'CI','ATC>CI(%)', 'LPV>CI(%)', 'Leads', 'ATPI','Purchases','CI>Purchases(%)', 'LPV>Purchases(%)',
                                     'Purchase Value','APV']]
    elif level == 'campaign':
        df_ad = df_ad.rename(columns={"spend": "Spends", "reach": "Reach", "impressions": "Impressions", "frequency": "Frequency",
                                  "cpm": "CPM", "date_start": "Start Date", "date_stop": "End Date", "created_time": "Launch Date",
                                  "adset_name": "AD Set Name", "campaign_name": "Campaign Name", 'clicks': 'Clicks'})
        df_ad_organised = df_ad[['Start Date','End Date', 'Campaign Name','Launch Date','Spends','ROAS','Reach',
                                 'Impressions','Frequency','CPM', 'Clicks', 'CTR','OBC', 'LPV','OBC>LPV(%)','ATC','LPV>ATC(%)',
                                 'CI','ATC>CI(%)', 'LPV>CI(%)', 'ATPI','Purchases','CI>Purchases(%)', 'LPV>Purchases(%)',
                                     'Purchase Value','APV']] #"ad_name": "AD Name",
#         df_ad_organised = df_ad_organised.sort_values(['Start Date',], ascending=[False, True])
        if leads:
            df_ad_organised = df_ad[['Start Date','End Date', 'Campaign Name','Launch Date','Spends','ROAS','Reach',
                                 'Impressions','Frequency','CPM', 'Clicks', 'CTR','OBC', 'LPV','OBC>LPV(%)','ATC','LPV>ATC(%)',
                                 'CI','ATC>CI(%)', 'LPV>CI(%)', 'Leads', 'ATPI','Purchases','CI>Purchases(%)', 'LPV>Purchases(%)',
                                     'Purchase Value','APV']]

#         df_ad = df_ad.rename(columns={"spend": "Spends", "reach": "Reach", "impressions": "Impressions", "frequency": "Frequency", "cpm": "CPM",  "date_start": "Start Date", "date_stop": "End Date",  "campaign_name": "Campaign Name", "lead": "Leads", 'clicks': "Clicks"})
#         df_ad_organised = df_ad[['Start Date', 'End Date', 'Campaign Name','Spends','ROAS','Reach','Impressions','Frequency','Clicks', 'CPM','CTR','OBC',
#                             'LPV','OBC>LPV(%)','ATC','LPV>ATC(%)', 'CI','ATC>CI(%)', 'LPV>CI(%)', 'ATPI','Purchases','CI>Purchases(%)', 'LPV>Purchases(%)',
#                                      'Purchase Value','APV']] #"ad_name": "AD Name",
# #         df_ad_organised = df_ad_organised.sort_values(['Start Date', 'Campaign Name'], ascending=[False, True])
    elif level == 'ad':
        df_ad = df_ad.rename(columns={"spend": "Spends", "ad_id": "AD ID", "ad_name": "AD Name", "reach": "Reach", "impressions": "Impressions", "frequency": "Frequency", "cpm": "CPM",  "date_start": "Start Date", "date_stop": "End Date", "created_time": "Launch Date", "adset_name": "AD Set Name", "campaign_name": "Campaign Name", "lead": "Leads",'clicks': 'Clicks'})
        df_ad_organised = df_ad[['Start Date', 'End Date',"AD ID", 'Campaign Name','AD Set Name','AD Name','Launch Date','Spends','ROAS','Reach','Impressions','Frequency','CPM','Clicks','CTR','OBC', 
                                    'LPV','OBC>LPV(%)','ATC','LPV>ATC(%)', 'CI','ATC>CI(%)', 'LPV>CI(%)', 'ATPI','Purchases','CI>Purchases(%)', 'LPV>Purchases(%)',
                                     'Purchase Value','APV']] #,
#         df_ad_organised = df_ad_organised.sort_values(['Start Date', 'AD Name'], ascending=[False, True])
    elif level == 'account':
        df_ad = df_ad.rename(columns={"spend": "Spends", "reach": "Reach", "impressions": "Impressions", "frequency": "Frequency", "cpm": "CPM",  "date_start": "Start Date", "date_stop": "End Date", "created_time": "Launch Date", "adset_name": "AD Set Name", "campaign_name": "Campaign Name", "lead": "Leads",})
        df_ad_organised = df_ad[['Start Date', 'End Date','Spends','ROAS','Reach','Impressions','Frequency','CPM', 'CTR','OBC',
                                    'LPV','OBC>LPV(%)','ATC','LPV>ATC(%)', 'CI','ATC>CI(%)', 'LPV>CI(%)', 'ATPI','Purchases','CI>Purchases(%)', 'LPV>Purchases(%)',
                                     'Purchase Value','APV']]
    else:
        raise Exception("Please check the level parameter")
    
    return df_ad_organised