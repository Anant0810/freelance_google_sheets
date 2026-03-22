from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
import pandas as pd
import numpy as np
import os

directory = os.path.dirname(os.path.realpath(__file__))

ga_client = GoogleAdsClient.load_from_storage(os.path.join(directory, "../google-ads.yaml"))
ga_service = ga_client.get_service("GoogleAdsService")

def get_results_ga_ads(client, start_date, end_date):
    query = f"""
        SELECT
          customer.descriptive_name,
          segments.date,
          campaign.name,
          metrics.cost_micros,
          metrics.average_cpm,
          metrics.interactions,
          metrics.ctr,
          metrics.impressions,
          metrics.clicks,
          metrics.conversions,
          metrics.cost_per_conversion,
          metrics.conversions_value
          
        FROM campaign
        WHERE
          segments.date >= '{start_date}' AND segments.date <= '{end_date}'
        ORDER BY segments.date DESC
    """

    response = ga_service.search_stream(customer_id=client, query=query)
    
    
    
    results = []
    for batch in response:
        for row in batch.results:
            results.append([
                    row.customer.descriptive_name,
                    row.segments.date,
                    row.campaign.name,
                    row.metrics.cost_micros/1000000,
                    row.metrics.average_cpm/1000000,
                    row.metrics.interactions,
                    row.metrics.ctr,
                    row.metrics.impressions,
                    row.metrics.clicks,
                    row.metrics.conversions,
                    row.metrics.cost_per_conversion/1000000,
                    row.metrics.conversions_value
               ])

    return results


def get_ad_group_results_ga_ads(client, start_date, end_date):
    query = f"""
        SELECT
          customer.descriptive_name,
          segments.date,
          campaign.name,
          ad_group.name,
          metrics.cost_micros,
          metrics.average_cpm,
          metrics.interactions,
          metrics.ctr,
          metrics.impressions,
          metrics.clicks,
          metrics.conversions,
          metrics.cost_per_conversion,
          metrics.conversions_value
          
        FROM ad_group
        WHERE
          segments.date >= '{start_date}' AND segments.date <= '{end_date}'
        ORDER BY segments.date DESC
    """
    
    response = ga_service.search_stream(customer_id=client, query=query)
    
    
    
    results = []
    for batch in response:
        for row in batch.results:
            results.append([
                    row.customer.descriptive_name,
                    row.segments.date,
                    row.campaign.name,
                    row.ad_group.name,
                    row.metrics.cost_micros/1000000,
                    row.metrics.average_cpm/1000000,
                    row.metrics.interactions,
                    row.metrics.ctr,
                    row.metrics.impressions,
                    row.metrics.clicks,
                    row.metrics.conversions,
                    row.metrics.cost_per_conversion/1000000,
                    row.metrics.conversions_value,
               ])

    return results
  

def build_ad_Group_ga_df(results):
    df = pd.DataFrame(results)
    df.columns = ['Account Name', 'Start Date', 'Campaign Name', 'Adgroup Name', 'Cost', 'Avg. CPM', 'Views', 
                  'CTR', 'Impressions', 'Clicks', 'Conversions', 'Cost Per Conversion',
                  "Conversion Value"]
    
    return df



def build_ga_df(results):
    df = pd.DataFrame(results)
    df.columns = ['Account Name', 'Start Date', 'Campaign Name', 'Cost', 'Avg. CPM', 'Views', 
                  'CTR', 'Impressions', 'Clicks', 'Conversions', 'Cost Per Conversion',
                  "Conversion Value"]
    
    return df

