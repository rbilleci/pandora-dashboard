import json

import boto3 as boto3
import pandas as pd
import os

from elasticsearch import helpers
from requests_aws4auth import AWS4Auth
from elasticsearch import Elasticsearch, RequestsHttpConnection

pd.options.display.max_columns = None
pd.options.display.max_rows = None
pd.options.display.max_info_columns = 1000


def prepare(fn, plan):
    df = pd.read_csv(fn, keep_default_na=False, na_values='')
    df['region_name'] = df['region_name'].fillna('')
    df['date'] = pd.to_datetime(df['date'])
    df['plan'] = plan
    df['prediction_start_date'] = df.loc[df['predicted'] == True]['date'].min()
    columns_to_drop = []
    for column in df.columns:
        if str(column).find('_ma_3') > 0:
            columns_to_drop.append(column)
        if str(column).find('_ma_21') > 0:
            columns_to_drop.append(column)
        if str(column).find('--') > 0:
            columns_to_drop.append(column)
    df = df.drop(columns=columns_to_drop)
    df = df.drop(columns=['predicted_new_cases',
                          'confirmed_deaths',
                          'country_code3',
                          'country_code_numeric',
                          'day_of_year',
                          'month',
                          'quarter',
                          'week',
                          'working_day_ma_7',
                          'working_day_tomorrow',
                          'working_day_yesterday',
                          'year'])
    df['working_day'] = df['working_day'].apply(lambda x: True if x == 1.0 else False)
    df['stringency'] = (df['c1_school_closing'] / 3.) + \
                       (df['c2_workplace_closing'] / 3.) + \
                       (df['c3_cancel_public_events'] / 2.) + \
                       (df['c4_restrictions_on_gatherings'] / 4.) + \
                       (df['c5_close_public_transport'] / 2.) + \
                       (df['c6_stay_at_home_requirements'] / 3.) + \
                       (df['c7_restrictions_on_internal_movement'] / 2.) + \
                       (df['c8_international_travel_controls'] / 4.) + \
                       (df['h1_public_information_campaigns'] / 2.) + \
                       (df['h2_testing_policy'] / 3.) + \
                       (df['h3_contact_tracing'] / 2.) + \
                       (df['h6_facial_coverings'] / 4.)

    # all countries and region
    df.to_json(f"processed_all{fn}".replace('.csv', '.json'),
               orient='records', lines=False, date_format='iso')

    # only countries
    df_countries = df.loc[df['region_name'] == '']
    df_countries.to_json(f"processed_countries{fn}".replace('.csv', '.json'),
                         orient='records', lines=False, date_format='iso')

    # us regions
    df_us = df.loc[(df['country_name'] == 'United States') & (df['region_name'] != '')]
    df_us.to_json(f"processed_us{fn}".replace('.csv', '.json'),
                  orient='records', lines=False, date_format='iso')

    # uk regions
    df_uk = df.loc[(df['country_name'] == 'United Kingdom') & (df['region_name'] != '')]
    df_uk.to_json(f"processed_uk{fn}".replace('.csv', '.json'),
                  orient='records', lines=False, date_format='iso')


def decorate_json(documents, _index, doc_type):
    for doc in documents:
        doc['date'] = doc['date'].split('T')[0]
        """
        if doc['plan'] == 'Baseline':
            doc['plan'] = 0
        elif doc['plan'] == 'Intervention Plan 1':
            doc['plan'] = 1
        elif doc['plan'] == 'Intervention Plan 2':
            doc['plan'] = 2
        elif doc['plan'] == 'Intervention Plan 3':
            doc['plan'] = 3
        elif doc['plan'] == 'Intervention Plan 4':
            doc['plan'] = 4
        elif doc['plan'] == 'Intervention Plan 5':
            doc['plan'] = 5
        elif doc['plan'] == 'Intervention Plan 6':
            doc['plan'] = 6
        elif doc['plan'] == 'Intervention Plan 7':
            doc['plan'] = 7
        elif doc['plan'] == 'Intervention Plan 8':
            doc['plan'] = 8
        elif doc['plan'] == 'Intervention Plan 9':
            doc['plan'] = 9
        elif doc['plan'] == 'Intervention Plan 10':
            doc['plan'] = 10
        """
        yield {
            "_index": _index,
            "_type": doc_type,
            "_id": f"{doc['geo_code']}-{doc['plan']}-{doc['date']}",
            "_source": doc}


def load():
    # get es connection
    region = 'us-east-1'
    endpoint = boto3.client('es').describe_elasticsearch_domain(DomainName=f"es-pandora-{region}")['DomainStatus'][
        'Endpoint']
    credentials = boto3.Session().get_credentials()
    auth = AWS4Auth(credentials.access_key, credentials.secret_key, region, 'es', session_token=credentials.token)
    es = Elasticsearch(
        hosts=[{'host': endpoint, 'port': 443}],
        http_auth=auth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
    )

    for ip in ['baseline', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9']:
        for typ in ['all', 'countries', 'us', 'uk']:
            fn = f"processed_{typ}_plan_{ip}.json"
            index = f"index_{typ}"
            print(fn)
            file = open(fn, 'r')
            documents = json.loads(file.read())
            documents = decorate_json(documents, index, 'update')
            helpers.bulk(es, documents)


"""
prepare("_plan_0.csv", 'Intervention Plan 1')
prepare("_plan_1.csv", 'Intervention Plan 2')
prepare("_plan_2.csv", 'Intervention Plan 3')
prepare("_plan_3.csv", 'Intervention Plan 4')
prepare("_plan_4.csv", 'Intervention Plan 5')
prepare("_plan_5.csv", 'Intervention Plan 6')
prepare("_plan_6.csv", 'Intervention Plan 7')
prepare("_plan_7.csv", 'Intervention Plan 8')
prepare("_plan_8.csv", 'Intervention Plan 9')
prepare("_plan_9.csv", 'Intervention Plan 10')
prepare("_plan_baseline.csv", 'Baseline')
"""
load()
