import yaml ### install the pyyaml package
import json
from lookerapi import LookerApi
from datetime import datetime
from pprint import pprint
from collections import defaultdict
from itertools import groupby

### ------- HERE ARE PARAMETERS TO CONFIGURE -------

# host name in config.yml
host = 'sandbox'
# model that you wish to analyze
model_name = 'trace_surfing'
# How far you wish to look back
timeframe = '28 days'

def main():
    my_host, my_token, my_secret = get_api_creds()

    looker = LookerApi(host=my_host,
                 token=my_token,
                 secret = my_secret)

    response = get_fields_usage(looker)
    print(response)

def get_fields_usage(looker):
    body={
        "model":"i__looker",
        "view":"history",
        "fields":["query.model","query.view","query.formatted_fields","query.formatted_filters","query.sorts","query.formatted_pivots","history.query_run_count"],
        "limit":"50000"
    }

    response = looker.run_inline_query("json", body)

    return response

    # print('Getting fields in '+model_name+'...')
    #
    # model = looker.get_model(model_name)
    #
    # explore_names = [i['name'] for i in model['explores']]
    #
    # explore = [looker.get_explore(model_name, i) for i in explore_names]

def get_api_creds():
    f = open('config.yml')
    params = yaml.load(f)
    f.close()

    my_host = params['hosts'][host]['host']
    my_secret = params['hosts'][host]['secret'] # client_secret
    my_token = params['hosts'][host]['token']  # client_id

    return my_host, my_token, my_secret

if __name__ == "__main__":
    main()
