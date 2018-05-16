import yaml ### install the pyyaml package
import json
from lookerapi import LookerApi
from pprint import pprint
from collections import defaultdict
from itertools import groupby
import pandas as pd
import re

### ------- HERE ARE PARAMETERS TO CONFIGURE -------

# host name in config.yml
host = 'mylooker'

# model that you wish to analyze
model = 'ML, postgres'

# How far you wish to look back
timeframe = '90 days'

def main():
    my_host, my_token, my_secret = get_api_creds()

    looker = LookerApi(host=my_host,
                 token=my_token,
                 secret = my_secret)

    # get list of all fields
    explore_fields = get_explore_fields(looker, model)

    # get list of fields used
    used_fields = get_field_usage(looker, model, timeframe)

    # unused_fields
    unused_fields = explore_fields - used_fields

# parses strings for view_name.field_name and returns a list  (empty if no matches)
def parse(string):
    return re.findall(r'(\w+\.\w+)', str(string))

# returns a list of explores in a given model
def get_explores(looker, model):
    explores = []
    for m in model.replace(' ','').split(','):
        model_body = looker.get_model(m)
        explore_names = [explore['name'] for explore in model_body['explores']]
        [explores.append(looker.get_explore(m, explore)) for explore in explore_names]
    return explores

# returns a list of view scoped fields of explores for a given model
def get_explore_fields(looker, model):
    fields =[]
    for explore in get_explores(looker, model):
        [fields.append(dimension['name']) for dimension in explore['fields']['dimensions']]
        [fields.append(measure['name']) for measure in explore['fields']['measures']]
    return set(fields)

# builds a dictionary from a list of fields, in them form of {'view': 'view_name', 'fields': []}
def schema_builder(fields):
    schema = []
    distinct_fields = sorted(set(fields))

    view_field_pairs = [field.split('.') for field in distinct_fields]
    for key, group in groupby(view_field_pairs, lambda x:x[0]):
        schema.append({"view": key,
        "fields": [i[1] for i in list(group)]
        })
    return schema

# returns list of view scoped fields used within a given timeframe
def get_field_usage(looker, model, timeframe):
    body={
        "model":"i__looker",
        "view":"history",
        "fields":["query.model","query.view","query.formatted_fields","query.formatted_filters","query.sorts","query.formatted_pivots","history.query_run_count"],
        "filters":{"history.created_date":timeframe,"query.model":model},
        "limit":"50000"
    }

    response = looker.run_inline_query("json", body)

    fields = []
    for row in response:
        fields.extend(parse(row['query.formatted_fields']))
        fields.extend(parse(row['query.formatted_filters']))
        fields.extend(parse(row['query.formatted_pivots']))
        fields.extend(parse(row['query.sorts']))

    fields = set(fields)

    return fields

# fetches api credentials from config.yml
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
