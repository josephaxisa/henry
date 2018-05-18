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
# host = 'mylooker'
host = 'cs_eng'

# model that you wish to analyze
# model = 'ML, postgres'
# model = 'snowflake_data, thelook'
# model = 'calendar, e_commerce'

# How far you wish to look back
timeframe = '90 days'

def main():
    my_host, my_token, my_secret = get_api_creds()

    looker = LookerApi(host=my_host,
                 token=my_token,
                 secret = my_secret)

    # # get list of all fields
    # explore_fields = get_explore_fields(looker, model)
    #
    # # get list of fields used
    # used_fields = get_field_usage(looker, model, timeframe)
    #
    # # unused_fields
    # unused_fields = explore_fields - used_fields
    print(get_models(looker))

# parses strings for view_name.field_name and returns a list  (empty if no matches)
def parse(string):
    return re.findall(r'(\w+\.\w+)', str(string))

# returns list of models (if no model parameter is specified) otherwise it returns specific model definition
def get_models(looker, model=None):
    if model is None:
        models = looker.get_models()
        return models
    else:
        model_list = model.replace(' ','').split(',')
        models = [looker.get_model(model) for model in model_list]
        return models


# returns a list of explores in a given model
def get_explores(looker, model):
    explores = []
    for model in get_models(looker, model):
        explore_names = [explore['name'] for explore in model['explores']]
        for explore in explore_names:
            explore_body = looker.get_explore(model['name'], explore)
            if explore_body is None:
                pass
            else:
                explores.append(explore_body)
    return explores

# returns a list of view scoped fields of explores for a given model
def get_explore_fields(looker, model):
    fields =[]
    for explore in get_explores(looker, model):
        [fields.append(dimension['name']) for dimension in explore['fields']['dimensions']]
        [fields.append(measure['name']) for measure in explore['fields']['measures']]
        [fields.append(measure['name']) for measure in explore['fields']['filters']]

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

# resturns a list of dictionaries in the format of {'model':'model_name', 'explores': ['explore_name1',...]}
def get_models_explores(looker, model):
    schema = []
    for model in get_models(looker, model):
        d = {'model': model['name'],
         'explores': [explore['name'] for explore in model['explores']]
        }
        schema.append(d)

    return(schema)

# returns a tree representation of a dictionary
def tree_maker(dict):
    tree_str = json.dumps(dict, indent=4)
    tree_str = tree_str.replace("\n    ", "\n")
    tree_str = tree_str.replace('"', "")
    tree_str = tree_str.replace(',', "")
    tree_str = tree_str.replace("{", "")
    tree_str = tree_str.replace("}", "")
    tree_str = tree_str.replace("    ", " | ")
    tree_str = tree_str.replace("  ", " ")
    tree_str = tree_str.replace("[", "")
    tree_str = tree_str.replace("]", "")

    return(tree_str)

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
