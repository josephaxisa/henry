import yaml
import json
from lookerapi import LookerApi
from collections import defaultdict
from collections import Counter
from itertools import groupby
import re
import argparse
import sys
from operator import itemgetter
from spinnerthread import SpinnerThread
import threading
import queue
from tabulate import tabulate
import requests
import colors

# ------- HERE ARE PARAMETERS TO CONFIGURE -------
# host name in config.yml
host = 'mylooker'

# model that you wish to analyze
model = ['thelook']

# How far you wish to look back
timeframe = '90 days'

colors = colors.Colors()

def main():
    parser = argparse.ArgumentParser()

    # auth arguments
    auth_parser = parser.add_argument_group("Authentication")
    auth_parser.add_argument('--host', type=str, default=host,
                             required=('--client_id' or
                                       '--client_secret') in sys.argv,
                             help='# Looker Host, Default: localhost')
    auth_parser.add_argument('--port', type=int, default=19999,
                             help='# Looker API Port, Default: 19999')
    auth_parser.add_argument('--client_id', type=str,
                             required='--client_secret' in sys.argv,
                             help="# API3 Client Id")
    auth_parser.add_argument('--client_secret', type=str,
                             required='--client_id' in sys.argv,
                             help="# API3 Client Secret")

    subparsers = parser.add_subparsers(title='Subcommands',
                                       dest='command',
                                       description='Valid Subcommands',
                                       help='additional help')
    # subparsers.required = True # works, but might do without for now.

    health_subparser = subparsers.add_parser('pulse', help='analyze help')

    ls_parser = subparsers.add_parser('analyze', help='analyze help')
    ls_parser.set_defaults(which=None)
    ls_subparsers = ls_parser.add_subparsers()
    projects_sc = ls_subparsers.add_parser('projects')
    models_sc = ls_subparsers.add_parser('models')
    explores_sc = ls_subparsers.add_parser('explores')
    views_sc = ls_subparsers.add_parser('views')

    # project subcommand
    projects_sc.set_defaults(which='projects')
    projects_sc.add_argument('-p', '--project',
                             type=str,
                             default=None,
                             help='Filter on a project')
    projects_sc.add_argument('--order_by',
                             nargs=2,
                             metavar=('ORDER_FIELD', 'ASC/DESC'),
                             dest='sortkey')
    projects_sc.add_argument('--limit',
                             type=int,
                             default=None,
                             nargs=1)
    projects_sc.add_argument('--plain',
                             default=None,
                             action='store_true')



    # models subcommand
    models_sc.set_defaults(which='models')
    models_sc.add_argument('-p', '--project',
                           type=str,
                           default=None,  # when -p is not called
                           help='Filter on project')
    models_sc.add_argument('-model', '--model',
                           type=str,
                           default=None,  # when -p is not called
                           help='Filter on model')
    models_sc.add_argument('--order_by',
                           nargs=2,
                           metavar=('ORDER_FIELD', 'ASC/DESC'),
                           dest='sortkey')
    models_sc.add_argument('--limit',
                           type=int,
                           default=None,
                           nargs=1)
    models_sc.add_argument('--plain',
                           default=None,
                           action='store_true')

    # explores subcommand
    explores_sc.set_defaults(which='explores')
    explores_group = explores_sc.add_mutually_exclusive_group()
    explores_group.add_argument('-p', '--project',
                                default=None,  # when -p is not called
                                help='Filter on project')

    explores_group.add_argument('-m', '--model',
                                default=None,
                                help='Filter on models')
    explores_sc.add_argument('--order_by',
                             nargs=2,
                             metavar=('ORDER_FIELD', 'ASC/DESC'),
                             dest='sortkey')
    explores_sc.add_argument('--limit',
                             type=int,
                             default=None,
                             nargs=1)
    explores_sc.add_argument('--plain',
                             default=None,
                             action='store_true')



    # VACUUM Subcommand
    vacuum_parser = subparsers.add_parser('vacuum', help='analyze help')
    vacuum_parser.set_defaults(which=None)
    vacuum_subparsers = vacuum_parser.add_subparsers()
    vacuum_models = vacuum_subparsers.add_parser('models')
    vacuum_explores = vacuum_subparsers.add_parser('explores')
    vacuum_views = vacuum_subparsers.add_parser('views')

    vacuum_models.set_defaults(which='models')
    vacuum_models.add_argument('-m', '--model',
                               type=str,
                               default=None,  # when -p is not called
                               help='Filter on model')

    vacuum_models.add_argument('--timeframe',
                               type=int,
                               default=90,  # when -p is not called
                               help='Usage period to examine (in the range of 0-90 days). Default: 90 days.')

    vacuum_models.add_argument('--min_queries',
                               type=int,
                               default=0,  # when -p is not called
                               help='Vacuum threshold. Explores with less queries in the given usage period will be vacuumed. Default: 0 queries.')


    vacuum_explores.set_defaults(which='explores')
    vacuum_explores.add_argument('-m', '--model',
                                 type=str,
                                 default=None,  # when -p is not called
                                 required=('--explore') in sys.argv,
                                 help='Filter on model')

    vacuum_explores.add_argument('-e', '--explore',
                                 type=str,
                                 default=None,  # when -p is not called
                                 help='Filter on explore')

    vacuum_explores.add_argument('--timeframe',
                                 type=int,
                                 default=90,  # when -p is not called
                                 help='Timeframe (between 0 and 90)')


    vacuum_explores.add_argument('--min_queries',
                                 type=int,
                                 default=0,  # when -p is not called
                                 help='Query threshold')

    args = vars(parser.parse_args())  # Namespace object
    auth_params = ('host', 'port', 'client_id', 'client_secret')
    auth_args = {k: args[k] for k in auth_params}

    # authenticate
    looker = authenticate(**auth_args)
    q = queue.Queue()
    # map subcommand to function
    if args['command'] == 'analyze':
        if args['which'] is None:
            parser.error("No command")
        else:
            #result = ls(looker, **args)
            spinner_thread = SpinnerThread()
            spinner_thread.start()
            task = threading.Thread(target=analyze, args=[looker, q], kwargs=args)
            task.start()
            task.join()
            spinner_thread.stop()
            print(q.get())
    elif args['command'] == 'vacuum':
        # do fu stuff
        spinner_thread = SpinnerThread()
        spinner_thread.start()
        task = threading.Thread(target=vacuum, args=[looker, q], kwargs=args)
        task.start()
        task.join()
        spinner_thread.stop()
        print(q.get())
    else:
        print('No command passed')

# analyze func
# If project flag was used, call get_projects with list of projects or None.
def analyze(looker, queue, **kwargs):
    format = 'plain' if kwargs['plain'] else 'psql'
    if kwargs['which'] == 'projects':
        p = kwargs['project'] if kwargs['project'] is not None else None
        r = analyze_projects(looker, project=kwargs['project'], sortkey=kwargs['sortkey'], limit=kwargs['limit'])
        result = tabulate(r, headers='keys', tablefmt=format, numalign='center')
    elif kwargs['which'] == 'models':
        p = kwargs['project']
        m = kwargs['model'].split(' ') if kwargs['model'] is not None else None
        r = analyze_models(looker, project=p, model=m, sortkey=kwargs['sortkey'], limit=kwargs['limit'])
        result = tabulate(r, headers='keys', tablefmt=format, numalign='center')
    elif kwargs['which'] == 'explores':
        p = kwargs['project']
        m = kwargs['model'].split(' ') if kwargs['model'] is not None else None
        r = analyze_explores(looker, project=p, model=m, sortkey=kwargs['sortkey'], limit=kwargs['limit'])
        result = tabulate(r, headers='keys', tablefmt=format, numalign='center')

    queue.put(result)
    return

def vacuum(looker, queue, **kwargs):
    m = kwargs['model'].split(' ') if kwargs['model'] is not None else None
    if kwargs['which'] == 'models':
        r = vacuum_models(looker, model=m, min_queries=kwargs['min_queries'], timeframe=kwargs['timeframe'])
        result = tabulate(r, headers='keys', tablefmt='grid', numalign='center')
    if kwargs['which'] == 'explores':
        r = vacuum_explores(looker, model=m, explore=kwargs['explore'], min_queries=kwargs['min_queries'], timeframe=kwargs['timeframe'])
        result = tabulate(r, headers='keys', tablefmt='grid', numalign='center')
    queue.put(result)
    return

# parses strings for view_name.field_name and returns a list (empty if no matches)
def parse(string):
    return re.findall(r'(\w+\.\w+)', str(string))


# function that returns list of model definitions (verbose=1) or model names
# (verbose=0). Allows the user to specify a project name, a model name or
# nothing at all. project paramater is a string while model parameter is a list
def get_models(looker, project=None, model=None, verbose=0, scoped_names=0):
    if project is None and model is None:
        models = looker.get_models()
    elif project is not None and model is None:
        # if no parameters are specified
        response = looker.get_models()
        models = list(filter(lambda x: x['project_name'] == project, response))
    elif project is not None and model is not None:
        # if both project and model paramaters are specified
        print('''Warning: Project parameter ignored.
              Model names are unique across projects in Looker.''')
        models = [looker.get_model(m) for m in model]
    else:
        # if project parameter wasn't passed but model was. Behaves as above.
        models = [looker.get_model(m) for m in model]

    # error handling in case response is empty
    try:
        models = list(filter(lambda x: x['has_content'] is True, models))
        if verbose == 0:
            models = [(m['project_name']+".")*scoped_names+m['name'] for m in models]
    except:
        print("No results found.")
        return

    return models

# takes in a list of dictionaries. Dictionary keys
# MUST be in ('project', 'model', 'explore' and their *_ equivalent )
def tree(data, group_field):
    output = ""
    if group_field == 'project':
        output += 'Projects:\n'
        for i in data:
            output += ('    ' + u'\u251C' + u'\u2500' + u'\u2500' + ' ' + i['_project'] + '\n')
    elif group_field == 'model':
        for key, group in groupby(data, key=lambda data: data['project']):
            output += str(key) + ':\n' # project
            for i in group:
                output += ('   ' + u'\u251C' + u'\u2500' + u'\u2500' + ' ' + i['_model'] + '\n')
    elif group_field == 'explore':
        for key, group in groupby(data, itemgetter('project', 'model')):
            output += str(key[0]) + ':\n'  # project
            output += ('    ' + u'\u251C' + u'\u2500' + u'\u2500' + ' ' + key[1] + '\n')  # model
            for i in group:
                output += ('\t' + u'\u251C' + u'\u2500' + u'\u2500' + ' ' + i['_explore'] + '\n')
    return output


# returns a list of explores in a given project and/or model
# model must be a list. everything else is a string
def get_explores(looker, project=None, model=None, explore=None, scoped_names=0, verbose=0):
    explores = []
    if explore is not None:
        explores.extend([looker.get_explore(model_name=model[0], explore_name=explore)])
    else:
        if project is not None and model is None:
            # if project is specified, get all models in that project
            model_list = get_models(looker, project=project, verbose=1)
        elif project is None and model is None:
            # if no project or model are specified, get all models
            model_list = get_models(looker, verbose=1)
        else:
            # if project and model are specified or if project is not specified
            # but model is.
            model_list = get_models(looker, model=model, verbose=1)
        # if verbose = 1, then return explore bodies otherwise return explore names
        # which can be fully scoped with project name
        for mdl in model_list:
            if verbose == 1:
                explores.extend([looker.get_explore(model_name=mdl['name'], explore_name=explore['name']) for explore in mdl['explores']])
            else:
                explores.extend([(mdl['project_name']+'.'+mdl['name']+'.')*scoped_names+explore['name'] for explore in mdl['explores']])

    return explores


# returns a list of scoped fields of explores for a given model or explore
def get_explore_fields(looker, model=None, explore=None, scoped_names=0):
    fields = []
    explore_list = get_explores(looker, model=model, verbose=1)

    if explore is not None:
        # filter list based on explore names supplied
        explore_list = list(filter(lambda x: x['name'] == explore,
                                   explore_list))

    for explore in explore_list:
        fields.extend([(explore['model_name']+'.'+explore['name']+'.')*scoped_names+dimension['name'] for dimension in explore['fields']['dimensions'] if dimension['hidden'] is not True])
        fields.extend([(explore['model_name']+'.'+explore['name']+'.')*scoped_names+measure['name'] for measure in explore['fields']['measures'] if measure['hidden'] is not True])
        fields.extend([(explore['model_name']+'.'+explore['name']+'.')*scoped_names+fltr['name'] for fltr in explore['fields']['filters'] if fltr['hidden'] is not True])

    return list(set(fields))


def get_views(looker, project=None, model=None, explore=None, scoped_names=0):
    fields = get_explore_fields(looker, model=None,
                                explore=None, scoped_names=0)
    views = [field.split('.')[0] for field in fields]
    return list(set(views))


def get_projects(looker, project=None, verbose=0):
    if project is None:
        projects = looker.get_projects()
    else:
        projects = [looker.get_project(p) for p in project]

    if len(projects) == 0:
        print('No Projects Found.')
        return
    elif verbose == 0:
        projects = [p['id'] for p in projects]

    return projects


# Function that returns a json describing a project
def get_project_files(looker, project=None):
    if project is None:
        projects = looker.get_projects()
    else:
        projects = [looker.get_project(project)]

    if projects[0] is None:
        raise FileNotFoundError("No matching projects found.")

    project_data = []
    for project in projects:
        project_files = looker.get_project_files(project=project['id'])

        project_data.append({
                'name': project['id'],
                'pr_mode': project['pull_request_mode'],
                'validation_required': project['validation_required'],
                'git_remote_url': project['git_remote_url'],
                'files': project_files
        })

    return project_data


# def i__looker_query_body(model=None, timeframe):
# returns list of view scoped fields used within a given timeframe
def get_field_usage(looker, timeframe=90, model=None, project=None):
    if model is None:
        model = ','.join(get_models(looker)) # can return models that have had no queries run against them as well (since this is from an API end point)
    else:
        model = ','.join(model)

    timeframe = str(timeframe) + ' days'
    body = {
            "model": "i__looker",
            "view": "history",
            "fields": ["query.model", "query.view", "query.formatted_fields",
                       "query.formatted_filters", "query.sorts",
                       "query.formatted_pivots", "history.query_run_count"],
            "filters": {"history.created_date": timeframe,
                        "query.model": model},
            "limit": "50000"
    }

    response = looker.run_inline_query("json", body)

    return {'response': response, 'model': model.split(',')}


def analyze_models(looker, project=None, model=None, verbose=0, sortkey=None, limit=None):
    models = get_models(looker, project=project, model=model, verbose=1)
    used_models = get_used_models(looker)

    info = []
    for m in models:
        explore_count = len(m['explores'])
        info.append({
                'project': m['project_name'],
                'model': m['name'],
                'explore_count': len(m['explores']),
                'query_run_count': used_models[m['name']] if m['name'] in used_models else 0
        })

    valid_values = list(info[0].keys())
    info = sort_results(info, valid_values, sortkey)
    info = limit_results(info, limit=limit)
    return info


def sort_results(data, valid_values, sortkey):

    if sortkey is None:
        return data
    else:
        valid_types = {'ASC': False, 'DESC': True}
        sk = sortkey[0] if sortkey[0] in valid_values else False
        type = valid_types[sortkey[1].upper()] if sortkey[1].upper() in valid_types.keys() else None
        if not sk:
            raise ValueError('Unrecognised order_by field, must be in %r' % valid_values)
        elif type is None:
            raise ValueError('Unrecognised order_by field, must be in %r' % list(valid_types.keys()))
        else:
            # if everything is fine
            data = sorted(data, key=itemgetter(sk), reverse=type)

    return data


def limit_results(data, limit=None):
    if limit is not None:
        return data[:limit[0]]
    else:
        return data


def analyze_projects(looker, project=None, sortkey=None, limit=None):
    projects = get_project_files(looker, project=project)
    info = []

    for p in projects:
        metadata = list(map(lambda x:
                            'model' if x['type'] == 'model' else
                            ('view' if x['type'] == 'view' else None),
                            p['files']))

        model_count = metadata.count('model')
        view_count = metadata.count('view')
        git_tests = test_git_connection(looker, project=p['name'])
        info.append({
                'project': p['name'],
                'model_count': model_count,
                'view_count': view_count,
                'Git Connection': git_tests,
                'Pull Requests': p['pr_mode'],
                'Validation Required': p['validation_required']
        })

    valid_values = list(info[0].keys())
    info = sort_results(info, valid_values, sortkey)
    info = limit_results(info, limit=limit)
    return info


def analyze_explores(looker, project=None, model=None, explore=None, sortkey=None, limit=None, threshold=0):

    # Step 1 - get explore definitions
    explores = get_explores(looker, project=project, model=model, verbose=1)

    # Step 2 - get stats if verbose == 0 else get actual fields
    explores_usage = {}
    info = []
    if len(explores) == 0:
        raise FileNotFoundError("No matching explores found.")
    else:
        for e in explores:
            _used_fields = get_used_explore_fields(looker, model=e['model_name'], explore=e['scopes'])
            used_fields = list(_used_fields.keys())
            exposed_fields = get_explore_fields(looker, model=[e['model_name']], explore=e['name'], scoped_names=1)
            all_fields = list(filter(lambda x: x['name']=="ALL_FIELDS", e['sets']))[0]['value']
            unused_fields = set(exposed_fields) - set(used_fields)
            view_count = len(e['scopes'])
            field_count = len(e['fields']['dimensions'] +
                              e['fields']['measures'] +
                              e['fields']['filters'])
            join_count = len(e['joins'])
            query_count = sum(_used_fields.values())
            used_views = len(set([i.split('.')[0] for i in used_fields]))
            used_joins_count = used_views - 1 if used_views >= 1 else 0
            unused_joins_count = join_count - used_joins_count
            info.append({
                    'model': e['model_name'],
                    'explore': e['name'],
                    'view_count': view_count,
                    'join_count': join_count,
                    'unused_joins': unused_joins_count,
                    'field_count': field_count,
                    'unused_fields': len(unused_fields),
                    'Hidden': e['hidden'],
                    'Has Description': (colors.FAIL + 'No' + colors.ENDC) if e['description'] is None else 'Yes',
                    'query_count': query_count,
                    })

        valid_values = list(info[0].keys())
        info = sort_results(info, valid_values, sortkey)
        info = limit_results(info, limit=limit)
    return info


# function that runs i__looker query and returns fully scoped fields used
# remember explore names are not unique, filter on model as well
# query.explore is the actual explore name
# query.model is the model
# query.fields/filters_used is view.field (but view is the view name used in the explore)
# to uniquely identify fields, explore.view.field should be used. or even better, model.explore.view.field
def get_used_explore_fields(looker, project=None, model=None, explore=None, view=None, timeframe=90, min_queries=0):
        m = model.replace('_', '^_') + ',' if model is not None else ''
        m += "-i^_^_looker"
        e = ','.join(explore).replace('_', '^_')
        min_queries = '>=' + str(min_queries)
        timeframe = str(timeframe) + ' days'
        body = {
                "model": "i__looker",
                "view": "history",
                "fields": ["query.model", "query.view", "query.formatted_fields",
                           "query.formatted_filters", "query.sorts",
                           "query.formatted_pivots", "history.query_run_count"],
                "filters": {"history.created_date": timeframe,
                            "query.model": m,
                            "query.view": e,
                            "history.query_run_count": min_queries},
                "limit": "50000"
        }
        # returns only fields used from a given explore
        response = looker.run_inline_query("json", body)

        formatted_fields = []
        for row in response:
            fields = []
            explore = row['query.view']
            model = row['query.model']
            run_count = row['history.query_run_count']
            fields.extend(parse(row['query.formatted_fields']))
            fields.extend(parse(row['query.formatted_filters']))
            fields.extend(parse(row['query.formatted_pivots']))
            fields.extend(parse(row['query.sorts']))
            formatted_fields.extend([model+'.'+explore+'.'+field+'.'+str(run_count) for field in fields])

        field_name = []
        field_use_count = []
        for row in formatted_fields:
            field = '.'.join(row.split('.')[:-1])  # remove the count
            field_name.append(field)  # fields are model.explore.view scoped
            count = int(row.split('.')[-1])
            field_use_count.append({
                'field_name': field,
                'count': count
            })

        c = Counter()

        for value in field_use_count:
            c[value['field_name']] += value['count']

        return dict(c)


# returns a dictionary in the form of {field/explore/model/view: name,
#                                                         count: # uses}
# model names - not scoped (they're unique)
# view names - not scoped
# explores - model scoped
# fields - explore scoped
def aggregate_usage(looker, model=None, timeframe='90 days', agg_level=None):

    # make sure agg_level specified is recognised
    valid_agg_levels = ('field', 'view', 'explore', 'model')
    if agg_level not in valid_agg_levels:
        raise ValueError('agg_level: type must be one of %r.' % valid_agg_levels)

    # get usage across all models or for a specified model
    field_usage = get_field_usage(looker, timeframe=timeframe, model=model)

    response = field_usage['response']
    models = field_usage['model']
    formatted_fields = []
    for row in response:
        fields = []
        explore = row['query.view']
        model = row['query.model']
        run_count = row['history.query_run_count']
        fields.extend(parse(row['query.formatted_fields']))
        fields.extend(parse(row['query.formatted_filters']))
        fields.extend(parse(row['query.formatted_pivots']))
        fields.extend(parse(row['query.sorts']))
        formatted_fields.extend([model + '.' + explore + '.' + field + '.' +
                                str(run_count) for field in fields])
    aggregator_count = []
    aggregator = []

    if agg_level == 'field':
        for row in formatted_fields:
            field = '.'.join(row.split('.')[2:4])
            aggregator.append(field)
            count = int(row.split('.')[4])
            aggregator_count.append({
                'aggregator': field,
                'count': count
            })

        fields = [get_explore_fields(looker, model=[m]) for m in models]
        # flatten the list
        fields = [y for x in fields for y in x]
        [aggregator_count.append({'aggregator': field, 'count': 0}) for field in fields]

    if agg_level == 'view':
        for row in formatted_fields:
            view = row.split('.')[2]
            aggregator.append(view)
            count = int(row.split('.')[4])
            aggregator_count.append({
                'aggregator': view,
                'count': count
            })

        views = [get_views(looker, model=[model]) for model in models]
        # flatten the list
        views = [y for x in views for y in x]
        [aggregator_count.append({'aggregator': view, 'count': 0}) for view in views]

    if agg_level == 'explore':
        for row in formatted_fields:
            explore = row.split('.')[1]
            aggregator.append(explore)
            count = int(row.split('.')[4])
            aggregator_count.append({
                'aggregator': explore,
                'count': count
            })
        explores = [get_explores(looker, model=[model]) for model in models]
        # flatten the list
        explores = [y for x in explores for y in x]
        [aggregator_count.append({'aggregator': explore, 'count': 0}) for explore in explores]

    if agg_level == 'model':
        for row in formatted_fields:
            model = row.split('.')[0] # take out model
            aggregator.append(model) # append the model
            count = int(row.split('.')[4]) # get the count
            aggregator_count.append({
                'aggregator': model,
                'count': count
            })
        models = get_models(looker, model=[model])
        models = [get_explores(looker, model=[model]) for model in models]
        # flatten the list
        models = [y for x in models for y in x]
        [aggregator_count.append({'aggregator': model, 'count': 0}) for model in models]

    c = Counter()

    for value in aggregator_count:
        c[value['aggregator']] += value['count']

    return dict(c)


# get list of models and consider any explores below the specified threshold as unused
def vacuum_models(looker, model=None, timeframe=90, min_queries=0):

    if model is None:
        model = get_models(looker)
    #     explores = get_explores(looker, model=model, verbose=1)
    # else:
    used_models = get_used_models(looker, timeframe)
    info = []
    for m in model:
        explores = [e['name'] for e in get_explores(looker, model=[m], verbose=1)]
        unused_explores = get_unused_explores(looker, model=m, timeframe=timeframe, min_queries=min_queries)
        query_run_count = used_models[m] if m in used_models.keys() else 0
        unused_explores = ('\n').join(unused_explores)
        info.append({
                'model': m,
                'unused_explores': unused_explores if len(unused_explores)>0 else 'None',
                'model_query_run_count': query_run_count})

    return info


# returns explores, their unused joins as well as unused fields. Fields are
# considered unused if they are below the min_queries threshold
# similary, joins are considered unused if they all their feilds are below
# the thresold
def vacuum_explores(looker, model=None, explore=None, timeframe=90, min_queries=0):
    explores = get_explores(looker, model=model, explore=explore, verbose=1)
    info = []
    for e in explores:
        # get field usage from i__looker using all the views inside explore, returns fields in the form of model.explore.view.field
        _used_fields = get_used_explore_fields(looker, model=e['model_name'], explore=e['scopes'], timeframe=timeframe, min_queries=min_queries)
        used_fields = list(_used_fields.keys())
        # get fields in the field picker in the form of model.explore.view.field
        exposed_fields = get_explore_fields(looker, model=[e['model_name']], explore=e['name'], scoped_names=1)
        _unused_fields = set(exposed_fields) - set(used_fields)

        # remove scoping
        all_joins = set(e['scopes'])
        all_joins.remove(e['name'])
        used_joins = set([i.split('.')[2] for i in used_fields])

        _unused_joins = list(all_joins - used_joins)
        unused_joins = ('\n').join(_unused_joins) if len(_unused_joins)>0 else "N/A"

        # only keep fields that belong to used joins (unused joins fields
        # don't matter) if there's at least one used join (including the base view).
        # else don't match anything
        pattern = ('|').join(list(used_joins)) if len(used_joins)>0 else 'ALL'
        unused_fields = []
        if pattern != 'ALL':
            for field in _unused_fields:
                f = re.match(r'^({0}).*'.format(pattern), '.'.join(field.split('.')[2:]))
                if f is not None:
                    unused_fields.append(f.group(0))
            unused_fields = sorted(unused_fields)
            unused_fields = ('\n').join(unused_fields)
        else:
            unused_fields = pattern
        info.append({
                'model': e['model_name'],
                'explore': e['name'],
                'unused_joins': unused_joins,
                'unused_fields': unused_fields
                })

    return info


# returns an instanstiated Looker object using the
# credentials supplied by the auth argument group
def authenticate(**kwargs):
    if kwargs['client_id'] and kwargs['client_secret']:
        # if client_id and client_secret are passed, then use them
        looker = LookerApi(host=kwargs['host'],
                           port=kwargs['port'],
                           token=kwargs['client_id'],
                           secret=kwargs['client_secret'])
    else:
        # otherwise, find credentials in config file
        try:
            f = open('config.yml')
            params = yaml.load(f)
            f.close()
        except:
            print('config.yml not found.')

        try:
            my_host = params['hosts'][kwargs['host']]['host']
            my_secret = params['hosts'][kwargs['host']]['secret']  # secret
            my_token = params['hosts'][kwargs['host']]['token']  # client_id
            looker = LookerApi(host=my_host,
                               port=kwargs['port'],
                               token=my_token,
                               secret=my_secret)
        except:
            print('%s host not found' % kwargs['host'])
            return

    return looker


def test_git_connection(looker, project):
    # enter dev mode
    looker.update_session(mode='dev')
    # obtain tests available
    tests = [test['id'] for test in looker.git_connection_tests(project_id=project)]
    verbose_result = ''
    fail_flag = 0
    for idx, test in enumerate(tests):
        s = '({}/{}) {}'.format(idx+1, len(tests), test)
        r = looker.run_git_connection_test(project_id=project, test_id=test) # seems to be broken
        verbose_result += colors.OKGREEN + s + colors.ENDC + '\n' if r['status']=='pass' else colors.FAIL + s + colors.ENDC + '\n' # (s+': '+r['status']+'\n')
        if r['status'] != 'pass':
            fail_flag = 1

    result = verbose_result if fail_flag == 1 else 'OK'
    return result

def check_scheduled_plans(looker):
    body = {
            "model": "i__looker",
            "view": "scheduled_plan",
            "fields": ["scheduled_job.status", "scheduled_job.count"],
            "filters": {
                        "scheduled_plan.run_once": "no",
                        "scheduled_job.status": "-NULL"
                       },
            "limit": "50000"
            }

    response = looker.run_inline_query("json", body)

    print(response)


def get_used_models(looker, timeframe=90):
    timeframe = str(timeframe) + ' days'
    body = {
            "model": "i__looker",
            "view": "history",
            "fields": ["query.model", "history.query_run_count"],
            "filters": {"history.created_date": timeframe,
                        "query.model": "-i^_^_looker"
                        },
            "limit": "50000"
            }

    response = looker.run_inline_query("json", body)

    x = {}
    for r in response:
        x[r['query.model']] = r['history.query_run_count']
    return(x)


def get_used_explores(looker, model=None, timeframe=90, min_queries=0):
    timeframe = str(timeframe) + ' days'
    min_queries = '>=' + str(min_queries)
    m = model.replace('_', '^_') + ',' if model is not None else ''
    body = {
            "model": "i__looker",
            "view": "history",
            "fields": ["query.view", "history.query_run_count"],
            "filters": {"history.created_date": timeframe,
                        "query.model": m,
                        "history.query_run_count": min_queries
                        },
            "limit": "50000"
            }

    response = looker.run_inline_query("json", body)

    x = {}
    for r in response:
        x[r['query.view']] = r['history.query_run_count']
    return(x)


def get_unused_explores(looker, model=None, timeframe=90, min_queries=0):
    used_explores = get_used_explores(looker, model=model, timeframe=timeframe, min_queries=min_queries)
    used_explores = used_explores.keys()
    model = [model] if model is not None else None
    all_explores = get_explores(looker, model=model)
    unused_explores = list(set(all_explores) - set(used_explores))

    return unused_explores

def check_integrations(looker):
    response = looker.get_integrations()
    integrations = []
    for r in response:
        if r['enabled']:
            integrations.append(r['label'])

    result = None if len(integrations) == 0 else integrations

    return result


def check_legacy_features(looker):
    response = looker.get_legacy_features()
    _result = []
    for r in response:
        if r['enabled'] is True:
            _result.append(r['name'])

    result = _result if len(_result) > 0 else "Pass"
    return result


def get_connection_activity(looker, connection_name):
    body = {
            "model": "i__looker",
            "view": "history",
            "fields": ["history.query_run_count"],
            "filters": {"history.created_date": "90 days",
                        "history.connection_name": connection_name},
            "limit": "50000"
    }
    result = looker.run_inline_query("json", body)[0]['history.query_run_count']
    return result


def check_connections(looker, connection_name=None):
    result = []
    print('Checking connections\' status')
    if connection_name is None:
        connection_name = [c['name'] for c in looker.get_connections()]
    for c in connection_name:
        looker.test_connection(connection_name=c)
        result.append({'name': c,
                       'status': looker.test_connection(connection_name=c),
                       'query_count': get_connection_activity(looker, connection_name=c)})
    return result


def check_version(looker):
    version = re.findall(r'(\d.\d+)',looker.get_version()['looker_release_version'])[0]
    bcolor = colors.Colors()
    session = requests.Session()
    latest_version = session.get('https://learn.looker.com:19999/versions').json()['looker_release_version']
    latest_version = re.findall(r'(\d.\d+)', latest_version)[0]

    if version == latest_version:
        return "Looker version " + version + " (" + colors.BOLD + colors.OKGREEN + "PASS" + colors.ENDC + ')'
    else:
        return "Looker version " + version + " (" + colors.BOLD + colors.FAIL + "FAIL" + colors.ENDC + ')'

if __name__ == "__main__":
    main()
