#!/usr/bin/env python3
import yaml
import json
from lookerapi import LookerApi
from collections import defaultdict
from collections import Counter
from itertools import groupby
import re
import argparse
import os
import errno
import sys
from operator import itemgetter
from spinnerthread import SpinnerThread
import threading
import queue
from tabulate import tabulate
import requests
import colors
from tqdm import tqdm
from tqdm import trange
import logging.config

# ------- HERE ARE PARAMETERS TO CONFIGURE -------
# host name in config.yml
host = 'mylooker'
#model that you wish to analyze
#model = ['thelook']

# How far you wish to look back
timeframe = '90 days'

colors = colors.Colors()


# progress bar specs
logging.config.fileConfig('logging.conf', disable_existing_loggers=False)
logger = logging.getLogger(__name__)

#sys.tracebacklimit = -1 # enable only on shipped release
def main():
    with open('help.rtf', 'r', encoding='unicode_escape') as myfile:
        descStr = myfile.read()

    parser = argparse.ArgumentParser(description=descStr,
                                     formatter_class=argparse.RawTextHelpFormatter,
                                     prog='henry',
                                     usage="""henry command subcommand [subcommand options] [global options]""",
                                     add_help=False)

    subparsers = parser.add_subparsers(dest='command',
                                       help=argparse.SUPPRESS)
    parser.add_argument("-h", "--help", action="help", help=argparse.SUPPRESS)

    # subparsers.required = True # works, but might do without for now.

    health_subparser = subparsers.add_parser('pulse', help='pulse help')

    ls_parser = subparsers.add_parser('analyze', help='analyze help', usage='henry analyze')
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
                             dest='sortkey',
                             help='Sort results by a field')
    projects_sc.add_argument('--limit',
                             type=int,
                             default=None,
                             nargs=1,
                             help='Limit results. No limit by default')

    # models subcommand
    models_sc.set_defaults(which='models')
    models_group = models_sc.add_mutually_exclusive_group()

    models_group.add_argument('-p', '--project',
                              type=str,
                              default=None,  # when -p is not called
                              help='Filter on project')
    models_group.add_argument('-model', '--model',
                              type=str,
                              default=None,  # when -p is not called
                              help='Filter on model')
    models_sc.add_argument('--timeframe',
                           type=int,
                           default=90,  # when -p is not called
                           help='Timeframe (between 0 and 90)')
    models_sc.add_argument('--min_queries',
                           type=int,
                           default=0,  # when -p is not called
                           help='Query threshold')
    models_sc.add_argument('--order_by',
                           nargs=2,
                           metavar=('ORDER_FIELD', 'ASC/DESC'),
                           dest='sortkey',
                           help='Sort results by a field')
    models_sc.add_argument('--limit',
                           type=int,
                           default=None,
                           nargs=1,
                           help='Limit results. No limit by default')

    # explores subcommand
    explores_sc.set_defaults(which='explores')
    explores_group = explores_sc.add_mutually_exclusive_group(required='--explore' in sys.argv)
    explores_group.add_argument('-p', '--project',
                                default=None,  # when -p is not called
                                help='Filter on project')
    explores_group.add_argument('-m', '--model',
                                default=None,
                                help='Filter on model')
    explores_sc.add_argument('-e', '--explore',
                             default=None,
                             help='Filter on model')
    explores_sc.add_argument('--timeframe',
                             type=int,
                             default=90,  # when -p is not called
                             help='Timeframe (between 0 and 90)')
    explores_sc.add_argument('--min_queries',
                             type=int,
                             default=0,  # when -p is not called
                             help='Query threshold')
    explores_sc.add_argument('--order_by',
                             nargs=2,
                             metavar=('ORDER_FIELD', 'ASC/DESC'),
                             dest='sortkey',
                             help='Sort results by a field')
    explores_sc.add_argument('--limit',
                             type=int,
                             default=None,
                             nargs=1,
                             help='Limit results. No limit by default')

    # VACUUM Subcommand
    vacuum_parser = subparsers.add_parser('vacuum', help='vacuum help', usage='henry vacuum')
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

    for subparser in [projects_sc, models_sc, explores_sc, vacuum_models, vacuum_explores, health_subparser]:
        subparser.add_argument('--output',
                               type=str,
                               default=None,
                               help='Path and/or name of file where to save output.')
        subparser.add_argument('-q', '--quiet',
                               action='store_true',
                               help='Silence output')
        subparser.add_argument_group("Authentication")
        subparser.add_argument('--host', type=str, default=host,
                               required='--client_id' in sys.argv
                                        or '--client_secret' in sys.argv
                                        or '--store' in sys.argv,
                               help=argparse.SUPPRESS)
        subparser.add_argument('--port', type=int, default=19999,
                               help=argparse.SUPPRESS)
        subparser.add_argument('--client_id', type=str,
                               required='--client_secret' in sys.argv
                                        or '--store' in sys.argv,
                               help=argparse.SUPPRESS)
        subparser.add_argument('--client_secret', type=str,
                               required='--client_id' in sys.argv
                                        or '--store' in sys.argv,
                               help=argparse.SUPPRESS)
        subparser.add_argument('--persist', action='store_true',
                               help=argparse.SUPPRESS)
        subparser.add_argument('--store', action='store_true',
                               help=argparse.SUPPRESS)
        subparser.add_argument('--path', type=str, default='',
                               help=argparse.SUPPRESS)
        subparser.add_argument('--plain',
                               default=None,
                               action='store_true',
                               help='Show results in a table format without the gridlines')

    args = vars(parser.parse_args())  # Namespace object
    logger.info('Parsing args, %s', args)
    auth_params = ('host', 'port', 'client_id', 'client_secret', 'persist', 'store', 'path')
    auth_args = {k: args[k] for k in auth_params}

    # authenticate
    looker = authenticate(**auth_args)
    q = queue.Queue()

    # map subcommand to function
    if args['command'] == 'analyze':
        if args['which'] is None:
            parser.error("No command")
        else:
            spinner_thread = SpinnerThread()
            spinner_thread.start()
            task = threading.Thread(target=analyze, args=[looker, q], kwargs=args)
            task.start()
            task.join()
            spinner_thread.stop()
            result = q.get()
    elif args['command'] == 'vacuum':
        # do fu stuff
        spinner_thread = SpinnerThread()
        spinner_thread.start()
        task = threading.Thread(target=vacuum, args=[looker, q], kwargs=args)
        task.start()
        task.join()
        spinner_thread.stop()
        result = q.get()
    elif args['command'] == 'pulse':
        pulse(looker)
        result = ''
    else:
        print('No command passed')

    # silence outout if --silence flag is used
    if not args['quiet']:
        print(result)

    # save to file if --output flag is used
    if args['output']:
        logger.info('Saving results to file: %s', args['output'])
        if os.path.isdir(args['output']):
            error = IsADirectoryError(errno.EISDIR, os.strerror(errno.EISDIR), args['output'])
            logger.error(error)
            raise error
        elif not (args['output'].endswith('.csv') or args['output'].endswith('.txt')):
            error = ValueError('Output file must be CSV or TXT')
            logger.error(error)
            raise error
        elif os.path.isfile(args['output']):
            error = FileExistsError(errno.EEXIST, os.strerror(errno.EEXIST), args['output'])
            logger.error(error)
            raise error
        else:
            try:
                f = open(args['output'], 'w+')
                f.write(result)
                f.close()
                logger.info('Results succesfully saved.')
            except Exception as e:
                logger.error(e)


def pulse(looker):
    logger.info('Checking instance pulse')
    logger.info('Checking Connections')
    result = check_connections(looker)
    print(result, end='\n')
    logger.info('Complete: Checking Connections')

    # check query stats
    logger.info('Analyzing Query Stats')
    r1, r2, r3 = check_query_stats(looker)
    print(r1)
    print(r2)
    print(r3, end='\n\n')
    logger.info('Complete: Analyzing Query Stats')

    # check scheduled plans
    logger.info('Analyzing Query Stats')
    with trange(1, desc='(3/5) Analyzing Scheduled Plans', bar_format="%s%s{postfix[0][value]}%s {desc}: {percentage:3.0f}%%|{bar}|[{elapsed}<{remaining}]" % (colors.BOLD, colors.OKGREEN, colors.ENDC), postfix=[dict(value="RUNNING")], ncols=100, miniters=0) as t:
        for i in t:
            result = check_scheduled_plans(looker)
            if type(result) == list and len(result) > 0:
                result = tabulate(result, headers="keys", tablefmt='psql', numalign='center')
            t.postfix[0]["value"] = 'DONE'
            t.update()
    print(result, end='\n\n')
    logger.info('Complete: Analyzing Scheduled Plans')


    # check enabled legacy features
    logger.info('Checking Legacy Features')
    with trange(1, desc='(4/5) Legacy Features', bar_format="%s%s{postfix[0][value]}%s {desc}: {percentage:3.0f}%%|{bar}|[{elapsed}<{remaining}]" % (colors.BOLD, colors.OKGREEN, colors.ENDC), postfix=[dict(value="RUNNING")], ncols=100, miniters=0) as t:
        for i in t:
            result = check_legacy_features(looker)
            t.postfix[0]["value"] = 'DONE'
            t.update()
    print(result, end='\n\n')
    logger.info('Complete: Checking Legacy Features')

    # check looker version
    logger.info('Checking Version')
    t = trange(1, desc='(5/5) Version', bar_format="%s%s{postfix[0][value]}%s {desc}: {percentage:3.0f}%%|{bar}|[{elapsed}<{remaining}]" % (colors.BOLD, colors.OKGREEN, colors.ENDC), postfix=[dict(value="RUNNING")], ncols=100)
    for i in t:
        result, status = check_version(looker)
        t.postfix[0]["value"] = "DONE"
        t.update()
    print(result, end='\n\n')
    logger.info('Complete: Checking Version')
    logger.info('Complete: Checking instance pulse')

    return


# analyze func
# If project flag was used, call get_projects with list of projects or None.
def analyze(looker, queue, **kwargs):
    format = 'plain' if kwargs['plain'] else 'psql'
    headers = '' if kwargs['plain'] else 'keys'
    p = kwargs['project']
    m = kwargs['model'].split(' ') if ('model' in kwargs.keys() and kwargs['model'] is not None) else None
    if kwargs['which'] == 'projects':
        logger.info('Analyzing Projects')
        logger.info('analyze projects params=%s', {k: kwargs[k] for k in {'project', 'sortkey', 'limit'}})
        r = analyze_projects(looker, project=p, sortkey=kwargs['sortkey'], limit=kwargs['limit'])
    elif kwargs['which'] == 'models':
        logger.info('Analyzing Models')
        logger.info('analyze models params=%s', {k: kwargs[k] for k in {'project', 'model', 'timeframe', 'min_queries', 'sortkey', 'limit'}})
        r = analyze_models(looker, project=p, model=m, sortkey=kwargs['sortkey'], limit=kwargs['limit'], timeframe=kwargs['timeframe'], min_queries=kwargs['min_queries'])
    elif kwargs['which'] == 'explores':
        logger.info('Analyzing Explores')
        logger.info('analyze explores params=%s', {k: kwargs[k] for k in {'project', 'model', 'explore', 'timeframe', 'min_queries', 'sortkey', 'limit'}})
        r = analyze_explores(looker, project=p, model=m, explore=kwargs['explore'], sortkey=kwargs['sortkey'], limit=kwargs['limit'], timeframe=kwargs['timeframe'], min_queries=kwargs['min_queries'])
    logger.info('Analyze Complete')
    result = tabulate(r, headers=headers, tablefmt=format, numalign='center')
    queue.put(result)
    return


def vacuum(looker, queue, **kwargs):
    m = kwargs['model'].split(' ') if kwargs['model'] is not None else None
    format = 'plain' if kwargs['plain'] else 'psql'
    headers = '' if kwargs['plain'] else 'keys'
    if kwargs['which'] == 'models':
        logger.info('Vacuuming Models')
        logger.info('vacuum models params=%s', {k: kwargs[k] for k in {'model', 'timeframe', 'min_queries'}})
        r = vacuum_models(looker, model=m, min_queries=kwargs['min_queries'], timeframe=kwargs['timeframe'])
    if kwargs['which'] == 'explores':
        logger.info('Vacuuming Explores')
        logger.info('vacuum explores params=%s', {k: kwargs[k] for k in {'model', 'explore', 'timeframe', 'min_queries'}})
        r = vacuum_explores(looker, model=m, explore=kwargs['explore'], min_queries=kwargs['min_queries'], timeframe=kwargs['timeframe'])
    logger.info('Vacuum Complete')
    result = tabulate(r, headers=headers, tablefmt=format, numalign='center')
    queue.put(result)
    return


# parse strings for view_name.field_name and return a list, empty if no matches
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
    if models[0] is None:
        return
    else:
        models = list(filter(lambda x: x['has_content'] is True, models))
        if verbose == 0:
            models = [(m['project_name']+".")*scoped_names+m['name'] for m in models]
        return models
    # try:
    #     # models = list(filter(lambda x: x['has_content'] is True, models))
    #     # if verbose == 0:
    #     #     models = [(m['project_name']+".")*scoped_names+m['name'] for m in models]
    # except IndexError as e:
    #     raise(e)
    # return models


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
        e = [looker.get_explore(model_name=model[0], explore_name=explore)]
        if e[0] is not None:
            explores.extend(e)
        else:
            pass
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
    explore_list = get_explores(looker, model=model, explore=explore, verbose=1)

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


def analyze_models(looker, project=None, model=None, verbose=0, sortkey=None, limit=None, timeframe=90, min_queries=0):
    models = get_models(looker, project=project, model=model, verbose=1)
    if models is None:
        os._exit(1)
    used_models = get_used_models(looker, timeframe, min_queries)

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
                'Pull Requests': p['pr_mode'] if p['pr_mode'] in ('recommended', 'required') else colors.FAIL+p['pr_mode']+colors.ENDC,
                'Validation Required': p['validation_required'] if p['validation_required'] else colors.FAIL+str(p['validation_required'])+colors.ENDC
        })

    valid_values = list(info[0].keys())
    info = sort_results(info, valid_values, sortkey)
    info = limit_results(info, limit=limit)
    return info


def analyze_explores(looker, project=None, model=None, explore=None, sortkey=None, limit=None, min_queries=0, timeframe=90):

    # Step 1 - get explore definitions
    explores = get_explores(looker, project=project, model=model, explore=explore, verbose=1)

    if explores == []:
        os._exit(1)
    # Step 2
    explores_usage = {}
    info = []
    if explores is None:
        raise FileNotFoundError("No matching explores found.")
    else:
        for e in explores:
            if e is None:
                pass
            else:
                _used_fields = get_used_explore_fields(looker, model=e['model_name'], explore=e['scopes'], timeframe=timeframe, min_queries=min_queries)
                used_fields = list(_used_fields.keys())
                exposed_fields = get_explore_fields(looker, model=[e['model_name']], explore=e['name'], scoped_names=1)
                unused_fields = set(exposed_fields) - set(used_fields)
                field_count = len(exposed_fields)
                query_count = get_used_explores(looker, model=e['model_name'], explore=e['name']) #, timeframe=timeframe, min_queries=min_queries)

                # joins
                all_joins = set(e['scopes'])
                all_joins.remove(e['name'])
                used_joins = set([i.split('.')[2] for i in used_fields])
                unused_joins = len(list(all_joins - used_joins))

                info.append({
                        'model': e['model_name'],
                        'explore': e['name'],
                        'join_count': len(all_joins),
                        'unused_joins': unused_joins,
                        'field_count': field_count,
                        'unused_fields': len(unused_fields),
                        'Hidden': e['hidden'],
                        'Has Description': (colors.FAIL + 'No' + colors.ENDC) if e['description'] is None else 'Yes',
                        'query_count': query_count[e['name']] if query_count.get(e['name']) else 0
                        })

        valid_values = list(info[0].keys())
        info = sort_results(info, valid_values, sortkey)
        info = limit_results(info, limit=limit)
    return info


# function that runs i__looker query and returns fully scoped fields used
# remember explore names are not unique, filter on model as well
# query.explore is the actual explore name
# query.model is the model
# query.fields are is view.field (view is the view name used in the explore)
# to uniquely identify fields, explore.view.field should be used,
# or even better, model.explore.view.field
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
        unused_joins = ('\n').join(_unused_joins) if len(_unused_joins) > 0 else "N/A"

        # only keep fields that belong to used joins (unused joins fields
        # don't matter) if there's at least one used join (including the base
        # view). else don't match anything
        temp = list(used_joins)
        temp.append(e['name'])
        pattern = ('|').join(temp) if len(used_joins) > 0 else 'ALL'
        unused_fields = []
        if pattern != 'ALL':
            for field in _unused_fields:
                f = re.match(r'^({0}).*'.format(pattern), '.'.join(field.split('.')[2:]))
                if f is not None:
                    unused_fields.append(f.group(0))
            unused_fields = sorted(unused_fields)
            unused_fields = ('\n').join(unused_fields)
        else:
            unused_fields = colors.FAIL+pattern+colors.ENDC
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
    logger.info('Authenticating into Looker API')
    filepath = kwargs['path']+'config.yml'
    cleanpath = os.path.abspath(filepath)
    if kwargs['client_id'] and kwargs['client_secret']:
        # if client_id and client_secret are passed, then use them
        logger.info('Fetching auth params passed in CLI')
        host = kwargs['host']
        client_id = kwargs['client_id']
        client_secret = kwargs['client_secret']
        token = None
    else:
        # otherwise, find credentials in config file
        logger.info('Opening config file from %s', cleanpath)
        try:
            f = open(cleanpath, 'r')
            params = yaml.load(f)
            f.close()
        except FileNotFoundError as error:
            logger.exception(error, exc_info=False)
            print(error)
            sys.exit(1)

        try:
            logger.info('Fetching auth credentials from file, %s', cleanpath)
            host = params['hosts'][kwargs['host']]['host']
            client_secret = params['hosts'][kwargs['host']]['secret']  # secret
            client_id = params['hosts'][kwargs['host']]['id']  # client_id
            token = params['hosts'][kwargs['host']]['access_token'] #  last auth token (it will work if --persist was previously used, otherwise it fails)
        except KeyError as error:
            logger.info(error, exc_info=False)
            print(error)
            sys.exit(1)

    logger.info('auth params=%s', {'host': host, 'port' : kwargs['port'], 'client_id' : client_id, 'client_secret' : "[FILTERED]"})
    looker = LookerApi(host=host,
                       port=kwargs['port'],
                       id=client_id,
                       secret=client_secret,
                       access_token=token)
    logger.info('Authentication Successful')

    # update config file with latest access token if user wants to persist session
    if kwargs['store']:
        logger.info('Saving credentials to file: %s', cleanpath)
        with open(cleanpath, 'r') as f:
            params['hosts'][kwargs['host']] = {}
            params['hosts'][kwargs['host']]['host'] = host
            params['hosts'][kwargs['host']]['id'] = client_id
            params['hosts'][kwargs['host']]['secret'] = client_secret
            params['hosts'][kwargs['host']]['access_token'] = ''

        with open(cleanpath, 'w') as f:
            yaml.safe_dump(params, f, default_flow_style=False)

        os.chmod(cleanpath, 0o600)

    if kwargs['persist']:
        logger.info('Persisting API session. Saving auth token under %s in %s', host, cleanpath)
        with open(cleanpath, 'r+') as f:
            params = yaml.safe_load(f)
            params['hosts'][kwargs['host']]['access_token'] = looker.get_access_token()

        with open(cleanpath, 'w') as f:
            yaml.safe_dump(params, f, default_flow_style=False)

        os.chmod(cleanpath, 0o600)

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
        r = looker.run_git_connection_test(project_id=project, test_id=test)
        verbose_result += colors.OKGREEN + s + colors.ENDC + '\n' if r['status']=='pass' else colors.FAIL + s + colors.ENDC + '\n'
        if r['status'] != 'pass':
            fail_flag = 1

    result = verbose_result if fail_flag == 1 else 'OK'
    return result


def check_scheduled_plans(looker):
    body = {
            "model": "i__looker",
            "view": "scheduled_plan",
            "fields": ["scheduled_job.status", "scheduled_job.count"],
            "pivots": ["scheduled_job.status"],
            "filters": {
                        "scheduled_plan.run_once": "no",
                        "scheduled_job.status": "-NULL",
                        "scheduled_job.created_date": "30 days"
                       },
            "limit": "50000"
            }

    r = looker.run_inline_query("json", body)
    result = []
    if len(r) > 0:
        r = r[0]
        failed = r['scheduled_job.count']['scheduled_job.status']['failure'] if r['scheduled_job.count']['scheduled_job.status']['failure'] is not None else 0
        succeeded = r['scheduled_job.count']['scheduled_job.status']['success'] if r['scheduled_job.count']['scheduled_job.status']['success'] is not None else 0
        result.append({'total': failed+succeeded,
                       'failure': failed,
                       'success': succeeded})
        return result
    else:
        return "No Plans Found"


def get_used_models(looker, timeframe=90, min_queries=0):
    timeframe = str(timeframe) + ' days'
    min_queries = '>=' + str(min_queries)
    body = {
            "model": "i__looker",
            "view": "history",
            "fields": ["query.model", "history.query_run_count"],
            "filters": {"history.created_date": timeframe,
                        "query.model": "-i^_^_looker",
                        "history.query_run_count": min_queries
                        },
            "limit": "50000"
            }

    response = looker.run_inline_query("json", body)

    x = {}
    for r in response:
        x[r['query.model']] = r['history.query_run_count']
    return(x)


def get_used_explores(looker, model=None, timeframe=90, min_queries=0, explore=None):
    timeframe = str(timeframe) + ' days'
    min_queries = '>=' + str(min_queries)
    m = model.replace('_', '^_') + ',' if model is not None else ''
    body = {
            "model": "i__looker",
            "view": "history",
            "fields": ["query.view", "history.query_run_count"],
            "filters": {"history.created_date": timeframe,
                        "query.model": m,
                        "history.query_run_count": min_queries,
                        "query.view": explore
                        },
            "limit": "50000"
            }

    response = looker.run_inline_query("json", body)

    x = {}
    for r in response:
        x[r['query.view']] = r['history.query_run_count']
    return(x)


def get_unused_explores(looker, model=None, timeframe=90, min_queries=0):
    used_explores = get_used_explores(looker,
                                      model=model,
                                      timeframe=timeframe,
                                      min_queries=min_queries)
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
            _result.append({'Legacy Features' : r['name']})

    result = tabulate(_result, headers="keys", tablefmt='psql') if len(_result) > 0 else 'No legacy features found'
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
    connections = []
    if connection_name is None:
        for c in looker.get_connections():
            if c['name'] != 'looker':
                c_tests = (', ').join(c['dialect']['connection_tests'])
                c_name = c['name']
                connections.append((c_name, c_tests))

    with tqdm(total=len(connections), bar_format="%s%s{postfix[0][value]}%s - {desc}: {percentage:3.0f}%%|{bar}|[{elapsed}<{remaining}]" % (colors.BOLD, colors.OKGREEN, colors.ENDC), postfix=[dict(value="RUNNING")], ncols=100, miniters=0, desc='(1/5) Testing Connections') as t:
        for idx, (c, tests) in enumerate(connections):
            test_result = looker.test_connection(connection_name=c, fields={'tests': tests})
            test_result = list(set([i['message'] for i in test_result]))
            status = ('\n').join(test_result)
            result.append({'name': c,
                           'status': status})
            if idx == len(connections)-1:
                t.postfix[0]['value'] = 'DONE'
            t.update()

    return tabulate(result, tablefmt='psql')

def check_version(looker):
    version = re.findall(r'(\d.\d+)', looker.get_version()['looker_release_version'])[0]
    session = requests.Session()
    latest_version = session.get('https://learn.looker.com:19999/versions').json() ['looker_release_version']
    latest_version = re.findall(r'(\d.\d+)', latest_version)[0]
    if version == latest_version:
        return version, "up-to-date"
    else:
        return version, "outdated"


def check_query_stats(looker):
    # check query stats
    with trange(3, desc='(2/5) Analyzing Query Stats', bar_format="%s%s{postfix[0][value]}%s {desc}: {percentage:3.0f}%%|{bar}|[{elapsed}<{remaining}]" % (colors.BOLD, colors.OKGREEN, colors.ENDC), postfix=[dict(value="RUNNING")], ncols=100, miniters=0) as t:
        for i in t:
            if i == 0:
                query_count = get_query_type_count(looker)
            if i == 1:
                query_runtime_stats = get_query_stats(looker, status='complete')
            if i == 2:
                query_queue_stats = get_query_stats(looker, status='pending')
                t.postfix[0]['value'] = 'DONE'

    r1 = '{} queries run, {} queued, {} errored, {} killed'.format(query_count['total'], query_count['queued'], query_count['errored'], query_count['killed'])
    r2 = 'Query Runtime min/avg/max: {}/{}/{} seconds'.format(query_runtime_stats['min'], query_runtime_stats['avg'], query_runtime_stats['max'])
    r3 = 'Queuing time min/avg/max: {}/{}/{}'.format(query_queue_stats['min'], query_queue_stats['avg'], query_queue_stats['max'])

    return r1, r2, r3


# get number of queries run, killed, completed, errored, queued
def get_query_type_count(looker):
    body = {
            "model": "i__looker",
            "view": "history",
            "fields": [
                "history.query_run_count",
                "history.status",
                "history.created_date"
                ],
            "pivots": [
                "history.status"
                ],
            "filters": {
                "history.created_date": "30 days",
                "history.status": "-NULL",
                "history.result_source": "query",
                "query.model": "-i^_^_looker"
            },
            "sorts": [
                "history.created_date desc",
                "history.result_source"
                ],
            "limit": "50000"
            }

    r = looker.run_inline_query("json", body, fields={"cache": "false"})
    completed = 0
    errored = 0
    killed = 0
    queued = 0
    if(len(r) > 0):
        for entry in r:
            if 'complete' in entry['history.query_run_count']['history.status']:
                c_i = entry['history.query_run_count']['history.status']['complete']
            else:
                c_i = 0
            c_i = c_i if c_i is not None else 0
            completed += c_i

            if 'error' in entry['history.query_run_count']['history.status']:
                e_i = entry['history.query_run_count']['history.status']['error']
            else:
                e_i = 0
            e_i = e_i if e_i is not None else 0
            errored += e_i

            if 'killed' in entry['history.query_run_count']['history.status']:
                k_i = entry['history.query_run_count']['history.status']['killed']
            else:
                k_i = 0
            k_i = k_i if k_i is not None else 0
            killed += k_i

            if 'pending' in entry['history.query_run_count']['history.status']:
                q_i = entry['history.query_run_count']['history.status']['pending']
            else:
                q_i = 0
            q_i = q_i if q_i is not None else 0
            queued += q_i

    response = {'total': completed+errored+killed,
                'completed': completed,
                'errored': errored,
                'killed': killed,
                'queued': queued}

    return response


# get number of queries run, killed, completed, errored, queued
def get_query_stats(looker, status):
    valid_statuses = ['error', 'complete', 'pending', 'running']
    if status not in valid_statuses:
        raise ValueError("Invalid query status, must be in %r" % valid_statuses)
    body = {
            "model": "i__looker",
            "view": "history",
            "fields": [
                "history.min_runtime",
                "history.max_runtime",
                "history.average_runtime",
                "history.total_runtime"
                ],
            "filters": {
                "history.created_date": "30 days",
                "history.status": status,
                "query.model": "-i^_^_looker"
            },
            "limit": "50000"
            }

    r = looker.run_inline_query("json", body, fields={"cache": "false"})[0]
    response = {'min': round(r['history.min_runtime'], 2) if r['history.min_runtime'] is not None else '-',
                'max': round(r['history.max_runtime'], 2) if r['history.max_runtime'] is not None else '-',
                'avg': round(r['history.average_runtime'], 2) if r['history.average_runtime'] is not None else '-',
                'total': r['history.total_runtime']}

    return response

if __name__ == "__main__":
    main()
