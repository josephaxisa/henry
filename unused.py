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
from spinner import Spinner
import threading
import queue
from tabulate import tabulate
import requests
import colors
from tqdm import tqdm
from tqdm import trange
import logging.config
logging.config.fileConfig('logging.conf', disable_existing_loggers=False)
from analyze import Analyze
from vacuum import Vacuum


# ------- HERE ARE PARAMETERS TO CONFIGURE -------
# host name in config.yml
host = 'mylooker'
#model that you wish to analyze
#model = ['thelook']

# How far you wish to look back
timeframe = '90 days'

colors = colors.Colors()


# progress bar specs
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
    explores_sc.add_argument('-model', '--model',
                             type=str,
                             default=None,  # when -p is not called
                             required=('--explore') in sys.argv,
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
    vm_group = vacuum_models.add_mutually_exclusive_group()
    vm_group.add_argument('-p', '--project',
                          type=str,
                          default=None,  # when -p is not called
                          help='Filter on Project')
    vm_group.add_argument('-m', '--model',
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
    analyze = Analyze(looker)
    vacuum = Vacuum(looker)
    #info = analyze.analyze_projects(project=args['project'], sortkey=args['sortkey'], limit=args['limit'])
    #info = analyze.analyze_models()
    #print(tabulate(info))
    #print('works!!')
    q = queue.Queue()

    # map subcommand to function
    if args['command'] == 'analyze':
        if args['which'] is None:
            parser.error("No command")
        else:
            with Spinner():
                result = analyze.analyze(**args)
    elif args['command'] == 'vacuum':
        # do fu stuff
            with Spinner():
                result = vacuum.vacuum(**args)
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
            logger.exception(error)
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
            tests = {'tests': tests}
            results = looker.test_connection(c, tests)
            formatted_results = []
            for i in results:
                formatted_results.append(colors.format(i['message'], i['status']))
            formatted_results = list(set(formatted_results))
            status = ('\n').join(formatted_results)
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
