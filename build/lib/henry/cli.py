#!/usr/bin/env python3
import yaml
from .modules.lookerapi import LookerApi
from itertools import groupby
import argparse
import os
import errno
import sys
from operator import itemgetter
from .modules.spinner import Spinner
import threading
from tabulate import tabulate
from .modules.auth import authenticate
import logging.config
import henry
LOGGING_CONFIG_PATH = os.path.join(os.path.dirname(henry.__file__), '.support_files/logging.conf')
LOGGING_LOG_PATH = os.path.join(os.path.expanduser('~'), '.henry')
if not os.path.exists(LOGGING_LOG_PATH):
    os.mkdir(LOGGING_LOG_PATH)
elif os.path.exists(LOGGING_LOG_PATH) and not os.path.isdir(LOGGING_LOG_PATH):
    print('Cannot create log directory in %s' % LOGGING_LOG_PATH)
    sys.exit(1)
LOGGING_LOG_PATH = os.path.join(LOGGING_LOG_PATH, 'henry.log')
logging.config.fileConfig(LOGGING_CONFIG_PATH,
                          defaults={'logfilename': LOGGING_LOG_PATH},
                          disable_existing_loggers=False)
from .commands.analyze import Analyze
from .commands.vacuum import Vacuum
from .commands.pulse import Pulse

''
# ------- HERE ARE PARAMETERS TO CONFIGURE -------
host = 'mylooker'
timeframe = '90 days'
logger = logging.getLogger('main')
# sys.tracebacklimit = -1 # enable only on shipped release


def main():
    HELP_PATH = os.path.join(os.path.dirname(henry.__file__), '.support_files/help.rtf')
    with open(HELP_PATH, 'r', encoding='unicode_escape') as myfile:
        descStr = myfile.read()

    parser = argparse.ArgumentParser(
        description=descStr,
        formatter_class=argparse.RawTextHelpFormatter,
        prog='henry',
        usage='henry command subcommand '
              '[subcommand options] [global '
              'options]',
        add_help=False)

    subparsers = parser.add_subparsers(dest='command',
                                       help=argparse.SUPPRESS)
    parser.add_argument("-h", "--help", action="help", help=argparse.SUPPRESS)

    # subparsers.required = True # works, but might do without for now.

    health_subparser = subparsers.add_parser('pulse', help='pulse help')

    ls_parser = subparsers.add_parser('analyze', help='analyze help',
                                      usage='henry analyze')
    ls_parser.set_defaults(which=None)
    ls_subparsers = ls_parser.add_subparsers()
    projects_sc = ls_subparsers.add_parser('projects')
    models_sc = ls_subparsers.add_parser('models')
    explores_sc = ls_subparsers.add_parser('explores')

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
                              default=None,
                              help='Filter on project')
    models_group.add_argument('-model', '--model',
                              type=str,
                              default=None,
                              help='Filter on model')
    models_sc.add_argument('--timeframe',
                           type=int,
                           default=90,
                           help='Timeframe (between 0 and 90)')
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
                             default=None,
                             required=('--explore') in sys.argv,
                             help='Filter on model')
    explores_sc.add_argument('-e', '--explore',
                             default=None,
                             help='Filter on model')
    explores_sc.add_argument('--timeframe',
                             type=int,
                             default=90,
                             help='Timeframe (between 0 and 90)')
    explores_sc.add_argument('--min_queries',
                             type=int,
                             default=0,
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
    vacuum_parser = subparsers.add_parser('vacuum', help='vacuum help',
                                          usage='henry vacuum')
    vacuum_parser.set_defaults(which=None)
    vacuum_subparsers = vacuum_parser.add_subparsers()
    vacuum_models = vacuum_subparsers.add_parser('models')
    vacuum_explores = vacuum_subparsers.add_parser('explores')
    vacuum_models.set_defaults(which='models')
    vm_group = vacuum_models.add_mutually_exclusive_group()
    vm_group.add_argument('-p', '--project',
                          type=str,
                          default=None,
                          help='Filter on Project')
    vm_group.add_argument('-m', '--model',
                          type=str,
                          default=None,
                          help='Filter on model')

    vacuum_models.add_argument('--timeframe',
                               type=int,
                               default=90,
                               help='Usage period to examine (in the range of '
                                    '0-90 days). Default: 90 days.')

    vacuum_models.add_argument('--min_queries',
                               type=int,
                               default=0,
                               help='Vacuum threshold. Explores with less '
                                    'queries in the given usage period will '
                                    'be vacuumed. Default: 0 queries.')

    vacuum_explores.set_defaults(which='explores')
    vacuum_explores.add_argument('-m', '--model',
                                 type=str,
                                 default=None,
                                 required=('--explore') in sys.argv,
                                 help='Filter on model')

    vacuum_explores.add_argument('-e', '--explore',
                                 type=str,
                                 default=None,
                                 help='Filter on explore')

    vacuum_explores.add_argument('--timeframe',
                                 type=int,
                                 default=90,
                                 help='Timeframe (between 0 and 90)')

    vacuum_explores.add_argument('--min_queries',
                                 type=int,
                                 default=0,
                                 help='Query threshold')

    for subparser in [projects_sc, models_sc, explores_sc, vacuum_models,
                      vacuum_explores, health_subparser]:
        subparser.add_argument('--output',
                               type=str,
                               default=None,
                               help='Path to file for saving the output.')
        subparser.add_argument('-q', '--quiet',
                               action='store_true',
                               help='Silence output')
        subparser.add_argument_group("Authentication")
        subparser.add_argument('--host', type=str, default=host,
                               required=any(k in sys.argv for k in
                                            ['--client_id', '--cliet_secret',
                                             '--store']),
                               help=argparse.SUPPRESS)
        subparser.add_argument('--port', type=int, default=19999,
                               help=argparse.SUPPRESS)
        subparser.add_argument('--client_id', type=str,
                               required=any(k in sys.argv for k in
                                            ['--client_secret', '--store']),
                               help=argparse.SUPPRESS)
        subparser.add_argument('--client_secret', type=str,
                               required=any(k in sys.argv for k in
                                            ['--client_id', '--store']),
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
                               help='Show results in a table format without '
                                    'the gridlines')

    args = vars(parser.parse_args())
    logger.info('Parsing args, %s', args)
    auth_params = ('host', 'port', 'client_id', 'client_secret', 'persist',
                   'store', 'path')
    auth_args = {k: args[k] for k in auth_params}

    # authenticate
    looker = authenticate(**auth_args)

    # map subcommand to function
    if args['command'] in ('analyze', 'vacuum'):
        if args['which'] is None:
            parser.error("No command")
        else:
            with Spinner():
                if args['command'] == 'analyze':
                    analyze = Analyze(looker)
                    result = analyze.analyze(**args)
                else:
                    vacuum = Vacuum(looker)
                    result = vacuum.vacuum(**args)
        # silence outout if --silence flag is used
        if not args['quiet']:
            print(result)
    elif args['command'] == 'pulse':
                pulse = Pulse(looker)
                result = pulse.run_all()
    else:
        print('No command passed')

    # save to file if --output flag is used
    if args['output']:
        logger.info('Saving results to file: %s', args['output'])
        if os.path.isdir(args['output']):
            error = IsADirectoryError(errno.EISDIR,
                                      os.strerror(errno.EISDIR),
                                      args['output'])
            logger.error(error)
            raise error
        elif not args['output'].endswith(('.csv', '.txt')):
            error = ValueError('Output file must be CSV or TXT')
            logger.exception(error)
            raise error
        elif os.path.isfile(args['output']):
            error = FileExistsError(errno.EEXIST,
                                    os.strerror(errno.EEXIST),
                                    args['output'])
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
                raise(e)

if __name__ == "__main__":
    main()
