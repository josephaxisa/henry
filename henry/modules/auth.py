import os
import logging
import yaml
import sys
from .lookerapi import LookerApi

auth_logger = logging.getLogger('auth')


# returns an instanstiated Looker object using the
# credentials supplied by the auth argument group
def authenticate(**kwargs):
    auth_logger.info('Authenticating into Looker API')
    filepath = kwargs['path'] + 'config.yml'
    cleanpath = os.path.abspath(filepath)
    if kwargs['client_id'] and kwargs['client_secret']:
        auth_logger.info('Fetching auth params passed in CLI')
        host = kwargs['host']
        client_id = kwargs['client_id']
        client_secret = kwargs['client_secret']
        token = None
    else:
        auth_logger.info('Checking permissions for %s', cleanpath)
        st = os.stat(cleanpath)
        ap = oct(st.st_mode)
        if ap != '0o100600':
            print('Config file permissions are set to %s and are not strict '
                  'enough. Change to rw------- or 600 and try again.' % ap)
            auth_logger.warning('Config file permissions are %s and not strict'
                                ' enough.' % ap)
            sys.exit(1)
        auth_logger.info('Opening config file from %s' % cleanpath)
        try:
            f = open(cleanpath, 'r')
            params = yaml.load(f)
            f.close()
        except FileNotFoundError as error:
            auth_logger.exception(error, exc_info=False)
            print('ERROR: %s not found' % filepath)
            sys.exit(1)

        try:
            auth_logger.info('Fetching auth credentials from file, %s',
                             cleanpath)
            host = params['hosts'][kwargs['host']]['host']
            client_secret = params['hosts'][kwargs['host']]['secret']
            client_id = params['hosts'][kwargs['host']]['id']
            #  last auth token. Works if --persist was previously used,
            # otherwise it fails)
            token = params['hosts'][kwargs['host']]['access_token']
        except KeyError as error:
            auth_logger.error('Auth Error: %s not found' % error,
                              exc_info=False)
            print('ERROR: %s not found' % error)
            sys.exit(1)

    auth_logger.info('auth params=%s', {'host': host,
                                        'port': kwargs['port'],
                                        'client_id': client_id,
                                        'client_secret': "[FILTERED]"})
    looker = LookerApi(host=host,
                       port=kwargs['port'],
                       id=client_id,
                       secret=client_secret,
                       access_token=token)
    auth_logger.info('Authentication Successful')

    if kwargs['store']:
        auth_logger.info('Saving credentials to file: %s', cleanpath)
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
        auth_logger.info('Persisting API session. Saving auth token under '
                         '%s in %s', host, cleanpath)
        with open(cleanpath, 'r+') as f:
            params = yaml.safe_load(f)
            access_token = looker.get_access_token()
            params['hosts'][kwargs['host']]['access_token'] = access_token

        with open(cleanpath, 'w') as f:
            yaml.safe_dump(params, f, default_flow_style=False)

        os.chmod(cleanpath, 0o600)

    return looker
