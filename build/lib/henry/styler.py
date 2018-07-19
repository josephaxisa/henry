# styler.py
import logging
from tabulate import tabulate
from operator import itemgetter

style_logger = logging.getLogger('styler')


def limit(data, limit=None):
    if limit is not None:
        style_logger.info('Limiting results to %s', limit[0])
        return data[:limit[0]]
    else:
        return data


def sort(data, valid_values, sortkey):
    if sortkey is None:
        return data
    else:
        style_logger.info('Sort params=> %s', sortkey)
        valid_types = {'ASC': False, 'DESC': True}
        if sortkey[1].upper() in valid_types.keys():
            type = valid_types[sortkey[1].upper()]
        else:
            type = None

        sk = sortkey[0] if sortkey[0] in valid_values else False
        if not sk:
            style_logger.error('Sortkey:%s is invalid', sortkey[0])
            raise ValueError('Unrecognised order_by field, must be in %r' %
                             valid_values)
        elif type is None:
            style_logger.error('Sort type is invalid')
            raise ValueError('Unrecognised order_by field, must be in %r' %
                             list(valid_types.keys()))
        else:
            style_logger.info('Sorting data by %s %s', sk, type)
            data = sorted(data, key=itemgetter(sk), reverse=type)
    return data
