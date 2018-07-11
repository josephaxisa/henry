# formatter.py


# string is the original string
# type determines the color/label added
# style can be color or text
class color(object):
    GREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'

    def format(self, string, type, style='color'):
        formatted_string = ''
        if style == 'color':
            if type in ('success', 'pass'):
                formatted_string += self.GREEN + string + self.ENDC
            elif type == 'warning':
                formatted_string += self.WARNING + string + self.ENDC
            elif type in ('error', 'fail'):
                formatted_string += self.FAIL + string + self.ENDC
        elif style == 'text':
            if type == 'success':
                formatted_string += string
            elif type == 'warning':
                formatted_string += 'WARNING: ' + string
            elif type == 'error':
                formatted_string += 'ERROR: ' + string

        return formatted_string


def limit(data, limit=None):
    if limit is not None:
        return data[:limit[0]]
    else:
        return data


def sort(data, valid_values, sortkey):
    if sortkey is None:
        return data
    else:
        valid_types = {'ASC': False, 'DESC': True}
        if sortkey[1].upper() in valid_types.keys():
            type = valid_types[sortkey[1].upper()]
        else:
            type = None

        sk = sortkey[0] if sortkey[0] in valid_values else False
        if not sk:
            raise ValueError('Unrecognised order_by field, must be in %r' %
                             valid_values)
        elif type is None:
            raise ValueError('Unrecognised order_by field, must be in %r' %
                             list(valid_types.keys()))
        else:
            data = sorted(data, key=itemgetter(sk), reverse=type)
    return data
