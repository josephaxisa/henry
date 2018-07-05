# formatter.py


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
