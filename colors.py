class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    LOOKER = '\033[35m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

    # string is the original string
    # type determines the color/label added
    # style can be color or text
    def format(self, string, type, style='color'):
        formatted_string = ''
        if style == 'color':
            if type in ('success', 'pass'):
                formatted_string += self.OKGREEN + string + self.ENDC
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
