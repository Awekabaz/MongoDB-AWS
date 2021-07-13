from datetime import *

def formatNumberHelper(number) -> int:
    if number != 'nan':
        total = ''
        [total := total + x for x in number.split(' ')]
        return total

def formatDateHelper(string) -> str:
    if string != 'nan' and string != 'NaT' and string != 'None' and '-' not in string:
        ts = float(string) / 1000.0
        return datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

    if string != 'nan' and string != 'NaT' and string != 'None':
        return datetime.strptime(string.split('.')[0], '%Y-%m-%d %H:%M:%S')

    return 'NaT'
