from dateutil.parser import parse
from dateutil.relativedelta import relativedelta

"""
Increments datetime by 1 unit

Parameters
----------

timeval: datetime.datetime
    A valid datetime

unit: relativedelta keyword argument
    The time unit to increment by

datestrformat: str
    datetime string format interpretable by datetime.strftime

decrement: boolean
    Weather to decrement rather than increment


Returns
-------

new_datestr: str
    Incremented string

"""

def increment(timeval, unit, decrement = False):
    kwargs = {unit: (-1 if decrement else 1)}
    timeval = timeval + relativedelta(**kwargs)

    return timeval


"""
Determines weather two continuous intervals intersect

Parameters
----------

interval1: tuple(datetime.datetime, datetime.datetime)
    A continuous interval in YYYY-MM-DD format

interval2: tuple(datetime.datetime, datetime.datetime)
    A continuous interval in YYYY-MM-DD format

unit: resample_options.member
    The minimum descrete unit for intervals

Returns
-------

intersects: bool
    Weather the intervals intersect
"""
def intervals_intersect(interval1, interval2, unit, datestrformat):
    s1, e1 = interval1
    s2, e2 = interval2

    return not (e1 < increment(s2, unit=unit, decrement=True) or increment(s1, unit=unit, decrement=True) > e2)