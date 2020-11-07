******************
Built in functions
******************

List of Built-in SQL functions.


===============
Numpy functions
===============


np.*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**np.\*** - All the functions from the `numpy` package are supported by default via the `np.*` namespace.

For example:
""""""""""""
| ``select np.log(total) from passengers``
| ``select np.power(np.min(size), 3) as cubed from measurements``



===================
Type cast functions
===================

bool
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**bool(arg)** - Casts argument to bool type.

float
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**float(arg)** - Casts argument to float type.

int
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**int(arg)** - Casts argument to int type.

str
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**str(arg)** - Casts argument to str type.





==============
Math functions
==============


abs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**abs(arg)** - returns the absolute value of the numerical
argument.

See: `numpy.absolute <https://numpy.org/doc/stable/reference/generated/numpy.absolute.html>`_


sqrt
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**sqrt(arg)** - returns the square root of the numerical
argument.

See: `numpy.sqrt <https://numpy.org/doc/stable/reference/generated/numpy.sqrt.html>`_

cos
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**cos(arg)** - returns the cosine of the argument.

See: `numpy.cos <https://numpy.org/doc/stable/reference/generated/numpy.cos.html>`_

sin
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**sin(arg)** - returns the sine of the argument.

See: `numpy.sin <https://numpy.org/doc/stable/reference/generated/numpy.sin.html>`_

tan
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**tan(arg)** - returns the tangent of the argument.

See: `numpy.tan <https://numpy.org/doc/stable/reference/generated/numpy.tan.html>`_

power
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**power(arg, power)** - returns the argument(s) raised to the power.

See: `numpy.power <https://numpy.org/doc/stable/reference/generated/numpy.power.html>`_

log
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**log(arg)** - returns the natural log of the argument.

See: `numpy.log <https://numpy.org/doc/stable/reference/generated/numpy.log.html>`_

log2
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**log2(arg)** - returns the log base 2 of the argument.

See: `numpy.log2 <https://numpy.org/doc/stable/reference/generated/numpy.log2.html>`_

log10
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**log10(arg)** - returns the log base 10 of the argument.

See: `numpy.log10 <https://numpy.org/doc/stable/reference/generated/numpy.log10.html>`_




================
String functions
================


concat
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**concat(arg1, arg2, ...)** - concatenate string arguments.

If argument is not a string type, would be converted to string.

See: `numpy.char.add <https://numpy.org/doc/stable/reference/generated/numpy.char.add.html>`_

upper
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**upper(arg)** - convert a string to uppercase.

See: `numpy.char.upper <https://numpy.org/doc/1.19/reference/generated/numpy.char.upper.html>`_

lower
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**lower(arg)** - convert a string to lowercase.

See: `numpy.char.lower <https://numpy.org/doc/1.19/reference/generated/numpy.char.lower.html>`_




==================
Datetime functions
==================



date
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**date(arg)** - converts the argument to `date` type.

Input is either a string in ISO8601 format or integer timestamp.

Use `date('now')` for current date.

See: `numpy.datetime <https://numpy.org/doc/stable/reference/arrays.datetime.html>`_


datetime
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**datetime(arg, unit)** - converts the argument to `datetime` type.

Input is either a string in ISO8601 format or integer timestamp.

Supported units are: ['D', 's', 'ms', 'us', 'ns']

| 'D' - days
| 's' - seconds
| 'ms' - milliseconds
| 'us' - microseconds
| 'ns' - nanoseconds


Use `datetime('now')` for current datetime.

See: `numpy.datetime <https://numpy.org/doc/stable/reference/arrays.datetime.html>`_


from_timestamp
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**from_timestamp(arg, unit)** - converts the integer timestamp to `datetime` type.
Argument represents integer value of the timestamp, ie number of seconds (or milliseconds) since epoch.

Supported units are : ['s', 'ms', 'us', 'ns']

| 's' - seconds
| 'ms' - milliseconds
| 'us' - microseconds
| 'ns' - nanoseconds


See: `numpy.datetime <https://numpy.org/doc/stable/reference/arrays.datetime.html>`_

timedelta
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**timedelta(arg, unit)** - returns the `timedelta` type.
Argument represents the duration.

Supported units are : ['Y', 'M', 'W', 'D', 'h', 'm', 's', 'ms', 'us', 'ns']

| 'Y' - years
| 'M' - months
| 'W' - weeks
| 'D' - days
| 'h' - hours
| 'm' - minutes
| 's' - seconds
| 'ms' - milliseconds
| 'us' - microseconds
| 'ns' - nanoseconds


See: `numpy.datetime.timedelta <https://numpy.org/doc/stable/reference/arrays.datetime.html#datetime-and-timedelta-arithmetic>`_


is_busday
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**is_busday(arg)** - returns True if the argument is a 'business' day.

See: `numpy.datetime.is_busday <https://numpy.org/doc/stable/reference/arrays.datetime.html#business-day-functionality>`_



===================
Aggregate functions
===================


count
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

| **count(*)** - returns the number of rows in the group.
| **count(expr | column)** - returns the number of non-null rows in the group.


sum
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**sum(expr | column)** - returns the sum of the values in the group.

See: `numpy.sum <https://numpy.org/doc/stable/reference/generated/numpy.sum.html>`_


min
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**min(expr | column)** - returns the minimum value in the group.

See: `numpy.minimum <https://numpy.org/doc/stable/reference/generated/numpy.minimum.html>`_


max
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**max(expr | column)** - returns the maximum value in the group.

See: `numpy.maximum <https://numpy.org/doc/stable/reference/generated/numpy.maximum.html>`_


avg
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**avg(expr | column)** - returns the arithmetic mean of the values in the group.

See: `numpy.mean <https://numpy.org/doc/stable/reference/generated/numpy.mean.html>`_


std
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**std(expr | column)** - returns the standard deviation of the values in the group.

See: `numpy.std <https://numpy.org/doc/stable/reference/generated/numpy.std.html>`_

