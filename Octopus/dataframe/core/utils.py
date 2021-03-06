import sys
import uuid
import inspect
import logging
import functools

__all__ = ['logging', 'derived_from']

PY3 = sys.version_info[0] == 3
PY2 = sys.version_info[0] == 2

if PY3:
    def _getargspec(func):
        return inspect.getfullargspec(func)

    def reraise(exc, tb=None):
        if exc.__traceback__ is not tb:
            raise exc.with_traceback(tb)
        raise exc

else:
    def _getargspec(func):
        return inspect.getargspec(func)


system_encoding = sys.getdefaultencoding()
if system_encoding == 'ascii':
    system_encoding = 'utf-8'


def _skip_doctest(line):
    # NumPy docstring contains cursor and comment only example
    stripped = line.strip()
    if stripped == '>>>' or stripped.startswith('>>> #'):
        return stripped
    elif '>>>' in stripped and '+SKIP' not in stripped:
        return line + '  # doctest: +SKIP'
    else:
        return line


def skip_doctest(doc):
    if doc is None:
        return ''
    return '\n'.join([_skip_doctest(line) for line in doc.split('\n')])


def getargspec(func):
    """Version of inspect.getargspec that works for functools.partial objects"""
    if isinstance(func, functools.partial):
        return _getargspec(func.func)
    else:
        if isinstance(func, type):
            return _getargspec(func.__init__)
        else:
            return _getargspec(func)


def extra_titles(doc):
    lines = doc.split('\n')
    titles = {i: lines[i].strip() for i in range(len(lines) - 1)
              if lines[i + 1] and all(c == '-' for c in lines[i + 1].strip())}

    seen = set()
    for i, title in sorted(titles.items()):
        if title in seen:
            new_title = 'Extra ' + title
            lines[i] = lines[i].replace(title, new_title)
            lines[i + 1] = lines[i + 1].replace('-' * len(title),
                                                '-' * len(new_title))
        else:
            seen.add(title)

    return '\n'.join(lines)


def derived_from(original_klass, version=None, ua_args=[]):
    """Decorator to attach original class's docstring to the wrapped method.

    Parameters
    ----------
    original_klass: type
        Original class which the method is derived from
    version : str
        Original package version which supports the wrapped method
    ua_args : list
        List of keywords which Dask doesn't support. Keywords existing in
        original but not in Dask will automatically be added.
    """
    def wrapper(method):
        method_name = method.__name__

        try:
            # do not use wraps here, as it hides keyword arguments displayed
            # in the doc
            original_method = getattr(original_klass, method_name)
            doc = original_method.__doc__
            if doc is None:
                doc = ''

            try:
                method_args = getargspec(method).args
                original_args = getargspec(original_method).args
                not_supported = [m for m in original_args if m not in method_args]
            except TypeError:
                not_supported = []

            if len(ua_args) > 0:
                not_supported.extend(ua_args)

            if len(not_supported) > 0:
                note = ("\n        Notes\n        -----\n"
                        "        Octopus doesn't support the following argument(s).\n\n")
                args = ''.join(['        * {0}\n'.format(a) for a in not_supported])
                doc = doc + note + args
            doc = skip_doctest(doc)
            doc = extra_titles(doc)

            method.__doc__ = doc
            return method

        except AttributeError:
            module_name = original_klass.__module__.split('.')[0]

            @functools.wraps(method)
            def wrapped(*args, **kwargs):
                msg = "Base package doesn't support '{0}'.".format(method_name)
                if version is not None:
                    msg2 = " Use {0} {1} or later to use this method."
                    msg += msg2.format(module_name, version)
                raise NotImplementedError(msg)
            return wrapped
    return wrapper


def compare_df(df1, df2, index=None):
    if index:
        df1 = df1.set_index(index)
        df2 = df2.set_index(index)
    d1, d2 = {}, {}
    for col1 in list(df1.columns):
        d1[col1] = df1[col1].tolist()
    for col2 in list(df2.columns):
        d2[col2] = df2[col2].tolist()
    for k in d1.keys():
        if d1[k] != d2[k]:
            return False
    return True


def exe_time(func):
    return func
    import time

    def new_func(*args, **args2):
        t0 = time.time()
        print("@%s, {%s} start" % (time.strftime("%X", time.localtime()), func.__name__))
        back = func(*args, **args2)
        print("@%s, {%s} end" % (time.strftime("%X", time.localtime()), func.__name__))
        print("@%.3fs taken for {%s}" % (time.time() - t0, func.__name__))
        return back
    return new_func


class IdGenerator:
    __id = str(uuid.uuid4())[-1:-8:-1]

    @classmethod
    def next_id(cls):
        # res = IdGenerator.__id
        IdGenerator.__id = str(uuid.uuid4())[-1:-8:-1]
        return IdGenerator.__id

    @classmethod
    def get_id(cls):
        res = IdGenerator.__id
        return res
