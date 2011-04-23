# -*- Mode: Python; test-case-name: paisley.test.test_pjson -*-
# vi:si:et:sw=4:sts=4:ts=4

# Copyright (c) 2011
# See LICENSE for details.

"""
Paisley JSON compatibility code.

json is the stdlib JSON library.
It has an unfortunate bug in 2.7: http://bugs.python.org/issue10038
where the C-based implementation returns str instead of unicode for text.
"""

STRICT = True


def set_strict(strict=True):
    """
    Set strictness of the loads function.
    Can be called after importing to change strictness level.

    Recommended to use only at startup.
    """
    global loads
    loads = _get_loads(strict)
    global STRICT
    STRICT = strict


def _get_loads(strict=STRICT):
    if not strict:
        try:
            from simplejson import loads
        except ImportError:
            from json import loads
        return loads

    from json import decoder
    res = decoder.c_scanstring('"str"', 1)
    if type(res[0]) is unicode:
        from json import loads
        return loads

    import json as _myjson
    from json import scanner

    class MyJSONDecoder(_myjson.JSONDecoder):

        def __init__(self, *args, **kwargs):
            _myjson.JSONDecoder.__init__(self, *args, **kwargs)

            # reset scanner to python-based one using python scanstring
            self.parse_string = decoder.py_scanstring
            self.scan_once = scanner.py_make_scanner(self)

    def loads(s, *args, **kwargs):
        if 'cls' not in kwargs:
            kwargs['cls'] = MyJSONDecoder
        return _myjson.loads(s, *args, **kwargs)

    return loads

def _get_dumps(strict=STRICT):
    if not strict:
        try:
            from simplejson import dumps
        except ImportError:
            from json import dumps
        return dumps

    from json import dumps
    return dumps

dumps = _get_dumps()
loads = _get_loads()
