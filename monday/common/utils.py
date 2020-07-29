
# 参考 werkzeug/local.py
# 用以获取当前 协程/线程 的唯一标识
try:
    from greenlet import getcurrent as get_ident
except ImportError:
    try:
        from thread import get_ident
    except ImportError:
        from _thread import get_ident
