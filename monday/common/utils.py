try:
    from greenlet import getcurrent as get_ident
except ImportError:
    from threading import get_ident

