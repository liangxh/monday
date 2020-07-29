import json
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from monday.common.utils import get_ident


class SessionManager(object):
    SCHEMA = 'mysql+pymysql'
    DEFAULT_PORT = 3306
    DEFAULT_CONF_PREFIX = 'database.sql.session'
    DEFAULT_CONF_NAME = 'default'

    __created_sessions__ = dict()
    __local_sessions__ = dict()

    @classmethod
    def _register(cls, session):
        """
        record the sessions used in this thread
        """
        ident = get_ident()
        if ident not in cls.__local_sessions__:
            cls.__local_sessions__[ident] = set()
        cls.__local_sessions__[ident].add(session)

    @classmethod
    def get(cls, url, conf=None):
        """
        :param url: {schema}://{username}:{password}@{host}:{port}/{database}
        :param conf: dict or None, used in create_engine(...)
        :return:
        """
        if url not in cls.__created_sessions__:
            engine = create_engine(url, **conf or dict())
            session = scoped_session(sessionmaker(bind=engine, autoflush=False))
            cls.__created_sessions__[url] = {
                'session': session,
                'conf': conf
            }
        else:
            created = cls.__created_sessions__[url]
            if not conf == created['conf']:
                raise Exception(
                    'repeated target ({}) with different config: {} != {}'.format(url, created['conf'], conf))
            session = created['session']
        cls._register(session)
        return session

    @classmethod
    def get_by_conf(cls, name=None, path_prefix=None):
        """
        get a session, using the configuration read by monday's conf module
        """
        from monday.common.conf import conf

        name = name if name is not None else cls.DEFAULT_CONF_NAME
        path_prefix = path_prefix if path_prefix is not None else cls.DEFAULT_CONF_PREFIX
        path = '{}.{}'.format(path_prefix, name)
        sess_conf = conf.get(path)
        url = '{}://{}:{}@{}:{}/{}'.format(
            cls.SCHEMA,
            sess_conf['user'], sess_conf['pw'],
            sess_conf['host'], sess_conf.get('port', cls.DEFAULT_PORT), sess_conf['db']
        )
        return cls.get(url=url, conf=sess_conf.get('conf', dict()))

    @classmethod
    def remove(cls):
        """
        release the sessions used in the thread
        """
        ident = get_ident()
        for session in cls.__local_sessions__.pop(ident, list()):
            session.remove()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.remove()


session_manager = SessionManager()
