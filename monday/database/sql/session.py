from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from monday.common.utils import get_ident


class SessionManager(object):
    DEFAULT_CONF_PREFIX = 'database.sql.session'
    DEFAULT_CONF_NAME = 'default'

    # 保存已创建的session
    # {
    #   "{url}": {
    #       "session": Session实例
    #       "conf": 该 Session 在 create_engine 时用的配置
    #   }
    # }
    # 这里意味着同一个 SessionManager 内相同的 url 只能对应一套配置
    __created_sessions__ = dict()

    # 保存各线程下用到的 Session
    # {
    #   "{ID}": [ Session实例 ]
    # }
    __local_sessions__ = dict()

    @classmethod
    def _register(cls, session):
        """
        记录当前 线程/协程 下用到此 Session
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
            # 该 url 对应的 session 第一次创建
            engine = create_engine(url, **conf or dict())
            session = scoped_session(sessionmaker(bind=engine, autoflush=False))
            cls.__created_sessions__[url] = {
                'session': session,
                'conf': conf
            }
        else:
            # [design]
            # 相同的url只允许用相同的配置, 从维护的角度来说同一个url不应该配在两个地方
            created = cls.__created_sessions__[url]
            if not conf == created['conf']:
                raise Exception(
                    'repeated target ({}) with different config: {} != {}'.format(url, created['conf'], conf))
            session = created['session']
        cls._register(session)
        return session

    @classmethod
    def from_conf(cls, name=None, prefix=None):
        """
        从 monday 的默认位置读取配置
        """
        # [design]
        # 这里没有用 name=DEFAULT_CONF_NAME, prefix=DEFAULT_CONF_PREFIX
        # 因为后面 monday.aws.redshift.session.SessionManager 要继承这个模块
        # 但是覆盖类成员变量 DEFAULT_CONF_PREFIX 的话函数的预设值不会相应的被覆盖
        from monday.common.conf import conf

        sess_conf = conf.get(path=name or cls.DEFAULT_CONF_NAME, prefix=prefix or cls.DEFAULT_CONF_PREFIX)
        return cls.get(url=sess_conf['url'], conf=sess_conf.get('conf'))

    @classmethod
    def remove(cls):
        """
        释放当前 线程/协程 下该用到的 Session 的连接
        """
        # [why]
        # scoped_session 在底层会引用当前 线程/协程 的实例, 不会影响其他 线程/协程
        ident = get_ident()
        for session in cls.__local_sessions__.pop(ident, list()):
            session.remove()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.remove()


session_manager = SessionManager()
