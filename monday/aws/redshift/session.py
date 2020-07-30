from monday.database.sql.session import SessionManager as _SessionManager


class SessionManager(_SessionManager):
    DEFAULT_CONF_PREFIX = 'aws.redshift'


session_manager = SessionManager()
