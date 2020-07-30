import commandr
from urllib.parse import unquote, urlparse
from monday.database.sql.session import SessionManager


@commandr.command('cmd')
def show_command_line(name):
    """
    读取 database.sql.session 下 {name} 的url, 返回终端登录用的命令
    """
    from monday.common.conf import conf
    sess_conf = conf.get(path=name, prefix=SessionManager.DEFAULT_CONF_PREFIX)
    url = sess_conf['url']
    url = urlparse(url)
    username = unquote(url.username) if url.username else None
    password = unquote(url.password) if url.password else None
    hostname = unquote(url.hostname) if url.hostname else None
    port = url.port
    db = None
    if url.path:
        db = unquote(url.path).replace('/', '') if url.path else None
        if db == '':
            db = None
    shell = 'mysql -u{}'.format(username)
    if hostname:
        shell += ' -h{}'.format(hostname)
    if port:
        shell += ' -P{}'.format(port)
    if password:
        shell += ' -p'
    if db:
        shell += ' {}'.format(db)
    print(shell)
    if password:
        print(password)


if __name__ == '__main__':
    commandr.Run()
