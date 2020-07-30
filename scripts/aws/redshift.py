import commandr
from monday.aws.redshift.session import session_manager


@commandr.command
def show_tables(session_name):
    with session_manager:
        sess = session_manager.from_conf(session_name)
        res = sess.execute('''
            SELECT DISTINCT tablename FROM pg_table_def WHERE schemaname='public';
        ''').fetchall()
        for row in res:
            print(row)


@commandr.command
def show_columns(table_name, session_name):
    with session_manager:
        sess = session_manager.from_conf(session_name)
        res = sess.execute('''
            SELECT column_name, data_type, is_nullable, column_default 
            FROM information_schema.columns 
            WHERE table_name='{table_name}';
        '''.format(
            table_name=table_name)
        ).fetchall()
        for row in res:
            print(row)


if __name__ == '__main__':
    commandr.Run()
