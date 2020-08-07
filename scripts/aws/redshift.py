import commandr
from monday.aws.redshift.redshift import Redshift


@commandr.command
def show_tables(session_name):
    with Redshift.from_conf(session_name) as redshift:
        table_names = redshift.show_tables()
        for table_name in table_names:
            print(table_name)


@commandr.command
def show_columns(table_name, session_name):
    stat = '''
        SELECT column_name, data_type, is_nullable, column_default 
        FROM information_schema.columns 
        WHERE table_name='{table_name}';
    '''.format(table_name=table_name)
    with Redshift.from_conf(session_name) as redshift:
        res = redshift.execute(stat).fetchall()
        for row in res:
            print(row)


@commandr.command('count')
def count_rows(table_name, session_name):
    with Redshift.from_conf(session_name) as redshift:
        count = redshift.count(table_name)
        print('Count: {}'.format(count))


@commandr.command
def drop_table(table_name, session_name, dryrun=True):
    if dryrun:
        print('(dryrun) trying to drop table {} for sess: {}'.format(table_name, session_name))
    else:
        with Redshift.from_conf(name=session_name) as redshift:
            stat = redshift.stat_drop_table(table_name=table_name)
            redshift.execute(stat)
            redshift.commit()


@commandr.command
def sample(table_name, session_name, limit=10):
    statement = '''
        SELECT * FROM {table_name} LIMIT {limit}
    '''.format(table_name=table_name, limit=limit)
    with Redshift.from_conf(session_name) as redshift:
        res = redshift.execute(statement).fetchall()
        for row in res:
            print(row)


if __name__ == '__main__':
    commandr.Run()
