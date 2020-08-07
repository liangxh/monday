from monday.common.conf import conf
from monday.aws.conf import IAMRoleConf
from monday.aws.redshift.session import session_manager


class Redshift(object):
    def __init__(self, sess, iam_role):
        self.sess = sess
        self.iam_role = iam_role

    @classmethod
    def from_conf(cls, name, prefix=None):
        prefix = prefix if prefix is not None else session_manager.DEFAULT_CONF_PREFIX

        _conf = conf.get(name, prefix)
        sess = session_manager.from_conf(name=name, prefix=prefix)
        iam_role = IAMRoleConf.from_conf(name=_conf['iam_role'])
        return cls(sess=sess, iam_role=iam_role)

    def execute(self, stat):
        return self.sess.execute(stat)

    def commit(self):
        return self.sess.commit()

    @classmethod
    def stat_drop_table(cls, table_name):
        stat = '''
           DROP TABLE {table_name}
        '''.format(table_name=table_name)
        return stat

    def count(self, table_name):
        stat = '''
           SELECT count(*) FROM {table_name}
       '''.format(table_name=table_name)
        resp = self.sess.execute(stat).fetchall()
        _count = resp[0][0]
        return _count

    def show_tables(self):
        state = '''
            SELECT 
                DISTINCT tablename 
            FROM pg_table_def 
            WHERE schemaname='public';
        '''
        resp = self.sess.execute(state)
        table_names = list()
        for row in resp:
            table_names.append(row[0])
        return table_names

    def stat_copy_csv(self, table_name, column_list, bucket, s3_key):
        stat = '''
            copy {table_name} ({column_list}) 
            from 's3://{bucket}/{s3_key}' 
            iam_role '{iam_role}' 
            format as csv delimiter ','
        '''.format(
            table_name=table_name,
            column_list=','.join(column_list),
            bucket=bucket, s3_key=s3_key,
            iam_role=self.iam_role
        )
        return stat

    def __enter__(self):
        _ = session_manager.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return session_manager.__exit__(exc_type, exc_val, exc_tb)
