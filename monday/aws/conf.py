from monday.common.conf import conf


class IAMUserConf:
    # [design]
    # 对于给一个 AWS IAM User 配置了多个 AWS 使用权限时
    # 程序应该在统一的位置配置所有用到的 User (Programmatic Access Key)
    # 其他 AWS 资源的使用模块应该从这个统一个地方选用对应的 User

    DEFAULT_CONF_PREFIX = 'aws.iam.user'

    def __init__(self, aws_access_key_id=None, aws_secret_access_key=None):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key

    @classmethod
    def from_conf(cls, name, prefix=None):
        _conf = conf.get(path=name, prefix=prefix or cls.DEFAULT_CONF_PREFIX)
        return cls(
            aws_access_key_id=_conf['aws_access_key_id'],
            aws_secret_access_key=_conf['aws_secret_access_key']
        )


class IAMRoleConf:
    DEFAULT_CONF_PREFIX = 'aws.iam.role'

    def __init__(self, arn=None):
        self.arn = arn

    @classmethod
    def from_conf(cls, name, prefix=None):
        _conf = conf.get(path=name, prefix=prefix or cls.DEFAULT_CONF_PREFIX)
        return cls(
            arn=_conf['arn']
        )
