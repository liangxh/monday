import boto3
from botocore.exceptions import ClientError
from boto3.s3.transfer import S3Transfer
from monday.common.conf import conf
from monday.aws.conf import IAMUserConf


class S3Bucket(object):
    DEFAULT_CONF_PREFIX = 'aws.s3'
    LIST_REQUEST_SIZE = 100

    def __init__(self, bucket, **kwargs):
        self._kwargs = kwargs
        self.client = boto3.client('s3', **kwargs)
        self.transfer = S3Transfer(self.client)
        self.bucket = bucket

    @classmethod
    def from_conf(cls, name, prefix=None):
        _conf = conf.get(path=name, prefix=prefix or cls.DEFAULT_CONF_PREFIX)
        kwargs = dict()
        kwargs.update(_conf)

        iam_user = kwargs.pop('iam_user', None)
        if iam_user is not None:
            iam_user = IAMUserConf.from_conf(iam_user)
            kwargs['aws_access_key_id'] = iam_user.aws_access_key_id
            kwargs['aws_secret_access_key'] = iam_user.aws_secret_access_key
        return cls(**kwargs)

    def head(self, key):
        return self.client.head_object(Bucket=self.bucket, Key=key)

    def put_file_obj(self, file_obj, key, extra_args=None):
        self.client.upload_fileobj(file_obj, self.bucket, key, ExtraArgs=extra_args)

    def put(self, src_path, key, extra_args=None):
        self.transfer.upload_file(src_path, self.bucket, key, extra_args=extra_args)

    def get_file_obj(self, key, file_obj, ignore_if_not_exist=True):
        try:
            self.client.download_fileobj(self.bucket, key, file_obj)
            file_obj.seek(0)
        except ClientError as e:
            if e.response['Error']['Code'] == '404' and ignore_if_not_exist:
                return False
            else:
                raise
        return True

    def get(self, key, dest_path, ignore_if_not_exist=True):
        try:
            self.transfer.download_file(self.bucket, key, dest_path)
        except ClientError as e:
            if e.response['Error']['Code'] == '404' and ignore_if_not_exist:
                return False
            else:
                raise
        return True

    def delete(self, key):
        self.client.delete_object(Bucket=self.bucket, Key=key)

    def copy(self, src_key, dst_key, extra_args=None):
        self.client.copy(
            CopySource={'Bucket': self.bucket, 'Key': src_key},
            Bucket=self.bucket,
            Key=dst_key,
            ExtraArgs=extra_args
        )
