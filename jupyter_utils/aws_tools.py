import boto3
import os
import boto3.s3.transfer
import base64, cloudpickle


def _get_s3_keys(bucket, s3_client):
    """Get a list of keys in an S3 bucket."""
    keys = []

    resp = s3_client.list_objects_v2(Bucket=bucket)
    for obj in resp['Contents']:
        keys.append(obj['Key'])
    return keys


def get_s3_keys(bucket):
    """Get a list of keys in an S3 bucket."""
    return _get_s3_keys(bucket, boto3.client('s3'))


def get_files_from_s3(bucket, data_path, configuration, logger):
    # bucket = "indalyz-ost-config"
    s3_client = boto3.client('s3')
    for key in _get_s3_keys(bucket, s3_client):
        if not (key.endswith(".csv") or key.endswith(".yml")):
            continue

        if not key.startswith(configuration + "/"):
            continue

        logger.info("Downloading {}".format(os.path.join(data_path, key)))
        s3_client.download_file(bucket, key, os.path.join(data_path, key))


def get_files(bucket, data_path, logger, suffix="", file_type=""):
    s3_client = boto3.client('s3')
    for key in _get_s3_keys(bucket, s3_client):
        if not (key.endswith(file_type)):
            continue

        if not key.startswith(suffix + "/"):
            continue

        path = os.path.join(data_path, key)
        logger.info("Downloading {}".format(path))
        s3_client.download_file(bucket, key, path)
        yield path


def get_file_from_s3(bucket, key, target_path):
    s3_client = boto3.client('s3')
    head, tail = os.path.split(key)
    s3_client.download_file(bucket, key, os.path.join(target_path, tail))
    return os.path.join(target_path, tail)


def write_file_to_s3(bucket, source, target):
    s3_client = boto3.client('s3')
    s3_transfer = boto3.s3.transfer.S3Transfer(s3_client)
    s3_transfer.upload_file(source, bucket, target)


def copy_file(bucket, source, target):
    s3_client = boto3.client('s3')
    s3_client.copy_object(Bucket=bucket, CopySource="{}/{}".format(bucket, source), Key=target)
    return target


class S3BucketClient:

    def __init__(self, s3_client, bucket):
        self._s3_client = s3_client
        self._bucket = bucket

    def get_keys(self):
        """Get a list of keys in an S3 bucket."""
        keys = []

        resp = self._s3_client.list_objects_v2(Bucket=self._bucket)
        for obj in resp['Contents']:
            keys.append(obj['Key'])
        return keys

    def write_string(self, contents, target_folder, target_filename):
        import tempfile
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp.write(contents)
            tmp.seek(0)
            self.write_file(tmp.name, target_folder, target_filename)
        os.unlink(tmp.name)

    def write_file(self, source, target_folder, target_filename=None):
        if target_filename is None:
            target_filename = os.path.basename(source)
        target_path = target_folder + "/" + target_filename
        s3_transfer = boto3.s3.transfer.S3Transfer(self._s3_client)
        s3_transfer.upload_file(source, self._bucket, target_path)

    def download_files(self, data_path, logger, suffix="", file_type=list()):
        for key in self.get_keys():
            if not key.startswith(suffix + "/"):
                continue

            if not any(key.lower().endswith(ft.lower()) for ft in file_type):
                continue

            path = os.path.join(data_path, os.path.basename(key))
            logger.info("Downloading {}".format(path))
            self._s3_client.download_file(self._bucket, key, path)
            yield path


class KmsReaderWriter:

    def __init__(self, logger, kms_key=None):
        self._kms_key = kms_key
        self._logger = logger

    def _create_kms_client(self):
        return boto3.client('kms')

    def write(self, filename:str, func):
        encoded_func = self.write_to_string(func)
        with open(filename, "wb") as fh:
            fh.write(encoded_func)

    def write_to_string(self, func):
        self._logger.info("(KMS) Serializing args...")
        serialized_func = cloudpickle.dumps(func)

        if self._kms_key is not None:
            kms_client = self._create_kms_client()
            encrypted_func = kms_client.encrypt(KeyId=self._kms_key, Plaintext=serialized_func)
            encoded_func = base64.b64encode(encrypted_func[u'CiphertextBlob'])
        else:
            encoded_func = base64.b64encode(serialized_func)

        return encoded_func

    def read(self, filename):
        self._logger.info("(KMS) Deserializing args...")
        with open(filename, 'rb') as fh:
            encoded = fh.read()

        byte_obj = base64.b64decode(encoded)

        if self._kms_key is not None:
            kms_client = self._create_kms_client()
            decrypted_func = kms_client.decrypt(CiphertextBlob=byte_obj)
            func = cloudpickle.loads(decrypted_func[u'Plaintext'])
        else:
            func = cloudpickle.loads(byte_obj)

        return func


def get_bucket_client(bucket):
    s3_client = boto3.client('s3')
    return S3BucketClient(s3_client, bucket)