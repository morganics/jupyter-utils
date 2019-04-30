import boto3
import os
import boto3.s3.transfer
import base64, cloudpickle
import aws_encryption_sdk
import io
import uuid
import datetime
import tempfile
import logging

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


class CloudDataSet:

    def __init__(self, s3_client:'S3BucketClient', kms_client:'KmsArgumentSerializer', logger:logging.Logger):
        self._s3_client = s3_client
        self._kms_client = kms_client
        self._logger = logger
        self._cache = {}

    def save(self, **kwargs):
        unique_id = "data-" + str(uuid.uuid4()) + "-" + datetime.datetime.utcnow().strftime("%d%m%Y-%H%M%S")
        obj_bytes = self._kms_client.serialize(kwargs)
        self._s3_client.write_string(obj_bytes, unique_id, "df.pickle")
        return unique_id

    def read(self, unique_id):
        self._logger.info("Downloading data cache under {}".format(unique_id))
        tf = tempfile.TemporaryDirectory()
        file = list(self._s3_client.download_files(tf.name, self._logger, unique_id))[0]
        self._cache.update({unique_id: self._kms_client.deserialize_from_file(file)})
        tf.cleanup()

    def get(self, unique_id, label):
        if unique_id not in self._cache:
            self.read(unique_id)

        return self._cache[unique_id][label]


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

            if len(file_type) > 0 and not any(key.lower().endswith(ft.lower()) for ft in file_type):
                continue

            path = os.path.join(data_path, os.path.basename(key))
            logger.info("Downloading {}".format(path))
            self._s3_client.download_file(self._bucket, key, path)
            yield path

class KmsArgumentSerializer:

    def __init__(self, kms_key_id, logger):
        self._kms_key_provider = aws_encryption_sdk.KMSMasterKeyProvider(key_ids=[
            kms_key_id
        ])
        self._logger = logger

    def _stream(self, source, destination, mode):
        with aws_encryption_sdk.stream(
                        mode=mode,
                        source=source,
                        key_provider=self._kms_key_provider
                ) as encryptor:
                    for chunk in encryptor:
                        destination.write(chunk)

        destination.seek(0)
        #return destination

    def serialize(self, raw):
        self._logger.info("Serializing using KMS...")
        serialized_func = cloudpickle.dumps(raw)
        with io.BytesIO() as destination:
            self._stream(io.BytesIO(serialized_func), destination, 'e')
            d = base64.b64encode(destination.read())

        #self._logger.info(d)
        return d

    def deserialize(self, encoded):
        self._logger.info("Deserializing using KMS...")
        contents = base64.b64decode(encoded)
        with io.BytesIO() as destination:
            self._stream(io.BytesIO(contents), destination, 'd')
            d = destination.read()
            x = cloudpickle.loads(d)
            #self._logger.info(str(x))
            return x

    def serialize_to_file(self, func, filename):
        with open(filename, 'wb') as fh:
            response = self.serialize(func)
            fh.write(response)

    def deserialize_from_file(self, filename):
        with open(filename, 'rb') as fh:
            return self.deserialize(fh.read())



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

    def read_from_string(self, s):
        byte_obj = base64.b64decode(s)

        if self._kms_key is not None:
            kms_client = self._create_kms_client()
            decrypted_func = kms_client.decrypt(CiphertextBlob=byte_obj)
            func = cloudpickle.loads(decrypted_func[u'Plaintext'])
        else:
            func = cloudpickle.loads(byte_obj)

        return func

    def read(self, filename):
        self._logger.info("(KMS) Deserializing args...")
        with open(filename, 'rb') as fh:
            encoded = fh.read()

        return self.read_from_string(encoded)


def get_bucket_client(bucket):
    s3_client = boto3.client('s3')
    return S3BucketClient(s3_client, bucket)