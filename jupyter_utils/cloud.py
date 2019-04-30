import cloudpickle, pip, tempfile, os
#https://boto3.readthedocs.io/en/latest/reference/services/batch.html
import boto3
import os
import numpy as np
import docker
import shutil
import base64
import uuid
import functools
import threading
import logging
import time
import jupyter_utils.aws_tools as aws_tools
import inspect

class CloudTaskRunner:

    def __init__(self, func, repo_name, job_queue_arn, job_definition_name,
                 bucket, logger, kms_key=None):
        self._repo_name = repo_name
        self._logger = logger
        self._queue = AwsJobQueue(job_queue_arn)
        self._job = None
        self._bucket = bucket
        self._job_definition_name = job_definition_name
        self._unique_id = str(uuid.uuid4())
        self._args = None
        self._func = func
        self._sig = inspect.signature(func)
        self._kms_key = kms_key
        self._kms = aws_tools.KmsArgumentSerializer(self._kms_key, self._logger)

    def get_logs(self):
        return self._job.logs()

    def get_result(self):
        s3bucket = aws_tools.get_bucket_client(self._bucket)

        with tempfile.TemporaryDirectory() as td:
            files = list(s3bucket.download_files(td, self._logger, self._unique_id, [".pickle"]))
            assert len(files) > 0, "Expecting a result file stored in S3"
            file_path = files[0]
            obj = self._kms.deserialize_from_file(file_path)

        return obj

    def get_status(self):
        return self._job.get_job_status()

    def with_args(self, **kwargs):
        for key in kwargs.keys():
            if key not in self._sig.parameters.keys():
                raise ValueError("Unexpected variable {} in arg list".format(key))

        for key in [key for key in self._sig.parameters.keys()
                    if self._sig.parameters[key].default == inspect.Parameter.empty]:
            if key not in kwargs and not (key == "log" or key == "logger" or key == "bucket"):
                raise ValueError("Expecting {} to be supplied in arg list".format(key))

        self._args = kwargs

    def run(self):
        if self._args is not None:
            s3bucket = aws_tools.get_bucket_client(self._bucket)
            arg_str = self._kms.serialize(self._args)
            s3bucket.write_string(arg_str, self._unique_id, "pickle.args")

        jd = AwsJobDefinition(self._job_definition_name)
        revision = jd.get_latest_revision()
        arn = jd.get_arn(revision)

        self._job = self._queue.submit_job(arn, self._unique_id,
                                           bucket_name=self._bucket,
                                           aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                                           aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                                           aws_default_region=os.getenv("AWS_DEFAULT_REGION"),
                                           job_id=self._unique_id,
                                           kms_key=self._kms_key
                                           )
        return self._job

    def run_and_wait(self):
        job = AwsJobPoller(self._logger).wait(self)
        status = job.get_job_status()
        if job.is_succeeded(status):
            return self, self.get_result()
        else:
            return self, self.get_logs()



class CloudJob:

    FUNC_NAME = "func.pkg"

    def __init__(self, ecr_repo, job_queue, s3_bucket, logger, kms_key=None):
        self._ecr_repo = ecr_repo
        self._logger = logger
        self._job_queue = job_queue
        #self._job_definition_name = job_definition_name
        self._s3_bucket = s3_bucket
        self._func = None
        self._kms = aws_tools.KmsArgumentSerializer(kms_key, self._logger)
        self._kms_key = kms_key

    def _get_dependencies(self, dependencies):
        import pkg_resources

        dists = [d for d in pkg_resources.working_set]
        #print(dists)
        installed_packages_list = sorted(["%s==%s" % (i.key, i.version)
                                          for i in dists])
        #print(installed_packages_list)
        if dependencies is not None:
            dependencies.extend(['cloudpickle', 'botocore', 'boto3'])
            installed_packages_list = [dep for dep in installed_packages_list if dep.split("==")[0] in dependencies]
            installed_packages_list.append("aws-encryption-sdk==1.3.8")
        #print(installed_packages_list)

        return installed_packages_list

    def data(self):
        return aws_tools.CloudDataSet(aws_tools.get_bucket_client(self._s3_bucket), self._kms, self._logger)

    def _login_to_ecr(self, docker_client):
        self._logger.info("Logging in to ECR...")
        ecr_client = boto3.client('ecr')
        token = ecr_client.get_authorization_token()
        username, password = base64.b64decode(token['authorizationData'][0]['authorizationToken']).decode().split(':')
        registry = token['authorizationData'][0]['proxyEndpoint']
        self._logger.info("Logging in to Docker client ({}) using auth token...".format(registry))
        self._logger.info(docker_client.login(username, password, registry=registry, reauth=True))
        return docker_client, {'username': username, 'password': password}

    def _write_docker_file(self, dir, deps):
        self._logger.info("Writing docker file and requirements.txt...")
        DockerFileWriter(deps, self._logger).write(dir)

    def build(self, func, dependencies=None, docker_image_name=None):

        td = tempfile.TemporaryDirectory()
        curr = os.path.dirname(os.path.abspath(__file__))

        deps = self._get_dependencies(dependencies)
        self._write_docker_file(td.name, deps)

        shutil.copy(os.path.join(curr, "_entrypoint.py"), os.path.join(td.name, "__main__.py"))
        os.mkdir(os.path.join(td.name, "jupyter_utils"))
        for f in [f for f in os.listdir(curr) if os.path.isfile(os.path.join(curr, f))]:
            shutil.copy(os.path.join(curr, f), os.path.join(td.name, os.path.join(td.name, "jupyter_utils", os.path.basename(f))))

        self._kms.serialize_to_file(func, os.path.join(td.name, "func.pkg"))

        docker_client = docker.DockerClient(base_url="tcp://127.0.0.1:2375")
        self._logger.info("Connected to local Docker daemon")
        self._logger.info("Building cloud job image ({})...".format(td.name))
        cwd = os.getcwd()
        os.chdir(td.name)
        gen = docker_client.api.build(path=".", tag=self._ecr_repo, decode=True)
        for chunk in gen:
            if 'stream' in chunk:
                for line in chunk['stream'].splitlines():
                    self._logger.info(line)

        os.chdir(cwd)
        #td.cleanup()

        self._func = func

        return self

    def publish(self):
        docker_client = docker.DockerClient(base_url="tcp://127.0.0.1:2375")
        docker_client, auth_config = self._login_to_ecr(docker_client)
        self._logger.info("Pushing image to repo {}...".format(self._ecr_repo))
        last_msg = None
        for line in docker_client.images.push(self._ecr_repo,
                                              stream=True, decode=True, auth_config=auth_config):

            if 'status' in line and last_msg != line['status']:
                self._logger.info(line['status'])
                last_msg = line['status']

        self._logger.info("Finished submission.")
        return self

    def get_task(self, job_definition_name, **kwargs):
        task = CloudTaskRunner(self._func, self._ecr_repo, self._job_queue, job_definition_name,
                               self._s3_bucket, self._logger, kms_key=self._kms_key)
        task.with_args(**kwargs)
        return task


class DockerFileWriter:

    def __init__(self, deps, logger):
        self._deps = deps
        self._base_image = "frolvlad/alpine-miniconda3"
        self._logger = logger

    def write(self, td):

        with open(os.path.join(td, "Dockerfile"), "w") as fh:
            dfi = """
FROM {}
# RUN apk add build-base gcc abuild binutils cmake
RUN mkdir /install
WORKDIR /install
COPY requirements.txt /requirements.txt
RUN pip install -r /requirements.txt --no-cache-dir

COPY ./ /app/
WORKDIR /app/
ENTRYPOINT ["python", "."]
                """.format(self._base_image)
            fh.write(dfi)

        with open(os.path.join(td, "requirements.txt"), "w") as fh:
            for dep in self._deps:
                self._logger.info(dep)
                fh.write("{}\r\n".format(dep))




def _create_client(service_name):
    #if os.getenv("AWS_ACCESS_KEY_ID") is None or os.getenv("AWS_SECRET_ACCESS_KEY") is None \
    #        or os.getenv("AWS_DEFAULT_REGION") is None:
    #    raise ValueError(
    #        "Could not locate AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY or AWS_DEFAULT_REGION in env variables.")

    return boto3.client(service_name)


class PeriodicTimer(object):
    def __init__(self, interval, callback):
        self.interval = interval

        @functools.wraps(callback)
        def wrapper(*args, **kwargs):
            result = callback(*args, **kwargs)
            if result:
                self.thread = threading.Timer(self.interval,
                                              self.callback)
                self.thread.start()

        self.callback = wrapper

    def start(self):
        self.thread = threading.Timer(self.interval, self.callback)
        self.thread.start()

    def cancel(self):
        self.thread.cancel()


class AwsJobPoller:

    def __init__(self, logger: logging.Logger):
        self._timer = None
        self._logger = logger

    def wait(self, task):
        return self.wait_all([task])[0]

    def wait_all(self, tasks):
        finished_jobs = []
        pending_jobs = []
        for job in tasks:
            pending_jobs.append(job.run())

        last_messages = {}
        self._logger.info("Checking pending/running jobs ({})".format(len(pending_jobs)))
        k = 0
        while True:
            for job in pending_jobs:
                try:
                    status = job.get_job_status()
                    if job.aws_job_id not in last_messages or last_messages[job.aws_job_id] != status or k > 10:
                        last_messages.update({job.aws_job_id: status})
                        self._logger.info("{}: {}".format(job.aws_job_id, status))
                        k = 0
                    else:
                        k += 1

                    if job.is_complete(status):
                        finished_jobs.append(job)

                        #if job.is_failed(status):
                        #    self._logger.info("Logs for {};".format(job.aws_job_id))
                        #    for log_line in list(job.logs()):
                        #        self._logger.info(log_line)

                except BaseException as e:
                    self._logger.warning(e)

            for job in finished_jobs:
                if job in pending_jobs:
                    pending_jobs.remove(job)

            if len(pending_jobs) == 0:
                break

            time.sleep(3)

        return finished_jobs


class AwsLogs:

    def __init__(self):
        self._client = self._create_client()

    def _create_client(self):
        return _create_client('logs')

    def get_all_logs(self, log_stream_name, tail=False):
        params = {'logGroupName':'/aws/batch/job',
                  'logStreamName': log_stream_name,
                  'startFromHead': not tail,
                  'startTime': 0}
        last_token = None
        while last_token is None:
            last_token = params['nextToken'] if 'nextToken' in params else None
            response = self._client.get_log_events(**params)

            yield response['events']

            params['nextToken'] = response['nextForwardToken']


class AwsJob:
    def __init__(self, client, aws_job_id):
        self._client = client
        self.aws_job_id = aws_job_id
        self._logs = AwsLogs()

    def logs(self):
        info = self.describe()
        status = self.get_job_status(info)
        if self.is_complete(status) or self.is_running(status):
            log_stream_name = info['container']['logStreamName']
            for records in self._logs.get_all_logs(log_stream_name):
                # an arry of dicts
                for record in records:
                    yield "{}: {}".format(record['timestamp'], record['message'])


    def get_job_status(self, response=None):
        if response is None:
            info = self.describe()
        else:
            info = response

        return info['status']

    def is_complete(self, status):
        return status == AwsJobQueue.SUCCEEDED or status == AwsJobQueue.FAILED

    def is_pending(self, status):
        return status == AwsJobQueue.PENDING or status == AwsJobQueue.SUBMITTED or status == AwsJobQueue.STARTING\
                or status == AwsJobQueue.RUNNABLE

    def is_succeeded(self, status):
        return status == AwsJobQueue.SUCCEEDED

    def is_running(self, status):
        return status == AwsJobQueue.RUNNING

    def is_failed(self, status):
        return status == AwsJobQueue.FAILED

    def cancel(self):
        self._client.terminate_job(jobId=self.aws_job_id, reason='User Requested')

    def describe(self):
        jobs = self._client.describe_jobs(jobs=[self.aws_job_id])['jobs']
        if len(jobs) == 0:
            return None
        else:
            return jobs[0]

    def exists(self):
        return self.describe() is not None


class AwsJobDefinition:
    def __init__(self, definition_name):
        self._client = self._create_client()
        #self._job_definition_arn = job_definition_arn
        self._job_definition_name = definition_name
        #self._job_definition_version = definition_version
        self._logs = AwsLogs()

    def get_arn(self, revision):
        for definition in self._get_job_definitions():
            if definition['revision'] == revision:
                return definition['jobDefinitionArn']

        return None

    def get_latest_revision(self):
        versions = []
        for definition in self._get_job_definitions():
            versions.append(definition['revision'])

        if len(versions) == 0:
            return None

        return max(versions)

    def _create_client(self):
        return _create_client('batch')

    def _get_job_definitions(self):
        return self._client.describe_job_definitions(jobDefinitionName=self._job_definition_name)['jobDefinitions']

    def exists(self):
        return len(self._get_job_definitions()) > 0



class AwsJobQueue:

    SUBMITTED = "SUBMITTED"
    PENDING = "PENDING"
    RUNNABLE = "RUNNABLE"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"

    All = "All"

    def __init__(self, job_queue_arn):
        self._client = self._create_client()
        self._job_queue_arn = job_queue_arn

    def _create_client(self):
        return _create_client('batch')

    def get_job(self, aws_job_id):
        job = AwsJob(self._create_client(), aws_job_id)
        if job.exists():
            return job
        return None

    def list_jobs(self, status):
        def _list_job(st):
            next_token = ''
            while next_token is not None:
                result = self._client.list_jobs(jobQueue=self._job_queue_arn, jobStatus=st)

                for job in result['jobSummaryList']:
                    yield job

                next_token = None
                if 'nextToken' in result:
                    next_token = result['nextToken']

        if status == self.All:
            for status_ in [self.PENDING, self.SUBMITTED, self.RUNNING, self.RUNNABLE,
                           self.SUCCEEDED, self.FAILED]:
                yield from _list_job(status_)
        else:
            yield from _list_job(status)


    def get_pending_job_count(self):
        return self.get_job_count(lambda x: x == self.PENDING or x == self.RUNNABLE or x == self.RUNNING
                                  or x == self.STARTING or x == self.SUBMITTED)

    def get_failed_job_count(self):
        return self.get_job_count(lambda x: x == self.FAILED)

    def get_succeeded_job_count(self):
        return self.get_job_count(lambda x: x == self.SUCCEEDED)

    def get_job_count(self, condition):
        count = 0
        for job in self.list_jobs(self.All):
            if condition(job['status']):
                count += 1

        return count

    def submit_job(self, job_definition_arn, job_identifier, **kwargs):

        response = self._client.submit_job(
            jobName=job_identifier,
            jobQueue=self._job_queue_arn,
            jobDefinition=job_definition_arn,
            parameters=kwargs,
            retryStrategy={
                'attempts': 1
            }
        )

        id = response['jobId']
        return AwsJob(self._client, id)
