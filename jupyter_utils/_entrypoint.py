import jupyter_utils.aws_tools

import logging
import tempfile

import argparse
import os, sys

import inspect

_logger = None
def create_logger():
    global _logger
    if _logger is None:
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)

        handler = logging.StreamHandler()
        logger.addHandler(handler)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        _logger = logger

    return _logger

def create_parser():
    parser = argparse.ArgumentParser(description='Parse individual submissions from file')
    parser.add_argument("--aws_access_key_id", help="File path for config", required=True)
    parser.add_argument("--aws_secret_access_key", help="Data subset identifier", required=True)
    parser.add_argument("--aws_default_region", help="Data subset identifier", required=True)
    parser.add_argument("--job_id", help="Influx descriptor", required=True)
    parser.add_argument("--bucket_name", help="Influx descriptor", required=True)
    parser.add_argument("--kms_key", required=False, default=None)
    return parser

def main():

    logger = create_logger()
    logger.info("Starting processing...")

    args = create_parser().parse_args(sys.argv[1:])
    os.environ.update({'AWS_ACCESS_KEY_ID': args.aws_access_key_id,
                       'AWS_SECRET_ACCESS_KEY': args.aws_secret_access_key,
                       'AWS_DEFAULT_REGION': args.aws_default_region})

    bucket = args.bucket_name
    job_id = args.job_id

    s3bucket = jupyter_utils.aws_tools.get_bucket_client(bucket)

    kms = jupyter_utils.aws_tools.KmsReaderWriter(logger, kms_key=args.kms_key)
    method = kms.read("./func.pkg")

    sig = inspect.signature(method)
    result = None
    if len(sig.parameters.keys()) > 1:
        # need to get these from s3.
        with tempfile.TemporaryDirectory() as td:
            a = list(s3bucket.download_files(td, logger, job_id, ["pickle.args"]))[0]
            arg_list = kms.read(a)

        result = method(logger, **arg_list)
    elif(len(sig.parameters.keys() == 1)):
        result = method(logger)
    else:
        result = method()

    logger.info("Finished processing.")

    ntf = tempfile.NamedTemporaryFile(delete=False)
    kms.write(ntf.name, result)
    s3bucket.write_file(ntf.name, job_id, "result.pickle")
    logger.info("Written result to {}".format(job_id))

if __name__ == "__main__":
    main()