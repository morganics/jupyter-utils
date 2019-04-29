import tempfile, subprocess, os
from sqlalchemy import MetaData, Table, create_engine

def subsample(engine, from_table, to_table, percentage=10):
    with engine.connect() as conn:
        conn.execute("CREATE TABLE {} AS (select * from {} TABLESAMPLE SYSTEM({}))".format(to_table, from_table, percentage))

def to_postgres(conn_str, table_name, df, logger, df_conn_str=None):

    if df_conn_str is None:
        df_conn_str = conn_str

    csv_file = tempfile.NamedTemporaryFile(delete=False)
    config_file = tempfile.NamedTemporaryFile(delete=False)

    pgload_cmd = """LOAD CSV
             FROM '/d/{}' 
             INTO {}
             TARGET TABLE "{}"
             WITH truncate,
                  skip header = 1,
                  fields optionally enclosed by '"',
                  fields escaped by double-quote,
                  fields terminated by ','
              SET client_encoding to 'latin1',
                  work_mem to '12MB',
                  standard_conforming_strings to 'on';""".format(os.path.basename(csv_file.name), conn_str, table_name)

    with open(config_file.name, 'w') as fh:
        fh.write(pgload_cmd)

    dir_name = os.path.dirname(csv_file.name)

    def run_docker():
        logger.info("Loading up pgloader in docker...")
        command = "docker run --rm --name pgloader -v {}:/d dimitri/pgloader:latest pgloader -v /d/{}".format(dir_name,
                                                                                                              os.path.basename(
                                                                                                                  config_file.name))

        def run(process, cmd):
            while True:
                line = process.stdout.readline().rstrip()
                if not line:
                    break
                yield line

        process = subprocess.Popen(command.split(" "), stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        for line in run(process, command):
            logger.info(line)

        logger.info(process.stderr.readlines())

    engine = create_engine(df_conn_str)

    logger.info("Testing docker connection...")
    run_docker()

    logger.info("Creating schema in DB...")
    df.head(0).to_sql(table_name, engine, index=False, if_exists='replace')
    logger.info("Writing dataframe to CSV...")

    with open(csv_file.name, 'w') as fh:
        df.to_csv(fh, index=False)

    run_docker()

    try:
        os.unlink(csv_file.name)
    except BaseException as e:
        logger.warn(e)

    try:
        os.unlink(config_file.name)
    except BaseException as e:
        logger.warn(e)