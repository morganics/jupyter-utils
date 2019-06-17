import pandas as pd
import os
from contextlib import contextmanager
import uuid

@contextmanager
def all(df, cols=10000, rows=1000):
    with pd.option_context("display.max_rows", rows, "display.max_columns", cols):
        yield df


@contextmanager
def all_cols(df):
    with all(df, rows=50):
        yield df


@contextmanager
def all_rows(df):
    with all(df, cols=50):
        yield df


def missing(df):
    return df.isnull().sum().sort_index() / len(df)


def facets_overview(**kwargs):
    ## assumes that facets has been cloned in to './facets/' using git clone https://github.com/pair-code/facets.git
    ## expects table=df as args

    if not os.path.exists('./facets'):
        raise FileNotFoundError("The facets folder is not present. This script assumes that facets has been cloned using"
                                "git clone https://github.com/pair-code/facets.git")

    import sys
    sys.path.insert(0, './facets/facets_overview/python/')
    # Create the feature stats for the datasets and stringify it.
    import base64
    from generic_feature_statistics_generator import GenericFeatureStatisticsGenerator
    gfsg = GenericFeatureStatisticsGenerator()
    args = [{'name': key, 'table': value} for key,value in kwargs.items()]
    proto = gfsg.ProtoFromDataFrames(args)
    protostr = base64.b64encode(proto.SerializeToString()).decode("utf-8")

    # Display the facets overview visualization for this data
    from IPython.core.display import display, HTML
    HTML_TEMPLATE = """<link rel="import" href="https://raw.githubusercontent.com/PAIR-code/facets/master/facets-dist/facets-jupyter.html" >
            <facets-overview id="elem{uid}"></facets-overview>
            <script>
              document.querySelector("#elem{uid}").protoInput = "{protostr}";
            </script>"""
    html = HTML_TEMPLATE.format(protostr=protostr, uid=str(uuid.uuid4()))
    return display(HTML(html))

    # HTML_TEMPLATE = """<script type="text/javascript" src="data:text/javascript;base64,{jupyter}" />
    #             <facets-overview id="elem"></facets-overview>
    #             <script>
    #               document.querySelector("#elem").protoInput = "{protostr}";
    #             </script>"""
    #
    # #   print(os.path.join(__file__, "./facets/facets-dist/facets-jupyter.html"))
    # html = HTML_TEMPLATE.format(protostr=protostr,
    #                             jupyter=base64.b64encode(
    #                                 pathlib.Path(os.path.join("./facets/facets-dist/facets-jupyter.html"))
    #                                     .read_bytes()).decode('UTF-8'))

def facets_dive(df):
    # Display the Dive visualization for the training data.
    from IPython.core.display import display, HTML
    jsonstr = df.to_json(orient='records')
    HTML_TEMPLATE = """<link rel="import" href="https://raw.githubusercontent.com/PAIR-code/facets/master/facets-dist/facets-jupyter.html">
            <facets-dive id="elem{uid}" height="600"></facets-dive>
            <script>
              var data = {jsonstr};
              document.querySelector("#elem{uid}").data = data;
            </script>"""
    html = HTML_TEMPLATE.format(jsonstr=jsonstr, uid=str(uuid.uuid4()))
    display(HTML(html))