from datetime import datetime
import logging
import sys

from elasticsearch import Elasticsearch, exceptions

LOGGER = logging.getLogger(__name__)

ES_INDEX = 'geomet-data-registry-tileindex-lp'
DATE_FORMAT = '%Y-%m-%dT%H:%M:%SZ'


def get_files(layers, fh, mr):

    """
    ES search to find files names
    param layers : arrays of three layers
    param fh : forcast hour datetime
    param mr : model run
    return : files : arrays of threee file paths
    """
    # TODO: use env variable for ES connection

    es = Elasticsearch(['localhost:9200'])
    list_files = []
    weather_variables = []

    for layer in layers:
        for time_ in fh.split(','):    
            files = {}
            s_object = {
                'query': {
                    'bool': {
                        'must': {
                            'match': {'properties.layer.raw': layer}
                        },
                        'filter': [
                            {'term': {'properties.forecast_hour_datetime': time_}},
                            {'term': {'properties.reference_datetime': mr}}
                        ]
                    }
                }
            }

            try:
                res = es.search(index=ES_INDEX, body=s_object)

                try:
                    filepath =  res['hits']['hits'][0]['_source']['properties']['filepath']
                    fh = res['hits']['hits'][0]['_source']['properties']['forecast_hour_datetime']
                    mr = res['hits']['hits'][0]['_source']['properties']['reference_datetime']

                    files['filepath'] = filepath
                    files['forecast_hour'] = fh
                    files['model_run'] = mr

                    list_files.append(files)

                except IndexError as error:
                    msg = 'invalid input value: {}' .format(error)
                    LOGGER.error(msg)
                    return None, None

            except exceptions.ElasticsearchException as error:
                msg = 'ES search failed: {}' .format(error)
                LOGGER.error(msg)
                return None, None
    return list_files


if __name__ == '__main__':
    if len(sys.argv) < 4:
        print('Usage: %s <model> <forecast hours> <model run>' % sys.argv[0])
        sys.exit(1)

    model = sys.argv[1]
    fh = sys.argv[2]
    mr = sys.argv[3]

    var_list = ['TT', 'WD', 'WSPD']
    layers = []

    if model.upper() == 'HRDPS':
        model = 'HRDPS.CONTINENTAL_{}'

    for layer in var_list:
        layers.append(model.format(layer)) 

    result = get_files(layers, fh, mr)
    print(result)

