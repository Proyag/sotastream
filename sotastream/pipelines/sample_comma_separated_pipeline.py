import random

from sotastream.augmentors import DataSource, UTF8File

from . import Pipeline, pipeline


@pipeline('sample_comma_separated')
class SampleCommaSeparatedFieldPipeline(Pipeline):
    def __init__(self, parallel_data, **kwargs):
        super().__init__(**kwargs)

        self.stream = self.create_data_stream(parallel_data, processor=ReadAndSample)

    @classmethod
    def get_data_sources_for_argparse(cls):
        return [('parallel_data', 'Path to parallel data (folder with .gz files, or compressed TSV)')]

    @classmethod
    def get_data_sources_default_weights(cls):
        return [1.0]

def SampleFromCommaSeparatedList(stream):
    """
    Randomly samples a single element from a comma-separated list in the third field.
    """
    for line in stream:
        line[2] = random.choice(line[2].split(','))
        yield line

def ReadAndSample(path: str):
    """
    Opens a file as a stream and passes it through SampleFromCommaSeparatedList.
    """
    stream = UTF8File(path)
    stream = SampleFromCommaSeparatedList(stream)

    return stream
