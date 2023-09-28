import random
from functools import partial
from urllib.parse import urlparse

from sotastream.augmentors import DataSource, UTF8File

from . import Pipeline, pipeline


@pipeline('sample_comma_separated')
class SampleCommaSeparatedFieldPipeline(Pipeline):
    def __init__(self, parallel_data, **kwargs):
        super().__init__(**kwargs)

        self.url_domains = kwargs.get('url_domains', False)
        self.sample_field = kwargs.get('sample_field', 2)
        self.stream = self.create_data_stream(parallel_data, processor=partial(ReadAndSample, url_domains=self.url_domains, sample_field=self.sample_field))

    @classmethod
    def get_data_sources_for_argparse(cls):
        return [('parallel_data', 'Path to parallel data (folder with .gz files, or compressed TSV)')]

    @classmethod
    def get_data_sources_default_weights(cls):
        return [1.0]
    
    @classmethod
    def add_cli_args(cls, parser):
        """
        Add pipeline-specific arguments.
        """
        super().add_cli_args(parser)

        parser.add_argument(
            "--url-domains",
            action='store_true',
            help="Trim URLs to only web domains",
        )
        parser.add_argument(
            "--sample-field",
            type=int,
            default=2,
            help="Which field to sample from (0-indexed)",
        )

def SampleFromCommaSeparatedList(stream, url_domains: bool = False, sample_field: int = 2):
    """
    Randomly samples a single element from a comma-separated list in the third field.
    """
    for line in stream:
        line[sample_field] = random.choice(line[sample_field].split(','))
        if url_domains:
            # Extract web domain from URL
            if line[sample_field].startswith('//') or line[sample_field].startswith('http://') or line[sample_field].startswith('https://'):
                line[sample_field] = urlparse(line[sample_field]).netloc
            else:
                # "urlparse recognizes a netloc only if it is properly introduced by ‘//’"
                line[sample_field] = urlparse(f"//{line[sample_field]}").netloc
        yield line

def ReadAndSample(path: str, url_domains: bool = False, sample_field: int = 2):
    """
    Opens a file as a stream and passes it through SampleFromCommaSeparatedList.
    """
    stream = UTF8File(path)
    stream = SampleFromCommaSeparatedList(stream, url_domains=url_domains, sample_field=sample_field)

    return stream
