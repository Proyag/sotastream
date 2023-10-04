import random
from functools import partial
from typing import List
from urllib.parse import urlparse

from sotastream.augmentors import UTF8File
from sotastream.filters import FieldFilter

from . import Pipeline, pipeline


@pipeline('sample_from_fields')
class SampleFromFieldsPipeline(Pipeline):
    def __init__(self, parallel_data, **kwargs):
        super().__init__(**kwargs)

        self.url_domains = kwargs.get('url_domains', -1)
        self.sample_fields = kwargs.get('sample_fields', [1])
        self.delimiter = kwargs.get('delimiter', ',')

        self.stream = self.create_data_stream(
            parallel_data,
            processor=partial(
                ReadAndSample,
                url_domains=self.url_domains,
                sample_fields=self.sample_fields,
                delimiter=self.delimiter,
            ),
        )
        self.stream = FieldFilter(self.stream, fields=kwargs.get('keep_fields', []))

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
            type=int,
            default=-1,
            help="Trim URLs to only web domains in field {METAVAR}",
        )
        parser.add_argument(
            "--sample-fields",
            type=int,
            nargs='+',
            default=[1],
            help="Which field(s) to sample from (0-indexed)",
        )
        parser.add_argument(
            "--keep-fields",
            type=int,
            nargs='*',
            default=[],
            help="Which fields to keep (0-indexed). Keeps all fields if empty.",
        )
        parser.add_argument("--delimiter", type=str, default=',', help="Delimiter to split list on")


def SampleFromFields(stream, url_domains: int = -1, sample_fields: List[int] = [1], delimiter: str = ','):
    """
    Randomly samples a single element from a comma-separated list in the third field.
    """
    for line in stream:
        for sample_field in sample_fields:
            if sample_field >= len(line):
                continue

            line[sample_field] = random.choice(line[sample_field].split(delimiter))

            if sample_field == url_domains:
                # Extract web domain from URL
                if (
                    line[sample_field].startswith('//')
                    or line[sample_field].startswith('http://')
                    or line[sample_field].startswith('https://')
                ):
                    line[sample_field] = urlparse(line[sample_field]).netloc
                else:
                    # "urlparse recognizes a netloc only if it is properly introduced by ‘//’"
                    line[sample_field] = urlparse(f"//{line[sample_field]}").netloc
        yield line


def ReadAndSample(path: str, url_domains: int = -1, sample_fields: List[int] = [1], delimiter: str = ','):
    """
    Opens a file as a stream and passes it through SampleFromCommaSeparatedList.
    """
    stream = UTF8File(path)
    stream = SampleFromFields(
        stream, url_domains=url_domains, sample_fields=sample_fields, delimiter=delimiter
    )

    return stream
