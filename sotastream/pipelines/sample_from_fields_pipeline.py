import random
from functools import partial
from typing import List
from urllib.parse import urlparse

from sotastream.augmentors import UTF8File, Mixer
from sotastream.filters import FieldFilter

from . import Pipeline, pipeline


@pipeline('sample_from_fields')
class SampleFromFieldsPipeline(Pipeline):
    def __init__(self, parallel_data, **kwargs):
        super().__init__(**kwargs)

        stream = self.create_data_stream(
            parallel_data,
            processor=partial(
                ReadAndSample,
                url_domains=kwargs.get('url_domains', -1),
                sample_fields=kwargs.get('sample_fields', [1]),
                delimiter=kwargs.get('delimiter', ','),
                keep_last_n_tokens=kwargs.get('keep_last_n_tokens', []),
                max_n=kwargs.get('max_last_n', 512),
                doc_sep=kwargs.get('separator', '<docline>'),
            ),
        )
        self.stream = FieldFilter(stream, fields=kwargs.get('keep_fields', []))

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
            help="Trim URLs to only web domains in field (0-indexed)",
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
        parser.add_argument(
            "--keep-last-n-tokens",
            type=int,
            nargs='*',
            default=[],
            help="Which field (0-indexed) to keep only the last N tokens of, where N is sampled from [1, max_n]. -1 to disable. max_n is currently hardcoded to 512.",
        )
        parser.add_argument(
            "--max-last-n",
            type=int,
            default=512,
            help="Maximum number of tokens to keep for keep-last-n-tokens",
        )
        parser.add_argument("--delimiter", type=str, default=',', help="Delimiter to split list on")


def SampleFromFields(stream, sample_fields: List[int] = [1], delimiter: str = ','):
    """
    Randomly samples a single element from a comma-separated list in the third field.
    """
    for line in stream:
        for sample_field in sample_fields:
            if sample_field >= len(line):
                continue

            line[sample_field] = random.choice(line[sample_field].split(delimiter))

        yield line


def GetURLDomain(stream, url_field: int = -1):
    """
    Extracts the domain from a URL field.
    """
    for line in stream:
        if url_field == -1 or url_field >= len(line):
            yield line
        else:
            if (
                line[url_field].startswith('//')
                or line[url_field].startswith('http://')
                or line[url_field].startswith('https://')
            ):
                line[url_field] = urlparse(line[url_field]).netloc
            else:
                # "urlparse recognizes a netloc only if it is properly introduced by ‘//’"
                line[url_field] = urlparse(f"//{line[url_field]}").netloc
            yield line


def KeepLastNTokens(stream, fields: List[int] = [], max_n: int = 512, doc_sep: str = '<docline>'):
    """
    Sample N from [1, max_n], and keep the last N tokens of the field.
    """
    for line in stream:
        if fields == []:
            yield line
        for field in fields:
            if field >= len(line):
                continue
            else:
                n = random.randint(1, max_n)
                if doc_sep == ' ':
                    line[field] = ' '.join(line[field].split()[-n:])
                else:
                    line_split = line[field].split(doc_sep)
                    line[field] = line_split.pop().strip(' ')
                    while len(line[field].split()) < n and len(line_split) > 0:
                        line[field] = line_split.pop().strip(' ') + ' ' + doc_sep + ' ' + line[field]
        yield line


def ReadAndSample(
    path: str,
    url_domains: int = -1,
    sample_fields: List[int] = [1],
    delimiter: str = ',',
    keep_last_n_tokens: List[int] = [],
    max_n: int = 512,
    doc_sep: str = '<docline>',
):
    """
    Opens a file as a stream and passes it through augmentors.
    """
    stream = UTF8File(path)
    stream = SampleFromFields(stream, sample_fields=sample_fields, delimiter=delimiter)
    stream = GetURLDomain(stream, url_field=url_domains)
    stream = KeepLastNTokens(stream, fields=keep_last_n_tokens, max_n=max_n, doc_sep=doc_sep)

    return stream


@pipeline('sample_from_fields_mixed')
class SampleFromFieldsMixedPipeline(SampleFromFieldsPipeline):
    def __init__(self, document_data, sentence_data, **kwargs):
        super().__init__(document_data, **kwargs)

        sentence_stream = self.create_data_stream(
            sentence_data,
            processor=partial(
                SentencesWithPlaceholders,
                placeholder=kwargs.get('placeholder', ''),
                placeholder_fields=kwargs.get('placeholder_fields', []),
            ),
        )
        self.stream = Mixer([self.stream, sentence_stream], self.mix_weights)

    @classmethod
    def get_data_sources_for_argparse(cls):
        return [
            ('document_data', 'Path to document data (folder with .gz files, or compressed TSV)'),
            ('sentence_data', 'Path to sentence data (folder with .gz files, or compressed TSV)'),
        ]

    @classmethod
    def get_data_sources_default_weights(cls):
        return [0.5, 0.5]

    @classmethod
    def add_cli_args(cls, parser):
        """
        Add pipeline-specific arguments.
        """
        super().add_cli_args(parser)

        parser.add_argument(
            "--placeholder",
            type=str,
            default="",
            help="Placeholder text to fill for data with no document field",
        )
        parser.add_argument(
            "--placeholder-fields",
            type=int,
            nargs='*',
            default=[],
            help="Which fields to fill with the placeholder text (0-indexed). No placeholders if empty.",
        )


def AddPlaceholderFields(stream, placeholder: str = "", placeholder_fields: List[int] = []):
    """
    Adds placeholder fields to lines with no document field.
    """
    for line in stream:
        if len(placeholder_fields) > 0:
            for placeholder_field in sorted(placeholder_fields):
                if placeholder_field > len(line):
                    continue
                line.fields.insert(placeholder_field, placeholder)
        yield line


def SentencesWithPlaceholders(path: str, placeholder: str = "", placeholder_fields: List[int] = []):
    """
    Opens files as streams and passes it through AddPlaceholderFields.
    """
    stream = UTF8File(path)
    stream = AddPlaceholderFields(stream, placeholder=placeholder, placeholder_fields=placeholder_fields)

    return stream
