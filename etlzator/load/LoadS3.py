from typing import Dict
from pytterns.Builder import Builder
from .Load import Load


@Builder
class LoadS3(Load):

    def __init__(self) -> None:
        self.format = None
        self.format_properties: Dict = {}
        self.repository = None
        self.layer = None
        self.schema = None
        self.entity = None
        self.url = None
        self.reference = None
        self.mode = None
        self.df_writer = None

    def build_connection_string(self):
        self.url = f's3://{self.repository}/{self.layer}/{self.schema}/{self.entity}'

        return self.url

    def execute(self):
        url = self.build_connection_string()

        format = self.format

        writer_format = self.df_writer.write.format(format)

        if bool(self.format_properties):
            writer_format = writer_format.options(**self.format_properties)

        if self.mode != None:
            writer_format = writer_format.mode(self.mode)

        self.df = writer_format.save(url)

        return self.reference, self.df
