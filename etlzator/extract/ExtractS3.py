from typing import Dict, List
from pytterns.Builder import Builder
from .Extract import Extract


@Builder
class ExtractS3(Extract):

    def __init__(self) -> None:
        self.plataform = None
        self.format = None
        self.format_properties: Dict = {}
        self.repository = None
        self.layer = None
        self.schema = None
        self.entity = None
        self.url = None
        self.partition: List = []
        self.url = None
        self.reference = None
        self.columns: List = []

    def build_connection_string(self):
        self.url = f's3://{self.repository}/{self.layer}/{self.schema}/{self.entity}'

        if self.partition != []:
            for partition in self.partition:
                self.url = f'{self.url}/{partition}'

        return self.url

    def execute(self):
        url = self.build_connection_string()

        format = self.format

        self.df = self.plataform.read.format(format).load(url)

        if bool(self.columns):
            self.df = self.df.select(self.columns)

        return self.reference, self.df
