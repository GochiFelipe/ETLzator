from pytterns.Builder import Builder
from etlzator.extract import Extract
from etlzator.load import Load
from etlzator.transform import Transform


@Builder
class ETL:
    def __init__(self, debug=False) -> None:
        self.debug = debug
        self.plataform = None
        self.repository = None

    def extract(self, extractor: Extract):
        if extractor.plataform == None:
            extractor.__setattr__('plataform', self.plataform)

        if extractor.repository == None:
            extractor.__setattr__('repository', self.repository)

        setattr(self.__class__, *extractor.execute())

        if self.debug:
            data = self.__getattribute__(extractor.reference)
            print(data.count())
            print(data.show())

        return self

    def transform(self, transformer: Transform):
        builder = transformer()

        model = builder.model()

        transformer_objects = vars(model)

        for transformer_object in transformer_objects:
            if not transformer_object.startswith('_'):
                try:
                    attribute = self.__getattribute__(transformer_object)
                    
                    transformer = transformer.__getattribute__(transformer_object)(attribute)

                except Exception as ex:
                    pass

        transformer = transformer.build
            
        setattr(self.__class__, *transformer.execute())

        return self

    def load(self, loader: Load):
        if loader.repository == None:
            loader.__setattr__('repository', self.repository)

        loader.__setattr__('df_writer', self.__getattribute__(loader.reference))
        setattr(self.__class__, *loader.execute())

        return self
