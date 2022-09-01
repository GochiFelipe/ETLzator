import string
from random import choice
from unittest import TestCase
from unittest.main import main

from etlzator import ExtractS3


class ExtractS3Tests(TestCase):
    def __get_random_string(self):
        letters = string.ascii_lowercase
        return ''.join(choice(letters) for i in range(14))

    def setUp(self):
        self.extract = ExtractS3()

    def test_plataform(self):
        plataform = self.__get_random_string()
        self.extract.plataform(plataform)
        self.assertEqual(self.extract.instance.plataform, plataform)

    def test_format(self):
        format = self.__get_random_string()
        self.extract.format(format)
        self.assertEqual(self.extract.instance.format, format)

    def test_repository(self):
        repository = self.__get_random_string()
        self.extract.repository(repository)
        self.assertEqual(self.extract.instance.repository, repository)

    def test_layer(self):
        layer = self.__get_random_string()
        self.extract.layer(layer)
        self.assertEqual(self.extract.instance.layer, layer)

    def test_schema(self):
        schema = self.__get_random_string()
        self.extract.schema(schema)
        self.assertEqual(self.extract.instance.schema, schema)

    def test_entity(self):
        entity = self.__get_random_string()
        self.extract.entity(entity)
        self.assertEqual(self.extract.instance.entity, entity)

    def test_url(self):
        url = self.__get_random_string()
        self.extract.url(url)
        self.assertEqual(self.extract.instance.url, url)

    def test_reference(self):
        reference = self.__get_random_string()
        self.extract.reference(reference)
        self.assertEqual(self.extract.instance.reference, reference)

    def test_build(self):
        instance = self.extract.build

        self.assertEqual(type(instance), type(ExtractS3().instance))

    def test_build_connection_string_without_partition(self):
        instance = self.extract.build

        instance.build_connection_string()

        repository = instance.repository
        layer = instance.layer
        schema = instance.schema
        entity = instance.entity

        url_expected = f's3a://{repository}/{layer}/{schema}/{entity}'

        self.assertEqual(instance.url, url_expected)


if __name__ == '__main__':
    main()
