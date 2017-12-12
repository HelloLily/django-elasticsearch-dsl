from unittest import TestCase
from mock import patch

from django.conf import settings

from django_elasticsearch_dsl.indices import Index
from django_elasticsearch_dsl.registries import DocumentRegistry
from tests import fixtures


class IndexTestCase(fixtures.WithFixturesMixin, TestCase):

    def test_documents_add_to_register(self):
        registry = DocumentRegistry()
        with patch('django_elasticsearch_dsl.indices.registry', new=registry):
            index = Index('test')
            index.doc_type(fixtures.DocA1)
            docs = list(registry.get_documents())
            self.assertEqual(len(docs), 1)
            self.assertIs(docs[0], fixtures.DocA1)

            index.doc_type(fixtures.DocA2)
            docs = registry.get_documents()
            self.assertEqual(docs, {fixtures.DocA1, fixtures.DocA2})

    def test__str__(self):
        index = Index('test')
        self.assertEqual(index.__str__(), 'test')

    def test__init__(self):
        settings.ELASTICSEARCH_DSL_INDEX_SETTINGS = {
            'number_of_replicas': 0,
            'number_of_shards': 2,
        }

        index = Index('test')
        self.assertEqual(index._settings, {
            'number_of_replicas': 0,
            'number_of_shards': 2,
        })

        settings.ELASTICSEARCH_DSL_INDEX_SETTINGS = {}
