from mock import Mock, patch
from unittest import TestCase

from django_elasticsearch_dsl import DocType
from django_elasticsearch_dsl.registries import DocumentRegistry

import fixtures


class DocumentRegistryTestCase(fixtures.WithFixturesMixin, TestCase):
    def setUp(self):
        super(DocumentRegistryTestCase, self).setUp()

        self.registry.register(self.index_1, fixtures.DocA1)
        self.registry.register(self.index_1, fixtures.DocA2)
        self.registry.register(self.index_2, fixtures.DocB1)
        self.registry.register(self.index_1, fixtures.DocC1)
        self.registry.register(self.index_1, fixtures.DocD1)

        self.bulk_patcher = patch('django_elasticsearch_dsl.registries.bulk')
        self.bulk_mock = self.bulk_patcher.start()

    def tearDown(self):
        self.bulk_patcher.stop()

    def test_empty_registry(self):
        registry = DocumentRegistry()
        self.assertEqual(registry._indices, {})
        self.assertEqual(registry._models, {})

    def test_register(self):
        self.assertEqual(
            self.registry._models[fixtures.ModelA],
            {fixtures.DocA1, fixtures.DocA2}
        )
        self.assertEqual(
            self.registry._models[fixtures.ModelB],
            {fixtures.DocB1}
        )

        self.assertEqual(
            self.registry._indices[self.index_1],
            {fixtures.DocA1, fixtures.DocA2, fixtures.DocC1, fixtures.DocD1}
        )
        self.assertEqual(
            self.registry._indices[self.index_2],
            {fixtures.DocB1}
        )

    def test_get_models(self):
        self.assertEqual(
            self.registry.get_models(),
            {fixtures.ModelA, fixtures.ModelB, fixtures.ModelC, fixtures.ModelD}
        )

    def test_get_documents(self):
        self.assertEqual(
            self.registry.get_documents(),
            {fixtures.DocA1, fixtures.DocA2, fixtures.DocB1,
             fixtures.DocC1, fixtures.DocD1}
        )

    def test_get_documents_by_model(self):
        self.assertEqual(
            self.registry.get_documents([fixtures.ModelA]),
            {fixtures.DocA1, fixtures.DocA2}
        )

    def test_get_documents_by_unregister_model(self):
        ModelC = Mock()
        self.assertFalse(self.registry.get_documents([ModelC]))

    def test_get_indices(self):
        self.assertEqual(
            self.registry.get_indices(),
            {self.index_1, self.index_2}
        )

    def test_get_indices_by_model(self):
        self.assertEqual(
            self.registry.get_indices([fixtures.ModelA]),
            {self.index_1}
        )

    def test_get_indices_by_unregister_model(self):
        ModelC = Mock()
        self.assertFalse(self.registry.get_indices([ModelC]))

    def test_update_instance(self):
        class DocA2(DocType):
            class Meta:
                model = fixtures.ModelA

        instance = fixtures.ModelA()

        registry = DocumentRegistry()
        registry.register(self.index_1, DocA2)
        registry.update(instance)

        self.bulk_mock.assert_called_once_with(actions=[
            {
                '_type': 'doc_a2',
                '_id': None,
                '_source': {},
                '_op_type': 'index',
                '_index': 'None',
            }
        ], client=DocA2().connection)

    def test_update_related_instances(self):
        class DocD(DocType):
            get_instances_from_related = Mock(return_value=fixtures.ModelD())

            class Meta:
                model = fixtures.ModelD
                related_models = [fixtures.ModelE]

        instance = fixtures.ModelE()

        registry = DocumentRegistry()
        registry.register(self.index_1, DocD)
        registry.update(instance)

        self.bulk_mock.assert_called_once_with(actions=[
            {
                '_type': 'doc_d',
                '_id': None,
                '_source': {},
                '_op_type': 'index',
                '_index': 'None',
            }
        ], client=DocD().connection)

    def test_update_related_isntances_not_defined(self):
        class DocD(DocType):
            get_instances_from_related = Mock(return_value=None)

            class Meta:
                model = fixtures.ModelD
                related_models = [fixtures.ModelE]

        instance = fixtures.ModelE()

        registry = DocumentRegistry()
        registry.register(self.index_1, DocD)
        registry.update(instance)

        self.bulk_mock.assert_not_called()

    def test_delete_instance(self):
        class DocA2(DocType):
            class Meta:
                model = fixtures.ModelA

        instance = fixtures.ModelA()

        registry = DocumentRegistry()
        registry.register(self.index_1, DocA2)
        registry.delete(instance)

        self.bulk_mock.assert_called_once_with(actions=[
            {
                '_type': 'doc_a2',
                '_id': None,
                '_source': None,
                '_op_type': 'delete',
                '_index': 'None',
            }
        ], client=DocA2().connection)

    def test_delete_related_instances(self):
        self.registry.delete(fixtures.ModelE())

        self.bulk_mock.assert_called_once_with(actions=[
            {
                '_type': 'doc_d1',
                '_id': None,
                '_source': {},
                '_op_type': 'index',
                '_index': 'None',
            }
        ], client=fixtures.DocD1().connection)
