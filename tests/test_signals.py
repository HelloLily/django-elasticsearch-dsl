from unittest import TestCase

from django.conf import settings
from django.db import models, connections
from mock import patch

from django_elasticsearch_dsl.signals import BaseSignalProcessor


class MyModel(models.Model):

    class Meta:
        app_label = 'test'


class BaseSignalProcessorTestCase(TestCase):
    def setUp(self):
        self.processor = BaseSignalProcessor(connections)

        self.bulk_patcher = patch('django_elasticsearch_dsl.signals.registry')
        self.registry_mock = self.bulk_patcher.start()

    def tearDown(self):
        self.bulk_patcher.stop()

    def test_handle_save(self):
        instance = MyModel()
        self.processor.handle_save(None, instance=instance)
        self.registry_mock.update.assert_called_with(instance, action='index', from_signal=True)

    def test_handle_save_no_autosync(self):
        settings.ELASTICSEARCH_DSL_AUTOSYNC = False

        self.processor.handle_save(None, instance=MyModel())
        self.registry_mock.update.assert_not_called()

        settings.ELASTICSEARCH_DSL_AUTOSYNC = True

    def test_handle_delete(self):
        instance = MyModel()
        self.processor.handle_delete(None, instance=instance)
        self.registry_mock.update.assert_called_with(instance, action='delete', from_signal=True)

    def test_handle_delete_no_autosync(self):
        settings.ELASTICSEARCH_DSL_AUTOSYNC = False

        self.processor.handle_delete(None, instance=MyModel())
        self.registry_mock.update.assert_not_called()

        settings.ELASTICSEARCH_DSL_AUTOSYNC = True
