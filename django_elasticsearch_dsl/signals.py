# encoding: utf-8
"""
A convenient way to attach django-elasticsearch-dsl to Django's signals and
cause things to index.
"""

from __future__ import absolute_import

from django.db import models

from django_elasticsearch_dsl.actions import ActionBuffer
from django_elasticsearch_dsl.apps import DEDConfig
from .registries import registry


class BaseSignalProcessor(object):
    """Base signal processor.

    By default, does nothing with signals but provides underlying
    functionality.
    """

    def __init__(self, connections):
        self.connections = connections
        self.setup()

    def setup(self):
        """Set up.

        A hook for setting up anything necessary for
        ``handle_save/handle_delete`` to be executed.

        Default behavior is to do nothing (``pass``).
        """
        # Do nothing.

    def teardown(self):
        """Tear-down.

        A hook for tearing down anything necessary for
        ``handle_save/handle_delete`` to no longer be executed.

        Default behavior is to do nothing (``pass``).
        """
        # Do nothing.

    def handle_save(self, sender, **kwargs):
        """Handle save.

        Given an individual model instance, update the object in the index.
        """
        self._handle_internal(kwargs['instance'], action='index')

    def handle_delete(self, sender, **kwargs):
        """Handle delete.

        Given an individual model instance, delete the object from index.
        """
        self._handle_internal(kwargs['instance'], action='delete')

    def _handle_internal(self, instance, action='index'):
        if not DEDConfig.autosync_enabled():
            return

        actions = ActionBuffer(registry=registry)

        for doc in registry.get_documents(instance.__class__):
            if not doc._doc_type.ignore_signals:
                actions.add_doc_actions(
                    doc, instance, action=action
                )

        for related in registry.get_related_models(
            instance.__class__
        ):
            for doc in registry.get_documents(related):
                if not doc._doc_type.ignore_signals:
                    rel_instance = doc().get_instances_from_related(
                        instance
                    )

                    if rel_instance:
                        actions.add_doc_actions(
                            doc, rel_instance, action='index'
                        )

        return actions.execute()


class RealTimeSignalProcessor(BaseSignalProcessor):
    """Real-time signal processor.

    Allows for observing when saves/deletes fire and automatically updates the
    search engine appropriately.
    """

    def setup(self):
        # Listen to all model saves.
        models.signals.post_save.connect(self.handle_save)
        models.signals.post_delete.connect(self.handle_delete)

    def teardown(self):
        # Listen to all model saves.
        models.signals.post_save.disconnect(self.handle_save)
        models.signals.post_delete.disconnect(self.handle_delete)
