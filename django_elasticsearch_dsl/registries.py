from collections import defaultdict
from itertools import chain

from django.utils.six import itervalues, iterkeys, iteritems

from django_elasticsearch_dsl.apps import DEDConfig


class DocumentRegistry(object):
    """
    Registry of models classes to a set of Document classes.
    """
    def __init__(self):
        self._indices = defaultdict(set)
        self._models = defaultdict(set)
        self._related_models = defaultdict(set)

    def register(self, index, doc_class):
        """Register the model with the registry"""
        self._models[doc_class._doc_type.model].add(doc_class)

        for related in doc_class._doc_type.related_models:
            self._related_models[related].add(doc_class._doc_type.model)

        for idx, docs in iteritems(self._indices):
            if index._name == idx._name:
                docs.add(doc_class)
                return

        self._indices[index].add(doc_class)

    def update(self, instance, action='index', from_signal=False, **kwargs):
        """
        Update all the Elasticsearch documents attached to this model.
        """
        connection_actions = defaultdict(set)

        if instance.__class__ in self._models:
            for doc in self._models[instance.__class__]:
                if not (from_signal and doc._doc_type.ignore_signals):
                    doc_inst = doc()
                    connection_actions[doc_inst.connection].add(
                        doc_inst._get_actions([instance],
                                              action=action, **kwargs))

        if instance.__class__ in self._related_models:
            for model in self._related_models[instance.__class__]:
                for doc in self._models[model]:
                    if not (from_signal and doc._doc_type.ignore_signals):
                        doc_inst = doc()
                        related = doc_inst.get_instances_from_related(
                            instance
                        )
                        if related:
                            connection_actions[doc_inst.connection].add(
                                doc_inst._get_actions(
                                    [related],  action='index', **kwargs
                                )
                            )

        if 'refresh' not in kwargs and DEDConfig.auto_refresh_enabled():
            kwargs['refresh'] = True

        for connection, actions in iteritems(connection_actions):
            bulk(client=connection, actions=list(chain(*actions)), **kwargs)

    def delete(self, instance, **kwargs):
        """
        Delete all the elasticsearch documents attached to this model.
        """
        self.update(instance, action="delete", **kwargs)

    def get_documents(self, models=None):
        """
        Get all documents in the registry or the documents for a list of models
        """
        if models is not None:
            return set(chain(*(self._models[model] for model in models
                               if model in self._models)))
        return set(chain(*itervalues(self._indices)))

    def get_models(self):
        """
        Get all models in the registry
        """
        return set(iterkeys(self._models))

    def get_indices(self, models=None):
        """
        Get all indices in the registry or the indices for a list of models
        """
        if models is not None:
            return set(
                indice for indice, docs in iteritems(self._indices)
                for doc in docs if doc._doc_type.model in models
            )

        return set(iterkeys(self._indices))


registry = DocumentRegistry()
