from collections import defaultdict
from itertools import chain

from django.db.models.base import ModelBase
from django.utils.six import iteritems, iterkeys, itervalues

from django_elasticsearch_dsl.actions import ActionBuffer


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

    def update(self, instance, action='index', **kwargs):
        """
        Update all the Elasticsearch documents attached to this model.
        """
        actions = ActionBuffer(registry=self)
        actions.add_model_actions(instance, action)
        return actions.execute(**kwargs)

    def delete(self, instance, **kwargs):
        """
        Delete all the elasticsearch documents attached to this model.
        """
        self.update(instance, action="delete", **kwargs)

    def get_documents(self, models=None):
        """
        Get all documents in the registry or the documents for a list of models
        """
        if models is None:
            return set(chain(*itervalues(self._indices)))
        elif isinstance(models, ModelBase):
            return set(self._models.get(models, {}))
        else:
            return set(chain(*(self._models[model] for model in models
                               if model in self._models)))

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

    def get_related_models(self, model):
        """
        Get a set of related models.

        Returns a set of models which should be updated when model is updated.
        """
        return self._related_models.get(model, {})


registry = DocumentRegistry()
