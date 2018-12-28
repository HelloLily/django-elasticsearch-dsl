from unittest import TestCase
from mock import patch, Mock

from django.db import models
from django.utils.translation import ugettext_lazy as _
from elasticsearch_dsl import GeoPoint

from django_elasticsearch_dsl import fields
from django_elasticsearch_dsl.documents import DocType
from django_elasticsearch_dsl.exceptions import (ModelFieldNotMappedError,
                                                 RedeclaredFieldError)
from tests import ES_MAJOR_VERSION


class Car(models.Model):
    name = models.CharField(max_length=255)
    price = models.FloatField()
    not_indexed = models.TextField()
    manufacturer = models.ForeignKey(
        'Manufacturer', null=True, on_delete=models.SET_NULL
    )

    class Meta:
        app_label = 'car'

    def type(self):
        return "break"


class Manufacturer(models.Model):
    name = models.CharField(max_length=255)

    class Meta:
        app_label = 'car'


class Country(models.Model):
    name = models.CharField(max_length=255)

    class Meta:
        app_label = 'car'


class CarDocument(DocType):
    color = fields.TextField()
    type = fields.StringField()

    def prepare_color(self, instance):
        return "blue"

    class Meta:
        fields = ['name', 'price']
        model = Car
        index = 'car_index'
        related_models = [Manufacturer]
        doc_type = 'car_document'


class DocTypeTestCase(TestCase):

    def setUp(self):
        self.buffer_patcher = patch(
            'django_elasticsearch_dsl.documents.ActionBuffer')
        self.action_buffer = self.buffer_patcher.start()
        self.action_buffer().add_doc_actions = Mock()
        self.action_buffer().execute = Mock()

    def tearDown(self):
        self.buffer_patcher.stop()

    def test_model_class_added(self):
        self.assertEqual(CarDocument._doc_type.model, Car)

    def test_ignore_signal_default(self):
        self.assertFalse(CarDocument._doc_type.ignore_signals)

    def test_auto_refresh_default(self):
        self.assertTrue(CarDocument._doc_type.auto_refresh)

    def test_ignore_signal_added(self):
        class CarDocument2(DocType):
            class Meta:
                model = Car
                ignore_signals = True

        self.assertTrue(CarDocument2._doc_type.ignore_signals)

    def test_auto_refresh_added(self):
        class CarDocument2(DocType):
            class Meta:
                model = Car
                auto_refresh = False

        self.assertFalse(CarDocument2._doc_type.auto_refresh)

    def test_queryset_pagination_added(self):
        class CarDocument2(DocType):
            class Meta:
                model = Car
                queryset_pagination = 120

        self.assertIsNone(CarDocument._doc_type.queryset_pagination)
        self.assertEqual(CarDocument2._doc_type.queryset_pagination, 120)

    def test_fields_populated(self):
        mapping = CarDocument._doc_type.mapping
        self.assertEqual(
            set(mapping.properties.properties.to_dict().keys()),
            set(['color', 'name', 'price', 'type'])
        )

    def test_related_models_added(self):
        class CarDocument2(DocType):
            manufacturer = fields.ObjectField(properties={
                'name': fields.StringField(),
                'country': fields.ObjectField(properties={
                    'name': fields.StringField()
                }, related_model=Country)
            })

            class Meta:
                model = Car
                related_models = [Manufacturer]

        related_models = CarDocument2._doc_type.related_models
        self.assertEqual({
            Manufacturer: 'get_instances_from_related',
            Country: 'get_instances_from_manufacturer_country',
        }, related_models)

    def test_duplicate_field_names_not_allowed(self):
        with self.assertRaises(RedeclaredFieldError):
            class CarDocument(DocType):
                color = fields.StringField()
                name = fields.StringField()

                class Meta:
                    fields = ['name']
                    model = Car

    def test_to_field(self):
        doc = DocType()
        nameField = doc.to_field('name', Car._meta.get_field('name'))
        self.assertIsInstance(nameField, fields.TextField)
        self.assertEqual(nameField._path, ['name'])

    def test_to_field_with_unknown_field(self):
        doc = DocType()
        with self.assertRaises(ModelFieldNotMappedError):
            doc.to_field('manufacturer', Car._meta.get_field('manufacturer'))

    def test_mapping(self):
        text_type = 'string' if ES_MAJOR_VERSION == 2 else 'text'

        self.assertEqual(
            CarDocument._doc_type.mapping.to_dict(), {
                'car_document': {
                    'properties': {
                        'name': {
                            'type': text_type
                        },
                        'color': {
                            'type': text_type
                        },
                        'type': {
                            'type': text_type
                        },
                        'price': {
                            'type': 'double'
                        }
                    }
                }
            }
        )

    def test_get_queryset(self):
        qs = CarDocument().get_queryset()
        self.assertIsInstance(qs, models.QuerySet)
        self.assertEqual(qs.model, Car)

    def test_prepare(self):
        car = Car(name="Type 57", price=5400000.0, not_indexed="not_indexex")
        doc = CarDocument()
        prepared_data = doc.prepare(car)
        self.assertEqual(
            prepared_data, {
                'color': doc.prepare_color(None),
                'type': car.type(),
                'name': car.name,
                'price': car.price
            }
        )

    def test_prepare_ignore_dsl_base_field(self):
        class CarDocumentDSlBaseField(DocType):
            position = GeoPoint()

            class Meta:
                model = Car
                index = 'car_index'
                fields = ['name', 'price']

        car = Car(name="Type 57", price=5400000.0, not_indexed="not_indexex")
        doc = CarDocumentDSlBaseField()
        prepared_data = doc.prepare(car)
        self.assertEqual(
            prepared_data, {
                'name': car.name,
                'price': car.price
            }
        )

    def test_model_instance_update(self):
        doc = CarDocument()
        car = Car(name="Type 57", price=5400000.0,
                  not_indexed="not_indexex", pk=51)

        doc.update(car)

        self.action_buffer().add_doc_actions.assert_called_with(
            doc, car, 'index',
        )
        self.action_buffer().execute.assert_called_with(
            doc.connection, refresh=True,
        )

    def test_model_instance_delete(self):
        doc = CarDocument()
        car = Car(name="Type 57", price=5400000.0,
                  not_indexed="not_indexex", pk=51)
        doc.delete(car)

        self.action_buffer().add_doc_actions.assert_called_with(
            doc, car, 'delete',
        )
        self.action_buffer().execute.assert_called_with(
            doc.connection, refresh=True,
        )

    def test_model_instance_iterable_update(self):
        doc = CarDocument()
        car = Car(name="Type 57", price=5400000.0,
                  not_indexed="not_indexex", pk=51)
        car2 = Car(name=_("Type 42"), price=50000.0,
                   not_indexed="not_indexex", pk=31)

        doc.update([car, car2], action='update')

        self.action_buffer().add_doc_actions.assert_called_with(
            doc, [car, car2], 'update',
        )
        self.action_buffer().execute.assert_called_with(
            doc.connection, refresh=True,
        )

    def test_model_instance_update_no_refresh(self):
        doc = CarDocument()
        doc._doc_type.auto_refresh = False
        car = Car()
        doc.update(car)

        self.action_buffer().add_doc_actions.assert_called_with(
            doc, car, 'index',
        )
        self.action_buffer().execute.assert_called_with(
            doc.connection
        )

    def test_model_instance_iterable_update_with_pagination(self):
        doc = CarDocument()
        car1 = Car()
        car2 = Car()
        car3 = Car()

        doc.update([car1, car2, car3])

        self.action_buffer().add_doc_actions.assert_called_with(
            doc, [car1, car2, car3], 'index',
        )
        self.action_buffer().execute.assert_called_with(
            doc.connection, refresh=True
        )
