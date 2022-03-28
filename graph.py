from io import StringIO
import logging
import re
# from unittest import result
from neomodel import db
from datetime import datetime
from neomodel import (StructuredNode, StringProperty, IntegerProperty,
    RelationshipTo, Relationship, DateProperty, ArrayProperty)
import neomodel 

def create_property_type(value) -> neomodel.core.Property:
    if isinstance(value, list):
        prop_type = ArrayProperty()
    elif isinstance(value, int):
        prop_type = IntegerProperty()
    elif isinstance(value, str):
        if re.fullmatch(r'(\d{4}-\d{2}-\d{2})',value):
            prop_type = DateProperty()
        else:
            prop_type = StringProperty()
    return prop_type

def add_relationships_to_class(class_:str, relations:dict):
    rels= list(globals()[f"{class_}"].__all_relationships__)
    for rel, val in relations.items():
        setattr(globals()[f"{class_}"], rel, val)
        rels.append((rel, val))
    rels = tuple(rels)
    globals()[f"{class_}"].__all_relationships__ = rels 

def add_properties_to_class(class_:str, properties:dict):
    props= list(globals()[f"{class_}"].__all_properties__)
    for prop, val in properties.items():
        val = create_property_type(val)
        setattr(globals()[f"{class_}"], prop, val)
        props.append((prop, val))
    props = tuple(props)
    globals()[f"{class_}"].__all_properties__ = props 

def create_class(class_:str, properties={}):
    globals()[f"{class_}"] = type(class_, (StructuredNode, ), properties)


class Ontology():
    def __init__(self, url):
        db.set_connection(url)

try:
    ontology = Ontology('bolt://Rami:1234@localhost:7687')
    nodes, _ = db.cypher_query('MATCH(n) RETURN n')
    relationshipsTo, _ = db.cypher_query('MATCH (start_n)-[rel]->(end_n) RETURN start_n, rel, end_n')
except Exception as e:
    logging.error(e)

properties = {}
for node in nodes:
    class_, = node[0]._labels
    if class_ not in properties:
        properties[class_] ={}
    for property, value in node[0]._properties.items():
        if property not in properties[class_]:
            properties[class_][property] = create_property_type(value)


for class_ in properties:
    if class_ not in [value.__label__ for value in db._NODE_CLASS_REGISTRY.values()]:
        create_class(class_, properties[class_])

relationships = {}
for class_ in properties:
    relationships[class_] = {}
REL_INDEX = 1
for rel in relationshipsTo:
    rel=rel[REL_INDEX]
    for class_ in relationships:
        start_node_class, = rel.start_node.labels
        if start_node_class == class_:
            if rel.type not in relationships[start_node_class]:
                end_node_class, = rel.end_node._labels
                end_node_class = db._NODE_CLASS_REGISTRY[frozenset({end_node_class})]
                relationships[start_node_class][rel.type] = RelationshipTo(end_node_class, rel.type)

for class_ in relationships:
    add_relationships_to_class(class_, relationships[class_])


def create_entity(class_:str, properties:dict) -> StructuredNode:
    if class_ not in [value.__label__ for value in db._NODE_CLASS_REGISTRY.values()]:
        create_class(class_)
    entity = globals()[class_]()
    for property in properties:
        if property not in [tupl[0] for tupl in globals()[class_].__all_properties__]:
            add_properties_to_class(class_, {property:properties[property]})
        setattr(entity, property, properties[property])
    entity.save()
    return entity

def read_entity(class_:str, properties:dict):
    if class_ in [value.__label__ for value in db._NODE_CLASS_REGISTRY.values()]:
        return globals()[class_].nodes.get(**properties)
    else:
        logging.error('There is no such a class')

def update_entity(entity:StructuredNode, updates: dict):
    for key, val in updates.items():
        setattr(entity, key, val)
    entity.save()

def delete_entity(entity:StructuredNode):
    entity.delete()
    del entity # we shouldn't delete information unless they're incorrect

def create_relationship(entity1:StructuredNode, entity2:StructuredNode, relationship:str):
    if relationship not in entity1.__dict__:
        add_relationships_to_class(entity1.__class__.__label__, 
                                    {relationship: RelationshipTo(entity2, relationship)})
        setattr(entity1, relationship, entity1.__class__().__dict__[relationship])
        entity1.save()
    entity1.__dict__[relationship].connect(entity2)

def delete_relationship(entity1:StructuredNode, entity2:StructuredNode, relationship:str):
    if entity1.__dict__[relationship].is_connected(entity2):
        entity1.__dict__[relationship].disconnect(entity2)

yoga = read_entity('Habit', {'name':'Yoga'})

# create_entity('User', {'name':'Maya', 'surname':'Johnson', 'born':datetime(1982,12,11)})
# update_entity('User', {'id_':1}, {'name':'Jay Ryan', 'born':datetime(1880,6,30)})

# jack = read_entity('User', {'name':'Jack Ryan'})
# sport = read_entity('Interest', {'name':'Sport'})
# create_relationship(jack, sport, 'LIKES')
# create_relationship(jack, sport, 'LIKES')
# jack = create_entity('User', {'name':'Jack Ryan'})
# jack.__dict__['KEEPS_UP'].disconnect(Habit.nodes.get(name='Alcohol'))
print('end')