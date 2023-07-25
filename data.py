from rdflib import Graph
import rdflib

FOLLOWS = rdflib.term.URIRef('http://db.uwaterloo.ca/~galuc/wsdbm/follows')
FRIEND_OF = rdflib.term.URIRef('http://db.uwaterloo.ca/~galuc/wsdbm/friendOf')
LIKES = rdflib.term.URIRef('http://db.uwaterloo.ca/~galuc/wsdbm/likes')
HAS_REVIEW = rdflib.term.URIRef('http://purl.org/stuff/rev#hasReview')

# Parse the RDF data and return a list of triples.
def _parse_rdf_data(file_path):
    g = Graph()
    g.parse(file_path, format="nt")
    return [(s, p, o) for s, p, o in g.triples((None, None, None)) if p in [FOLLOWS, FRIEND_OF, LIKES, HAS_REVIEW]]


# Build a dictionary for all "object" strings in the triples.
# The dictionary maps each string to a unique integer.
def _build_string_dictionary(triples):
    strings = set()
    for s, _, o in triples:
        if isinstance(s, str):
            strings.add(s)
        if isinstance(o, str):
            strings.add(o)

    return {s: idx for idx, s in enumerate(strings)}

# Vertically partition the triples based on properties.
def _vertically_partition(triples, dictionary):
    property_tables = {}
    for triple in triples:
        subject, property, object = triple
        if property not in property_tables:
            property_tables[property] = []

        property_tables[property].append([dictionary[subject], dictionary[object]])
    #print(dictionary.values())
    return property_tables



# Combine all steps and preprocess the data.
def preprocess_data(file_path):
    triples = _parse_rdf_data(file_path)
    print("Data parsed")
    
    # TODO: This is not needed for the join. I skip this step for now.
    string_dict = _build_string_dictionary(triples) 
    print("String_dict built")

    property_tables = _vertically_partition(triples, dictionary=string_dict)
    print("Property tables built")
    return string_dict, property_tables
