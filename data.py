from rdflib import Graph, Literal, URIRef

FOLLOWS_10M = URIRef('http://db.uwaterloo.ca/~galuc/wsdbm/follows')
FRIEND_OF_10M = URIRef('http://db.uwaterloo.ca/~galuc/wsdbm/friendOf')
LIKES_10M = URIRef('http://db.uwaterloo.ca/~galuc/wsdbm/likes')
HAS_REVIEW_10M = URIRef('http://purl.org/stuff/rev#hasReview')

FOLLOWS_100K = URIRef('wsdbm:follows')
FRIEND_OF_100K = URIRef('wsdbm:friendOf')
LIKES_100K = URIRef('wsdbm:likes')
HAS_REVIEW_100K = URIRef('rev:hasReview')

FILE_PATH_10M = "/home/jangruhnert/Documents/watdiv.10M/watdiv.10M.nt"
FILE_PATH_100K = "/home/jangruhnert/Documents/watdiv.10M/100k.txt"


def _parse_rdf_data(format="nt"):
    '''Parse the RDF data and return a list of triples.'''
    g = Graph()
    if(format == "nt"):
        g.parse(FILE_PATH_10M, format=format)
        return [(s, p, o) for s, p, o in g.triples((None, None, None)) if p in [FOLLOWS_10M, FRIEND_OF_10M, LIKES_10M, HAS_REVIEW_10M]]
    else:
        with open(FILE_PATH_100K, 'r') as file:
            for line in file:
                line = line.strip()
                if line:  # Skip empty lines
                    subject, predicate, obj = line.split('\t')
                    subject = URIRef(subject)
                    predicate = URIRef(predicate)

                    # Check if the object is a URI or a literal
                    if obj.startswith('http://') or obj.startswith('https://'):
                        obj = URIRef(obj)
                    else:
                        obj = Literal(obj)

                    if predicate in [FOLLOWS_100K, FRIEND_OF_100K, LIKES_100K, HAS_REVIEW_100K]:
                        g.add((subject, predicate, obj))

        return g
    
def _build_string_dictionary(triples):
    '''Build a dictionary that maps subject and object strings of triples to unique integer IDs.'''
    strings = set()
    for s, _, o in triples:
            strings.add(s)
            strings.add(o)

    return {element: idx for idx, element in enumerate(strings)}


def _vertically_partition(triples, dictionary):
    '''Vetically partition the triples into property tables. 
    Replacing the strings with their dictionary integer IDs.'''
    property_tables = {}
    for triple in triples:
        subject, property, object = triple
        if property not in property_tables:
            property_tables[property] = []

        property_tables[property].append([dictionary[subject], dictionary[object]])

    return property_tables



# Combine all steps and preprocess the data.
def preprocess_data(format):
    triples = _parse_rdf_data(format=format)
    print("Data parsed")
    
    string_dict = _build_string_dictionary(triples) 
    print("String_dict built")

    property_tables = _vertically_partition(triples, dictionary=string_dict)
    print("Property tables built")

    return string_dict, property_tables
