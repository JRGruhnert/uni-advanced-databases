from rdflib import Graph, Literal, URIRef

FOLLOWS = URIRef('http://db.uwaterloo.ca/~galuc/wsdbm/follows')
FRIEND_OF = URIRef('http://db.uwaterloo.ca/~galuc/wsdbm/friendOf')
LIKES = URIRef('http://db.uwaterloo.ca/~galuc/wsdbm/likes')
HAS_REVIEW = URIRef('http://purl.org/stuff/rev#hasReview')

FOLLOWS2 = URIRef('wsdbm:follows')
FRIEND_OF2 = URIRef('wsdbm:friendOf')
LIKES2 = URIRef('wsdbm:likes')
HAS_REVIEW2 = URIRef('rev:hasReview')

# Parse the RDF data and return a list of triples.
def _parse_rdf_data(file_path, format="nt"):
    if(format == "nt"):
        g = Graph()
        g.parse(file_path, format=format)
        return [(s, p, o) for s, p, o in g.triples((None, None, None)) if p in [FOLLOWS, FRIEND_OF, LIKES, HAS_REVIEW]]
    else:
        return parse_txt_to_rdf(file_path)

def parse_txt_to_rdf(file_path):
    rdf_graph = Graph()

    with open(file_path, 'r') as file:
        for line in file:
            line = line.strip()
            if line:  # Skip empty lines
                subject, predicate, obj = line.split('\t')
                subject = URIRef(subject)
                predicate = URIRef(predicate)
                #print(subject)
                print(predicate)

                # Check if the object is a URI or a literal
                if obj.startswith('http://') or obj.startswith('https://'):
                    obj = URIRef(obj)
                else:
                    obj = Literal(obj)

                if predicate in [FOLLOWS2, FRIEND_OF2, LIKES2, HAS_REVIEW2]:
                    rdf_graph.add((subject, predicate, obj))

    return rdf_graph

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
def preprocess_data(file_path, format):
    triples = _parse_rdf_data(file_path, format=format)
    print("Data parsed")
    
    # TODO: This is not needed for the join. I skip this step for now.
    string_dict = _build_string_dictionary(triples) 
    print("String_dict built")

    property_tables = _vertically_partition(triples, dictionary=string_dict)
    print("Property tables built")
    return string_dict, property_tables
