from rdflib import Graph

# Parse the RDF data and return a list of triples.
def _parse_rdf_data(file_path):
    g = Graph()
    g.parse(file_path, format="nt")
    return list(g)

# Build a dictionary for all "object" strings in the triples.
# The dictionary maps each string to a unique integer.
def _build_string_dictionary(triples):
    string_dict = {}
    next_id = 0
    for triple in triples:
        subject, property, obj = triple
       
        #if isinstance(subject, str) and subject not in string_dict:
        #    string_dict[subject] = next_id
        #    next_id += 1

        if isinstance(obj, str) and obj not in string_dict:
            string_dict[obj] = next_id
            next_id += 1

    return string_dict

# Vertically partition the triples based on properties.
def _vertically_partition(triples):
    property_tables = {}
    for triple in triples:
        subject, property, object = triple
        if property not in property_tables:
            property_tables[property] = []

        property_tables[property].append({'Subject': subject, 'Object': object})

    return property_tables



# Combine all steps and preprocess the data.
def preprocess_data(file_path):
    triples = _parse_rdf_data(file_path)
    print("Data parsed")
    
    # TODO: This is not needed for the join. I skip this step for now.
    string_dict = _build_string_dictionary(triples) 
    print("String_dict built")

    property_tables = _vertically_partition(triples)
    print("Property tables built")
    return string_dict, property_tables
