import xmlschema

xsd_path = 'house_purchase.xsd'
xml_path = 'house_purchase.xml'

schema = xmlschema.XMLSchema(xsd_path)
xml_resource = xmlschema.XMLResource(xml_path, lazy=True)

is_valid = schema.is_valid(xml_resource)

print(f'O XML é válido? {is_valid}')

if not is_valid:
    for error in schema.iter_errors(xml_resource):
        print(error)
