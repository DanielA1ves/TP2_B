import pandas as pd

df = pd.read_csv('global_house_purchase_dataset.csv', nrows=1)

xsd_start = '''<?xml version="1.0"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:element name="properties">
    <xs:complexType>
      <xs:sequence>
        <xs:element name="property" minOccurs="0" maxOccurs="unbounded">
          <xs:complexType>
            <xs:sequence>
'''

xsd_end = '''            </xs:sequence>
            <xs:attribute name="property_id" type="xs:integer" use="required"/>
          </xs:complexType>
        </xs:element>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
</xs:schema>
'''

def python_to_xsd(dtype):
    if dtype.startswith('int'):
        return 'xs:integer'
    elif dtype.startswith('float'):
        return 'xs:float'
    else:
        return 'xs:string'

xsd_fields = ''
for col in df.columns[1:]:
    field_type = python_to_xsd(str(df[col].dtype))
    xsd_fields += f'              <xs:element name="{col}" type="{field_type}" minOccurs="0"/>\n'

with open('house_purchase.xsd', 'w') as f:
    f.write(xsd_start + xsd_fields + xsd_end)
