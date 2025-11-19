import pandas as pd
from xml.sax.saxutils import escape

df = pd.read_csv('global_house_purchase_dataset.csv', encoding='utf-8')

def row_to_xml(row):
    elements = [f'<{col}>{escape(str(row[col]))}</{col}>' for col in df.columns[1:]]
    elements_str = ''.join(elements)
    xml = f'<property property_id="{escape(str(row.property_id))}">{elements_str}</property>\n'
    return xml

with open('house_purchase.xml', 'w', encoding='utf-8') as f:
    f.write('<properties>\n')
    for _, row in df.iterrows():
        f.write(row_to_xml(row))
    f.write('</properties>')
