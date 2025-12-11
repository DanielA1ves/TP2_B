import glob
import os
import pandas as pd
import utils

DEFAULT_CANDIDATES = [
    "global_house_purchase_dataset.csv",
    "global_commodity_trade_statistics.csv",
]


def resolve_csv() -> str:
    if os.getenv("DATA_CSV"):
        return os.getenv("DATA_CSV")
    for candidate in DEFAULT_CANDIDATES:
        if os.path.exists(candidate):
            return candidate
    found = glob.glob("*.csv")
    if found:
        return found[0]
    raise FileNotFoundError("Nenhum CSV encontrado. Defina DATA_CSV ou coloque um CSV na raiz.")


def python_to_xsd(dtype: str) -> str:
    # Para robustez com dados mistos/NaN, usar string
    return "xs:string"


def derive_tags(id_column: str, df_columns) -> tuple[str, str, str]:
    root_tag = os.getenv("ROOT_TAG")
    item_tag = os.getenv("ITEM_TAG")
    id_attr = os.getenv("ID_ATTR", id_column)

    if not root_tag or not item_tag:
        if id_column == "property_id" and "city" in df_columns:
            root_tag = root_tag or "properties"
            item_tag = item_tag or "property"
        else:
            root_tag = root_tag or "records"
            item_tag = item_tag or "record"
    return root_tag, item_tag, id_attr


def generate_xsd(
    csv_path: str,
    id_column: str,
    xsd_path: str = "house_purchase.xsd",
    root_tag: str = "records",
    item_tag: str = "record",
    id_attr: str = "id",
) -> str:
    df = pd.read_csv(csv_path, nrows=1)
    if id_column not in df.columns:
        df[id_column] = [1]

    root_tag, item_tag, id_attr = derive_tags(id_column, df.columns)

    cols = [id_column] + [c for c in df.columns if c != id_column]
    df = df[cols]

    xsd_start = f'''<?xml version="1.0"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:element name="{root_tag}">
    <xs:complexType>
      <xs:sequence>
        <xs:element name="{item_tag}" minOccurs="0" maxOccurs="unbounded">
          <xs:complexType>
            <xs:sequence>
'''

    xsd_end = f'''            </xs:sequence>
            <xs:attribute name="{id_attr}" type="xs:integer" use="required"/>
          </xs:complexType>
        </xs:element>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
</xs:schema>
'''

    xsd_fields = ""
    tag_map = utils.get_unique_tag_map(df.columns)
    for col in df.columns:
        if col == id_column:
            continue
        field_type = python_to_xsd(str(df[col].dtype))
        sanitized_name = tag_map[col]
        xsd_fields += f'              <xs:element name="{sanitized_name}" type="{field_type}" minOccurs="0"/>\n'

    with open(xsd_path, "w", encoding="utf-8") as f:
        f.write(xsd_start + xsd_fields + xsd_end)

    return xsd_path


if __name__ == "__main__":
    CSV_PATH = resolve_csv()
    ID_COLUMN = os.getenv("ID_COLUMN", "property_id")
    xsd_path = os.getenv("XSD_PATH", "house_purchase.xsd")
    generate_xsd(CSV_PATH, ID_COLUMN, xsd_path=xsd_path)
