import os
import pandas as pd
from xml.sax.saxutils import escape
import utils


DEFAULT_CANDIDATES = [
    "global_house_purchase_dataset.csv",
    "commodity_trade_statistics_data.csv",
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


def generate_xml(
    csv_path: str,
    id_column: str,
    xml_path: str = "house_purchase.xml",
    root_tag: str = "records",
    item_tag: str = "record",
    id_attr: str = "id",
    max_rows: int | None = None,
    chunk_size: int | None = None,
) -> str:
    """Gera um XML a partir de um CSV, garantindo um ID numérico."""
    nrows = max_rows if max_rows and max_rows > 0 else None

    if chunk_size is None:
        chunk_env = os.getenv("CHUNK_SIZE")
        if chunk_env:
            chunk_size = int(chunk_env)
        else:
            file_size = os.path.getsize(csv_path)
            if file_size > 200 * 1024 * 1024:
                chunk_size = 50000

    tag_map = {}

    def write_rows(df_chunk, start_index, writer, rt, it, ia):
        df_chunk[id_column] = range(start_index, start_index + len(df_chunk))
        # Ensure consistent column ordering for map integrity if processed in chunks
        cols_local = [id_column] + [c for c in df_chunk.columns if c != id_column]
        df_chunk = df_chunk[cols_local]
        
        nonlocal tag_map
        if not tag_map:
             tag_map = utils.get_unique_tag_map(df_chunk.columns)

        for _, row in df_chunk.iterrows():
            elements = []
            for col in df_chunk.columns:
                if col == id_column:
                    continue
                tag = tag_map[col]
                # clean value and escape
                val = utils.clean_xml_value(row[col])
                elements.append(f"<{tag}>{escape(val)}</{tag}>")
            
            elements_str = "".join(elements)
            # clean id attribute too
            # Force integer cast to avoid '1.0' float formatting which violates xs:integer
            id_val = utils.clean_xml_value(int(row[id_column]))
            writer.write(f'<{it} {ia}="{escape(id_val)}">{elements_str}</{it}>\n')
        return start_index + len(df_chunk)

    with open(xml_path, "w", encoding="utf-8") as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        total_written = 1
        if chunk_size:
            reader = pd.read_csv(csv_path, encoding="utf-8", chunksize=chunk_size, low_memory=False)
            first_chunk = next(reader)
            root_tag, item_tag, id_attr = derive_tags(id_column, first_chunk.columns)
            f.write(f"<{root_tag}>\n")
            total_written = write_rows(first_chunk, total_written, f, root_tag, item_tag, id_attr)
            for chunk in reader:
                total_written = write_rows(chunk, total_written, f, root_tag, item_tag, id_attr)
                if max_rows and total_written > max_rows:
                    break
        else:
            df = pd.read_csv(csv_path, encoding="utf-8", nrows=nrows, low_memory=False)
            root_tag, item_tag, id_attr = derive_tags(id_column, df.columns)
            f.write(f"<{root_tag}>\n")
            total_written = write_rows(df, total_written, f, root_tag, item_tag, id_attr)

        f.write(f"</{root_tag}>")

    return xml_path


if __name__ == "__main__":
    CSV_PATH = resolve_csv()
    ID_COLUMN = os.getenv("ID_COLUMN", "property_id")
    xml_path = os.getenv("XML_PATH", "house_purchase.xml")
    max_rows_env = os.getenv("MAX_ROWS")
    max_rows = int(max_rows_env) if max_rows_env else None
    chunk_env = os.getenv("CHUNK_SIZE")
    chunk_size = int(chunk_env) if chunk_env else None
    generate_xml(CSV_PATH, ID_COLUMN, xml_path=xml_path, max_rows=max_rows, chunk_size=chunk_size)
