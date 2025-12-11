import glob
import os
import time

import grpc
import xmlschema
import pandas as pd

import property_service_pb2 as pb2
import property_service_pb2_grpc as pb2_grpc
import xml_converter
import schema_creator


def resolve_csv() -> str:
    if os.getenv("DATA_CSV"):
        return os.getenv("DATA_CSV")
    for candidate in [
        "global_house_purchase_dataset.csv",
        "global_commodity_trade_statistics.csv",
    ]:
        if os.path.exists(candidate):
            return candidate
    found = glob.glob("*.csv")
    if found:
        return found[0]
    raise FileNotFoundError("Nenhum CSV encontrado. Defina DATA_CSV ou coloque um CSV na raiz.")


def auto_infer_config(csv_path: str):
    df = pd.read_csv(csv_path, nrows=1)
    id_column = os.getenv("ID_COLUMN")
    if not id_column:
        if "property_id" in df.columns:
            id_column = "property_id"
        elif "id" in df.columns:
            id_column = "id"
        else:
            id_column = "id"

    root_tag = os.getenv("ROOT_TAG")
    item_tag = os.getenv("ITEM_TAG")
    id_attr = os.getenv("ID_ATTR", id_column)
    if not root_tag or not item_tag:
        if id_column == "property_id" and "city" in df.columns:
            root_tag = root_tag or "properties"
            item_tag = item_tag or "property"
        else:
            root_tag = root_tag or "records"
            item_tag = item_tag or "record"

    xpath_query = os.getenv("XPATH_QUERY")
    if not xpath_query:
        # pega primeira coluna não-id
        non_id_cols = [c for c in df.columns if c != id_column]
        if non_id_cols:
            xpath_query = f"//{non_id_cols[0]}/text()"
        else:
            xpath_query = "//text()"

    max_rows_env = os.getenv("MAX_ROWS")
    max_rows = int(max_rows_env) if max_rows_env else None
    test_id = int(os.getenv("TEST_ID", "1"))
    cfg = {
        "csv_path": csv_path,
        "id_column": id_column,
        "root_tag": root_tag,
        "item_tag": item_tag,
        "id_attr": id_attr,
        "xpath_query": xpath_query,
        "test_id": test_id,
        "max_rows": max_rows,
    }
    if os.getenv("XML_PATH"):
        cfg["xml_out"] = os.getenv("XML_PATH")
        cfg["xsd_out"] = os.getenv("XSD_PATH", "house_purchase.xsd")
    else:
        if "commodity_trade_statistics" in csv_path:
            cfg["xml_out"] = "trade_statistics.xml"
            cfg["xsd_out"] = "trade_statistics.xsd"
        else:
            # Derive from CSV name instead of default "house_purchase"
            base_name = os.path.splitext(os.path.basename(csv_path))[0]
            cfg["xml_out"] = f"{base_name}.xml"
            cfg["xsd_out"] = f"{base_name}.xsd"
            # Fallback for old default if empty
            if not base_name: 
                cfg["xml_out"] = "house_purchase.xml"
                cfg["xsd_out"] = "house_purchase.xsd"
    return cfg


def generate_and_validate(cfg):
    if os.getenv("XML_PATH"):
        xml_out = os.getenv("XML_PATH")
        xsd_out = os.getenv("XSD_PATH", "house_purchase.xsd")
    else:
        if "commodity_trade_statistics" in cfg["csv_path"]:
            xml_out = "trade_statistics.xml"
            xsd_out = "trade_statistics.xsd"
        else:
            # Derive from CSV name
            base_name = os.path.splitext(os.path.basename(cfg["csv_path"]))[0]
            xml_out = f"{base_name}.xml"
            xsd_out = f"{base_name}.xsd"
            if not base_name:
                xml_out = "house_purchase.xml"
                xsd_out = "house_purchase.xsd"

    xml_converter.generate_xml(
        cfg["csv_path"],
        cfg["id_column"],
        xml_path=xml_out,
        max_rows=cfg.get("max_rows"),
    )
    schema_creator.generate_xsd(
        cfg["csv_path"],
        cfg["id_column"],
        xsd_path=xsd_out,
    )

    schema = xmlschema.XMLSchema(xsd_out)
    xml_resource = xmlschema.XMLResource(xml_out, lazy=True)
    if not schema.is_valid(xml_resource):
        # Para debug, ver logs anteriores ou ativar utils.debug
        raise ValueError("XML gerado localmente não é válido.")

    with open(xml_out, "r", encoding="utf-8") as f:
        xml_data = f.read()
    with open(xsd_out, "r", encoding="utf-8") as f:
        xsd_data = f.read()
    return xml_data, xsd_data


def upload_and_test():
    csv_path = resolve_csv()
    cfg = auto_infer_config(csv_path)

    xml_out = cfg.get("xml_out", "house_purchase.xml")
    xsd_out = cfg.get("xsd_out", "house_purchase.xsd")

    host = os.getenv("GRPC_SERVER_HOST", "server")
    port = os.getenv("GRPC_SERVER_PORT", "50051")
    target = f"{host}:{port}"

    print(f"CSV selecionado: {csv_path}")
    print(f"Config: id={cfg['id_column']}, root={cfg['root_tag']}, item={cfg['item_tag']}, id_attr={cfg['id_attr']}")
    print(f"Target gRPC: {target}")
    print("Aguardar 5 segundos para o servidor gRPC inicializar...")
    time.sleep(5)

    xml_data, xsd_data = generate_and_validate(cfg)
    print(f"XML gerado localmente ({xml_out}) com {len(xml_data)} bytes.")

    options = [
        ("grpc.max_receive_message_length", 200 * 1024 * 1024),
        ("grpc.max_send_message_length", 200 * 1024 * 1024),
    ]
    with grpc.insecure_channel(target, options=options) as channel:
        try:
            grpc.channel_ready_future(channel).result(timeout=10)
        except grpc.FutureTimeoutError:
            print("Canal gRPC não ficou pronto a tempo.")
            return

        stub = pb2_grpc.PropertyServiceStub(channel)

        print("\n=== Upload para o servidor ===")
        resp_upload = stub.UploadData(pb2.UploadRequest(xml_data=xml_data, xsd_data=xsd_data))
        print(f"Upload ok? {resp_upload.ok} - {resp_upload.message}")
        if not resp_upload.ok:
            return

        print("\n=== Testes gRPC ===")

        print("\n1. Contando registos:")
        response_count = stub.CountRecords(pb2.Empty())
        print(f"   Total: {response_count.count} registos")

        print(f"\n2. A obter registo pelo ID={cfg['test_id']}:")
        record = stub.GetRecordByID(pb2.RecordRequest(property_id=cfg["test_id"]))
        snippet = record.record_xml[:500]
        print(f"   XML do registo:\n{snippet}{'...' if len(record.record_xml) > 500 else ''}")

        print(f"\n3. XPath - query ({cfg['xpath_query']}):")
        results = stub.ExecuteXPath(pb2.QueryRequest(query=cfg["xpath_query"]))
        print(f"   Resultados: {results.results[:3]}")


if __name__ == "__main__":
    upload_and_test()
