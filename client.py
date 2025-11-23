import os
import time

import grpc
import xmlschema

import property_service_pb2 as pb2
import property_service_pb2_grpc as pb2_grpc
import xml_converter  # noqa: F401
import schema_creator  # noqa: F401
import validator  # noqa: F401


def generate_and_validate():
    """Gera XML/XSD a partir do CSV e valida localmente."""
    schema = xmlschema.XMLSchema("house_purchase.xsd")
    xml_resource = xmlschema.XMLResource("house_purchase.xml", lazy=True)
    if not schema.is_valid(xml_resource):
        raise ValueError("XML gerado localmente não é válido.")
    with open("house_purchase.xml", "r", encoding="utf-8") as f:
        xml_data = f.read()
    with open("house_purchase.xsd", "r", encoding="utf-8") as f:
        xsd_data = f.read()
    return xml_data, xsd_data


def upload_and_test():
    host = os.getenv("GRPC_SERVER_HOST", "server")
    port = os.getenv("GRPC_SERVER_PORT", "50051")
    target = f"{host}:{port}"

    print(f"Target gRPC: {target}")
    print("Aguardar 5 segundos para o servidor gRPC inicializar...")
    time.sleep(5)

    xml_data, xsd_data = generate_and_validate()
    print(f"XML gerado localmente com {len(xml_data)} bytes.")

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

        print("\n2. A obter registo pelo ID=1:")
        record = stub.GetRecordByID(pb2.RecordRequest(property_id=1))
        snippet = record.record_xml[:500]
        print(f"   XML do registo:\n{snippet}{'...' if len(record.record_xml) > 500 else ''}")

        print("\n3. XPath - primeiras 3 cidades únicas:")
        cities = stub.ExecuteXPath(pb2.QueryRequest(query="//city/text()"))
        unique_cities = []
        for city in cities.results:
            if city not in unique_cities:
                unique_cities.append(city)
            if len(unique_cities) >= 3:
                break
        print(f"   Cidades: {unique_cities}")


if __name__ == "__main__":
    upload_and_test()
