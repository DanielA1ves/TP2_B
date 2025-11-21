import grpc
# Importar os ficheiros gerados
import property_service_pb2 as pb2
import property_service_pb2_grpc as pb2_grpc


def run():
    # Conectar ao servidor gRPC na porta 50051
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = pb2_grpc.PropertyServiceStub(channel)

        print("=== Testes gRPC ===")

        # 1. Teste CountRecords
        print("\n1. Contando registos:")
        response_count = stub.CountRecords(pb2.Empty())
        print(f"   Total: {response_count.count} registos")

        # 2. Teste GetRecordByID
        test_id = 10
        print(f"\n2. Procurando registo com ID={test_id}:")
        response_record = stub.GetRecordByID(pb2.RecordRequest(property_id=test_id))
        print(f"   Resultado (início): {response_record.record_xml[:100]}...")

        # 3. Teste ExecuteXPath (Buscar as 3 primeiras cidades únicas)
        xpath_query = '//city/text()'
        print(f"\n3. Procurando cidades únicas com XPath: {xpath_query}")
        response_query = stub.ExecuteXPath(pb2.QueryRequest(query=xpath_query))

        if response_query.results:
            unique_ordered = []
            for loc in response_query.results:
                if loc not in unique_ordered:
                    unique_ordered.append(loc)
                if len(unique_ordered) >= 3:
                    break
            print(f"   Localizações únicas (3): {unique_ordered}")
        else:
            print("   Nenhuma localização encontrada ou erro na query.")


if __name__ == '__main__':
    run()