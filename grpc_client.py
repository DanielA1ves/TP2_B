import grpc
import time  # Certifique-se que esta linha existe!
# Importar os ficheiros gerados
import property_service_pb2 as pb2
import property_service_pb2_grpc as pb2_grpc


def run():
    # Adiciona a espera de 20 segundos antes de tentar a conexão
    print("Aguardando 20 segundos para o servidor inicializar...")
    time.sleep(20)

    # CORREÇÃO: Conecta-se ao nome do serviço Docker 'grpc_server'
    # O erro anterior era tentar ligar-se a 127.0.0.1 (localhost)
    with grpc.insecure_channel('grpc-service:50051') as channel:
        stub = pb2_grpc.PropertyServiceStub(channel)

        print("=== Testes gRPC ===")

        # 1. Teste CountRecords
        print("\n1. Contando registos:")
        response_count = stub.CountRecords(pb2.Empty())
        print(f"   Total: {response_count.count} registos")

        # ... (O resto do seu código de teste gRPC deve seguir aqui) ...
        # ... (Testes GetPropertyByPrice, GetPropertyByTypology, etc.) ...
        # ...

# A sua chamada final 'if __name__ == '__main__': run()' deve permanecer no final.