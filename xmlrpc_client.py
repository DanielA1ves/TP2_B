import xmlrpc.client

class XMLRPCClient:
    def __init__(self, host='localhost', port=8000):
        self.server = xmlrpc.client.ServerProxy(f'http://{host}:{port}')
        print(f"Cliente conectado ao servidor {host}:{port}")
    
    def test_xpath_queries(self):
        """Testa várias queries XPath"""
        print("\n=== TESTES XPATH ===")
        
        # Contar registos
        print("\n1. Contando registos totais:")
        count = self.server.count_records()
        print(f"   Total: {count} registos")
        
        # Buscar todos os preços (corrigido para minúsculas)
        print("\n2. Buscando todos os preços:")
        prices = self.server.execute_xpath('//price/text()')
        if isinstance(prices, list):
            print(f"   Encontrados {len(prices)} preços")
            print(f"   Primeiros 3: {prices[:3]}")
        else:
            print(f"   Resultado: {prices}")
        
        # Buscar por localização específica — mostrar as 3 primeiras cidades únicas:
        print("\n3. Buscar as 3 primeiras cidades únicas:")
        locations = self.server.execute_xpath('//city/text()')
        if isinstance(locations, list):
            unique_ordered = []
            for loc in locations:
                if loc not in unique_ordered:
                    unique_ordered.append(loc)
                if len(unique_ordered) >= 3:
                    break
            print(f"   Localizações encontradas: {unique_ordered}")
        else:
            print(f"   Resultado: {locations}")
    
def main():
    """Função principal para demonstração"""
    try:
        client = XMLRPCClient()
        
        # Executar testes automáticos
        client.test_xpath_queries()
        
    except ConnectionRefusedError:
        print("Erro: Não foi possível conectar ao servidor.")
        print("Certifica-te de que o servidor XML-RPC está a correr.")
        print("Executa: python xmlrpc_server.py")
    except Exception as e:
        print(f"Erro: {e}")

if __name__ == '__main__':
    main()
