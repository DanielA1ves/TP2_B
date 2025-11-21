import grpc
from concurrent import futures
from lxml import etree
import time

# Importar os ficheiros gerados
import property_service_pb2 as pb2
import property_service_pb2_grpc as pb2_grpc


class PropertyServicer(pb2_grpc.PropertyServiceServicer):
    def __init__(self, xml_path):
        self.xml_path = xml_path
        try:
            self.tree = etree.parse(xml_path)
            self.root = self.tree.getroot()
            print(f"XML '{xml_path}' carregado com sucesso.")
        except Exception as e:
            print(f"Erro ao carregar o XML: {e}")
            self.root = None

    def CountRecords(self, request, context):
        if self.root is None:
            return pb2.CountResponse(count=0)

        count = len(self.root.xpath('//property'))
        return pb2.CountResponse(count=count)

    def GetRecordByID(self, request, context):
        if self.root is None:
            return pb2.RecordResponse(record_xml="<error>Servidor não carregado</error>")

        record_id = request.property_id
        record = self.root.xpath(f'//property[@property_id="{record_id}"]')

        if record:
            xml_string = etree.tostring(record[0], encoding='unicode')
            return pb2.RecordResponse(record_xml=xml_string)

        return pb2.RecordResponse(record_xml="<error>Registo não encontrado</error>")

    def ExecuteXPath(self, request, context):
        if self.root is None:
            return pb2.QueryResponse(results=["<error>Servidor não carregado</error>"])

        xpath_query = request.query
        try:
            results = self.root.xpath(xpath_query, smart_strings=False)

            string_results = []
            for item in results:
                if isinstance(item, etree._Element):
                    string_results.append(etree.tostring(item, encoding='unicode'))
                else:
                    string_results.append(str(item))

            return pb2.QueryResponse(results=string_results)

        except etree.XPathEvalError as e:
            return pb2.QueryResponse(results=[f"Erro ao executar XPath: {str(e)}"])
        except Exception as e:
            return pb2.QueryResponse(results=[f"Erro desconhecido: {str(e)}"])


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_PropertyServiceServicer_to_server(
        PropertyServicer('house_purchase.xml'), server)

    server.add_insecure_port('[::]:50051')
    server.start()
    print("gRPC Server a correr na porta 50051...")

    try:
        while True:
            time.sleep(60 * 60 * 24)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == '__main__':
    serve()