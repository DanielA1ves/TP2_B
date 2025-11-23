import os
import threading
import time
from concurrent import futures
from xmlrpc.server import SimpleXMLRPCServer

import grpc
from lxml import etree

import property_service_pb2 as pb2
import property_service_pb2_grpc as pb2_grpc

DEFAULT_XMLS = [
    ("house_purchase.xml", "house_purchase.xsd"),
    ("trade_statistics.xml", "trade_statistics.xsd"),
]


class SharedState:
    def __init__(self):
        self.tree = None
        self.root = None
        self.item_tag = None
        self.id_attr = None
        self.xml_path, self.xsd_path = self._resolve_paths()

    def _resolve_paths(self):
        # se envs definidos, respeita-os
        if os.getenv("XML_PATH"):
            return os.getenv("XML_PATH"), os.getenv("XSD_PATH", "house_purchase.xsd")
        # tenta escolher pelo CSV disponível
        if os.path.exists("global_house_purchase_dataset.csv"):
            return "house_purchase.xml", "house_purchase.xsd"
        if os.path.exists("commodity_trade_statistics_data.csv"):
            return "trade_statistics.xml", "trade_statistics.xsd"
        # tenta escolher o primeiro XML existente
        for xmlp, xsd in DEFAULT_XMLS:
            if os.path.exists(xmlp):
                return xmlp, xsd
        # fallback para defaults
        return "house_purchase.xml", "house_purchase.xsd"

    def load_from_files(self):
        self.tree = etree.parse(self.xml_path)
        self.root = self.tree.getroot()
        self._infer_tags()
        return True

    def _infer_tags(self):
        # tenta inferir item_tag e id_attr a partir do primeiro elemento
        if self.root is not None and len(self.root):
            first = self.root[0]
            self.item_tag = self.item_tag or first.tag
            if first.attrib:
                self.id_attr = self.id_attr or list(first.attrib.keys())[0]
        # fallback para env/defaults
        self.item_tag = self.item_tag or os.getenv("ITEM_TAG", "record")
        self.id_attr = self.id_attr or os.getenv("ID_ATTR", "id")


class PropertyServicer(pb2_grpc.PropertyServiceServicer):
    def __init__(self, state: SharedState):
        self.state = state

    def UploadData(self, request, context):
        try:
            with open(self.state.xml_path, "w", encoding="utf-8") as f:
                f.write(request.xml_data)
            if request.xsd_data:
                with open(self.state.xsd_path, "w", encoding="utf-8") as f:
                    f.write(request.xsd_data)
            self.state.tree = etree.fromstring(request.xml_data.encode("utf-8")).getroottree()
            self.state.root = self.state.tree.getroot()
            self.state._infer_tags()
            print(
                f"UploadData: XML carregado em memória. "
                f"item_tag={self.state.item_tag}, id_attr={self.state.id_attr}, xml={self.state.xml_path}"
            )
            return pb2.UploadResponse(ok=True, message=f"XML recebido e guardado em {self.state.xml_path}.")
        except Exception as exc:
            print(f"Erro em UploadData: {exc}")
            self.state.tree = None
            self.state.root = None
            return pb2.UploadResponse(ok=False, message=str(exc))

    def CountRecords(self, request, context):
        if self.state.root is None:
            return pb2.CountResponse(count=0)
        try:
            return pb2.CountResponse(count=len(self.state.root.xpath(f"//{self.state.item_tag}")))
        except Exception as exc:
            print(f"Erro em CountRecords: {exc}")
            return pb2.CountResponse(count=0)

    def GetRecordByID(self, request, context):
        if self.state.root is None:
            return pb2.RecordResponse(record_xml="<error>Servidor nao carregado</error>")
        target_id = str(request.property_id)
        try:
            for prop in self.state.root.iter(self.state.item_tag):
                if prop.get(self.state.id_attr) == target_id:
                    xml_string = etree.tostring(
                        prop,
                        encoding="unicode",
                        pretty_print=False,
                        method="xml",
                        with_tail=False,
                    )
                    return pb2.RecordResponse(record_xml=xml_string)
            return pb2.RecordResponse(record_xml="<error>Registo nao encontrado</error>")
        except Exception as exc:
            print(f"Erro em GetRecordByID: {exc}")
            return pb2.RecordResponse(record_xml=f"<error>{exc}</error>")

    def ExecuteXPath(self, request, context):
        if self.state.root is None:
            return pb2.QueryResponse(results=["<error>Servidor nao carregado</error>"])
        xpath_query = request.query
        try:
            results = self.state.root.xpath(xpath_query, smart_strings=False)
            out = []
            for item in results:
                if isinstance(item, etree._Element):
                    out.append(etree.tostring(item, encoding="unicode"))
                else:
                    out.append(str(item))
            return pb2.QueryResponse(results=out)
        except etree.XPathEvalError as exc:
            return pb2.QueryResponse(results=[f"Erro ao executar XPath: {exc}"])
        except Exception as exc:
            return pb2.QueryResponse(results=[f"Erro desconhecido: {exc}"])


def run_grpc(state: SharedState):
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10),
        options=[
            ("grpc.max_receive_message_length", 200 * 1024 * 1024),
            ("grpc.max_send_message_length", 200 * 1024 * 1024),
        ],
    )
    pb2_grpc.add_PropertyServiceServicer_to_server(PropertyServicer(state), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    print("gRPC Server a correr na porta 50051...")
    server.wait_for_termination()


def run_xmlrpc(state: SharedState):
    while True:
        try:
            state.load_from_files()
            break
        except Exception as exc:
            print(f"XML-RPC: XML not ready ({exc}), aguardando arquivo {state.xml_path}...")
            time.sleep(2)

    handler_state = state
    server = SimpleXMLRPCServer(("0.0.0.0", 8000), allow_none=True)

    def execute_xpath(query):
        if handler_state.root is None:
            return "<error>Servidor nao carregado</error>"
        try:
            res = handler_state.root.xpath(query, smart_strings=False)
            if isinstance(res, list):
                return [
                    etree.tostring(item, encoding="unicode") if isinstance(item, etree._Element) else str(item)
                    for item in res
                ]
            return str(res)
        except Exception as exc:
            return f"Erro ao executar XPath: {exc}"

    def get_record_by_id(record_id):
        if handler_state.root is None:
            return "<error>Servidor nao carregado</error>"
        rec = handler_state.root.xpath(f'//{handler_state.item_tag}[@{handler_state.id_attr}="{record_id}"]')
        if rec:
            return etree.tostring(rec[0], encoding="unicode")
        return "Registo nao encontrado"

    def count_records():
        if handler_state.root is None:
            return 0
        return len(handler_state.root.xpath(f"//{handler_state.item_tag}"))

    server.register_function(execute_xpath, "execute_xpath")
    server.register_function(get_record_by_id, "get_record_by_id")
    server.register_function(count_records, "count_records")

    print("XML-RPC Server a correr na porta 8000...")
    server.serve_forever()


def main():
    state = SharedState()
    try:
        state.load_from_files()
        print(f"XML pré-carregado no arranque. item_tag={state.item_tag}, id_attr={state.id_attr}")
    except Exception:
        print("Nenhum XML válido no arranque; aguardando upload ou disponibilidade.")

    t1 = threading.Thread(target=run_grpc, args=(state,), daemon=True)
    t2 = threading.Thread(target=run_xmlrpc, args=(state,), daemon=True)
    t1.start()
    t2.start()

    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        print("Encerrando servidor dual.")


if __name__ == "__main__":
    main()
