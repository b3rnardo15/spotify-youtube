"""
MongoDB Manager para operações CRUD e gerenciamento de conexões.
"""

import logging
from typing import Dict, List, Optional, Any
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import ConnectionFailure, PyMongoError


class MongoDBManager:
    """
    Classe para gerenciar conexões e operações CRUD com MongoDB.
    """
    
    def __init__(self, uri: str, database_name: str):
        """
        Inicializa o gerenciador MongoDB.
        
        Args:
            uri (str): URI de conexão do MongoDB
            database_name (str): Nome da base de dados
        """
        self.uri = uri
        self.database_name = database_name
        self.client: Optional[MongoClient] = None
        self.database: Optional[Database] = None
        self.logger = logging.getLogger(__name__)
        
    def connect(self) -> bool:
        """
        Estabelece conexão com o MongoDB.
        
        Returns:
            bool: True se a conexão foi bem-sucedida, False caso contrário
        """
        try:
            self.client = MongoClient(self.uri, serverSelectionTimeoutMS=5000)
            # Testa a conexão
            self.client.admin.command('ping')
            self.database = self.client[self.database_name]
            self.logger.info(f"Conectado ao MongoDB: {self.database_name}")
            return True
        except ConnectionFailure as e:
            self.logger.error(f"Falha na conexão com MongoDB: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Erro inesperado ao conectar ao MongoDB: {e}")
            return False
    
    def disconnect(self) -> None:
        """
        Fecha a conexão com o MongoDB.
        """
        if self.client:
            self.client.close()
            self.logger.info("Conexão com MongoDB fechada")
    
    def get_collection(self, collection_name: str) -> Optional[Collection]:
        """
        Obtém uma coleção do MongoDB.
        
        Args:
            collection_name (str): Nome da coleção
            
        Returns:
            Collection: Objeto da coleção ou None se não conectado
        """
        if self.database is None:
            self.logger.error("Não conectado ao MongoDB")
            return None
        return self.database[collection_name]
    
    def insert_one(self, collection_name: str, document: Dict[str, Any]) -> Optional[str]:
        """
        Insere um documento na coleção.
        
        Args:
            collection_name (str): Nome da coleção
            document (Dict): Documento a ser inserido
            
        Returns:
            str: ID do documento inserido ou None em caso de erro
        """
        try:
            collection = self.get_collection(collection_name)
            if collection is None:
                return None
            
            result = collection.insert_one(document)
            self.logger.info(f"Documento inserido na coleção {collection_name}: {result.inserted_id}")
            return str(result.inserted_id)
        except PyMongoError as e:
            self.logger.error(f"Erro ao inserir documento: {e}")
            return None
    
    def insert_many(self, collection_name: str, documents: List[Dict[str, Any]]) -> Optional[List[str]]:
        """
        Insere múltiplos documentos na coleção.
        
        Args:
            collection_name (str): Nome da coleção
            documents (List[Dict]): Lista de documentos a serem inseridos
            
        Returns:
            List[str]: Lista de IDs dos documentos inseridos ou None em caso de erro
        """
        try:
            collection = self.get_collection(collection_name)
            if collection is None:
                return None
            
            result = collection.insert_many(documents)
            self.logger.info(f"{len(result.inserted_ids)} documentos inseridos na coleção {collection_name}")
            return [str(id) for id in result.inserted_ids]
        except PyMongoError as e:
            self.logger.error(f"Erro ao inserir documentos: {e}")
            return None
    
    def find_one(self, collection_name: str, filter_dict: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Busca um documento na coleção.
        
        Args:
            collection_name (str): Nome da coleção
            filter_dict (Dict): Filtro para busca
            
        Returns:
            Dict: Documento encontrado ou None
        """
        try:
            collection = self.get_collection(collection_name)
            if collection is None:
                return None
            
            result = collection.find_one(filter_dict or {})
            return result
        except PyMongoError as e:
            self.logger.error(f"Erro ao buscar documento: {e}")
            return None
    
    def find_many(self, collection_name: str, filter_dict: Dict[str, Any] = None, 
                  limit: int = None) -> Optional[List[Dict[str, Any]]]:
        """
        Busca múltiplos documentos na coleção.
        
        Args:
            collection_name (str): Nome da coleção
            filter_dict (Dict): Filtro para busca (opcional)
            limit (int): Limite de documentos retornados (opcional)
            
        Returns:
            List[Dict]: Lista de documentos encontrados ou None em caso de erro
        """
        try:
            collection = self.get_collection(collection_name)
            if collection is None:
                return []
            
            cursor = collection.find(filter_dict or {})
            if limit:
                cursor = cursor.limit(limit)
            
            documents = list(cursor)
            return documents
        except PyMongoError as e:
            self.logger.error(f"Erro ao buscar documentos: {e}")
            return []
    
    def update_one(self, collection_name: str, filter_dict: Dict[str, Any], 
                   update_dict: Dict[str, Any], upsert: bool = False) -> bool:
        """
        Atualiza um documento na coleção.
        
        Args:
            collection_name (str): Nome da coleção
            filter_dict (Dict): Filtro para identificar o documento
            update_dict (Dict): Dados para atualização
            upsert (bool): Se True, cria o documento se não existir
            
        Returns:
            bool: True se a operação foi bem-sucedida
        """
        try:
            collection = self.get_collection(collection_name)
            if collection is None:
                return False
            
            result = collection.update_one(filter_dict, update_dict, upsert=upsert)
            return result.modified_count > 0 or (upsert and result.upserted_id is not None)
        except PyMongoError as e:
            self.logger.error(f"Erro ao atualizar documento: {e}")
            return False
    
    def update_many(self, collection_name: str, filter_dict: Dict[str, Any], 
                    update_dict: Dict[str, Any], upsert: bool = False) -> int:
        """
        Atualiza múltiplos documentos na coleção.
        
        Args:
            collection_name (str): Nome da coleção
            filter_dict (Dict): Filtro para identificar os documentos
            update_dict (Dict): Dados para atualização
            upsert (bool): Se True, cria documentos se não existirem
            
        Returns:
            int: Número de documentos modificados
        """
        try:
            collection = self.get_collection(collection_name)
            if collection is None:
                return 0
            
            result = collection.update_many(filter_dict, update_dict, upsert=upsert)
            return result.modified_count
        except PyMongoError as e:
            self.logger.error(f"Erro ao atualizar documentos: {e}")
            return 0
    
    def delete_one(self, collection_name: str, filter_dict: Dict[str, Any]) -> bool:
        """
        Remove um documento da coleção.
        
        Args:
            collection_name (str): Nome da coleção
            filter_dict (Dict): Filtro para identificar o documento
            
        Returns:
            bool: True se a operação foi bem-sucedida
        """
        try:
            collection = self.get_collection(collection_name)
            if collection is None:
                return False
            
            result = collection.delete_one(filter_dict)
            return result.deleted_count > 0
        except PyMongoError as e:
            self.logger.error(f"Erro ao deletar documento: {e}")
            return False
    
    def delete_many(self, collection_name: str, filter_dict: Dict[str, Any]) -> int:
        """
        Remove múltiplos documentos da coleção.
        
        Args:
            collection_name (str): Nome da coleção
            filter_dict (Dict): Filtro para identificar os documentos
            
        Returns:
            int: Número de documentos removidos
        """
        try:
            collection = self.get_collection(collection_name)
            if collection is None:
                return 0
            
            result = collection.delete_many(filter_dict)
            return result.deleted_count
        except PyMongoError as e:
            self.logger.error(f"Erro ao deletar documentos: {e}")
            return 0
    
    def count_documents(self, collection_name: str, filter_dict: Dict[str, Any] = None) -> int:
        """
        Conta documentos na coleção.
        
        Args:
            collection_name (str): Nome da coleção
            filter_dict (Dict): Filtro para contagem (opcional)
            
        Returns:
            int: Número de documentos ou -1 em caso de erro
        """
        try:
            collection = self.get_collection(collection_name)
            if collection is None:
                return -1
            
            count = collection.count_documents(filter_dict or {})
            return count
        except PyMongoError as e:
            self.logger.error(f"Erro ao contar documentos: {e}")
            return -1
    
    def create_index(self, collection_name: str, index_spec: List[tuple]) -> bool:
        """
        Cria um índice na coleção.
        
        Args:
            collection_name (str): Nome da coleção
            index_spec (List[tuple]): Especificação do índice
            
        Returns:
            bool: True se o índice foi criado com sucesso
        """
        try:
            collection = self.get_collection(collection_name)
            if collection is None:
                return False
            
            collection.create_index(index_spec)
            self.logger.info(f"Índice criado na coleção {collection_name}: {index_spec}")
            return True
        except PyMongoError as e:
            self.logger.error(f"Erro ao criar índice: {e}")
            return False
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()