"""
Módulo de carga de dados do pipeline ETL.
Responsável por persistir os dados transformados no MongoDB.
"""

import os
import sys
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
import time
import pymongo
from pymongo.errors import BulkWriteError, DuplicateKeyError

# Adiciona o diretório pai ao path para imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.mongodb_manager import MongoDBManager


class DataLoader:
    """
    Classe responsável pela carga de dados transformados no MongoDB.
    """
    
    def __init__(self, mongo_manager: MongoDBManager):
        """
        Inicializa o carregador de dados.
        
        Args:
            mongo_manager (MongoDBManager): Instância do gerenciador MongoDB
        """
        self.mongo_manager = mongo_manager
        self.logger = logging.getLogger(__name__)
        
        # Configurações de coleções
        self.collections = {
            'spotify_tracks': 'spotify_tracks',
            'spotify_features': 'spotify_features',
            'youtube_videos': 'youtube_videos',
            'correlated_data': 'correlated_data',
            'regional_engagement': 'regional_engagement',
            'load_metadata': 'load_metadata'
        }
    
    def load_spotify_tracks(self, tracks: List[Dict[str, Any]], 
                           batch_size: int = 1000) -> Dict[str, Any]:
        """
        Carrega faixas do Spotify no MongoDB.
        
        Args:
            tracks (List[Dict]): Lista de faixas transformadas
            batch_size (int): Tamanho do lote para inserção
            
        Returns:
            Dict: Resultado da operação de carga
        """
        self.logger.info(f"Iniciando carga de {len(tracks)} faixas do Spotify")
        
        collection = self.mongo_manager.get_collection(self.collections['spotify_tracks'])
        
        # Cria índices se não existirem
        self._ensure_spotify_tracks_indexes(collection)
        
        # Processa em lotes
        results = {
            'total_processed': 0,
            'inserted': 0,
            'updated': 0,
            'errors': 0,
            'error_details': []
        }
        
        for i in range(0, len(tracks), batch_size):
            batch = tracks[i:i + batch_size]
            batch_result = self._load_batch_spotify_tracks(collection, batch)
            
            # Atualiza resultados
            results['total_processed'] += batch_result['processed']
            results['inserted'] += batch_result['inserted']
            results['updated'] += batch_result['updated']
            results['errors'] += batch_result['errors']
            results['error_details'].extend(batch_result['error_details'])
            
            self.logger.info(f"Lote processado: {i + len(batch)}/{len(tracks)}")
        
        # Registra metadados da carga
        self._log_load_metadata('spotify_tracks', results)
        
        self.logger.info(f"Carga de faixas concluída: {results}")
        return results
    
    def load_spotify_features(self, features: List[Dict[str, Any]], 
                            batch_size: int = 1000) -> Dict[str, Any]:
        """
        Carrega características de áudio do Spotify no MongoDB.
        
        Args:
            features (List[Dict]): Lista de características transformadas
            batch_size (int): Tamanho do lote para inserção
            
        Returns:
            Dict: Resultado da operação de carga
        """
        self.logger.info(f"Iniciando carga de {len(features)} características de áudio")
        
        collection = self.mongo_manager.get_collection(self.collections['spotify_features'])
        
        # Cria índices se não existirem
        self._ensure_spotify_features_indexes(collection)
        
        # Processa em lotes
        results = {
            'total_processed': 0,
            'inserted': 0,
            'updated': 0,
            'errors': 0,
            'error_details': []
        }
        
        for i in range(0, len(features), batch_size):
            batch = features[i:i + batch_size]
            batch_result = self._load_batch_spotify_features(collection, batch)
            
            # Atualiza resultados
            results['total_processed'] += batch_result['processed']
            results['inserted'] += batch_result['inserted']
            results['updated'] += batch_result['updated']
            results['errors'] += batch_result['errors']
            results['error_details'].extend(batch_result['error_details'])
            
            self.logger.info(f"Lote processado: {i + len(batch)}/{len(features)}")
        
        # Registra metadados da carga
        self._log_load_metadata('spotify_features', results)
        
        self.logger.info(f"Carga de características concluída: {results}")
        return results
    
    def load_youtube_videos(self, videos: List[Dict[str, Any]], 
                          batch_size: int = 1000) -> Dict[str, Any]:
        """
        Carrega vídeos do YouTube no MongoDB.
        
        Args:
            videos (List[Dict]): Lista de vídeos transformados
            batch_size (int): Tamanho do lote para inserção
            
        Returns:
            Dict: Resultado da operação de carga
        """
        self.logger.info(f"Iniciando carga de {len(videos)} vídeos do YouTube")
        
        collection = self.mongo_manager.get_collection(self.collections['youtube_videos'])
        
        # Cria índices se não existirem
        self._ensure_youtube_videos_indexes(collection)
        
        # Processa em lotes
        results = {
            'total_processed': 0,
            'inserted': 0,
            'updated': 0,
            'errors': 0,
            'error_details': []
        }
        
        for i in range(0, len(videos), batch_size):
            batch = videos[i:i + batch_size]
            batch_result = self._load_batch_youtube_videos(collection, batch)
            
            # Atualiza resultados
            results['total_processed'] += batch_result['processed']
            results['inserted'] += batch_result['inserted']
            results['updated'] += batch_result['updated']
            results['errors'] += batch_result['errors']
            results['error_details'].extend(batch_result['error_details'])
            
            self.logger.info(f"Lote processado: {i + len(batch)}/{len(videos)}")
        
        # Registra metadados da carga
        self._log_load_metadata('youtube_videos', results)
        
        self.logger.info(f"Carga de vídeos concluída: {results}")
        return results
    
    def load_correlated_data(self, correlations: List[Dict[str, Any]], 
                           batch_size: int = 1000) -> Dict[str, Any]:
        """
        Carrega dados correlacionados no MongoDB.
        
        Args:
            correlations (List[Dict]): Lista de correlações
            batch_size (int): Tamanho do lote para inserção
            
        Returns:
            Dict: Resultado da operação de carga
        """
        self.logger.info(f"Iniciando carga de {len(correlations)} correlações")
        
        collection = self.mongo_manager.get_collection(self.collections['correlated_data'])
        
        # Cria índices se não existirem
        self._ensure_correlated_data_indexes(collection)
        
        # Processa em lotes
        results = {
            'total_processed': 0,
            'inserted': 0,
            'updated': 0,
            'errors': 0,
            'error_details': []
        }
        
        for i in range(0, len(correlations), batch_size):
            batch = correlations[i:i + batch_size]
            batch_result = self._load_batch_correlated_data(collection, batch)
            
            # Atualiza resultados
            results['total_processed'] += batch_result['processed']
            results['inserted'] += batch_result['inserted']
            results['updated'] += batch_result['updated']
            results['errors'] += batch_result['errors']
            results['error_details'].extend(batch_result['error_details'])
            
            self.logger.info(f"Lote processado: {i + len(batch)}/{len(correlations)}")
        
        # Registra metadados da carga
        self._log_load_metadata('correlated_data', results)
        
        self.logger.info(f"Carga de correlações concluída: {results}")
        return results
    
    def load_regional_data(self, regional_data: List[Dict[str, Any]], 
                         batch_size: int = 100) -> Dict[str, Any]:
        """
        Carrega dados regionais agregados no MongoDB.
        
        Args:
            regional_data (List[Dict]): Lista de dados regionais
            batch_size (int): Tamanho do lote para inserção
            
        Returns:
            Dict: Resultado da operação de carga
        """
        self.logger.info(f"Iniciando carga de {len(regional_data)} dados regionais")
        
        collection = self.mongo_manager.get_collection(self.collections['regional_engagement'])
        
        # Cria índices se não existirem
        self._ensure_regional_data_indexes(collection)
        
        # Processa em lotes
        results = {
            'total_processed': 0,
            'inserted': 0,
            'updated': 0,
            'errors': 0,
            'error_details': []
        }
        
        for i in range(0, len(regional_data), batch_size):
            batch = regional_data[i:i + batch_size]
            batch_result = self._load_batch_regional_data(collection, batch)
            
            # Atualiza resultados
            results['total_processed'] += batch_result['processed']
            results['inserted'] += batch_result['inserted']
            results['updated'] += batch_result['updated']
            results['errors'] += batch_result['errors']
            results['error_details'].extend(batch_result['error_details'])
            
            self.logger.info(f"Lote processado: {i + len(batch)}/{len(regional_data)}")
        
        # Registra metadados da carga
        self._log_load_metadata('regional_engagement', results)
        
        self.logger.info(f"Carga de dados regionais concluída: {results}")
        return results
    
    def load_all_data(self, transformed_data: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
        """
        Carrega todos os tipos de dados transformados.
        
        Args:
            transformed_data (Dict): Dicionário com todos os dados transformados
            
        Returns:
            Dict: Resultado consolidado de todas as operações
        """
        self.logger.info("Iniciando carga completa de dados")
        
        all_results = {
            'start_time': datetime.utcnow().isoformat(),
            'spotify_tracks': {},
            'spotify_features': {},
            'youtube_videos': {},
            'correlated_data': {},
            'regional_data': {},
            'total_errors': 0,
            'success': True
        }
        
        try:
            # Carrega faixas do Spotify
            if 'spotify_tracks' in transformed_data:
                all_results['spotify_tracks'] = self.load_spotify_tracks(
                    transformed_data['spotify_tracks']
                )
                all_results['total_errors'] += all_results['spotify_tracks']['errors']
            
            # Carrega características do Spotify
            if 'spotify_features' in transformed_data:
                all_results['spotify_features'] = self.load_spotify_features(
                    transformed_data['spotify_features']
                )
                all_results['total_errors'] += all_results['spotify_features']['errors']
            
            # Carrega vídeos do YouTube
            if 'youtube_videos' in transformed_data:
                all_results['youtube_videos'] = self.load_youtube_videos(
                    transformed_data['youtube_videos']
                )
                all_results['total_errors'] += all_results['youtube_videos']['errors']
            
            # Carrega dados correlacionados
            if 'correlated_data' in transformed_data:
                all_results['correlated_data'] = self.load_correlated_data(
                    transformed_data['correlated_data']
                )
                all_results['total_errors'] += all_results['correlated_data']['errors']
            
            # Carrega dados regionais
            if 'regional_data' in transformed_data:
                all_results['regional_data'] = self.load_regional_data(
                    transformed_data['regional_data']
                )
                all_results['total_errors'] += all_results['regional_data']['errors']
            
        except Exception as e:
            self.logger.error(f"Erro durante carga completa: {e}")
            all_results['success'] = False
            all_results['error'] = str(e)
        
        all_results['end_time'] = datetime.utcnow().isoformat()
        
        # Registra metadados da carga completa
        self._log_load_metadata('full_pipeline', all_results)
        
        self.logger.info(f"Carga completa finalizada: {all_results['success']}")
        return all_results
    
    # Métodos auxiliares para carga em lotes
    
    def _load_batch_spotify_tracks(self, collection, batch: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Carrega lote de faixas do Spotify."""
        result = {'processed': 0, 'inserted': 0, 'updated': 0, 'errors': 0, 'error_details': []}
        
        operations = []
        for track in batch:
            try:
                operation = pymongo.UpdateOne(
                    {'track_id': track['track_id']},
                    {'$set': track},
                    upsert=True
                )
                operations.append(operation)
                result['processed'] += 1
                
            except Exception as e:
                result['errors'] += 1
                result['error_details'].append({
                    'track_id': track.get('track_id', 'unknown'),
                    'error': str(e)
                })
        
        if operations:
            try:
                bulk_result = collection.bulk_write(operations, ordered=False)
                result['inserted'] = bulk_result.upserted_count
                result['updated'] = bulk_result.modified_count
                
            except BulkWriteError as e:
                result['errors'] += len(e.details.get('writeErrors', []))
                for error in e.details.get('writeErrors', []):
                    result['error_details'].append({
                        'operation_index': error.get('index'),
                        'error': error.get('errmsg')
                    })
        
        return result
    
    def _load_batch_spotify_features(self, collection, batch: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Carrega lote de características do Spotify."""
        result = {'processed': 0, 'inserted': 0, 'updated': 0, 'errors': 0, 'error_details': []}
        
        operations = []
        for feature in batch:
            try:
                operation = pymongo.UpdateOne(
                    {'track_id': feature['track_id']},
                    {'$set': feature},
                    upsert=True
                )
                operations.append(operation)
                result['processed'] += 1
                
            except Exception as e:
                result['errors'] += 1
                result['error_details'].append({
                    'track_id': feature.get('track_id', 'unknown'),
                    'error': str(e)
                })
        
        if operations:
            try:
                bulk_result = collection.bulk_write(operations, ordered=False)
                result['inserted'] = bulk_result.upserted_count
                result['updated'] = bulk_result.modified_count
                
            except BulkWriteError as e:
                result['errors'] += len(e.details.get('writeErrors', []))
                for error in e.details.get('writeErrors', []):
                    result['error_details'].append({
                        'operation_index': error.get('index'),
                        'error': error.get('errmsg')
                    })
        
        return result
    
    def _load_batch_youtube_videos(self, collection, batch: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Carrega lote de vídeos do YouTube."""
        result = {'processed': 0, 'inserted': 0, 'updated': 0, 'errors': 0, 'error_details': []}
        
        operations = []
        for video in batch:
            try:
                operation = pymongo.UpdateOne(
                    {'video_id': video['video_id']},
                    {'$set': video},
                    upsert=True
                )
                operations.append(operation)
                result['processed'] += 1
                
            except Exception as e:
                result['errors'] += 1
                result['error_details'].append({
                    'video_id': video.get('video_id', 'unknown'),
                    'error': str(e)
                })
        
        if operations:
            try:
                bulk_result = collection.bulk_write(operations, ordered=False)
                result['inserted'] = bulk_result.upserted_count
                result['updated'] = bulk_result.modified_count
                
            except BulkWriteError as e:
                result['errors'] += len(e.details.get('writeErrors', []))
                for error in e.details.get('writeErrors', []):
                    result['error_details'].append({
                        'operation_index': error.get('index'),
                        'error': error.get('errmsg')
                    })
        
        return result
    
    def _load_batch_correlated_data(self, collection, batch: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Carrega lote de dados correlacionados."""
        result = {'processed': 0, 'inserted': 0, 'updated': 0, 'errors': 0, 'error_details': []}
        
        operations = []
        for correlation in batch:
            try:
                operation = pymongo.UpdateOne(
                    {'correlation_id': correlation['correlation_id']},
                    {'$set': correlation},
                    upsert=True
                )
                operations.append(operation)
                result['processed'] += 1
                
            except Exception as e:
                result['errors'] += 1
                result['error_details'].append({
                    'correlation_id': correlation.get('correlation_id', 'unknown'),
                    'error': str(e)
                })
        
        if operations:
            try:
                bulk_result = collection.bulk_write(operations, ordered=False)
                result['inserted'] = bulk_result.upserted_count
                result['updated'] = bulk_result.modified_count
                
            except BulkWriteError as e:
                result['errors'] += len(e.details.get('writeErrors', []))
                for error in e.details.get('writeErrors', []):
                    result['error_details'].append({
                        'operation_index': error.get('index'),
                        'error': error.get('errmsg')
                    })
        
        return result
    
    def _load_batch_regional_data(self, collection, batch: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Carrega lote de dados regionais."""
        result = {'processed': 0, 'inserted': 0, 'updated': 0, 'errors': 0, 'error_details': []}
        
        operations = []
        for region_data in batch:
            try:
                operation = pymongo.UpdateOne(
                    {'region_code': region_data['region_code']},
                    {'$set': region_data},
                    upsert=True
                )
                operations.append(operation)
                result['processed'] += 1
                
            except Exception as e:
                result['errors'] += 1
                result['error_details'].append({
                    'region_code': region_data.get('region_code', 'unknown'),
                    'error': str(e)
                })
        
        if operations:
            try:
                bulk_result = collection.bulk_write(operations, ordered=False)
                result['inserted'] = bulk_result.upserted_count
                result['updated'] = bulk_result.modified_count
                
            except BulkWriteError as e:
                result['errors'] += len(e.details.get('writeErrors', []))
                for error in e.details.get('writeErrors', []):
                    result['error_details'].append({
                        'operation_index': error.get('index'),
                        'error': error.get('errmsg')
                    })
        
        return result
    
    # Métodos para criação de índices
    
    def _ensure_spotify_tracks_indexes(self, collection):
        """Cria índices para coleção de faixas do Spotify."""
        try:
            collection.create_index('track_id', unique=True)
            collection.create_index('artist_name')
            collection.create_index('popularity')
            collection.create_index('release_date')
            collection.create_index('source_playlist_id')
            collection.create_index([('artist_name', 1), ('name', 1)])
            
        except Exception as e:
            self.logger.warning(f"Erro ao criar índices para spotify_tracks: {e}")
    
    def _ensure_spotify_features_indexes(self, collection):
        """Cria índices para coleção de características do Spotify."""
        try:
            collection.create_index('track_id', unique=True)
            collection.create_index('danceability')
            collection.create_index('energy')
            collection.create_index('valence')
            collection.create_index('tempo')
            
        except Exception as e:
            self.logger.warning(f"Erro ao criar índices para spotify_features: {e}")
    
    def _ensure_youtube_videos_indexes(self, collection):
        """Cria índices para coleção de vídeos do YouTube."""
        try:
            collection.create_index('video_id', unique=True)
            collection.create_index('channel_id')
            collection.create_index('view_count')
            collection.create_index('published_date')
            collection.create_index('source_region')
            collection.create_index([('search_artist', 1), ('search_track', 1)])
            
        except Exception as e:
            self.logger.warning(f"Erro ao criar índices para youtube_videos: {e}")
    
    def _ensure_correlated_data_indexes(self, collection):
        """Cria índices para coleção de dados correlacionados."""
        try:
            collection.create_index('correlation_id', unique=True)
            collection.create_index('track_id')
            collection.create_index('video_id')
            collection.create_index('similarity_score')
            collection.create_index([('track_id', 1), ('video_id', 1)])
            
        except Exception as e:
            self.logger.warning(f"Erro ao criar índices para correlated_data: {e}")
    
    def _ensure_regional_data_indexes(self, collection):
        """Cria índices para coleção de dados regionais."""
        try:
            collection.create_index('region_code', unique=True)
            collection.create_index('total_views')
            collection.create_index('avg_engagement_rate')
            collection.create_index('created_at')
            
        except Exception as e:
            self.logger.warning(f"Erro ao criar índices para regional_engagement: {e}")
    
    def _log_load_metadata(self, operation_type: str, results: Dict[str, Any]):
        """Registra metadados da operação de carga."""
        try:
            metadata_collection = self.mongo_manager.get_collection(self.collections['load_metadata'])
            
            metadata = {
                'operation_type': operation_type,
                'timestamp': datetime.utcnow().isoformat(),
                'results': results,
                'success': results.get('success', results.get('errors', 0) == 0)
            }
            
            metadata_collection.insert_one(metadata)
            
        except Exception as e:
            self.logger.error(f"Erro ao registrar metadados: {e}")
    
    def get_load_statistics(self) -> Dict[str, Any]:
        """
        Obtém estatísticas das operações de carga.
        
        Returns:
            Dict: Estatísticas consolidadas
        """
        try:
            stats = {}
            
            for collection_name in self.collections.values():
                if collection_name == 'load_metadata':
                    continue
                
                collection = self.mongo_manager.get_collection(collection_name)
                count = collection.count_documents({})
                stats[collection_name] = {
                    'total_documents': count,
                    'last_updated': None
                }
                
                # Busca último documento atualizado
                last_doc = collection.find_one(
                    sort=[('transformed_at', -1)]
                )
                if last_doc:
                    stats[collection_name]['last_updated'] = last_doc.get('transformed_at')
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Erro ao obter estatísticas: {e}")
            return {}