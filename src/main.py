"""
Pipeline ETL principal para integração Spotify-YouTube.
Orquestra a extração, transformação e carga de dados.
"""

import os
import sys
import logging
import argparse
from typing import Dict, Any, Optional
from datetime import datetime
from dotenv import load_dotenv

# Adiciona o diretório atual ao path para imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from api_clients.spotify_client import SpotifyClient
from api_clients.youtube_client import YouTubeClient
from database.mongodb_manager import MongoDBManager
from etl.extract import DataExtractor
from etl.transform import DataTransformer
from etl.load import DataLoader


class ETLPipeline:
    """
    Classe principal que orquestra todo o pipeline ETL.
    """
    
    def __init__(self, config_path: str = ".env"):
        """
        Inicializa o pipeline ETL.
        
        Args:
            config_path (str): Caminho para o arquivo de configuração
        """
        # Carrega variáveis de ambiente
        load_dotenv(config_path)
        
        # Configura logging
        self._setup_logging()
        
        self.logger = logging.getLogger(__name__)
        self.logger.info("Inicializando pipeline ETL")
        
        # Inicializa componentes
        self._initialize_components()
        
        # Configurações do pipeline
        self.config = self._load_config()
        
    def _setup_logging(self):
        """Configura o sistema de logging."""
        log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        
        logging.basicConfig(
            level=getattr(logging, log_level),
            format=log_format,
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler('etl_pipeline.log')
            ]
        )
    
    def _initialize_components(self):
        """Inicializa todos os componentes do pipeline."""
        try:
            # Clientes de API
            spotify_client_id = os.getenv('SPOTIFY_CLIENT_ID')
            spotify_client_secret = os.getenv('SPOTIFY_CLIENT_SECRET')
            youtube_api_key = os.getenv('YOUTUBE_API_KEY')
            
            if not spotify_client_id or not spotify_client_secret:
                raise ValueError("Credenciais do Spotify não encontradas no .env")
            
            self.spotify_client = SpotifyClient(spotify_client_id, spotify_client_secret)
            
            # Autentica o cliente Spotify
            if not self.spotify_client.authenticate():
                raise Exception("Falha na autenticação do Spotify")
            
            # Inicializa YouTube apenas se a chave estiver disponível
            self.youtube_client = None
            if youtube_api_key:
                try:
                    self.youtube_client = YouTubeClient(youtube_api_key)
                    if not self.youtube_client.authenticate():
                        self.logger.warning("Falha na autenticação do YouTube - continuando apenas com Spotify")
                        self.youtube_client = None
                except Exception as e:
                    self.logger.warning(f"Erro ao inicializar YouTube: {e} - continuando apenas com Spotify")
                    self.youtube_client = None
            
            # Gerenciador de banco de dados
            mongo_uri = os.getenv('MONGO_URI', 'mongodb://localhost:27017/')
            db_name = os.getenv('MONGO_DB_NAME', 'spotify_youtube_analytics')
            self.mongo_manager = MongoDBManager(mongo_uri, db_name)
            
            # Componentes ETL
            self.extractor = DataExtractor(self.spotify_client, self.youtube_client)
            self.transformer = DataTransformer()
            self.loader = DataLoader(self.mongo_manager)
            
            self.logger.info("Componentes inicializados com sucesso")
            
        except Exception as e:
            self.logger.error(f"Erro ao inicializar componentes: {e}")
            raise
    
    def _load_config(self) -> Dict[str, Any]:
        """Carrega configurações do pipeline."""
        return {
            # Configurações do Spotify
            'spotify_playlists': os.getenv('SPOTIFY_PLAYLIST_IDS', '').split(','),
            'spotify_countries': os.getenv('SPOTIFY_COUNTRIES', 'US,BR,GB').split(','),
            'max_tracks_per_playlist': int(os.getenv('MAX_TRACKS_PER_PLAYLIST', '50')),
            
            # Configurações do YouTube
            'youtube_regions': os.getenv('YOUTUBE_REGION_CODES', 'US,BR,GB').split(','),
            'max_videos_per_region': int(os.getenv('MAX_VIDEOS_PER_REGION', '50')),
            'youtube_categories': os.getenv('YOUTUBE_CATEGORIES', '10').split(','),  # 10 = Music
            
            # Configurações do pipeline
            'enable_audio_features': os.getenv('ENABLE_AUDIO_FEATURES', 'true').lower() == 'true',
            'enable_correlation': os.getenv('ENABLE_CORRELATION', 'true').lower() == 'true',
            'enable_regional_analysis': os.getenv('ENABLE_REGIONAL_ANALYSIS', 'true').lower() == 'true',
            'batch_size': int(os.getenv('BATCH_SIZE', '1000')),
            
            # Rate limiting
            'spotify_rate_limit': float(os.getenv('SPOTIFY_RATE_LIMIT', '1.0')),
            'youtube_rate_limit': float(os.getenv('YOUTUBE_RATE_LIMIT', '1.0')),
        }
    
    def run_full_pipeline(self) -> Dict[str, Any]:
        """
        Executa o pipeline ETL completo.
        
        Returns:
            Dict: Resultado da execução do pipeline
        """
        self.logger.info("Iniciando execução do pipeline ETL completo")
        
        pipeline_result = {
            'start_time': datetime.utcnow().isoformat(),
            'success': False,
            'stages': {
                'extraction': {'success': False, 'data': {}},
                'transformation': {'success': False, 'data': {}},
                'loading': {'success': False, 'results': {}}
            },
            'errors': [],
            'statistics': {}
        }
        
        try:
            with self.mongo_manager:
                # Etapa 1: Extração
                self.logger.info("=== INICIANDO ETAPA DE EXTRAÇÃO ===")
                extraction_result = self._run_extraction()
                pipeline_result['stages']['extraction'] = extraction_result
                
                if not extraction_result['success']:
                    raise Exception("Falha na etapa de extração")
                
                # Etapa 2: Transformação
                self.logger.info("=== INICIANDO ETAPA DE TRANSFORMAÇÃO ===")
                transformation_result = self._run_transformation(extraction_result['data'])
                pipeline_result['stages']['transformation'] = transformation_result
                
                if not transformation_result['success']:
                    raise Exception("Falha na etapa de transformação")
                
                # Etapa 3: Carga
                self.logger.info("=== INICIANDO ETAPA DE CARGA ===")
                loading_result = self._run_loading(transformation_result['data'])
                pipeline_result['stages']['loading'] = loading_result
                
                if not loading_result['success']:
                    raise Exception("Falha na etapa de carga")
                
                # Pipeline executado com sucesso
                pipeline_result['success'] = True
                pipeline_result['statistics'] = self._generate_pipeline_statistics()
                
        except Exception as e:
            self.logger.error(f"Erro durante execução do pipeline: {e}")
            pipeline_result['errors'].append(str(e))
        
        pipeline_result['end_time'] = datetime.utcnow().isoformat()
        
        # Log do resultado final
        self._log_pipeline_result(pipeline_result)
        
        return pipeline_result
    
    def _run_extraction(self) -> Dict[str, Any]:
        """Executa a etapa de extração de dados."""
        extraction_config = {
            'spotify_playlist_ids': [pid.strip() for pid in self.config['spotify_playlists'] if pid.strip()],
            'youtube_region_codes': self.config['youtube_regions'],
            'youtube_max_results_per_region': self.config['max_videos_per_region'],
            'max_music_search_queries': self.config['max_tracks_per_playlist'],
            'youtube_max_results_per_query': 5,
            'extract_youtube_music': True
        }
        
        try:
            extracted_data = self.extractor.extract_all_data(extraction_config)
            
            return {
                'success': True,
                'data': extracted_data,
                'statistics': {
                    'spotify_tracks': len(extracted_data.get('spotify_tracks', [])),
                    'spotify_features': len(extracted_data.get('spotify_features', [])),
                    'youtube_videos': len(extracted_data.get('youtube_videos', [])),
                    'youtube_music_videos': len(extracted_data.get('youtube_music_videos', []))
                }
            }
            
        except Exception as e:
            self.logger.error(f"Erro na extração: {e}")
            return {
                'success': False,
                'error': str(e),
                'data': {}
            }
    
    def _run_transformation(self, extracted_data: Dict[str, Any]) -> Dict[str, Any]:
        """Executa a etapa de transformação de dados."""
        try:
            transformed_data = {}
            
            # Transforma faixas do Spotify
            if 'spotify_tracks' in extracted_data and extracted_data['spotify_tracks'].get('data'):
                self.logger.info("Transformando faixas do Spotify")
                transformed_data['spotify_tracks'] = self.transformer.transform_spotify_tracks(
                    extracted_data['spotify_tracks']['data']
                )
            
            # Transforma características de áudio
            if 'spotify_features' in extracted_data and self.config['enable_audio_features'] and extracted_data['spotify_features'].get('data'):
                self.logger.info("Transformando características de áudio")
                transformed_data['spotify_features'] = self.transformer.transform_spotify_features(
                    extracted_data['spotify_features']['data']
                )
            
            # Transforma vídeos do YouTube
            youtube_videos = []
            if 'youtube_videos' in extracted_data and extracted_data['youtube_videos'].get('data'):
                youtube_videos.extend(extracted_data['youtube_videos']['data'])
            if 'youtube_music_videos' in extracted_data and extracted_data['youtube_music_videos'].get('data'):
                youtube_videos.extend(extracted_data['youtube_music_videos']['data'])
            
            if youtube_videos:
                self.logger.info("Transformando vídeos do YouTube")
                transformed_data['youtube_videos'] = self.transformer.transform_youtube_videos(
                    youtube_videos
                )
            
            # Correlaciona dados entre plataformas
            if (self.config['enable_correlation'] and 
                'spotify_tracks' in transformed_data and 
                'youtube_videos' in transformed_data):
                
                self.logger.info("Correlacionando dados Spotify-YouTube")
                transformed_data['correlated_data'] = self.transformer.correlate_spotify_youtube_data(
                    transformed_data['spotify_tracks'],
                    transformed_data['youtube_videos']
                )
            
            # Agrega dados regionais
            if (self.config['enable_regional_analysis'] and 
                'youtube_videos' in transformed_data):
                
                self.logger.info("Agregando dados regionais")
                transformed_data['regional_data'] = self.transformer.aggregate_regional_data(
                    transformed_data['youtube_videos']
                )
            
            return {
                'success': True,
                'data': transformed_data,
                'statistics': {
                    'spotify_tracks': len(transformed_data.get('spotify_tracks', [])),
                    'spotify_features': len(transformed_data.get('spotify_features', [])),
                    'youtube_videos': len(transformed_data.get('youtube_videos', [])),
                    'correlated_data': len(transformed_data.get('correlated_data', [])),
                    'regional_data': len(transformed_data.get('regional_data', []))
                }
            }
            
        except Exception as e:
            self.logger.error(f"Erro na transformação: {e}")
            return {
                'success': False,
                'error': str(e),
                'data': {}
            }
    
    def _run_loading(self, transformed_data: Dict[str, Any]) -> Dict[str, Any]:
        """Executa a etapa de carga de dados."""
        try:
            loading_results = self.loader.load_all_data(transformed_data)
            
            return {
                'success': loading_results['success'],
                'results': loading_results,
                'statistics': {
                    'total_errors': loading_results['total_errors'],
                    'collections_updated': len([k for k, v in loading_results.items() 
                                              if isinstance(v, dict) and v.get('inserted', 0) > 0])
                }
            }
            
        except Exception as e:
            self.logger.error(f"Erro na carga: {e}")
            return {
                'success': False,
                'error': str(e),
                'results': {}
            }
    
    def _generate_pipeline_statistics(self) -> Dict[str, Any]:
        """Gera estatísticas do pipeline."""
        try:
            return self.loader.get_load_statistics()
        except Exception as e:
            self.logger.error(f"Erro ao gerar estatísticas: {e}")
            return {}
    
    def _log_pipeline_result(self, result: Dict[str, Any]):
        """Registra o resultado do pipeline."""
        if result['success']:
            self.logger.info("=== PIPELINE ETL EXECUTADO COM SUCESSO ===")
            
            # Log das estatísticas
            for stage, data in result['stages'].items():
                if 'statistics' in data:
                    self.logger.info(f"{stage.upper()}: {data['statistics']}")
        else:
            self.logger.error("=== PIPELINE ETL FALHOU ===")
            for error in result['errors']:
                self.logger.error(f"Erro: {error}")
    
    def run_extraction_only(self) -> Dict[str, Any]:
        """Executa apenas a etapa de extração."""
        self.logger.info("Executando apenas extração de dados")
        return self._run_extraction()
    
    def run_spotify_only(self) -> Dict[str, Any]:
        """Executa pipeline apenas para dados do Spotify."""
        self.logger.info("Executando pipeline apenas para Spotify")
        
        # Temporariamente desabilita YouTube
        original_regions = self.config['youtube_regions']
        self.config['youtube_regions'] = []
        
        try:
            result = self.run_full_pipeline()
        finally:
            # Restaura configuração original
            self.config['youtube_regions'] = original_regions
        
        return result
    
    def run_youtube_only(self) -> Dict[str, Any]:
        """Executa pipeline apenas para dados do YouTube."""
        self.logger.info("Executando pipeline apenas para YouTube")
        
        # Temporariamente desabilita Spotify
        original_playlists = self.config['spotify_playlists']
        self.config['spotify_playlists'] = []
        
        try:
            result = self.run_full_pipeline()
        finally:
            # Restaura configuração original
            self.config['spotify_playlists'] = original_playlists
        
        return result
    
    def get_pipeline_status(self) -> Dict[str, Any]:
        """Obtém status atual do pipeline."""
        try:
            with self.mongo_manager:
                stats = self.loader.get_load_statistics()
                
                return {
                    'database_connected': True,
                    'collections': stats,
                    'last_run': self._get_last_pipeline_run(),
                    'configuration': {
                        'spotify_playlists_count': len([p for p in self.config['spotify_playlists'] if p]),
                        'youtube_regions_count': len(self.config['youtube_regions']),
                        'features_enabled': {
                            'audio_features': self.config['enable_audio_features'],
                            'correlation': self.config['enable_correlation'],
                            'regional_analysis': self.config['enable_regional_analysis']
                        }
                    }
                }
        except Exception as e:
            return {
                'database_connected': False,
                'error': str(e)
            }
    
    def _get_last_pipeline_run(self) -> Optional[Dict[str, Any]]:
        """Obtém informações da última execução do pipeline."""
        try:
            metadata_collection = self.mongo_manager.get_collection('load_metadata')
            last_run = metadata_collection.find_one(
                {'operation_type': 'full_pipeline'},
                sort=[('timestamp', -1)]
            )
            
            if last_run:
                return {
                    'timestamp': last_run['timestamp'],
                    'success': last_run['success'],
                    'total_errors': last_run.get('results', {}).get('total_errors', 0)
                }
            
            return None
            
        except Exception as e:
            self.logger.error(f"Erro ao obter última execução: {e}")
            return None


def main():
    """Função principal para execução via linha de comando."""
    parser = argparse.ArgumentParser(description='Pipeline ETL Spotify-YouTube')
    parser.add_argument('--mode', choices=['full', 'extract', 'spotify', 'youtube', 'status'], 
                       default='full', help='Modo de execução do pipeline')
    parser.add_argument('--config', default='.env', help='Arquivo de configuração')
    
    args = parser.parse_args()
    
    try:
        # Inicializa pipeline
        pipeline = ETLPipeline(args.config)
        
        # Executa modo selecionado
        if args.mode == 'full':
            result = pipeline.run_full_pipeline()
        elif args.mode == 'extract':
            result = pipeline.run_extraction_only()
        elif args.mode == 'spotify':
            result = pipeline.run_spotify_only()
        elif args.mode == 'youtube':
            result = pipeline.run_youtube_only()
        elif args.mode == 'status':
            result = pipeline.get_pipeline_status()
        
        # Exibe resultado
        print("\n" + "="*50)
        print("RESULTADO DO PIPELINE")
        print("="*50)
        
        if args.mode == 'status':
            print(f"Database conectado: {result.get('database_connected', False)}")
            if 'collections' in result:
                for collection, stats in result['collections'].items():
                    print(f"{collection}: {stats['total_documents']} documentos")
        else:
            print(f"Sucesso: {result.get('success', False)}")
            if 'statistics' in result:
                print(f"Estatísticas: {result['statistics']}")
        
        print("="*50)
        
        # Exit code baseado no sucesso
        sys.exit(0 if result.get('success', False) else 1)
        
    except Exception as e:
        print(f"Erro fatal: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()