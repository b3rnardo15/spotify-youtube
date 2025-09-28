"""
Módulo de extração de dados do pipeline ETL.
Responsável por coletar dados das APIs do Spotify e YouTube.
"""

import os
import sys
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
import time

# Adiciona o diretório pai ao path para imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api_clients.spotify_client import SpotifyClient
from api_clients.youtube_client import YouTubeClient


class DataExtractor:
    """
    Classe responsável pela extração de dados das APIs externas.
    """
    
    def __init__(self, spotify_client: SpotifyClient, youtube_client: YouTubeClient):
        """
        Inicializa o extrator de dados.
        
        Args:
            spotify_client (SpotifyClient): Cliente autenticado do Spotify
            youtube_client (YouTubeClient): Cliente autenticado do YouTube
        """
        self.spotify_client = spotify_client
        self.youtube_client = youtube_client
        self.logger = logging.getLogger(__name__)
        
    def extract_spotify_playlist_data(self, playlist_ids: List[str]) -> Dict[str, Any]:
        """
        Extrai dados de playlists do Spotify.
        
        Args:
            playlist_ids (List[str]): Lista de IDs das playlists
            
        Returns:
            Dict: Dados extraídos com metadados
        """
        extraction_start = datetime.utcnow()
        all_tracks = []
        extraction_metadata = {
            'source': 'spotify_playlists',
            'extraction_timestamp': extraction_start.isoformat(),
            'playlist_ids': playlist_ids,
            'total_tracks': 0,
            'successful_playlists': 0,
            'failed_playlists': 0,
            'errors': []
        }
        
        self.logger.info(f"Iniciando extração de {len(playlist_ids)} playlists do Spotify")
        
        for playlist_id in playlist_ids:
            try:
                self.logger.info(f"Extraindo playlist: {playlist_id}")
                tracks = self.spotify_client.get_playlist_tracks(playlist_id)
                
                if tracks:
                    # Adiciona metadados de extração a cada track
                    for track in tracks:
                        track['extraction_timestamp'] = extraction_start.isoformat()
                        track['source_playlist_id'] = playlist_id
                        track['data_source'] = 'spotify'
                    
                    all_tracks.extend(tracks)
                    extraction_metadata['successful_playlists'] += 1
                    self.logger.info(f"Extraídas {len(tracks)} faixas da playlist {playlist_id}")
                else:
                    extraction_metadata['failed_playlists'] += 1
                    error_msg = f"Falha ao extrair playlist {playlist_id}"
                    extraction_metadata['errors'].append(error_msg)
                    self.logger.warning(error_msg)
                
                # Rate limiting entre playlists
                time.sleep(0.5)
                
            except Exception as e:
                extraction_metadata['failed_playlists'] += 1
                error_msg = f"Erro ao extrair playlist {playlist_id}: {str(e)}"
                extraction_metadata['errors'].append(error_msg)
                self.logger.error(error_msg)
        
        extraction_metadata['total_tracks'] = len(all_tracks)
        extraction_metadata['extraction_duration'] = (datetime.utcnow() - extraction_start).total_seconds()
        
        self.logger.info(f"Extração do Spotify concluída: {len(all_tracks)} faixas de {extraction_metadata['successful_playlists']} playlists")
        
        return {
            'data': all_tracks,
            'metadata': extraction_metadata
        }
    
    def extract_spotify_audio_features(self, track_ids: List[str]) -> Dict[str, Any]:
        """
        Extrai características de áudio das faixas do Spotify.
        
        Args:
            track_ids (List[str]): Lista de IDs das faixas
            
        Returns:
            Dict: Características de áudio com metadados
        """
        extraction_start = datetime.utcnow()
        extraction_metadata = {
            'source': 'spotify_audio_features',
            'extraction_timestamp': extraction_start.isoformat(),
            'requested_tracks': len(track_ids),
            'successful_features': 0,
            'errors': []
        }
        
        self.logger.info(f"Extraindo características de áudio para {len(track_ids)} faixas")
        
        try:
            features = self.spotify_client.get_multiple_tracks_features(track_ids)
            
            if features:
                # Adiciona metadados de extração
                for feature in features:
                    feature['extraction_timestamp'] = extraction_start.isoformat()
                    feature['data_source'] = 'spotify_features'
                
                extraction_metadata['successful_features'] = len(features)
            else:
                features = []
                extraction_metadata['errors'].append("Falha ao obter características de áudio")
            
        except Exception as e:
            features = []
            error_msg = f"Erro ao extrair características de áudio: {str(e)}"
            extraction_metadata['errors'].append(error_msg)
            self.logger.error(error_msg)
        
        extraction_metadata['extraction_duration'] = (datetime.utcnow() - extraction_start).total_seconds()
        
        self.logger.info(f"Extração de características concluída: {len(features)} características obtidas")
        
        return {
            'data': features,
            'metadata': extraction_metadata
        }
    
    def extract_youtube_popular_videos(self, region_codes: List[str], max_results_per_region: int = 50) -> Dict[str, Any]:
        """
        Extrai vídeos populares do YouTube por região.
        
        Args:
            region_codes (List[str]): Lista de códigos de região
            max_results_per_region (int): Máximo de resultados por região
            
        Returns:
            Dict: Dados extraídos com metadados
        """
        extraction_start = datetime.utcnow()
        all_videos = []
        extraction_metadata = {
            'source': 'youtube_popular',
            'extraction_timestamp': extraction_start.isoformat(),
            'region_codes': region_codes,
            'max_results_per_region': max_results_per_region,
            'total_videos': 0,
            'successful_regions': 0,
            'failed_regions': 0,
            'errors': []
        }
        
        self.logger.info(f"Extraindo vídeos populares de {len(region_codes)} regiões")
        
        for region_code in region_codes:
            try:
                self.logger.info(f"Extraindo vídeos populares da região: {region_code}")
                videos = self.youtube_client.get_popular_videos(
                    region_code=region_code,
                    max_results=max_results_per_region
                )
                
                if videos:
                    # Adiciona metadados de extração
                    for video in videos:
                        video['extraction_timestamp'] = extraction_start.isoformat()
                        video['source_region'] = region_code
                        video['data_source'] = 'youtube'
                    
                    all_videos.extend(videos)
                    extraction_metadata['successful_regions'] += 1
                    self.logger.info(f"Extraídos {len(videos)} vídeos da região {region_code}")
                else:
                    extraction_metadata['failed_regions'] += 1
                    error_msg = f"Falha ao extrair vídeos da região {region_code}"
                    extraction_metadata['errors'].append(error_msg)
                    self.logger.warning(error_msg)
                
                # Rate limiting entre regiões
                time.sleep(1.0)
                
            except Exception as e:
                extraction_metadata['failed_regions'] += 1
                error_msg = f"Erro ao extrair vídeos da região {region_code}: {str(e)}"
                extraction_metadata['errors'].append(error_msg)
                self.logger.error(error_msg)
        
        extraction_metadata['total_videos'] = len(all_videos)
        extraction_metadata['extraction_duration'] = (datetime.utcnow() - extraction_start).total_seconds()
        
        self.logger.info(f"Extração do YouTube concluída: {len(all_videos)} vídeos de {extraction_metadata['successful_regions']} regiões")
        
        return {
            'data': all_videos,
            'metadata': extraction_metadata
        }
    
    def extract_youtube_music_videos(self, search_queries: List[Dict[str, str]], max_results_per_query: int = 10) -> Dict[str, Any]:
        """
        Extrai vídeos musicais do YouTube baseado em queries de busca.
        
        Args:
            search_queries (List[Dict]): Lista de queries com 'artist' e 'track'
            max_results_per_query (int): Máximo de resultados por query
            
        Returns:
            Dict: Dados extraídos com metadados
        """
        extraction_start = datetime.utcnow()
        all_videos = []
        extraction_metadata = {
            'source': 'youtube_music_search',
            'extraction_timestamp': extraction_start.isoformat(),
            'total_queries': len(search_queries),
            'max_results_per_query': max_results_per_query,
            'total_videos': 0,
            'successful_queries': 0,
            'failed_queries': 0,
            'errors': []
        }
        
        self.logger.info(f"Buscando vídeos musicais para {len(search_queries)} queries")
        
        for query_data in search_queries:
            try:
                artist = query_data.get('artist', '')
                track = query_data.get('track', '')
                
                if not artist or not track:
                    extraction_metadata['failed_queries'] += 1
                    error_msg = f"Query inválida: {query_data}"
                    extraction_metadata['errors'].append(error_msg)
                    continue
                
                self.logger.info(f"Buscando: {artist} - {track}")
                videos = self.youtube_client.search_music_videos(
                    artist=artist,
                    track=track,
                    max_results=max_results_per_query
                )
                
                if videos:
                    # Adiciona metadados de extração
                    for video in videos:
                        video['extraction_timestamp'] = extraction_start.isoformat()
                        video['search_artist'] = artist
                        video['search_track'] = track
                        video['data_source'] = 'youtube_music_search'
                    
                    all_videos.extend(videos)
                    extraction_metadata['successful_queries'] += 1
                    self.logger.info(f"Encontrados {len(videos)} vídeos para {artist} - {track}")
                else:
                    extraction_metadata['failed_queries'] += 1
                    error_msg = f"Nenhum vídeo encontrado para {artist} - {track}"
                    extraction_metadata['errors'].append(error_msg)
                    self.logger.warning(error_msg)
                
                # Rate limiting entre queries
                time.sleep(0.5)
                
            except Exception as e:
                extraction_metadata['failed_queries'] += 1
                error_msg = f"Erro ao buscar vídeos para {query_data}: {str(e)}"
                extraction_metadata['errors'].append(error_msg)
                self.logger.error(error_msg)
        
        extraction_metadata['total_videos'] = len(all_videos)
        extraction_metadata['extraction_duration'] = (datetime.utcnow() - extraction_start).total_seconds()
        
        self.logger.info(f"Busca de vídeos musicais concluída: {len(all_videos)} vídeos de {extraction_metadata['successful_queries']} queries")
        
        return {
            'data': all_videos,
            'metadata': extraction_metadata
        }
    
    def extract_all_data(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Executa extração completa de dados conforme configuração.
        
        Args:
            config (Dict): Configuração da extração
            
        Returns:
            Dict: Todos os dados extraídos com metadados
        """
        extraction_start = datetime.utcnow()
        results = {
            'extraction_timestamp': extraction_start.isoformat(),
            'spotify_tracks': {},
            'spotify_features': {},
            'youtube_popular': {},
            'youtube_music': {},
            'extraction_summary': {
                'total_duration': 0,
                'total_spotify_tracks': 0,
                'total_youtube_videos': 0,
                'errors': []
            }
        }
        
        self.logger.info("Iniciando extração completa de dados")
        
        try:
            # Extração de playlists do Spotify
            if config.get('spotify_playlist_ids'):
                self.logger.info("Extraindo dados do Spotify...")
                results['spotify_tracks'] = self.extract_spotify_playlist_data(
                    config['spotify_playlist_ids']
                )
                
                # Extração de características de áudio
                if results['spotify_tracks'].get('data'):
                    track_ids = [track['track_id'] for track in results['spotify_tracks']['data']]
                    results['spotify_features'] = self.extract_spotify_audio_features(track_ids)
            
            # Extração de vídeos populares do YouTube
            if config.get('youtube_region_codes'):
                self.logger.info("Extraindo vídeos populares do YouTube...")
                results['youtube_popular'] = self.extract_youtube_popular_videos(
                    config['youtube_region_codes'],
                    config.get('youtube_max_results_per_region', 50)
                )
            
            # Extração de vídeos musicais baseado nas faixas do Spotify
            if results['spotify_tracks'].get('data') and config.get('extract_youtube_music', True):
                self.logger.info("Buscando vídeos musicais no YouTube...")
                search_queries = [
                    {'artist': track['artist_name'], 'track': track['name']}
                    for track in results['spotify_tracks']['data'][:config.get('max_music_search_queries', 100)]
                ]
                
                results['youtube_music'] = self.extract_youtube_music_videos(
                    search_queries,
                    config.get('youtube_max_results_per_query', 5)
                )
            
        except Exception as e:
            error_msg = f"Erro durante extração completa: {str(e)}"
            results['extraction_summary']['errors'].append(error_msg)
            self.logger.error(error_msg)
        
        # Calcula estatísticas finais
        results['extraction_summary']['total_duration'] = (datetime.utcnow() - extraction_start).total_seconds()
        results['extraction_summary']['total_spotify_tracks'] = len(results.get('spotify_tracks', {}).get('data', []))
        
        youtube_popular_count = len(results.get('youtube_popular', {}).get('data', []))
        youtube_music_count = len(results.get('youtube_music', {}).get('data', []))
        results['extraction_summary']['total_youtube_videos'] = youtube_popular_count + youtube_music_count
        
        self.logger.info(f"Extração completa finalizada em {results['extraction_summary']['total_duration']:.2f}s")
        self.logger.info(f"Total extraído: {results['extraction_summary']['total_spotify_tracks']} faixas Spotify, {results['extraction_summary']['total_youtube_videos']} vídeos YouTube")
        
        return results