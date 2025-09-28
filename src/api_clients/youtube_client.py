"""
Cliente para interação com a YouTube Data API.
"""

import logging
import time
from typing import Dict, List, Optional, Any
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


class YouTubeClient:
    """
    Cliente para interação com a YouTube Data API.
    """
    
    def __init__(self, api_key: str):
        """
        Inicializa o cliente YouTube.
        
        Args:
            api_key (str): Chave da API do YouTube
        """
        self.api_key = api_key
        self.youtube = None
        self.logger = logging.getLogger(__name__)
        
    def authenticate(self) -> bool:
        """
        Autentica com a YouTube Data API.
        
        Returns:
            bool: True se a autenticação foi bem-sucedida
        """
        try:
            self.youtube = build('youtube', 'v3', developerKey=self.api_key)
            
            # Testa a autenticação fazendo uma busca simples
            self.youtube.search().list(
                part='snippet',
                q='test',
                type='video',
                maxResults=1
            ).execute()
            
            self.logger.info("Autenticação com YouTube bem-sucedida")
            return True
        except HttpError as e:
            self.logger.error(f"Erro na autenticação com YouTube: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Erro inesperado na autenticação: {e}")
            return False
    
    def get_popular_videos(self, region_code: str = "BR", max_results: int = 50) -> Optional[List[Dict[str, Any]]]:
        """
        Obtém vídeos populares de uma região.
        
        Args:
            region_code (str): Código da região (ISO 3166-1 alpha-2)
            max_results (int): Número máximo de resultados
            
        Returns:
            List[Dict]: Lista de vídeos populares ou None em caso de erro
        """
        if not self.youtube:
            self.logger.error("Cliente não autenticado")
            return None
        
        try:
            videos = []
            next_page_token = None
            
            while len(videos) < max_results:
                remaining = max_results - len(videos)
                current_limit = min(50, remaining)  # API limita a 50 por requisição
                
                request = self.youtube.videos().list(
                    part='snippet,statistics,contentDetails',
                    chart='mostPopular',
                    regionCode=region_code,
                    maxResults=current_limit,
                    pageToken=next_page_token
                )
                
                response = request.execute()
                
                for item in response.get('items', []):
                    video_data = self._extract_video_data(item)
                    videos.append(video_data)
                
                next_page_token = response.get('nextPageToken')
                if not next_page_token:
                    break
                
                # Rate limiting
                time.sleep(0.1)
            
            self.logger.info(f"Obtidos {len(videos)} vídeos populares da região {region_code}")
            return videos
            
        except HttpError as e:
            self.logger.error(f"Erro ao obter vídeos populares: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Erro inesperado: {e}")
            return None
    
    def search_videos(self, query: str, max_results: int = 50, order: str = "relevance") -> Optional[List[Dict[str, Any]]]:
        """
        Busca vídeos por query.
        
        Args:
            query (str): Termo de busca
            max_results (int): Número máximo de resultados
            order (str): Ordem dos resultados (relevance, date, rating, viewCount, title)
            
        Returns:
            List[Dict]: Lista de vídeos encontrados ou None em caso de erro
        """
        if not self.youtube:
            self.logger.error("Cliente não autenticado")
            return None
        
        try:
            videos = []
            next_page_token = None
            
            while len(videos) < max_results:
                remaining = max_results - len(videos)
                current_limit = min(50, remaining)
                
                # Primeiro, busca os vídeos
                search_request = self.youtube.search().list(
                    part='snippet',
                    q=query,
                    type='video',
                    order=order,
                    maxResults=current_limit,
                    pageToken=next_page_token
                )
                
                search_response = search_request.execute()
                
                if not search_response.get('items'):
                    break
                
                # Extrai os IDs dos vídeos
                video_ids = [item['id']['videoId'] for item in search_response['items']]
                
                # Obtém detalhes dos vídeos (estatísticas e duração)
                videos_request = self.youtube.videos().list(
                    part='snippet,statistics,contentDetails',
                    id=','.join(video_ids)
                )
                
                videos_response = videos_request.execute()
                
                for item in videos_response.get('items', []):
                    video_data = self._extract_video_data(item)
                    videos.append(video_data)
                
                next_page_token = search_response.get('nextPageToken')
                if not next_page_token:
                    break
                
                # Rate limiting
                time.sleep(0.1)
            
            self.logger.info(f"Encontrados {len(videos)} vídeos para query: {query}")
            return videos
            
        except HttpError as e:
            self.logger.error(f"Erro na busca de vídeos: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Erro inesperado: {e}")
            return None
    
    def get_video_details(self, video_id: str) -> Optional[Dict[str, Any]]:
        """
        Obtém detalhes de um vídeo específico.
        
        Args:
            video_id (str): ID do vídeo
            
        Returns:
            Dict: Detalhes do vídeo ou None em caso de erro
        """
        if not self.youtube:
            self.logger.error("Cliente não autenticado")
            return None
        
        try:
            request = self.youtube.videos().list(
                part='snippet,statistics,contentDetails',
                id=video_id
            )
            
            response = request.execute()
            
            if response.get('items'):
                return self._extract_video_data(response['items'][0])
            
            return None
            
        except HttpError as e:
            self.logger.error(f"Erro ao obter detalhes do vídeo {video_id}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Erro inesperado: {e}")
            return None
    
    def get_multiple_videos_details(self, video_ids: List[str]) -> Optional[List[Dict[str, Any]]]:
        """
        Obtém detalhes de múltiplos vídeos.
        
        Args:
            video_ids (List[str]): Lista de IDs dos vídeos
            
        Returns:
            List[Dict]: Lista de detalhes dos vídeos ou None em caso de erro
        """
        if not self.youtube:
            self.logger.error("Cliente não autenticado")
            return None
        
        try:
            all_videos = []
            batch_size = 50  # API permite até 50 IDs por requisição
            
            for i in range(0, len(video_ids), batch_size):
                batch = video_ids[i:i + batch_size]
                
                request = self.youtube.videos().list(
                    part='snippet,statistics,contentDetails',
                    id=','.join(batch)
                )
                
                response = request.execute()
                
                for item in response.get('items', []):
                    video_data = self._extract_video_data(item)
                    all_videos.append(video_data)
                
                # Rate limiting
                time.sleep(0.1)
            
            self.logger.info(f"Obtidos detalhes para {len(all_videos)} vídeos")
            return all_videos
            
        except HttpError as e:
            self.logger.error(f"Erro ao obter detalhes dos vídeos: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Erro inesperado: {e}")
            return None
    
    def search_music_videos(self, artist: str, track: str, max_results: int = 10) -> Optional[List[Dict[str, Any]]]:
        """
        Busca vídeos musicais específicos por artista e faixa.
        
        Args:
            artist (str): Nome do artista
            track (str): Nome da faixa
            max_results (int): Número máximo de resultados
            
        Returns:
            List[Dict]: Lista de vídeos encontrados ou None em caso de erro
        """
        # Constrói query de busca otimizada para música
        query = f"{artist} {track} official music video"
        
        return self.search_videos(
            query=query,
            max_results=max_results,
            order="relevance"
        )
    
    def _extract_video_data(self, video: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extrai dados relevantes de um vídeo.
        
        Args:
            video (Dict): Dados do vídeo da API
            
        Returns:
            Dict: Dados estruturados do vídeo
        """
        snippet = video.get('snippet', {})
        statistics = video.get('statistics', {})
        content_details = video.get('contentDetails', {})
        
        return {
            'video_id': video['id'],
            'title': snippet.get('title', ''),
            'description': snippet.get('description', ''),
            'channel_title': snippet.get('channelTitle', ''),
            'channel_id': snippet.get('channelId', ''),
            'published_at': snippet.get('publishedAt', ''),
            'thumbnails': snippet.get('thumbnails', {}),
            'tags': snippet.get('tags', []),
            'category_id': snippet.get('categoryId', ''),
            'default_language': snippet.get('defaultLanguage', ''),
            'default_audio_language': snippet.get('defaultAudioLanguage', ''),
            'view_count': int(statistics.get('viewCount', 0)),
            'like_count': int(statistics.get('likeCount', 0)),
            'dislike_count': int(statistics.get('dislikeCount', 0)),
            'favorite_count': int(statistics.get('favoriteCount', 0)),
            'comment_count': int(statistics.get('commentCount', 0)),
            'duration': content_details.get('duration', ''),
            'dimension': content_details.get('dimension', ''),
            'definition': content_details.get('definition', ''),
            'caption': content_details.get('caption', ''),
            'licensed_content': content_details.get('licensedContent', False),
            'projection': content_details.get('projection', ''),
            'upload_status': video.get('status', {}).get('uploadStatus', ''),
            'privacy_status': video.get('status', {}).get('privacyStatus', ''),
            'license': video.get('status', {}).get('license', ''),
            'embeddable': video.get('status', {}).get('embeddable', True),
            'public_stats_viewable': video.get('status', {}).get('publicStatsViewable', True)
        }
    
    def _parse_duration(self, duration: str) -> int:
        """
        Converte duração ISO 8601 para segundos.
        
        Args:
            duration (str): Duração no formato ISO 8601 (ex: PT4M13S)
            
        Returns:
            int: Duração em segundos
        """
        import re
        
        if not duration:
            return 0
        
        # Remove PT do início
        duration = duration.replace('PT', '')
        
        # Extrai horas, minutos e segundos
        hours = re.search(r'(\d+)H', duration)
        minutes = re.search(r'(\d+)M', duration)
        seconds = re.search(r'(\d+)S', duration)
        
        total_seconds = 0
        if hours:
            total_seconds += int(hours.group(1)) * 3600
        if minutes:
            total_seconds += int(minutes.group(1)) * 60
        if seconds:
            total_seconds += int(seconds.group(1))
        
        return total_seconds