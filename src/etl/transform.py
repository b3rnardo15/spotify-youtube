"""
Módulo de transformação de dados do pipeline ETL.
Responsável por limpar, padronizar e enriquecer os dados extraídos.
"""

import os
import sys
import logging
import re
from typing import Dict, List, Optional, Any
from datetime import datetime
import pandas as pd
import numpy as np
from difflib import SequenceMatcher

# Adiciona o diretório pai ao path para imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class DataTransformer:
    """
    Classe responsável pela transformação e enriquecimento dos dados.
    """
    
    def __init__(self):
        """
        Inicializa o transformador de dados.
        """
        self.logger = logging.getLogger(__name__)
        
    def transform_spotify_tracks(self, raw_tracks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transforma e limpa dados de faixas do Spotify.
        
        Args:
            raw_tracks (List[Dict]): Dados brutos das faixas
            
        Returns:
            List[Dict]: Dados transformados
        """
        self.logger.info(f"Transformando {len(raw_tracks)} faixas do Spotify")
        
        transformed_tracks = []
        
        for track in raw_tracks:
            try:
                transformed_track = {
                    # IDs e identificadores
                    'track_id': track.get('track_id', ''),
                    'album_id': track.get('album_id', ''),
                    
                    # Informações básicas
                    'name': self._clean_text(track.get('name', '')),
                    'artist_name': self._clean_text(track.get('artist_name', '')),
                    'album_name': self._clean_text(track.get('album_name', '')),
                    'artists': [self._clean_text(artist) for artist in track.get('artists', [])],
                    
                    # Métricas
                    'popularity': self._safe_int(track.get('popularity', 0)),
                    'duration_ms': self._safe_int(track.get('duration_ms', 0)),
                    'duration_seconds': self._safe_int(track.get('duration_ms', 0)) // 1000,
                    
                    # Flags
                    'explicit': bool(track.get('explicit', False)),
                    'is_local': bool(track.get('is_local', False)),
                    
                    # URLs e links
                    'preview_url': track.get('preview_url', ''),
                    'external_urls': track.get('external_urls', {}),
                    
                    # Informações do álbum
                    'release_date': self._parse_date(track.get('release_date', '')),
                    'total_tracks': self._safe_int(track.get('total_tracks', 0)),
                    'disc_number': self._safe_int(track.get('disc_number', 1)),
                    'track_number': self._safe_int(track.get('track_number', 1)),
                    
                    # Mercados disponíveis
                    'available_markets': track.get('available_markets', []),
                    'market_count': len(track.get('available_markets', [])),
                    
                    # Metadados de extração
                    'extraction_timestamp': track.get('extraction_timestamp', ''),
                    'source_playlist_id': track.get('source_playlist_id', ''),
                    'data_source': track.get('data_source', 'spotify'),
                    
                    # Campos derivados
                    'artist_count': len(track.get('artists', [])),
                    'is_collaboration': len(track.get('artists', [])) > 1,
                    'name_length': len(track.get('name', '')),
                    'artist_name_length': len(track.get('artist_name', '')),
                    
                    # Timestamp de transformação
                    'transformed_at': datetime.utcnow().isoformat()
                }
                
                # Adiciona categoria de popularidade
                transformed_track['popularity_category'] = self._categorize_popularity(
                    transformed_track['popularity']
                )
                
                # Adiciona categoria de duração
                transformed_track['duration_category'] = self._categorize_duration(
                    transformed_track['duration_seconds']
                )
                
                transformed_tracks.append(transformed_track)
                
            except Exception as e:
                self.logger.error(f"Erro ao transformar faixa {track.get('track_id', 'unknown')}: {e}")
                continue
        
        self.logger.info(f"Transformação concluída: {len(transformed_tracks)} faixas processadas")
        return transformed_tracks
    
    def transform_spotify_features(self, raw_features: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transforma características de áudio do Spotify.
        
        Args:
            raw_features (List[Dict]): Dados brutos das características
            
        Returns:
            List[Dict]: Características transformadas
        """
        self.logger.info(f"Transformando {len(raw_features)} características de áudio")
        
        transformed_features = []
        
        for feature in raw_features:
            try:
                transformed_feature = {
                    # ID da faixa
                    'track_id': feature.get('id', ''),
                    
                    # Características de áudio (0.0 - 1.0)
                    'danceability': self._safe_float(feature.get('danceability', 0.0)),
                    'energy': self._safe_float(feature.get('energy', 0.0)),
                    'speechiness': self._safe_float(feature.get('speechiness', 0.0)),
                    'acousticness': self._safe_float(feature.get('acousticness', 0.0)),
                    'instrumentalness': self._safe_float(feature.get('instrumentalness', 0.0)),
                    'liveness': self._safe_float(feature.get('liveness', 0.0)),
                    'valence': self._safe_float(feature.get('valence', 0.0)),
                    
                    # Características musicais
                    'loudness': self._safe_float(feature.get('loudness', 0.0)),
                    'tempo': self._safe_float(feature.get('tempo', 0.0)),
                    'key': self._safe_int(feature.get('key', -1)),
                    'mode': self._safe_int(feature.get('mode', -1)),
                    'time_signature': self._safe_int(feature.get('time_signature', 4)),
                    
                    # Duração
                    'duration_ms': self._safe_int(feature.get('duration_ms', 0)),
                    
                    # Metadados
                    'extraction_timestamp': feature.get('extraction_timestamp', ''),
                    'data_source': feature.get('data_source', 'spotify_features'),
                    'transformed_at': datetime.utcnow().isoformat()
                }
                
                # Adiciona categorias derivadas
                transformed_feature.update(self._categorize_audio_features(transformed_feature))
                
                # Adiciona score de "dançabilidade geral"
                transformed_feature['dance_score'] = (
                    transformed_feature['danceability'] * 0.4 +
                    transformed_feature['energy'] * 0.3 +
                    transformed_feature['valence'] * 0.3
                )
                
                # Adiciona score de "mood"
                transformed_feature['mood_score'] = (
                    transformed_feature['valence'] * 0.5 +
                    transformed_feature['energy'] * 0.3 +
                    (1 - transformed_feature['acousticness']) * 0.2
                )
                
                transformed_features.append(transformed_feature)
                
            except Exception as e:
                self.logger.error(f"Erro ao transformar características {feature.get('id', 'unknown')}: {e}")
                continue
        
        self.logger.info(f"Transformação de características concluída: {len(transformed_features)} processadas")
        return transformed_features
    
    def transform_youtube_videos(self, raw_videos: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transforma dados de vídeos do YouTube.
        
        Args:
            raw_videos (List[Dict]): Dados brutos dos vídeos
            
        Returns:
            List[Dict]: Dados transformados
        """
        self.logger.info(f"Transformando {len(raw_videos)} vídeos do YouTube")
        
        transformed_videos = []
        
        for video in raw_videos:
            try:
                transformed_video = {
                    # IDs e identificadores
                    'video_id': video.get('video_id', ''),
                    'channel_id': video.get('channel_id', ''),
                    
                    # Informações básicas
                    'title': self._clean_text(video.get('title', '')),
                    'description': self._clean_text(video.get('description', ''))[:1000],  # Limita descrição
                    'channel_title': self._clean_text(video.get('channel_title', '')),
                    
                    # Métricas de engajamento
                    'view_count': self._safe_int(video.get('view_count', 0)),
                    'like_count': self._safe_int(video.get('like_count', 0)),
                    'dislike_count': self._safe_int(video.get('dislike_count', 0)),
                    'comment_count': self._safe_int(video.get('comment_count', 0)),
                    'favorite_count': self._safe_int(video.get('favorite_count', 0)),
                    
                    # Datas
                    'published_at': self._parse_datetime(video.get('published_at', '')),
                    'published_date': self._parse_date(video.get('published_at', '')),
                    
                    # Conteúdo
                    'tags': video.get('tags', []),
                    'category_id': video.get('category_id', ''),
                    'default_language': video.get('default_language', ''),
                    'default_audio_language': video.get('default_audio_language', ''),
                    
                    # Características técnicas
                    'duration': video.get('duration', ''),
                    'duration_seconds': self._parse_duration_to_seconds(video.get('duration', '')),
                    'dimension': video.get('dimension', ''),
                    'definition': video.get('definition', ''),
                    'caption': video.get('caption', ''),
                    'licensed_content': bool(video.get('licensed_content', False)),
                    
                    # Status e configurações
                    'upload_status': video.get('upload_status', ''),
                    'privacy_status': video.get('privacy_status', ''),
                    'license': video.get('license', ''),
                    'embeddable': bool(video.get('embeddable', True)),
                    'public_stats_viewable': bool(video.get('public_stats_viewable', True)),
                    
                    # Thumbnails
                    'thumbnails': video.get('thumbnails', {}),
                    
                    # Metadados de extração
                    'extraction_timestamp': video.get('extraction_timestamp', ''),
                    'source_region': video.get('source_region', ''),
                    'search_artist': video.get('search_artist', ''),
                    'search_track': video.get('search_track', ''),
                    'data_source': video.get('data_source', 'youtube'),
                    
                    # Timestamp de transformação
                    'transformed_at': datetime.utcnow().isoformat()
                }
                
                # Calcula métricas derivadas
                transformed_video.update(self._calculate_engagement_metrics(transformed_video))
                
                # Adiciona categorias
                transformed_video['view_category'] = self._categorize_views(transformed_video['view_count'])
                transformed_video['duration_category'] = self._categorize_duration(transformed_video['duration_seconds'])
                
                # Adiciona flags de conteúdo
                transformed_video['is_music_video'] = self._is_music_video(transformed_video)
                transformed_video['is_official'] = self._is_official_content(transformed_video)
                
                # Adiciona contagem de tags
                transformed_video['tag_count'] = len(transformed_video['tags'])
                transformed_video['title_length'] = len(transformed_video['title'])
                transformed_video['description_length'] = len(transformed_video['description'])
                
                transformed_videos.append(transformed_video)
                
            except Exception as e:
                self.logger.error(f"Erro ao transformar vídeo {video.get('video_id', 'unknown')}: {e}")
                continue
        
        self.logger.info(f"Transformação de vídeos concluída: {len(transformed_videos)} processados")
        return transformed_videos
    
    def correlate_spotify_youtube_data(self, spotify_tracks: List[Dict[str, Any]], 
                                     youtube_videos: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Correlaciona dados do Spotify com vídeos do YouTube.
        
        Args:
            spotify_tracks (List[Dict]): Faixas transformadas do Spotify
            youtube_videos (List[Dict]): Vídeos transformados do YouTube
            
        Returns:
            List[Dict]: Dados correlacionados
        """
        self.logger.info(f"Correlacionando {len(spotify_tracks)} faixas com {len(youtube_videos)} vídeos")
        
        correlations = []
        
        for track in spotify_tracks:
            try:
                # Busca vídeos relacionados
                related_videos = self._find_related_videos(track, youtube_videos)
                
                if related_videos:
                    for video, similarity_score in related_videos:
                        correlation = {
                            # IDs
                            'correlation_id': f"{track['track_id']}_{video['video_id']}",
                            'track_id': track['track_id'],
                            'video_id': video['video_id'],
                            
                            # Dados do Spotify
                            'spotify_track_name': track['name'],
                            'spotify_artist_name': track['artist_name'],
                            'spotify_popularity': track['popularity'],
                            'spotify_duration_seconds': track['duration_seconds'],
                            
                            # Dados do YouTube
                            'youtube_title': video['title'],
                            'youtube_channel': video['channel_title'],
                            'youtube_view_count': video['view_count'],
                            'youtube_like_count': video['like_count'],
                            'youtube_duration_seconds': video['duration_seconds'],
                            
                            # Métricas de correlação
                            'similarity_score': similarity_score,
                            'correlation_strength': self._categorize_correlation_strength(similarity_score),
                            
                            # Análise comparativa
                            'duration_difference_seconds': abs(track['duration_seconds'] - video['duration_seconds']),
                            'duration_similarity': self._calculate_duration_similarity(
                                track['duration_seconds'], video['duration_seconds']
                            ),
                            
                            # Metadados
                            'created_at': datetime.utcnow().isoformat(),
                            'data_source': 'correlation_engine'
                        }
                        
                        # Adiciona métricas de engajamento cruzado
                        correlation.update(self._calculate_cross_platform_metrics(track, video))
                        
                        correlations.append(correlation)
                
            except Exception as e:
                self.logger.error(f"Erro ao correlacionar faixa {track.get('track_id', 'unknown')}: {e}")
                continue
        
        self.logger.info(f"Correlação concluída: {len(correlations)} correlações encontradas")
        return correlations
    
    def aggregate_regional_data(self, youtube_videos: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Agrega dados de engajamento por região.
        
        Args:
            youtube_videos (List[Dict]): Vídeos transformados do YouTube
            
        Returns:
            List[Dict]: Dados agregados por região
        """
        self.logger.info("Agregando dados regionais")
        
        # Agrupa por região
        regional_data = {}
        
        for video in youtube_videos:
            region = video.get('source_region', 'unknown')
            
            if region not in regional_data:
                regional_data[region] = {
                    'region_code': region,
                    'total_videos': 0,
                    'total_views': 0,
                    'total_likes': 0,
                    'total_comments': 0,
                    'avg_duration': 0,
                    'music_videos_count': 0,
                    'official_videos_count': 0,
                    'top_channels': {},
                    'top_categories': {},
                    'created_at': datetime.utcnow().isoformat()
                }
            
            region_stats = regional_data[region]
            
            # Atualiza contadores
            region_stats['total_videos'] += 1
            region_stats['total_views'] += video.get('view_count', 0)
            region_stats['total_likes'] += video.get('like_count', 0)
            region_stats['total_comments'] += video.get('comment_count', 0)
            
            if video.get('is_music_video', False):
                region_stats['music_videos_count'] += 1
            
            if video.get('is_official', False):
                region_stats['official_videos_count'] += 1
            
            # Conta canais
            channel = video.get('channel_title', 'unknown')
            region_stats['top_channels'][channel] = region_stats['top_channels'].get(channel, 0) + 1
            
            # Conta categorias
            category = video.get('category_id', 'unknown')
            region_stats['top_categories'][category] = region_stats['top_categories'].get(category, 0) + 1
        
        # Calcula médias e finaliza agregações
        aggregated_data = []
        
        for region, stats in regional_data.items():
            if stats['total_videos'] > 0:
                stats['avg_views_per_video'] = stats['total_views'] / stats['total_videos']
                stats['avg_likes_per_video'] = stats['total_likes'] / stats['total_videos']
                stats['avg_comments_per_video'] = stats['total_comments'] / stats['total_videos']
                
                # Calcula engagement rate médio
                if stats['total_views'] > 0:
                    stats['avg_engagement_rate'] = (stats['total_likes'] + stats['total_comments']) / stats['total_views']
                else:
                    stats['avg_engagement_rate'] = 0
                
                # Pega top 5 canais e categorias
                stats['top_5_channels'] = sorted(
                    stats['top_channels'].items(), 
                    key=lambda x: x[1], 
                    reverse=True
                )[:5]
                
                stats['top_5_categories'] = sorted(
                    stats['top_categories'].items(), 
                    key=lambda x: x[1], 
                    reverse=True
                )[:5]
                
                # Remove dicionários completos para economizar espaço
                del stats['top_channels']
                del stats['top_categories']
                
                aggregated_data.append(stats)
        
        self.logger.info(f"Agregação regional concluída: {len(aggregated_data)} regiões processadas")
        return aggregated_data
    
    # Métodos auxiliares
    
    def _clean_text(self, text: str) -> str:
        """Limpa e padroniza texto."""
        if not text:
            return ""
        
        # Remove caracteres especiais e normaliza espaços
        cleaned = re.sub(r'\s+', ' ', text.strip())
        return cleaned
    
    def _safe_int(self, value: Any) -> int:
        """Converte valor para int de forma segura."""
        try:
            return int(value) if value is not None else 0
        except (ValueError, TypeError):
            return 0
    
    def _safe_float(self, value: Any) -> float:
        """Converte valor para float de forma segura."""
        try:
            return float(value) if value is not None else 0.0
        except (ValueError, TypeError):
            return 0.0
    
    def _parse_date(self, date_str: str) -> str:
        """Padroniza formato de data."""
        if not date_str:
            return ""
        
        try:
            # Tenta diferentes formatos
            if len(date_str) == 4:  # Apenas ano
                return f"{date_str}-01-01"
            elif len(date_str) == 7:  # Ano-mês
                return f"{date_str}-01"
            else:
                return date_str[:10]  # YYYY-MM-DD
        except:
            return ""
    
    def _parse_datetime(self, datetime_str: str) -> str:
        """Padroniza formato de datetime."""
        if not datetime_str:
            return ""
        
        try:
            # Remove timezone info se presente e padroniza
            if 'T' in datetime_str:
                return datetime_str.replace('Z', '').split('.')[0]
            return datetime_str
        except:
            return ""
    
    def _parse_duration_to_seconds(self, duration: str) -> int:
        """Converte duração ISO 8601 para segundos."""
        if not duration:
            return 0
        
        try:
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
        except:
            return 0
    
    def _categorize_popularity(self, popularity: int) -> str:
        """Categoriza popularidade do Spotify."""
        if popularity >= 80:
            return "very_high"
        elif popularity >= 60:
            return "high"
        elif popularity >= 40:
            return "medium"
        elif popularity >= 20:
            return "low"
        else:
            return "very_low"
    
    def _categorize_duration(self, duration_seconds: int) -> str:
        """Categoriza duração."""
        if duration_seconds < 60:
            return "very_short"
        elif duration_seconds < 180:
            return "short"
        elif duration_seconds < 300:
            return "medium"
        elif duration_seconds < 600:
            return "long"
        else:
            return "very_long"
    
    def _categorize_views(self, view_count: int) -> str:
        """Categoriza visualizações do YouTube."""
        if view_count >= 10000000:  # 10M+
            return "viral"
        elif view_count >= 1000000:  # 1M+
            return "very_popular"
        elif view_count >= 100000:  # 100K+
            return "popular"
        elif view_count >= 10000:  # 10K+
            return "moderate"
        else:
            return "low"
    
    def _categorize_audio_features(self, features: Dict[str, Any]) -> Dict[str, str]:
        """Categoriza características de áudio."""
        categories = {}
        
        # Energia
        energy = features.get('energy', 0)
        if energy >= 0.8:
            categories['energy_level'] = "very_high"
        elif energy >= 0.6:
            categories['energy_level'] = "high"
        elif energy >= 0.4:
            categories['energy_level'] = "medium"
        elif energy >= 0.2:
            categories['energy_level'] = "low"
        else:
            categories['energy_level'] = "very_low"
        
        # Valência (positividade)
        valence = features.get('valence', 0)
        if valence >= 0.8:
            categories['mood'] = "very_positive"
        elif valence >= 0.6:
            categories['mood'] = "positive"
        elif valence >= 0.4:
            categories['mood'] = "neutral"
        elif valence >= 0.2:
            categories['mood'] = "negative"
        else:
            categories['mood'] = "very_negative"
        
        # Dançabilidade
        danceability = features.get('danceability', 0)
        if danceability >= 0.8:
            categories['danceability_level'] = "very_danceable"
        elif danceability >= 0.6:
            categories['danceability_level'] = "danceable"
        elif danceability >= 0.4:
            categories['danceability_level'] = "moderate"
        else:
            categories['danceability_level'] = "not_danceable"
        
        return categories
    
    def _calculate_engagement_metrics(self, video: Dict[str, Any]) -> Dict[str, float]:
        """Calcula métricas de engajamento."""
        metrics = {}
        
        view_count = video.get('view_count', 0)
        like_count = video.get('like_count', 0)
        comment_count = video.get('comment_count', 0)
        
        if view_count > 0:
            metrics['like_rate'] = like_count / view_count
            metrics['comment_rate'] = comment_count / view_count
            metrics['engagement_rate'] = (like_count + comment_count) / view_count
        else:
            metrics['like_rate'] = 0.0
            metrics['comment_rate'] = 0.0
            metrics['engagement_rate'] = 0.0
        
        # Score de engajamento (0-100)
        metrics['engagement_score'] = min(100, metrics['engagement_rate'] * 1000)
        
        return metrics
    
    def _is_music_video(self, video: Dict[str, Any]) -> bool:
        """Determina se é um vídeo musical."""
        title = video.get('title', '').lower()
        description = video.get('description', '').lower()
        tags = [tag.lower() for tag in video.get('tags', [])]
        
        music_keywords = ['music', 'song', 'official', 'video', 'audio', 'lyrics', 'album']
        
        # Verifica título
        title_score = sum(1 for keyword in music_keywords if keyword in title)
        
        # Verifica tags
        tag_score = sum(1 for tag in tags for keyword in music_keywords if keyword in tag)
        
        # Verifica se tem dados de busca musical
        has_search_data = bool(video.get('search_artist') and video.get('search_track'))
        
        return title_score >= 2 or tag_score >= 3 or has_search_data
    
    def _is_official_content(self, video: Dict[str, Any]) -> bool:
        """Determina se é conteúdo oficial."""
        title = video.get('title', '').lower()
        channel = video.get('channel_title', '').lower()
        
        official_keywords = ['official', 'vevo', 'records', 'music']
        
        return any(keyword in title or keyword in channel for keyword in official_keywords)
    
    def _find_related_videos(self, track: Dict[str, Any], videos: List[Dict[str, Any]]) -> List[tuple]:
        """Encontra vídeos relacionados a uma faixa."""
        related = []
        
        track_name = track.get('name', '').lower()
        artist_name = track.get('artist_name', '').lower()
        
        for video in videos:
            similarity_score = self._calculate_similarity(track, video)
            
            if similarity_score > 0.3:  # Threshold mínimo
                related.append((video, similarity_score))
        
        # Ordena por similaridade e retorna top 5
        related.sort(key=lambda x: x[1], reverse=True)
        return related[:5]
    
    def _calculate_similarity(self, track: Dict[str, Any], video: Dict[str, Any]) -> float:
        """Calcula similaridade entre faixa e vídeo."""
        track_name = track.get('name', '').lower()
        artist_name = track.get('artist_name', '').lower()
        video_title = video.get('title', '').lower()
        
        # Se tem dados de busca, usa eles
        if video.get('search_artist') and video.get('search_track'):
            search_artist = video.get('search_artist', '').lower()
            search_track = video.get('search_track', '').lower()
            
            if search_artist == artist_name and search_track == track_name:
                return 1.0
        
        # Calcula similaridade textual
        name_similarity = SequenceMatcher(None, track_name, video_title).ratio()
        artist_in_title = 1.0 if artist_name in video_title else 0.0
        
        # Score combinado
        similarity = (name_similarity * 0.7) + (artist_in_title * 0.3)
        
        return similarity
    
    def _categorize_correlation_strength(self, similarity_score: float) -> str:
        """Categoriza força da correlação."""
        if similarity_score >= 0.9:
            return "very_strong"
        elif similarity_score >= 0.7:
            return "strong"
        elif similarity_score >= 0.5:
            return "moderate"
        elif similarity_score >= 0.3:
            return "weak"
        else:
            return "very_weak"
    
    def _calculate_duration_similarity(self, duration1: int, duration2: int) -> float:
        """Calcula similaridade de duração."""
        if duration1 == 0 or duration2 == 0:
            return 0.0
        
        diff = abs(duration1 - duration2)
        max_duration = max(duration1, duration2)
        
        return max(0.0, 1.0 - (diff / max_duration))
    
    def _calculate_cross_platform_metrics(self, track: Dict[str, Any], video: Dict[str, Any]) -> Dict[str, Any]:
        """Calcula métricas cruzadas entre plataformas."""
        return {
            'spotify_youtube_ratio': video.get('view_count', 0) / max(track.get('popularity', 1), 1),
            'cross_platform_score': (
                track.get('popularity', 0) * 0.3 +
                min(100, video.get('view_count', 0) / 10000) * 0.7
            ),
            'platform_consistency': self._calculate_duration_similarity(
                track.get('duration_seconds', 0),
                video.get('duration_seconds', 0)
            )
        }