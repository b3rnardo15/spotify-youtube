"""
Cliente para interação com a API do Spotify usando Spotipy.
"""

import logging
import time
from typing import Dict, List, Optional, Any
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from spotipy.exceptions import SpotifyException


class SpotifyClient:
    """
    Cliente para interação com a API do Spotify.
    """
    
    def __init__(self, client_id: str, client_secret: str):
        """
        Inicializa o cliente Spotify.
        
        Args:
            client_id (str): ID do cliente Spotify
            client_secret (str): Secret do cliente Spotify
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.spotify: Optional[spotipy.Spotify] = None
        self.logger = logging.getLogger(__name__)
        
    def authenticate(self) -> bool:
        """
        Autentica com a API do Spotify.
        
        Returns:
            bool: True se a autenticação foi bem-sucedida
        """
        try:
            client_credentials_manager = SpotifyClientCredentials(
                client_id=self.client_id,
                client_secret=self.client_secret
            )
            self.spotify = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
            
            # Testa a autenticação fazendo uma busca simples
            self.spotify.search(q="test", type="track", limit=1)
            self.logger.info("Autenticação com Spotify bem-sucedida")
            return True
        except SpotifyException as e:
            self.logger.error(f"Erro na autenticação com Spotify: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Erro inesperado na autenticação: {e}")
            return False
    
    def get_playlist_tracks(self, playlist_id: str) -> Optional[List[Dict[str, Any]]]:
        """
        Obtém as faixas de uma playlist.
        
        Args:
            playlist_id (str): ID da playlist
            
        Returns:
            List[Dict]: Lista de faixas ou None em caso de erro
        """
        if not self.spotify:
            self.logger.error("Cliente não autenticado")
            return None
        
        try:
            tracks = []
            results = self.spotify.playlist_tracks(playlist_id)
            
            while results:
                for item in results['items']:
                    if item['track'] and item['track']['id']:
                        track_data = self._extract_track_data(item['track'])
                        tracks.append(track_data)
                
                # Paginação
                if results['next']:
                    results = self.spotify.next(results)
                else:
                    break
            
            self.logger.info(f"Obtidas {len(tracks)} faixas da playlist {playlist_id}")
            return tracks
            
        except SpotifyException as e:
            self.logger.error(f"Erro ao obter faixas da playlist {playlist_id}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Erro inesperado: {e}")
            return None
    
    def search_tracks(self, query: str, limit: int = 50) -> Optional[List[Dict[str, Any]]]:
        """
        Busca faixas por query.
        
        Args:
            query (str): Termo de busca
            limit (int): Limite de resultados
            
        Returns:
            List[Dict]: Lista de faixas encontradas ou None em caso de erro
        """
        if not self.spotify:
            self.logger.error("Cliente não autenticado")
            return None
        
        try:
            tracks = []
            offset = 0
            batch_size = min(50, limit)  # API do Spotify limita a 50 por requisição
            
            while len(tracks) < limit:
                remaining = limit - len(tracks)
                current_limit = min(batch_size, remaining)
                
                results = self.spotify.search(
                    q=query,
                    type='track',
                    limit=current_limit,
                    offset=offset
                )
                
                if not results['tracks']['items']:
                    break
                
                for track in results['tracks']['items']:
                    if track and track['id']:
                        track_data = self._extract_track_data(track)
                        tracks.append(track_data)
                
                offset += current_limit
                
                # Rate limiting
                time.sleep(0.1)
            
            self.logger.info(f"Encontradas {len(tracks)} faixas para query: {query}")
            return tracks
            
        except SpotifyException as e:
            self.logger.error(f"Erro na busca de faixas: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Erro inesperado: {e}")
            return None
    
    def get_track_features(self, track_id: str) -> Optional[Dict[str, Any]]:
        """
        Obtém características de áudio de uma faixa.
        
        Args:
            track_id (str): ID da faixa
            
        Returns:
            Dict: Características da faixa ou None em caso de erro
        """
        if not self.spotify:
            self.logger.error("Cliente não autenticado")
            return None
        
        try:
            features = self.spotify.audio_features(track_id)
            if features and features[0]:
                return features[0]
            return None
            
        except SpotifyException as e:
            self.logger.error(f"Erro ao obter características da faixa {track_id}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Erro inesperado: {e}")
            return None
    
    def get_multiple_tracks_features(self, track_ids: List[str]) -> Optional[List[Dict[str, Any]]]:
        """
        Obtém características de áudio de múltiplas faixas.
        
        Args:
            track_ids (List[str]): Lista de IDs das faixas
            
        Returns:
            List[Dict]: Lista de características ou None em caso de erro
        """
        if not self.spotify:
            self.logger.error("Cliente não autenticado")
            return None
        
        try:
            all_features = []
            batch_size = 100  # API do Spotify permite até 100 por requisição
            
            for i in range(0, len(track_ids), batch_size):
                batch = track_ids[i:i + batch_size]
                features = self.spotify.audio_features(batch)
                
                if features:
                    # Filtra resultados None
                    valid_features = [f for f in features if f is not None]
                    all_features.extend(valid_features)
                
                # Rate limiting
                time.sleep(0.1)
            
            self.logger.info(f"Obtidas características para {len(all_features)} faixas")
            return all_features
            
        except SpotifyException as e:
            self.logger.error(f"Erro ao obter características das faixas: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Erro inesperado: {e}")
            return None
    
    def get_top_tracks_by_country(self, country_code: str = "BR") -> Optional[List[Dict[str, Any]]]:
        """
        Obtém as faixas mais populares de um país.
        
        Args:
            country_code (str): Código do país (ISO 3166-1 alpha-2)
            
        Returns:
            List[Dict]: Lista de faixas populares ou None em caso de erro
        """
        # Usa a playlist Top 50 do país específico
        playlist_mapping = {
            "BR": "37i9dQZEVXbMXbN3EUUhlg",  # Top 50 - Brazil
            "US": "37i9dQZEVXbLRQDuF5jeBp",  # Top 50 - USA
            "GB": "37i9dQZEVXbLnolsZ8PSNw",  # Top 50 - United Kingdom
            "DE": "37i9dQZEVXbJiZcmkrIHGU",  # Top 50 - Germany
            "FR": "37i9dQZEVXbIPWwFssbupI",  # Top 50 - France
        }
        
        playlist_id = playlist_mapping.get(country_code, "37i9dQZEVXbMDoHDwVN2tF")  # Global como fallback
        return self.get_playlist_tracks(playlist_id)
    
    def _extract_track_data(self, track: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extrai dados relevantes de uma faixa.
        
        Args:
            track (Dict): Dados da faixa da API
            
        Returns:
            Dict: Dados estruturados da faixa
        """
        artists = [artist['name'] for artist in track.get('artists', [])]
        
        return {
            'track_id': track['id'],
            'name': track['name'],
            'artist_name': ', '.join(artists),
            'artists': artists,
            'album_name': track.get('album', {}).get('name', ''),
            'album_id': track.get('album', {}).get('id', ''),
            'popularity': track.get('popularity', 0),
            'duration_ms': track.get('duration_ms', 0),
            'explicit': track.get('explicit', False),
            'preview_url': track.get('preview_url', ''),
            'external_urls': track.get('external_urls', {}),
            'release_date': track.get('album', {}).get('release_date', ''),
            'total_tracks': track.get('album', {}).get('total_tracks', 0),
            'available_markets': track.get('available_markets', []),
            'disc_number': track.get('disc_number', 1),
            'track_number': track.get('track_number', 1),
            'is_local': track.get('is_local', False)
        }