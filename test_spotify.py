import os
import sys
sys.path.append(os.path.dirname(os.path.abspath('.')))
from src.api_clients.spotify_client import SpotifyClient
from dotenv import load_dotenv

load_dotenv()
client = SpotifyClient(
    client_id=os.getenv('SPOTIFY_CLIENT_ID'),
    client_secret=os.getenv('SPOTIFY_CLIENT_SECRET')
)

print('Testando autenticação...')
if client.authenticate():
    print('Autenticação OK!')
    
    # Testa com playlists públicas conhecidas
    playlists = ['6UeSakyzhiEt4NB3UAd6NQ', '4hOKQuZbraPDIfaGbM3lKI', '5FJXhjdILmRA2z5bvz4nzf']
    
    for playlist_id in playlists:
        print(f'\nTestando playlist: {playlist_id}')
        tracks = client.get_playlist_tracks(playlist_id)
        print(f'Tracks encontradas: {len(tracks) if tracks else 0}')
        
        if tracks and len(tracks) > 0:
            print(f'Primeira track: {tracks[0].get("name", "N/A")} - {tracks[0].get("artist", "N/A")}')
            break
    
    # Testa busca por música
    print('\nTestando busca...')
    search_results = client.search_tracks('Shape of You', limit=5)
    print(f'Resultados da busca: {len(search_results) if search_results else 0}')
    
else:
    print('Falha na autenticação')