# Spotify-YouTube Analytics Pipeline

Um pipeline ETL completo para integração e análise de dados do Spotify e YouTube, desenvolvido em Python com MongoDB para análise de tendências musicais e audiovisuais.

## Índice

- [Visão Geral](#visão-geral)
- [Funcionalidades](#funcionalidades)
- [Tecnologias e Dependências](#tecnologias-e-dependências)
- [APIs Utilizadas](#apis-utilizadas)
- [Métricas e Análises](#métricas-e-análises)
- [Instalação](#instalação)
- [Configuração](#configuração)
- [Uso](#uso)
- [Estrutura do Projeto](#estrutura-do-projeto)
- [Dashboard](#dashboard)
- [Troubleshooting](#troubleshooting)

## Visão Geral

Este projeto implementa um pipeline ETL (Extract, Transform, Load) robusto que integra dados das APIs do Spotify e YouTube para análise de tendências musicais e audiovisuais. O sistema extrai dados de playlists populares do Spotify e vídeos trending do YouTube, correlaciona informações entre as plataformas e apresenta insights através de um dashboard interativo.

### Objetivos

- Analisar tendências musicais entre Spotify e YouTube
- Identificar correlações entre popularidade nas duas plataformas
- Fornecer insights sobre engajamento regional
- Criar visualizações interativas para análise de dados
- Calcular métricas derivadas para análise cross-platform

## Funcionalidades

### Pipeline ETL

**Extração de Dados:**
- Extração automatizada de faixas musicais de playlists do Spotify
- Coleta de características de áudio detalhadas (danceability, energy, valence, etc.)
- Extração de vídeos populares do YouTube por região
- Coleta de métricas de engajamento (views, likes, comentários)

**Transformação de Dados:**
- Limpeza e padronização de dados de ambas as plataformas
- Correlação inteligente entre faixas do Spotify e vídeos do YouTube
- Cálculo de métricas derivadas e scores de similaridade
- Agregação de dados por região geográfica
- Processamento de características de áudio

**Carga de Dados:**
- Armazenamento otimizado no MongoDB
- Prevenção de duplicatas com operações upsert
- Processamento em lotes para melhor performance
- Sistema de logging completo

### Dashboard Interativo

**Análise Spotify:**
- Visualização de faixas mais populares
- Análise de características de áudio com radar charts
- Distribuição de popularidade e duração
- Análise temporal de lançamentos
- Filtros por artista, gênero e popularidade

**Análise YouTube:**
- Top vídeos por visualizações
- Métricas de engajamento (likes, comentários, views)
- Análise regional de popularidade
- Distribuição de taxa de engajamento
- Análise por categoria de vídeo

**Análises Cross-Platform:**
- Correlações entre popularidade do Spotify e views do YouTube
- Scores de similaridade entre faixas e vídeos
- Métricas derivadas de engajamento cruzado
- Análise de consistência entre plataformas

**Visualizações Geográficas:**
- Mapas de popularidade por país
- Análise regional de engajamento
- Comparação de tendências entre regiões
- Distribuição geográfica de artistas

## Tecnologias e Dependências

### Linguagem e Runtime
- **Python 3.8+** - Linguagem principal do projeto

### Banco de Dados
- **MongoDB** - Banco de dados NoSQL para armazenamento
- **PyMongo 4.6.0** - Driver oficial MongoDB para Python

### APIs e Clientes
- **Spotipy 2.22.1** - Cliente oficial da API do Spotify Web
- **Google API Python Client 2.108.0** - Cliente para YouTube Data API v3

### Análise e Manipulação de Dados
- **Pandas 2.1.4** - Manipulação e análise de dados estruturados
- **NumPy** - Operações matemáticas e arrays (dependência do Pandas)

### Visualização e Dashboard
- **Streamlit 1.28.2** - Framework para criação de dashboards web
- **Plotly 5.17.0** - Biblioteca de visualização interativa

### Utilitários
- **Python-dotenv 1.0.0** - Gerenciamento de variáveis de ambiente
- **Logging** - Sistema de logs nativo do Python para monitoramento

### Instalação das Dependências

```bash
pip install -r requirements.txt
```

## APIs Utilizadas

### Spotify Web API

**Endpoints Principais:**
- `/v1/playlists/{playlist_id}/tracks` - Extração de faixas de playlists
- `/v1/audio-features` - Características de áudio detalhadas
- `/v1/artists/{id}` - Informações de artistas
- `/v1/albums/{id}` - Metadados de álbuns

**Dados Coletados:**
- Nome da faixa, artista, álbum
- Popularidade (0-100)
- Duração, data de lançamento
- Características de áudio: danceability, energy, valence, acousticness, instrumentalness, liveness, speechiness, tempo
- Informações de mercado e disponibilidade

### YouTube Data API v3

**Endpoints Principais:**
- `/youtube/v3/videos` - Dados de vídeos populares
- `/youtube/v3/search` - Busca de vídeos relacionados
- `/youtube/v3/channels` - Informações de canais

**Dados Coletados:**
- Título, descrição, canal
- Contagem de views, likes, comentários
- Data de publicação, duração
- Categoria, região de popularidade
- Thumbnails e metadados

## Métricas e Análises

### Métricas do Spotify

**Popularidade:**
- Score de popularidade (0-100)
- Ranking por artista e faixa
- Distribuição de popularidade

**Características de Áudio:**
- **Danceability** (0.0-1.0): Quão adequada a faixa é para dançar
- **Energy** (0.0-1.0): Medida de intensidade e atividade
- **Valence** (0.0-1.0): Positividade musical transmitida
- **Acousticness** (0.0-1.0): Confiança de que a faixa é acústica
- **Instrumentalness** (0.0-1.0): Predição se a faixa não contém vocais
- **Liveness** (0.0-1.0): Detecção de presença de audiência
- **Speechiness** (0.0-1.0): Detecção de palavras faladas
- **Tempo** (BPM): Tempo estimado da faixa

### Métricas do YouTube

**Engajamento:**
- View count (visualizações totais)
- Like count (curtidas)
- Comment count (comentários)
- Taxa de engajamento (likes/views)

**Popularidade Regional:**
- Trending por código de região
- Distribuição geográfica de views
- Análise de preferências regionais

### Métricas Derivadas Cross-Platform

**Correlação entre Plataformas:**
- **Spotify-YouTube Ratio**: Relação entre views do YouTube e popularidade do Spotify
- **Cross-Platform Score**: Score combinado considerando ambas as plataformas
- **Platform Consistency**: Similaridade de duração entre faixas e vídeos
- **Match Confidence**: Confiança na correlação entre faixa e vídeo (0.0-1.0)

**Análise Regional:**
- Popularidade média do Spotify por região
- Total de visualizações do YouTube por região
- Top faixas e vídeos por localização
- Métricas de engajamento regionalizadas

## Instalação

### Pré-requisitos

- Python 3.8 ou superior
- MongoDB instalado e rodando
- Credenciais das APIs do Spotify e YouTube

### Passos de Instalação

1. **Clone o repositório:**
```bash
git clone <repository-url>
cd spotify-youtube-analytics
```

2. **Instale as dependências:**
```bash
pip install -r requirements.txt
```

3. **Configure o MongoDB:**
```bash
# Inicie o MongoDB
mongod --dbpath /path/to/your/db
```

## Configuração

### Variáveis de Ambiente

Copie o arquivo `.env.template` para `.env` e configure:

```env
# Spotify API Credentials
SPOTIPY_CLIENT_ID=your_spotify_client_id_here
SPOTIPY_CLIENT_SECRET=your_spotify_client_secret_here

# YouTube Data API Key
YOUTUBE_API_KEY=your_youtube_api_key_here

# MongoDB Configuration
MONGO_URI=mongodb://localhost:27017/
MONGO_DB_NAME=spotify_youtube_analytics

# Pipeline Configuration
SPOTIFY_PLAYLIST_IDS=37i9dQZEVXbMDoHDwVN2tF,37i9dQZEVXbMFLuQp5sK2D
YOUTUBE_REGION_CODES=BR,US
```

### Obtenção de Credenciais

**Spotify API:**
1. Acesse [Spotify Developer Dashboard](https://developer.spotify.com/dashboard/)
2. Crie uma nova aplicação
3. Copie Client ID e Client Secret

**YouTube API:**
1. Acesse [Google Cloud Console](https://console.developers.google.com/)
2. Crie um projeto e ative a YouTube Data API v3
3. Gere uma API Key

## Uso

### Comandos Disponíveis

**Verificar status do sistema:**
```bash
python src/main.py --mode status
```

**Executar pipeline completo:**
```bash
python src/main.py --mode full
```

**Executar apenas extração do Spotify:**
```bash
python src/main.py --mode spotify
```

**Executar apenas extração do YouTube:**
```bash
python src/main.py --mode youtube
```

**Executar apenas fase de extração:**
```bash
python src/main.py --mode extract
```

### Executar Dashboard

```bash
streamlit run dashboard.py
```

Acesse: http://localhost:8501

## Estrutura do Projeto

```
spotify-youtube-analytics/
├── src/
│   ├── api_clients/
│   │   ├── spotify_client.py      # Cliente Spotify API
│   │   └── youtube_client.py      # Cliente YouTube API
│   ├── database/
│   │   └── mongodb_manager.py     # Gerenciador MongoDB
│   ├── etl/
│   │   ├── extract.py            # Extração de dados
│   │   ├── transform.py          # Transformação e correlação
│   │   └── load.py               # Carga no banco
│   └── main.py                   # Script principal
├── dashboard.py                  # Dashboard Streamlit
├── requirements.txt              # Dependências
├── .env.template                # Template de configuração
└── README.md                    # Documentação
```

## Dashboard

### Abas Disponíveis

1. **Visão Geral** - Métricas gerais e resumo dos dados
2. **Spotify** - Análise detalhada de dados musicais
3. **YouTube** - Análise de vídeos e engajamento
4. **Correlações** - Análise cross-platform
5. **Geográfico** - Visualizações regionais
6. **Métricas Derivadas** - Análises avançadas
7. **Explorador** - Visualização de dados brutos

### Funcionalidades do Dashboard

- Filtros interativos por artista, gênero, popularidade
- Visualizações com Plotly (gráficos de barras, dispersão, radar, mapas)
- Métricas em tempo real
- Exportação de dados
- Interface responsiva

## Troubleshooting

### Problemas Comuns

**Erro de Conexão MongoDB:**
```
pymongo.errors.ServerSelectionTimeoutError
```
- Verifique se o MongoDB está rodando
- Confirme a URI no arquivo `.env`

**Erro de Autenticação Spotify:**
```
spotipy.oauth2.SpotifyOauthError
```
- Verifique as credenciais no `.env`
- Confirme as configurações no Spotify Developer Dashboard

**Quota Excedida YouTube:**
```
googleapiclient.errors.HttpError: 403 quotaExceeded
```
- Verifique sua quota no Google Cloud Console
- Aguarde o reset diário da quota

**Erro de Dependências:**
```
ModuleNotFoundError
```
- Execute: `pip install -r requirements.txt`
- Verifique a versão do Python (3.8+)

### Logs e Monitoramento

O sistema gera logs detalhados em `etl_pipeline.log` para monitoramento e debug das operações.

---

**Desenvolvido para análise integrada de tendências musicais e audiovisuais entre Spotify e YouTube.**