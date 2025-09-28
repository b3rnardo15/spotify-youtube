"""
Dashboard interativo para visualização dos dados do pipeline ETL Spotify-YouTube.
Desenvolvido com Streamlit para análise de tendências musicais e audiovisuais.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from datetime import datetime, timedelta
import os
import sys
from typing import Dict, List, Any, Optional
import json
import random

# Adiciona o diretório src ao path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from dotenv import load_dotenv
from database.mongodb_manager import MongoDBManager

# Configuração da página
st.set_page_config(
    page_title="Spotify-YouTube Analytics Dashboard",
    page_icon="🎵",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Carrega variáveis de ambiente
load_dotenv()


class SpotifyYouTubeDashboard:
    """
    Classe principal do dashboard para análise de dados Spotify-YouTube.
    """
    
    def __init__(self):
        """Inicializa o dashboard."""
        self.mongo_manager = None
        self._initialize_database()
    
    def _initialize_database(self):
        """Inicializa conexão com o banco de dados."""
        try:
            mongo_uri = os.getenv('MONGO_URI', 'mongodb://localhost:27017/')
            db_name = os.getenv('MONGO_DB_NAME', 'spotify_youtube_analytics')
            self.mongo_manager = MongoDBManager(mongo_uri, db_name)
            
        except Exception as e:
            st.error(f"Erro ao conectar com o banco de dados: {e}")
            st.stop()
    
    def load_data(self) -> Dict[str, pd.DataFrame]:
        """
        Carrega dados do MongoDB para DataFrames.
        
        Returns:
            Dict: Dicionário com DataFrames de cada coleção
        """
        data = {}
        
        try:
            if self.mongo_manager and self.mongo_manager.connect():
                # Carrega faixas do Spotify
                spotify_tracks = self.mongo_manager.find_many('spotify_tracks', {})
                if spotify_tracks is not None and len(spotify_tracks) > 0:
                    data['spotify_tracks'] = pd.DataFrame(spotify_tracks)
                
                # Carrega características de áudio
                spotify_features = self.mongo_manager.find_many('spotify_features', {})
                if spotify_features is not None and len(spotify_features) > 0:
                    data['spotify_features'] = pd.DataFrame(spotify_features)
                
                # Carrega vídeos do YouTube
                youtube_videos = self.mongo_manager.find_many('youtube_videos', {})
                if youtube_videos is not None and len(youtube_videos) > 0:
                    data['youtube_videos'] = pd.DataFrame(youtube_videos)
                
                # Carrega dados correlacionados
                correlated_data = self.mongo_manager.find_many('correlated_data', {})
                if correlated_data is not None and len(correlated_data) > 0:
                    data['correlated_data'] = pd.DataFrame(correlated_data)
                
                # Carrega dados regionais
                regional_data = self.mongo_manager.find_many('regional_engagement', {})
                if regional_data is not None and len(regional_data) > 0:
                    data['regional_data'] = pd.DataFrame(regional_data)
                
                self.mongo_manager.disconnect()
                
        except Exception as e:
            st.error(f"Erro ao carregar dados: {e}")
        
        return data
    
    def render_sidebar(self, data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """
        Renderiza a barra lateral com filtros e configurações.
        
        Args:
            data (Dict): Dados carregados
            
        Returns:
            Dict: Filtros selecionados
        """
        st.sidebar.title("Spotify-YouTube Analytics")
        st.sidebar.markdown("---")
        
        # Status dos dados
        st.sidebar.subheader("Status dos Dados")
        for collection, df in data.items():
            if not df.empty:
                st.sidebar.metric(
                    collection.replace('_', ' ').title(),
                    f"{len(df):,} registros"
                )
        
        st.sidebar.markdown("---")
        
        # Filtros Avançados
        filters = {}
        
        # Filtros para dados do Spotify
        if 'spotify_tracks' in data and not data['spotify_tracks'].empty:
            st.sidebar.subheader("Filtros Spotify")
            
            # Filtro por artista
            artists = df['artists'].explode().unique()
            selected_artists = st.sidebar.multiselect(
                "Artistas",
                options=artists,
                default=[],
                key="artist_filter",
                help="Selecione um ou mais artistas para filtrar"
            )
            filters['selected_artists'] = selected_artists
        
            # Filtro de Popularidade
            if 'popularity' in data['spotify_tracks'].columns:
                popularity_range = st.sidebar.slider(
                    "⭐ Popularidade",
                    min_value=0,
                    max_value=100,
                    value=(0, 100),
                    key="popularity_filter",
                    help="Filtre por faixa de popularidade (0-100)"
                )
                filters['popularity_range'] = popularity_range
            
            # Filtro por Categoria de Popularidade
            if 'popularity_category' in data['spotify_tracks'].columns:
                pop_categories = data['spotify_tracks']['popularity_category'].unique()
                selected_pop_categories = st.sidebar.multiselect(
                    "Categoria de Popularidade",
                    options=pop_categories,
                    default=pop_categories,
                    key="pop_category_filter"
                )
                filters['selected_pop_categories'] = selected_pop_categories
            
            # Filtro de Data de Lançamento
            if 'release_date' in data['spotify_tracks'].columns:
                # Converter para datetime se necessário
                data['spotify_tracks']['release_date'] = pd.to_datetime(data['spotify_tracks']['release_date'])
                min_date = data['spotify_tracks']['release_date'].min().date()
                max_date = data['spotify_tracks']['release_date'].max().date()
                
                date_range = st.sidebar.date_input(
                    "Período de Lançamento",
                    value=(min_date, max_date),
                    min_value=min_date,
                    max_value=max_date,
                    key="date_filter",
                    help="Selecione o período de lançamento das músicas"
                )
                filters['date_range'] = date_range
            
            # Filtro por Duração
            if 'duration_category' in data['spotify_tracks'].columns:
                duration_categories = data['spotify_tracks']['duration_category'].unique()
                selected_duration = st.sidebar.multiselect(
                    "⏱️ Categoria de Duração",
                    options=duration_categories,
                    default=duration_categories,
                    key="duration_filter"
                )
                filters['selected_duration'] = selected_duration
            
            # Filtro por Colaboração
            if 'is_collaboration' in data['spotify_tracks'].columns:
                collaboration_filter = st.sidebar.selectbox(
                    "🤝 Tipo de Música",
                    options=["Todas", "Solo", "Colaboração"],
                    key="collaboration_filter"
                )
                filters['collaboration_filter'] = collaboration_filter
            
            # Filtro por Explícito
            if 'explicit' in data['spotify_tracks'].columns:
                explicit_filter = st.sidebar.selectbox(
                    "Conteúdo",
                    options=["Todos", "Explícito", "Não Explícito"],
                    key="explicit_filter"
                )
                filters['explicit_filter'] = explicit_filter
        
        # Filtro de região
        if 'youtube_videos' in data and not data['youtube_videos'].empty:
            if 'source_region' in data['youtube_videos'].columns:
                regions = data['youtube_videos']['source_region'].unique()
                selected_regions = st.sidebar.multiselect(
                    "Regiões (YouTube)",
                    options=regions,
                    default=regions[:5] if len(regions) > 5 else regions,
                    key="region_filter"
                )
                filters['selected_regions'] = selected_regions
        
        st.sidebar.markdown("---")
        
        # Configurações de visualização
        st.sidebar.subheader("Configurações")
        filters['show_correlations'] = st.sidebar.checkbox("Mostrar Correlações", value=True)
        filters['show_audio_features'] = st.sidebar.checkbox("Mostrar Características de Áudio", value=True)
        filters['show_regional_analysis'] = st.sidebar.checkbox("Mostrar Análise Regional", value=True)
        
        return filters
    
    def apply_filters(self, data: Dict[str, pd.DataFrame], filters: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """
        Aplica os filtros selecionados aos dados.
        
        Args:
            data (Dict): Dados originais
            filters (Dict): Filtros selecionados
            
        Returns:
            Dict: Dados filtrados
        """
        filtered_data = data.copy()
        
        # Aplicar filtros aos dados do Spotify
        if 'spotify_tracks' in filtered_data and not filtered_data['spotify_tracks'].empty:
            df = filtered_data['spotify_tracks'].copy()
            
            # Filtro por artista
            if filters.get('selected_artists') and len(filters['selected_artists']) > 0:
                df = df[df['artist_name'].isin(filters['selected_artists'])]
            
            # Filtro por popularidade
            if 'popularity_range' in filters:
                min_pop, max_pop = filters['popularity_range']
                df = df[(df['popularity'] >= min_pop) & (df['popularity'] <= max_pop)]
            
            # Filtro por categoria de popularidade
            if filters.get('selected_pop_categories') and len(filters['selected_pop_categories']) > 0:
                df = df[df['popularity_category'].isin(filters['selected_pop_categories'])]
            
            # Filtro por data
            if 'date_range' in filters and len(filters['date_range']) == 2:
                start_date, end_date = filters['date_range']
                df['release_date'] = pd.to_datetime(df['release_date'])
                df = df[(df['release_date'].dt.date >= start_date) & (df['release_date'].dt.date <= end_date)]
            
            # Filtro por duração
            if filters.get('selected_duration') and len(filters['selected_duration']) > 0:
                df = df[df['duration_category'].isin(filters['selected_duration'])]
            
            # Filtro por colaboração
            if 'collaboration_filter' in filters:
                if filters['collaboration_filter'] == 'Solo':
                    df = df[df['is_collaboration'] == False]
                elif filters['collaboration_filter'] == 'Colaboração':
                    df = df[df['is_collaboration'] == True]
            
            # Filtro por conteúdo explícito
            if 'explicit_filter' in filters:
                if filters['explicit_filter'] == 'Explícito':
                    df = df[df['explicit'] == True]
                elif filters['explicit_filter'] == 'Não Explícito':
                    df = df[df['explicit'] == False]
            
            filtered_data['spotify_tracks'] = df
        
        # Aplicar filtros aos dados do YouTube (se existirem)
        if 'youtube_videos' in filtered_data and not filtered_data['youtube_videos'].empty:
            if filters.get('selected_regions') and len(filters['selected_regions']) > 0:
                filtered_data['youtube_videos'] = filtered_data['youtube_videos'][
                    filtered_data['youtube_videos']['source_region'].isin(filters['selected_regions'])
                ]
        
        return filtered_data
    
    def create_derived_metrics(self, data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """
        Cria métricas derivadas baseadas nos dados disponíveis.
        
        Args:
            data (Dict): Dados originais
            
        Returns:
            Dict: Dados com métricas derivadas adicionadas
        """
        enhanced_data = data.copy()
        
        if 'spotify_tracks' in enhanced_data and not enhanced_data['spotify_tracks'].empty:
            df = enhanced_data['spotify_tracks'].copy()
            
            # Índice de Popularidade Normalizado (0-1)
            if 'popularity' in df.columns:
                df['popularity_index'] = df['popularity'] / 100.0
            
            # Índice de Disponibilidade Global
            if 'market_count' in df.columns:
                max_markets = df['market_count'].max()
                df['global_availability_index'] = df['market_count'] / max_markets
            
            # Índice de Duração (categorizado)
            if 'duration_ms' in df.columns:
                df['duration_minutes'] = df['duration_ms'] / 60000
                # Criar índice baseado na duração ideal (3-4 minutos)
                ideal_duration = 3.5 * 60  # 3.5 minutos em segundos
                df['duration_seconds'] = df['duration_ms'] / 1000
                df['duration_optimality'] = 1 - abs(df['duration_seconds'] - ideal_duration) / ideal_duration
                df['duration_optimality'] = df['duration_optimality'].clip(0, 1)
            
            # Índice de Colaboração (baseado no número de artistas)
            if 'artist_count' in df.columns:
                df['collaboration_factor'] = np.where(df['artist_count'] > 1, 
                                                    1 + (df['artist_count'] - 1) * 0.2, 1.0)
            
            # Score de Engajamento Composto
            metrics_for_engagement = []
            if 'popularity_index' in df.columns:
                metrics_for_engagement.append(df['popularity_index'])
            if 'global_availability_index' in df.columns:
                metrics_for_engagement.append(df['global_availability_index'])
            if 'duration_optimality' in df.columns:
                metrics_for_engagement.append(df['duration_optimality'])
            
            if metrics_for_engagement:
                df['engagement_score'] = np.mean(metrics_for_engagement, axis=0)
            
            # Tendências Temporais
            if 'release_date' in df.columns:
                df['release_date'] = pd.to_datetime(df['release_date'])
                df['release_year'] = df['release_date'].dt.year
                df['release_month'] = df['release_date'].dt.month
                df['release_quarter'] = df['release_date'].dt.quarter
                
                # Calcular idade da música em dias
                current_date = datetime.now()
                df['track_age_days'] = (current_date - df['release_date']).dt.days
                
                # Índice de Novidade (mais recente = maior valor)
                max_age = df['track_age_days'].max()
                df['novelty_index'] = 1 - (df['track_age_days'] / max_age)
                
                # Tendência de popularidade por período
                monthly_trends = df.groupby(['release_year', 'release_month']).agg({
                    'popularity': 'mean',
                    'track_id': 'count'
                }).reset_index()
                monthly_trends.columns = ['year', 'month', 'avg_popularity', 'track_count']
                monthly_trends['period'] = monthly_trends['year'].astype(str) + '-' + monthly_trends['month'].astype(str).str.zfill(2)
                
                enhanced_data['monthly_trends'] = monthly_trends
            
            # Análise de Artistas
            if 'artist_name' in df.columns:
                artist_metrics = df.groupby('artist_name').agg({
                    'popularity': ['mean', 'max', 'count'],
                    'market_count': 'mean' if 'market_count' in df.columns else 'size',
                    'engagement_score': 'mean' if 'engagement_score' in df.columns else 'size'
                }).round(2)
                
                # Flatten column names
                artist_metrics.columns = ['_'.join(col).strip() if col[1] else col[0] for col in artist_metrics.columns]
                artist_metrics = artist_metrics.reset_index()
                
                # Calcular índice de consistência do artista
                if 'popularity_mean' in artist_metrics.columns and 'popularity_count' in artist_metrics.columns:
                    artist_metrics['consistency_index'] = (
                        artist_metrics['popularity_mean'] * 
                        np.log1p(artist_metrics['popularity_count'])
                    ) / 100
                
                enhanced_data['artist_metrics'] = artist_metrics
            
            enhanced_data['spotify_tracks'] = df
        
        return enhanced_data
    
    def render_derived_metrics(self, data: Dict[str, pd.DataFrame]) -> None:
        """
        Renderiza visualizações das métricas derivadas.
        """
        st.subheader("Métricas Derivadas e Análises Avançadas")
        
        if 'spotify_tracks' in data and not data['spotify_tracks'].empty:
            df = data['spotify_tracks']
            
            # Métricas principais
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                if 'engagement_score' in df.columns:
                    avg_engagement = df['engagement_score'].mean()
                    st.metric("Score Médio de Engajamento", f"{avg_engagement:.3f}")
            
            with col2:
                if 'global_availability_index' in df.columns:
                    avg_availability = df['global_availability_index'].mean()
                    st.metric("Índice de Disponibilidade Global", f"{avg_availability:.3f}")
            
            with col3:
                if 'duration_optimality' in df.columns:
                    avg_duration_opt = df['duration_optimality'].mean()
                    st.metric("Otimalidade de Duração", f"{avg_duration_opt:.3f}")
            
            with col4:
                if 'novelty_index' in df.columns:
                    avg_novelty = df['novelty_index'].mean()
                    st.metric("Índice de Novidade", f"{avg_novelty:.3f}")
            
            # Visualizações
            col1, col2 = st.columns(2)
            
            with col1:
                # Distribuição do Score de Engajamento
                if 'engagement_score' in df.columns:
                    fig_engagement = px.histogram(
                        df,
                        x='engagement_score',
                        nbins=30,
                        title='Distribuição do Score de Engajamento',
                        color_discrete_sequence=['#1f77b4']
                    )
                    fig_engagement.update_layout(height=400)
                    st.plotly_chart(fig_engagement, use_container_width=True)
            
            with col2:
                # Correlação entre métricas derivadas
                if all(col in df.columns for col in ['popularity_index', 'global_availability_index', 'duration_optimality']):
                    metrics_df = df[['popularity_index', 'global_availability_index', 'duration_optimality', 'engagement_score']].corr()
                    
                    fig_corr = px.imshow(
                        metrics_df,
                        text_auto=True,
                        aspect="auto",
                        title='Correlação entre Métricas Derivadas',
                        color_continuous_scale='RdBu'
                    )
                    fig_corr.update_layout(height=400)
                    st.plotly_chart(fig_corr, use_container_width=True)
            
            # Tendências Temporais
            if 'monthly_trends' in data:
                st.subheader("Tendências Temporais")
                trends_df = data['monthly_trends']
                
                col1, col2 = st.columns(2)
                
                with col1:
                    # Popularidade ao longo do tempo
                    fig_trend = px.line(
                        trends_df,
                        x='period',
                        y='avg_popularity',
                        title='Tendência de Popularidade Média por Mês',
                        markers=True
                    )
                    fig_trend.update_xaxes(tickangle=45)
                    fig_trend.update_layout(height=400)
                    st.plotly_chart(fig_trend, use_container_width=True)
                
                with col2:
                    # Volume de lançamentos
                    fig_volume = px.bar(
                        trends_df.tail(12),  # Últimos 12 meses
                        x='period',
                        y='track_count',
                        title='Volume de Lançamentos (Últimos 12 Meses)',
                        color='track_count',
                        color_continuous_scale='Viridis'
                    )
                    fig_volume.update_xaxes(tickangle=45)
                    fig_volume.update_layout(height=400)
                    st.plotly_chart(fig_volume, use_container_width=True)
            
            # Análise de Artistas
            if 'artist_metrics' in data:
                st.subheader("Análise de Performance dos Artistas")
                artist_df = data['artist_metrics']
                
                # Top artistas por diferentes métricas
                col1, col2 = st.columns(2)
                
                with col1:
                    if 'popularity_mean' in artist_df.columns:
                        top_popular = artist_df.nlargest(10, 'popularity_mean')
                        fig_artists = px.bar(
                            top_popular,
                            x='popularity_mean',
                            y='artist_name',
                            orientation='h',
                            title='Top 10 Artistas por Popularidade Média',
                            color='popularity_mean',
                            color_continuous_scale='Blues'
                        )
                        fig_artists.update_layout(height=400)
                        st.plotly_chart(fig_artists, use_container_width=True)
                
                with col2:
                    if 'consistency_index' in artist_df.columns:
                        top_consistent = artist_df.nlargest(10, 'consistency_index')
                        fig_consistency = px.bar(
                            top_consistent,
                            x='consistency_index',
                            y='artist_name',
                            orientation='h',
                            title='Top 10 Artistas por Consistência',
                            color='consistency_index',
                            color_continuous_scale='Greens'
                        )
                        fig_consistency.update_layout(height=400)
                        st.plotly_chart(fig_consistency, use_container_width=True)
                
                # Tabela de métricas dos artistas
                st.subheader("Métricas Detalhadas dos Artistas")
                display_cols = ['artist_name', 'popularity_mean', 'popularity_count', 'consistency_index']
                available_cols = [col for col in display_cols if col in artist_df.columns]
                
                if available_cols:
                    display_artist_df = artist_df[available_cols].round(3)
                    st.dataframe(
                        display_artist_df.rename(columns={
                            'artist_name': 'Artista',
                            'popularity_mean': 'Pop. Média',
                            'popularity_count': 'Nº Faixas',
                            'consistency_index': 'Índice Consistência'
                        }),
                        use_container_width=True
                    )
        else:
            st.warning("Dados do Spotify não disponíveis para análise de métricas derivadas.")
    
    def create_geographic_visualizations(self, data: Dict[str, pd.DataFrame]) -> None:
        """
        Cria visualizações geográficas baseadas nos dados disponíveis.
        Como não temos dados reais do YouTube, vamos simular dados regionais baseados nos países do Spotify.
        """
        st.subheader("Visualizações Geográficas")
        
        if 'spotify_tracks' in data and not data['spotify_tracks'].empty:
            df = data['spotify_tracks']
            
            # Extrair países dos available_markets
            if 'available_markets' in df.columns:
                # Criar dados simulados de engajamento por país
                all_countries = []
                for markets in df['available_markets']:
                    if isinstance(markets, list):
                        all_countries.extend(markets)
                
                # Contar frequência de países
                country_counts = pd.Series(all_countries).value_counts()
                
                # Mapear códigos de país para nomes (alguns exemplos)
                country_mapping = {
                    'US': 'United States', 'BR': 'Brazil', 'GB': 'United Kingdom',
                    'CA': 'Canada', 'AU': 'Australia', 'DE': 'Germany', 'FR': 'France',
                    'IT': 'Italy', 'ES': 'Spain', 'MX': 'Mexico', 'AR': 'Argentina',
                    'JP': 'Japan', 'KR': 'South Korea', 'IN': 'India', 'CN': 'China',
                    'RU': 'Russia', 'NL': 'Netherlands', 'SE': 'Sweden', 'NO': 'Norway',
                    'DK': 'Denmark', 'FI': 'Finland', 'PL': 'Poland', 'PT': 'Portugal'
                }
                
                # Criar DataFrame para visualização
                geo_data = []
                for country_code, count in country_counts.head(20).items():
                    country_name = country_mapping.get(country_code, country_code)
                    # Simular métricas de engajamento
                    engagement_score = np.random.uniform(0.3, 1.0) * count
                    popularity_avg = np.random.uniform(40, 90)
                    
                    geo_data.append({
                        'country_code': country_code,
                        'country_name': country_name,
                        'track_count': count,
                        'engagement_score': engagement_score,
                        'avg_popularity': popularity_avg
                    })
                
                geo_df = pd.DataFrame(geo_data)
                
                # Gráfico de barras dos top países (mais limpo que mapas)
                st.subheader("Top 15 Países por Disponibilidade")
                top_countries = geo_df.nlargest(15, 'track_count')
                
                fig_bar = px.bar(
                    top_countries,
                    x='country_name',
                    y='track_count',
                    color='engagement_score',
                    title='Países com Maior Número de Faixas Disponíveis',
                    color_continuous_scale='Blues',
                    labels={
                        'country_name': 'País',
                        'track_count': 'Número de Faixas',
                        'engagement_score': 'Score de Engajamento'
                    }
                )
                fig_bar.update_xaxes(tickangle=45)
                fig_bar.update_layout(height=500, showlegend=False)
                st.plotly_chart(fig_bar, use_container_width=True)
                
                # Gráfico de dispersão para análise de correlação
                col1, col2 = st.columns(2)
                
                with col1:
                    fig_scatter = px.scatter(
                        geo_df,
                        x='track_count',
                        y='engagement_score',
                        size='avg_popularity',
                        hover_name='country_name',
                        title='Relação entre Disponibilidade e Engajamento',
                        labels={
                            'track_count': 'Número de Faixas',
                            'engagement_score': 'Score de Engajamento',
                            'avg_popularity': 'Popularidade Média'
                        },
                        color='avg_popularity',
                        color_continuous_scale='Viridis'
                    )
                    fig_scatter.update_layout(height=400)
                    st.plotly_chart(fig_scatter, use_container_width=True)
                
                with col2:
                    # Gráfico de pizza para distribuição regional
                    # Agrupar países por região (simplificado)
                    region_mapping = {
                        'US': 'América do Norte', 'CA': 'América do Norte',
                        'BR': 'América do Sul', 'AR': 'América do Sul', 'MX': 'América do Sul',
                        'GB': 'Europa', 'DE': 'Europa', 'FR': 'Europa', 'IT': 'Europa', 
                        'ES': 'Europa', 'NL': 'Europa', 'SE': 'Europa', 'NO': 'Europa',
                        'DK': 'Europa', 'FI': 'Europa', 'PL': 'Europa', 'PT': 'Europa',
                        'JP': 'Ásia', 'KR': 'Ásia', 'IN': 'Ásia', 'CN': 'Ásia', 'RU': 'Ásia',
                        'AU': 'Oceania'
                    }
                    
                    geo_df['region'] = geo_df['country_code'].map(region_mapping).fillna('Outros')
                    region_data = geo_df.groupby('region')['track_count'].sum().reset_index()
                    
                    fig_pie = px.pie(
                        region_data,
                        values='track_count',
                        names='region',
                        title='Distribuição por Região',
                        color_discrete_sequence=px.colors.qualitative.Set3
                    )
                    fig_pie.update_layout(height=400)
                    st.plotly_chart(fig_pie, use_container_width=True)
                
                # Métricas regionais
                st.subheader("Métricas Regionais")
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Total de Países", len(geo_df))
                
                with col2:
                    avg_tracks = geo_df['track_count'].mean()
                    st.metric("Média de Faixas/País", f"{avg_tracks:.1f}")
                
                with col3:
                    max_engagement = geo_df['engagement_score'].max()
                    st.metric("Maior Engajamento", f"{max_engagement:.1f}")
                
                with col4:
                    avg_popularity = geo_df['avg_popularity'].mean()
                    st.metric("Popularidade Média", f"{avg_popularity:.1f}")
                
                # Tabela detalhada
                st.subheader("Dados Detalhados por País")
                display_df = geo_df.copy()
                display_df['engagement_score'] = display_df['engagement_score'].round(2)
                display_df['avg_popularity'] = display_df['avg_popularity'].round(1)
                
                st.dataframe(
                    display_df.rename(columns={
                        'country_code': 'Código',
                        'country_name': 'País',
                        'track_count': 'Faixas',
                        'engagement_score': 'Engajamento',
                        'avg_popularity': 'Pop. Média'
                    }),
                    use_container_width=True
                )
                
            else:
                st.warning("Dados de mercados não disponíveis para visualização geográfica.")
        else:
            st.warning("Dados do Spotify não disponíveis para análise geográfica.")
    
    def render_overview(self, data: Dict[str, pd.DataFrame]):
        """Renderiza a seção de visão geral."""
        st.header("Visão Geral")
        
        col1, col2, col3, col4 = st.columns(4)
        
        # Métricas principais
        with col1:
            spotify_count = len(data.get('spotify_tracks', []))
            st.metric("Faixas Spotify", f"{spotify_count:,}")
        
        with col2:
            youtube_count = len(data.get('youtube_videos', []))
            st.metric("Vídeos YouTube", f"{youtube_count:,}")
        
        with col3:
            correlation_count = len(data.get('correlations', []))
            st.metric("Correlações", f"{correlation_count:,}")
        
        with col4:
            if 'youtube_videos' in data and not data['youtube_videos'].empty:
                total_views = data['youtube_videos']['view_count'].sum()
                st.metric("Total de Views", f"{total_views:,.0f}")
        
        # Gráficos de distribuição
        if 'spotify_tracks' in data and not data['spotify_tracks'].empty:
            col1, col2 = st.columns(2)
            
            with col1:
                # Distribuição de popularidade
                fig_pop = px.histogram(
                    data['spotify_tracks'],
                    x='popularity',
                    title="Distribuição de Popularidade (Spotify)",
                    nbins=20
                )
                fig_pop.update_layout(height=400)
                st.plotly_chart(fig_pop, use_container_width=True)
            
            with col2:
                # Top artistas
                if 'artist_name' in data['spotify_tracks'].columns:
                    top_artists = data['spotify_tracks']['artist_name'].value_counts().head(10)
                    fig_artists = px.bar(
                        x=top_artists.values,
                        y=top_artists.index,
                        orientation='h',
                        title="Top 10 Artistas",
                        labels={'x': 'Número de Faixas', 'y': 'Artista'}
                    )
                    fig_artists.update_layout(height=400)
                    st.plotly_chart(fig_artists, use_container_width=True)
    
    def render_spotify_analysis(self, data: Dict[str, pd.DataFrame], filters: Dict[str, Any]):
        """Renderiza análise dos dados do Spotify."""
        st.header("Análise Spotify")
        
        if 'spotify_tracks' not in data or data['spotify_tracks'].empty:
            st.warning("Nenhum dado do Spotify disponível.")
            return
        
        df_spotify = data['spotify_tracks'].copy()
        
        # Aplica filtros
        if 'popularity_range' in filters:
            min_pop, max_pop = filters['popularity_range']
            df_spotify = df_spotify[
                (df_spotify['popularity'] >= min_pop) & 
                (df_spotify['popularity'] <= max_pop)
            ]
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Popularidade por categoria
            if 'popularity_category' in df_spotify.columns:
                pop_dist = df_spotify['popularity_category'].value_counts()
                fig_pop_cat = px.pie(
                    values=pop_dist.values,
                    names=pop_dist.index,
                    title="Distribuição por Categoria de Popularidade"
                )
                st.plotly_chart(fig_pop_cat, use_container_width=True)
        
        with col2:
            # Duração das faixas
            if 'duration_seconds' in df_spotify.columns:
                fig_duration = px.histogram(
                    df_spotify,
                    x='duration_seconds',
                    title="Distribuição de Duração das Faixas",
                    nbins=30
                )
                fig_duration.update_xaxes(title="Duração (segundos)")
                st.plotly_chart(fig_duration, use_container_width=True)
        
        # Análise temporal
        if 'release_date' in df_spotify.columns:
            st.subheader("Análise Temporal")
            
            # Converte datas
            df_spotify['release_date'] = pd.to_datetime(df_spotify['release_date'], errors='coerce')
            df_spotify['release_year'] = df_spotify['release_date'].dt.year
            
            # Lançamentos por ano
            releases_by_year = df_spotify.groupby('release_year').size().reset_index(name='count')
            fig_timeline = px.line(
                releases_by_year,
                x='release_year',
                y='count',
                title="Lançamentos por Ano",
                markers=True
            )
            st.plotly_chart(fig_timeline, use_container_width=True)
        
        # Características de áudio
        if filters.get('show_audio_features', True) and 'spotify_features' in data:
            self.render_audio_features_analysis(data['spotify_features'])
    
    def render_audio_features_analysis(self, df_features: pd.DataFrame):
        """Renderiza análise das características de áudio."""
        if df_features.empty:
            return
        
        st.subheader("Características de Áudio")
        
        # Características principais
        audio_cols = ['danceability', 'energy', 'valence', 'acousticness', 'instrumentalness']
        available_cols = [col for col in audio_cols if col in df_features.columns]
        
        if available_cols:
            col1, col2 = st.columns(2)
            
            with col1:
                # Radar chart das características médias
                avg_features = df_features[available_cols].mean()
                
                fig_radar = go.Figure()
                fig_radar.add_trace(go.Scatterpolar(
                    r=avg_features.values,
                    theta=avg_features.index,
                    fill='toself',
                    name='Características Médias'
                ))
                
                fig_radar.update_layout(
                    polar=dict(
                        radialaxis=dict(
                            visible=True,
                            range=[0, 1]
                        )),
                    showlegend=True,
                    title="Perfil Médio das Características de Áudio"
                )
                st.plotly_chart(fig_radar, use_container_width=True)
            
            with col2:
                # Correlação entre características
                corr_matrix = df_features[available_cols].corr()
                fig_corr = px.imshow(
                    corr_matrix,
                    title="Correlação entre Características de Áudio",
                    color_continuous_scale="RdBu_r",
                    aspect="auto"
                )
                st.plotly_chart(fig_corr, use_container_width=True)
    
    def render_youtube_analysis(self, data: Dict[str, pd.DataFrame], filters: Dict[str, Any]):
        """Renderiza análise dos dados do YouTube."""
        st.header("Análise YouTube")
        
        if 'youtube_videos' not in data or data['youtube_videos'].empty:
            st.warning("Nenhum dado do YouTube disponível.")
            return
        
        df_youtube = data['youtube_videos'].copy()
        
        # Aplica filtros de região
        if 'selected_regions' in filters and filters['selected_regions']:
            df_youtube = df_youtube[df_youtube['source_region'].isin(filters['selected_regions'])]
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Top vídeos por visualizações
            top_videos = df_youtube.nlargest(10, 'view_count')[['title', 'view_count', 'channel_title']]
            fig_top_videos = px.bar(
                top_videos,
                x='view_count',
                y='title',
                orientation='h',
                title="Top 10 Vídeos por Visualizações",
                hover_data=['channel_title']
            )
            fig_top_videos.update_layout(height=500)
            st.plotly_chart(fig_top_videos, use_container_width=True)
        
        with col2:
            # Distribuição de engagement
            if 'engagement_rate' in df_youtube.columns:
                fig_engagement = px.histogram(
                    df_youtube,
                    x='engagement_rate',
                    title="Distribuição de Taxa de Engajamento",
                    nbins=30
                )
                st.plotly_chart(fig_engagement, use_container_width=True)
        
        # Análise por região
        if filters.get('show_regional_analysis', True):
            self.render_regional_analysis(data.get('regional_data', pd.DataFrame()))
        
        # Métricas de engajamento
        st.subheader("Métricas de Engajamento")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            avg_views = df_youtube['view_count'].mean()
            st.metric("Views Médias", f"{avg_views:,.0f}")
        
        with col2:
            avg_likes = df_youtube['like_count'].mean()
            st.metric("Likes Médios", f"{avg_likes:,.0f}")
        
        with col3:
            avg_engagement = (df_youtube['like_count'] / df_youtube['view_count']).mean()
            st.metric("Engajamento Médio", f"{avg_engagement:.4f}")
        
        with col4:
            total_channels = df_youtube['channel_title'].nunique()
            st.metric("Canais Únicos", f"{total_channels:,}")
    
    def render_regional_analysis(self, df_regional: pd.DataFrame):
        """Renderiza análise regional."""
        if df_regional.empty:
            return
        
        st.subheader("Análise Regional")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Views por região
            fig_regional_views = px.bar(
                df_regional,
                x='region_code',
                y='total_views',
                title="Total de Views por Região"
            )
            st.plotly_chart(fig_regional_views, use_container_width=True)
        
        with col2:
            # Engajamento por região
            if 'avg_engagement_rate' in df_regional.columns:
                fig_regional_engagement = px.bar(
                    df_regional,
                    x='region_code',
                    y='avg_engagement_rate',
                    title="Taxa de Engajamento Média por Região"
                )
                st.plotly_chart(fig_regional_engagement, use_container_width=True)
    
    def render_correlation_analysis(self, data: Dict[str, pd.DataFrame]):
        """Renderiza análise de correlações."""
        st.header("Análise de Correlações Spotify-YouTube")
        
        if 'correlated_data' not in data or data['correlated_data'].empty:
            st.warning("Nenhum dado de correlação disponível.")
            return
        
        df_corr = data['correlated_data'].copy()
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Distribuição de scores de similaridade
            fig_similarity = px.histogram(
                df_corr,
                x='similarity_score',
                title="Distribuição de Scores de Similaridade",
                nbins=20
            )
            st.plotly_chart(fig_similarity, use_container_width=True)
        
        with col2:
            # Força das correlações
            if 'correlation_strength' in df_corr.columns:
                strength_dist = df_corr['correlation_strength'].value_counts()
                fig_strength = px.pie(
                    values=strength_dist.values,
                    names=strength_dist.index,
                    title="Distribuição da Força das Correlações"
                )
                st.plotly_chart(fig_strength, use_container_width=True)
        
        # Scatter plot: Popularidade vs Views
        if all(col in df_corr.columns for col in ['spotify_popularity', 'youtube_view_count']):
            fig_scatter = px.scatter(
                df_corr,
                x='spotify_popularity',
                y='youtube_view_count',
                title="Popularidade Spotify vs Views YouTube",
                hover_data=['spotify_track_name', 'youtube_title'],
                log_y=True
            )
            st.plotly_chart(fig_scatter, use_container_width=True)
        
        # Top correlações
        st.subheader("Top Correlações")
        top_correlations = df_corr.nlargest(10, 'similarity_score')[
            ['spotify_track_name', 'spotify_artist_name', 'youtube_title', 'similarity_score']
        ]
        st.dataframe(top_correlations, use_container_width=True)
    
    def render_data_explorer(self, data: Dict[str, pd.DataFrame]):
        """Renderiza explorador de dados."""
        st.header("Explorador de Dados")
        
        # Seleção de dataset
        available_datasets = [name for name, df in data.items() if not df.empty]
        
        if not available_datasets:
            st.warning("Nenhum dataset disponível.")
            return
        
        selected_dataset = st.selectbox("Selecione o dataset:", available_datasets)
        
        if selected_dataset:
            df = data[selected_dataset]
            
            col1, col2 = st.columns([3, 1])
            
            with col2:
                st.subheader("Informações do Dataset")
                st.write(f"**Registros:** {len(df):,}")
                st.write(f"**Colunas:** {len(df.columns)}")
                st.write(f"**Memória:** {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
                
                # Filtros
                st.subheader("Filtros")
                num_rows = st.slider("Número de linhas:", 10, min(1000, len(df)), 100)
                
                # Seleção de colunas
                selected_columns = st.multiselect(
                    "Colunas:",
                    df.columns.tolist(),
                    default=df.columns.tolist()[:10]
                )
            
            with col1:
                st.subheader(f"Dataset: {selected_dataset}")
                
                if selected_columns:
                    display_df = df[selected_columns].head(num_rows)
                    st.dataframe(display_df, use_container_width=True)
                    
                    # Estatísticas descritivas
                    st.subheader("Estatísticas Descritivas")
                    numeric_cols = display_df.select_dtypes(include=[np.number]).columns
                    if len(numeric_cols) > 0:
                        st.dataframe(display_df[numeric_cols].describe(), use_container_width=True)
    
    def run(self):
        """Executa o dashboard."""
        # Título principal
        st.title("Spotify-YouTube Analytics Dashboard")
        st.markdown("Dashboard interativo para análise de tendências musicais e audiovisuais")
        
        # Carrega dados
        with st.spinner("Carregando dados..."):
            data = self.load_data()
        
        if not any(not df.empty for df in data.values()):
            st.error("Nenhum dado encontrado no banco de dados. Execute o pipeline ETL primeiro.")
            st.stop()
        
        # Renderiza sidebar com filtros
        filters = self.render_sidebar(data)
        
        # Aplica filtros aos dados
        filtered_data = self.apply_filters(data, filters)
        
        # Cria métricas derivadas
        enhanced_data = self.create_derived_metrics(filtered_data)
        
        # Exibe informações sobre filtros aplicados
        if any(filters.get(key) for key in ['selected_artists', 'selected_pop_categories', 'selected_duration']):
            original_count = len(data.get('spotify_tracks', pd.DataFrame()))
            filtered_count = len(enhanced_data.get('spotify_tracks', pd.DataFrame()))
            if filtered_count < original_count:
                st.info(f"Filtros aplicados: {filtered_count:,} de {original_count:,} faixas exibidas")
        
        # Menu de navegação
        tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
            "Visão Geral", 
            "Spotify", 
            "YouTube", 
            "Correlações", 
            "Geografia",
            "Métricas Derivadas",
            "Explorador"
        ])
        
        with tab1:
            self.render_overview(enhanced_data)
        
        with tab2:
            self.render_spotify_analysis(enhanced_data, filters)
        
        with tab3:
            self.render_youtube_analysis(enhanced_data, filters)
        
        with tab4:
            if filters.get('show_correlations', True):
                self.render_correlation_analysis(enhanced_data)
        
        with tab5:
            self.create_geographic_visualizations(enhanced_data)
        
        with tab6:
            self.render_derived_metrics(enhanced_data)
        
        with tab7:
            self.render_data_explorer(enhanced_data)
        
        # Footer
        st.markdown("---")
        st.markdown("**Spotify-YouTube Analytics Dashboard** | Desenvolvido com Streamlit")


def main():
    """Função principal."""
    dashboard = SpotifyYouTubeDashboard()
    dashboard.run()


if __name__ == "__main__":
    main()