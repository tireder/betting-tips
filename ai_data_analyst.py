import tempfile
import csv
import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from agno.agent import Agent
from agno.models.openai import OpenAIChat
from agno.tools.duckdb import DuckDbTools
from agno.tools.pandas import PandasTools
import threading
import time
import math

# Import our custom modules
try:
    from api_football import APIFootball, fetch_all_winner_fixtures, WINNER_LEAGUES
    from data_merger import DataMerger, ValueBetCalculator, analyze_merged_match, get_top_bets, generate_accumulators
    from betting_panel import format_betting_panel, format_top_bets_table, format_accumulator, generate_full_report, format_fixture_card
    API_MODULES_AVAILABLE = True
except ImportError as e:
    API_MODULES_AVAILABLE = False
    print(f"Warning: API modules not available: {e}")

# Import team history cache for enhanced analytics
try:
    from team_history import TeamHistoryCache, TeamHistoryFetcher, get_team_history_cache
    TEAM_CACHE_AVAILABLE = True
except ImportError as e:
    TEAM_CACHE_AVAILABLE = False
    print(f"Warning: Team history cache not available: {e}")

# AllSportsAPI removed - using API-Football V3 only

# Initialize team history cache (singleton)
def get_cache():
    """Get the team history cache instance"""
    if TEAM_CACHE_AVAILABLE:
        return get_team_history_cache()
    return None

# ============== MODERN UI STYLING ==============
MODERN_CSS = """
<style>
/* Modern Dark Theme */
:root {
    --primary: #6366f1;
    --primary-dark: #4f46e5;
    --secondary: #22d3ee;
    --success: #22c55e;
    --warning: #f59e0b;
    --danger: #ef4444;
    --dark: #0f172a;
    --dark-light: #1e293b;
    --text: #f1f5f9;
    --text-muted: #94a3b8;
}

/* Stat Cards */
.stat-card {
    background: linear-gradient(135deg, var(--dark-light) 0%, var(--dark) 100%);
    border-radius: 16px;
    padding: 20px;
    margin: 10px 0;
    border: 1px solid rgba(99, 102, 241, 0.2);
    box-shadow: 0 4px 20px rgba(0,0,0,0.3);
    transition: transform 0.3s, box-shadow 0.3s;
}
.stat-card:hover {
    transform: translateY(-5px);
    box-shadow: 0 8px 30px rgba(99, 102, 241, 0.3);
}
.stat-value {
    font-size: 2.5rem;
    font-weight: 700;
    background: linear-gradient(135deg, var(--primary) 0%, var(--secondary) 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
}
.stat-label {
    font-size: 0.9rem;
    color: var(--text-muted);
    text-transform: uppercase;
    letter-spacing: 1px;
}

/* Bet Cards */
.bet-card {
    background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
    border-radius: 12px;
    padding: 16px;
    margin: 8px 0;
    border-left: 4px solid var(--primary);
    transition: all 0.3s;
}
.bet-card:hover {
    border-left-color: var(--secondary);
    background: linear-gradient(135deg, #1e1e3f 0%, #1a2744 100%);
}
.bet-card.high-conf {
    border-left-color: var(--success);
}
.bet-card.med-conf {
    border-left-color: var(--warning);
}
.bet-card.low-conf {
    border-left-color: var(--danger);
}

/* Match Header */
.match-header {
    background: linear-gradient(90deg, var(--primary) 0%, var(--primary-dark) 100%);
    border-radius: 12px 12px 0 0;
    padding: 15px 20px;
    color: white;
    font-weight: 600;
}

/* Progress Bars */
.prob-bar {
    height: 8px;
    border-radius: 4px;
    background: var(--dark);
    overflow: hidden;
    margin: 8px 0;
}
.prob-fill {
    height: 100%;
    border-radius: 4px;
    transition: width 0.5s ease;
}
.prob-fill.high { background: linear-gradient(90deg, var(--success) 0%, #4ade80 100%); }
.prob-fill.med { background: linear-gradient(90deg, var(--warning) 0%, #fbbf24 100%); }
.prob-fill.low { background: linear-gradient(90deg, var(--danger) 0%, #f87171 100%); }

/* Hexagon Stats */
.hex-container {
    display: flex;
    justify-content: center;
    align-items: center;
    padding: 20px;
}

/* Value Badge */
.value-badge {
    display: inline-block;
    padding: 4px 12px;
    border-radius: 20px;
    font-size: 0.8rem;
    font-weight: 600;
}
.value-badge.positive {
    background: rgba(34, 197, 94, 0.2);
    color: var(--success);
    border: 1px solid var(--success);
}
.value-badge.negative {
    background: rgba(239, 68, 68, 0.2);
    color: var(--danger);
    border: 1px solid var(--danger);
}

/* Table Styling */
.modern-table {
    width: 100%;
    border-collapse: separate;
    border-spacing: 0 8px;
}
.modern-table th {
    background: var(--primary);
    color: white;
    padding: 12px 16px;
    text-align: left;
    font-weight: 600;
}
.modern-table td {
    background: var(--dark-light);
    padding: 12px 16px;
    border: none;
}
.modern-table tr:hover td {
    background: rgba(99, 102, 241, 0.1);
}

/* Accumulator Card */
.acc-card {
    background: linear-gradient(135deg, #0f172a 0%, #1e1b4b 100%);
    border-radius: 16px;
    padding: 20px;
    margin: 15px 0;
    border: 2px solid transparent;
    background-clip: padding-box;
    position: relative;
}
.acc-card::before {
    content: '';
    position: absolute;
    top: 0; right: 0; bottom: 0; left: 0;
    z-index: -1;
    margin: -2px;
    border-radius: inherit;
    background: linear-gradient(135deg, var(--primary), var(--secondary));
}

/* Confidence Indicators */
.conf-high { color: #22c55e; }
.conf-med { color: #f59e0b; }
.conf-low { color: #ef4444; }

/* Animation */
@keyframes pulse {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.7; }
}
.live-indicator {
    animation: pulse 2s infinite;
    color: var(--danger);
}

/* Under Maintenance Card */
.maintenance-card {
    background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
    border-radius: 20px;
    padding: 40px;
    margin: 30px auto;
    max-width: 600px;
    border: 2px dashed #f59e0b;
    text-align: center;
    box-shadow: 0 10px 40px rgba(245, 158, 11, 0.1);
}
.maintenance-icon {
    font-size: 80px;
    margin-bottom: 20px;
    animation: bounce 2s ease-in-out infinite;
}
@keyframes bounce {
    0%, 100% { transform: translateY(0); }
    50% { transform: translateY(-10px); }
}
.maintenance-title {
    font-size: 2rem;
    font-weight: 700;
    color: #f59e0b;
    margin-bottom: 15px;
}
.maintenance-text {
    font-size: 1.1rem;
    color: #94a3b8;
    margin-bottom: 25px;
    line-height: 1.6;
}
.maintenance-badge {
    display: inline-block;
    background: rgba(245, 158, 11, 0.2);
    color: #f59e0b;
    padding: 8px 20px;
    border-radius: 20px;
    font-weight: 600;
    border: 1px solid #f59e0b;
}

/* Team Hexagon in Match Card */
.match-hexagon-container {
    display: flex;
    justify-content: space-around;
    align-items: center;
    padding: 15px;
    background: rgba(99, 102, 241, 0.05);
    border-radius: 12px;
    margin: 15px 0;
}
.hex-team-label {
    text-align: center;
    font-weight: 600;
    color: var(--text);
    margin-bottom: 10px;
}

/* Form Indicator */
.form-badge {
    display: inline-flex;
    gap: 3px;
    padding: 4px 8px;
    border-radius: 8px;
    background: var(--dark-light);
}
.form-badge span {
    width: 20px;
    height: 20px;
    border-radius: 4px;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 11px;
    font-weight: 700;
}
.form-w { background: #22c55e; color: white; }
.form-d { background: #94a3b8; color: white; }
.form-l { background: #ef4444; color: white; }

/* Cache Stats Card */
.cache-stats {
    background: linear-gradient(135deg, var(--dark-light) 0%, var(--dark) 100%);
    border-radius: 12px;
    padding: 15px;
    margin: 10px 0;
    border: 1px solid rgba(99, 102, 241, 0.2);
}
.cache-stat-item {
    display: flex;
    justify-content: space-between;
    padding: 8px 0;
    border-bottom: 1px solid rgba(255,255,255,0.05);
}
.cache-stat-item:last-child {
    border-bottom: none;
}

/* Mobile Responsive */
@media (max-width: 768px) {
    .stat-card { padding: 15px; }
    .stat-value { font-size: 1.8rem; }
    .match-hexagon-container { flex-direction: column; gap: 20px; }
    .maintenance-card { padding: 25px; margin: 15px; }
    .maintenance-icon { font-size: 60px; }
    .maintenance-title { font-size: 1.5rem; }
}
</style>
"""

def generate_hexagon_svg(stats: dict, size: int = 200) -> str:
    """
    Generate SVG hexagon radar chart for team stats.
    stats should contain keys like 'attack', 'defense', 'form', 'home', 'away', 'consistency'
    with values 0-100.
    """
    cx, cy = size // 2, size // 2
    radius = size * 0.4
    
    # 6 stats for hexagon
    labels = ['Attack', 'Defense', 'Form', 'Home', 'Away', 'Consistency']
    stat_keys = ['attack', 'defense', 'form', 'home', 'away', 'consistency']
    
    # Calculate points for the outer hexagon
    outer_points = []
    for i in range(6):
        angle = math.pi / 2 + (2 * math.pi * i / 6)
        x = cx + radius * math.cos(angle)
        y = cy - radius * math.sin(angle)
        outer_points.append((x, y))
    
    # Calculate points for the stat polygon
    stat_points = []
    for i, key in enumerate(stat_keys):
        value = stats.get(key, 50) / 100  # Normalize to 0-1
        angle = math.pi / 2 + (2 * math.pi * i / 6)
        x = cx + radius * value * math.cos(angle)
        y = cy - radius * value * math.sin(angle)
        stat_points.append((x, y))
    
    # Build SVG
    outer_path = ' '.join([f"{x},{y}" for x, y in outer_points])
    stat_path = ' '.join([f"{x},{y}" for x, y in stat_points])
    
    # Grid lines (25%, 50%, 75%)
    grid_lines = ""
    for pct in [0.25, 0.5, 0.75]:
        grid_points = []
        for i in range(6):
            angle = math.pi / 2 + (2 * math.pi * i / 6)
            x = cx + radius * pct * math.cos(angle)
            y = cy - radius * pct * math.sin(angle)
            grid_points.append((x, y))
        grid_path = ' '.join([f"{x},{y}" for x, y in grid_points])
        grid_lines += f'<polygon points="{grid_path}" fill="none" stroke="#334155" stroke-width="1" opacity="0.5"/>'
    
    # Axis lines
    axis_lines = ""
    for i in range(6):
        angle = math.pi / 2 + (2 * math.pi * i / 6)
        x2 = cx + radius * math.cos(angle)
        y2 = cy - radius * math.sin(angle)
        axis_lines += f'<line x1="{cx}" y1="{cy}" x2="{x2}" y2="{y2}" stroke="#334155" stroke-width="1" opacity="0.5"/>'
    
    # Labels
    label_elements = ""
    for i, label in enumerate(labels):
        angle = math.pi / 2 + (2 * math.pi * i / 6)
        lx = cx + (radius + 25) * math.cos(angle)
        ly = cy - (radius + 25) * math.sin(angle)
        value = stats.get(stat_keys[i], 50)
        color = "#22c55e" if value >= 70 else "#f59e0b" if value >= 50 else "#ef4444"
        label_elements += f'<text x="{lx}" y="{ly}" text-anchor="middle" fill="{color}" font-size="11" font-weight="600">{label}</text>'
        label_elements += f'<text x="{lx}" y="{ly + 12}" text-anchor="middle" fill="#94a3b8" font-size="10">{value}%</text>'
    
    svg = f'''
    <svg width="{size}" height="{size}" viewBox="0 0 {size} {size}" xmlns="http://www.w3.org/2000/svg">
        <defs>
            <linearGradient id="statGrad" x1="0%" y1="0%" x2="100%" y2="100%">
                <stop offset="0%" style="stop-color:#6366f1;stop-opacity:0.8"/>
                <stop offset="100%" style="stop-color:#22d3ee;stop-opacity:0.8"/>
            </linearGradient>
        </defs>
        <!-- Background hex -->
        <polygon points="{outer_path}" fill="#1e293b" stroke="#4f46e5" stroke-width="2"/>
        <!-- Grid -->
        {grid_lines}
        <!-- Axes -->
        {axis_lines}
        <!-- Stat polygon -->
        <polygon points="{stat_path}" fill="url(#statGrad)" stroke="#6366f1" stroke-width="2" opacity="0.85"/>
        <!-- Stat points -->
        {''.join([f'<circle cx="{x}" cy="{y}" r="4" fill="#22d3ee" stroke="white" stroke-width="1"/>' for x, y in stat_points])}
        <!-- Labels -->
        {label_elements}
    </svg>
    '''
    return svg

def render_stat_card(label: str, value: str, icon: str = "📊", color: str = "primary") -> str:
    """Render a modern stat card"""
    colors = {
        "primary": "#6366f1",
        "success": "#22c55e",
        "warning": "#f59e0b",
        "danger": "#ef4444",
        "info": "#22d3ee"
    }
    c = colors.get(color, colors["primary"])
    return f'''
    <div class="stat-card">
        <div style="font-size: 1.5rem; margin-bottom: 8px;">{icon}</div>
        <div class="stat-value" style="background: linear-gradient(135deg, {c} 0%, #22d3ee 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent;">{value}</div>
        <div class="stat-label">{label}</div>
    </div>
    '''

def render_bet_card(match: str, bet: str, prob: float, odds: float = None, confidence: str = "med") -> str:
    """Render a modern bet card"""
    conf_class = f"bet-card {confidence.lower()}-conf"
    conf_color = {"high": "#22c55e", "med": "#f59e0b", "low": "#ef4444"}.get(confidence.lower(), "#f59e0b")
    prob_width = min(prob * 100, 100)
    prob_class = "high" if prob >= 0.7 else "med" if prob >= 0.55 else "low"
    
    odds_html = f'<span style="color: #22d3ee; font-weight: 600;">@ {odds:.2f}</span>' if odds else ''
    
    return f'''
    <div class="{conf_class}">
        <div style="display: flex; justify-content: space-between; align-items: center;">
            <div>
                <div style="font-weight: 600; color: #f1f5f9; font-size: 1.1rem;">⚽ {match}</div>
                <div style="color: #94a3b8; margin-top: 4px;">🎯 {bet} {odds_html}</div>
            </div>
            <div style="text-align: right;">
                <div style="font-size: 1.5rem; font-weight: 700; color: {conf_color};">{prob*100:.1f}%</div>
                <div style="font-size: 0.8rem; color: {conf_color}; text-transform: uppercase;">{confidence}</div>
            </div>
        </div>
        <div class="prob-bar">
            <div class="prob-fill {prob_class}" style="width: {prob_width};"></div>
        </div>
    </div>
    '''


def render_match_hexagons(home_team: str, away_team: str, home_stats: dict, away_stats: dict, size: int = 180) -> str:
    """Render side-by-side hexagon charts for match analysis"""
    home_hex = generate_hexagon_svg(home_stats, size)
    away_hex = generate_hexagon_svg(away_stats, size)
    
    return f'''
    <div class="match-hexagon-container">
        <div style="text-align: center;">
            <div class="hex-team-label">🏠 {home_team}</div>
            {home_hex}
        </div>
        <div style="font-size: 24px; color: #6366f1; font-weight: bold;">VS</div>
        <div style="text-align: center;">
            <div class="hex-team-label">✈️ {away_team}</div>
            {away_hex}
        </div>
    </div>
    '''


def render_form_badge(form_string: str) -> str:
    """Render a visual form badge (e.g., WWDLW)"""
    if not form_string:
        return '<span style="color: #94a3b8;">No form data</span>'
    
    badges = []
    for result in form_string[:5]:
        if result.upper() == 'W':
            badges.append('<span class="form-w">W</span>')
        elif result.upper() == 'D':
            badges.append('<span class="form-d">D</span>')
        elif result.upper() == 'L':
            badges.append('<span class="form-l">L</span>')
    
    return f'<div class="form-badge">{"".join(badges)}</div>'


def render_maintenance_page(sport: str = "Basketball", icon: str = "🏀") -> str:
    """Render an under maintenance page for sports not yet available"""
    return f'''
    <div class="maintenance-card">
        <div class="maintenance-icon">{icon}</div>
        <div class="maintenance-title">{sport} Coming Soon!</div>
        <div class="maintenance-text">
            We're working hard to bring you {sport.lower()} analytics with the same 
            powerful AI-driven insights you love for soccer. Stay tuned for updates!
        </div>
        <div class="maintenance-badge">🚧 Under Development</div>
        <div style="margin-top: 25px; color: #64748b; font-size: 0.9rem;">
            Expected features:
            <ul style="text-align: left; display: inline-block; margin-top: 10px;">
                <li>Point spread analysis</li>
                <li>Over/Under totals predictions</li>
                <li>Team performance hexagon stats</li>
                <li>Historical matchup data</li>
                <li>AI-powered game predictions</li>
            </ul>
        </div>
    </div>
    '''


def render_cache_stats_card(cache_stats: dict) -> str:
    """Render cache statistics display"""
    return f'''
    <div class="cache-stats">
        <div style="font-weight: 600; color: #f1f5f9; margin-bottom: 10px;">📊 Cache Statistics</div>
        <div class="cache-stat-item">
            <span style="color: #94a3b8;">Teams Cached</span>
            <span style="color: #22c55e; font-weight: 600;">{cache_stats.get('teams', 0)}</span>
        </div>
        <div class="cache-stat-item">
            <span style="color: #94a3b8;">Matches Stored</span>
            <span style="color: #22d3ee; font-weight: 600;">{cache_stats.get('matches', 0)}</span>
        </div>
        <div class="cache-stat-item">
            <span style="color: #94a3b8;">H2H Records</span>
            <span style="color: #f59e0b; font-weight: 600;">{cache_stats.get('h2h_records', 0)}</span>
        </div>
        <div class="cache-stat-item">
            <span style="color: #94a3b8;">Form Records</span>
            <span style="color: #6366f1; font-weight: 600;">{cache_stats.get('form_records', 0)}</span>
        </div>
    </div>
    '''


def get_team_stats_for_match(home_team: str, away_team: str, team_stats_df: pd.DataFrame) -> tuple:
    """
    Get hexagon stats for both teams in a match from the team stats dataframe.
    Returns (home_stats_dict, away_stats_dict) for use with generate_hexagon_svg.
    """
    home_stats = {'attack': 50, 'defense': 50, 'form': 50, 'home': 55, 'away': 45, 'consistency': 50}
    away_stats = {'attack': 50, 'defense': 50, 'form': 50, 'home': 45, 'away': 55, 'consistency': 50}
    
    if team_stats_df is None or team_stats_df.empty:
        return home_stats, away_stats
    
    # Find home team
    home_match = team_stats_df[team_stats_df['team'].str.lower().str.contains(home_team.lower(), na=False)]
    if not home_match.empty:
        t = home_match.iloc[0].to_dict()
        home_stats = calculate_advanced_team_stats(t)
    
    # Find away team
    away_match = team_stats_df[team_stats_df['team'].str.lower().str.contains(away_team.lower(), na=False)]
    if not away_match.empty:
        t = away_match.iloc[0].to_dict()
        away_stats = calculate_advanced_team_stats(t)
    
    return home_stats, away_stats

def calculate_advanced_team_stats(team_data: dict) -> dict:
    """Calculate advanced stats for hexagon visualization"""
    attack = team_data.get('attacking_rating', 50)
    defense = team_data.get('defensive_rating', 50)
    overall = team_data.get('overall_strength', 50)
    home_matches = team_data.get('home_matches', 1)
    away_matches = team_data.get('away_matches', 1)
    total_matches = team_data.get('matches', 1)
    avg_win = team_data.get('avg_win_prob', 0.5) * 100
    
    # Calculate form (based on recent win probability trend)
    form = min(avg_win * 1.2, 100)  # Boost form slightly based on win prob
    
    # Home strength
    home_ratio = home_matches / max(total_matches, 1)
    home = min(overall * (1 + home_ratio * 0.2), 100)
    
    # Away strength (typically lower)
    away_ratio = away_matches / max(total_matches, 1)
    away = min(overall * (1 - (1 - away_ratio) * 0.15), 100)
    
    # Consistency (lower variance = higher consistency)
    consistency = min(100 - abs(attack - defense) * 0.5, 100)
    
    return {
        'attack': round(attack, 1),
        'defense': round(defense, 1),
        'form': round(form, 1),
        'home': round(home, 1),
        'away': round(away, 1),
        'consistency': round(consistency, 1)
    }

# ============== SUPPORTED LEAGUES ==============
SUPPORTED_LEAGUES = {
    # UEFA Competitions (International)
    'UEFA Champions League', 'Champions League', 'UEFA - Champions League',
    'UEFA Europa League', 'Europa League', 'UEFA - Europa League',
    'UEFA Conference League', 'Conference League', 'UEFA - Conference League',
    'UEFA Euro Championship', 'Euro Championship', 'UEFA - Euro Championship',
    'UEFA Nations League', 'Nations League',
    'UEFA Super Cup', 'Super Cup',
    'UEFA Youth League',
    # Major European Leagues
    'Bundesliga', '2. Bundesliga', '3. Liga', 'Regionalliga Nord', 'Regionalliga Nordost', 
    'Regionalliga West', 'Regionalliga Sudwest', 'Regionalliga Bayern',
    'La Liga', 'La Liga 2', 'Primera RFEF', 'Segunda RFEF', 'Copa del Rey',
    'Serie A', 'Serie B', 'Serie C - Group A', 'Serie C - Group B', 'Serie C - Group C', 'Coppa Italia Serie C',
    'Ligue 1', 'Ligue 2', 'Championnat National', 'Coupe de France',
    'Premier League', 'Championship', 'League One', 'League Two', 'National League',
    'Eredivisie', 'Eerste Divisie',
    'Primeira Liga', 'Liga Portugal 2', 'Liga 3', 'Portuguese Cup',
    'Belgian First Division A', 'Belgian First Division B',
    'Austrian Bundesliga', 'Austrian 2. Liga', 'Regionalliga East',
    'Swiss Super League', 'Swiss Challenge League', 'Swiss Promotion League',
    # UK & Ireland
    'Scottish Premiership', 'Scottish Championship', 'Scottish League One', 'Scottish League Two',
    'Scottish Challenge Cup', 'Scottish Cup',
    'Welsh Premier League', 'Welsh Second Division North', 'Welsh Second Division South',
    'Isthmian Premier League', 'Southern League Central', 'Southern League South',
    'English Regional North', 'English Regional South', 'English Seventh Division North',
    # Scandinavia
    'Danish Superliga', 'Danish 1st Division', 'Danish 2nd Division',
    'Eliteserien', 'Norwegian First Division', 'Norwegian Second Division', 'Norwegian Women\'s First Division',
    'Allsvenskan', 'Superettan',
    # Eastern Europe
    'Ekstraklasa', 'Polish First League', 'Polish Third League', 'Polish Cup',
    'Czech First League', 'Czech National League', 'Czech Third League',
    'Slovak Super Liga', 'Slovak Cup',
    'Romanian Liga I', 'Romanian Liga II', 'Romanian Liga III', 'Romanian Cup',
    'Bulgarian First League', 'Bulgarian Second League',
    'Croatian First League', 'Croatian Second League',
    'Serbian SuperLiga', 'Serbian First League',
    'Slovenian PrvaLiga', 'Slovenian Second League', 'Slovenian Cup',
    'Hungarian First Division', 'NB I',
    'Greek Super League',
    'Cypriot First Division', 'Cypriot Second Division',
    'Ukrainian Premier League',
    'Russian Premier League', 'Russian First League', 'Russian Cup',
    'Belarus Premier League', 'Belarus First League', 'Belarus Cup',
    'Moldovan National Division',
    'Armenian Premier League',
    'Georgian First Division', 'Erovnuli Liga', 'Georgian Second Division',
    'Azerbaijan Premier League',
    'Montenegrin First League', 'Montenegrin Second League',
    'Macedonian First League',
    'Kosovo Superleague',
    'Albanian Superliga',
    'Premier League of Bosnia',
    'Estonian Meistriliiga',
    'Latvian Higher League',
    # Middle East & Africa
    'Turkish Super Lig', 'Turkish First League', 'Turkish Second League', 'Turkish Third League',
    'Israeli Premier League', 'Ligat Ha\'Al', 'Liga Leumit', 'State Cup', 'Toto Cup',
    'Saudi Pro League', 'Saudi First Division', 'King\'s Cup',
    'UAE Pro League', 'UAE League Cup',
    'Qatar Stars League', 'Qatari Stars Cup',
    'Egyptian Premier League', 'Egyptian Second Division', 'Egypt Cup',
    'Algerian Ligue 1',
    'Moroccan First Division', 'Botola Pro', 'Botola 2',
    'Tunisian Ligue Professionnelle 1', 'Tunisian Ligue 2',
    'South African Premier Division',
    'Ghana Premier League',
    'Nigeria Premier League',
    'Kenya Premier League',
    'Uganda Premier League',
    'Ethiopian Premier League',
    'Senegal Premier League',
    'Girabola', 'Angola Premier League',
    # Americas
    'MLS',
    'Liga MX', 'Liga de Expansión MX',
    'Costa Rican Primera Division', 'Costa Rican Second Division',
    'Honduran Liga Nacional',
    'Nicaraguan Primera Division',
    'El Salvador Primera Division',
    'Primera Division Argentina', 'Primera Nacional', 'Primera B Metropolitana',
    'Serie A Brazil', 'Serie B Brazil',
    'Chilean Primera Division', 'Primera B Chile', 'Segunda Division Chile',
    'Colombian Primera A', 'Colombian Primera B', 'Copa Colombia',
    'Peruvian Primera Division',
    'Ecuadorian Serie A', 'Ecuador Cup',
    'Bolivian Primera Division',
    'Paraguayan Primera Division', 'Copa Paraguay',
    'Uruguayan Primera Division', 'Uruguayan Segunda Division',
    # Asia & Oceania
    'A-League',
    'J2 League', 'J3 League',
    'K League 1', 'K League 2',
    'Chinese Super League',
    'Thai League 1', 'Thai League 2',
    'Malaysian Super League',
    'Indonesian Liga 1',
    'Vietnamese Cup',
    'Jordan League 2',
}

# Countries/regions that are NOT supported by Winner
UNSUPPORTED_REGIONS = {
    'ivory coast', 'cote d\'ivoire', 'tanzania', 'rwanda', 'mauritania', 'sudan',
    'burkina faso', 'mali', 'niger', 'chad', 'cameroon', 'senegal', 'guinea',
    'benin', 'togo', 'gabon', 'congo', 'zambia', 'zimbabwe', 'malawi', 'mozambique',
    'botswana', 'namibia', 'lesotho', 'swaziland', 'eswatini', 'madagascar',
    'mauritius', 'seychelles', 'comoros', 'djibouti', 'eritrea', 'somalia',
    'burundi', 'central african', 'equatorial guinea', 'sao tome', 'cape verde',
    'liberia', 'sierra leone', 'gambia', 'guinea-bissau',
    # Asian regions not supported
    'myanmar', 'cambodia', 'laos', 'bangladesh', 'nepal', 'bhutan', 'mongolia',
    'turkmenistan', 'tajikistan', 'kyrgyzstan', 'uzbekistan', 'afghanistan',
    # Other
    'fiji', 'vanuatu', 'solomon', 'papua', 'tahiti', 'new caledonia',
}

def is_supported_league(league_name):
    """Check if a league is in the supported leagues list - strict matching"""
    if not league_name:
        return False
    league_lower = str(league_name).lower().strip()
    
    # First check if it contains an unsupported region
    for region in UNSUPPORTED_REGIONS:
        if region in league_lower:
            return False
    
    # Then check for exact or near-exact match with supported leagues
    for supported in SUPPORTED_LEAGUES:
        supported_lower = supported.lower()
        # Exact match
        if supported_lower == league_lower:
            return True
        # Supported name is fully contained AND league doesn't have extra country prefix
        # e.g., "Premier League" matches "England Premier League" but not "Tanzania Premier League"
        if supported_lower in league_lower:
            # Check if it's a known supported country prefix
            known_prefixes = ['england', 'english', 'spain', 'spanish', 'germany', 'german', 
                            'italy', 'italian', 'france', 'french', 'portugal', 'portuguese',
                            'netherlands', 'dutch', 'belgium', 'belgian', 'scotland', 'scottish',
                            'turkey', 'turkish', 'israel', 'israeli', 'usa', 'america', 'mls',
                            'brazil', 'brazilian', 'argentina', 'argentine', 'mexico', 'mexican',
                            'greece', 'greek', 'russia', 'russian', 'ukraine', 'ukrainian',
                            'poland', 'polish', 'czech', 'austria', 'austrian', 'switzerland', 'swiss',
                            'denmark', 'danish', 'norway', 'norwegian', 'sweden', 'swedish',
                            'romania', 'romanian', 'serbia', 'serbian', 'croatia', 'croatian',
                            'japan', 'japanese', 'korea', 'korean', 'saudi', 'uae', 'qatar',
                            'australia', 'australian', 'china', 'chinese', 'uefa', 'fifa', 'copa']
            has_known_prefix = any(prefix in league_lower for prefix in known_prefixes)
            # If no prefix or has known prefix, it's supported
            if has_known_prefix or league_lower == supported_lower:
                return True
        # League name is fully contained in supported name (rare but possible)
        if league_lower in supported_lower and len(league_lower) > 5:
            return True
    
    return False

def filter_supported_leagues(df):
    """Filter dataframe to only include supported leagues"""
    if 'league' not in df.columns:
        return df
    mask = df['league'].apply(is_supported_league)
    return df[mask]

def parse_match_date(date_val):
    """Parse various date formats and return datetime object"""
    if pd.isna(date_val) or date_val == 'Unknown' or date_val == 'nan':
        return None
    
    if isinstance(date_val, datetime):
        return date_val
    
    if isinstance(date_val, pd.Timestamp):
        return date_val.to_pydatetime()
    
    date_str = str(date_val).strip()
    
    # Try multiple date formats
    formats = [
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d %H:%M',
        '%Y-%m-%d',
        '%d/%m/%Y %H:%M:%S',
        '%d/%m/%Y %H:%M',
        '%d/%m/%Y',
        '%d-%m-%Y %H:%M:%S',
        '%d-%m-%Y %H:%M',
        '%d-%m-%Y',
        '%m/%d/%Y %H:%M:%S',
        '%m/%d/%Y %H:%M',
        '%m/%d/%Y',
        '%Y/%m/%d %H:%M',
        '%Y/%m/%d',
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    
    return None

def get_matches_by_date_range(df, days_ahead=2):
    """Filter matches within the next N days"""
    if 'date' not in df.columns:
        return df
    
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = today + timedelta(days=days_ahead + 1)  # Include the end day
    
    filtered_matches = []
    for idx, row in df.iterrows():
        match_date = parse_match_date(row.get('date'))
        if match_date:
            if today <= match_date < end_date:
                filtered_matches.append(idx)
    
    if filtered_matches:
        return df.loc[filtered_matches]
    return pd.DataFrame()

# ============== PROBABILITY HELPER FUNCTIONS ==============

def normalize_probability(prob):
    """
    Normalize probability to decimal format (0-1).
    Handles both percentage (60, 75) and decimal (0.60, 0.75) formats.
    """
    if prob is None:
        return 0.0
    try:
        prob = float(prob)
        # If probability is greater than 1, assume it's a percentage
        if prob > 1:
            return prob / 100
        return prob
    except (ValueError, TypeError):
        return 0.0

def get_confidence_level(prob):
    """
    Get confidence level based on probability.
    Returns tuple of (icon_text, css_class)
    """
    # Normalize probability first
    prob = normalize_probability(prob)
    
    if prob >= 0.70:
        return "🟢 HIGH", "confidence-high"
    elif prob >= 0.60:
        return "🟡 MEDIUM", "confidence-medium"
    else:
        return "🔴 LOW", "confidence-low"

# ============== BETTING ANALYSIS FUNCTIONS ==============

def calculate_implied_probability(odds):
    """Convert decimal odds to implied probability"""
    if odds and odds > 0:
        return 1 / odds
    return None

def calculate_value_bet(probability, implied_prob, threshold=0.05):
    """
    Determine if a bet has value.
    Value exists when the predicted probability > implied probability + threshold
    """
    if probability and implied_prob:
        edge = probability - implied_prob
        return edge > threshold, edge
    return False, 0

def get_kelly_criterion(probability, odds, fraction=0.25):
    """
    Calculate Kelly Criterion stake recommendation.
    Uses fractional Kelly (default 25%) for safety.
    """
    if probability and odds and odds > 1:
        b = odds - 1
        q = 1 - probability
        p = probability
        kelly = (b * p - q) / b
        return max(0, kelly * fraction)
    return 0

def extract_odds_from_api(odds_data) -> dict:
    """Extract and structure odds from raw API response (standalone function for Tab 8)"""
    def safe_float(val):
        if val is None:
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None
    
    extracted = {
        'home_win': None,
        'draw': None,
        'away_win': None,
        'over_1.5': None,
        'under_1.5': None,
        'over_2.5': None,
        'under_2.5': None,
        'over_3.5': None,
        'under_3.5': None,
        'btts_yes': None,
        'btts_no': None,
        'bookmaker': None,
    }
    
    if not odds_data:
        return extracted
    
    # Handle list format from API-Football V3
    if isinstance(odds_data, list):
        for odds_item in odds_data:
            bookmakers = odds_item.get('bookmakers', [])
            for bookmaker in bookmakers:
                if not extracted['bookmaker']:
                    extracted['bookmaker'] = bookmaker.get('name', '')
                
                bets = bookmaker.get('bets', [])
                for bet in bets:
                    bet_name = bet.get('name', '')
                    values = bet.get('values', [])
                    
                    # Match Winner (1X2)
                    if bet_name == 'Match Winner':
                        for val in values:
                            v = val.get('value', '')
                            odd = safe_float(val.get('odd'))
                            if v == 'Home':
                                extracted['home_win'] = odd
                            elif v == 'Draw':
                                extracted['draw'] = odd
                            elif v == 'Away':
                                extracted['away_win'] = odd
                    
                    # Goals Over/Under
                    elif bet_name == 'Goals Over/Under':
                        for val in values:
                            v = val.get('value', '')
                            odd = safe_float(val.get('odd'))
                            if 'Over 1.5' in v:
                                extracted['over_1.5'] = odd
                            elif 'Under 1.5' in v:
                                extracted['under_1.5'] = odd
                            elif 'Over 2.5' in v:
                                extracted['over_2.5'] = odd
                            elif 'Under 2.5' in v:
                                extracted['under_2.5'] = odd
                            elif 'Over 3.5' in v:
                                extracted['over_3.5'] = odd
                            elif 'Under 3.5' in v:
                                extracted['under_3.5'] = odd
                    
                    # Both Teams Score
                    elif bet_name == 'Both Teams Score':
                        for val in values:
                            v = val.get('value', '')
                            odd = safe_float(val.get('odd'))
                            if v == 'Yes':
                                extracted['btts_yes'] = odd
                            elif v == 'No':
                                extracted['btts_no'] = odd
                
                # Only process first bookmaker
                break
    
    return extracted

# ============== BET OVERRIDE/CONVERSION FUNCTIONS ==============
# Available bet types for override (Double Chance removed as Winner doesn't support it)
# User can still manually select these in custom mode
AVAILABLE_BET_TYPES = [
    'Home Win', 'Draw', 'Away Win',
    'Over 0.5', 'Over 1.5', 'Over 2.5', 'Over 3.5', 'Over 4.5',
    'Under 0.5', 'Under 1.5', 'Under 2.5', 'Under 3.5', 'Under 4.5',
    'BTTS Yes', 'BTTS No'
]

# Double chance options available only for manual selection
DOUBLE_CHANCE_BET_TYPES = [
    'Home or Draw (1X)', 'Away or Draw (X2)', 'Home or Away (12)'
]

def get_bet_probability_from_row(row, bet_type):
    """
    Get or calculate probability for a specific bet type from a match row.
    Uses existing probabilities or calculates derived ones.
    """
    # Direct mappings
    direct_map = {
        'Home Win': '1x2_h',
        'Draw': '1x2_d', 
        'Away Win': '1x2_a',
        'Over 1.5': 'o_1.5',
        'Over 2.5': 'o_2.5',
        'Over 3.5': 'o_3.5',
        'Under 1.5': 'u_1.5',
        'Under 2.5': 'u_2.5',
        'Under 3.5': 'u_3.5',
    }
    
    if bet_type in direct_map:
        col = direct_map[bet_type]
        val = row.get(col)
        if pd.notna(val) and val is not None:
            return float(val)
    
    # Derived probabilities
    home_prob = float(row.get('1x2_h', 0) or 0)
    draw_prob = float(row.get('1x2_d', 0) or 0)
    away_prob = float(row.get('1x2_a', 0) or 0)
    over_25 = float(row.get('o_2.5', 0) or 0)
    under_25 = float(row.get('u_2.5', 0) or 0)
    over_15 = float(row.get('o_1.5', 0) or 0)
    over_35 = float(row.get('o_3.5', 0) or 0)
    
    # Double chance
    if bet_type == 'Home or Draw (1X)':
        return min(home_prob + draw_prob, 0.99)
    elif bet_type == 'Away or Draw (X2)':
        return min(away_prob + draw_prob, 0.99)
    elif bet_type == 'Home or Away (12)':
        return min(home_prob + away_prob, 0.99)
    
    # Estimate Over/Under from existing data
    if bet_type == 'Over 0.5':
        # Usually very high, estimate from Over 1.5
        if over_15 > 0:
            return min(over_15 * 1.15, 0.98)
        elif over_25 > 0:
            return min(over_25 * 1.25, 0.98)
        return 0.90  # Default high probability
    
    if bet_type == 'Over 1.5':
        if over_15 > 0:
            return over_15
        elif over_25 > 0:
            # Estimate: Over 1.5 is typically 10-15% higher than Over 2.5
            return min(over_25 * 1.12, 0.95)
        return None
    
    if bet_type == 'Over 2.5':
        return over_25 if over_25 > 0 else None
    
    if bet_type == 'Over 3.5':
        if over_35 > 0:
            return over_35
        elif over_25 > 0:
            # Estimate: Over 3.5 is typically 25-35% lower than Over 2.5
            return max(over_25 * 0.70, 0.10)
        return None
    
    if bet_type == 'Over 4.5':
        if over_35 > 0:
            return max(over_35 * 0.65, 0.05)
        elif over_25 > 0:
            return max(over_25 * 0.45, 0.05)
        return None
    
    if bet_type == 'Under 0.5':
        if over_15 > 0:
            return max(1 - over_15 * 1.15, 0.02)
        return 0.10  # Default low probability
    
    if bet_type == 'Under 1.5':
        if over_15 > 0:
            return 1 - over_15
        elif over_25 > 0:
            return max(1 - over_25 * 1.12, 0.05)
        return None
    
    if bet_type == 'Under 2.5':
        return under_25 if under_25 > 0 else (1 - over_25 if over_25 > 0 else None)
    
    if bet_type == 'Under 3.5':
        if over_35 > 0:
            return 1 - over_35
        elif over_25 > 0:
            return min(1 - over_25 * 0.70, 0.90)
        return None
    
    if bet_type == 'Under 4.5':
        if over_35 > 0:
            return min(1 - over_35 * 0.65, 0.95)
        elif over_25 > 0:
            return min(1 - over_25 * 0.45, 0.95)
        return None
    
    # BTTS (Both Teams To Score) - estimate from goals data
    if bet_type == 'BTTS Yes':
        # Rough estimate based on Over 2.5 and result probabilities
        if over_25 > 0:
            # If high scoring likely, BTTS more likely
            return min(over_25 * 0.85 + 0.10, 0.85)
        return None
    
    if bet_type == 'BTTS No':
        btts_yes = get_bet_probability_from_row(row, 'BTTS Yes')
        if btts_yes:
            return 1 - btts_yes
        return None
    
    # Asian Handicap - estimate based on 1X2 probabilities
    # Home -1 means home team needs to win by 2+ goals
    if bet_type == 'Home -1':
        # Strong favorite needs comfortable win
        if home_prob > 0.5:
            return max(home_prob * 0.65, 0.15)
        return max(home_prob * 0.50, 0.10)
    
    if bet_type == 'Home -2':
        # Very strong favorite, needs 3+ goal win
        if home_prob > 0.6:
            return max(home_prob * 0.45, 0.10)
        return max(home_prob * 0.30, 0.05)
    
    if bet_type == 'Away +1':
        # Away team gets 1 goal head start (wins if they win/draw/lose by 1)
        return min(away_prob + draw_prob + (home_prob * 0.35), 0.95)
    
    if bet_type == 'Away +2':
        # Away team gets 2 goals head start
        return min(away_prob + draw_prob + (home_prob * 0.55), 0.98)
    
    if bet_type == 'Home +1':
        # Home team gets 1 goal head start
        return min(home_prob + draw_prob + (away_prob * 0.35), 0.95)
    
    if bet_type == 'Home +2':
        # Home team gets 2 goals head start
        return min(home_prob + draw_prob + (away_prob * 0.55), 0.98)
    
    if bet_type == 'Away -1':
        # Away needs to win by 2+ goals
        if away_prob > 0.5:
            return max(away_prob * 0.65, 0.15)
        return max(away_prob * 0.50, 0.10)
    
    if bet_type == 'Away -2':
        # Away needs to win by 3+ goals
        if away_prob > 0.6:
            return max(away_prob * 0.45, 0.10)
        return max(away_prob * 0.30, 0.05)
    
    return None

def get_best_bet_for_match(row) -> tuple:
    """
    Find the best bet for a match based on probabilities.
    Returns (bet_type, probability)
    Focuses on Over/Under 2.5 only (Winner/Toto standard market)
    """
    # Standard Winner/Toto markets - Over/Under 2.5 ONLY for goals
    bets_to_check = [
        ('Home Win', row.get('1x2_h', 0)),
        ('Draw', row.get('1x2_d', 0)),
        ('Away Win', row.get('1x2_a', 0)),
        ('Over 2.5', row.get('o_2.5', 0)),
        ('Under 2.5', row.get('u_2.5', 0)),
    ]
    
    # Convert and find best
    best_bet = 'Home Win'
    best_prob = 0.0
    
    for bet_type, prob in bets_to_check:
        try:
            prob_val = float(prob) if prob else 0
            if prob_val > best_prob:
                best_prob = prob_val
                best_bet = bet_type
        except (ValueError, TypeError):
            continue
    
    # Double chance removed from default - Winner doesn't support it
    # User can still manually select double chance options if needed
    
    return best_bet, best_prob
    
    return best_bet, best_prob

def get_odds_for_bet_type(bookmaker_odds: dict, bet_type: str) -> float:
    """
    Get odds for a specific bet type from bookmaker odds dict.
    """
    if not bookmaker_odds:
        return None
    
    odds_mapping = {
        'Home Win': 'home_win',
        'Draw': 'draw',
        'Away Win': 'away_win',
        'Over 1.5': 'over_1.5',
        'Over 2.5': 'over_2.5',
        'Over 3.5': 'over_3.5',
        'Under 1.5': 'under_1.5',
        'Under 2.5': 'under_2.5',
        'Under 3.5': 'under_3.5',
        'BTTS Yes': 'btts_yes',
        'BTTS No': 'btts_no',
        # Asian Handicap (if available from API)
        'Home -1': 'home_minus_1',
        'Home -2': 'home_minus_2',
        'Away +1': 'away_plus_1',
        'Away +2': 'away_plus_2',
        'Home +1': 'home_plus_1',
        'Home +2': 'home_plus_2',
        'Away -1': 'away_minus_1',
        'Away -2': 'away_minus_2',
    }
    
    key = odds_mapping.get(bet_type)
    if key:
        return bookmaker_odds.get(key)
    
    return None

def apply_bet_override(match_key, original_bet, new_bet, new_probability):
    """Store a bet override in session state"""
    st.session_state.bet_overrides[match_key] = {
        'original_bet': original_bet,
        'new_bet': new_bet,
        'new_probability': new_probability,
        'timestamp': datetime.now().isoformat()
    }

def get_bet_override(match_key):
    """Get bet override for a match if exists"""
    return st.session_state.bet_overrides.get(match_key)

def clear_bet_override(match_key):
    """Clear bet override for a match"""
    if match_key in st.session_state.bet_overrides:
        del st.session_state.bet_overrides[match_key]

def get_effective_bet(match_key, original_bet, original_probability):
    """Get the effective bet (override if exists, otherwise original)"""
    override = get_bet_override(match_key)
    if override:
        return override['new_bet'], override['new_probability'], True
    return original_bet, original_probability, False

# Probability conversion ratios between bet types (based on statistical relationships)
BET_CONVERSION_RATIOS = {
    # Goals market conversions
    'over_25': {'over_15': 1.12, 'over_35': 0.70, 'over_05': 1.25, 'under_25': -1, 'under_15': 0.88, 'under_35': 1.30},
    'over_15': {'over_25': 0.89, 'over_35': 0.62, 'over_05': 1.12, 'under_15': -1, 'under_25': 1.11, 'under_35': 1.38},
    'over_35': {'over_15': 1.43, 'over_25': 1.43, 'over_05': 1.60, 'under_35': -1, 'under_25': 0.77, 'under_15': 0.72},
    'under_25': {'under_15': 0.75, 'under_35': 1.35, 'over_25': -1, 'over_15': 0.88, 'over_35': 1.30},
    'under_15': {'under_25': 1.33, 'under_35': 1.80, 'over_15': -1, 'over_25': 1.14, 'over_35': 1.39},
    'under_35': {'under_25': 0.74, 'under_15': 0.56, 'over_35': -1, 'over_25': 0.77, 'over_15': 0.72},
    # 1X2 market conversions (approximate based on typical odds relationships)
    'home_win': {'draw': 0.60, 'away_win': 0.50, 'home_or_draw': 1.40, 'away_or_draw': 0.80},
    'draw': {'home_win': 0.90, 'away_win': 0.90, 'home_or_draw': 1.25, 'away_or_draw': 1.25},
    'away_win': {'draw': 0.60, 'home_win': 0.50, 'away_or_draw': 1.40, 'home_or_draw': 0.80},
}

def convert_bet_probability(original_bet, target_bet, original_prob):
    """
    Convert probability from one bet type to another using statistical ratios.
    Returns the converted probability or None if conversion not available.
    """
    if original_bet == target_bet:
        return original_prob
    
    # Normalize bet names
    original_bet = original_bet.lower().replace(' ', '_').replace('.', '')
    target_bet = target_bet.lower().replace(' ', '_').replace('.', '')
    
    # Get conversion ratio
    if original_bet in BET_CONVERSION_RATIOS:
        conversions = BET_CONVERSION_RATIOS[original_bet]
        if target_bet in conversions:
            ratio = conversions[target_bet]
            if ratio == -1:
                # Inverse relationship (e.g., over_25 to under_25)
                return max(0.01, min(0.99, 1 - original_prob))
            else:
                # Apply ratio with bounds
                return max(0.01, min(0.99, original_prob * ratio))
    
    return None

def format_bet_type_display(bet_key):
    """Format bet key for display (e.g., 'over_25' -> 'Over 2.5')"""
    if not bet_key:
        return 'Unknown'
    
    display = bet_key.replace('_', ' ').title()
    # Fix numbers
    display = display.replace('25', '2.5').replace('15', '1.5').replace('35', '3.5')
    display = display.replace('05', '0.5').replace('45', '4.5')
    return display

def get_supported_conversions(current_bet):
    """Get list of bet types that the current bet can be converted to"""
    current_bet_key = current_bet.lower().replace(' ', '_').replace('.', '')
    if current_bet_key in BET_CONVERSION_RATIOS:
        return list(BET_CONVERSION_RATIOS[current_bet_key].keys())
    return []

# ============== SAVE/LOAD MERGED DATA FUNCTIONS ==============
import json
import base64

def save_merged_data_to_json():
    """Save merged data, analyses, and bet overrides to JSON for persistence"""
    save_data = {
        'version': '1.0',
        'saved_at': datetime.now().isoformat(),
        'merged_data': st.session_state.get('merged_data', []),
        'merged_analyses': st.session_state.get('merged_analyses', []),
        'bet_overrides': st.session_state.get('bet_overrides', {}),
        'api_fixtures': st.session_state.get('api_fixtures', []),
        'merge_cache_key': st.session_state.get('merge_cache_key', None)
    }
    
    # Convert any non-serializable objects
    def make_serializable(obj):
        if isinstance(obj, (datetime,)):
            return obj.isoformat()
        elif isinstance(obj, pd.DataFrame):
            return obj.to_dict('records')
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, (np.int64, np.int32)):
            return int(obj)
        elif isinstance(obj, (np.float64, np.float32)):
            return float(obj)
        elif isinstance(obj, dict):
            return {k: make_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [make_serializable(item) for item in obj]
        return obj
    
    save_data = make_serializable(save_data)
    return json.dumps(save_data, indent=2, default=str)

def load_merged_data_from_json(json_str):
    """Load merged data, analyses, and bet overrides from JSON"""
    try:
        data = json.loads(json_str)
        
        if data.get('merged_data'):
            st.session_state.merged_data = data['merged_data']
        if data.get('merged_analyses'):
            st.session_state.merged_analyses = data['merged_analyses']
        if data.get('bet_overrides'):
            st.session_state.bet_overrides = data['bet_overrides']
        if data.get('api_fixtures'):
            st.session_state.api_fixtures = data['api_fixtures']
        if data.get('merge_cache_key'):
            st.session_state.merge_cache_key = data['merge_cache_key']
        
        return True, f"Loaded {len(data.get('merged_data', []))} matches, {len(data.get('bet_overrides', {}))} overrides"
    except Exception as e:
        return False, f"Error loading data: {e}"

def get_bet_override_display(match_key, market='all'):
    """Get bet override for display in any tab"""
    overrides = st.session_state.get('bet_overrides', {})
    result = {}
    
    if market in ['all', '1x2'] and f"{match_key}_1x2" in overrides:
        result['1x2'] = overrides[f"{match_key}_1x2"]
    if market in ['all', 'goals'] and f"{match_key}_goals" in overrides:
        result['goals'] = overrides[f"{match_key}_goals"]
    
    return result if result else None

def render_bet_override_ui(match_key, recommendations, prefix=""):
    """Render bet override UI that can be used in any tab"""
    st.markdown("#### 🔄 Bet Override")
    st.caption("Change bet type - probability will be recalculated")
    
    # Get current recommendations for this match
    recs = recommendations if isinstance(recommendations, list) else []
    
    col_a, col_b = st.columns(2)
    
    with col_a:
        # 1X2 Market Override
        has_1x2 = any('Win' in str(r.get('bet_type', r.get('pick', ''))) or 'Draw' in str(r.get('bet_type', r.get('pick', ''))) for r in recs)
        if has_1x2:
            orig_1x2_bet = 'home_win'
            orig_1x2_prob = 0.0
            for r in recs:
                bt = str(r.get('bet_type', r.get('pick', '')))
                if 'Home' in bt and 'Win' in bt:
                    orig_1x2_bet = 'home_win'
                    orig_1x2_prob = float(r.get('adjusted_probability', r.get('probability', r.get('prob', 0))) or 0)
                    break
                elif 'Draw' in bt:
                    orig_1x2_bet = 'draw'
                    orig_1x2_prob = float(r.get('adjusted_probability', r.get('probability', r.get('prob', 0))) or 0)
                    break
                elif 'Away' in bt and 'Win' in bt:
                    orig_1x2_bet = 'away_win'
                    orig_1x2_prob = float(r.get('adjusted_probability', r.get('probability', r.get('prob', 0))) or 0)
                    break
            
            # Check if override exists
            current_override = st.session_state.bet_overrides.get(f"{match_key}_1x2", {})
            current_idx = 0
            options_1x2 = ['No Override', 'Home Win', 'Draw', 'Away Win']
            if current_override:
                override_bet = current_override.get('override_bet', '').replace('_', ' ').title()
                if override_bet in options_1x2:
                    current_idx = options_1x2.index(override_bet)
            
            override_1x2 = st.selectbox(
                "1X2 Market",
                options_1x2,
                index=current_idx,
                key=f"{prefix}1x2_{match_key}",
                help=f"Original: {format_bet_type_display(orig_1x2_bet)} ({orig_1x2_prob*100:.1f}%)"
            )
            
            if override_1x2 != 'No Override':
                target_1x2 = override_1x2.lower().replace(' ', '_')
                conv_prob = convert_bet_probability(orig_1x2_bet, target_1x2, orig_1x2_prob)
                if conv_prob is not None:
                    st.session_state.bet_overrides[f"{match_key}_1x2"] = {
                        'original_bet': orig_1x2_bet,
                        'override_bet': target_1x2,
                        'original_prob': orig_1x2_prob,
                        'converted_prob': conv_prob
                    }
                    change = (conv_prob - orig_1x2_prob) * 100
                    change_str = f"+{change:.1f}%" if change > 0 else f"{change:.1f}%"
                    st.success(f"✅ {override_1x2}: {conv_prob*100:.1f}% ({change_str})")
            elif f"{match_key}_1x2" in st.session_state.bet_overrides:
                del st.session_state.bet_overrides[f"{match_key}_1x2"]
    
    with col_b:
        # Goals Market Override
        has_goals = any('Over' in str(r.get('bet_type', r.get('pick', ''))) or 'Under' in str(r.get('bet_type', r.get('pick', ''))) for r in recs)
        if has_goals:
            orig_goals_bet = 'over_25'
            orig_goals_prob = 0.0
            for r in recs:
                bt = str(r.get('bet_type', r.get('pick', '')))
                if 'Over' in bt or 'Under' in bt:
                    if 'Over 2.5' in bt or 'over_25' in bt.lower():
                        orig_goals_bet = 'over_25'
                    elif 'Over 1.5' in bt or 'over_15' in bt.lower():
                        orig_goals_bet = 'over_15'
                    elif 'Over 3.5' in bt or 'over_35' in bt.lower():
                        orig_goals_bet = 'over_35'
                    elif 'Under 2.5' in bt or 'under_25' in bt.lower():
                        orig_goals_bet = 'under_25'
                    elif 'Under 1.5' in bt or 'under_15' in bt.lower():
                        orig_goals_bet = 'under_15'
                    elif 'Under 3.5' in bt or 'under_35' in bt.lower():
                        orig_goals_bet = 'under_35'
                    orig_goals_prob = float(r.get('adjusted_probability', r.get('probability', r.get('prob', 0))) or 0)
                    break
            
            # Check if override exists
            current_override = st.session_state.bet_overrides.get(f"{match_key}_goals", {})
            current_idx = 0
            options_goals = ['No Override', 'Over 1.5', 'Over 2.5', 'Over 3.5', 'Under 1.5', 'Under 2.5', 'Under 3.5']
            if current_override:
                override_bet = current_override.get('override_bet', '').replace('_', ' ').replace('25', '2.5').replace('15', '1.5').replace('35', '3.5').title()
                if override_bet in options_goals:
                    current_idx = options_goals.index(override_bet)
            
            override_goals = st.selectbox(
                "Goals Market",
                options_goals,
                index=current_idx,
                key=f"{prefix}goals_{match_key}",
                help=f"Original: {format_bet_type_display(orig_goals_bet)} ({orig_goals_prob*100:.1f}%)"
            )
            
            if override_goals != 'No Override':
                target_goals = override_goals.lower().replace(' ', '_').replace('.', '')
                conv_prob = convert_bet_probability(orig_goals_bet, target_goals, orig_goals_prob)
                if conv_prob is not None:
                    st.session_state.bet_overrides[f"{match_key}_goals"] = {
                        'original_bet': orig_goals_bet,
                        'override_bet': target_goals,
                        'original_prob': orig_goals_prob,
                        'converted_prob': conv_prob
                    }
                    change = (conv_prob - orig_goals_prob) * 100
                    change_str = f"+{change:.1f}%" if change > 0 else f"{change:.1f}%"
                    st.success(f"✅ {override_goals}: {conv_prob*100:.1f}% ({change_str})")
            elif f"{match_key}_goals" in st.session_state.bet_overrides:
                del st.session_state.bet_overrides[f"{match_key}_goals"]
    
    # Show current overrides
    overrides = get_bet_override_display(match_key)
    if overrides:
        st.info(f"🔄 Active overrides: {len(overrides)}")

def format_match_datetime(date_val):
    """Format date for display with time"""
    parsed = parse_match_date(date_val)
    if parsed:
        return parsed.strftime('%a %d %b %Y, %H:%M')
    return str(date_val) if date_val else 'TBD'

def analyze_match_value(row):
    """Analyze a single match for betting value"""
    date_formatted = format_match_datetime(row.get('date'))
    results = {
        'match': f"{row.get('home', 'Unknown')} vs {row.get('away', 'Unknown')}",
        'league': row.get('league', 'Unknown'),
        'date': date_formatted,
        'date_raw': row.get('date', 'Unknown'),
        'recommendations': []
    }
    
    def get_confidence(prob):
        """Standard confidence levels: HIGH >= 70%, MEDIUM >= 60%, LOW < 60%"""
        if prob >= 0.70:
            return 'HIGH'
        elif prob >= 0.60:
            return 'MEDIUM'
        else:
            return 'LOW'
    
    # Analyze 1X2 market
    home_prob = row.get('1x2_h', 0) or 0
    draw_prob = row.get('1x2_d', 0) or 0
    away_prob = row.get('1x2_a', 0) or 0
    
    # Strong favorites/underdogs (only recommend if >= 60%)
    if home_prob >= 0.60:
        results['recommendations'].append({
            'type': '1X2',
            'pick': 'Home Win',
            'probability': home_prob,
            'confidence': get_confidence(home_prob)
        })
    elif away_prob >= 0.60:
        results['recommendations'].append({
            'type': '1X2',
            'pick': 'Away Win',
            'probability': away_prob,
            'confidence': get_confidence(away_prob)
        })
    
    # Analyze Over/Under 2.5 goals (only recommend if >= 55%)
    over_25 = row.get('o_2.5', 0) or 0
    under_25 = row.get('u_2.5', 0) or 0
    
    if over_25 >= 0.55:
        results['recommendations'].append({
            'type': 'Goals',
            'pick': 'Over 2.5',
            'probability': over_25,
            'confidence': get_confidence(over_25)
        })
    elif under_25 >= 0.55:
        results['recommendations'].append({
            'type': 'Goals',
            'pick': 'Under 2.5',
            'probability': under_25,
            'confidence': get_confidence(under_25)
        })
    
    return results

def get_best_bets(df, min_probability=0.60, top_n=20, sort_by='probability', include_all_goals=False):
    """
    Extract the best betting opportunities from the dataset.
    By default, only includes Over/Under 2.5 goals. Set include_all_goals=True for 1.5/3.5.
    """
    best_bets = []
    
    for _, row in df.iterrows():
        date_formatted = format_match_datetime(row.get('date'))
        date_raw = row.get('date', '')
        parsed_date = parse_match_date(date_raw)
        
        match_info = {
            'id': row.get('id', ''),
            'match': f"{row.get('home', 'Unknown')} vs {row.get('away', 'Unknown')}",
            'home': row.get('home', 'Unknown'),
            'away': row.get('away', 'Unknown'),
            'league': row.get('league', 'Unknown'),
            'date': date_formatted,
            'date_raw': parsed_date if parsed_date else datetime(2099, 12, 31),
        }
        
        # Check 1X2 market
        home_prob = pd.to_numeric(row.get('1x2_h', 0), errors='coerce') or 0
        draw_prob = pd.to_numeric(row.get('1x2_d', 0), errors='coerce') or 0
        away_prob = pd.to_numeric(row.get('1x2_a', 0), errors='coerce') or 0
        
        if home_prob >= min_probability:
            best_bets.append({**match_info, 'bet_type': 'Home Win', 'probability': home_prob, 'market': '1X2'})
        if away_prob >= min_probability:
            best_bets.append({**match_info, 'bet_type': 'Away Win', 'probability': away_prob, 'market': '1X2'})
        
        # Check Over/Under 2.5 ONLY by default (standard Winner/Toto market)
        over_25 = pd.to_numeric(row.get('o_2.5', 0), errors='coerce') or 0
        under_25 = pd.to_numeric(row.get('u_2.5', 0), errors='coerce') or 0
        
        if over_25 >= min_probability:
            best_bets.append({**match_info, 'bet_type': 'Over 2.5', 'probability': over_25, 'market': 'Goals'})
        if under_25 >= min_probability:
            best_bets.append({**match_info, 'bet_type': 'Under 2.5', 'probability': under_25, 'market': 'Goals'})
        
        # Only include 1.5/3.5 if explicitly requested
        if include_all_goals:
            for goals in ['1.5', '3.5']:
                over_col = f'o_{goals}'
                under_col = f'u_{goals}'
                
                over_prob = pd.to_numeric(row.get(over_col, 0), errors='coerce') or 0
                under_prob = pd.to_numeric(row.get(under_col, 0), errors='coerce') or 0
                
                if over_prob >= min_probability:
                    best_bets.append({**match_info, 'bet_type': f'Over {goals}', 'probability': over_prob, 'market': 'Goals'})
                if under_prob >= min_probability:
                    best_bets.append({**match_info, 'bet_type': f'Under {goals}', 'probability': under_prob, 'market': 'Goals'})
        
        # Double Chance markets removed from default - Winner doesn't support them
        # User can still manually select these in custom accumulator builder
    
    # Sort and return top N
    best_bets_df = pd.DataFrame(best_bets)
    if not best_bets_df.empty:
        if sort_by == 'date':
            best_bets_df = best_bets_df.sort_values(['date_raw', 'probability'], ascending=[True, False]).head(top_n)
        else:
            best_bets_df = best_bets_df.sort_values('probability', ascending=False).head(top_n)
    return best_bets_df

def get_accumulator_suggestions(df, num_legs=4, min_prob=0.65):
    """Generate accumulator/parlay suggestions"""
    accumulators = []
    
    # Get high probability bets
    high_prob_bets = []
    for _, row in df.iterrows():
        home_prob = pd.to_numeric(row.get('1x2_h', 0), errors='coerce') or 0
        away_prob = pd.to_numeric(row.get('1x2_a', 0), errors='coerce') or 0
        over_25 = pd.to_numeric(row.get('o_2.5', 0), errors='coerce') or 0
        under_25 = pd.to_numeric(row.get('u_2.5', 0), errors='coerce') or 0
        
        match = f"{row.get('home', '')} vs {row.get('away', '')}"
        date_formatted = format_match_datetime(row.get('date'))
        
        if home_prob >= min_prob:
            high_prob_bets.append({'match': match, 'bet': 'Home Win', 'prob': home_prob, 'date': date_formatted})
        if away_prob >= min_prob:
            high_prob_bets.append({'match': match, 'bet': 'Away Win', 'prob': away_prob, 'date': date_formatted})
        if over_25 >= min_prob:
            high_prob_bets.append({'match': match, 'bet': 'Over 2.5', 'prob': over_25, 'date': date_formatted})
        if under_25 >= min_prob:
            high_prob_bets.append({'match': match, 'bet': 'Under 2.5', 'prob': under_25, 'date': date_formatted})
    
    # Sort by probability
    high_prob_bets = sorted(high_prob_bets, key=lambda x: x['prob'], reverse=True)
    
    # Create accumulators
    if len(high_prob_bets) >= num_legs:
        # Safe accumulator (highest probabilities)
        safe_acc = high_prob_bets[:num_legs]
        combined_prob = np.prod([b['prob'] for b in safe_acc])
        accumulators.append({
            'type': 'SAFE ACCUMULATOR',
            'legs': safe_acc,
            'combined_probability': combined_prob
        })
    
    return accumulators

def get_league_stats(df):
    """Get statistics by league"""
    league_stats = []
    
    for league in df['league'].unique():
        league_df = df[df['league'] == league]
        
        # Calculate averages
        avg_home = pd.to_numeric(league_df['1x2_h'], errors='coerce').mean()
        avg_away = pd.to_numeric(league_df['1x2_a'], errors='coerce').mean()
        avg_over25 = pd.to_numeric(league_df.get('o_2.5', pd.Series([0])), errors='coerce').mean()
        
        league_stats.append({
            'league': league,
            'matches': len(league_df),
            'avg_home_prob': round(avg_home, 3) if pd.notna(avg_home) else 0,
            'avg_away_prob': round(avg_away, 3) if pd.notna(avg_away) else 0,
            'avg_over25': round(avg_over25, 3) if pd.notna(avg_over25) else 0
        })
    
    return pd.DataFrame(league_stats).sort_values('matches', ascending=False)

def get_team_stats(df):
    """Get attacking and defensive statistics by team"""
    team_stats = {}
    
    for _, row in df.iterrows():
        home = row.get('home', 'Unknown')
        away = row.get('away', 'Unknown')
        league = row.get('league', 'Unknown')
        
        # Get probabilities
        home_prob = pd.to_numeric(row.get('1x2_h', 0), errors='coerce') or 0
        away_prob = pd.to_numeric(row.get('1x2_a', 0), errors='coerce') or 0
        draw_prob = pd.to_numeric(row.get('1x2_d', 0), errors='coerce') or 0
        over_25 = pd.to_numeric(row.get('o_2.5', 0), errors='coerce') or 0
        over_15 = pd.to_numeric(row.get('o_1.5', 0), errors='coerce') or 0
        under_25 = pd.to_numeric(row.get('u_2.5', 0), errors='coerce') or 0
        
        # Home team stats - when playing at home
        if home not in team_stats:
            team_stats[home] = {
                'team': home,
                'league': league,
                'matches': 0,
                'home_matches': 0,
                'away_matches': 0,
                'win_probs': [],
                'goals_for_indicator': [],  # Higher when team likely to score
                'goals_against_indicator': [],  # Lower when team has good defense
            }
        
        team_stats[home]['matches'] += 1
        team_stats[home]['home_matches'] += 1
        team_stats[home]['win_probs'].append(home_prob)
        # Home team attacking: high home win prob + high over goals = attacking team
        # Use home win probability as proxy for home team's attacking strength
        team_stats[home]['goals_for_indicator'].append(home_prob * 0.6 + over_25 * 0.4)
        # Home team defense: high home win prob + low goals = good defense
        # If home team wins often but games are low scoring, they have good defense
        team_stats[home]['goals_against_indicator'].append(home_prob * 0.5 + under_25 * 0.5)
        
        # Away team stats - when playing away
        if away not in team_stats:
            team_stats[away] = {
                'team': away,
                'league': league,
                'matches': 0,
                'home_matches': 0,
                'away_matches': 0,
                'win_probs': [],
                'goals_for_indicator': [],
                'goals_against_indicator': [],
            }
        
        team_stats[away]['matches'] += 1
        team_stats[away]['away_matches'] += 1
        team_stats[away]['win_probs'].append(away_prob)
        # Away team attacking: high away win prob indicates strong attack
        team_stats[away]['goals_for_indicator'].append(away_prob * 0.6 + over_25 * 0.4)
        # Away team defense: if away team wins + under goals, good defense
        team_stats[away]['goals_against_indicator'].append(away_prob * 0.5 + under_25 * 0.5)
    
    # Calculate averages
    result = []
    for team, stats in team_stats.items():
        avg_win_prob = np.mean(stats['win_probs']) if stats['win_probs'] else 0
        attacking = np.mean(stats['goals_for_indicator']) if stats['goals_for_indicator'] else 0
        defensive = np.mean(stats['goals_against_indicator']) if stats['goals_against_indicator'] else 0
        
        result.append({
            'team': team,
            'league': stats['league'],
            'matches': stats['matches'],
            'home_matches': stats['home_matches'],
            'away_matches': stats['away_matches'],
            'avg_win_prob': round(avg_win_prob, 3),
            'attacking_rating': round(attacking * 100, 1),
            'defensive_rating': round(defensive * 100, 1),
            'overall_strength': round(avg_win_prob * 100, 1)
        })
    
    return pd.DataFrame(result).sort_values('overall_strength', ascending=False)

def build_custom_accumulator(df, selected_match, selected_bet, num_additional_legs=3, min_prob=0.65):
    """Build an accumulator starting with a user-selected match and find best matches to pair with it"""
    accumulators = []
    
    # Get the selected match details
    selected_home = selected_match.get('home', '')
    selected_away = selected_match.get('away', '')
    selected_match_str = f"{selected_home} vs {selected_away}"
    
    # Get probability for selected bet
    if selected_bet == 'Home Win':
        selected_prob = pd.to_numeric(selected_match.get('1x2_h', 0), errors='coerce') or 0
    elif selected_bet == 'Away Win':
        selected_prob = pd.to_numeric(selected_match.get('1x2_a', 0), errors='coerce') or 0
    elif selected_bet == 'Draw':
        selected_prob = pd.to_numeric(selected_match.get('1x2_d', 0), errors='coerce') or 0
    elif 'Over' in selected_bet:
        goals = selected_bet.split(' ')[1]
        selected_prob = pd.to_numeric(selected_match.get(f'o_{goals}', 0), errors='coerce') or 0
    elif 'Under' in selected_bet:
        goals = selected_bet.split(' ')[1]
        selected_prob = pd.to_numeric(selected_match.get(f'u_{goals}', 0), errors='coerce') or 0
    else:
        selected_prob = 0.5
    
    selected_date = format_match_datetime(selected_match.get('date'))
    
    # First leg is the user's selection
    first_leg = {
        'match': selected_match_str,
        'bet': selected_bet,
        'prob': selected_prob,
        'date': selected_date,
        'is_user_pick': True
    }
    
    # Find best additional matches (excluding the selected one)
    additional_bets = []
    for _, row in df.iterrows():
        match_home = row.get('home', '')
        match_away = row.get('away', '')
        
        # Skip the selected match
        if match_home == selected_home and match_away == selected_away:
            continue
        
        match = f"{match_home} vs {match_away}"
        date_formatted = format_match_datetime(row.get('date'))
        
        # Check all bet types
        home_prob = pd.to_numeric(row.get('1x2_h', 0), errors='coerce') or 0
        away_prob = pd.to_numeric(row.get('1x2_a', 0), errors='coerce') or 0
        over_25 = pd.to_numeric(row.get('o_2.5', 0), errors='coerce') or 0
        under_25 = pd.to_numeric(row.get('u_2.5', 0), errors='coerce') or 0
        
        if home_prob >= min_prob:
            additional_bets.append({'match': match, 'bet': 'Home Win', 'prob': home_prob, 'date': date_formatted, 'is_user_pick': False})
        if away_prob >= min_prob:
            additional_bets.append({'match': match, 'bet': 'Away Win', 'prob': away_prob, 'date': date_formatted, 'is_user_pick': False})
        if over_25 >= min_prob:
            additional_bets.append({'match': match, 'bet': 'Over 2.5', 'prob': over_25, 'date': date_formatted, 'is_user_pick': False})
        if under_25 >= min_prob:
            additional_bets.append({'match': match, 'bet': 'Under 2.5', 'prob': under_25, 'date': date_formatted, 'is_user_pick': False})
    
    # Sort by probability
    additional_bets = sorted(additional_bets, key=lambda x: x['prob'], reverse=True)
    
    # Build accumulator with user's pick + best additional legs
    if len(additional_bets) >= num_additional_legs:
        legs = [first_leg] + additional_bets[:num_additional_legs]
        combined_prob = np.prod([leg['prob'] for leg in legs])
        
        accumulators.append({
            'type': f'CUSTOM ACCUMULATOR (Your Pick + {num_additional_legs} Best Matches)',
            'legs': legs,
            'combined_probability': combined_prob,
            'user_pick': first_leg
        })
    
    return accumulators

def simulate_match_outcome(home_prob, draw_prob, away_prob, over_25_prob, injuries_home=0, injuries_away=0, h2h_boost=0):
    """Simulate match outcome with custom adjustments"""
    # Apply injury adjustments
    if injuries_home >= 4:
        home_prob *= 0.80
        over_25_prob *= 0.90
    elif injuries_home >= 2:
        home_prob *= 0.90
        over_25_prob *= 0.95
    elif injuries_home >= 1:
        home_prob *= 0.95
    
    if injuries_away >= 4:
        away_prob *= 0.80
        over_25_prob *= 0.90
    elif injuries_away >= 2:
        away_prob *= 0.90
        over_25_prob *= 0.95
    elif injuries_away >= 1:
        away_prob *= 0.95
    
    # Apply H2H boost
    home_prob *= (1 + h2h_boost / 100)
    
    # Normalize probabilities
    total = home_prob + draw_prob + away_prob
    if total > 0:
        home_prob /= total
        draw_prob /= total
        away_prob /= total
    
    return {
        'home_prob': round(home_prob, 4),
        'draw_prob': round(draw_prob, 4),
        'away_prob': round(away_prob, 4),
        'over_25_prob': round(min(over_25_prob, 0.99), 4),
        'under_25_prob': round(max(1 - over_25_prob, 0.01), 4)
    }

def get_bets_by_risk(df, risk_level='high'):
    """Get bets filtered by risk level"""
    bets = []
    
    if risk_level == 'low':  # Safest bets (75%+)
        min_prob, max_prob = 0.75, 1.0
    elif risk_level == 'medium':  # Medium risk (60-75%)
        min_prob, max_prob = 0.60, 0.75
    elif risk_level == 'high':  # High risk (45-60%)
        min_prob, max_prob = 0.45, 0.60
    else:  # Very high risk (<45%)
        min_prob, max_prob = 0.30, 0.45
    
    for _, row in df.iterrows():
        date_formatted = format_match_datetime(row.get('date'))
        match_info = {
            'match': f"{row.get('home', 'Unknown')} vs {row.get('away', 'Unknown')}",
            'league': row.get('league', 'Unknown'),
            'date': date_formatted,
        }
        
        # Check all markets
        markets = [
            ('1x2_h', 'Home Win'),
            ('1x2_a', 'Away Win'),
            ('1x2_d', 'Draw'),
            ('o_2.5', 'Over 2.5'),
            ('u_2.5', 'Under 2.5'),
            ('o_1.5', 'Over 1.5'),
            ('u_3.5', 'Under 3.5'),
        ]
        
        for col, bet_type in markets:
            prob = pd.to_numeric(row.get(col, 0), errors='coerce') or 0
            if min_prob <= prob < max_prob:
                bets.append({**match_info, 'bet_type': bet_type, 'probability': prob})
    
    return pd.DataFrame(bets).sort_values('probability', ascending=False) if bets else pd.DataFrame()

def generate_html_report(best_bets_df, accumulators, league_stats, risk_bets=None):
    """Generate a beautiful HTML report for export with odds"""
    from datetime import datetime
    
    html = f'''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>🎯 Betting Analysis Report - {datetime.now().strftime("%Y-%m-%d %H:%M")}</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
            color: #fff;
            min-height: 100vh;
            padding: 20px;
        }}
        .container {{ max-width: 1200px; margin: 0 auto; }}
        .header {{
            text-align: center;
            padding: 40px 20px;
            background: rgba(255,255,255,0.05);
            border-radius: 20px;
            margin-bottom: 30px;
            backdrop-filter: blur(10px);
        }}
        .header h1 {{ font-size: 2.5em; margin-bottom: 10px; }}
        .header .subtitle {{ color: #94a3b8; font-size: 1.1em; }}
        .section {{
            background: rgba(255,255,255,0.08);
            border-radius: 15px;
            padding: 25px;
            margin-bottom: 25px;
            backdrop-filter: blur(10px);
            border: 1px solid rgba(255,255,255,0.1);
        }}
        .section h2 {{
            color: #60a5fa;
            margin-bottom: 20px;
            padding-bottom: 10px;
            border-bottom: 2px solid rgba(96,165,250,0.3);
            display: flex;
            align-items: center;
            gap: 10px;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 15px;
        }}
        th, td {{
            padding: 12px 15px;
            text-align: left;
            border-bottom: 1px solid rgba(255,255,255,0.1);
        }}
        th {{
            background: rgba(96,165,250,0.2);
            color: #60a5fa;
            font-weight: 600;
            text-transform: uppercase;
            font-size: 0.85em;
            letter-spacing: 0.5px;
        }}
        tr:hover {{ background: rgba(255,255,255,0.05); }}
        .confidence-high {{ color: #4ade80; font-weight: bold; }}
        .confidence-medium {{ color: #fbbf24; font-weight: bold; }}
        .confidence-low {{ color: #f87171; font-weight: bold; }}
        .probability {{
            background: linear-gradient(90deg, #3b82f6, #8b5cf6);
            padding: 5px 12px;
            border-radius: 20px;
            font-weight: bold;
            display: inline-block;
        }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-top: 20px;
        }}
        .stat-card {{
            background: rgba(59,130,246,0.1);
            padding: 20px;
            border-radius: 12px;
            text-align: center;
            border: 1px solid rgba(59,130,246,0.3);
        }}
        .stat-card .value {{ font-size: 2em; font-weight: bold; color: #60a5fa; }}
        .stat-card .label {{ color: #94a3b8; margin-top: 5px; }}
        .accumulator {{
            background: linear-gradient(135deg, rgba(139,92,246,0.2), rgba(59,130,246,0.2));
            border: 1px solid rgba(139,92,246,0.4);
            border-radius: 12px;
            padding: 20px;
            margin-top: 15px;
        }}
        .accumulator h3 {{ color: #a78bfa; margin-bottom: 15px; }}
        .accumulator .combined {{ 
            font-size: 1.3em; 
            color: #4ade80; 
            margin-top: 15px;
            padding: 10px;
            background: rgba(74,222,128,0.1);
            border-radius: 8px;
            text-align: center;
        }}
        .footer {{
            text-align: center;
            padding: 30px;
            color: #64748b;
            font-size: 0.9em;
        }}
        .risk-badge {{
            padding: 4px 10px;
            border-radius: 12px;
            font-size: 0.8em;
            font-weight: bold;
        }}
        .risk-low {{ background: rgba(74,222,128,0.2); color: #4ade80; }}
        .risk-medium {{ background: rgba(251,191,36,0.2); color: #fbbf24; }}
        .risk-high {{ background: rgba(248,113,113,0.2); color: #f87171; }}
        @media print {{
            body {{ background: #fff; color: #000; }}
            .section {{ border: 1px solid #ddd; }}
            th {{ background: #f3f4f6; color: #374151; }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🎯 AI Betting Analysis Report</h1>
            <p class="subtitle">Generated on {datetime.now().strftime("%B %d, %Y at %H:%M")}</p>
        </div>
'''
    
    # Best Bets Section
    if best_bets_df is not None and not best_bets_df.empty:
        html += '''
        <div class="section">
            <h2>🏆 Top Value Bets</h2>
            <table>
                <thead>
                    <tr>
                        <th>Date/Time</th>
                        <th>Match</th>
                        <th>League</th>
                        <th>Bet Type</th>
                        <th>Odds</th>
                        <th>Probability</th>
                        <th>Confidence</th>
                    </tr>
                </thead>
                <tbody>
'''
        for _, row in best_bets_df.iterrows():
            prob = row.get('probability', 0)
            if isinstance(prob, str):
                prob_val = float(prob.replace('%', '')) / 100
            else:
                prob_val = prob
            
            match_name = str(row.get('match', 'Unknown'))
            bet_type = str(row.get('bet_type', 'N/A'))
            odds = row.get('odds', '-')
            if odds and odds != '-':
                try:
                    odds_val = float(odds)
                    odds_display = f"{odds_val:.2f}"
                except:
                    odds_display = str(odds)
            else:
                odds_display = '-'
            
            if prob_val >= 0.70:
                conf_class = 'confidence-high'
                conf_text = '🟢 HIGH'
            elif prob_val >= 0.60:
                conf_class = 'confidence-medium'
                conf_text = '🟡 MEDIUM'
            else:
                conf_class = 'confidence-low'
                conf_text = '🔴 LOW'
            
            prob_display = f"{prob_val*100:.1f}%" if isinstance(prob_val, float) else prob
            
            html += f'''
                    <tr>
                        <td>📅 {row.get('date', 'TBD')}</td>
                        <td><strong>{match_name}</strong></td>
                        <td>{row.get('league', 'N/A')}</td>
                        <td>{bet_type}</td>
                        <td style="color: #fbbf24; font-weight: bold;">{odds_display}</td>
                        <td><span class="probability">{prob_display}</span></td>
                        <td class="{conf_class}">{conf_text}</td>
                    </tr>
'''
        html += '''
                </tbody>
            </table>
        </div>
'''
    
    # Accumulators Section
    if accumulators:
        html += '''
        <div class="section">
            <h2>🎰 Accumulator Suggestions</h2>
'''
        for acc in accumulators:
            html += f'''
            <div class="accumulator">
                <h3>📋 {acc['type']}</h3>
                <table>
                    <thead>
                        <tr>
                            <th>#</th>
                            <th>Date</th>
                            <th>Match</th>
                            <th>Pick</th>
                            <th>Probability</th>
                        </tr>
                    </thead>
                    <tbody>
'''
            for i, leg in enumerate(acc['legs'], 1):
                leg_bet = leg.get('bet', leg.get('bet_type', leg.get('pick', 'N/A')))
                leg_prob = leg.get('prob', leg.get('probability', 0))
                if isinstance(leg_prob, str):
                    leg_prob = float(leg_prob.replace('%', '')) / 100
                html += f'''
                        <tr>
                            <td>{i}</td>
                            <td>📅 {leg.get('date', 'TBD')}</td>
                            <td>{leg.get('match', 'N/A')}</td>
                            <td><strong>{leg_bet}</strong></td>
                            <td><span class="probability">{leg_prob*100:.1f}%</span></td>
                        </tr>
'''
            html += f'''
                    </tbody>
                </table>
                <div class="combined">💰 Combined Probability: <strong>{acc['combined_probability']*100:.1f}%</strong></div>
            </div>
'''
        html += '</div>'
    
    # League Stats Section
    if league_stats is not None and not league_stats.empty:
        html += '''
        <div class="section">
            <h2>📊 League Statistics</h2>
            <div class="stats-grid">
'''
        html += f'''
                <div class="stat-card">
                    <div class="value">{len(league_stats)}</div>
                    <div class="label">Total Leagues</div>
                </div>
                <div class="stat-card">
                    <div class="value">{league_stats['matches'].sum()}</div>
                    <div class="label">Total Matches</div>
                </div>
'''
        html += '''
            </div>
            <table style="margin-top: 25px;">
                <thead>
                    <tr>
                        <th>League</th>
                        <th>Matches</th>
                        <th>Avg Home Win</th>
                        <th>Avg Away Win</th>
                        <th>Avg Over 2.5</th>
                    </tr>
                </thead>
                <tbody>
'''
        for _, row in league_stats.head(15).iterrows():
            html += f'''
                    <tr>
                        <td><strong>{row['league']}</strong></td>
                        <td>{row['matches']}</td>
                        <td>{row['avg_home_prob']*100:.1f}%</td>
                        <td>{row['avg_away_prob']*100:.1f}%</td>
                        <td>{row['avg_over25']*100:.1f}%</td>
                    </tr>
'''
        html += '''
                </tbody>
            </table>
        </div>
'''
    
    # Footer
    html += f'''
        <div class="footer">
            <p>⚠️ Disclaimer: This report is for informational purposes only. Bet responsibly.</p>
            <p>Generated by AI Sports Betting Analyst</p>
        </div>
    </div>
</body>
</html>
'''
    
    return html

def generate_hebrew_html_report(best_bets_df, accumulators, league_stats, risk_bets=None):
    """Generate a Hebrew RTL HTML report for export with odds"""
    from datetime import datetime
    
    html = f'''
<!DOCTYPE html>
<html lang="he" dir="rtl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>🎯 דוח ניתוח הימורים - {datetime.now().strftime("%Y-%m-%d %H:%M")}</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
            color: #fff;
            min-height: 100vh;
            padding: 20px;
            direction: rtl;
        }}
        .container {{ max-width: 1200px; margin: 0 auto; }}
        .header {{
            text-align: center;
            padding: 40px 20px;
            background: rgba(255,255,255,0.05);
            border-radius: 20px;
            margin-bottom: 30px;
            backdrop-filter: blur(10px);
        }}
        .header h1 {{ font-size: 2.5em; margin-bottom: 10px; }}
        .header .subtitle {{ color: #94a3b8; font-size: 1.1em; }}
        .section {{
            background: rgba(255,255,255,0.08);
            border-radius: 15px;
            padding: 25px;
            margin-bottom: 25px;
            backdrop-filter: blur(10px);
            border: 1px solid rgba(255,255,255,0.1);
        }}
        .section h2 {{
            color: #60a5fa;
            margin-bottom: 20px;
            padding-bottom: 10px;
            border-bottom: 2px solid rgba(96,165,250,0.3);
            display: flex;
            align-items: center;
            gap: 10px;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 15px;
        }}
        th, td {{
            padding: 12px 15px;
            text-align: right;
            border-bottom: 1px solid rgba(255,255,255,0.1);
        }}
        th {{
            background: rgba(96,165,250,0.2);
            color: #60a5fa;
            font-weight: 600;
            text-transform: uppercase;
            font-size: 0.85em;
            letter-spacing: 0.5px;
        }}
        tr:hover {{ background: rgba(255,255,255,0.05); }}
        .confidence-high {{ color: #4ade80; font-weight: bold; }}
        .confidence-medium {{ color: #fbbf24; font-weight: bold; }}
        .confidence-low {{ color: #f87171; font-weight: bold; }}
        .probability {{
            background: linear-gradient(90deg, #3b82f6, #8b5cf6);
            padding: 5px 12px;
            border-radius: 20px;
            font-weight: bold;
            display: inline-block;
        }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-top: 20px;
        }}
        .stat-card {{
            background: rgba(59,130,246,0.1);
            padding: 20px;
            border-radius: 12px;
            text-align: center;
            border: 1px solid rgba(59,130,246,0.3);
        }}
        .stat-card .value {{ font-size: 2em; font-weight: bold; color: #60a5fa; }}
        .stat-card .label {{ color: #94a3b8; margin-top: 5px; }}
        .accumulator {{
            background: linear-gradient(135deg, rgba(139,92,246,0.2), rgba(59,130,246,0.2));
            border: 1px solid rgba(139,92,246,0.4);
            border-radius: 12px;
            padding: 20px;
            margin-top: 15px;
        }}
        .accumulator h3 {{ color: #a78bfa; margin-bottom: 15px; }}
        .accumulator .combined {{ 
            font-size: 1.3em; 
            color: #4ade80; 
            margin-top: 15px;
            padding: 10px;
            background: rgba(74,222,128,0.1);
            border-radius: 8px;
            text-align: center;
        }}
        .footer {{
            text-align: center;
            padding: 30px;
            color: #64748b;
            font-size: 0.9em;
        }}
        .risk-badge {{
            padding: 4px 10px;
            border-radius: 12px;
            font-size: 0.8em;
            font-weight: bold;
        }}
        .risk-low {{ background: rgba(74,222,128,0.2); color: #4ade80; }}
        .risk-medium {{ background: rgba(251,191,36,0.2); color: #fbbf24; }}
        .risk-high {{ background: rgba(248,113,113,0.2); color: #f87171; }}
        @media print {{
            body {{ background: #fff; color: #000; }}
            .section {{ border: 1px solid #ddd; }}
            th {{ background: #f3f4f6; color: #374151; }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🎯 דוח ניתוח הימורים AI</h1>
            <p class="subtitle">נוצר בתאריך {datetime.now().strftime("%d/%m/%Y בשעה %H:%M")}</p>
        </div>
'''
    
    # Best Bets Section
    if best_bets_df is not None and not best_bets_df.empty:
        html += '''
        <div class="section">
            <h2>🏆 הימורי ערך מובילים</h2>
            <table>
                <thead>
                    <tr>
                        <th>תאריך/שעה</th>
                        <th>משחק</th>
                        <th>ליגה</th>
                        <th>סוג הימור</th>
                        <th>מכפיל</th>
                        <th>הסתברות</th>
                        <th>ביטחון</th>
                    </tr>
                </thead>
                <tbody>
'''
        for _, row in best_bets_df.iterrows():
            prob = row.get('probability', 0)
            if isinstance(prob, str):
                prob_val = float(prob.replace('%', '')) / 100
            else:
                prob_val = prob
            
            match_name = str(row.get('match', 'Unknown'))
            bet_type = str(row.get('bet_type', 'N/A'))
            odds = row.get('odds', '-')
            if odds and odds != '-':
                try:
                    odds_val = float(odds)
                    odds_display = f"{odds_val:.2f}"
                except:
                    odds_display = str(odds)
            else:
                odds_display = '-'
            
            if prob_val >= 0.70:
                conf_class = 'confidence-high'
                conf_text = '🟢 גבוה'
            elif prob_val >= 0.60:
                conf_class = 'confidence-medium'
                conf_text = '🟡 בינוני'
            else:
                conf_class = 'confidence-low'
                conf_text = '🔴 נמוך'
            
            prob_display = f"{prob_val*100:.1f}%" if isinstance(prob_val, float) else prob
            
            # Translate bet types
            bet_type_heb = bet_type.replace('Home Win', 'ניצחון בית').replace('Away Win', 'ניצחון חוץ').replace('Draw', 'תיקו').replace('Over', 'מעל').replace('Under', 'מתחת')
            
            html += f'''
                    <tr>
                        <td>📅 {row.get('date', 'TBD')}</td>
                        <td><strong>{match_name}</strong></td>
                        <td>{row.get('league', 'N/A')}</td>
                        <td>{bet_type_heb}</td>
                        <td style="color: #fbbf24; font-weight: bold;">{odds_display}</td>
                        <td><span class="probability">{prob_display}</span></td>
                        <td class="{conf_class}">{conf_text}</td>
                    </tr>
'''
        html += '''
                </tbody>
            </table>
        </div>
'''
    
    # Accumulators Section
    if accumulators:
        html += '''
        <div class="section">
            <h2>🎰 הצעות מצברים</h2>
'''
        for acc in accumulators:
            acc_type_heb = acc['type'].replace('SAFE ACCUMULATOR', 'מצבר בטוח')
            html += f'''
            <div class="accumulator">
                <h3>📋 {acc_type_heb}</h3>
                <table>
                    <thead>
                        <tr>
                            <th>#</th>
                            <th>תאריך</th>
                            <th>משחק</th>
                            <th>בחירה</th>
                            <th>הסתברות</th>
                        </tr>
                    </thead>
                    <tbody>
'''
            for i, leg in enumerate(acc['legs'], 1):
                leg_bet = leg.get('bet', leg.get('bet_type', leg.get('pick', 'N/A')))
                leg_prob = leg.get('prob', leg.get('probability', 0))
                if isinstance(leg_prob, str):
                    leg_prob = float(leg_prob.replace('%', '')) / 100
                bet_heb = leg_bet.replace('Home Win', 'ניצחון בית').replace('Away Win', 'ניצחון חוץ').replace('Draw', 'תיקו').replace('Over', 'מעל').replace('Under', 'מתחת')
                html += f'''
                        <tr>
                            <td>{i}</td>
                            <td>📅 {leg.get('date', 'TBD')}</td>
                            <td>{leg.get('match', 'N/A')}</td>
                            <td><strong>{bet_heb}</strong></td>
                            <td><span class="probability">{leg_prob*100:.1f}%</span></td>
                        </tr>
'''
            html += f'''
                    </tbody>
                </table>
                <div class="combined">💰 הסתברות משולבת: <strong>{acc['combined_probability']*100:.1f}%</strong></div>
            </div>
'''
        html += '</div>'
    
    # League Stats Section
    if league_stats is not None and not league_stats.empty:
        html += '''
        <div class="section">
            <h2>📊 סטטיסטיקות ליגות</h2>
            <div class="stats-grid">
'''
        html += f'''
                <div class="stat-card">
                    <div class="value">{len(league_stats)}</div>
                    <div class="label">סה"כ ליגות</div>
                </div>
                <div class="stat-card">
                    <div class="value">{league_stats['matches'].sum()}</div>
                    <div class="label">סה"כ משחקים</div>
                </div>
'''
        html += '''
            </div>
            <table style="margin-top: 25px;">
                <thead>
                    <tr>
                        <th>ליגה</th>
                        <th>משחקים</th>
                        <th>ממוצע ניצחון בית</th>
                        <th>ממוצע ניצחון חוץ</th>
                        <th>ממוצע מעל 2.5</th>
                    </tr>
                </thead>
                <tbody>
'''
        for _, row in league_stats.head(15).iterrows():
            html += f'''
                    <tr>
                        <td><strong>{row['league']}</strong></td>
                        <td>{row['matches']}</td>
                        <td>{row['avg_home_prob']*100:.1f}%</td>
                        <td>{row['avg_away_prob']*100:.1f}%</td>
                        <td>{row['avg_over25']*100:.1f}%</td>
                    </tr>
'''
        html += '''
                </tbody>
            </table>
        </div>
'''
    
    # Footer
    html += f'''
        <div class="footer">
            <p>⚠️ הערה: דוח זה מיועד למידע בלבד. הימרו באחריות.</p>
            <p>נוצר על ידי AI Sports Betting Analyst</p>
        </div>
    </div>
</body>
</html>
'''
    
    return html

def generate_accumulator_html(accumulators, lang='en'):
    """Generate HTML report for accumulators"""
    is_hebrew = lang == 'he'
    
    # Common styles
    styles = '''
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
            color: #fff;
            min-height: 100vh;
            padding: 20px;
        }
        .container { max-width: 1200px; margin: 0 auto; }
        .header {
            text-align: center;
            padding: 40px 20px;
            background: rgba(255,255,255,0.05);
            border-radius: 20px;
            margin-bottom: 30px;
        }
        .header h1 { font-size: 2.5em; margin-bottom: 10px; }
        .accumulator {
            background: linear-gradient(135deg, rgba(139,92,246,0.2), rgba(59,130,246,0.2));
            border: 1px solid rgba(139,92,246,0.4);
            border-radius: 15px;
            padding: 25px;
            margin-bottom: 25px;
        }
        .accumulator h2 { color: #a78bfa; margin-bottom: 20px; }
        table { width: 100%; border-collapse: collapse; margin: 15px 0; }
        th, td { padding: 12px 15px; border-bottom: 1px solid rgba(255,255,255,0.1); }
        th { background: rgba(96,165,250,0.2); color: #60a5fa; font-weight: 600; }
        .probability {
            background: linear-gradient(90deg, #3b82f6, #8b5cf6);
            padding: 5px 12px;
            border-radius: 20px;
            font-weight: bold;
        }
        .combined {
            font-size: 1.3em;
            color: #4ade80;
            margin-top: 15px;
            padding: 15px;
            background: rgba(74,222,128,0.1);
            border-radius: 8px;
            text-align: center;
        }
        .footer { text-align: center; padding: 30px; color: #64748b; }
    '''
    
    if is_hebrew:
        html = f'''<!DOCTYPE html>
<html lang="he" dir="rtl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>🎰 מצברים - {datetime.now().strftime("%Y-%m-%d %H:%M")}</title>
    <style>{styles} body {{ direction: rtl; }} th, td {{ text-align: right; }}</style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🎰 הצעות מצברים</h1>
            <p>נוצר בתאריך {datetime.now().strftime("%d/%m/%Y בשעה %H:%M")}</p>
        </div>
'''
    else:
        html = f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>🎰 Accumulators - {datetime.now().strftime("%Y-%m-%d %H:%M")}</title>
    <style>{styles}</style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🎰 Accumulator Suggestions</h1>
            <p>Generated on {datetime.now().strftime("%B %d, %Y at %H:%M")}</p>
        </div>
'''
    
    if accumulators:
        for acc in accumulators:
            acc_type = acc.get('type', 'Accumulator')
            if is_hebrew:
                acc_type = acc_type.replace('SAFE ACCUMULATOR', 'מצבר בטוח').replace('CUSTOM ACCUMULATOR', 'מצבר מותאם אישית').replace('Custom', 'מותאם אישית').replace('Fold', 'רגליים')
            
            html += f'''
        <div class="accumulator">
            <h2>📋 {acc_type}</h2>
            <table>
                <thead>
                    <tr>
                        <th>#</th>
                        <th>{"תאריך" if is_hebrew else "Date"}</th>
                        <th>{"משחק" if is_hebrew else "Match"}</th>
                        <th>{"בחירה" if is_hebrew else "Pick"}</th>
                        <th>{"הסתברות" if is_hebrew else "Probability"}</th>
                        <th>{"מכפיל" if is_hebrew else "Odds"}</th>
                    </tr>
                </thead>
                <tbody>
'''
            for i, leg in enumerate(acc.get('legs', []), 1):
                bet = leg.get('bet', 'N/A')
                if is_hebrew:
                    bet = bet.replace('Home Win', 'ניצחון בית').replace('Away Win', 'ניצחון חוץ').replace('Draw', 'תיקו').replace('Over', 'מעל').replace('Under', 'מתחת')
                
                is_user = "⭐" if leg.get('is_user_pick') else ""
                odds = leg.get('odds')
                odds_display = f"{odds:.2f}" if odds else "-"
                html += f'''
                    <tr>
                        <td>{i} {is_user}</td>
                        <td>📅 {leg.get('date', 'TBD')}</td>
                        <td><strong>{leg.get('match', 'N/A')}</strong></td>
                        <td>{bet}</td>
                        <td><span class="probability">{leg.get('prob', 0)*100:.1f}%</span></td>
                        <td style="color: #fbbf24; font-weight: bold;">{odds_display}</td>
                    </tr>
'''
            combined = acc.get('combined_probability', 0)
            combined_odds = acc.get('combined_odds')
            odds_text = f" | {'מכפיל משולב' if is_hebrew else 'Combined Odds'}: <strong>{combined_odds:.2f}</strong>" if combined_odds else ""
            html += f'''
                </tbody>
            </table>
            <div class="combined">💰 {"הסתברות משולבת" if is_hebrew else "Combined Probability"}: <strong>{combined*100:.1f}%</strong>{odds_text}</div>
        </div>
'''
    else:
        html += f'<p style="text-align:center;">{"אין מצברים זמינים" if is_hebrew else "No accumulators available"}</p>'
    
    html += f'''
        <div class="footer">
            <p>⚠️ {"הערה: דוח זה מיועד למידע בלבד. הימרו באחריות." if is_hebrew else "Disclaimer: This report is for informational purposes only. Bet responsibly."}</p>
        </div>
    </div>
</body>
</html>'''
    
    return html

def generate_game_lab_html(match_info, adjusted_probs, recommendations, lang='en'):
    """Generate HTML report for Game Lab analysis"""
    is_hebrew = lang == 'he'
    
    styles = '''
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
            color: #fff;
            min-height: 100vh;
            padding: 20px;
        }
        .container { max-width: 800px; margin: 0 auto; }
        .header { text-align: center; padding: 30px; background: rgba(255,255,255,0.05); border-radius: 20px; margin-bottom: 25px; }
        .header h1 { font-size: 2em; margin-bottom: 10px; }
        .section { background: rgba(255,255,255,0.08); border-radius: 15px; padding: 25px; margin-bottom: 20px; }
        .section h2 { color: #60a5fa; margin-bottom: 15px; border-bottom: 2px solid rgba(96,165,250,0.3); padding-bottom: 10px; }
        .prob-grid { display: grid; grid-template-columns: repeat(2, 1fr); gap: 15px; }
        .prob-item { background: rgba(59,130,246,0.1); padding: 15px; border-radius: 10px; text-align: center; }
        .prob-item .label { color: #94a3b8; font-size: 0.9em; }
        .prob-item .value { font-size: 1.5em; font-weight: bold; color: #60a5fa; }
        .recommendation { background: linear-gradient(135deg, rgba(74,222,128,0.2), rgba(59,130,246,0.2)); padding: 20px; border-radius: 12px; text-align: center; margin-top: 20px; }
        .recommendation h3 { color: #4ade80; font-size: 1.5em; }
        .footer { text-align: center; padding: 20px; color: #64748b; }
    '''
    
    if is_hebrew:
        html = f'''<!DOCTYPE html>
<html lang="he" dir="rtl">
<head>
    <meta charset="UTF-8">
    <title>🧪 ניתוח משחק - {match_info.get('match', '')}</title>
    <style>{styles} body {{ direction: rtl; }}</style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🧪 ניתוח מעבדת משחק</h1>
            <p>⚽ {match_info.get('match', 'N/A')}</p>
            <p>🏆 {match_info.get('league', 'N/A')} | 📅 {match_info.get('date', 'TBD')}</p>
        </div>
        <div class="section">
            <h2>📊 הסתברויות מותאמות</h2>
            <div class="prob-grid">
                <div class="prob-item"><div class="label">ניצחון בית</div><div class="value">{adjusted_probs.get('home_prob', 0)*100:.1f}%</div></div>
                <div class="prob-item"><div class="label">תיקו</div><div class="value">{adjusted_probs.get('draw_prob', 0)*100:.1f}%</div></div>
                <div class="prob-item"><div class="label">ניצחון חוץ</div><div class="value">{adjusted_probs.get('away_prob', 0)*100:.1f}%</div></div>
                <div class="prob-item"><div class="label">מעל 2.5</div><div class="value">{adjusted_probs.get('over_25_prob', 0)*100:.1f}%</div></div>
            </div>
        </div>
        <div class="recommendation">
            <h3>🎯 המלצה: {recommendations.get('pick', 'N/A').replace('Home Win', 'ניצחון בית').replace('Away Win', 'ניצחון חוץ').replace('Draw', 'תיקו').replace('Over', 'מעל').replace('Under', 'מתחת')}</h3>
            <p>הסתברות: {recommendations.get('probability', 0)*100:.1f}% | הימור מומלץ: {recommendations.get('stake', 0)*100:.1f}%</p>
        </div>
        <div class="footer"><p>נוצר על ידי AI Sports Betting Analyst</p></div>
    </div>
</body>
</html>'''
    else:
        html = f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>🧪 Game Lab - {match_info.get('match', '')}</title>
    <style>{styles}</style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🧪 Game Lab Analysis</h1>
            <p>⚽ {match_info.get('match', 'N/A')}</p>
            <p>🏆 {match_info.get('league', 'N/A')} | 📅 {match_info.get('date', 'TBD')}</p>
        </div>
        <div class="section">
            <h2>📊 Adjusted Probabilities</h2>
            <div class="prob-grid">
                <div class="prob-item"><div class="label">Home Win</div><div class="value">{adjusted_probs.get('home_prob', 0)*100:.1f}%</div></div>
                <div class="prob-item"><div class="label">Draw</div><div class="value">{adjusted_probs.get('draw_prob', 0)*100:.1f}%</div></div>
                <div class="prob-item"><div class="label">Away Win</div><div class="value">{adjusted_probs.get('away_prob', 0)*100:.1f}%</div></div>
                <div class="prob-item"><div class="label">Over 2.5</div><div class="value">{adjusted_probs.get('over_25_prob', 0)*100:.1f}%</div></div>
            </div>
        </div>
        <div class="recommendation">
            <h3>🎯 Recommended: {recommendations.get('pick', 'N/A')}</h3>
            <p>Probability: {recommendations.get('probability', 0)*100:.1f}% | Suggested Stake: {recommendations.get('stake', 0)*100:.1f}%</p>
        </div>
        <div class="footer"><p>Generated by AI Sports Betting Analyst</p></div>
    </div>
</body>
</html>'''
    
    return html

def generate_team_stats_html(team_stats_df, lang='en'):
    """Generate HTML report for team statistics"""
    is_hebrew = lang == 'he'
    
    styles = '''
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
            color: #fff;
            min-height: 100vh;
            padding: 20px;
        }
        .container { max-width: 1200px; margin: 0 auto; }
        .header { text-align: center; padding: 30px; background: rgba(255,255,255,0.05); border-radius: 20px; margin-bottom: 25px; }
        .header h1 { font-size: 2.2em; }
        table { width: 100%; border-collapse: collapse; background: rgba(255,255,255,0.05); border-radius: 10px; overflow: hidden; }
        th, td { padding: 12px 15px; border-bottom: 1px solid rgba(255,255,255,0.1); }
        th { background: rgba(96,165,250,0.2); color: #60a5fa; font-weight: 600; }
        tr:hover { background: rgba(255,255,255,0.05); }
        .high { color: #4ade80; }
        .medium { color: #fbbf24; }
        .low { color: #f87171; }
        .footer { text-align: center; padding: 20px; color: #64748b; }
    '''
    
    if is_hebrew:
        html = f'''<!DOCTYPE html>
<html lang="he" dir="rtl">
<head>
    <meta charset="UTF-8">
    <title>📊 סטטיסטיקות קבוצות</title>
    <style>{styles} body {{ direction: rtl; }} th, td {{ text-align: right; }}</style>
</head>
<body>
    <div class="container">
        <div class="header"><h1>📊 סטטיסטיקות קבוצות - התקפה והגנה</h1></div>
        <table>
            <thead>
                <tr><th>קבוצה</th><th>ליגה</th><th>משחקים</th><th>התקפה</th><th>הגנה</th><th>כוח כללי</th></tr>
            </thead>
            <tbody>
'''
    else:
        html = f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>📊 Team Statistics</title>
    <style>{styles}</style>
</head>
<body>
    <div class="container">
        <div class="header"><h1>📊 Team Statistics - Attacking & Defense</h1></div>
        <table>
            <thead>
                <tr><th>Team</th><th>League</th><th>Matches</th><th>Attacking</th><th>Defensive</th><th>Overall</th></tr>
            </thead>
            <tbody>
'''
    
    for _, row in team_stats_df.head(30).iterrows():
        att_class = 'high' if row['attacking_rating'] >= 60 else 'medium' if row['attacking_rating'] >= 45 else 'low'
        def_class = 'high' if row['defensive_rating'] >= 60 else 'medium' if row['defensive_rating'] >= 45 else 'low'
        ovr_class = 'high' if row['overall_strength'] >= 60 else 'medium' if row['overall_strength'] >= 40 else 'low'
        
        html += f'''
                <tr>
                    <td><strong>{row['team']}</strong></td>
                    <td>{row['league']}</td>
                    <td>{row['matches']}</td>
                    <td class="{att_class}">{row['attacking_rating']:.1f}%</td>
                    <td class="{def_class}">{row['defensive_rating']:.1f}%</td>
                    <td class="{ovr_class}">{row['overall_strength']:.1f}%</td>
                </tr>
'''
    
    html += f'''
            </tbody>
        </table>
        <div class="footer"><p>{"נוצר על ידי AI Sports Betting Analyst" if is_hebrew else "Generated by AI Sports Betting Analyst"}</p></div>
    </div>
</body>
</html>'''
    
    return html

# Function to preprocess and save the uploaded file
def preprocess_and_save(file):
    try:
        # Read the uploaded file into a DataFrame
        if file.name.endswith('.csv'):
            df = pd.read_csv(file, encoding='utf-8', na_values=['NA', 'N/A', 'missing', ''])
        elif file.name.endswith('.xlsx'):
            df = pd.read_excel(file, na_values=['NA', 'N/A', 'missing', ''])
        else:
            st.error("Unsupported file format. Please upload a CSV or Excel file.")
            return None, None, None
        
        # Clean column names (strip whitespace)
        df.columns = df.columns.str.strip()
        
        # Ensure string columns are properly quoted
        for col in df.select_dtypes(include=['object']):
            df[col] = df[col].astype(str).replace({r'"': '""'}, regex=True)
        
        # Parse dates and numeric columns
        for col in df.columns:
            if 'date' in col.lower():
                df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
            elif df[col].dtype == 'object' and col not in ['home', 'away', 'league', 'id']:
                try:
                    df[col] = pd.to_numeric(df[col])
                except (ValueError, TypeError):
                    pass
        
        # Create a temporary file to save the preprocessed data
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as temp_file:
            temp_path = temp_file.name
            df.to_csv(temp_path, index=False, quoting=csv.QUOTE_ALL)
        
        return temp_path, df.columns.tolist(), df
    except Exception as e:
        st.error(f"Error processing file: {e}")
        return None, None, None

# Streamlit app
st.set_page_config(page_title="⚽ AI Betting Analyst", page_icon="🎯", layout="wide")

# Inject Modern CSS
st.markdown(MODERN_CSS, unsafe_allow_html=True)

st.title("🎯 AI Sports Betting Analyst")
st.markdown("*Powered by AI - Merge predictions with live data for the best betting insights*")

# Initialize session state
if 'api_fixtures' not in st.session_state:
    st.session_state.api_fixtures = None
if 'merged_data' not in st.session_state:
    st.session_state.merged_data = None
if 'merged_analyses' not in st.session_state:
    st.session_state.merged_analyses = []
if 'merge_cache_key' not in st.session_state:
    st.session_state.merge_cache_key = None
if 'selected_matches' not in st.session_state:
    st.session_state.selected_matches = []
if 'analysis_results' not in st.session_state:
    st.session_state.analysis_results = {}
if 'custom_acc_match' not in st.session_state:
    st.session_state.custom_acc_match = None
if 'custom_acc_bet' not in st.session_state:
    st.session_state.custom_acc_bet = None
if 'game_lab_match' not in st.session_state:
    st.session_state.game_lab_match = None
if 'fixture_odds_cache' not in st.session_state:
    st.session_state.fixture_odds_cache = {}  # {fixture_id: odds_dict}
if 'custom_acc_legs' not in st.session_state:
    st.session_state.custom_acc_legs = []  # List of custom accumulator legs

# AllSportsAPI removed - API-Football V3 only

# Initialize bet overrides session state
if 'bet_overrides' not in st.session_state:
    st.session_state.bet_overrides = {}  # {match_key: {'original_bet': str, 'new_bet': str, 'new_probability': float}}

# Sidebar for API keys and settings
with st.sidebar:
    st.header("⚙️ Settings")
    openai_key = st.text_input("Enter your OpenAI API key:", type="password")
    if openai_key:
        st.session_state.openai_key = openai_key
        st.success("✅ API key saved!")
    else:
        st.warning("⚠️ Enter OpenAI API key for AI analysis")
    
    st.markdown("---")
    
    # API-Football Integration
    st.header("🌐 Live Data (API-Football)")
    if API_MODULES_AVAILABLE:
        st.success("✅ API modules loaded")
        
        days_range = st.selectbox("Days range:", [0, 1, 2, 3, 7], index=2, 
                                  help="Fetch fixtures for today + this many days ahead")
        fetch_extra = st.checkbox("+ H2H & Predictions", value=True,
                                 help="Also fetch predictions and head-to-head data")
        
        if st.button("🔄 Fetch Live Data", use_container_width=True):
            with st.spinner("Fetching fixtures for multiple days..."):
                try:
                    from datetime import datetime, timedelta
                    api = APIFootball("8333df5e3877e41485704e1c3ad026e6")
                    
                    all_fixtures = []
                    seen_fixture_ids = set()
                    
                    # Fetch fixtures for EACH day separately (today through today + days_range)
                    for day_offset in range(days_range + 1):  # +1 to include the end day
                        target_date = (datetime.now() + timedelta(days=day_offset)).strftime('%Y-%m-%d')
                        st.info(f"📅 Fetching fixtures for {target_date}...")
                        
                        day_fixtures = fetch_all_winner_fixtures(api, target_date)
                        
                        # Add unique fixtures only
                        for fix in day_fixtures:
                            fix_id = fix.get('fixture', {}).get('id')
                            if fix_id and fix_id not in seen_fixture_ids:
                                seen_fixture_ids.add(fix_id)
                                all_fixtures.append(fix)
                        
                        st.success(f"  ✅ {target_date}: {len(day_fixtures)} fixtures")
                    
                    # Now fetch predictions and H2H for each fixture if enabled
                    if fetch_extra and all_fixtures:
                        st.info(f"🔮 Fetching predictions & H2H for {len(all_fixtures)} fixtures...")
                        progress_bar = st.progress(0)
                        status_text = st.empty()
                        
                        predictions_count = 0
                        h2h_count = 0
                        
                        for i, fix in enumerate(all_fixtures):
                            fixture_id = fix.get('fixture', {}).get('id')
                            home_id = fix.get('teams', {}).get('home', {}).get('id')
                            away_id = fix.get('teams', {}).get('away', {}).get('id')
                            
                            try:
                                # Get predictions
                                if fixture_id:
                                    predictions = api.get_predictions(fixture_id)
                                    if predictions:
                                        fix['predictions'] = predictions
                                        predictions_count += 1
                                
                                # Get H2H
                                if home_id and away_id:
                                    h2h = api.get_h2h(home_id, away_id, last=10)
                                    if h2h:
                                        fix['h2h'] = h2h
                                        h2h_count += 1
                                
                                # Get odds
                                if fixture_id:
                                    odds_data = api.get_odds(fixture=fixture_id)
                                    odds_response = odds_data.get('response', [])
                                    if odds_response:
                                        fix['odds'] = odds_response
                                        
                            except Exception as e:
                                pass  # Skip errors for individual fixtures
                            
                            progress_bar.progress((i + 1) / len(all_fixtures))
                            status_text.text(f"Processing {i+1}/{len(all_fixtures)} - Predictions: {predictions_count}, H2H: {h2h_count}")
                            
                            # Small delay to avoid rate limiting
                            import time
                            time.sleep(0.1)
                        
                        progress_bar.empty()
                        status_text.empty()
                        st.success(f"✅ Extra data: {predictions_count} predictions, {h2h_count} H2H records")
                    
                    st.session_state.api_fixtures = all_fixtures
                    # Clear merge cache when new fixtures are fetched
                    st.session_state.merge_cache_key = None
                    st.session_state.merged_data = None
                    st.session_state.merged_analyses = []
                    st.success(f"✅ Total: {len(all_fixtures)} unique fixtures loaded for {days_range + 1} days")
                    
                    # Save fixtures to debug file (with predictions and H2H info)
                    try:
                        debug_fixtures = []
                        for fix in all_fixtures:
                            teams = fix.get('teams', {})
                            league = fix.get('league', {})
                            fixture_info = fix.get('fixture', {})
                            predictions = fix.get('predictions', {})
                            h2h = fix.get('h2h', [])
                            odds = fix.get('odds', [])
                            
                            debug_entry = {
                                'fixture_id': fixture_info.get('id'),
                                'date': fixture_info.get('date'),
                                'home_team': teams.get('home', {}).get('name', 'Unknown'),
                                'home_id': teams.get('home', {}).get('id'),
                                'away_team': teams.get('away', {}).get('name', 'Unknown'),
                                'away_id': teams.get('away', {}).get('id'),
                                'league': league.get('name', 'Unknown'),
                                'league_id': league.get('id'),
                                'country': league.get('country', 'Unknown'),
                                'has_predictions': bool(predictions),
                                'has_h2h': bool(h2h),
                                'has_odds': bool(odds),
                                'h2h_matches': len(h2h) if h2h else 0,
                            }
                            
                            # Add prediction details if available
                            if predictions:
                                pred_data = predictions.get('predictions', {})
                                debug_entry['prediction_winner'] = pred_data.get('winner', {}).get('name') if pred_data else None
                                debug_entry['prediction_advice'] = pred_data.get('advice') if pred_data else None
                            
                            debug_fixtures.append(debug_entry)
                        
                        import json
                        with open('api_fixtures_debug.json', 'w', encoding='utf-8') as f:
                            json.dump(debug_fixtures, f, indent=2, ensure_ascii=False, default=str)
                        st.info(f"📁 Debug file saved: api_fixtures_debug.json")
                    except Exception as debug_err:
                        st.warning(f"Could not save debug file: {debug_err}")
                        
                except Exception as e:
                    st.error(f"Error: {e}")
        
        if st.session_state.api_fixtures:
            st.info(f"📡 {len(st.session_state.api_fixtures)} live fixtures cached")
    else:
        st.warning("⚠️ API modules not available")
    
    st.markdown("---")
    
    # ============== SAVE/IMPORT MERGED DATA ==============
    st.header("💾 Save & Import Data")
    
    # Initialize import tracking
    if 'import_done' not in st.session_state:
        st.session_state.import_done = False
    
    # Show current data status
    merged_count = len(st.session_state.get('merged_data', []) or [])
    override_count = len(st.session_state.get('bet_overrides', {}))
    api_count = len(st.session_state.get('api_fixtures', []) or [])
    
    if merged_count > 0 or override_count > 0:
        st.success(f"📊 {merged_count} merged | 🔄 {override_count} overrides | 📡 {api_count} API")
    else:
        st.info("No data to save yet")
    
    col_save, col_clear = st.columns(2)
    with col_save:
        if merged_count > 0 or override_count > 0:
            # Generate fresh JSON data each time
            json_data = save_merged_data_to_json()
            st.download_button(
                label="💾 Save Session",
                data=json_data,
                file_name=f"betting_session_{datetime.now().strftime('%Y%m%d_%H%M')}.json",
                mime="application/json",
                use_container_width=True,
                help="Save merged data, overrides, and API fixtures",
                key=f"save_btn_{merged_count}_{override_count}"  # Dynamic key to force refresh
            )
    
    with col_clear:
        if st.button("🗑️ Clear All", use_container_width=True, help="Clear all cached data"):
            st.session_state.merged_data = None
            st.session_state.merged_analyses = []
            st.session_state.bet_overrides = {}
            st.session_state.api_fixtures = None
            st.session_state.merge_cache_key = None
            st.session_state.import_done = False
            st.success("✅ All data cleared!")
            st.rerun()
    
    # Import session - use a form to prevent repeated imports
    st.markdown("##### 📥 Import Session")
    import_file = st.file_uploader(
        "Choose JSON file",
        type=['json'],
        key='import_session',
        help="Load previously saved session data",
        label_visibility="collapsed"
    )
    
    if import_file is not None and not st.session_state.import_done:
        if st.button("📥 Load Data", use_container_width=True, type="primary"):
            try:
                json_str = import_file.read().decode('utf-8')
                success, message = load_merged_data_from_json(json_str)
                if success:
                    st.session_state.import_done = True
                    st.success(f"✅ {message}")
                    st.rerun()
                else:
                    st.error(message)
            except Exception as e:
                st.error(f"Error importing: {e}")
    elif st.session_state.import_done:
        st.info("✅ Data loaded! Clear the file to import again.")
        if st.button("🔄 Reset Import", use_container_width=True):
            st.session_state.import_done = False
            st.rerun()
    
    st.markdown("---")
    st.header("📅 Date Filters")
    date_filter = st.selectbox(
        "Show matches for:",
        ["All Dates", "Today Only", "Next 2 Days", "Next 3 Days", "Next 7 Days"],
        index=2  # Default to Next 2 Days
    )
    
    sort_option = st.selectbox(
        "Sort bets by:",
        ["Probability (Highest)", "Date (Soonest)", "Edge (Highest)"],
        index=0
    )
    
    st.markdown("---")
    st.header("🏟️ League Filter")
    filter_leagues = st.checkbox("Only Supported Leagues", value=True, 
                                  help="Filter to show only officially supported leagues")
    
    st.markdown("---")
    st.header("🎚️ Analysis Parameters")
    min_prob_threshold = st.slider("Minimum Probability %", 50, 90, 60, help="Only show bets above this probability threshold") / 100
    value_edge_threshold = st.slider("Value Edge Threshold %", 0, 15, 5, help="Minimum edge to consider a value bet") / 100
    top_bets_count = st.slider("Top Bets to Show", 5, 50, 20, help="Number of best bets to display")
    accumulator_legs = st.slider("Accumulator Legs", 2, 8, 4, help="Number of legs in suggested accumulators")
    
    st.markdown("---")
    st.markdown("""
    ### 📖 Guide
    - **🟢 HIGH**: Probability > 70%
    - **🟡 MEDIUM**: Probability 60-70%
    - **🔴 LOW**: Probability < 60%
    - **Value Bet**: Edge ≥ 5%
    """)
    
    # Show cache statistics in sidebar
    if TEAM_CACHE_AVAILABLE:
        st.markdown("---")
        st.markdown("### 💾 History Cache")
        cache = get_cache()
        if cache:
            cache_stats = cache.get_cache_stats()
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Teams", cache_stats.get('teams', 0), help="Cached team records")
                st.metric("Matches", cache_stats.get('matches', 0), help="Stored match results")
            with col2:
                st.metric("H2H", cache_stats.get('h2h_records', 0), help="Head-to-head records")
                st.metric("Form", cache_stats.get('form_records', 0), help="Team form records")

# File upload widget
uploaded_file = st.file_uploader("📤 Upload Predictions CSV/Excel", type=["csv", "xlsx"])

if uploaded_file is not None:
    # Preprocess and save the uploaded file
    temp_path, columns, raw_df = preprocess_and_save(uploaded_file)
    
    if temp_path and columns and raw_df is not None:
        # Apply supported leagues filter
        if filter_leagues:
            df = filter_supported_leagues(raw_df)
            filtered_out = len(raw_df) - len(df)
            st.info(f"📊 Loaded {len(df)} matches from supported leagues ({filtered_out} filtered out)")
        else:
            df = raw_df
            st.success(f"✅ Loaded {len(df)} matches from {uploaded_file.name}")
        
        # Apply date filter
        if date_filter != "All Dates":
            days_map = {
                "Today Only": 0,
                "Next 2 Days": 2,
                "Next 3 Days": 3,
                "Next 7 Days": 7
            }
            days_ahead = days_map.get(date_filter, 2)
            df_filtered = get_matches_by_date_range(df, days_ahead=days_ahead)
            if not df_filtered.empty:
                st.success(f"📅 Showing {len(df_filtered)} matches for {date_filter.lower()}")
                df = df_filtered
            else:
                st.warning(f"⚠️ No matches found for {date_filter.lower()}. Showing all matches.")
        
        # Determine sort method
        sort_by = 'date' if sort_option == "Date (Soonest)" else 'edge' if sort_option == "Edge (Highest)" else 'probability'
        
        # ============== MERGE DATA IF API DATA AVAILABLE ==============
        merged_data = st.session_state.get('merged_data')
        merged_analyses = st.session_state.get('merged_analyses', [])
        
        # Show merge controls if API fixtures available
        if API_MODULES_AVAILABLE and st.session_state.api_fixtures:
            # Need to merge - show button to trigger merge
            col1, col2 = st.columns([2, 1])
            with col1:
                if merged_data:
                    matches_with_api = sum(1 for m in merged_data if m.get('has_api_data'))
                    st.success(f"✅ Merged: {matches_with_api}/{len(merged_data)} matches")
                else:
                    st.info(f"📡 {len(st.session_state.api_fixtures)} API fixtures ready")
            with col2:
                merge_now = st.button("🔄 Merge Data", use_container_width=True, key="merge_btn")
            
            if merge_now:
                with st.spinner("🔄 Merging predictions with live data..."):
                    try:
                        merger = DataMerger()
                        # Convert API fixtures to full data format
                        api = APIFootball("8333df5e3877e41485704e1c3ad026e6")
                        full_fixtures = []
                        progress_bar = st.progress(0)
                        status_text = st.empty()
                        fixtures_to_process = st.session_state.api_fixtures[:100]  # Process more fixtures
                        
                        odds_found = 0
                        for i, fix in enumerate(fixtures_to_process):
                            try:
                                fixture_id = fix.get('fixture', {}).get('id')
                                if fixture_id:
                                    full_data = api.get_full_match_data(fixture_id)
                                    if full_data:
                                        full_fixtures.append(full_data)
                                        # Check if odds were found
                                        if full_data.get('odds'):
                                            odds_found += 1
                            except Exception as e:
                                pass  # Skip errors for individual fixtures
                            progress_bar.progress((i + 1) / len(fixtures_to_process))
                            status_text.text(f"Fetching fixture {i+1}/{len(fixtures_to_process)} (Odds found: {odds_found})")
                        
                        progress_bar.empty()
                        status_text.empty()
                        
                        merged_data = merger.merge_data(df, full_fixtures)
                        
                        # Analyze each merged record
                        merged_analyses = []
                        for record in merged_data:
                            analysis = analyze_merged_match(record)
                            merged_analyses.append(analysis)
                        
                        # Store results in session state
                        st.session_state.merged_data = merged_data
                        st.session_state.merged_analyses = merged_analyses
                        
                        matches_with_api = sum(1 for m in merged_data if m.get('has_api_data'))
                        with_odds = sum(1 for m in merged_data if m.get('bookmaker_odds', {}).get('home_win'))
                        st.success(f"✅ Merged {matches_with_api}/{len(merged_data)} with API-Football V3 ({with_odds} with odds)")
                        
                        # Show unmatched teams info
                        if hasattr(merger, 'unmatched_teams') and merger.unmatched_teams:
                            with st.expander(f"⚠️ {len(merger.unmatched_teams)} unmatched matches (not in API)", expanded=False):
                                st.markdown("**These matches from CSV were not found in API fixtures:**")
                                for i, um in enumerate(merger.unmatched_teams[:10]):
                                    st.text(f"{i+1}. {um['home']} vs {um['away']} ({um['league']})")
                                if len(merger.unmatched_teams) > 10:
                                    st.text(f"... and {len(merger.unmatched_teams) - 10} more")
                                st.info("💡 These leagues may not be supported by Winner or the matches aren't scheduled yet in the API.")
                        
                        st.rerun()  # Refresh to show merged data
                    except Exception as e:
                        st.warning(f"⚠️ Merge error: {e}")
        
        # Create tabs for different analysis views (including basketball placeholder)
        tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9, tab10 = st.tabs([
            "🏆 Best Bets", 
            "🔥 Merged Analysis",
            "🎰 Accumulators", 
            "🧪 Game Lab",
            "📊 Team Stats",
            "📈 League Stats", 
            "📋 All Fixtures",
            "🌐 Live API Data",
            "🤖 AI Analysis",
            "🏀 Basketball"
        ])
        
        # ============== TAB 1: BEST BETS ==============
        with tab1:
            st.header("🏆 Top Value Bets")
            
            # Date range info
            today = datetime.now()
            st.markdown(f"*📅 Today: {today.strftime('%A, %d %B %Y')} | Showing bets with probability ≥ {min_prob_threshold*100:.0f}%*")
            
            best_bets_df = get_best_bets(df, min_probability=min_prob_threshold, top_n=top_bets_count, sort_by=sort_by)
            
            if not best_bets_df.empty:
                # Add confidence column
                def get_confidence(prob):
                    prob = normalize_probability(prob)
                    if prob >= 0.70:
                        return "🟢 HIGH"
                    elif prob >= 0.60:
                        return "🟡 MEDIUM"
                    else:
                        return "🔴 LOW"
                
                display_df = best_bets_df.copy()
                display_df['confidence'] = display_df['probability'].apply(get_confidence)
                display_df['probability_pct'] = (display_df['probability'] * 100).round(1).astype(str) + '%'
                
                # Display as styled table with date
                st.dataframe(
                    display_df[['match', 'league', 'date', 'bet_type', 'market', 'probability_pct', 'confidence']].rename(
                        columns={'probability_pct': 'probability'}
                    ),
                    use_container_width=True,
                    hide_index=True
                )
                
                # Summary stats
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total Value Bets", len(best_bets_df))
                with col2:
                    home_wins = len(best_bets_df[best_bets_df['bet_type'] == 'Home Win'])
                    st.metric("Home Wins", home_wins)
                with col3:
                    over_bets = len(best_bets_df[best_bets_df['bet_type'].str.contains('Over')])
                    st.metric("Over Goals Bets", over_bets)
                
                # Export section
                st.markdown("---")
                st.subheader("📥 Export Bets")
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    # Generate HTML report
                    accumulators = get_accumulator_suggestions(df, num_legs=accumulator_legs, min_prob=0.65)
                    league_stats = get_league_stats(df)
                    
                    # Prepare best_bets for export (with raw probability)
                    export_bets = get_best_bets(df, min_probability=min_prob_threshold, top_n=top_bets_count)
                    
                    html_report = generate_html_report(export_bets, accumulators, league_stats)
                    
                    st.download_button(
                        label="📄 Download HTML Report",
                        data=html_report,
                        file_name=f"betting_report_{pd.Timestamp.now().strftime('%Y%m%d_%H%M')}.html",
                        mime="text/html",
                        use_container_width=True
                    )
                
                with col2:
                    # Hebrew HTML export
                    hebrew_html_report = generate_hebrew_html_report(export_bets, accumulators, league_stats)
                    
                    st.download_button(
                        label="🇮🇱 Download Hebrew HTML",
                        data=hebrew_html_report,
                        file_name=f"betting_report_hebrew_{pd.Timestamp.now().strftime('%Y%m%d_%H%M')}.html",
                        mime="text/html",
                        use_container_width=True
                    )
                
                with col3:
                    # CSV export
                    csv_data = best_bets_df.to_csv(index=False)
                    st.download_button(
                        label="📊 Download CSV Data",
                        data=csv_data,
                        file_name=f"best_bets_{pd.Timestamp.now().strftime('%Y%m%d_%H%M')}.csv",
                        mime="text/csv",
                        use_container_width=True
                    )
            else:
                st.warning("No bets found matching your criteria. Try lowering the probability threshold.")
        
        # ============== TAB 2: MERGED ANALYSIS ==============
        with tab2:
            st.header("🔥 Merged Analysis - Model + Live Data")
            st.markdown("*Combining your predictions with live API-Football data for the most accurate insights*")
            
            if merged_analyses:
                # Get ALL merged bets (not limited)
                all_merged_bets = get_top_bets(merged_data, top_n=999) if merged_data else []
                
                # Summary metrics
                col1, col2, col3, col4, col5 = st.columns(5)
                with col1:
                    st.metric("Matches Merged", len(merged_data))
                with col2:
                    with_api = sum(1 for m in merged_data if m.get('has_api_data'))
                    st.metric("API-Football", with_api)
                with col3:
                    with_odds = sum(1 for m in merged_data if m.get('bookmaker_odds') and any(v for k, v in m.get('bookmaker_odds', {}).items() if k not in ['raw_odds', 'bookmaker'] and v))
                    st.metric("With Odds", with_odds)
                with col4:
                    value_bets = sum(1 for b in all_merged_bets if b.get('is_value_bet'))
                    st.metric("Value Bets", value_bets)
                with col5:
                    high_conf = sum(1 for b in all_merged_bets if b.get('confidence') == 'HIGH')
                    st.metric("High Confidence", high_conf)
                
                # Controls for how many to display
                st.markdown("---")
                col1, col2, col3 = st.columns([2, 2, 2])
                with col1:
                    show_all_matches = st.checkbox("📋 Show ALL matches", value=True, key="show_all_merged")
                with col2:
                    if not show_all_matches:
                        max_matches_display = st.slider("Max matches to show:", 5, 100, 20, key="max_merged_display")
                    else:
                        max_matches_display = 999
                with col3:
                    filter_api_only = st.checkbox("🔗 Only with API data", value=False, key="filter_api_merged")
                
                # Debug info
                with st.expander("🔧 Merge Debug Info", expanded=False):
                    st.write(f"**Total CSV rows:** {len(merged_data)}")
                    st.write(f"**Matched with API:** {sum(1 for m in merged_data if m.get('has_api_data'))}")
                    st.write(f"**Total bets found:** {len(all_merged_bets)}")
                    
                    # Show first few unmatched
                    unmatched = [m for m in merged_data if not m.get('has_api_data')][:5]
                    if unmatched:
                        st.write("**First 5 unmatched matches:**")
                        for m in unmatched:
                            st.write(f"- {m.get('csv_home')} vs {m.get('csv_away')} ({m.get('csv_date')})")
                    
                    # Show first few matched
                    matched = [m for m in merged_data if m.get('has_api_data')][:5]
                    if matched:
                        st.write("**First 5 matched matches:**")
                        for m in matched:
                            odds_info = m.get('bookmaker_odds', {})
                            has_odds = "✅" if odds_info.get('home_win') else "❌"
                            st.write(f"- {m.get('csv_home')} ({m.get('api_home')}) vs {m.get('csv_away')} ({m.get('api_away')}) | Odds: {has_odds}")
                            if odds_info.get('home_win'):
                                st.write(f"  Odds: H={odds_info.get('home_win')}, D={odds_info.get('draw')}, A={odds_info.get('away_win')}")
                
                st.markdown("---")
                
                # Top Bets Table - show all or limited
                bets_to_display = all_merged_bets[:max_matches_display] if not show_all_matches else all_merged_bets
                st.subheader(f"🏆 Top Bets ({len(bets_to_display)} shown of {len(all_merged_bets)} total)")
                if bets_to_display:
                    st.markdown(format_top_bets_table(bets_to_display))
                
                st.markdown("---")
                
                # Accumulator Suggestions from Merged Data
                st.subheader("🎰 Accumulator Suggestions (From Merged Data)")
                merged_accs = generate_accumulators(merged_data, legs=accumulator_legs, min_prob=0.65) if merged_data else []
                
                for acc in merged_accs:
                    st.markdown(format_accumulator(acc))
                
                st.markdown("---")
                
                # Detailed Betting Panels - show ALL matches
                st.subheader(f"📋 All Match Panels ({len(merged_analyses)} matches)")
                
                # Filter and sort matches
                matches_to_show = merged_analyses.copy()
                if filter_api_only:
                    matches_to_show = [a for a in matches_to_show if a.get('has_api_data')]
                
                # Sort by recommendations (best first)
                matches_to_show.sort(
                    key=lambda x: (
                        x['recommendations'][0].get('edge', 0) if x.get('recommendations') else -1,
                        x['recommendations'][0].get('adjusted_probability', 0) if x.get('recommendations') else 0
                    ),
                    reverse=True
                )
                
                # Apply limit if not showing all
                if not show_all_matches:
                    matches_to_show = matches_to_show[:max_matches_display]
                
                st.caption(f"Displaying {len(matches_to_show)} matches")
                
                # Get team stats for hexagon visualization
                team_stats_df = get_team_stats(df) if 'get_team_stats' in dir() else None
                
                for analysis in matches_to_show:
                    # Build source badges
                    sources = {
                        'csv': True,
                        'api_football': analysis.get('has_api_data', False),
                        'live': False
                    }
                    
                    badges = []
                    if sources['csv']:
                        badges.append("📊 CSV")
                    if sources['api_football']:
                        badges.append("⚽ API-Football V3")
                    if sources['live']:
                        badges.append("🔴 LIVE")
                    
                    badge_str = " | ".join(badges)
                    
                    with st.expander(f"⚽ {analysis['match']} | {analysis['league']} [{badge_str}]", expanded=False):
                        # Standard betting panel
                        st.markdown(format_betting_panel(analysis))
                        
                        # Add hexagon stats visualization
                        match_str = analysis.get('match', 'Unknown vs Unknown')
                        if ' vs ' in match_str:
                            home_team, away_team = match_str.split(' vs ', 1)
                            home_stats, away_stats = get_team_stats_for_match(home_team.strip(), away_team.strip(), team_stats_df)
                            
                            st.markdown("---")
                            st.markdown("### ⬡ Team Performance Radar")
                            st.markdown(render_match_hexagons(home_team.strip(), away_team.strip(), home_stats, away_stats, size=160), unsafe_allow_html=True)
                            
                            # Add form indicators if available from cache
                            if TEAM_CACHE_AVAILABLE:
                                cache = get_cache()
                                if cache:
                                    home_form = cache.get_team_form(team_name=home_team.strip())
                                    away_form = cache.get_team_form(team_name=away_team.strip())
                                    
                                    if home_form or away_form:
                                        st.markdown("### 📈 Recent Form")
                                        form_col1, form_col2 = st.columns(2)
                                        with form_col1:
                                            if home_form:
                                                st.markdown(f"**{home_team.strip()}:** ", unsafe_allow_html=False)
                                                st.markdown(render_form_badge(home_form.get('form_string', '')), unsafe_allow_html=True)
                                        with form_col2:
                                            if away_form:
                                                st.markdown(f"**{away_team.strip()}:** ", unsafe_allow_html=False)
                                                st.markdown(render_form_badge(away_form.get('form_string', '')), unsafe_allow_html=True)
                
                # Export full report
                st.markdown("---")
                st.subheader("📥 Export Full Merged Report")
                
                full_report = generate_full_report(merged_analyses, all_merged_bets, merged_accs)
                
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.download_button(
                        label="📄 HTML Report",
                        data=generate_html_report(pd.DataFrame(all_merged_bets) if all_merged_bets else pd.DataFrame(), merged_accs, get_league_stats(df)),
                        file_name=f"merged_report_{pd.Timestamp.now().strftime('%Y%m%d_%H%M')}.html",
                        mime="text/html",
                        use_container_width=True
                    )
                with col2:
                    st.download_button(
                        label="🇮🇱 Hebrew HTML",
                        data=generate_hebrew_html_report(pd.DataFrame(all_merged_bets) if all_merged_bets else pd.DataFrame(), merged_accs, get_league_stats(df)),
                        file_name=f"merged_report_heb_{pd.Timestamp.now().strftime('%Y%m%d_%H%M')}.html",
                        mime="text/html",
                        use_container_width=True
                    )
                with col3:
                    st.download_button(
                        label="📄 Markdown",
                        data=full_report,
                        file_name=f"merged_betting_report_{pd.Timestamp.now().strftime('%Y%m%d_%H%M')}.md",
                        mime="text/markdown",
                        use_container_width=True
                    )
                with col4:
                    if all_merged_bets:
                        merged_df = pd.DataFrame(all_merged_bets)
                        st.download_button(
                            label="📊 CSV Data",
                            data=merged_df.to_csv(index=False),
                            file_name=f"merged_bets_{pd.Timestamp.now().strftime('%Y%m%d_%H%M')}.csv",
                            mime="text/csv",
                            use_container_width=True
                        )
            else:
                st.info("👆 Click '🔄 Fetch Live Data' in the sidebar to merge with API-Football data")
                st.markdown("""
                **How it works:**
                1. Upload your predictions CSV
                2. Click 'Fetch Live Data' to get fixtures from API-Football
                3. The system automatically matches and merges:
                   - Your AI model probabilities
                   - Live bookmaker odds
                   - Injury reports
                   - Head-to-head records
                4. Get enhanced betting insights with value detection
                """)
        
        # ============== TAB 3: ACCUMULATORS ==============
        with tab3:
            st.header("🎰 Accumulator Builder")
            
            # Two sections: Auto-generated and Custom Builder
            acc_mode = st.radio(
                "Choose Mode:",
                ["🤖 Auto-Generate Best Accumulators", "🎯 Build Around Your Pick"],
                horizontal=True
            )
            
            st.markdown("---")
            
            if acc_mode == "🤖 Auto-Generate Best Accumulators":
                st.subheader(f"🤖 Auto-Generated {accumulator_legs}-Fold Accumulators")
                st.markdown(f"*Best {accumulator_legs}-fold accumulators with highest probability picks*")
                
                accumulators = get_accumulator_suggestions(df, num_legs=accumulator_legs, min_prob=0.65)
                
                if accumulators:
                    for acc in accumulators:
                        st.subheader(f"📋 {acc['type']}")
                        st.markdown(f"**Combined Probability: {acc['combined_probability']*100:.1f}%**")
                        
                        acc_data = []
                        for i, leg in enumerate(acc['legs'], 1):
                            leg_bet = leg.get('bet', leg.get('bet_type', leg.get('pick', 'N/A')))
                            leg_prob = leg.get('prob', leg.get('probability', 0))
                            if isinstance(leg_prob, str):
                                leg_prob = float(leg_prob.replace('%', '')) / 100
                            acc_data.append({
                                'Leg': f"#{i}",
                                'Match': leg.get('match', 'N/A'),
                                'Date': leg.get('date', 'TBD'),
                                'Pick': leg_bet,
                                'Probability': f"{leg_prob*100:.1f}%"
                            })
                        
                        st.dataframe(pd.DataFrame(acc_data), use_container_width=True, hide_index=True)
                        
                        # Kelly criterion for the accumulator
                        kelly_stake = get_kelly_criterion(acc['combined_probability'], 1/acc['combined_probability'] + 1)
                        st.info(f"💰 Suggested stake (Kelly): {kelly_stake*100:.1f}% of bankroll")
                    
                    # Export Accumulators
                    st.markdown("---")
                    st.subheader("📥 Export Accumulators")
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        acc_html = generate_accumulator_html(accumulators, lang='en')
                        st.download_button(
                            label="📄 HTML Report",
                            data=acc_html,
                            file_name=f"accumulators_{pd.Timestamp.now().strftime('%Y%m%d_%H%M')}.html",
                            mime="text/html",
                            use_container_width=True
                        )
                    with col2:
                        acc_html_heb = generate_accumulator_html(accumulators, lang='he')
                        st.download_button(
                            label="🇮🇱 Hebrew HTML",
                            data=acc_html_heb,
                            file_name=f"accumulators_heb_{pd.Timestamp.now().strftime('%Y%m%d_%H%M')}.html",
                            mime="text/html",
                            use_container_width=True
                        )
                else:
                    st.warning("Not enough high-probability bets for accumulators. Try lowering requirements.")
            
            else:  # Custom Builder
                st.subheader("🎯 Build Your Own Custom Accumulator")
                st.markdown("*Select matches and bets to create your perfect accumulator*")
                
                # Initialize custom accumulator in session state
                if 'custom_acc_legs' not in st.session_state:
                    st.session_state.custom_acc_legs = []
                
                # Mode selector
                build_mode = st.radio(
                    "Build Mode:",
                    ["🎲 Full Custom (Pick matches & bets)", "🤖 AI-Assisted (Pick matches, AI suggests bets)"],
                    horizontal=True,
                    key="custom_acc_mode"
                )
                
                st.markdown("---")
                
                # Available matches
                match_options = []
                match_data = {}
                for idx, row in df.iterrows():
                    home = row.get('home', 'Unknown')
                    away = row.get('away', 'Unknown')
                    league = row.get('league', 'Unknown')
                    date_str = format_match_datetime(row.get('date'))
                    match_str = f"{home} vs {away}"
                    match_options.append(match_str)
                    match_data[match_str] = {
                        'home': home,
                        'away': away,
                        'league': league,
                        'date': date_str,
                        'row': row.to_dict()
                    }
                
                # Bet type options - Winner/Toto standard markets
                bet_options = [
                    # 1X2
                    'Home Win', 'Draw', 'Away Win',
                    # Goals (Over 2.5 is standard)
                    'Over 2.5', 'Over 3.5', 'Under 2.5', 'Under 3.5',
                    # BTTS
                    'BTTS Yes', 'BTTS No',
                    # Asian Handicap
                    'Home -1', 'Home -2', 'Away +1', 'Away +2',
                    'Home +1', 'Home +2', 'Away -1', 'Away -2',
                ]
                
                # Double Chance - available for manual selection only (Winner doesn't support)
                bet_options_with_dc = bet_options + [
                    '--- Double Chance (Manual Only) ---',
                    'Home or Draw (1X)', 'Away or Draw (X2)', 'Home or Away (12)',
                ]
                
                # ============ ADD NEW LEG SECTION ============
                st.subheader("➕ Add Match to Accumulator")
                
                col1, col2, col3 = st.columns([3, 2, 1])
                
                with col1:
                    new_match = st.selectbox(
                        "Select Match:",
                        options=["-- Select a match --"] + match_options,
                        key="new_acc_match"
                    )
                
                with col2:
                    if build_mode.startswith("🎲"):
                        # Full custom - user picks bet (includes double chance for manual selection)
                        new_bet = st.selectbox(
                            "Select Bet:",
                            options=bet_options_with_dc,
                            key="new_acc_bet"
                        )
                        # Filter out separator option
                        if new_bet and new_bet.startswith('---'):
                            st.warning("⚠️ Please select a valid bet type")
                            new_bet = None
                    else:
                        # AI-assisted - show recommended bet
                        if new_match and new_match != "-- Select a match --" and new_match in match_data:
                            match_info = match_data[new_match]
                            row = match_info['row']
                            
                            # Find best bet for this match
                            best_bet, best_prob = get_best_bet_for_match(row)
                            st.info(f"🤖 AI Suggests: **{best_bet}** ({best_prob*100:.1f}%)")
                            
                            # Allow override
                            override_bet = st.checkbox("Override AI suggestion", key="override_ai_bet")
                            if override_bet:
                                new_bet = st.selectbox(
                                    "Your Bet:",
                                    options=bet_options_with_dc,
                                    key="new_acc_bet_override"
                                )
                                if new_bet and new_bet.startswith('---'):
                                    new_bet = None
                            else:
                                new_bet = best_bet
                        else:
                            new_bet = None
                            st.caption("Select a match first")
                
                with col3:
                    st.markdown("")
                    st.markdown("")
                    add_disabled = new_match == "-- Select a match --" or new_bet is None
                    if st.button("➕ Add", type="primary", use_container_width=True, disabled=add_disabled, key="add_leg_btn"):
                        if new_match in match_data:
                            match_info = match_data[new_match]
                            row = match_info['row']
                            prob = get_bet_probability_from_row(row, new_bet) or 0.5
                            
                            # Check if match already in accumulator
                            existing_matches = [leg['match'] for leg in st.session_state.custom_acc_legs]
                            if new_match in existing_matches:
                                st.warning("⚠️ This match is already in your accumulator!")
                            else:
                                # Get odds from merged data if available
                                odds = None
                                for m in (st.session_state.get('merged_data', []) or []):
                                    csv_home = m.get('csv_home', '')
                                    csv_away = m.get('csv_away', '')
                                    if match_info['home'].lower() in csv_home.lower() or csv_home.lower() in match_info['home'].lower():
                                        if match_info['away'].lower() in csv_away.lower() or csv_away.lower() in match_info['away'].lower():
                                            odds = get_odds_for_bet_type(m.get('bookmaker_odds', {}), new_bet)
                                            break
                                
                                st.session_state.custom_acc_legs.append({
                                    'match': new_match,
                                    'home': match_info['home'],
                                    'away': match_info['away'],
                                    'league': match_info['league'],
                                    'date': match_info['date'],
                                    'bet': new_bet,
                                    'probability': prob,
                                    'odds': odds
                                })
                                st.rerun()
                
                st.markdown("---")
                
                # ============ CURRENT ACCUMULATOR ============
                st.subheader(f"📋 Your Accumulator ({len(st.session_state.custom_acc_legs)} legs)")
                
                if st.session_state.custom_acc_legs:
                    # Calculate combined stats
                    combined_prob = 1.0
                    combined_odds = 1.0
                    has_all_odds = True
                    
                    for leg in st.session_state.custom_acc_legs:
                        combined_prob *= leg['probability']
                        if leg['odds']:
                            combined_odds *= leg['odds']
                        else:
                            has_all_odds = False
                    
                    # Summary metrics
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Legs", len(st.session_state.custom_acc_legs))
                    with col2:
                        st.metric("Combined Probability", f"{combined_prob*100:.2f}%")
                    with col3:
                        if has_all_odds:
                            st.metric("Combined Odds", f"{combined_odds:.2f}")
                        else:
                            st.metric("Combined Odds", "Partial")
                    with col4:
                        kelly = get_kelly_criterion(combined_prob, combined_odds if has_all_odds else 1/combined_prob)
                        st.metric("Kelly Stake", f"{kelly*100:.1f}%")
                    
                    st.markdown("")
                    
                    # Display legs with edit/remove options
                    for i, leg in enumerate(st.session_state.custom_acc_legs):
                        col1, col2, col3, col4, col5 = st.columns([3, 2, 1.5, 1, 0.5])
                        
                        with col1:
                            st.markdown(f"**{i+1}. {leg['match']}**")
                            st.caption(f"{leg['league']} | {leg['date']}")
                        
                        with col2:
                            # Editable bet selector
                            new_bet_for_leg = st.selectbox(
                                "Bet",
                                options=bet_options,
                                index=bet_options.index(leg['bet']) if leg['bet'] in bet_options else 0,
                                key=f"edit_bet_{i}",
                                label_visibility="collapsed"
                            )
                            if new_bet_for_leg != leg['bet']:
                                # Update bet and probability
                                row_data = match_data.get(leg['match'], {}).get('row', {})
                                new_prob = get_bet_probability_from_row(row_data, new_bet_for_leg) or 0.5
                                st.session_state.custom_acc_legs[i]['bet'] = new_bet_for_leg
                                st.session_state.custom_acc_legs[i]['probability'] = new_prob
                                # Update odds
                                for m in (st.session_state.get('merged_data', []) or []):
                                    csv_home = m.get('csv_home', '')
                                    if leg['home'].lower() in csv_home.lower():
                                        st.session_state.custom_acc_legs[i]['odds'] = get_odds_for_bet_type(m.get('bookmaker_odds', {}), new_bet_for_leg)
                                        break
                                st.rerun()
                        
                        with col3:
                            prob_display = f"{leg['probability']*100:.1f}%"
                            odds_display = f"@ {leg['odds']:.2f}" if leg['odds'] else ""
                            st.markdown(f"**{prob_display}** {odds_display}")
                        
                        with col4:
                            # Confidence indicator
                            if leg['probability'] >= 0.70:
                                st.markdown("🟢 HIGH")
                            elif leg['probability'] >= 0.60:
                                st.markdown("🟡 MED")
                            else:
                                st.markdown("🔴 LOW")
                        
                        with col5:
                            if st.button("🗑️", key=f"remove_leg_{i}", help="Remove this leg"):
                                st.session_state.custom_acc_legs.pop(i)
                                st.rerun()
                        
                        st.markdown("---")
                    
                    # Action buttons
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        if st.button("🗑️ Clear All", use_container_width=True, key="clear_custom_acc"):
                            st.session_state.custom_acc_legs = []
                            st.rerun()
                    
                    with col2:
                        # Export as HTML
                        if st.session_state.custom_acc_legs:
                            custom_acc_for_export = [{
                                'type': f"Custom {len(st.session_state.custom_acc_legs)}-Fold Accumulator",
                                'combined_probability': combined_prob,
                                'combined_odds': combined_odds if has_all_odds else None,
                                'legs': [
                                    {
                                        'match': leg['match'],
                                        'date': leg['date'],
                                        'bet': leg['bet'],
                                        'prob': leg['probability'],
                                        'odds': leg['odds']
                                    }
                                    for leg in st.session_state.custom_acc_legs
                                ]
                            }]
                            custom_html = generate_accumulator_html(custom_acc_for_export, lang='en')
                            st.download_button(
                                label="📄 Export HTML",
                                data=custom_html,
                                file_name=f"my_accumulator_{pd.Timestamp.now().strftime('%Y%m%d_%H%M')}.html",
                                mime="text/html",
                                use_container_width=True,
                                key="export_custom_acc_html"
                            )
                    
                    with col3:
                        # Hebrew export
                        if st.session_state.custom_acc_legs:
                            custom_html_heb = generate_accumulator_html(custom_acc_for_export, lang='he')
                            st.download_button(
                                label="🇮🇱 Hebrew HTML",
                                data=custom_html_heb,
                                file_name=f"my_accumulator_heb_{pd.Timestamp.now().strftime('%Y%m%d_%H%M')}.html",
                                mime="text/html",
                                use_container_width=True,
                                key="export_custom_acc_heb"
                            )
                    
                    # Risk Assessment
                    st.markdown("---")
                    st.subheader("📊 Accumulator Analysis")
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.markdown("**Risk Assessment:**")
                        if combined_prob >= 0.25:
                            st.success("🟢 Lower Risk - Decent combined probability")
                        elif combined_prob >= 0.10:
                            st.warning("🟡 Medium Risk - Moderate combined probability")
                        else:
                            st.error("🔴 High Risk - Low combined probability")
                        
                        # Confidence breakdown
                        high_conf = sum(1 for leg in st.session_state.custom_acc_legs if leg['probability'] >= 0.70)
                        med_conf = sum(1 for leg in st.session_state.custom_acc_legs if 0.60 <= leg['probability'] < 0.70)
                        low_conf = sum(1 for leg in st.session_state.custom_acc_legs if leg['probability'] < 0.60)
                        
                        st.markdown(f"- 🟢 High confidence legs: **{high_conf}**")
                        st.markdown(f"- 🟡 Medium confidence legs: **{med_conf}**")
                        st.markdown(f"- 🔴 Low confidence legs: **{low_conf}**")
                    
                    with col2:
                        st.markdown("**Potential Returns:**")
                        stake = 10  # Base stake for calculation
                        if has_all_odds:
                            potential_return = stake * combined_odds
                            st.markdown(f"- Stake: **${stake:.2f}**")
                            st.markdown(f"- Combined Odds: **{combined_odds:.2f}**")
                            st.markdown(f"- Potential Return: **${potential_return:.2f}**")
                            st.markdown(f"- Potential Profit: **${potential_return - stake:.2f}**")
                        else:
                            estimated_odds = 1 / combined_prob if combined_prob > 0 else 10
                            st.markdown(f"- Estimated Odds: **{estimated_odds:.2f}**")
                            st.markdown(f"- (Based on probability, actual odds may vary)")
                
                else:
                    st.info("👆 Add matches above to build your accumulator")
                    
                    # Quick add suggestions
                    st.markdown("---")
                    st.subheader("💡 Quick Suggestions")
                    
                    # Get top bets
                    top_bets_for_suggestion = get_best_bets(df, min_probability=0.65, top_n=5)
                    
                    if not top_bets_for_suggestion.empty:
                        st.markdown("**Top high-probability bets you could add:**")
                        for idx, row in top_bets_for_suggestion.iterrows():
                            match_name = row.get('match', 'Unknown')
                            bet_type = row.get('bet_type', 'N/A')
                            prob = row.get('probability', 0)
                            
                            col1, col2 = st.columns([4, 1])
                            with col1:
                                st.markdown(f"- **{match_name}** - {bet_type} ({prob*100:.1f}%)")
                            with col2:
                                if st.button("➕", key=f"quick_add_{idx}", help="Add to accumulator"):
                                    # Find match data
                                    for m_str, m_data in match_data.items():
                                        if m_data['home'] in match_name and m_data['away'] in match_name:
                                            st.session_state.custom_acc_legs.append({
                                                'match': m_str,
                                                'home': m_data['home'],
                                                'away': m_data['away'],
                                                'league': m_data['league'],
                                                'date': m_data['date'],
                                                'bet': bet_type,
                                                'probability': prob,
                                                'odds': None
                                            })
                                            st.rerun()
                                            break
        
        # ============== TAB 4: GAME LAB (Self-Analyze) ==============
        with tab4:
            st.header("🧪 Game Lab - Custom Analysis")
            st.markdown("*Adjust match parameters and see how they affect betting recommendations*")
            
            # Match selector
            col1, col2 = st.columns([2, 1])
            with col1:
                match_options_lab = []
                match_data_lab = {}
                for idx, row in df.iterrows():
                    home = row.get('home', 'Unknown')
                    away = row.get('away', 'Unknown')
                    league = row.get('league', 'Unknown')
                    date_fmt = format_match_datetime(row.get('date'))
                    match_str = f"{home} vs {away} | {league} | {date_fmt}"
                    match_options_lab.append(match_str)
                    match_data_lab[match_str] = row.to_dict()
                
                selected_lab_match = st.selectbox(
                    "🔬 Select a match to analyze:",
                    options=match_options_lab,
                    key="lab_match_select"
                )
            
            with col2:
                st.markdown("")
                st.markdown("")
                if st.button("🔄 Reset to Original", use_container_width=True):
                    st.rerun()
            
            if selected_lab_match and selected_lab_match in match_data_lab:
                match = match_data_lab[selected_lab_match]
                
                st.markdown("---")
                
                # Original probabilities
                st.subheader("📊 Original Model Predictions")
                orig_home = pd.to_numeric(match.get('1x2_h', 0), errors='coerce') or 0
                orig_draw = pd.to_numeric(match.get('1x2_d', 0), errors='coerce') or 0
                orig_away = pd.to_numeric(match.get('1x2_a', 0), errors='coerce') or 0
                orig_over25 = pd.to_numeric(match.get('o_2.5', 0), errors='coerce') or 0
                
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Home Win", f"{orig_home*100:.1f}%")
                with col2:
                    st.metric("Draw", f"{orig_draw*100:.1f}%")
                with col3:
                    st.metric("Away Win", f"{orig_away*100:.1f}%")
                with col4:
                    st.metric("Over 2.5", f"{orig_over25*100:.1f}%")
                
                st.markdown("---")
                
                # Adjustment sliders
                st.subheader("⚙️ Adjust Parameters")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown("**🏥 Injuries**")
                    injuries_home = st.slider(
                        f"🏠 {match.get('home', 'Home')} injuries:",
                        0, 6, 0, key="injuries_home",
                        help="Missing key players reduce team's winning probability"
                    )
                    injuries_away = st.slider(
                        f"✈️ {match.get('away', 'Away')} injuries:",
                        0, 6, 0, key="injuries_away",
                        help="Missing key players reduce team's winning probability"
                    )
                
                with col2:
                    st.markdown("**📈 Form & H2H Adjustments**")
                    h2h_boost = st.slider(
                        "Home team H2H advantage (%):",
                        -15, 15, 0, key="h2h_boost",
                        help="Positive = home team historically dominates, Negative = away team dominates"
                    )
                    form_adjustment = st.slider(
                        "Recent form adjustment (%):",
                        -20, 20, 0, key="form_adj",
                        help="Positive = home team in better form, Negative = away team in better form"
                    )
                
                st.markdown("---")
                
                # Manual probability overrides
                st.subheader("🎛️ Manual Probability Overrides (Optional)")
                
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    manual_home = st.number_input(
                        "Home Win %", 
                        min_value=0.0, max_value=100.0, 
                        value=float(orig_home * 100),
                        step=1.0,
                        key="manual_home"
                    ) / 100
                with col2:
                    manual_draw = st.number_input(
                        "Draw %", 
                        min_value=0.0, max_value=100.0, 
                        value=float(orig_draw * 100),
                        step=1.0,
                        key="manual_draw"
                    ) / 100
                with col3:
                    manual_away = st.number_input(
                        "Away Win %", 
                        min_value=0.0, max_value=100.0, 
                        value=float(orig_away * 100),
                        step=1.0,
                        key="manual_away"
                    ) / 100
                with col4:
                    manual_over25 = st.number_input(
                        "Over 2.5 %", 
                        min_value=0.0, max_value=100.0, 
                        value=float(orig_over25 * 100),
                        step=1.0,
                        key="manual_over25"
                    ) / 100
                
                st.markdown("---")
                
                # Calculate adjusted probabilities
                if st.button("🧮 Calculate Adjusted Analysis", type="primary", use_container_width=True):
                    # Use manual inputs if changed, otherwise apply injury/H2H adjustments
                    use_manual = (manual_home != orig_home or manual_draw != orig_draw or 
                                  manual_away != orig_away or manual_over25 != orig_over25)
                    
                    if use_manual:
                        adjusted = {
                            'home_prob': manual_home,
                            'draw_prob': manual_draw,
                            'away_prob': manual_away,
                            'over_25_prob': manual_over25,
                            'under_25_prob': 1 - manual_over25
                        }
                    else:
                        # Apply form adjustment to home probability
                        adj_home = orig_home * (1 + form_adjustment / 100)
                        adj_away = orig_away * (1 - form_adjustment / 100)
                        
                        adjusted = simulate_match_outcome(
                            adj_home, orig_draw, adj_away, orig_over25,
                            injuries_home, injuries_away, h2h_boost
                        )
                    
                    st.subheader("📈 Adjusted Analysis Results")
                    
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        delta_home = (adjusted['home_prob'] - orig_home) * 100
                        st.metric("Home Win", f"{adjusted['home_prob']*100:.1f}%", f"{delta_home:+.1f}%")
                    with col2:
                        delta_draw = (adjusted['draw_prob'] - orig_draw) * 100
                        st.metric("Draw", f"{adjusted['draw_prob']*100:.1f}%", f"{delta_draw:+.1f}%")
                    with col3:
                        delta_away = (adjusted['away_prob'] - orig_away) * 100
                        st.metric("Away Win", f"{adjusted['away_prob']*100:.1f}%", f"{delta_away:+.1f}%")
                    with col4:
                        delta_over = (adjusted['over_25_prob'] - orig_over25) * 100
                        st.metric("Over 2.5", f"{adjusted['over_25_prob']*100:.1f}%", f"{delta_over:+.1f}%")
                    
                    # Best bet recommendation
                    st.markdown("---")
                    st.subheader("🎯 Recommended Bet (After Adjustments)")
                    
                    best_bets = [
                        ('Home Win', adjusted['home_prob']),
                        ('Draw', adjusted['draw_prob']),
                        ('Away Win', adjusted['away_prob']),
                        ('Over 2.5', adjusted['over_25_prob']),
                        ('Under 2.5', adjusted['under_25_prob'])
                    ]
                    best_bets.sort(key=lambda x: x[1], reverse=True)
                    
                    best_bet = best_bets[0]
                    second_bet = best_bets[1]
                    
                    if best_bet[1] >= 0.70:
                        confidence = "🟢 HIGH"
                        conf_class = "high"
                    elif best_bet[1] >= 0.60:
                        confidence = "🟡 MEDIUM"
                        conf_class = "medium"
                    else:
                        confidence = "🔴 LOW"
                        conf_class = "low"
                    
                    kelly = get_kelly_criterion(best_bet[1], 1/best_bet[1])
                    
                    st.success(f"""
                    **🎯 Best Bet: {best_bet[0]}**
                    - Probability: {best_bet[1]*100:.1f}%
                    - Confidence: {confidence}
                    - Kelly Stake: {kelly*100:.1f}% of bankroll
                    
                    **Alternative: {second_bet[0]}** ({second_bet[1]*100:.1f}%)
                    """)
                    
                    # Show what changed
                    if injuries_home > 0 or injuries_away > 0 or h2h_boost != 0 or form_adjustment != 0:
                        st.markdown("---")
                        st.markdown("**📝 Adjustments Applied:**")
                        if injuries_home > 0:
                            st.markdown(f"- 🏥 Home injuries: -{10 if injuries_home >= 2 else 5}% to home win probability")
                        if injuries_away > 0:
                            st.markdown(f"- 🏥 Away injuries: -{10 if injuries_away >= 2 else 5}% to away win probability")
                        if h2h_boost != 0:
                            st.markdown(f"- 📊 H2H adjustment: {h2h_boost:+}% to home win probability")
                        if form_adjustment != 0:
                            st.markdown(f"- 📈 Form adjustment: {form_adjustment:+}% shift")
                    
                    # Export Game Lab Analysis
                    st.markdown("---")
                    st.subheader("📥 Export Analysis")
                    
                    # Create data for export
                    match_info = {
                        'match': selected_lab_match,
                        'home_team': match.get('home', 'Unknown'),
                        'away_team': match.get('away', 'Unknown'),
                        'league': match.get('league', 'Unknown'),
                        'date': format_match_datetime(match.get('date'))
                    }
                    
                    recommendations = {
                        'pick': best_bet[0],
                        'probability': best_bet[1],
                        'stake': kelly,
                        'confidence': confidence
                    }
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        game_lab_html = generate_game_lab_html(match_info, adjusted, recommendations, lang='en')
                        st.download_button(
                            label="📄 HTML Report",
                            data=game_lab_html,
                            file_name=f"game_lab_{pd.Timestamp.now().strftime('%Y%m%d_%H%M')}.html",
                            mime="text/html",
                            use_container_width=True,
                            key="game_lab_html_btn"
                        )
                    with col2:
                        game_lab_html_heb = generate_game_lab_html(match_info, adjusted, recommendations, lang='he')
                        st.download_button(
                            label="🇮🇱 Hebrew HTML",
                            data=game_lab_html_heb,
                            file_name=f"game_lab_heb_{pd.Timestamp.now().strftime('%Y%m%d_%H%M')}.html",
                            mime="text/html",
                            use_container_width=True,
                            key="game_lab_heb_btn"
                        )
                    
                    # Bet Override for Game Lab
                    st.markdown("---")
                    st.subheader("🔄 Save as Bet Override")
                    st.caption("Save your adjusted bet as an override for exports")
                    
                    match_key = f"{match.get('home', 'Unknown')}_{match.get('away', 'Unknown')}".replace(' ', '_')
                    
                    if st.button("💾 Save Override", key="save_game_lab_override"):
                        # Determine which market to override based on best bet
                        if best_bet[0] in ['Home Win', 'Draw', 'Away Win']:
                            market_key = f"{match_key}_1x2"
                            orig_bet = 'home_win' if best_bet[0] == 'Home Win' else ('draw' if best_bet[0] == 'Draw' else 'away_win')
                        else:
                            market_key = f"{match_key}_goals"
                            orig_bet = 'over_25' if 'Over' in best_bet[0] else 'under_25'
                        
                        st.session_state.bet_overrides[market_key] = {
                            'original_bet': orig_bet,
                            'override_bet': orig_bet,  # Same bet, but with adjusted probability
                            'original_prob': orig_home if 'Win' in best_bet[0] or 'Draw' in best_bet[0] else orig_over25,
                            'converted_prob': best_bet[1],
                            'source': 'game_lab'
                        }
                        st.success(f"✅ Saved: {best_bet[0]} @ {best_bet[1]*100:.1f}%")
        
        # ============== TAB 5: TEAM STATS (Attacking/Defense) ==============
        with tab5:
            st.header("📊 Team Statistics - Attacking & Defense")
            st.markdown("*Analyze team strengths based on model predictions*")
            
            team_stats = get_team_stats(df)
            
            if not team_stats.empty:
                # Summary metrics
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Total Teams", len(team_stats))
                with col2:
                    avg_attacking = team_stats['attacking_rating'].mean()
                    st.metric("Avg Attacking Rating", f"{avg_attacking:.1f}")
                with col3:
                    avg_defensive = team_stats['defensive_rating'].mean()
                    st.metric("Avg Defensive Rating", f"{avg_defensive:.1f}")
                with col4:
                    top_team = team_stats.iloc[0]['team']
                    st.metric("Strongest Team", top_team)
                
                st.markdown("---")
                
                # Filters
                col1, col2, col3 = st.columns(3)
                with col1:
                    leagues_list = ['All'] + sorted(team_stats['league'].unique().tolist())
                    selected_league_stats = st.selectbox("Filter by League:", leagues_list, key="team_stats_league")
                with col2:
                    search_team_stats = st.text_input("Search Team:", key="team_stats_search")
                with col3:
                    sort_by_stats = st.selectbox(
                        "Sort by:", 
                        ["Overall Strength", "Attacking Rating", "Defensive Rating", "Matches"],
                        key="team_stats_sort"
                    )
                
                # Apply filters
                filtered_stats = team_stats.copy()
                if selected_league_stats != 'All':
                    filtered_stats = filtered_stats[filtered_stats['league'] == selected_league_stats]
                if search_team_stats:
                    filtered_stats = filtered_stats[filtered_stats['team'].str.contains(search_team_stats, case=False, na=False)]
                
                # Sort
                sort_map = {
                    "Overall Strength": "overall_strength",
                    "Attacking Rating": "attacking_rating", 
                    "Defensive Rating": "defensive_rating",
                    "Matches": "matches"
                }
                filtered_stats = filtered_stats.sort_values(sort_map[sort_by_stats], ascending=False)
                
                st.markdown("---")
                
                # Two views: Attacking focus and Defensive focus
                view_mode = st.radio("View Mode:", ["🏆 Full Stats", "⚔️ Top Attackers", "🛡️ Best Defenders"], horizontal=True)
                
                if view_mode == "🏆 Full Stats":
                    st.subheader("📊 Complete Team Statistics")
                    
                    display_stats = filtered_stats.copy()
                    display_stats['attacking_rating'] = display_stats['attacking_rating'].round(1).astype(str) + '%'
                    display_stats['defensive_rating'] = display_stats['defensive_rating'].round(1).astype(str) + '%'
                    display_stats['overall_strength'] = display_stats['overall_strength'].round(1).astype(str) + '%'
                    display_stats['avg_win_prob'] = (display_stats['avg_win_prob'] * 100).round(1).astype(str) + '%'
                    
                    st.dataframe(
                        display_stats[['team', 'league', 'matches', 'attacking_rating', 'defensive_rating', 'overall_strength', 'avg_win_prob']].rename(columns={
                            'team': 'Team',
                            'league': 'League',
                            'matches': 'Matches',
                            'attacking_rating': 'Attacking',
                            'defensive_rating': 'Defensive',
                            'overall_strength': 'Overall',
                            'avg_win_prob': 'Avg Win %'
                        }),
                        use_container_width=True,
                        hide_index=True
                    )
                
                elif view_mode == "⚔️ Top Attackers":
                    st.subheader("⚔️ Top Attacking Teams")
                    st.markdown("*Teams most likely to score goals (based on Over 2.5 probability)*")
                    
                    top_attackers = filtered_stats.sort_values('attacking_rating', ascending=False).head(15)
                    
                    for i, (_, team) in enumerate(top_attackers.iterrows(), 1):
                        col1, col2, col3, col4 = st.columns([3, 1, 1, 1])
                        with col1:
                            st.markdown(f"**{i}. {team['team']}** ({team['league']})")
                        with col2:
                            st.markdown(f"⚔️ {team['attacking_rating']:.1f}%")
                        with col3:
                            st.markdown(f"🎯 {team['matches']} games")
                        with col4:
                            st.markdown(f"📊 {team['overall_strength']:.1f}%")
                    
                    st.markdown("---")
                    st.info("💡 **Tip:** High attacking teams are good for Over goals bets when they play")
                
                else:  # Best Defenders
                    st.subheader("🛡️ Best Defensive Teams")
                    st.markdown("*Teams most likely to keep clean sheets (based on Under 2.5 probability)*")
                    
                    top_defenders = filtered_stats.sort_values('defensive_rating', ascending=False).head(15)
                    
                    for i, (_, team) in enumerate(top_defenders.iterrows(), 1):
                        col1, col2, col3, col4 = st.columns([3, 1, 1, 1])
                        with col1:
                            st.markdown(f"**{i}. {team['team']}** ({team['league']})")
                        with col2:
                            st.markdown(f"🛡️ {team['defensive_rating']:.1f}%")
                        with col3:
                            st.markdown(f"🎯 {team['matches']} games")
                        with col4:
                            st.markdown(f"📊 {team['overall_strength']:.1f}%")
                    
                    st.markdown("---")
                    st.info("💡 **Tip:** High defensive teams are good for Under goals bets when they play")
                
                # Team comparison with Hexagon visualization
                st.markdown("---")
                st.subheader("⬡ Team Analysis & Comparison")
                
                col1, col2 = st.columns(2)
                with col1:
                    team1 = st.selectbox("Select Team 1:", options=team_stats['team'].tolist(), key="compare_team1")
                with col2:
                    team2 = st.selectbox("Select Team 2:", options=team_stats['team'].tolist(), key="compare_team2", index=1 if len(team_stats) > 1 else 0)
                
                if st.button("⬡ Analyze & Compare", type="primary", use_container_width=True):
                    t1 = team_stats[team_stats['team'] == team1].iloc[0].to_dict()
                    t2 = team_stats[team_stats['team'] == team2].iloc[0].to_dict()
                    
                    # Calculate advanced stats for hexagon
                    t1_hex = calculate_advanced_team_stats(t1)
                    t2_hex = calculate_advanced_team_stats(t2)
                    
                    st.markdown("---")
                    
                    # Hexagon charts side by side
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.markdown(f"### 🏠 {team1}")
                        # Generate and display hexagon SVG
                        hex_svg1 = generate_hexagon_svg(t1_hex, size=280)
                        st.markdown(f'<div class="hex-container">{hex_svg1}</div>', unsafe_allow_html=True)
                        
                        # Quick stats below hexagon
                        st.markdown("**Key Metrics:**")
                        stat_cols = st.columns(3)
                        with stat_cols[0]:
                            st.metric("⚔️ Attack", f"{t1_hex['attack']:.0f}%")
                        with stat_cols[1]:
                            st.metric("🛡️ Defense", f"{t1_hex['defense']:.0f}%")
                        with stat_cols[2]:
                            st.metric("📈 Form", f"{t1_hex['form']:.0f}%")
                    
                    with col2:
                        st.markdown(f"### ✈️ {team2}")
                        # Generate and display hexagon SVG
                        hex_svg2 = generate_hexagon_svg(t2_hex, size=280)
                        st.markdown(f'<div class="hex-container">{hex_svg2}</div>', unsafe_allow_html=True)
                        
                        # Quick stats below hexagon
                        st.markdown("**Key Metrics:**")
                        stat_cols = st.columns(3)
                        with stat_cols[0]:
                            st.metric("⚔️ Attack", f"{t2_hex['attack']:.0f}%")
                        with stat_cols[1]:
                            st.metric("🛡️ Defense", f"{t2_hex['defense']:.0f}%")
                        with stat_cols[2]:
                            st.metric("📈 Form", f"{t2_hex['form']:.0f}%")
                    
                    # Comparison analysis
                    st.markdown("---")
                    st.subheader("📊 Head-to-Head Comparison")
                    
                    # Calculate differences
                    diff_attack = t1_hex['attack'] - t2_hex['attack']
                    diff_defense = t1_hex['defense'] - t2_hex['defense']
                    diff_form = t1_hex['form'] - t2_hex['form']
                    diff_overall = t1['overall_strength'] - t2['overall_strength']
                    
                    comp_cols = st.columns(4)
                    with comp_cols[0]:
                        if diff_attack > 5:
                            st.success(f"⚔️ Attack: {team1} +{diff_attack:.1f}%")
                        elif diff_attack < -5:
                            st.error(f"⚔️ Attack: {team2} +{abs(diff_attack):.1f}%")
                        else:
                            st.info(f"⚔️ Attack: Even")
                    
                    with comp_cols[1]:
                        if diff_defense > 5:
                            st.success(f"🛡️ Defense: {team1} +{diff_defense:.1f}%")
                        elif diff_defense < -5:
                            st.error(f"🛡️ Defense: {team2} +{abs(diff_defense):.1f}%")
                        else:
                            st.info(f"🛡️ Defense: Even")
                    
                    with comp_cols[2]:
                        if diff_form > 5:
                            st.success(f"📈 Form: {team1} +{diff_form:.1f}%")
                        elif diff_form < -5:
                            st.error(f"📈 Form: {team2} +{abs(diff_form):.1f}%")
                        else:
                            st.info(f"📈 Form: Even")
                    
                    with comp_cols[3]:
                        if diff_overall > 5:
                            st.success(f"🏆 Overall: {team1} +{diff_overall:.1f}%")
                        elif diff_overall < -5:
                            st.error(f"🏆 Overall: {team2} +{abs(diff_overall):.1f}%")
                        else:
                            st.warning(f"🏆 Overall: Close match!")
                    
                    # Prediction suggestion
                    st.markdown("---")
                    st.subheader("🎯 Match Prediction")
                    
                    # Simple prediction based on overall strength
                    avg_attack = (t1_hex['attack'] + t2_hex['attack']) / 2
                    if diff_overall > 10:
                        prediction = f"**{team1}** is significantly stronger. Consider: Home Win or {team1} -1 Handicap"
                        conf = "🟢 HIGH"
                    elif diff_overall > 5:
                        prediction = f"**{team1}** has the edge. Consider: Home Win or Home/Draw (1X)"
                        conf = "🟡 MEDIUM"
                    elif diff_overall < -10:
                        prediction = f"**{team2}** is significantly stronger. Consider: Away Win or {team2} -1 Handicap"
                        conf = "🟢 HIGH"
                    elif diff_overall < -5:
                        prediction = f"**{team2}** has the edge. Consider: Away Win or Away/Draw (X2)"
                        conf = "🟡 MEDIUM"
                    else:
                        prediction = "**Close match!** Consider: Draw or checking form carefully"
                        conf = "🔴 LOW"
                    
                    st.markdown(f"{conf} Confidence")
                    st.markdown(prediction)
                    
                    # Goals suggestion
                    if avg_attack > 60:
                        st.info(f"⚽ **Goals:** Both teams have strong attacks (avg {avg_attack:.0f}%). Consider **Over 2.5 Goals**")
                    elif avg_attack < 40:
                        st.info(f"⚽ **Goals:** Both teams have weaker attacks (avg {avg_attack:.0f}%). Consider **Under 2.5 Goals**")
                
                # Export section for Team Stats
                st.markdown("---")
                st.subheader("📥 Export Team Stats")
                col1, col2, col3 = st.columns(3)
                with col1:
                    team_stats_csv = team_stats.to_csv(index=False)
                    st.download_button(
                        label="📊 CSV Data",
                        data=team_stats_csv,
                        file_name=f"team_stats_{pd.Timestamp.now().strftime('%Y%m%d_%H%M')}.csv",
                        mime="text/csv",
                        use_container_width=True,
                        key="export_team_stats_csv"
                    )
                with col2:
                    team_stats_html = f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"><title>Team Stats</title>
<style>body{{font-family:Arial;padding:20px}}table{{border-collapse:collapse;width:100%}}th,td{{border:1px solid #ddd;padding:8px;text-align:left}}th{{background:#4CAF50;color:white}}tr:nth-child(even){{background:#f2f2f2}}</style></head>
<body><h1>📊 Team Statistics</h1><p>Generated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}</p>
{team_stats.to_html(index=False)}</body></html>"""
                    st.download_button(
                        label="📄 HTML Report",
                        data=team_stats_html,
                        file_name=f"team_stats_{pd.Timestamp.now().strftime('%Y%m%d_%H%M')}.html",
                        mime="text/html",
                        use_container_width=True,
                        key="export_team_stats_html"
                    )
                with col3:
                    team_stats_heb_html = f"""<!DOCTYPE html>
<html dir="rtl" lang="he"><head><meta charset="UTF-8"><title>סטטיסטיקות קבוצות</title>
<style>body{{font-family:Arial;padding:20px;direction:rtl}}table{{border-collapse:collapse;width:100%}}th,td{{border:1px solid #ddd;padding:8px;text-align:right}}th{{background:#4CAF50;color:white}}tr:nth-child(even){{background:#f2f2f2}}</style></head>
<body><h1>📊 סטטיסטיקות קבוצות</h1><p>נוצר: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}</p>
{team_stats.to_html(index=False)}</body></html>"""
                    st.download_button(
                        label="🇮🇱 Hebrew HTML",
                        data=team_stats_heb_html,
                        file_name=f"team_stats_heb_{pd.Timestamp.now().strftime('%Y%m%d_%H%M')}.html",
                        mime="text/html",
                        use_container_width=True,
                        key="export_team_stats_heb"
                    )
            else:
                st.warning("No team statistics available. Upload a predictions file first.")
        
        # ============== TAB 6: LEAGUE STATS ==============
        with tab6:
            st.header("📊 League Analysis")
            
            league_stats = get_league_stats(df)
            
            if not league_stats.empty:
                col1, col2 = st.columns([2, 1])
                
                with col1:
                    st.subheader("League Statistics")
                    display_stats = league_stats.copy()
                    display_stats['avg_home_prob'] = (display_stats['avg_home_prob'] * 100).round(1).astype(str) + '%'
                    display_stats['avg_away_prob'] = (display_stats['avg_away_prob'] * 100).round(1).astype(str) + '%'
                    display_stats['avg_over25'] = (display_stats['avg_over25'] * 100).round(1).astype(str) + '%'
                    display_stats.columns = ['League', 'Matches', 'Avg Home Win', 'Avg Away Win', 'Avg Over 2.5']
                    st.dataframe(display_stats, use_container_width=True, hide_index=True)
                
                with col2:
                    st.subheader("Quick Stats")
                    st.metric("Total Leagues", len(league_stats))
                    st.metric("Total Matches", league_stats['matches'].sum())
                    
                    # High-scoring leagues
                    high_scoring = league_stats[league_stats['avg_over25'] > 0.5]
                    st.metric("High-Scoring Leagues", len(high_scoring))
                
                # Export section for League Stats
                st.markdown("---")
                st.subheader("📥 Export League Stats")
                col1, col2, col3 = st.columns(3)
                with col1:
                    league_stats_csv = league_stats.to_csv(index=False)
                    st.download_button(
                        label="📊 CSV Data",
                        data=league_stats_csv,
                        file_name=f"league_stats_{pd.Timestamp.now().strftime('%Y%m%d_%H%M')}.csv",
                        mime="text/csv",
                        use_container_width=True,
                        key="export_league_stats_csv"
                    )
                with col2:
                    league_stats_html = f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"><title>League Stats</title>
<style>body{{font-family:Arial;padding:20px}}table{{border-collapse:collapse;width:100%}}th,td{{border:1px solid #ddd;padding:8px;text-align:left}}th{{background:#2196F3;color:white}}tr:nth-child(even){{background:#f2f2f2}}</style></head>
<body><h1>📊 League Statistics</h1><p>Generated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}</p>
{league_stats.to_html(index=False)}</body></html>"""
                    st.download_button(
                        label="📄 HTML Report",
                        data=league_stats_html,
                        file_name=f"league_stats_{pd.Timestamp.now().strftime('%Y%m%d_%H%M')}.html",
                        mime="text/html",
                        use_container_width=True,
                        key="export_league_stats_html"
                    )
                with col3:
                    league_stats_heb_html = f"""<!DOCTYPE html>
<html dir="rtl" lang="he"><head><meta charset="UTF-8"><title>סטטיסטיקות ליגות</title>
<style>body{{font-family:Arial;padding:20px;direction:rtl}}table{{border-collapse:collapse;width:100%}}th,td{{border:1px solid #ddd;padding:8px;text-align:right}}th{{background:#2196F3;color:white}}tr:nth-child(even){{background:#f2f2f2}}</style></head>
<body><h1>📊 סטטיסטיקות ליגות</h1><p>נוצר: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}</p>
{league_stats.to_html(index=False)}</body></html>"""
                    st.download_button(
                        label="🇮🇱 Hebrew HTML",
                        data=league_stats_heb_html,
                        file_name=f"league_stats_heb_{pd.Timestamp.now().strftime('%Y%m%d_%H%M')}.html",
                        mime="text/html",
                        use_container_width=True,
                        key="export_league_stats_heb"
                    )
        
        # ============== TAB 7: ALL FIXTURES (with Analyze Button) ==============
        with tab7:
            st.header("📋 All Fixtures - Match Data")
            
            # Filters
            col1, col2, col3 = st.columns([2, 2, 2])
            with col1:
                leagues = ['All'] + sorted(df['league'].unique().tolist())
                selected_league = st.selectbox("Filter by League", leagues, key="fixture_league")
            with col2:
                search_team = st.text_input("Search Team", key="fixture_search")
            with col3:
                show_only_analyzed = st.checkbox("Show Only Analyzed", value=False, key="show_analyzed")
            
            # Apply filters
            filtered_df = df.copy()
            if selected_league != 'All':
                filtered_df = filtered_df[filtered_df['league'] == selected_league]
            if search_team:
                mask = (filtered_df['home'].str.contains(search_team, case=False, na=False) | 
                       filtered_df['away'].str.contains(search_team, case=False, na=False))
                filtered_df = filtered_df[mask]
            
            st.caption(f"Showing {len(filtered_df)} matches")
            
            # Add "Analyze All" button
            st.markdown("---")
            col1, col2, col3 = st.columns([2, 1, 1])
            with col1:
                st.subheader("🎯 Quick Analysis")
            with col2:
                analyze_all_btn = st.button("🔍 Analyze All", use_container_width=True, key="analyze_all")
            with col3:
                clear_analysis_btn = st.button("🗑️ Clear Analysis", use_container_width=True, key="clear_analysis")
            
            if clear_analysis_btn:
                st.session_state.analysis_results = {}
                st.rerun()
            
            if analyze_all_btn:
                with st.spinner("Analyzing matches..."):
                    for idx, row in filtered_df.iterrows():
                        match_key = f"{row.get('home', '')}_vs_{row.get('away', '')}"
                        analysis = analyze_match_value(row)
                        st.session_state.analysis_results[match_key] = analysis
                st.success(f"✅ Analyzed {len(filtered_df)} matches!")
            
            # Display matches with analysis option
            st.markdown("---")
            
            for idx, row in filtered_df.iterrows():
                home = row.get('home', 'Unknown')
                away = row.get('away', 'Unknown')
                league = row.get('league', 'Unknown')
                date_fmt = format_match_datetime(row.get('date'))
                match_key = f"{home}_vs_{away}"
                
                # Check if analyzed
                is_analyzed = match_key in st.session_state.analysis_results
                
                if show_only_analyzed and not is_analyzed:
                    continue
                
                # Build title with badges
                title_prefix = '✅' if is_analyzed else '⚽'
                
                # Create expandable card for each match
                with st.expander(
                    f"{title_prefix} {home} vs {away} | {league} | 📅 {date_fmt}",
                    expanded=is_analyzed
                ):
                    col1, col2 = st.columns([3, 1])
                    
                    with col1:
                        # Show probabilities
                        prob_cols = ['1x2_h', '1x2_d', '1x2_a', 'o_1.5', 'o_2.5', 'o_3.5', 'u_1.5', 'u_2.5', 'u_3.5']
                        available = [c for c in prob_cols if c in row.index and pd.notna(row.get(c))]
                        
                        if available:
                            prob_data = []
                            for col in available:
                                val = row.get(col, 0)
                                if pd.notna(val) and val is not None:
                                    label = col.replace('1x2_h', 'Home Win').replace('1x2_d', 'Draw').replace('1x2_a', 'Away Win')
                                    label = label.replace('o_1.5', 'Over 1.5').replace('o_2.5', 'Over 2.5').replace('o_3.5', 'Over 3.5')
                                    label = label.replace('u_1.5', 'Under 1.5').replace('u_2.5', 'Under 2.5').replace('u_3.5', 'Under 3.5')
                                    prob_data.append({'Market': label, 'Model Prob': f"{float(val)*100:.1f}%"})
                            
                            if prob_data:
                                st.dataframe(pd.DataFrame(prob_data), use_container_width=True, hide_index=True)
                        
                        # Check for odds from merged data
                        match_odds = None
                        for m in (st.session_state.get('merged_data', []) or []):
                            csv_home = m.get('csv_home', '')
                            csv_away = m.get('csv_away', '')
                            if (home.lower() in csv_home.lower() or csv_home.lower() in home.lower()) and \
                               (away.lower() in csv_away.lower() or csv_away.lower() in away.lower()):
                                match_odds = m.get('bookmaker_odds', {})
                                break
                        
                        if match_odds and any(v for k, v in match_odds.items() if k not in ['raw_odds', 'bookmaker'] and v):
                            st.markdown("**📊 Live Odds:**")
                            odds_data = []
                            if match_odds.get('home_win'):
                                odds_data.append({'Market': 'Home', 'Odds': f"{match_odds['home_win']:.2f}"})
                            if match_odds.get('draw'):
                                odds_data.append({'Market': 'Draw', 'Odds': f"{match_odds['draw']:.2f}"})
                            if match_odds.get('away_win'):
                                odds_data.append({'Market': 'Away', 'Odds': f"{match_odds['away_win']:.2f}"})
                            if match_odds.get('over_2.5'):
                                odds_data.append({'Market': 'O2.5', 'Odds': f"{match_odds['over_2.5']:.2f}"})
                            if match_odds.get('under_2.5'):
                                odds_data.append({'Market': 'U2.5', 'Odds': f"{match_odds['under_2.5']:.2f}"})
                            if odds_data:
                                st.dataframe(pd.DataFrame(odds_data), use_container_width=True, hide_index=True)
                                if match_odds.get('bookmaker'):
                                    st.caption(f"*via {match_odds['bookmaker']}*")
                    
                    with col2:
                        # Analyze button for individual match
                        if st.button(f"🔍 Analyze", key=f"analyze_{idx}", use_container_width=True):
                            analysis = analyze_match_value(row)
                            st.session_state.analysis_results[match_key] = analysis
                            st.rerun()
                    
                    # Show analysis if available
                    if is_analyzed:
                        analysis = st.session_state.analysis_results[match_key]
                        recommendations = analysis.get('recommendations', [])
                        
                        if recommendations:
                            st.markdown("**🎯 Recommendations:**")
                            for rec in recommendations:
                                conf_icon = '🟢' if rec['confidence'] == 'HIGH' else '🟡' if rec['confidence'] == 'MEDIUM' else '🔴'
                                st.markdown(
                                    f"- **{rec['pick']}** ({rec['type']}) - {rec['probability']*100:.1f}% {conf_icon} {rec['confidence']}"
                                )
                            
                            # Calculate Kelly stake
                            best_rec = recommendations[0]
                            kelly = get_kelly_criterion(best_rec['probability'], 1/best_rec['probability'])
                            st.info(f"💰 Suggested Stake: {kelly*100:.1f}% of bankroll")
                        else:
                            st.info("No strong recommendations for this match")
            
            st.markdown("---")
            
            # Raw data view
            with st.expander("📊 Raw Data View"):
                display_cols = ['home', 'away', 'league', 'date', '1x2_h', '1x2_d', '1x2_a', 'o_2.5', 'u_2.5']
                available_cols = [col for col in display_cols if col in filtered_df.columns]
                st.dataframe(filtered_df[available_cols], use_container_width=True, hide_index=True)
            
            # Export section for All Fixtures
            st.markdown("---")
            st.subheader("📥 Export All Fixtures")
            col1, col2, col3 = st.columns(3)
            with col1:
                fixtures_csv = filtered_df.to_csv(index=False)
                st.download_button(
                    label="📊 CSV Data",
                    data=fixtures_csv,
                    file_name=f"all_fixtures_{pd.Timestamp.now().strftime('%Y%m%d_%H%M')}.csv",
                    mime="text/csv",
                    use_container_width=True,
                    key="export_fixtures_csv"
                )
            with col2:
                fixtures_html = f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"><title>All Fixtures</title>
<style>body{{font-family:Arial;padding:20px}}table{{border-collapse:collapse;width:100%}}th,td{{border:1px solid #ddd;padding:8px;text-align:left}}th{{background:#FF9800;color:white}}tr:nth-child(even){{background:#f2f2f2}}</style></head>
<body><h1>📋 All Fixtures</h1><p>Generated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}</p><p>Total: {len(filtered_df)} matches</p>
{filtered_df[available_cols].to_html(index=False) if available_cols else filtered_df.to_html(index=False)}</body></html>"""
                st.download_button(
                    label="📄 HTML Report",
                    data=fixtures_html,
                    file_name=f"all_fixtures_{pd.Timestamp.now().strftime('%Y%m%d_%H%M')}.html",
                    mime="text/html",
                    use_container_width=True,
                    key="export_fixtures_html"
                )
            with col3:
                fixtures_heb_html = f"""<!DOCTYPE html>
<html dir="rtl" lang="he"><head><meta charset="UTF-8"><title>כל המשחקים</title>
<style>body{{font-family:Arial;padding:20px;direction:rtl}}table{{border-collapse:collapse;width:100%}}th,td{{border:1px solid #ddd;padding:8px;text-align:right}}th{{background:#FF9800;color:white}}tr:nth-child(even){{background:#f2f2f2}}</style></head>
<body><h1>📋 כל המשחקים</h1><p>נוצר: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}</p><p>סה"כ: {len(filtered_df)} משחקים</p>
{filtered_df[available_cols].to_html(index=False) if available_cols else filtered_df.to_html(index=False)}</body></html>"""
                st.download_button(
                    label="🇮🇱 Hebrew HTML",
                    data=fixtures_heb_html,
                    file_name=f"all_fixtures_heb_{pd.Timestamp.now().strftime('%Y%m%d_%H%M')}.html",
                    mime="text/html",
                    use_container_width=True,
                    key="export_fixtures_heb"
                )
        
        # ============== TAB 8: LIVE API DATA ==============
        with tab8:
            st.header("🌐 Live API-Football Data")
            
            if API_MODULES_AVAILABLE:
                if st.session_state.api_fixtures:
                    fixtures = st.session_state.api_fixtures
                    
                    st.success(f"📡 {len(fixtures)} live fixtures loaded")
                    
                    # Summary metrics
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Total Fixtures", len(fixtures))
                    with col2:
                        leagues_count = len(set(f.get('league', {}).get('name', 'Unknown') for f in fixtures))
                        st.metric("Leagues", leagues_count)
                    with col3:
                        merged_list = st.session_state.get('merged_data', []) or []
                        with_api = sum(1 for m in merged_list if m.get('has_api_data'))
                        st.metric("Matched", with_api)
                    with col4:
                        with_odds = sum(1 for m in merged_list if m.get('bookmaker_odds', {}).get('home_win'))
                        st.metric("With Odds", with_odds)
                    
                    # Debug expander
                    with st.expander("🔧 Debug Info", expanded=False):
                        st.write("**Sample fixture structure (first fixture):**")
                        if fixtures:
                            sample = fixtures[0]
                            st.json({
                                'keys': list(sample.keys()),
                                'fixture_keys': list(sample.get('fixture', {}).keys()) if isinstance(sample.get('fixture'), dict) else 'N/A',
                                'has_teams_at_root': 'teams' in sample,
                                'has_teams_in_fixture': 'teams' in sample.get('fixture', {}),
                                'home_team': sample.get('teams', {}).get('home', {}).get('name', 'NOT FOUND'),
                                'away_team': sample.get('teams', {}).get('away', {}).get('name', 'NOT FOUND'),
                            })
                        
                        merged_list = st.session_state.get('merged_data', []) or []
                        if merged_list:
                            st.write("**Sample merged record (first with API data):**")
                            for m in merged_list:
                                if m.get('has_api_data'):
                                    odds_info = m.get('bookmaker_odds', {})
                                    raw_odds = m.get('api_fixture', {}).get('odds', [])
                                    st.json({
                                        'csv_home': m.get('csv_home'),
                                        'csv_away': m.get('csv_away'),
                                        'api_home': m.get('api_home'),
                                        'api_away': m.get('api_away'),
                                        'fixture_id': m.get('fixture_id'),
                                        'has_extracted_odds': bool(odds_info.get('home_win')),
                                        'raw_odds_count': len(raw_odds) if raw_odds else 0,
                                        'odds_keys': list(odds_info.keys()) if odds_info else [],
                                        'bookmaker': odds_info.get('bookmaker'),
                                        'raw_odds_preview': raw_odds[:1] if raw_odds else 'No raw odds',
                                    })
                                    break
                        
                        # Test odds API directly
                        st.markdown("---")
                        st.write("**Test Odds API:**")
                        if st.button("🎰 Test Odds Fetch"):
                            try:
                                test_api = APIFootball("8333df5e3877e41485704e1c3ad026e6")
                                # Get first fixture ID
                                if fixtures:
                                    test_fix_id = fixtures[0].get('fixture', {}).get('id')
                                    if test_fix_id:
                                        odds_result = test_api.get_odds(fixture=test_fix_id)
                                        st.json({
                                            'fixture_id': test_fix_id,
                                            'response_keys': list(odds_result.keys()) if odds_result else [],
                                            'response_count': len(odds_result.get('response', [])),
                                            'errors': odds_result.get('errors', {}),
                                            'sample': odds_result.get('response', [])[:1] if odds_result.get('response') else 'No odds',
                                        })
                            except Exception as e:
                                st.error(f"Error testing odds: {e}")
                    
                    st.markdown("---")
                    
                    # Group by league
                    leagues_data = {}
                    for fix in fixtures:
                        league_name = fix.get('league', {}).get('name', 'Unknown')
                        if league_name not in leagues_data:
                            leagues_data[league_name] = []
                        leagues_data[league_name].append(fix)
                    
                    # Display by league
                    for league_name, league_fixtures in sorted(leagues_data.items()):
                        with st.expander(f"🏆 {league_name} ({len(league_fixtures)} matches)", expanded=False):
                            for fix in league_fixtures:
                                home = fix.get('teams', {}).get('home', {}).get('name', 'Unknown')
                                away = fix.get('teams', {}).get('away', {}).get('name', 'Unknown')
                                fixture_info = fix.get('fixture', {})
                                date = fixture_info.get('date', 'TBD')
                                fixture_id = fixture_info.get('id')
                                
                                # Parse date
                                if isinstance(date, str) and 'T' in date:
                                    try:
                                        dt = datetime.fromisoformat(date.replace('Z', '+00:00'))
                                        date_fmt = dt.strftime('%a %d %b, %H:%M')
                                    except:
                                        date_fmt = date
                                else:
                                    date_fmt = str(date)
                                
                                # Match card
                                st.markdown(f"### ⚽ {home} vs {away}")
                                st.caption(f"📅 {date_fmt} | ID: {fixture_id}")
                                
                                # Check multiple sources for odds: 1) cached fixture odds, 2) merged data
                                odds = None
                                odds_source = None
                                
                                # First check if we have cached odds for this fixture
                                cached_fixture_odds = st.session_state.get('fixture_odds_cache', {})
                                if fixture_id and str(fixture_id) in cached_fixture_odds:
                                    odds = cached_fixture_odds[str(fixture_id)]
                                    odds_source = 'Live API'
                                
                                # Fall back to merged data
                                if not odds:
                                    for m in (st.session_state.get('merged_data', []) or []):
                                        if m.get('fixture_id') == fixture_id:
                                            if m.get('bookmaker_odds'):
                                                odds = m.get('bookmaker_odds', {})
                                                odds_source = odds.get('bookmaker', 'Merged Data')
                                            break
                                
                                if odds and any(v for k, v in odds.items() if k not in ['raw_odds', 'bookmaker'] and v):
                                    col1, col2, col3 = st.columns(3)
                                    with col1:
                                        st.markdown("**1X2 Odds:**")
                                        if odds.get('home_win'):
                                            st.markdown(f"- Home: **{odds['home_win']:.2f}**")
                                        if odds.get('draw'):
                                            st.markdown(f"- Draw: **{odds['draw']:.2f}**")
                                        if odds.get('away_win'):
                                            st.markdown(f"- Away: **{odds['away_win']:.2f}**")
                                    
                                    with col2:
                                        st.markdown("**Goals Odds:**")
                                        if odds.get('over_1.5'):
                                            st.markdown(f"- O1.5: **{odds['over_1.5']:.2f}**")
                                        if odds.get('over_2.5'):
                                            st.markdown(f"- O2.5: **{odds['over_2.5']:.2f}**")
                                        if odds.get('over_3.5'):
                                            st.markdown(f"- O3.5: **{odds['over_3.5']:.2f}**")
                                    
                                    with col3:
                                        st.markdown("**Under/BTTS:**")
                                        if odds.get('under_2.5'):
                                            st.markdown(f"- U2.5: **{odds['under_2.5']:.2f}**")
                                        if odds.get('btts_yes'):
                                            st.markdown(f"- BTTS Y: **{odds['btts_yes']:.2f}**")
                                        if odds.get('btts_no'):
                                            st.markdown(f"- BTTS N: **{odds['btts_no']:.2f}**")
                                    
                                    st.caption(f"*Source: {odds_source}*")
                                else:
                                    # Show button to fetch odds for this fixture
                                    if st.button(f"🎰 Fetch Odds", key=f"fetch_odds_{fixture_id}"):
                                        try:
                                            odds_api = APIFootball("8333df5e3877e41485704e1c3ad026e6")
                                            full_data = odds_api.get_full_match_data(fixture_id)
                                            if full_data and full_data.get('odds'):
                                                # Extract and parse odds
                                                extracted_odds = extract_odds_from_api(full_data.get('odds', []))
                                                if extracted_odds:
                                                    # Cache the odds
                                                    if 'fixture_odds_cache' not in st.session_state:
                                                        st.session_state.fixture_odds_cache = {}
                                                    st.session_state.fixture_odds_cache[str(fixture_id)] = extracted_odds
                                                    st.rerun()
                                                else:
                                                    st.warning("No odds available for this match")
                                            else:
                                                st.warning("No odds data returned from API")
                                        except Exception as e:
                                            st.error(f"Error fetching odds: {e}")
                                
                                st.markdown("---")
                else:
                    st.info("👆 Click '🔄 Fetch Live Data' in the sidebar to load fixtures")
                    
                    st.markdown("""
                    **API-Football provides:**
                    - Live fixtures for Winner-supported leagues
                    - Real-time bookmaker odds
                    - Injury reports
                    - Head-to-head statistics
                    - Team lineups (when available)
                    """)
                
                # Export section for Live API Data
                st.markdown("---")
                st.subheader("📥 Export Live API Data")
                if st.session_state.api_fixtures:
                    # Prepare API fixtures for export
                    api_export_data = []
                    for fix in st.session_state.api_fixtures:
                        home = fix.get('teams', {}).get('home', {}).get('name', 'Unknown')
                        away = fix.get('teams', {}).get('away', {}).get('name', 'Unknown')
                        league_name = fix.get('league', {}).get('name', 'Unknown')
                        date = fix.get('fixture', {}).get('date', '')
                        fixture_id = fix.get('fixture', {}).get('id', '')
                        api_export_data.append({
                            'fixture_id': fixture_id,
                            'home': home,
                            'away': away,
                            'league': league_name,
                            'date': date
                        })
                    
                    api_df = pd.DataFrame(api_export_data)
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.download_button(
                            label="📊 CSV Data",
                            data=api_df.to_csv(index=False),
                            file_name=f"api_fixtures_{pd.Timestamp.now().strftime('%Y%m%d_%H%M')}.csv",
                            mime="text/csv",
                            use_container_width=True,
                            key="export_api_csv"
                        )
                    with col2:
                        api_html = f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"><title>API Fixtures</title>
<style>body{{font-family:Arial;padding:20px}}table{{border-collapse:collapse;width:100%}}th,td{{border:1px solid #ddd;padding:8px;text-align:left}}th{{background:#9C27B0;color:white}}tr:nth-child(even){{background:#f2f2f2}}</style></head>
<body><h1>🌐 Live API Fixtures</h1><p>Generated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}</p><p>Total: {len(api_df)} fixtures</p>
{api_df.to_html(index=False)}</body></html>"""
                        st.download_button(
                            label="📄 HTML Report",
                            data=api_html,
                            file_name=f"api_fixtures_{pd.Timestamp.now().strftime('%Y%m%d_%H%M')}.html",
                            mime="text/html",
                            use_container_width=True,
                            key="export_api_html"
                        )
                    with col3:
                        api_heb_html = f"""<!DOCTYPE html>
<html dir="rtl" lang="he"><head><meta charset="UTF-8"><title>משחקים מה-API</title>
<style>body{{font-family:Arial;padding:20px;direction:rtl}}table{{border-collapse:collapse;width:100%}}th,td{{border:1px solid #ddd;padding:8px;text-align:right}}th{{background:#9C27B0;color:white}}tr:nth-child(even){{background:#f2f2f2}}</style></head>
<body><h1>🌐 משחקים מה-API</h1><p>נוצר: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}</p><p>סה"כ: {len(api_df)} משחקים</p>
{api_df.to_html(index=False)}</body></html>"""
                        st.download_button(
                            label="🇮🇱 Hebrew HTML",
                            data=api_heb_html,
                            file_name=f"api_fixtures_heb_{pd.Timestamp.now().strftime('%Y%m%d_%H%M')}.html",
                            mime="text/html",
                            use_container_width=True,
                            key="export_api_heb"
                        )
            else:
                st.error("API modules not available. Check installation.")
        
        # ============== TAB 9: AI ANALYSIS ==============
        with tab9:
            st.header("🤖 AI-Powered Analysis")
            
            if "openai_key" in st.session_state:
                # Initialize DuckDbTools
                duckdb_tools = DuckDbTools()
                
                # Load the CSV file into DuckDB as a table
                duckdb_tools.load_local_csv_to_table(
                    path=temp_path,
                    table="predictions",
                )
                
                # Betting-focused system prompt with strict behavioral rules
                today_str = datetime.now().strftime('%Y-%m-%d')
                betting_system_prompt = f"""
============================================================
🎯 CORE IDENTITY
============================================================
You are the CORE ANALYTICAL BRAIN of an elite sports betting intelligence engine.
You are NOT a normal chatbot. You are a professional betting analyst AI.

TODAY'S DATE: {today_str}

============================================================
🎯 HOW YOU MUST BEHAVE
============================================================

1. ALWAYS use the MERGED MATCH MODEL.
   - Do NOT analyze CSV-only data in isolation.
   - Do NOT analyze API-only data in isolation.
   - ALWAYS assume merged_data contains:
     • Model probabilities (from CSV: 1x2_h, 1x2_d, 1x2_a, o_X, u_X)
     • Bookmaker odds (from API-Football)
     • Implied probabilities (calculated from odds)
     • Injuries (from API-Football)
     • Team form (recent results)
     • H2H history (last 10 meetings)
     • League & team statistics
     • Value detection status
     • Match date/time

2. ALWAYS reference these in your analysis:
   - Match date & time
   - League name
   - Teams involved
   - Probability values (model probability AND implied probability from odds)
   - Injury impact (list missing players if known)
   - H2H influence (who historically dominates)
   - League scoring profile (high/low scoring league)
   - Value bet status (YES/NO)
   - Risk rating (🟢 LOW / 🟡 MEDIUM / 🔴 HIGH)
   - Recommended stake % (Kelly Criterion)

3. ALWAYS apply this internal logic:
   - VALUE BET = model_prob > implied_prob + 5% edge
   - INJURY ADJUSTMENTS:
     • 1 key player out: -3% to -5%
     • 2-3 players out: -8% to -12%
     • 4+ players out: -15% to -20%
   - FORM ADJUSTMENTS:
     • Hot streak (3+ wins): +3% to +5%
     • Cold streak (3+ losses): -3% to -5%
     • Mixed form: +0%
   - H2H ADJUSTMENTS:
     • Dominant H2H (70%+ wins in last 10): +5% to +8%
     • Slight edge (50-70%): +3% to +5%
     • Even H2H: +0%
   - KELLY CRITERION for stake sizing:
     • Kelly % = (edge × probability) / (1 - probability)
     • Use fractional Kelly (25% of full Kelly) for safety
     • Cap stakes at 5% max
   - ONLY Winner/Toto supported leagues allowed
   - RANKING priority: Value → Probability → Date (soonest first)

============================================================
📋 OUTPUT FORMAT (MANDATORY)
============================================================
Every match output MUST include ALL of these:

📅 **[Day] [Date], [Time]**
⚽ **[Home Team] vs [Away Team]**
🏆 [League Name]

**Model Probabilities:**
- Home Win: XX.X%
- Draw: XX.X%
- Away Win: XX.X%
- Over 2.5: XX.X%

**Bookmaker Odds & Implied Prob:**
- Home: X.XX (implied: XX.X%)
- Draw: X.XX (implied: XX.X%)
- Away: X.XX (implied: XX.X%)

**🎯 RECOMMENDED BET: [Pick]**
- Model Probability: XX.X%
- Implied Probability: XX.X%
- Edge: +X.X%
- 🔥 VALUE BET: YES/NO

**📉 Injury Report:**
- [Home Team]: [Player1, Player2] OR "No major injuries"
- [Away Team]: [Player1, Player2] OR "No major injuries"
- Injury Impact: -X% adjustment applied

**📈 Form & H2H:**
- [Home Team] form: [W/D/L last 5]
- [Away Team] form: [W/D/L last 5]
- H2H (last 10): [Home wins]-[Draws]-[Away wins]
- H2H Boost: +X% applied

**Risk & Stake:**
- 🟢/🟡/🔴 Risk Rating: [LOW/MEDIUM/HIGH]
- 💰 Suggested Stake: X.X% of bankroll (Kelly)

---

============================================================
📌 QUERY HANDLING RULES
============================================================

When user asks:
- "Best bets today" → Filter by today's date ({today_str}), show top 5-10 value bets
- "Best bets tomorrow" → Filter by tomorrow's date
- "This weekend" → Saturday and Sunday matches only
- "Low risk bets" → Only show bets with probability >75%
- "Medium risk" → 60-75% probability range
- "High risk / longshots" → 40-60% probability (warn about risk)
- "Build me an accumulator" → Choose 3-6 strongest value legs, calculate combined probability
- "Compare [Team A] vs [Team B]" → Use Team Stats to compare attacking/defensive ratings
- "League analysis [League]" → Show league patterns, avg goals, home/away trends
- "Analyze [Team A] vs [Team B]" → Full merged match analysis with all data points
- "Over/Under picks" → Focus on goals markets with value
- "Home favorites" → High home win probability matches
- "Away upsets" → Away teams with decent probability but good odds

If user asks for something impossible or not in data:
- Respond: "⚠️ This information is not available in the merged data."

============================================================
📌 SUMMARY OF YOUR JOB
============================================================

You produce:
✅ Best value bets (ranked by edge)
✅ Risk ratings for every pick
✅ Accumulator suggestions (multiple legs)
✅ Deep match analysis (injuries, form, H2H)
✅ Team & league insights
✅ Custom queries the user asks

Using ALL data sources merged together:
📊 CSV Predictions + 🌐 API-Football + 📈 Internal Statistics

============================================================
⚠️ CRITICAL RULES - NEVER BREAK THESE
============================================================

1. NEVER produce output without merged, unified match data.
2. NEVER skip the full output format for match analysis.
3. NEVER recommend bets without showing the edge/value calculation.
4. NEVER ignore injuries or H2H when available.
5. NEVER exceed 5% stake recommendation.
6. NEVER include non-Winner/Toto leagues.
7. ALWAYS be specific with numbers and percentages.
8. ALWAYS show your reasoning.

============================================================
DATABASE ACCESS
============================================================

The 'predictions' table contains:
- id, home, away, league, date: Match identifiers
- 1x2_h, 1x2_d, 1x2_a: Home/Draw/Away win probabilities (0-1)
- o_0.5 to o_4: Over X goals probabilities
- u_0.5 to u_4: Under X goals probabilities
- ah_*: Asian handicap probabilities

Use SQL queries on this table to analyze the data.
"""

                # Initialize the Agent with betting focus
                betting_agent = Agent(
                    model=OpenAIChat(id="gpt-4o", api_key=st.session_state.openai_key),
                    tools=[duckdb_tools, PandasTools()],
                    system_message=betting_system_prompt,
                    markdown=True,
                )
                
                # Quick analysis buttons - Row 1: Date-Based
                st.subheader("⏰ Time-Based Analysis")
                
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    if st.button("📅 Today's Best", use_container_width=True):
                        st.session_state.ai_query = f"Find the best betting picks for TODAY ({today_str}). Show date/time, match, league, bet type, and probability. Order by match time. Include at least 5 picks with 60%+ probability."
                with col2:
                    if st.button("📆 Next 2 Days", use_container_width=True):
                        st.session_state.ai_query = f"Find the TOP 10 best bets for the next 2 days (from {today_str}). Include date and kickoff time for each match. Sort by date first, then by probability. Focus on high-confidence picks."
                with col3:
                    if st.button("🗓️ Weekend Special", use_container_width=True):
                        st.session_state.ai_query = f"Create a weekend betting preview. Find matches on Saturday and Sunday, group by day. Show best bets for each day with dates, times, and probabilities. Suggest a weekend accumulator."
                with col4:
                    if st.button("🎯 Top 5 Upcoming", use_container_width=True):
                        st.session_state.ai_query = f"Show the TOP 5 best betting opportunities coming up, sorted by date. For each pick show: date, time, teams, league, bet type, probability, and suggested stake."
                
                # Row 2: Risk Levels
                st.subheader("⚠️ Risk-Based Analysis")
                
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    if st.button("🟢 LOW RISK (75%+)", use_container_width=True):
                        st.session_state.ai_query = "Find all bets with probability above 75%. These are the safest picks. Show date, match, bet type, probability. Recommend 3-5% stake per bet. Sort by date."
                with col2:
                    if st.button("🟡 MEDIUM RISK (60-75%)", use_container_width=True):
                        st.session_state.ai_query = "Find all bets with probability between 60% and 75%. Show date/time, match, bet type, probability. Recommend 2-3% stake per bet."
                with col3:
                    if st.button("🟠 HIGH RISK (45-60%)", use_container_width=True):
                        st.session_state.ai_query = "Find bets with probability between 45% and 60%. Higher risk but better odds. Show top 10 with dates. Suggest 1-2% stake max."
                with col4:
                    if st.button("🔴 VERY HIGH RISK", use_container_width=True):
                        st.session_state.ai_query = "Find potential upset bets where away team or draw has 30-45% probability. Show top 5 with dates. Maximum 1% stake warning."
                
                # Row 3: Market-specific analysis
                st.subheader("📈 Market Analysis")
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    if st.button("🔥 Safest Bets", use_container_width=True):
                        st.session_state.ai_query = "Find the top 10 safest bets with highest probabilities. Show date/time, match, bet type, probability, and league. Focus on home wins and over/under markets."
                
                with col2:
                    if st.button("⚽ Over 2.5 Goals", use_container_width=True):
                        st.session_state.ai_query = "Find all matches where over 2.5 goals probability is above 60%. Show date/time for each match. Sort by probability descending."
                
                with col3:
                    if st.button("🏠 Strong Home Teams", use_container_width=True):
                        st.session_state.ai_query = "Find matches where home team has >70% win probability. Show date, time, teams, and league. These are strong home favorites."
                
                with col4:
                    if st.button("🎯 Value Accumulators", use_container_width=True):
                        st.session_state.ai_query = "Suggest a 4-fold accumulator with each leg having at least 65% probability. Show date/time for each leg. Calculate combined probability."
                
                # Row 4: Special analysis
                st.subheader("🎲 Special Analysis")
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    if st.button("🎰 BTTS Likely", use_container_width=True):
                        st.session_state.ai_query = "Find matches where both Over 1.5 goals AND home/away win probability is high. Show date/time. These indicate both teams likely to score."
                
                with col2:
                    if st.button("🔒 Under 2.5 Safe", use_container_width=True):
                        st.session_state.ai_query = "Find matches with Under 2.5 goals probability above 65%. Show date/time. These are likely low-scoring games."
                
                with col3:
                    if st.button("⚔️ Close Matches", use_container_width=True):
                        st.session_state.ai_query = "Find matches where home and away probabilities are within 10% of each other. Show dates. Good for draw bets or avoiding."
                
                with col4:
                    if st.button("🌟 Best Picks Slip", use_container_width=True):
                        st.session_state.ai_query = f"Create a professional betting slip with TOP 5 best bets. For each: date/time, teams, league, pick, probability, stake %. Include a SAFE accumulator suggestion."
                
                st.markdown("---")
                
                # Custom query input
                st.subheader("💬 Ask AI Analyst")
                
                default_query = st.session_state.get('ai_query', '')
                user_query = st.text_area(
                    "Ask anything about the predictions:",
                    value=default_query,
                    placeholder="e.g., 'Which matches have the best value for over 2.5 goals?' or 'Find all German Bundesliga matches and recommend best bets'"
                )
                
                if st.button("🚀 Analyze", type="primary"):
                    if user_query.strip() == "":
                        st.warning("Please enter a query.")
                    else:
                        try:
                            with st.spinner('🔍 AI is analyzing predictions...'):
                                response = betting_agent.run(user_query)
                                
                                if hasattr(response, 'content'):
                                    response_content = response.content
                                else:
                                    response_content = str(response)
                            
                            st.markdown("### 📊 Analysis Results")
                            st.markdown(response_content)
                            
                            # Clear the quick analysis query after use
                            if 'ai_query' in st.session_state:
                                del st.session_state.ai_query
                                
                        except Exception as e:
                            st.error(f"Error: {e}")
                            st.info("Try rephrasing your query or check your API key.")
            else:
                st.warning("⚠️ Please enter your OpenAI API key in the sidebar to use AI analysis.")
                st.info("You can still use the other tabs for basic analysis without an API key!")
        
        # ============== TAB 10: BASKETBALL (Under Maintenance) ==============
        with tab10:
            st.header("🏀 Basketball Analytics")
            
            # Render the maintenance page
            st.markdown(render_maintenance_page("Basketball", "🏀"), unsafe_allow_html=True)
            
            # Additional info section
            st.markdown("---")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("""
                ### 🎯 What's Coming
                
                Our basketball analytics module will include:
                
                - **NBA & International Leagues** - Coverage of major basketball competitions
                - **Point Spread Analysis** - AI-powered spread predictions
                - **Over/Under Totals** - Smart totals forecasting
                - **Player Props** - Individual player performance predictions
                - **Live Game Analysis** - Real-time adjustments and insights
                """)
            
            with col2:
                st.markdown("""
                ### 📊 Planned Features
                
                - Team performance hexagon charts
                - Historical H2H records
                - Injury impact analysis
                - Rest days & schedule analysis
                - Pace & efficiency metrics
                - AI-powered game predictions
                """)
            
            # Show cache statistics if available
            if TEAM_CACHE_AVAILABLE:
                st.markdown("---")
                st.markdown("### 💾 System Cache Status")
                cache = get_cache()
                if cache:
                    cache_stats = cache.get_cache_stats()
                    st.markdown(render_cache_stats_card(cache_stats), unsafe_allow_html=True)
                    
                    if st.button("🗑️ Clear Cache", help="Clear all cached team history data"):
                        cache.clear_cache()
                        st.success("Cache cleared successfully!")
                        st.rerun()

else:
    # Welcome screen when no file is uploaded
    st.markdown("""
    ## 👋 Welcome to AI Sports Betting Analyst!
    
    Upload your predictions CSV file to get started. The file should contain:
    
    | Column | Description |
    |--------|-------------|
    | `home`, `away` | Team names |
    | `league` | Competition name |
    | `date` | Match date |
    | `1x2_h`, `1x2_d`, `1x2_a` | Win/Draw/Loss probabilities |
    | `o_X`, `u_X` | Over/Under X goals probabilities |
    
    ### 🎯 Features:
    - **Best Bets**: Find highest probability betting opportunities
    - **Accumulators**: AI-generated accumulator suggestions
    - **League Stats**: Analyze patterns by competition
    - **AI Analysis**: Ask questions in natural language
    - **🏀 Basketball** (Coming Soon!)
    
    ---
    *Upload a file to begin!*
    """)