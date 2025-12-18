"""
Team History Cache Module
=========================
SQLite-based caching for team statistics, match history, and H2H records.
Stores data in temp directory for persistence across sessions.
Caches last 10 matches per team for form analysis.
"""

import sqlite3
import json
import os
import tempfile
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import hashlib


class TeamHistoryCache:
    """
    Cache for team historical data including:
    - Match results (last 10 games)
    - Team performance stats
    - Head-to-head records
    - Form trends
    """
    
    CACHE_EXPIRY_HOURS = 24  # Re-fetch data after 24 hours
    MAX_MATCHES_PER_TEAM = 10
    
    def __init__(self, db_name: str = "team_history_cache.db"):
        """Initialize the cache with SQLite database in temp directory"""
        self.temp_dir = tempfile.gettempdir()
        self.db_path = os.path.join(self.temp_dir, db_name)
        self._init_database()
    
    def _init_database(self):
        """Create database tables if they don't exist"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Teams table - basic team info and aggregated stats
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS teams (
                team_id INTEGER PRIMARY KEY,
                team_name TEXT NOT NULL,
                league_id INTEGER,
                league_name TEXT,
                country TEXT,
                logo_url TEXT,
                last_updated TIMESTAMP,
                -- Aggregated stats
                total_matches INTEGER DEFAULT 0,
                wins INTEGER DEFAULT 0,
                draws INTEGER DEFAULT 0,
                losses INTEGER DEFAULT 0,
                goals_for INTEGER DEFAULT 0,
                goals_against INTEGER DEFAULT 0,
                home_wins INTEGER DEFAULT 0,
                home_draws INTEGER DEFAULT 0,
                home_losses INTEGER DEFAULT 0,
                away_wins INTEGER DEFAULT 0,
                away_draws INTEGER DEFAULT 0,
                away_losses INTEGER DEFAULT 0,
                clean_sheets INTEGER DEFAULT 0,
                failed_to_score INTEGER DEFAULT 0,
                -- Calculated ratings
                attack_rating REAL DEFAULT 50.0,
                defense_rating REAL DEFAULT 50.0,
                form_rating REAL DEFAULT 50.0,
                home_rating REAL DEFAULT 50.0,
                away_rating REAL DEFAULT 50.0,
                consistency_rating REAL DEFAULT 50.0,
                overall_rating REAL DEFAULT 50.0
            )
        ''')
        
        # Matches table - individual match records
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS matches (
                match_id INTEGER PRIMARY KEY,
                fixture_id INTEGER UNIQUE,
                home_team_id INTEGER,
                away_team_id INTEGER,
                home_team_name TEXT,
                away_team_name TEXT,
                league_id INTEGER,
                league_name TEXT,
                match_date DATE,
                match_time TEXT,
                home_goals INTEGER,
                away_goals INTEGER,
                home_xg REAL,
                away_xg REAL,
                home_shots INTEGER,
                away_shots INTEGER,
                home_shots_on_target INTEGER,
                away_shots_on_target INTEGER,
                home_possession REAL,
                away_possession REAL,
                home_corners INTEGER,
                away_corners INTEGER,
                status TEXT,
                venue TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (home_team_id) REFERENCES teams(team_id),
                FOREIGN KEY (away_team_id) REFERENCES teams(team_id)
            )
        ''')
        
        # H2H records table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS h2h_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                team1_id INTEGER,
                team2_id INTEGER,
                team1_name TEXT,
                team2_name TEXT,
                total_matches INTEGER DEFAULT 0,
                team1_wins INTEGER DEFAULT 0,
                draws INTEGER DEFAULT 0,
                team2_wins INTEGER DEFAULT 0,
                team1_goals INTEGER DEFAULT 0,
                team2_goals INTEGER DEFAULT 0,
                last_matches_json TEXT,
                last_updated TIMESTAMP,
                UNIQUE(team1_id, team2_id)
            )
        ''')
        
        # Team form table - rolling form data
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS team_form (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                team_id INTEGER,
                team_name TEXT,
                form_string TEXT,  -- e.g., "WWDLW"
                last_5_points INTEGER,
                last_5_goals_for INTEGER,
                last_5_goals_against INTEGER,
                last_5_clean_sheets INTEGER,
                trend TEXT,  -- "UP", "DOWN", "STABLE"
                trend_strength REAL,  -- 0-100
                last_updated TIMESTAMP,
                UNIQUE(team_id)
            )
        ''')
        
        # Create indexes for faster queries
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_matches_home ON matches(home_team_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_matches_away ON matches(away_team_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_matches_date ON matches(match_date)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_teams_name ON teams(team_name)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_h2h ON h2h_records(team1_id, team2_id)')
        
        conn.commit()
        conn.close()
    
    def _get_connection(self) -> sqlite3.Connection:
        """Get database connection"""
        return sqlite3.connect(self.db_path)
    
    def _is_cache_valid(self, last_updated: Optional[str]) -> bool:
        """Check if cached data is still valid"""
        if not last_updated:
            return False
        try:
            updated_time = datetime.fromisoformat(last_updated)
            return datetime.now() - updated_time < timedelta(hours=self.CACHE_EXPIRY_HOURS)
        except:
            return False
    
    # ============== TEAM OPERATIONS ==============
    
    def get_team(self, team_id: int = None, team_name: str = None) -> Optional[Dict]:
        """Get team data from cache"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        if team_id:
            cursor.execute('SELECT * FROM teams WHERE team_id = ?', (team_id,))
        elif team_name:
            cursor.execute('SELECT * FROM teams WHERE team_name LIKE ?', (f'%{team_name}%',))
        else:
            conn.close()
            return None
        
        row = cursor.fetchone()
        conn.close()
        
        if row:
            columns = [desc[0] for desc in cursor.description]
            return dict(zip(columns, row))
        return None
    
    def save_team(self, team_data: Dict) -> bool:
        """Save or update team data in cache"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO teams (
                    team_id, team_name, league_id, league_name, country, logo_url,
                    last_updated, total_matches, wins, draws, losses,
                    goals_for, goals_against, home_wins, home_draws, home_losses,
                    away_wins, away_draws, away_losses, clean_sheets, failed_to_score,
                    attack_rating, defense_rating, form_rating, home_rating,
                    away_rating, consistency_rating, overall_rating
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                team_data.get('team_id'),
                team_data.get('team_name'),
                team_data.get('league_id'),
                team_data.get('league_name'),
                team_data.get('country'),
                team_data.get('logo_url'),
                datetime.now().isoformat(),
                team_data.get('total_matches', 0),
                team_data.get('wins', 0),
                team_data.get('draws', 0),
                team_data.get('losses', 0),
                team_data.get('goals_for', 0),
                team_data.get('goals_against', 0),
                team_data.get('home_wins', 0),
                team_data.get('home_draws', 0),
                team_data.get('home_losses', 0),
                team_data.get('away_wins', 0),
                team_data.get('away_draws', 0),
                team_data.get('away_losses', 0),
                team_data.get('clean_sheets', 0),
                team_data.get('failed_to_score', 0),
                team_data.get('attack_rating', 50.0),
                team_data.get('defense_rating', 50.0),
                team_data.get('form_rating', 50.0),
                team_data.get('home_rating', 50.0),
                team_data.get('away_rating', 50.0),
                team_data.get('consistency_rating', 50.0),
                team_data.get('overall_rating', 50.0)
            ))
            conn.commit()
            return True
        except Exception as e:
            print(f"Error saving team: {e}")
            return False
        finally:
            conn.close()
    
    # ============== MATCH OPERATIONS ==============
    
    def get_team_matches(self, team_id: int = None, team_name: str = None, limit: int = 10) -> List[Dict]:
        """Get recent matches for a team"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        if team_id:
            cursor.execute('''
                SELECT * FROM matches 
                WHERE home_team_id = ? OR away_team_id = ?
                ORDER BY match_date DESC
                LIMIT ?
            ''', (team_id, team_id, limit))
        elif team_name:
            cursor.execute('''
                SELECT * FROM matches 
                WHERE home_team_name LIKE ? OR away_team_name LIKE ?
                ORDER BY match_date DESC
                LIMIT ?
            ''', (f'%{team_name}%', f'%{team_name}%', limit))
        else:
            conn.close()
            return []
        
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        conn.close()
        
        return [dict(zip(columns, row)) for row in rows]
    
    def save_match(self, match_data: Dict) -> bool:
        """Save a match result to cache"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO matches (
                    fixture_id, home_team_id, away_team_id, home_team_name, away_team_name,
                    league_id, league_name, match_date, match_time,
                    home_goals, away_goals, home_xg, away_xg,
                    home_shots, away_shots, home_shots_on_target, away_shots_on_target,
                    home_possession, away_possession, home_corners, away_corners,
                    status, venue
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                match_data.get('fixture_id'),
                match_data.get('home_team_id'),
                match_data.get('away_team_id'),
                match_data.get('home_team_name'),
                match_data.get('away_team_name'),
                match_data.get('league_id'),
                match_data.get('league_name'),
                match_data.get('match_date'),
                match_data.get('match_time'),
                match_data.get('home_goals'),
                match_data.get('away_goals'),
                match_data.get('home_xg'),
                match_data.get('away_xg'),
                match_data.get('home_shots'),
                match_data.get('away_shots'),
                match_data.get('home_shots_on_target'),
                match_data.get('away_shots_on_target'),
                match_data.get('home_possession'),
                match_data.get('away_possession'),
                match_data.get('home_corners'),
                match_data.get('away_corners'),
                match_data.get('status'),
                match_data.get('venue')
            ))
            conn.commit()
            return True
        except Exception as e:
            print(f"Error saving match: {e}")
            return False
        finally:
            conn.close()
    
    def save_matches_bulk(self, matches: List[Dict]) -> int:
        """Save multiple matches at once"""
        saved = 0
        for match in matches:
            if self.save_match(match):
                saved += 1
        return saved
    
    # ============== H2H OPERATIONS ==============
    
    def get_h2h(self, team1_id: int = None, team2_id: int = None, 
                team1_name: str = None, team2_name: str = None) -> Optional[Dict]:
        """Get head-to-head record between two teams"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        if team1_id and team2_id:
            # Order IDs for consistent storage
            id1, id2 = min(team1_id, team2_id), max(team1_id, team2_id)
            cursor.execute('''
                SELECT * FROM h2h_records 
                WHERE team1_id = ? AND team2_id = ?
            ''', (id1, id2))
        elif team1_name and team2_name:
            cursor.execute('''
                SELECT * FROM h2h_records 
                WHERE (team1_name LIKE ? AND team2_name LIKE ?)
                   OR (team1_name LIKE ? AND team2_name LIKE ?)
            ''', (f'%{team1_name}%', f'%{team2_name}%', f'%{team2_name}%', f'%{team1_name}%'))
        else:
            conn.close()
            return None
        
        row = cursor.fetchone()
        conn.close()
        
        if row:
            columns = [desc[0] for desc in cursor.description]
            h2h = dict(zip(columns, row))
            # Parse JSON for last matches
            if h2h.get('last_matches_json'):
                try:
                    h2h['last_matches'] = json.loads(h2h['last_matches_json'])
                except:
                    h2h['last_matches'] = []
            return h2h
        return None
    
    def save_h2h(self, h2h_data: Dict) -> bool:
        """Save H2H record"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            # Ensure consistent ordering of team IDs
            team1_id = h2h_data.get('team1_id', 0)
            team2_id = h2h_data.get('team2_id', 0)
            
            if team1_id > team2_id:
                # Swap to maintain consistent order
                team1_id, team2_id = team2_id, team1_id
                h2h_data['team1_name'], h2h_data['team2_name'] = h2h_data.get('team2_name'), h2h_data.get('team1_name')
                h2h_data['team1_wins'], h2h_data['team2_wins'] = h2h_data.get('team2_wins', 0), h2h_data.get('team1_wins', 0)
                h2h_data['team1_goals'], h2h_data['team2_goals'] = h2h_data.get('team2_goals', 0), h2h_data.get('team1_goals', 0)
            
            last_matches_json = json.dumps(h2h_data.get('last_matches', []))
            
            cursor.execute('''
                INSERT OR REPLACE INTO h2h_records (
                    team1_id, team2_id, team1_name, team2_name,
                    total_matches, team1_wins, draws, team2_wins,
                    team1_goals, team2_goals, last_matches_json, last_updated
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                team1_id,
                team2_id,
                h2h_data.get('team1_name'),
                h2h_data.get('team2_name'),
                h2h_data.get('total_matches', 0),
                h2h_data.get('team1_wins', 0),
                h2h_data.get('draws', 0),
                h2h_data.get('team2_wins', 0),
                h2h_data.get('team1_goals', 0),
                h2h_data.get('team2_goals', 0),
                last_matches_json,
                datetime.now().isoformat()
            ))
            conn.commit()
            return True
        except Exception as e:
            print(f"Error saving H2H: {e}")
            return False
        finally:
            conn.close()
    
    # ============== FORM OPERATIONS ==============
    
    def get_team_form(self, team_id: int = None, team_name: str = None) -> Optional[Dict]:
        """Get team's current form"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        if team_id:
            cursor.execute('SELECT * FROM team_form WHERE team_id = ?', (team_id,))
        elif team_name:
            cursor.execute('SELECT * FROM team_form WHERE team_name LIKE ?', (f'%{team_name}%',))
        else:
            conn.close()
            return None
        
        row = cursor.fetchone()
        conn.close()
        
        if row:
            columns = [desc[0] for desc in cursor.description]
            return dict(zip(columns, row))
        return None
    
    def calculate_and_save_form(self, team_id: int, team_name: str, matches: List[Dict]) -> Dict:
        """Calculate form from recent matches and save to cache"""
        if not matches:
            return {
                'form_string': '',
                'last_5_points': 0,
                'trend': 'STABLE',
                'trend_strength': 50.0
            }
        
        # Sort matches by date (most recent first)
        sorted_matches = sorted(matches, key=lambda x: x.get('match_date', ''), reverse=True)[:5]
        
        form_results = []
        points = 0
        goals_for = 0
        goals_against = 0
        clean_sheets = 0
        
        for match in sorted_matches:
            home_goals = match.get('home_goals', 0) or 0
            away_goals = match.get('away_goals', 0) or 0
            is_home = match.get('home_team_id') == team_id or team_name.lower() in match.get('home_team_name', '').lower()
            
            if is_home:
                team_goals = home_goals
                opponent_goals = away_goals
            else:
                team_goals = away_goals
                opponent_goals = home_goals
            
            goals_for += team_goals
            goals_against += opponent_goals
            
            if opponent_goals == 0:
                clean_sheets += 1
            
            if team_goals > opponent_goals:
                form_results.append('W')
                points += 3
            elif team_goals == opponent_goals:
                form_results.append('D')
                points += 1
            else:
                form_results.append('L')
        
        form_string = ''.join(form_results)
        
        # Calculate trend
        recent_points = sum([3 if r == 'W' else 1 if r == 'D' else 0 for r in form_results[:3]])
        older_points = sum([3 if r == 'W' else 1 if r == 'D' else 0 for r in form_results[3:]])
        
        if recent_points > older_points + 2:
            trend = 'UP'
            trend_strength = min(80 + (recent_points - older_points) * 5, 100)
        elif recent_points < older_points - 2:
            trend = 'DOWN'
            trend_strength = max(20 - (older_points - recent_points) * 5, 0)
        else:
            trend = 'STABLE'
            trend_strength = 50.0
        
        form_data = {
            'team_id': team_id,
            'team_name': team_name,
            'form_string': form_string,
            'last_5_points': points,
            'last_5_goals_for': goals_for,
            'last_5_goals_against': goals_against,
            'last_5_clean_sheets': clean_sheets,
            'trend': trend,
            'trend_strength': trend_strength
        }
        
        # Save to database
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO team_form (
                    team_id, team_name, form_string, last_5_points,
                    last_5_goals_for, last_5_goals_against, last_5_clean_sheets,
                    trend, trend_strength, last_updated
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                team_id,
                team_name,
                form_string,
                points,
                goals_for,
                goals_against,
                clean_sheets,
                trend,
                trend_strength,
                datetime.now().isoformat()
            ))
            conn.commit()
        except Exception as e:
            print(f"Error saving form: {e}")
        finally:
            conn.close()
        
        return form_data
    
    # ============== ADVANCED ANALYTICS ==============
    
    def calculate_team_ratings(self, team_id: int, team_name: str, matches: List[Dict]) -> Dict:
        """Calculate comprehensive team ratings from match history"""
        if not matches:
            return {
                'attack': 50.0,
                'defense': 50.0,
                'form': 50.0,
                'home': 50.0,
                'away': 50.0,
                'consistency': 50.0
            }
        
        home_results = []
        away_results = []
        goals_scored = []
        goals_conceded = []
        
        for match in matches[:self.MAX_MATCHES_PER_TEAM]:
            home_goals = match.get('home_goals', 0) or 0
            away_goals = match.get('away_goals', 0) or 0
            is_home = match.get('home_team_id') == team_id or team_name.lower() in match.get('home_team_name', '').lower()
            
            if is_home:
                goals_scored.append(home_goals)
                goals_conceded.append(away_goals)
                result = 'W' if home_goals > away_goals else 'D' if home_goals == away_goals else 'L'
                home_results.append(result)
            else:
                goals_scored.append(away_goals)
                goals_conceded.append(home_goals)
                result = 'W' if away_goals > home_goals else 'D' if away_goals == home_goals else 'L'
                away_results.append(result)
        
        # Calculate ratings (0-100 scale)
        avg_goals_scored = sum(goals_scored) / len(goals_scored) if goals_scored else 0
        avg_goals_conceded = sum(goals_conceded) / len(goals_conceded) if goals_conceded else 0
        
        # Attack rating: based on goals scored (2 goals = 100)
        attack = min(avg_goals_scored / 2.0 * 100, 100)
        
        # Defense rating: based on goals conceded (0 goals = 100, 3+ = 0)
        defense = max(100 - (avg_goals_conceded / 3.0 * 100), 0)
        
        # Form rating: based on recent results
        all_results = home_results + away_results
        points = sum([3 if r == 'W' else 1 if r == 'D' else 0 for r in all_results[:5]])
        form = (points / 15) * 100  # Max 15 points from 5 games
        
        # Home rating
        home_points = sum([3 if r == 'W' else 1 if r == 'D' else 0 for r in home_results[:5]])
        home = (home_points / max(len(home_results[:5]) * 3, 1)) * 100 if home_results else 50
        
        # Away rating
        away_points = sum([3 if r == 'W' else 1 if r == 'D' else 0 for r in away_results[:5]])
        away = (away_points / max(len(away_results[:5]) * 3, 1)) * 100 if away_results else 50
        
        # Consistency: lower variance in goals scored/conceded = higher consistency
        if len(goals_scored) > 1:
            import statistics
            variance = statistics.variance(goals_scored) + statistics.variance(goals_conceded)
            consistency = max(100 - variance * 20, 0)
        else:
            consistency = 50
        
        return {
            'attack': round(attack, 1),
            'defense': round(defense, 1),
            'form': round(form, 1),
            'home': round(home, 1),
            'away': round(away, 1),
            'consistency': round(consistency, 1)
        }
    
    def get_prediction_adjustments(self, home_team: str, away_team: str) -> Dict:
        """
        Get prediction adjustments based on cached historical data.
        Returns adjustments to apply to base predictions.
        """
        adjustments = {
            'home_form_adj': 0.0,
            'away_form_adj': 0.0,
            'h2h_home_adj': 0.0,
            'h2h_away_adj': 0.0,
            'home_venue_adj': 0.0,
            'away_venue_adj': 0.0,
            'total_home_adj': 0.0,
            'total_away_adj': 0.0,
            'confidence_boost': 0.0,
            'insights': []
        }
        
        # Get form data
        home_form = self.get_team_form(team_name=home_team)
        away_form = self.get_team_form(team_name=away_team)
        
        if home_form:
            if home_form.get('trend') == 'UP':
                adjustments['home_form_adj'] = home_form.get('trend_strength', 50) / 100 * 5  # Up to +5%
                adjustments['insights'].append(f"📈 {home_team} trending UP ({home_form.get('form_string', '')})")
            elif home_form.get('trend') == 'DOWN':
                adjustments['home_form_adj'] = -((100 - home_form.get('trend_strength', 50)) / 100 * 5)  # Up to -5%
                adjustments['insights'].append(f"📉 {home_team} trending DOWN ({home_form.get('form_string', '')})")
        
        if away_form:
            if away_form.get('trend') == 'UP':
                adjustments['away_form_adj'] = away_form.get('trend_strength', 50) / 100 * 5
                adjustments['insights'].append(f"📈 {away_team} trending UP ({away_form.get('form_string', '')})")
            elif away_form.get('trend') == 'DOWN':
                adjustments['away_form_adj'] = -((100 - away_form.get('trend_strength', 50)) / 100 * 5)
                adjustments['insights'].append(f"📉 {away_team} trending DOWN ({away_form.get('form_string', '')})")
        
        # Get H2H data
        h2h = self.get_h2h(team1_name=home_team, team2_name=away_team)
        
        if h2h and h2h.get('total_matches', 0) >= 3:
            total = h2h['total_matches']
            
            # Determine which team is team1/team2 in the record
            if home_team.lower() in h2h.get('team1_name', '').lower():
                home_wins = h2h.get('team1_wins', 0)
                away_wins = h2h.get('team2_wins', 0)
            else:
                home_wins = h2h.get('team2_wins', 0)
                away_wins = h2h.get('team1_wins', 0)
            
            home_win_pct = home_wins / total
            away_win_pct = away_wins / total
            
            if home_win_pct > 0.6:
                adjustments['h2h_home_adj'] = (home_win_pct - 0.5) * 10  # Up to +5%
                adjustments['insights'].append(f"🏆 {home_team} dominates H2H: {home_wins}-{h2h.get('draws', 0)}-{away_wins}")
            elif away_win_pct > 0.6:
                adjustments['h2h_away_adj'] = (away_win_pct - 0.5) * 10
                adjustments['insights'].append(f"🏆 {away_team} dominates H2H: {away_wins}-{h2h.get('draws', 0)}-{home_wins}")
        
        # Get venue-specific data
        home_team_data = self.get_team(team_name=home_team)
        away_team_data = self.get_team(team_name=away_team)
        
        if home_team_data and home_team_data.get('home_rating', 50) > 60:
            adjustments['home_venue_adj'] = (home_team_data['home_rating'] - 50) / 50 * 3  # Up to +3%
            adjustments['insights'].append(f"🏠 {home_team} strong at home ({home_team_data['home_rating']:.0f}%)")
        
        if away_team_data and away_team_data.get('away_rating', 50) > 60:
            adjustments['away_venue_adj'] = (away_team_data['away_rating'] - 50) / 50 * 3
            adjustments['insights'].append(f"✈️ {away_team} strong away ({away_team_data['away_rating']:.0f}%)")
        
        # Calculate totals
        adjustments['total_home_adj'] = (
            adjustments['home_form_adj'] + 
            adjustments['h2h_home_adj'] + 
            adjustments['home_venue_adj']
        )
        adjustments['total_away_adj'] = (
            adjustments['away_form_adj'] + 
            adjustments['h2h_away_adj'] + 
            adjustments['away_venue_adj']
        )
        
        # Confidence boost based on data availability
        data_points = sum([
            1 if home_form else 0,
            1 if away_form else 0,
            1 if h2h else 0,
            1 if home_team_data else 0,
            1 if away_team_data else 0
        ])
        adjustments['confidence_boost'] = data_points * 2  # Up to +10%
        
        return adjustments
    
    # ============== UTILITY METHODS ==============
    
    def clear_cache(self):
        """Clear all cached data"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('DELETE FROM matches')
        cursor.execute('DELETE FROM teams')
        cursor.execute('DELETE FROM h2h_records')
        cursor.execute('DELETE FROM team_form')
        conn.commit()
        conn.close()
    
    def get_cache_stats(self) -> Dict:
        """Get statistics about cached data"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('SELECT COUNT(*) FROM teams')
        teams_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM matches')
        matches_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM h2h_records')
        h2h_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM team_form')
        form_count = cursor.fetchone()[0]
        
        conn.close()
        
        return {
            'teams': teams_count,
            'matches': matches_count,
            'h2h_records': h2h_count,
            'form_records': form_count,
            'db_path': self.db_path
        }


# ============== API INTEGRATION HELPER ==============

class TeamHistoryFetcher:
    """
    Helper class to fetch and cache team history data from API-Football.
    Integrates with TeamHistoryCache for on-demand data population.
    """
    
    def __init__(self, api_client, cache: TeamHistoryCache):
        """
        Initialize with API client and cache.
        
        Args:
            api_client: Instance of APIFootballClient from api_football.py
            cache: Instance of TeamHistoryCache
        """
        self.api = api_client
        self.cache = cache
    
    def fetch_and_cache_team_history(self, team_id: int, team_name: str, 
                                      league_id: int = None, season: int = None) -> Dict:
        """
        Fetch team's recent matches from API and cache them.
        Returns the calculated team ratings.
        """
        # Check if we have recent cached data
        existing = self.cache.get_team(team_id=team_id)
        if existing and self.cache._is_cache_valid(existing.get('last_updated')):
            # Return cached ratings
            return {
                'attack': existing.get('attack_rating', 50),
                'defense': existing.get('defense_rating', 50),
                'form': existing.get('form_rating', 50),
                'home': existing.get('home_rating', 50),
                'away': existing.get('away_rating', 50),
                'consistency': existing.get('consistency_rating', 50)
            }
        
        # Fetch from API
        try:
            # Get team's last 10 fixtures
            fixtures = self.api.get_team_fixtures(team_id, last=10)
            
            if fixtures:
                # Save matches to cache
                for fixture in fixtures:
                    match_data = self._parse_fixture(fixture)
                    self.cache.save_match(match_data)
                
                # Get matches from cache for rating calculation
                matches = self.cache.get_team_matches(team_id=team_id, limit=10)
                
                # Calculate ratings
                ratings = self.cache.calculate_team_ratings(team_id, team_name, matches)
                
                # Calculate and save form
                self.cache.calculate_and_save_form(team_id, team_name, matches)
                
                # Save team with ratings
                team_data = {
                    'team_id': team_id,
                    'team_name': team_name,
                    'league_id': league_id,
                    'attack_rating': ratings['attack'],
                    'defense_rating': ratings['defense'],
                    'form_rating': ratings['form'],
                    'home_rating': ratings['home'],
                    'away_rating': ratings['away'],
                    'consistency_rating': ratings['consistency'],
                    'overall_rating': sum(ratings.values()) / 6
                }
                self.cache.save_team(team_data)
                
                return ratings
        except Exception as e:
            print(f"Error fetching team history: {e}")
        
        # Return defaults if fetch failed
        return {
            'attack': 50.0,
            'defense': 50.0,
            'form': 50.0,
            'home': 50.0,
            'away': 50.0,
            'consistency': 50.0
        }
    
    def fetch_and_cache_h2h(self, team1_id: int, team2_id: int,
                            team1_name: str, team2_name: str) -> Optional[Dict]:
        """Fetch H2H data from API and cache it"""
        # Check cache first
        existing = self.cache.get_h2h(team1_id=team1_id, team2_id=team2_id)
        if existing and self.cache._is_cache_valid(existing.get('last_updated')):
            return existing
        
        try:
            # Fetch from API
            h2h_data = self.api.get_h2h(team1_id, team2_id)
            
            if h2h_data:
                # Parse and save
                parsed = self._parse_h2h(h2h_data, team1_id, team2_id, team1_name, team2_name)
                self.cache.save_h2h(parsed)
                return parsed
        except Exception as e:
            print(f"Error fetching H2H: {e}")
        
        return existing  # Return cached version if fetch failed
    
    def _parse_fixture(self, fixture: Dict) -> Dict:
        """Parse API fixture response into cache format"""
        return {
            'fixture_id': fixture.get('fixture', {}).get('id'),
            'home_team_id': fixture.get('teams', {}).get('home', {}).get('id'),
            'away_team_id': fixture.get('teams', {}).get('away', {}).get('id'),
            'home_team_name': fixture.get('teams', {}).get('home', {}).get('name'),
            'away_team_name': fixture.get('teams', {}).get('away', {}).get('name'),
            'league_id': fixture.get('league', {}).get('id'),
            'league_name': fixture.get('league', {}).get('name'),
            'match_date': fixture.get('fixture', {}).get('date', '')[:10],
            'match_time': fixture.get('fixture', {}).get('date', '')[11:16],
            'home_goals': fixture.get('goals', {}).get('home'),
            'away_goals': fixture.get('goals', {}).get('away'),
            'status': fixture.get('fixture', {}).get('status', {}).get('short'),
            'venue': fixture.get('fixture', {}).get('venue', {}).get('name')
        }
    
    def _parse_h2h(self, h2h_data: List[Dict], team1_id: int, team2_id: int,
                   team1_name: str, team2_name: str) -> Dict:
        """Parse API H2H response into cache format"""
        team1_wins = 0
        team2_wins = 0
        draws = 0
        team1_goals = 0
        team2_goals = 0
        last_matches = []
        
        for match in h2h_data[:10]:  # Last 10 H2H matches
            home_id = match.get('teams', {}).get('home', {}).get('id')
            home_goals = match.get('goals', {}).get('home', 0) or 0
            away_goals = match.get('goals', {}).get('away', 0) or 0
            
            if home_id == team1_id:
                team1_goals += home_goals
                team2_goals += away_goals
                if home_goals > away_goals:
                    team1_wins += 1
                elif home_goals < away_goals:
                    team2_wins += 1
                else:
                    draws += 1
            else:
                team1_goals += away_goals
                team2_goals += home_goals
                if away_goals > home_goals:
                    team1_wins += 1
                elif away_goals < home_goals:
                    team2_wins += 1
                else:
                    draws += 1
            
            last_matches.append({
                'date': match.get('fixture', {}).get('date', '')[:10],
                'home': match.get('teams', {}).get('home', {}).get('name'),
                'away': match.get('teams', {}).get('away', {}).get('name'),
                'score': f"{home_goals}-{away_goals}"
            })
        
        return {
            'team1_id': team1_id,
            'team2_id': team2_id,
            'team1_name': team1_name,
            'team2_name': team2_name,
            'total_matches': len(h2h_data[:10]),
            'team1_wins': team1_wins,
            'draws': draws,
            'team2_wins': team2_wins,
            'team1_goals': team1_goals,
            'team2_goals': team2_goals,
            'last_matches': last_matches
        }


# Singleton instance for easy access
_cache_instance = None

def get_team_history_cache() -> TeamHistoryCache:
    """Get or create the singleton cache instance"""
    global _cache_instance
    if _cache_instance is None:
        _cache_instance = TeamHistoryCache()
    return _cache_instance
