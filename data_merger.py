"""
Data Merger Engine
Intelligently combines CSV predictions with API-Football live data
Enhanced with team history caching for better predictions
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from difflib import SequenceMatcher
import re

# Import team history cache for enhanced analytics
try:
    from team_history import TeamHistoryCache, get_team_history_cache
    TEAM_CACHE_AVAILABLE = True
except ImportError:
    TEAM_CACHE_AVAILABLE = False


class TeamNameMatcher:
    """Fuzzy matching for team names across different data sources"""
    
    # Common name variations/aliases
    TEAM_ALIASES = {
        # English
        "man utd": ["manchester united", "man united", "manchester utd", "mufc"],
        "man city": ["manchester city", "manchester c", "mcfc"],
        "spurs": ["tottenham", "tottenham hotspur", "tottenham hotspurs"],
        "wolves": ["wolverhampton", "wolverhampton wanderers"],
        "west ham": ["west ham united", "west ham utd"],
        "newcastle": ["newcastle united", "newcastle utd"],
        "brighton": ["brighton & hove albion", "brighton hove albion", "brighton & hove"],
        "nottingham forest": ["nottm forest", "nott'm forest", "nottingham", "forest"],
        "luton": ["luton town"],
        "sheffield utd": ["sheffield united"],
        "crystal palace": ["c palace", "c. palace"],
        "arsenal": ["arsenal fc"],
        "chelsea": ["chelsea fc"],
        "liverpool": ["liverpool fc"],
        "everton": ["everton fc"],
        "aston villa": ["villa"],
        "bournemouth": ["afc bournemouth"],
        "burnley": ["burnley fc"],
        "fulham": ["fulham fc"],
        "brentford": ["brentford fc"],
        "ipswich": ["ipswich town"],
        "leicester": ["leicester city"],
        "southampton": ["southampton fc"],
        
        # German
        "bayern": ["bayern munich", "bayern münchen", "fc bayern", "bayern munchen"],
        "dortmund": ["borussia dortmund", "bvb", "bvb dortmund"],
        "leverkusen": ["bayer leverkusen", "bayer 04", "bayer 04 leverkusen"],
        "gladbach": ["borussia mönchengladbach", "borussia m'gladbach", "m'gladbach", "monchengladbach", "borussia monchengladbach"],
        "frankfurt": ["eintracht frankfurt", "e. frankfurt"],
        "rb leipzig": ["leipzig", "rasenballsport leipzig"],
        "wolfsburg": ["vfl wolfsburg"],
        "freiburg": ["sc freiburg"],
        "hoffenheim": ["tsg hoffenheim", "tsg 1899 hoffenheim"],
        "mainz": ["mainz 05", "1. fsv mainz 05"],
        "augsburg": ["fc augsburg"],
        "bremen": ["werder bremen", "sv werder bremen"],
        "koln": ["fc köln", "fc koln", "1. fc koln", "1. fc köln", "cologne"],
        "union berlin": ["1. fc union berlin", "fc union berlin"],
        "bochum": ["vfl bochum"],
        "heidenheim": ["fc heidenheim", "1. fc heidenheim"],
        "st pauli": ["fc st. pauli", "fc st pauli", "st. pauli"],
        "holstein kiel": ["kiel", "holstein"],
        
        # Spanish
        "real madrid": ["r madrid", "r. madrid", "madrid"],
        "barcelona": ["fc barcelona", "barca", "barça"],
        "atletico": ["atletico madrid", "atlético madrid", "atl madrid", "atl. madrid", "atletico de madrid"],
        "real betis": ["betis"],
        "athletic": ["athletic bilbao", "athletic club"],
        "celta": ["celta vigo", "rc celta"],
        "real sociedad": ["r sociedad", "r. sociedad", "sociedad"],
        "villarreal": ["villarreal cf"],
        "sevilla": ["sevilla fc"],
        "valencia": ["valencia cf"],
        "mallorca": ["rcd mallorca"],
        "osasuna": ["ca osasuna"],
        "getafe": ["getafe cf"],
        "rayo vallecano": ["rayo", "vallecano"],
        "alaves": ["deportivo alaves", "deportivo alavés"],
        "las palmas": ["ud las palmas"],
        "girona": ["girona fc"],
        "leganes": ["cd leganes", "cd leganés"],
        "espanyol": ["rcd espanyol"],
        "valladolid": ["real valladolid"],
        
        # Italian
        "inter": ["inter milan", "internazionale", "fc internazionale", "inter milano"],
        "ac milan": ["milan"],
        "juventus": ["juve"],
        "napoli": ["ssc napoli"],
        "roma": ["as roma"],
        "lazio": ["ss lazio"],
        "atalanta": ["atalanta bc", "atalanta bergamo"],
        "fiorentina": ["acf fiorentina"],
        "torino": ["torino fc"],
        "bologna": ["bologna fc"],
        "verona": ["hellas verona"],
        "udinese": ["udinese calcio"],
        "empoli": ["empoli fc"],
        "lecce": ["us lecce"],
        "genoa": ["genoa cfc"],
        "monza": ["ac monza"],
        "cagliari": ["cagliari calcio"],
        "parma": ["parma calcio"],
        "como": ["como 1907"],
        "venezia": ["venezia fc"],
        
        # French
        "psg": ["paris saint-germain", "paris saint germain", "paris sg", "paris"],
        "marseille": ["olympique marseille", "om", "olympique de marseille"],
        "lyon": ["olympique lyonnais", "olympique lyon", "ol"],
        "monaco": ["as monaco"],
        "lille": ["losc lille", "losc"],
        "nice": ["ogc nice"],
        "lens": ["rc lens"],
        "rennes": ["stade rennais", "stade rennais fc"],
        "strasbourg": ["rc strasbourg", "rc strasbourg alsace"],
        "nantes": ["fc nantes"],
        "reims": ["stade de reims"],
        "toulouse": ["toulouse fc"],
        "montpellier": ["montpellier hsc"],
        "brest": ["stade brestois", "stade brestois 29"],
        "le havre": ["le havre ac"],
        "auxerre": ["aj auxerre"],
        "angers": ["angers sco"],
        "st etienne": ["as saint-etienne", "as saint etienne", "saint-etienne", "saint etienne"],
        
        # Dutch
        "ajax": ["afc ajax"],
        "psv": ["psv eindhoven"],
        "feyenoord": ["feyenoord rotterdam"],
        "az": ["az alkmaar"],
        "twente": ["fc twente"],
        "utrecht": ["fc utrecht"],
        
        # Portuguese
        "benfica": ["sl benfica"],
        "porto": ["fc porto"],
        "sporting": ["sporting cp", "sporting lisbon"],
        "braga": ["sc braga", "sporting braga"],
        
        # Israeli
        "maccabi ta": ["maccabi tel aviv", "maccabi tel-aviv", "m. tel aviv", "maccabi t.a"],
        "hapoel ta": ["hapoel tel aviv", "hapoel tel-aviv", "h. tel aviv", "hapoel t.a"],
        "beitar": ["beitar jerusalem", "beitar j'lem", "beitar j'salem"],
        "maccabi haifa": ["m haifa", "m. haifa"],
        "hapoel bs": ["hapoel beer sheva", "hapoel be'er sheva", "h. beer sheva", "hapoel b.s", "hapoel beer-sheva"],
        "hapoel haifa": ["h. haifa", "h haifa"],
        "bnei sakhnin": ["b. sakhnin", "sakhnin"],
        "maccabi netanya": ["m. netanya", "m netanya"],
        "maccabi petah tikva": ["m. petah tikva", "maccabi pt", "m petah tikva", "maccabi p.t"],
        "hapoel jerusalem": ["h. jerusalem", "hapoel j'lem"],
        "fc ashdod": ["ashdod", "ms ashdod", "m.s. ashdod", "ironi ashdod"],
        "bnei yehuda": ["bnei yehuda ta", "bnei yehuda tel aviv"],
        "hapoel kfar saba": ["h. kfar saba", "kfar saba"],
        "hapoel raanana": ["h. ra'anana", "raanana", "hapoel ra'anana"],
        "hapoel hadera": ["h. hadera", "hadera"],
        "ironi kiryat shmona": ["kiryat shmona", "hapoel kiryat shmona"],
        "sektzia nes tziona": ["nes tziona", "sektzia"],
        "hapoel afula": ["h. afula", "afula"],
        "hapoel petah tikva": ["h. petah tikva", "hapoel p.t"],
        "hapoel nof hagalil": ["nof hagalil", "h. nof hagalil"],
        
        # Scottish
        "celtic": ["celtic fc", "celtic glasgow"],
        "rangers": ["rangers fc", "glasgow rangers"],
        "hearts": ["heart of midlothian", "hearts fc"],
        "hibernian": ["hibs"],
        "aberdeen": ["aberdeen fc"],
        
        # Belgian
        "club brugge": ["club bruges", "brugge"],
        "anderlecht": ["rsc anderlecht"],
        "genk": ["krc genk", "racing genk"],
        "standard": ["standard liege", "standard liège"],
        "gent": ["kaa gent"],
        
        # Turkish
        "galatasaray": ["galatasaray sk"],
        "fenerbahce": ["fenerbahçe", "fenerbahce sk"],
        "besiktas": ["beşiktaş", "besiktas jk"],
        "trabzonspor": ["trabzon"],
        
        # Brazilian
        "flamengo": ["cr flamengo", "flamengo rj"],
        "palmeiras": ["se palmeiras"],
        "corinthians": ["sc corinthians", "corinthians sp"],
        "sao paulo": ["são paulo", "sao paulo fc", "são paulo fc"],
        "santos": ["santos fc"],
        "fluminense": ["fluminense fc", "fluminense rj"],
        "gremio": ["grêmio", "gremio fb"],
        "internacional": ["sc internacional", "inter rs"],
        "athletico pr": ["athletico paranaense", "athletico-pr", "cap"],
        "atletico mg": ["atletico mineiro", "atlético mineiro", "atlético-mg", "atletico-mg"],
        "cruzeiro": ["cruzeiro mg", "cruzeiro ec"],
        "botafogo": ["botafogo fr", "botafogo rj"],
        "vasco": ["vasco da gama", "cr vasco da gama"],
        "ceara": ["ceará", "ceara sc"],
        "fortaleza": ["fortaleza ec"],
        "bahia": ["ec bahia"],
        "sport": ["sport recife", "sport club recife"],
        "vitoria": ["ec vitória", "ec vitoria"],
        "coritiba": ["coritiba fc"],
        "goias": ["goiás", "goias ec"],
        "cuiaba": ["cuiabá", "cuiaba ec"],
        "juventude": ["ec juventude"],
        "america mg": ["america mineiro", "américa mineiro", "américa-mg"],
        "red bull bragantino": ["bragantino", "rb bragantino"],
        
        # Mexican
        "america": ["club america", "club américa"],
        "guadalajara": ["chivas", "cd guadalajara"],
        "cruz azul": ["cruz azul fc"],
        "tigres": ["tigres uanl"],
        "monterrey": ["cf monterrey"],
        "toluca": ["deportivo toluca"],
        "pumas": ["pumas unam", "unam"],
        "santos laguna": ["santos lag"],
        "leon": ["club leon", "león"],
        "pachuca": ["cf pachuca"],
        "necaxa": ["club necaxa"],
        "atlas": ["atlas fc"],
        "mazatlan": ["mazatlán", "mazatlan fc"],
        "queretaro": ["querétaro", "queretaro fc"],
        "puebla": ["club puebla"],
        "tijuana": ["club tijuana", "xolos"],
        
        # African
        "al ahly": ["al-ahly", "ahly cairo"],
        "zamalek": ["zamalek sc"],
        "al hilal": ["al-hilal", "al hilal omdurman"],
        "esperance": ["esperance tunis", "es tunis"],
        "wydad": ["wydad casablanca", "wydad ac"],
        "raja": ["raja casablanca", "raja ca"],
        "mamelodi sundowns": ["sundowns"],
        "kaizer chiefs": ["kaizer chiefs fc"],
        "orlando pirates": ["orlando pirates fc"],
        "tp mazembe": ["tout puissant mazembe"],
        "simba": ["simba sc"],
        "young africans": ["young africans sc", "yanga"],
    }
    
    def __init__(self):
        # Build reverse lookup
        self.alias_lookup = {}
        for canonical, aliases in self.TEAM_ALIASES.items():
            self.alias_lookup[canonical.lower()] = canonical
            for alias in aliases:
                self.alias_lookup[alias.lower()] = canonical
    
    def normalize_name(self, name: str) -> str:
        """Normalize team name for comparison - aggressive normalization"""
        if not name:
            return ""
        
        name = name.lower().strip()
        
        # Remove common prefixes and suffixes (fc, sc, etc.)
        name = re.sub(r'\s*(fc|cf|sc|ac|afc|ssc|rc|rcd|cd|ud|sd|as|us|ss|sl|fk|nk|sk|bk|if|ff|gf|ik|tk|pk|pk|jk|hk|mk|ok|rk|vk|ms|ks|1\.)\s*$', '', name)
        name = re.sub(r'^(fc|cf|sc|ac|afc|ssc|rc|rcd|cd|ud|sd|as|us|ss|sl|fk|nk|sk|bk|if|ff|gf|ik|tk|pk|pk|jk|hk|mk|ok|rk|vk|ms|ks|1\.)\s*', '', name)
        
        # Remove location suffixes like (H), (A), city abbreviations
        name = re.sub(r'\s*\([^)]*\)\s*', '', name)  # Remove parentheses content
        name = re.sub(r'\s*-\s*$', '', name)  # Remove trailing dashes
        
        # Remove special characters but keep spaces
        name = re.sub(r"['\.\-\"\(\)]", '', name)
        name = re.sub(r'[^\w\s]', ' ', name)
        name = ' '.join(name.split())  # Normalize whitespace
        
        # Check aliases first
        if name in self.alias_lookup:
            return self.alias_lookup[name]
        
        # Also check without spaces
        name_nospace = name.replace(' ', '')
        if name_nospace in self.alias_lookup:
            return self.alias_lookup[name_nospace]
        
        return name
    
    def similarity_score(self, name1: str, name2: str) -> float:
        """Calculate similarity between two team names - enhanced matching"""
        n1 = self.normalize_name(name1)
        n2 = self.normalize_name(name2)
        
        # Exact match after normalization
        if n1 == n2:
            return 1.0
        
        # No-space comparison
        n1_nospace = n1.replace(' ', '')
        n2_nospace = n2.replace(' ', '')
        if n1_nospace == n2_nospace:
            return 1.0
        
        # Check if one contains the other (key word match)
        if n1 in n2 or n2 in n1:
            return 0.92
        if n1_nospace in n2_nospace or n2_nospace in n1_nospace:
            return 0.90
        
        # Check if main word matches (e.g., "Netanya" in "Maccabi Netanya")
        words1 = set(n1.split())
        words2 = set(n2.split())
        common_words = words1 & words2
        if common_words:
            # If a significant word matches (4+ chars)
            significant = [w for w in common_words if len(w) >= 4]
            if significant:
                # More significant words = higher score
                return min(0.88, 0.80 + len(significant) * 0.04)
        
        # Check for partial word matches (first 4 chars)
        for w1 in words1:
            if len(w1) >= 4:
                for w2 in words2:
                    if len(w2) >= 4:
                        if w1[:4] == w2[:4]:
                            return 0.82
        
        # Use sequence matcher as fallback
        seq_score = SequenceMatcher(None, n1, n2).ratio()
        # Also try no-space comparison
        seq_score_nospace = SequenceMatcher(None, n1_nospace, n2_nospace).ratio()
        
        return max(seq_score, seq_score_nospace)
    
    def is_match(self, name1: str, name2: str, threshold: float = 0.75) -> bool:
        """Check if two team names match"""
        return self.similarity_score(name1, name2) >= threshold


class DataMerger:
    """Merges prediction data from CSV with live API-Football data"""
    
    def __init__(self):
        self.team_matcher = TeamNameMatcher()
    
    def parse_date(self, date_val) -> Optional[datetime]:
        """Parse various date formats"""
        if pd.isna(date_val) or date_val == 'Unknown' or date_val == 'nan':
            return None
        
        if isinstance(date_val, datetime):
            return date_val
        
        if isinstance(date_val, pd.Timestamp):
            return date_val.to_pydatetime()
        
        date_str = str(date_val).strip()
        
        # Handle ISO format from API
        if 'T' in date_str:
            try:
                return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            except:
                pass
        
        formats = [
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d %H:%M',
            '%Y-%m-%d',
            '%d/%m/%Y %H:%M:%S',
            '%d/%m/%Y %H:%M',
            '%d/%m/%Y',
            '%d-%m-%Y',
            '%m/%d/%Y',
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        
        return None
    
    def match_fixture(self, csv_row: pd.Series, api_fixtures: List[Dict], debug: bool = False) -> Optional[Dict]:
        """Find matching API fixture for a CSV prediction row"""
        csv_home = str(csv_row.get('home', '')).strip()
        csv_away = str(csv_row.get('away', '')).strip()
        csv_date = self.parse_date(csv_row.get('date'))
        
        best_match = None
        best_score = 0
        best_debug_info = None
        
        for fixture_data in api_fixtures:
            api_home = ''
            api_away = ''
            api_date = None
            
            # Detect format by checking if 'fixture' contains 'teams' (get_full_match_data format)
            # or if 'teams' is at root level (raw API format)
            fixture_key = fixture_data.get('fixture', {})
            
            if isinstance(fixture_key, dict) and 'teams' in fixture_key:
                # get_full_match_data format: fixture_data['fixture'] IS the raw API fixture
                # Structure: {fixture: {fixture: {id,date}, teams: {home,away}, league: {}}, odds: [], ...}
                teams = fixture_key.get('teams', {})
                api_home = teams.get('home', {}).get('name', '')
                api_away = teams.get('away', {}).get('name', '')
                fixture_info = fixture_key.get('fixture', {})
                api_date = self.parse_date(fixture_info.get('date') if isinstance(fixture_info, dict) else None)
            elif 'teams' in fixture_data:
                # Raw API fixture format: {fixture: {id, date}, teams: {home, away}, league: {}}
                teams = fixture_data.get('teams', {})
                api_home = teams.get('home', {}).get('name', '')
                api_away = teams.get('away', {}).get('name', '')
                fixture_info = fixture_data.get('fixture', {})
                api_date = self.parse_date(fixture_info.get('date') if isinstance(fixture_info, dict) else None)
            
            if not api_home or not api_away:
                continue
            
            # Check team match
            home_score = self.team_matcher.similarity_score(csv_home, api_home)
            away_score = self.team_matcher.similarity_score(csv_away, api_away)
            
            # Both teams must match reasonably well (lowered threshold to 0.45)
            if home_score < 0.45 or away_score < 0.45:
                if debug and (home_score >= 0.3 or away_score >= 0.3):
                    # Close match but not close enough - store for debug
                    pass
                continue
            
            team_score = (home_score + away_score) / 2
            
            # Check date match (if available) - allow up to 3 days
            date_score = 1.0
            if csv_date and api_date:
                date_diff = abs((csv_date.date() - api_date.date()).days)
                if date_diff > 3:  # Allow up to 3 days difference
                    continue
                date_score = 1.0 if date_diff == 0 else (0.9 if date_diff == 1 else (0.8 if date_diff == 2 else 0.7))
            
            total_score = team_score * 0.85 + date_score * 0.15
            
            if total_score > best_score and total_score > 0.55:  # Lowered threshold to 0.55
                best_score = total_score
                best_match = fixture_data
                best_debug_info = {
                    'csv_home': csv_home, 'csv_away': csv_away,
                    'api_home': api_home, 'api_away': api_away,
                    'home_score': home_score, 'away_score': away_score,
                    'total_score': total_score
                }
        
        return best_match
    
    def merge_data(self, predictions_df: pd.DataFrame, api_fixtures: List[Dict], debug: bool = False) -> List[Dict]:
        """Merge CSV predictions with API fixture data
        
        Args:
            predictions_df: DataFrame with CSV predictions
            api_fixtures: List of API fixture data
            debug: If True, returns unmatched info
        
        Returns:
            List of merged records
        """
        merged_data = []
        self.unmatched_teams = []  # Store unmatched for debugging
        
        for idx, row in predictions_df.iterrows():
            # Find matching API fixture
            api_fixture = self.match_fixture(row, api_fixtures)
            
            if not api_fixture:
                self.unmatched_teams.append({
                    'home': row.get('home', ''),
                    'away': row.get('away', ''),
                    'league': row.get('league', ''),
                    'date': str(row.get('date', ''))
                })
            
            merged_record = {
                # From CSV
                'csv_id': row.get('id', idx),
                'csv_home': row.get('home', ''),
                'csv_away': row.get('away', ''),
                'csv_league': row.get('league', ''),
                'csv_date': row.get('date', ''),
                
                # Model probabilities from CSV
                'model_probs': {
                    'home_win': self._safe_float(row.get('1x2_h')),
                    'draw': self._safe_float(row.get('1x2_d')),
                    'away_win': self._safe_float(row.get('1x2_a')),
                    'over_1.5': self._safe_float(row.get('o_1.5')),
                    'over_2.5': self._safe_float(row.get('o_2.5')),
                    'over_3.5': self._safe_float(row.get('o_3.5')),
                    'under_1.5': self._safe_float(row.get('u_1.5')),
                    'under_2.5': self._safe_float(row.get('u_2.5')),
                    'under_3.5': self._safe_float(row.get('u_3.5')),
                },
                
                # API data
                'has_api_data': api_fixture is not None,
                'api_fixture': api_fixture
            }
            
            if api_fixture:
                # Detect format: check if 'fixture' key contains 'teams' (get_full_match_data format)
                fixture_key = api_fixture.get('fixture', {})
                
                if isinstance(fixture_key, dict) and 'teams' in fixture_key:
                    # get_full_match_data format: fixture_key IS the raw API fixture
                    # Structure: {fixture: {fixture: {id,date}, teams: {home,away}, league: {}}, odds: [], ...}
                    teams = fixture_key.get('teams', {})
                    league_info = fixture_key.get('league', {})
                    fixture_info = fixture_key.get('fixture', {})
                    venue_info = fixture_info.get('venue', {}) if isinstance(fixture_info, dict) else {}
                    home_name = teams.get('home', {}).get('name', '')
                    away_name = teams.get('away', {}).get('name', '')
                    api_date = fixture_info.get('date') if isinstance(fixture_info, dict) else None
                    fixture_id = fixture_info.get('id') if isinstance(fixture_info, dict) else None
                else:
                    # Raw API fixture format: {fixture: {id, date}, teams: {home, away}, league: {}}
                    teams = api_fixture.get('teams', {})
                    league_info = api_fixture.get('league', {})
                    fixture_info = api_fixture.get('fixture', {})
                    venue_info = fixture_info.get('venue', {}) if isinstance(fixture_info, dict) else {}
                    home_name = teams.get('home', {}).get('name', '')
                    away_name = teams.get('away', {}).get('name', '')
                    api_date = fixture_info.get('date') if isinstance(fixture_info, dict) else None
                    fixture_id = fixture_info.get('id') if isinstance(fixture_info, dict) else None
                
                # Extract odds from list
                odds_list = api_fixture.get('odds', [])
                
                merged_record.update({
                    'fixture_id': fixture_id,
                    'api_home': home_name,
                    'api_away': away_name,
                    'api_league': league_info.get('name', '') if isinstance(league_info, dict) else '',
                    'api_date': api_date,
                    'venue': venue_info.get('name', '') if isinstance(venue_info, dict) else '',
                    
                    # Bookmaker odds - handle list format
                    'bookmaker_odds': self._extract_odds(odds_list),
                    
                    # H2H
                    'h2h': api_fixture.get('h2h', []),
                    
                    # Lineups
                    'lineups': api_fixture.get('lineups', []),
                    
                    # Predictions from API
                    'api_predictions': api_fixture.get('predictions', {}),
                    
                    # Statistics
                    'statistics': api_fixture.get('statistics', []),
                })
            
            merged_data.append(merged_record)
        
        return merged_data
    
    def _safe_float(self, val) -> Optional[float]:
        """Safely convert value to float"""
        if pd.isna(val) or val is None:
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None
    
    def _extract_odds(self, odds_data) -> Dict:
        """Extract and structure odds from API response (handles list format)"""
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
            'raw_odds': []
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
                                odd = self._safe_float(val.get('odd'))
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
                                odd = self._safe_float(val.get('odd'))
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
                                odd = self._safe_float(val.get('odd'))
                                if v == 'Yes':
                                    extracted['btts_yes'] = odd
                                elif v == 'No':
                                    extracted['btts_no'] = odd
                        
                        # Store raw for reference
                        extracted['raw_odds'].append({
                            'market': bet_name,
                            'values': values
                        })
                    
                    # Only process first bookmaker
                    break
        
        # Handle dict format (legacy)
        elif isinstance(odds_data, dict):
            best_odds = odds_data.get('best_odds', {})
            if '1x2' in best_odds:
                extracted['home_win'] = self._safe_float(best_odds['1x2'].get('home'))
                extracted['draw'] = self._safe_float(best_odds['1x2'].get('draw'))
                extracted['away_win'] = self._safe_float(best_odds['1x2'].get('away'))
            if odds_data.get('bookmakers'):
                extracted['bookmaker'] = odds_data['bookmakers'][0].get('name')
        
        return extracted


class ValueBetCalculator:
    """Calculates value bets, edges, and Kelly criterion"""
    
    @staticmethod
    def odds_to_probability(decimal_odds: float) -> Optional[float]:
        """Convert decimal odds to implied probability"""
        if decimal_odds and decimal_odds > 0:
            return 1 / decimal_odds
        return None
    
    @staticmethod
    def probability_to_odds(probability: float) -> Optional[float]:
        """Convert probability to decimal odds"""
        if probability and 0 < probability <= 1:
            return 1 / probability
        return None
    
    @staticmethod
    def calculate_edge(model_prob: float, implied_prob: float) -> float:
        """Calculate the edge (value) of a bet"""
        if model_prob and implied_prob:
            return model_prob - implied_prob
        return 0
    
    @staticmethod
    def is_value_bet(model_prob: float, implied_prob: float, threshold: float = 0.05) -> Tuple[bool, float]:
        """Check if a bet has value (edge > threshold)"""
        edge = ValueBetCalculator.calculate_edge(model_prob, implied_prob)
        return edge >= threshold, edge
    
    @staticmethod
    def kelly_criterion(probability: float, odds: float, fraction: float = 0.25) -> float:
        """
        Calculate Kelly Criterion stake
        Uses fractional Kelly (default 25%) for safety
        Returns stake as percentage (0-5%)
        """
        if not probability or not odds or odds <= 1:
            return 0
        
        b = odds - 1  # Net odds
        p = probability
        q = 1 - p
        
        kelly = (b * p - q) / b
        
        # Apply fractional Kelly and cap at 5%
        stake = max(0, kelly * fraction)
        return min(stake, 0.05)
    
    @staticmethod
    def get_confidence_level(probability: float, edge: float, injury_factor: float = 1.0) -> Tuple[str, float]:
        """
        Determine confidence level based on probability (standardized thresholds)
        Returns (level_string, adjusted_probability)
        
        Thresholds:
        - HIGH: >= 70%
        - MEDIUM: >= 60%
        - LOW: < 60%
        """
        # Adjust for injuries
        adjusted_prob = probability * injury_factor
        
        if adjusted_prob >= 0.70:
            return "HIGH", adjusted_prob
        elif adjusted_prob >= 0.60:
            return "MEDIUM", adjusted_prob
        else:
            return "LOW", adjusted_prob
    
    @staticmethod
    def get_risk_rating(probability: float, edge: float) -> str:
        """Determine risk rating"""
        if probability >= 0.70:
            return "Low"
        elif probability >= 0.55:
            return "Medium"
        else:
            return "High"
    
    @staticmethod
    def calculate_injury_impact(injuries: List[Dict], is_key_player: bool = True) -> float:
        """
      Calculate injury impact factor (0.8-1.0)
        More injuries = lower factor
        """
        if not injuries:
            return 1.0
        
        num_injuries = len(injuries)
        
        if num_injuries >= 4:
            return 0.80  # -20% confidence
        elif num_injuries >= 2:
            return 0.90  # -10% confidence
        elif num_injuries >= 1:
            return 0.95  # -5% confidence
        
        return 1.0
    
    @staticmethod
    def calculate_h2h_boost(h2h: Dict, team_position: str) -> float:
        """
        Calculate confidence boost from H2H record
        Returns boost factor (1.0-1.12)
        """
        if not h2h or not h2h.get('matches'):
            return 1.0
        
        total = h2h.get('team1_wins', 0) + h2h.get('team2_wins', 0) + h2h.get('draws', 0)
        if total < 3:
            return 1.0
        
        if team_position == 'home':
            win_rate = h2h.get('team1_wins', 0) / total
        else:
            win_rate = h2h.get('team2_wins', 0) / total
        
        if win_rate >= 0.7:
            return 1.12  # +12% boost
        elif win_rate >= 0.5:
            return 1.07  # +7% boost
        elif win_rate >= 0.3:
            return 1.03  # +3% boost
        
        return 1.0


def analyze_merged_match(merged_record: Dict) -> Dict:
    """Analyze a merged match record and generate betting recommendations"""
    calc = ValueBetCalculator()
    
    analysis = {
        'match': f"{merged_record.get('csv_home', 'Unknown')} vs {merged_record.get('csv_away', 'Unknown')}",
        'league': merged_record.get('csv_league') or merged_record.get('api_league', 'Unknown'),
        'date': merged_record.get('api_date') or merged_record.get('csv_date', 'TBD'),
        'has_api_data': merged_record.get('has_api_data', False),
        'model_probs': merged_record.get('model_probs', {}),
        'bookmaker_odds': merged_record.get('bookmaker_odds', {}),
        'injuries': {
            'home': merged_record.get('home_injuries', []),
            'away': merged_record.get('away_injuries', [])
        },
        'h2h': merged_record.get('h2h', {}),
        'recommendations': [],
        'historical_insights': []  # New: insights from cached data
    }
    
    model_probs = merged_record.get('model_probs', {})
    bookmaker_odds = merged_record.get('bookmaker_odds', {})
    
    # Calculate injury impact
    home_injury_factor = calc.calculate_injury_impact(merged_record.get('home_injuries', []))
    away_injury_factor = calc.calculate_injury_impact(merged_record.get('away_injuries', []))
    
    # NEW: Get historical adjustments from cache
    historical_home_adj = 0.0
    historical_away_adj = 0.0
    confidence_boost = 0.0
    
    if TEAM_CACHE_AVAILABLE:
        try:
            cache = get_team_history_cache()
            home_team = merged_record.get('csv_home', '')
            away_team = merged_record.get('csv_away', '')
            
            if cache and home_team and away_team:
                adjustments = cache.get_prediction_adjustments(home_team, away_team)
                historical_home_adj = adjustments.get('total_home_adj', 0) / 100  # Convert % to decimal
                historical_away_adj = adjustments.get('total_away_adj', 0) / 100
                confidence_boost = adjustments.get('confidence_boost', 0) / 100
                
                # Add insights to analysis
                analysis['historical_insights'] = adjustments.get('insights', [])
        except Exception as e:
            pass  # Silently fail if cache unavailable
    
    # Analyze each market
    markets = [
        ('home_win', 'Home Win', model_probs.get('home_win'), bookmaker_odds.get('home_win'), home_injury_factor, historical_home_adj),
        ('draw', 'Draw', model_probs.get('draw'), bookmaker_odds.get('draw'), 1.0, 0.0),
        ('away_win', 'Away Win', model_probs.get('away_win'), bookmaker_odds.get('away_win'), away_injury_factor, historical_away_adj),
        ('over_1.5', 'Over 1.5 Goals', model_probs.get('over_1.5'), bookmaker_odds.get('over_1.5'), 1.0, 0.0),
        ('under_1.5', 'Under 1.5 Goals', model_probs.get('under_1.5'), bookmaker_odds.get('under_1.5'), 1.0, 0.0),
        ('over_2.5', 'Over 2.5 Goals', model_probs.get('over_2.5'), bookmaker_odds.get('over_2.5'), 1.0, 0.0),
        ('under_2.5', 'Under 2.5 Goals', model_probs.get('under_2.5'), bookmaker_odds.get('under_2.5'), 1.0, 0.0),
        ('over_3.5', 'Over 3.5 Goals', model_probs.get('over_3.5'), bookmaker_odds.get('over_3.5'), 1.0, 0.0),
        ('under_3.5', 'Under 3.5 Goals', model_probs.get('under_3.5'), bookmaker_odds.get('under_3.5'), 1.0, 0.0),
    ]
    
    for market_key, market_name, model_prob, odds, injury_factor, hist_adj in markets:
        if model_prob is None:
            continue
        
        # Apply historical adjustment to model probability
        adjusted_model_prob = min(max(model_prob + hist_adj, 0.01), 0.99)
        
        # Calculate implied probability from odds
        implied_prob = calc.odds_to_probability(odds) if odds else None
        
        # Calculate edge using adjusted probability
        is_value, edge = calc.is_value_bet(adjusted_model_prob, implied_prob) if implied_prob else (False, 0)
        
        # Get confidence level with injury adjustment
        confidence, adjusted_prob = calc.get_confidence_level(adjusted_model_prob, edge, injury_factor)
        
        # Apply confidence boost from historical data
        adjusted_prob = min(adjusted_prob + confidence_boost, 0.99)
        
        # Calculate Kelly stake
        kelly_stake = calc.kelly_criterion(adjusted_prob, odds) if odds else calc.kelly_criterion(adjusted_prob, 1/model_prob)
        
        # Get risk rating
        risk = calc.get_risk_rating(adjusted_prob, edge)
        
        # Only recommend if probability is decent or has value
        if adjusted_prob >= 0.55 or is_value:
            analysis['recommendations'].append({
                'market': market_name,
                'market_key': market_key,
                'model_probability': model_prob,
                'adjusted_probability': adjusted_prob,
                'bookmaker_odds': odds,
                'implied_probability': implied_prob,
                'edge': edge,
                'is_value_bet': is_value,
                'confidence': confidence,
                'risk': risk,
                'kelly_stake': kelly_stake,
                'injury_factor': injury_factor,
                'historical_adjustment': hist_adj  # New: track adjustment applied
            })
    
    # Sort recommendations by edge/probability
    analysis['recommendations'].sort(key=lambda x: (x['edge'], x['adjusted_probability']), reverse=True)
    
    return analysis


def get_top_bets(merged_data: List[Dict], top_n: int = 10, min_odds: float = 1.3) -> List[Dict]:
    """Get top N best betting opportunities from merged data
    
    Args:
        merged_data: List of merged match records
        top_n: Number of top bets to return
        min_odds: Minimum odds to include (default 1.3, bets below this are filtered out)
    """
    all_bets = []
    
    for record in merged_data:
        analysis = analyze_merged_match(record)
        
        for rec in analysis.get('recommendations', []):
            # Get the odds for this bet
            odds = rec.get('bookmaker_odds')
            
            # Filter: skip bets without odds or with odds < min_odds
            if odds is None or odds < min_odds:
                continue
            
            bet = {
                'match': analysis['match'],
                'league': analysis['league'],
                'date': analysis['date'],
                'odds': odds,
                **rec
            }
            all_bets.append(bet)
    
    # Sort by edge first, then probability
    all_bets.sort(key=lambda x: (x.get('edge', 0), x.get('adjusted_probability', 0)), reverse=True)
    
    return all_bets[:top_n]


def generate_accumulators(merged_data: List[Dict], legs: int = 4, min_prob: float = 0.65, min_odds: float = 1.3) -> List[Dict]:
    """Generate accumulator suggestions
    
    Args:
        merged_data: List of merged match records
        legs: Number of legs in the accumulator
        min_prob: Minimum probability for bets
        min_odds: Minimum odds to include (default 1.3)
    """
    all_bets = []
    
    for record in merged_data:
        analysis = analyze_merged_match(record)
        
        for rec in analysis.get('recommendations', []):
            # Get odds for this bet
            odds = rec.get('bookmaker_odds')
            
            # Filter: skip bets without odds or with odds < min_odds
            if odds is None or odds < min_odds:
                continue
                
            if rec.get('adjusted_probability', 0) >= min_prob:
                all_bets.append({
                    'match': analysis['match'],
                    'league': analysis['league'],
                    'date': analysis['date'],
                    'odds': odds,
                    **rec
                })
    
    # Sort by probability
    all_bets.sort(key=lambda x: x.get('adjusted_probability', 0), reverse=True)
    
    accumulators = []
    
    # Safe accumulator
    if len(all_bets) >= legs:
        safe_legs = all_bets[:legs]
        combined_prob = np.prod([b['adjusted_probability'] for b in safe_legs])
        
        # Calculate combined odds
        combined_odds = 1.0
        for b in safe_legs:
            if b.get('bookmaker_odds'):
                combined_odds *= b['bookmaker_odds']
            else:
                combined_odds *= (1 / b['adjusted_probability'])
        
        accumulators.append({
            'type': 'SAFE ACCUMULATOR',
            'legs': safe_legs,
            'combined_probability': combined_prob,
            'combined_odds': combined_odds,
            'expected_value': combined_prob * combined_odds
        })
    
    # Value accumulator (best edges)
    value_bets = sorted(all_bets, key=lambda x: x.get('edge', 0), reverse=True)
    if len(value_bets) >= legs:
        value_legs = value_bets[:legs]
        combined_prob = np.prod([b['adjusted_probability'] for b in value_legs])
        
        combined_odds = 1.0
        for b in value_legs:
            if b.get('bookmaker_odds'):
                combined_odds *= b['bookmaker_odds']
            else:
                combined_odds *= (1 / b['adjusted_probability'])
        
        accumulators.append({
            'type': 'VALUE ACCUMULATOR',
            'legs': value_legs,
            'combined_probability': combined_prob,
            'combined_odds': combined_odds,
            'expected_value': combined_prob * combined_odds
        })
    
    return accumulators
