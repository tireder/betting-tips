"""
API-Football V3 Full Integration
Elite Sports Betting Intelligence Engine
"""

import requests
import streamlit as st
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any
import time

# Winner Israeli Premier League - Selected Leagues
WINNER_LEAGUES = {
    # Top European Leagues
    39: "England - Premier League",
    40: "England - Championship",
    41: "England - League One",
    42: "England - League Two",
    140: "Spain - La Liga",
    141: "Spain - La Liga 2",
    78: "Germany - Bundesliga",
    79: "Germany - 2. Bundesliga",
    135: "Italy - Serie A",
    136: "Italy - Serie B",
    61: "France - Ligue 1",
    62: "France - Ligue 2",
    94: "Portugal - Primeira Liga",
    88: "Netherlands - Eredivisie",
    89: "Netherlands - Eerste Divisie",
    144: "Belgium - Jupiler Pro League",
    145: "Belgium - First Division B",
    203: "Turkey - Süper Lig",
    
    # UEFA Competitions
    2: "UEFA - Champions League",
    3: "UEFA - Europa League",
    848: "UEFA - Conference League",
    4: "UEFA - Euro Championship",
    1: "FIFA - World Cup",
    
    # South American
    71: "Brazil - Serie A",
    72: "Brazil - Serie B",
    128: "Argentina -  Liga Profesional Argentina",
    129: "Argentina - Primera Nacional",
    
    # Other European
    179: "Scotland - Premiership",
    180: "Scotland - Championship",
    197: "Greece - Super League",
    283: "Romania - Liga I",
    207: "Switzerland - Super League",
    208: "Switzerland - Challenge League",
    218: "Austria - Bundesliga",
    219: "Austria - 2. Liga",
    235: "Russia - Premier League",
    253: "USA - MLS",
    383: "Israel - Ligat Ha'al",
    
    # Scandinavian
    103: "Norway - Eliteserien",
    104: "Norway - 1. Division",
    119: "Denmark - Superliga",
    120: "Denmark - 1st Division",
    113: "Sweden - Allsvenskan",
    
    # Eastern European
    106: "Poland - Ekstraklasa",
    107: "Poland - I Liga",
    345: "Czech Republic - First League",
    332: "Slovakia - Super Liga",
    333: "Ukraine - Premier League",
    
    # Asian Leagues
    292: "South Korea - K League 1",
    98: "Japan - J1 League",
    99: "Japan - J2 League",
    307: "Saudi Arabia - Pro League",
    
    # Israel
    383: "Israel - Ligat Ha'al",
    382: "Israel - Leumit",
    384: "Israel - State Cup",
    385: "Israel - Toto Cup Ligat Al",
}


class APIFootball:
    """
    Comprehensive API-Football V3 Client
    Full endpoint coverage for betting analysis
    """
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://v3.football.api-sports.io"
        self.headers = {
            "x-rapidapi-host": "v3.football.api-sports.io",
            "x-rapidapi-key": api_key
        }
        self.rate_limit_remaining = 100
        self.rate_limit_reset = None
        
    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """Make API request with rate limiting and error handling"""
        url = f"{self.base_url}/{endpoint}"
        
        try:
            response = requests.get(url, headers=self.headers, params=params, timeout=30)
            
            # Track rate limits
            if 'x-ratelimit-remaining' in response.headers:
                self.rate_limit_remaining = int(response.headers.get('x-ratelimit-remaining', 100))
            if 'x-ratelimit-reset' in response.headers:
                self.rate_limit_reset = response.headers.get('x-ratelimit-reset')
                
            if response.status_code == 200:
                data = response.json()
                if data.get('errors') and len(data['errors']) > 0:
                    st.warning(f"API Warning: {data['errors']}")
                return data
            elif response.status_code == 429:
                st.error("Rate limit exceeded. Please wait...")
                time.sleep(60)
                return self._make_request(endpoint, params)
            else:
                st.error(f"API Error: {response.status_code}")
                return {"response": [], "errors": {"code": response.status_code}}
                
        except requests.exceptions.Timeout:
            st.error("Request timeout - API server slow")
            return {"response": [], "errors": {"timeout": True}}
        except requests.exceptions.RequestException as e:
            st.error(f"Request failed: {str(e)}")
            return {"response": [], "errors": {"exception": str(e)}}
    
    # ============================================================
    # GENERAL ENDPOINTS
    # ============================================================
    
    def get_status(self) -> Dict:
        """Get account status and remaining requests"""
        return self._make_request("status")
    
    def get_timezones(self) -> List[str]:
        """Get available timezones"""
        data = self._make_request("timezone")
        return data.get("response", [])
    
    def get_countries(self, name: Optional[str] = None, code: Optional[str] = None) -> List[Dict]:
        """Get countries list"""
        params = {}
        if name:
            params["name"] = name
        if code:
            params["code"] = code
        data = self._make_request("countries", params)
        return data.get("response", [])
    
    # ============================================================
    # LEAGUES ENDPOINTS
    # ============================================================
    
    def get_leagues(self, 
                   id: Optional[int] = None,
                   name: Optional[str] = None,
                   country: Optional[str] = None,
                   code: Optional[str] = None,
                   season: Optional[int] = None,
                   team: Optional[int] = None,
                   type: Optional[str] = None,
                   current: Optional[str] = None,
                   search: Optional[str] = None) -> List[Dict]:
        """
        Get leagues with various filters
        type: 'league' or 'cup'
        current: 'true' or 'false'
        """
        params = {}
        if id:
            params["id"] = id
        if name:
            params["name"] = name
        if country:
            params["country"] = country
        if code:
            params["code"] = code
        if season:
            params["season"] = season
        if team:
            params["team"] = team
        if type:
            params["type"] = type
        if current:
            params["current"] = current
        if search:
            params["search"] = search
            
        data = self._make_request("leagues", params)
        return data.get("response", [])
    
    def get_league_seasons(self) -> List[int]:
        """Get all available seasons"""
        data = self._make_request("leagues/seasons")
        return data.get("response", [])
    
    # ============================================================
    # TEAMS ENDPOINTS
    # ============================================================
    
    def get_teams(self,
                 id: Optional[int] = None,
                 name: Optional[str] = None,
                 league: Optional[int] = None,
                 season: Optional[int] = None,
                 country: Optional[str] = None,
                 code: Optional[str] = None,
                 venue: Optional[int] = None,
                 search: Optional[str] = None) -> List[Dict]:
        """Get teams with filters"""
        params = {}
        if id:
            params["id"] = id
        if name:
            params["name"] = name
        if league:
            params["league"] = league
        if season:
            params["season"] = season
        if country:
            params["country"] = country
        if code:
            params["code"] = code
        if venue:
            params["venue"] = venue
        if search:
            params["search"] = search
            
        data = self._make_request("teams", params)
        return data.get("response", [])
    
    def get_team_statistics(self,
                           league: int,
                           season: int,
                           team: int,
                           date: Optional[str] = None) -> Dict:
        """
        Get comprehensive team statistics for a season
        Returns: form, fixtures, goals, clean sheets, penalties, lineups, cards
        """
        params = {
            "league": league,
            "season": season,
            "team": team
        }
        if date:
            params["date"] = date
            
        data = self._make_request("teams/statistics", params)
        return data.get("response", {})
    
    def get_team_seasons(self, team: int) -> List[int]:
        """Get all seasons for a specific team"""
        data = self._make_request("teams/seasons", {"team": team})
        return data.get("response", [])
    
    def get_team_countries(self) -> List[Dict]:
        """Get available countries for teams"""
        data = self._make_request("teams/countries")
        return data.get("response", [])
    
    # ============================================================
    # VENUES ENDPOINTS
    # ============================================================
    
    def get_venues(self,
                  id: Optional[int] = None,
                  name: Optional[str] = None,
                  city: Optional[str] = None,
                  country: Optional[str] = None,
                  search: Optional[str] = None) -> List[Dict]:
        """Get venues information"""
        params = {}
        if id:
            params["id"] = id
        if name:
            params["name"] = name
        if city:
            params["city"] = city
        if country:
            params["country"] = country
        if search:
            params["search"] = search
            
        data = self._make_request("venues", params)
        return data.get("response", [])
    
    # ============================================================
    # STANDINGS ENDPOINTS
    # ============================================================
    
    def get_standings(self,
                     league: int,
                     season: int,
                     team: Optional[int] = None) -> List[Dict]:
        """
        Get league standings/table
        Returns: rank, points, goals, form, win/draw/loss counts
        """
        params = {
            "league": league,
            "season": season
        }
        if team:
            params["team"] = team
            
        data = self._make_request("standings", params)
        return data.get("response", [])
    
    # ============================================================
    # FIXTURES ENDPOINTS
    # ============================================================
    
    def get_fixtures(self,
                    id: Optional[int] = None,
                    ids: Optional[str] = None,
                    live: Optional[str] = None,
                    date: Optional[str] = None,
                    league: Optional[int] = None,
                    season: Optional[int] = None,
                    team: Optional[int] = None,
                    last: Optional[int] = None,
                    next: Optional[int] = None,
                    from_date: Optional[str] = None,
                    to_date: Optional[str] = None,
                    round: Optional[str] = None,
                    status: Optional[str] = None,
                    venue: Optional[int] = None,
                    timezone: Optional[str] = None) -> List[Dict]:
        """
        Get fixtures with comprehensive filters
        live: 'all' for all live, or specific league ids 'id-id'
        status: NS, 1H, HT, 2H, ET, P, FT, AET, PEN, BT, SUSP, INT, PST, CANC, ABD, AWD, WO, LIVE
        """
        params = {}
        if id:
            params["id"] = id
        if ids:
            params["ids"] = ids
        if live:
            params["live"] = live
        if date:
            params["date"] = date
        if league:
            params["league"] = league
        if season:
            params["season"] = season
        if team:
            params["team"] = team
        if last:
            params["last"] = last
        if next:
            params["next"] = next
        if from_date:
            params["from"] = from_date
        if to_date:
            params["to"] = to_date
        if round:
            params["round"] = round
        if status:
            params["status"] = status
        if venue:
            params["venue"] = venue
        if timezone:
            params["timezone"] = timezone
            
        data = self._make_request("fixtures", params)
        return data.get("response", [])
    
    def get_fixtures_by_date(self, date: str, league_ids: Optional[List[int]] = None) -> List[Dict]:
        """
        Get fixtures for a specific date, optionally filtered by leagues
        date format: YYYY-MM-DD
        """
        fixtures = []
        
        if league_ids:
            for league_id in league_ids:
                league_fixtures = self.get_fixtures(date=date, league=league_id)
                fixtures.extend(league_fixtures)
        else:
            fixtures = self.get_fixtures(date=date)
            
        return fixtures
    
    def get_rounds(self, league: int, season: int, current: Optional[bool] = None) -> List[str]:
        """Get available rounds for a league"""
        params = {
            "league": league,
            "season": season
        }
        if current is not None:
            params["current"] = "true" if current else "false"
            
        data = self._make_request("fixtures/rounds", params)
        return data.get("response", [])
    
    def get_head_to_head(self,
                        h2h: str,
                        date: Optional[str] = None,
                        league: Optional[int] = None,
                        season: Optional[int] = None,
                        last: Optional[int] = None,
                        next: Optional[int] = None,
                        from_date: Optional[str] = None,
                        to_date: Optional[str] = None,
                        status: Optional[str] = None,
                        venue: Optional[int] = None,
                        timezone: Optional[str] = None) -> List[Dict]:
        """
        Get head-to-head fixtures between two teams
        h2h format: 'team1_id-team2_id'
        """
        params = {"h2h": h2h}
        if date:
            params["date"] = date
        if league:
            params["league"] = league
        if season:
            params["season"] = season
        if last:
            params["last"] = last
        if next:
            params["next"] = next
        if from_date:
            params["from"] = from_date
        if to_date:
            params["to"] = to_date
        if status:
            params["status"] = status
        if venue:
            params["venue"] = venue
        if timezone:
            params["timezone"] = timezone
            
        data = self._make_request("fixtures/headtohead", params)
        return data.get("response", [])
    
    def get_h2h(self, team1_id: int, team2_id: int, last: int = 10) -> List[Dict]:
        """Convenience wrapper for head-to-head"""
        return self.get_head_to_head(h2h=f"{team1_id}-{team2_id}", last=last)
    
    def get_fixture_statistics(self, fixture: int, team: Optional[int] = None, type: Optional[str] = None) -> List[Dict]:
        """
        Get statistics for a specific fixture
        type: Shots on Goal, Shots off Goal, Total Shots, Blocked Shots, etc.
        """
        params = {"fixture": fixture}
        if team:
            params["team"] = team
        if type:
            params["type"] = type
            
        data = self._make_request("fixtures/statistics", params)
        return data.get("response", [])
    
    def get_fixture_events(self,
                          fixture: int,
                          team: Optional[int] = None,
                          player: Optional[int] = None,
                          type: Optional[str] = None) -> List[Dict]:
        """
        Get events (goals, cards, subs) for a fixture
        type: Goal, Card, subst, Var
        """
        params = {"fixture": fixture}
        if team:
            params["team"] = team
        if player:
            params["player"] = player
        if type:
            params["type"] = type
            
        data = self._make_request("fixtures/events", params)
        return data.get("response", [])
    
    def get_lineups(self, fixture: int, team: Optional[int] = None, player: Optional[int] = None) -> List[Dict]:
        """Get lineups for a fixture"""
        params = {"fixture": fixture}
        if team:
            params["team"] = team
        if player:
            params["player"] = player
            
        data = self._make_request("fixtures/lineups", params)
        return data.get("response", [])
    
    def get_fixture_players(self, fixture: int, team: Optional[int] = None) -> List[Dict]:
        """Get player statistics for a specific fixture"""
        params = {"fixture": fixture}
        if team:
            params["team"] = team
            
        data = self._make_request("fixtures/players", params)
        return data.get("response", [])
    
    # ============================================================
    # PLAYERS ENDPOINTS
    # ============================================================
    
    def get_players(self,
                   id: Optional[int] = None,
                   team: Optional[int] = None,
                   league: Optional[int] = None,
                   season: Optional[int] = None,
                   search: Optional[str] = None,
                   page: Optional[int] = None) -> Dict:
        """Get players with statistics"""
        params = {}
        if id:
            params["id"] = id
        if team:
            params["team"] = team
        if league:
            params["league"] = league
        if season:
            params["season"] = season
        if search:
            params["search"] = search
        if page:
            params["page"] = page
            
        data = self._make_request("players", params)
        return data
    
    def get_player_seasons(self, player: Optional[int] = None) -> List[int]:
        """Get available seasons for players"""
        params = {}
        if player:
            params["player"] = player
            
        data = self._make_request("players/seasons", params)
        return data.get("response", [])
    
    def get_squads(self, team: Optional[int] = None, player: Optional[int] = None) -> List[Dict]:
        """Get current squad for a team or teams for a player"""
        params = {}
        if team:
            params["team"] = team
        if player:
            params["player"] = player
            
        data = self._make_request("players/squads", params)
        return data.get("response", [])
    
    def get_top_scorers(self, league: int, season: int) -> List[Dict]:
        """Get top scorers for a league/season"""
        data = self._make_request("players/topscorers", {"league": league, "season": season})
        return data.get("response", [])
    
    def get_top_assists(self, league: int, season: int) -> List[Dict]:
        """Get top assists for a league/season"""
        data = self._make_request("players/topassists", {"league": league, "season": season})
        return data.get("response", [])
    
    def get_top_yellow_cards(self, league: int, season: int) -> List[Dict]:
        """Get players with most yellow cards"""
        data = self._make_request("players/topyellowcards", {"league": league, "season": season})
        return data.get("response", [])
    
    def get_top_red_cards(self, league: int, season: int) -> List[Dict]:
        """Get players with most red cards"""
        data = self._make_request("players/topredcards", {"league": league, "season": season})
        return data.get("response", [])
    
    # ============================================================
    # TRANSFERS ENDPOINTS
    # ============================================================
    
    def get_transfers(self, player: Optional[int] = None, team: Optional[int] = None) -> List[Dict]:
        """Get transfer history"""
        params = {}
        if player:
            params["player"] = player
        if team:
            params["team"] = team
            
        data = self._make_request("transfers", params)
        return data.get("response", [])
    
    # ============================================================
    # TROPHIES ENDPOINTS
    # ============================================================
    
    def get_trophies(self, player: Optional[int] = None, coach: Optional[int] = None) -> List[Dict]:
        """Get trophies won by player or coach"""
        params = {}
        if player:
            params["player"] = player
        if coach:
            params["coach"] = coach
            
        data = self._make_request("trophies", params)
        return data.get("response", [])
    
    # ============================================================
    # SIDELINED/INJURIES ENDPOINTS
    # ============================================================
    
    def get_sidelined(self, player: Optional[int] = None, coach: Optional[int] = None) -> List[Dict]:
        """Get sidelined (injured/suspended) status"""
        params = {}
        if player:
            params["player"] = player
        if coach:
            params["coach"] = coach
            
        data = self._make_request("sidelined", params)
        return data.get("response", [])
    
    def get_injuries(self,
                    league: Optional[int] = None,
                    season: Optional[int] = None,
                    fixture: Optional[int] = None,
                    team: Optional[int] = None,
                    player: Optional[int] = None,
                    date: Optional[str] = None,
                    timezone: Optional[str] = None) -> List[Dict]:
        """
        Get current injuries
        At least one parameter required
        """
        params = {}
        if league:
            params["league"] = league
        if season:
            params["season"] = season
        if fixture:
            params["fixture"] = fixture
        if team:
            params["team"] = team
        if player:
            params["player"] = player
        if date:
            params["date"] = date
        if timezone:
            params["timezone"] = timezone
            
        data = self._make_request("injuries", params)
        return data.get("response", [])
    
    # ============================================================
    # PREDICTIONS ENDPOINTS
    # ============================================================
    
    def get_predictions(self, fixture: int) -> Dict:
        """
        Get AI predictions for a fixture
        Returns: winner, percent, goals, advice, form comparison
        """
        data = self._make_request("predictions", {"fixture": fixture})
        response = data.get("response", [])
        return response[0] if response else {}
    
    # ============================================================
    # COACHS ENDPOINTS
    # ============================================================
    
    def get_coachs(self,
                  id: Optional[int] = None,
                  team: Optional[int] = None,
                  search: Optional[str] = None) -> List[Dict]:
        """Get coach information"""
        params = {}
        if id:
            params["id"] = id
        if team:
            params["team"] = team
        if search:
            params["search"] = search
            
        data = self._make_request("coachs", params)
        return data.get("response", [])
    
    # ============================================================
    # ODDS ENDPOINTS
    # ============================================================
    
    def get_odds(self,
                fixture: Optional[int] = None,
                league: Optional[int] = None,
                season: Optional[int] = None,
                date: Optional[str] = None,
                timezone: Optional[str] = None,
                page: Optional[int] = None,
                bookmaker: Optional[int] = None,
                bet: Optional[int] = None) -> Dict:
        """
        Get betting odds
        Common bet types: 1=Match Winner, 2=Home/Away, 3=Second Half Winner, 
        5=Goals Over/Under, 6=Goals Over/Under First Half, etc.
        """
        params = {}
        if fixture:
            params["fixture"] = fixture
        if league:
            params["league"] = league
        if season:
            params["season"] = season
        if date:
            params["date"] = date
        if timezone:
            params["timezone"] = timezone
        if page:
            params["page"] = page
        if bookmaker:
            params["bookmaker"] = bookmaker
        if bet:
            params["bet"] = bet
            
        data = self._make_request("odds", params)
        return data
    
    def get_odds_mapping(self) -> List[Dict]:
        """Get mapping between fixtures and available odds"""
        data = self._make_request("odds/mapping")
        return data.get("response", [])
    
    def get_bookmakers(self, id: Optional[int] = None, search: Optional[str] = None) -> List[Dict]:
        """Get available bookmakers"""
        params = {}
        if id:
            params["id"] = id
        if search:
            params["search"] = search
            
        data = self._make_request("odds/bookmakers", params)
        return data.get("response", [])
    
    def get_bet_types(self, id: Optional[int] = None, search: Optional[str] = None) -> List[Dict]:
        """Get available bet types/markets"""
        params = {}
        if id:
            params["id"] = id
        if search:
            params["search"] = search
            
        data = self._make_request("odds/bets", params)
        return data.get("response", [])
    
    def get_odds_live(self, fixture: Optional[int] = None, league: Optional[int] = None, bet: Optional[int] = None) -> List[Dict]:
        """Get live in-play odds"""
        params = {}
        if fixture:
            params["fixture"] = fixture
        if league:
            params["league"] = league
        if bet:
            params["bet"] = bet
            
        data = self._make_request("odds/live", params)
        return data.get("response", [])
    
    def get_odds_live_bets(self, id: Optional[int] = None, search: Optional[str] = None) -> List[Dict]:
        """Get available live bet types"""
        params = {}
        if id:
            params["id"] = id
        if search:
            params["search"] = search
            
        data = self._make_request("odds/live/bets", params)
        return data.get("response", [])
    
    # ============================================================
    # LIVE SCORES
    # ============================================================
    
    def get_live_scores(self, league_ids: Optional[List[int]] = None) -> List[Dict]:
        """Get all live fixtures"""
        if league_ids:
            live_param = "-".join(str(lid) for lid in league_ids)
        else:
            live_param = "all"
            
        return self.get_fixtures(live=live_param)
    
    # ============================================================
    # CONVENIENCE METHODS FOR BETTING ANALYSIS
    # ============================================================
    
    def get_full_match_data(self, fixture_id: int) -> Dict:
        """
        Get comprehensive data for a single fixture
        Combines fixture info, statistics, lineups, predictions, odds
        """
        fixture_data = self.get_fixtures(id=fixture_id)
        fixture = fixture_data[0] if fixture_data else {}
        
        if not fixture:
            return {}
            
        # Get additional data - wrap in try/except to handle API plan limitations
        lineups = []
        statistics = []
        events = []
        predictions = {}
        odds_response = []
        h2h = []
        
        try:
            lineups = self.get_lineups(fixture=fixture_id)
        except:
            pass
            
        try:
            statistics = self.get_fixture_statistics(fixture=fixture_id)
        except:
            pass
            
        try:
            events = self.get_fixture_events(fixture=fixture_id)
        except:
            pass
            
        try:
            predictions = self.get_predictions(fixture=fixture_id)
        except:
            pass
            
        try:
            odds_data = self.get_odds(fixture=fixture_id)
            odds_response = odds_data.get("response", [])
        except:
            pass
        
        # Extract teams for h2h
        try:
            home_id = fixture.get('teams', {}).get('home', {}).get('id')
            away_id = fixture.get('teams', {}).get('away', {}).get('id')
            if home_id and away_id:
                h2h = self.get_h2h(home_id, away_id, last=10)
        except:
            pass
        
        return {
            "fixture": fixture,
            "lineups": lineups,
            "statistics": statistics,
            "events": events,
            "predictions": predictions,
            "odds": odds_response,
            "h2h": h2h
        }
    
    def get_team_form(self, team_id: int, last: int = 5) -> List[Dict]:
        """Get last N matches for a team"""
        return self.get_fixtures(team=team_id, last=last)
    
    def get_upcoming_fixtures(self, team_id: int, next_count: int = 5) -> List[Dict]:
        """Get next N fixtures for a team"""
        return self.get_fixtures(team=team_id, next=next_count)
    
    def analyze_h2h_stats(self, team1_id: int, team2_id: int, last: int = 10) -> Dict:
        """
        Analyze head-to-head statistics
        Returns: wins, draws, goals, recent form
        """
        h2h_matches = self.get_h2h(team1_id, team2_id, last)
        
        stats = {
            "total_matches": len(h2h_matches),
            "team1_wins": 0,
            "team2_wins": 0,
            "draws": 0,
            "team1_goals": 0,
            "team2_goals": 0,
            "both_scored": 0,
            "over_25": 0,
            "matches": []
        }
        
        for match in h2h_matches:
            home = match.get('teams', {}).get('home', {})
            away = match.get('teams', {}).get('away', {})
            goals = match.get('goals', {})
            
            home_goals = goals.get('home', 0) or 0
            away_goals = goals.get('away', 0) or 0
            total_goals = home_goals + away_goals
            
            # Determine winner
            if home.get('winner'):
                if home.get('id') == team1_id:
                    stats['team1_wins'] += 1
                else:
                    stats['team2_wins'] += 1
            elif away.get('winner'):
                if away.get('id') == team1_id:
                    stats['team1_wins'] += 1
                else:
                    stats['team2_wins'] += 1
            else:
                stats['draws'] += 1
            
            # Goals stats
            if home.get('id') == team1_id:
                stats['team1_goals'] += home_goals
                stats['team2_goals'] += away_goals
            else:
                stats['team1_goals'] += away_goals
                stats['team2_goals'] += home_goals
            
            # Both scored
            if home_goals > 0 and away_goals > 0:
                stats['both_scored'] += 1
                
            # Over 2.5
            if total_goals > 2.5:
                stats['over_25'] += 1
                
            stats['matches'].append({
                'date': match.get('fixture', {}).get('date'),
                'home': home.get('name'),
                'away': away.get('name'),
                'score': f"{home_goals}-{away_goals}"
            })
        
        return stats
    
    def get_league_top_stats(self, league: int, season: int) -> Dict:
        """Get league top statistics (scorers, assists, cards)"""
        return {
            "top_scorers": self.get_top_scorers(league, season)[:10],
            "top_assists": self.get_top_assists(league, season)[:10],
            "top_yellow": self.get_top_yellow_cards(league, season)[:10],
            "top_red": self.get_top_red_cards(league, season)[:5]
        }


def fetch_all_winner_fixtures(api: APIFootball, date: str) -> List[Dict]:
    """
    Fetch fixtures for all Winner leagues on a given date.
    Tries both current and previous season to catch all active leagues.
    """
    all_fixtures = []
    league_ids = list(set(WINNER_LEAGUES.keys()))  # Remove duplicates
    seen_fixture_ids = set()
    
    # Dynamic season detection based on the fixture date
    try:
        fixture_date = datetime.strptime(date, "%Y-%m-%d")
    except:
        fixture_date = datetime.now()
    
    # European leagues: season starts in Aug, so Aug-Dec = current year, Jan-Jul = previous year
    # But we also have calendar-year leagues (MLS, etc.)
    current_year = fixture_date.year
    if fixture_date.month >= 8:
        # Aug-Dec: European season is current year
        seasons_to_try = [current_year]
    else:
        # Jan-Jul: European season started previous year, but calendar leagues use current year
        seasons_to_try = [current_year - 1, current_year]
    
    progress = st.progress(0, text="Fetching fixtures from API-Football V3...")
    total_steps = len(league_ids) * len(seasons_to_try)
    step = 0
    
    for season in seasons_to_try:
        for league_id in league_ids:
            step += 1
            try:
                fixtures = api.get_fixtures(date=date, league=league_id, season=season)
                for fix in fixtures:
                    fix_id = fix.get('fixture', {}).get('id')
                    if fix_id and fix_id not in seen_fixture_ids:
                        seen_fixture_ids.add(fix_id)
                        all_fixtures.append(fix)
            except Exception as e:
                pass  # Skip errors for individual leagues
            
            progress.progress(step / total_steps, text=f"Fetching {WINNER_LEAGUES.get(league_id, 'League')} ({season})...")
            time.sleep(0.05)  # Rate limiting
    
    progress.empty()
    
    # If we got very few fixtures, try fetching ALL fixtures for the date (no league filter)
    if len(all_fixtures) < 20:
        st.info("🔄 Fetching additional fixtures...")
        try:
            all_date_fixtures = api.get_fixtures(date=date)
            for fix in all_date_fixtures:
                fix_id = fix.get('fixture', {}).get('id')
                if fix_id and fix_id not in seen_fixture_ids:
                    seen_fixture_ids.add(fix_id)
                    all_fixtures.append(fix)
        except:
            pass
    
    return all_fixtures


def get_current_season() -> int:
    """Get current football season year"""
    today = datetime.now()
    # Football season typically starts in August
    if today.month >= 8:
        return today.year
    return today.year - 1


# Export for imports
__all__ = [
    'APIFootball',
    'WINNER_LEAGUES',
    'fetch_all_winner_fixtures',
    'get_current_season'
]
