"""
Betting Panel Formatter
Generates formatted betting panel output for matches
"""

from datetime import datetime
from typing import Dict, List, Optional
import pandas as pd


def format_betting_panel(analysis: Dict) -> str:
    """
    Format a single match analysis into betting panel format
    
    Example output:
    ### 📅 Sat 30 Nov 2024, 15:00 — Premier League
    ### ⚽ Manchester United vs Chelsea
    
    **Model Probabilities**
    - Home Win: 45.2%
    - Draw: 28.1%
    - Away Win: 26.7%
    
    **Bookmaker Odds**
    - Home: 2.10
    - Draw: 3.40
    - Away: 3.50
    
    **Injuries**
    - Home missing: Rashford, Shaw
    - Away missing: None
    
    **Recommended Bet**
    🎯 Home Win
    Confidence: MEDIUM
    Probability: 45.2%
    Value Rating: 7.3% edge
    Suggested Stake: 2.1%
    """
    
    # Parse date
    date_str = analysis.get('date', 'TBD')
    if isinstance(date_str, datetime):
        formatted_date = date_str.strftime('%a %d %b %Y, %H:%M')
    elif isinstance(date_str, str) and 'T' in date_str:
        try:
            dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            formatted_date = dt.strftime('%a %d %b %Y, %H:%M')
        except:
            formatted_date = date_str
    else:
        formatted_date = str(date_str)
    
    # Build panel
    panel = []
    panel.append(f"### 📅 {formatted_date} — {analysis.get('league', 'Unknown League')}")
    panel.append(f"### ⚽ {analysis.get('match', 'Unknown Match')}")
    panel.append("")
    
    # Model Probabilities
    model_probs = analysis.get('model_probs', {})
    panel.append("**Model Probabilities**")
    panel.append(f"- Home Win: {format_prob(model_probs.get('home_win'))}")
    panel.append(f"- Draw: {format_prob(model_probs.get('draw'))}")
    panel.append(f"- Away Win: {format_prob(model_probs.get('away_win'))}")
    panel.append("")
    
    # Bookmaker Odds
    bm_odds = analysis.get('bookmaker_odds', {})
    if bm_odds and any(v for k, v in bm_odds.items() if k != 'raw_odds' and k != 'bookmaker'):
        panel.append("**📊 Bookmaker Odds**")
        panel.append(f"| Market | Odds |")
        panel.append(f"|--------|------|")
        if bm_odds.get('home_win'):
            panel.append(f"| Home Win | {format_odds(bm_odds.get('home_win'))} |")
        if bm_odds.get('draw'):
            panel.append(f"| Draw | {format_odds(bm_odds.get('draw'))} |")
        if bm_odds.get('away_win'):
            panel.append(f"| Away Win | {format_odds(bm_odds.get('away_win'))} |")
        if bm_odds.get('over_1.5'):
            panel.append(f"| Over 1.5 | {format_odds(bm_odds.get('over_1.5'))} |")
        if bm_odds.get('under_1.5'):
            panel.append(f"| Under 1.5 | {format_odds(bm_odds.get('under_1.5'))} |")
        if bm_odds.get('over_2.5'):
            panel.append(f"| Over 2.5 | {format_odds(bm_odds.get('over_2.5'))} |")
        if bm_odds.get('under_2.5'):
            panel.append(f"| Under 2.5 | {format_odds(bm_odds.get('under_2.5'))} |")
        if bm_odds.get('over_3.5'):
            panel.append(f"| Over 3.5 | {format_odds(bm_odds.get('over_3.5'))} |")
        if bm_odds.get('under_3.5'):
            panel.append(f"| Under 3.5 | {format_odds(bm_odds.get('under_3.5'))} |")
        if bm_odds.get('btts_yes'):
            panel.append(f"| BTTS Yes | {format_odds(bm_odds.get('btts_yes'))} |")
        if bm_odds.get('btts_no'):
            panel.append(f"| BTTS No | {format_odds(bm_odds.get('btts_no'))} |")
        if bm_odds.get('bookmaker'):
            panel.append(f"")
            panel.append(f"*via {bm_odds.get('bookmaker')}*")
        panel.append("")
    else:
        panel.append("**📊 Bookmaker Odds**")
        panel.append("- *No live odds available*")
        panel.append("")
    
    # Injuries
    injuries = analysis.get('injuries', {})
    home_injuries = injuries.get('home', [])
    away_injuries = injuries.get('away', [])
    
    panel.append("**Injuries**")
    if home_injuries:
        names = [i.get('player', 'Unknown') for i in home_injuries[:5]]
        panel.append(f"- Home missing: {', '.join(names)}")
    else:
        panel.append("- Home missing: None reported")
    
    if away_injuries:
        names = [i.get('player', 'Unknown') for i in away_injuries[:5]]
        panel.append(f"- Away missing: {', '.join(names)}")
    else:
        panel.append("- Away missing: None reported")
    panel.append("")
    
    # H2H Summary
    h2h = analysis.get('h2h', {})
    # Handle both list format (raw API) and dict format (analyzed)
    if isinstance(h2h, list) and h2h:
        # Raw API format - it's a list of matches
        panel.append("**Head-to-Head (Recent Matches)**")
        panel.append(f"- Last {len(h2h)} meetings available")
        panel.append("")
    elif isinstance(h2h, dict) and h2h.get('matches'):
        panel.append("**Head-to-Head (Last 10)**")
        panel.append(f"- Home Wins: {h2h.get('team1_wins', 0)}")
        panel.append(f"- Draws: {h2h.get('draws', 0)}")
        panel.append(f"- Away Wins: {h2h.get('team2_wins', 0)}")
        panel.append(f"- Avg Goals: {h2h.get('avg_goals', 0)}")
        panel.append("")
    
    # Historical Insights (from cache)
    historical_insights = analysis.get('historical_insights', [])
    if historical_insights:
        panel.append("**📈 Historical Insights**")
        for insight in historical_insights[:4]:  # Max 4 insights
            panel.append(f"- {insight}")
        panel.append("")
    
    # Recommendations
    recommendations = analysis.get('recommendations', [])
    if recommendations:
        best_rec = recommendations[0]  # Top recommendation
        
        panel.append("**Recommended Bet**")
        panel.append(f"🎯 {best_rec.get('market', 'N/A')}")
        
        conf = best_rec.get('confidence', 'LOW')
        if conf == 'HIGH':
            panel.append(f"Confidence: 🟢 {conf}")
        elif conf == 'MEDIUM':
            panel.append(f"Confidence: 🟡 {conf}")
        else:
            panel.append(f"Confidence: 🔴 {conf}")
        
        panel.append(f"Probability: {format_prob(best_rec.get('adjusted_probability'))}")
        
        edge = best_rec.get('edge', 0)
        if edge > 0:
            panel.append(f"Value Rating: {edge*100:.1f}% edge ✅")
        else:
            panel.append(f"Value Rating: No edge detected")
        
        kelly = best_rec.get('kelly_stake', 0)
        panel.append(f"Suggested Stake: {kelly*100:.1f}%")
        panel.append(f"Risk: {best_rec.get('risk', 'Unknown')}")
        
        # Show if historical adjustment was applied
        hist_adj = best_rec.get('historical_adjustment', 0)
        if hist_adj != 0:
            adj_str = f"+{hist_adj*100:.1f}%" if hist_adj > 0 else f"{hist_adj*100:.1f}%"
            panel.append(f"📊 Historical Adj: {adj_str}")
        
        # Additional recommendations
        if len(recommendations) > 1:
            panel.append("")
            panel.append("**Alternative Picks:**")
            for rec in recommendations[1:3]:
                edge_str = f" (+{rec.get('edge', 0)*100:.1f}%)" if rec.get('edge', 0) > 0 else ""
                panel.append(f"- {rec.get('market')}: {format_prob(rec.get('adjusted_probability'))}{edge_str}")
    else:
        panel.append("**Recommended Bet**")
        panel.append("- No strong recommendations for this match")
    
    panel.append("")
    panel.append("---")
    
    return "\n".join(panel)


def format_prob(prob: Optional[float]) -> str:
    """Format probability as percentage"""
    if prob is None:
        return "N/A"
    return f"{prob*100:.1f}%"


def format_odds(odds: Optional[float]) -> str:
    """Format decimal odds"""
    if odds is None:
        return "N/A"
    return f"{odds:.2f}"


def format_top_bets_table(bets: List[Dict]) -> str:
    """Format top bets as a markdown table with odds"""
    if not bets:
        return "*No bets available*"
    
    lines = []
    lines.append("| # | Match | League | Date | Bet | Prob | Odds | Edge | Stake | Conf |")
    lines.append("|---|-------|--------|------|-----|------|------|------|-------|------|")
    
    for i, bet in enumerate(bets, 1):
        date = bet.get('date', 'TBD')
        if isinstance(date, datetime):
            date_str = date.strftime('%d %b')
        elif isinstance(date, str) and 'T' in date:
            try:
                dt = datetime.fromisoformat(date.replace('Z', '+00:00'))
                date_str = dt.strftime('%d %b %H:%M')
            except:
                date_str = date[:10]
        else:
            date_str = str(date)[:10]
        
        conf = bet.get('confidence', 'LOW')
        if conf == 'HIGH':
            conf_icon = '🟢'
        elif conf == 'MEDIUM':
            conf_icon = '🟡'
        else:
            conf_icon = '🔴'
        
        edge = bet.get('edge', 0)
        edge_str = f"+{edge*100:.1f}%" if edge > 0 else "—"
        
        # Get odds
        odds = bet.get('bookmaker_odds')
        odds_str = f"{odds:.2f}" if odds else "—"
        
        lines.append(
            f"| {i} | {bet.get('match', 'N/A')[:25]} | {bet.get('league', 'N/A')[:15]} | "
            f"{date_str} | {bet.get('market', 'N/A')} | {format_prob(bet.get('adjusted_probability'))} | "
            f"{odds_str} | {edge_str} | {bet.get('kelly_stake', 0)*100:.1f}% | {conf_icon} |"
        )
    
    return "\n".join(lines)
    
    return "\n".join(lines)


def format_accumulator(acc: Dict) -> str:
    """Format accumulator suggestion"""
    lines = []
    
    acc_type = acc.get('type', 'ACCUMULATOR')
    lines.append(f"### 🎰 {acc_type}")
    lines.append("")
    
    lines.append("| # | Match | Pick | Probability |")
    lines.append("|---|-------|------|-------------|")
    
    for i, leg in enumerate(acc.get('legs', []), 1):
        lines.append(
            f"| {i} | {leg.get('match', 'N/A')[:30]} | {leg.get('market', 'N/A')} | "
            f"{format_prob(leg.get('adjusted_probability'))} |"
        )
    
    lines.append("")
    lines.append(f"**Combined Probability:** {format_prob(acc.get('combined_probability'))}")
    
    if acc.get('combined_odds'):
        lines.append(f"**Estimated Odds:** {acc.get('combined_odds'):.2f}")
    
    if acc.get('expected_value'):
        ev = acc.get('expected_value')
        ev_str = "✅ +EV" if ev > 1 else "❌ -EV"
        lines.append(f"**Expected Value:** {ev:.2f} {ev_str}")
    
    lines.append("")
    
    return "\n".join(lines)


def generate_full_report(analyses: List[Dict], top_bets: List[Dict], accumulators: List[Dict]) -> str:
    """Generate complete betting report"""
    
    report = []
    report.append("# 🎯 AI Sports Betting Intelligence Report")
    report.append(f"*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}*")
    report.append("")
    report.append("---")
    report.append("")
    
    # Summary
    total_matches = len(analyses)
    value_bets = sum(1 for b in top_bets if b.get('is_value_bet'))
    high_conf = sum(1 for b in top_bets if b.get('confidence') == 'HIGH')
    
    report.append("## 📊 Summary")
    report.append(f"- **Matches Analyzed:** {total_matches}")
    report.append(f"- **Value Bets Found:** {value_bets}")
    report.append(f"- **High Confidence Picks:** {high_conf}")
    report.append("")
    
    # Top 10 Bets
    report.append("## 🏆 Top 10 Best Bets")
    report.append("")
    report.append(format_top_bets_table(top_bets[:10]))
    report.append("")
    
    # Accumulators
    if accumulators:
        report.append("## 🎰 Accumulator Suggestions")
        report.append("")
        for acc in accumulators:
            report.append(format_accumulator(acc))
    
    # Detailed match panels
    report.append("## 📋 Match Analysis (Top Picks)")
    report.append("")
    
    # Show panels for matches with best recommendations
    matches_with_recs = [a for a in analyses if a.get('recommendations')]
    matches_with_recs.sort(
        key=lambda x: (
            x['recommendations'][0].get('edge', 0) if x['recommendations'] else 0,
            x['recommendations'][0].get('adjusted_probability', 0) if x['recommendations'] else 0
        ),
        reverse=True
    )
    
    for analysis in matches_with_recs[:10]:
        report.append(format_betting_panel(analysis))
    
    report.append("")
    report.append("---")
    report.append("*⚠️ Disclaimer: This analysis is for informational purposes only. Bet responsibly.*")
    
    return "\n".join(report)


def format_fixture_card(fixture: Dict, analysis: Optional[Dict] = None) -> Dict:
    """Format fixture data for display card (used in UI)"""
    
    date_str = fixture.get('date', 'TBD')
    if isinstance(date_str, str) and 'T' in date_str:
        try:
            dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            formatted_date = dt.strftime('%a %d %b, %H:%M')
        except:
            formatted_date = date_str
    else:
        formatted_date = str(date_str)
    
    card = {
        'fixture_id': fixture.get('fixture_id'),
        'home_team': fixture.get('home', {}).get('name', 'Unknown'),
        'away_team': fixture.get('away', {}).get('name', 'Unknown'),
        'home_logo': fixture.get('home', {}).get('logo'),
        'away_logo': fixture.get('away', {}).get('logo'),
        'league': fixture.get('league', {}).get('name', 'Unknown'),
        'league_logo': fixture.get('league', {}).get('logo'),
        'date': formatted_date,
        'venue': fixture.get('venue'),
        'status': fixture.get('status', 'NS'),
        'has_odds': bool(fixture.get('odds', {}).get('best_odds')),
        'has_injuries': bool(fixture.get('injuries')),
        'has_h2h': bool(fixture.get('h2h', {}).get('matches')),
    }
    
    # Add analysis summary if available
    if analysis:
        recommendations = analysis.get('recommendations', [])
        if recommendations:
            best = recommendations[0]
            card['best_bet'] = best.get('market')
            card['best_prob'] = best.get('adjusted_probability')
            card['best_edge'] = best.get('edge')
            card['confidence'] = best.get('confidence')
            card['can_analyze'] = True
        else:
            card['can_analyze'] = False
    else:
        card['can_analyze'] = True  # Can still analyze with predictions
    
    return card


def create_bet_slip_markdown(selected_bets: List[Dict]) -> str:
    """Create a bet slip summary"""
    if not selected_bets:
        return "*No bets selected*"
    
    lines = []
    lines.append("## 🎫 Your Bet Slip")
    lines.append("")
    
    total_prob = 1.0
    total_odds = 1.0
    
    for i, bet in enumerate(selected_bets, 1):
        prob = bet.get('probability', 0)
        odds = bet.get('odds', 1/prob if prob > 0 else 1)
        
        total_prob *= prob
        total_odds *= odds
        
        lines.append(f"**{i}. {bet.get('match')}**")
        lines.append(f"   - Pick: {bet.get('market')}")
        lines.append(f"   - Probability: {prob*100:.1f}%")
        lines.append(f"   - Odds: {odds:.2f}")
        lines.append("")
    
    lines.append("---")
    lines.append(f"**Combined Probability:** {total_prob*100:.2f}%")
    lines.append(f"**Combined Odds:** {total_odds:.2f}")
    
    # Kelly for accumulator
    from data_merger import ValueBetCalculator
    kelly = ValueBetCalculator.kelly_criterion(total_prob, total_odds)
    lines.append(f"**Suggested Stake:** {kelly*100:.1f}%")
    
    return "\n".join(lines)
