"""The 30 NBA team ids, in one place.

wowy_scrape.py carries its own nba_team_ids dict keyed by nickname ("Nuggets"),
because it had to look teams up from 538's team column. Nothing scraped here comes
from 538 any more -- pbpstats hands back a TeamId directly -- so the id is the key
and the names hang off it.

validate() checks the list against the ids actually appearing in a season's
schedule, which is how a relocation or expansion team would get noticed.
"""

TEAMS = {
    1610612737: ("ATL", "Hawks"),
    1610612738: ("BOS", "Celtics"),
    1610612739: ("CLE", "Cavaliers"),
    1610612740: ("NOP", "Pelicans"),
    1610612741: ("CHI", "Bulls"),
    1610612742: ("DAL", "Mavericks"),
    1610612743: ("DEN", "Nuggets"),
    1610612744: ("GSW", "Warriors"),
    1610612745: ("HOU", "Rockets"),
    1610612746: ("LAC", "Clippers"),
    1610612747: ("LAL", "Lakers"),
    1610612748: ("MIA", "Heat"),
    1610612749: ("MIL", "Bucks"),
    1610612750: ("MIN", "Timberwolves"),
    1610612751: ("BKN", "Nets"),
    1610612752: ("NYK", "Knicks"),
    1610612753: ("ORL", "Magic"),
    1610612754: ("IND", "Pacers"),
    1610612755: ("PHI", "76ers"),
    1610612756: ("PHX", "Suns"),
    1610612757: ("POR", "Trail Blazers"),
    1610612758: ("SAC", "Kings"),
    1610612759: ("SAS", "Spurs"),
    1610612760: ("OKC", "Thunder"),
    1610612761: ("TOR", "Raptors"),
    1610612762: ("UTA", "Jazz"),
    1610612763: ("MEM", "Grizzlies"),
    1610612764: ("WAS", "Wizards"),
    1610612765: ("DET", "Pistons"),
    1610612766: ("CHA", "Hornets"),
}

TEAM_IDS = sorted(TEAMS)


def abbrev(team_id):
    return TEAMS[int(team_id)][0]


def nickname(team_id):
    return TEAMS[int(team_id)][1]


def validate(season="2025-26"):
    """Compare the hardcoded ids against a season's actual schedule."""
    from pbpstats_client import get_json
    games = get_json("get-games/nba", {"Season": season,
                                       "SeasonType": "Regular Season"})["results"]
    seen = {int(g["HomeTeamId"]) for g in games} | {int(g["AwayTeamId"]) for g in games}
    missing, extra = seen - set(TEAMS), set(TEAMS) - seen
    if missing or extra:
        raise SystemExit(f"team list is stale for {season}: "
                         f"in schedule but not listed={sorted(missing)}, "
                         f"listed but not in schedule={sorted(extra)}")
    return True


if __name__ == "__main__":
    validate()
    print(f"{len(TEAMS)} teams, validated against the 2025-26 schedule")
