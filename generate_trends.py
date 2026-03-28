import pandas as pd
import json

# =========================
# 1. LOAD DATA
# =========================

url = "https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_2025.parquet"
df = pd.read_parquet(url)

# =========================
# 2. TEAM METRICS (LAYER 1)
# =========================

team_pass_attempts = df.groupby('posteam')['pass'].sum()
team_plays = df.groupby('posteam').size()
team_pass_rate = team_pass_attempts / team_plays

team_metrics = {
    team: {
        "pass_attempts": int(team_pass_attempts[team]),
        "pass_rate": float(team_pass_rate[team]),
        "plays": int(team_plays[team])
    }
    for team in team_pass_attempts.index
}

# =========================
# 3. PLAYER USAGE (LAYER 2)
# =========================

# Targets
targets = df[df['pass'] == 1].groupby(['posteam', 'receiver_player_name']).size()

# Rush attempts
rushes = df[df['rush'] == 1].groupby(['posteam', 'rusher_player_name']).size()

player_usage = {}

def ensure_player(player):
    if player not in player_usage:
        player_usage[player] = {}

# Targets
for (team, player), val in targets.items():
    if pd.notna(player):
        ensure_player(player)
        player_usage[player]["team"] = team
        player_usage[player]["targets"] = int(val)

# Rushes
for (team, player), val in rushes.items():
    if pd.notna(player):
        ensure_player(player)
        player_usage[player]["team"] = team
        player_usage[player]["rush_attempts"] = int(val)

# =========================
# 4. TARGET SHARE
# =========================

team_target_totals = targets.groupby(level=0).sum()

for player, data in player_usage.items():
    team = data.get("team")
    if team in team_target_totals and "targets" in data:
        data["target_share"] = float(data["targets"] / team_target_totals[team])

# =========================
# 5. EFFICIENCY (LAYER 3)
# =========================

# Receiving efficiency
yards = df.groupby('receiver_player_name')['yards_gained'].sum()
player_targets_total = df[df['pass'] == 1].groupby('receiver_player_name').size()
epa_target = df[df['pass'] == 1].groupby('receiver_player_name')['epa'].mean()

# Rushing efficiency
epa_rush = df[df['rush'] == 1].groupby('rusher_player_name')['epa'].mean()

for player in player_usage:

    # Receiving
    if player in yards and player in player_targets_total:
        ypt = yards[player] / player_targets_total[player]
        player_usage[player]["yards_per_target"] = float(ypt)

    if player in epa_target:
        player_usage[player]["epa_per_target"] = float(epa_target[player])

    # Rushing (NEW)
    if player in epa_rush:
        player_usage[player]["epa_per_rush"] = float(epa_rush[player])

# =========================
# 6. RESOLVER LAYER (NEW)
# =========================

def normalize(name):
    return name.lower().replace(".", "").replace(" ", "")

def resolve_player(name):
    name_clean = normalize(name)

    for player in player_usage:
        if normalize(player) == name_clean:
            return player

    # fuzzy fallback
    for player in player_usage:
        if name_clean in normalize(player):
            return player

    return None

# =========================
# 7. TEAM/POSITION FALLBACK (NEW)
# =========================

def get_team_rbs(team):
    return [
        p for p, d in player_usage.items()
        if d.get("team") == team and "rush_attempts" in d
    ]

# =========================
# 8. COACHING CHANGES
# =========================

COACH_CHANGES = {
    "TEN": True,
    "LAC": True,
    "CAR": True
}

coach_results = []

for team, metrics in team_metrics.items():
    changed = COACH_CHANGES.get(team, False)

    result = {
        "team": team,
        "coach_change": changed,
        "pass_rate": metrics["pass_rate"],
        "plays": metrics["plays"]
    }

    if changed:
        result["expected_shift"] = (
            "MORE PASSING" if metrics["pass_rate"] < 0.48 else "UNCERTAIN"
        )
    else:
        result["expected_shift"] = "STABLE"

    coach_results.append(result)

# =========================
# 9. PLAYER MOVEMENT MODEL
# =========================

PLAYER_MOVES = [
    {
        "player": "DJ Moore",
        "old_team": "CHI",
        "new_team": "BUF"
    }
]

player_movement_results = []

for move in PLAYER_MOVES:
    old_team = move["old_team"]
    new_team = move["new_team"]

    old_volume = team_metrics.get(old_team, {}).get("pass_attempts", 500)
    new_volume = team_metrics.get(new_team, {}).get("pass_attempts", 500)

    volume_change = new_volume - old_volume

    result = {
        "player": move["player"],
        "old_team": old_team,
        "new_team": new_team,
        "old_team_pass_attempts": old_volume,
        "new_team_pass_attempts": new_volume,
        "volume_change": int(volume_change),
        "trend": (
            "MORE OPPORTUNITY" if volume_change > 50 else
            "LESS OPPORTUNITY" if volume_change < -50 else
            "SIMILAR ROLE"
        )
    }

    player_movement_results.append(result)

# =========================
# 10. OUTPUT (CLEAN STRUCTURE)
# =========================

with open("team_metrics.json", "w") as f:
    json.dump(team_metrics, f, indent=2)

with open("player_usage.json", "w") as f:
    json.dump(player_usage, f, indent=2)

with open("coach_analysis.json", "w") as f:
    json.dump(coach_results, f, indent=2)

with open("player_movement.json", "w") as f:
    json.dump(player_movement_results, f, indent=2)

print("✅ Data build complete")
