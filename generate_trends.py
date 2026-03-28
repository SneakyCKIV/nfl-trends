import pandas as pd
import json

# =========================
# 1. LOAD DATA
# =========================

url = "https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_2025.parquet"
df = pd.read_parquet(url)

# =========================
# 2. TEAM METRICS
# =========================

team_pass_attempts = df.groupby('posteam')['pass'].sum()
team_plays = df.groupby('posteam').size()
team_pass_rate = team_pass_attempts / team_plays

team_metrics = {}

for team in team_pass_attempts.index:
    team_metrics[team] = {
        "pass_attempts": int(team_pass_attempts[team]),
        "pass_rate": float(team_pass_rate[team]),
        "plays": int(team_plays[team])
    }

# =========================
# 3. PLAYER USAGE
# =========================

# Targets
targets = df[df['pass'] == 1].groupby(['posteam', 'receiver_player_name']).size()

# Rush attempts
rushes = df[df['rush'] == 1].groupby(['posteam', 'rusher_player_name']).size()

player_usage = {}

# Targets
for (team, player), val in targets.items():
    if player not in player_usage:
        player_usage[player] = {}
    player_usage[player]["team"] = team
    player_usage[player]["targets"] = int(val)

# Rushes
for (team, player), val in rushes.items():
    if player not in player_usage:
        player_usage[player] = {}
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
# 5. EFFICIENCY
# =========================

# Yards per target
yards = df.groupby('receiver_player_name')['yards_gained'].sum()
player_targets_total = df[df['pass'] == 1].groupby('receiver_player_name').size()

for player in player_usage:
    if player in yards and player in player_targets_total:
        ypt = yards[player] / player_targets_total[player]
        player_usage[player]["yards_per_target"] = float(ypt)

# EPA per target
epa = df.groupby('receiver_player_name')['epa'].mean()

for player in player_usage:
    if player in epa:
        player_usage[player]["epa_per_target"] = float(epa[player])

# =========================
# 6. COACHING CHANGES
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
        if metrics["pass_rate"] < 0.48:
            result["expected_shift"] = "MORE PASSING"
        else:
            result["expected_shift"] = "UNCERTAIN"
    else:
        result["expected_shift"] = "STABLE"

    coach_results.append(result)

# =========================
# 7. PLAYER MOVEMENT MODEL
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
        "volume_change": int(volume_change)
    }

    if volume_change > 50:
        result["trend"] = "MORE OPPORTUNITY"
    elif volume_change < -50:
        result["trend"] = "LESS OPPORTUNITY"
    else:
        result["trend"] = "SIMILAR ROLE"

    player_movement_results.append(result)

# =========================
# 8. OUTPUT
# =========================

output = {
    "team_metrics": team_metrics,
    "player_usage": player_usage,
    "coach_analysis": coach_results,
    "player_movement": player_movement_results
}

with open("trending.json", "w") as f:
    json.dump(output, f, indent=2)
