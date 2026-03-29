import pandas as pd

# Load play-by-play data
df = pd.read_csv(
    "https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_2025.csv.gz",
    compression="gzip",
    low_memory=False
)

# ----------------------
# TEAM-LEVEL METRICS
# ----------------------
team = df.groupby("posteam").apply(
    lambda x: pd.Series({
        # Success rates
        "early_down_success": x[x["down"].isin([1,2])]["success"].mean(),
        "third_down_conv": x[x["down"] == 3]["first_down"].mean(),

        # First down rate (important for your scoring)
        "first_down_rate": x["first_down"].mean(),

        # Efficiency
        "epa_per_play": x["epa"].mean(),

        # Red zone TD rate
        "redzone_td_rate": x[x["yardline_100"] <= 20]["touchdown"].mean()
    })
).reset_index()

team.to_json("data/situational_team.json", orient="records")


# ----------------------
# PLAYER-LEVEL METRICS
# ----------------------

passes = df[df["pass_attempt"] == 1].copy()

# Drop missing receivers (important)
passes = passes.dropna(subset=["receiver_player_name"])

player = passes.groupby("receiver_player_name").apply(
    lambda x: pd.Series({
        "targets": x["target"].sum(),
        "first_downs": x["first_down"].sum(),
        "targets_3rd": x[x["down"] == 3]["target"].sum(),
        "first_downs_3rd": x[x["down"] == 3]["first_down"].sum(),
        "targets_rz": x[x["yardline_100"] <= 20]["target"].sum(),
        "tds_rz": x[x["yardline_100"] <= 20]["touchdown"].sum(),
        "epa_per_target": x["epa"].mean()
    })
).reset_index()

player.to_json("data/situational_player.json", orient="records")


print("✅ Situational data updated")
