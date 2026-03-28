import pandas as pd
import json

url = "https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_2025.parquet"
df = pd.read_parquet(url)

team_pass_rate = df.groupby('posteam')['pass'].mean().reset_index()

output = {
    "teams": team_pass_rate.to_dict(orient="records")
}

with open("trending.json", "w") as f:
    json.dump(output, f)
