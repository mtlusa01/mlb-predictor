# MLB Predictor

Pitcher-centric MLB game prediction model with park factors.

## Daily Pipeline

```bash
python get_data.py           # Fetch team stats + pitcher data from ESPN
python fetch_odds.py         # Fetch odds from The Odds API
python mlb_model.py --today  # Generate predictions
python export_projections.py # Export to mlb_game_projections.json
python grade_mlb.py          # Grade finished games + push to mattev-sports
```

## Model Weights

| Factor | Weight |
|--------|--------|
| Starting pitcher quality | 35% |
| Team batting | 20% |
| Bullpen strength | 15% |
| Home/away advantage | 10% |
| Park factor | 8% |
| Recent form | 7% |
| Rest/travel | 5% |

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env
# Add your Odds API key to .env
```
