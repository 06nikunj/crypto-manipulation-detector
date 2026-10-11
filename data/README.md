# Dataset

## Single Authoritative Dataset

**SystemsLab-Sapienza Pump-and-Dump Benchmark Dataset**
- **Source:** https://github.com/SystemsLab-Sapienza/pump-and-dump-dataset
- **Paper:** "Pump and Dumps in the Bitcoin Era: Real Time Detection of Cryptocurrency Market Manipulations," ICCCN 2020
- **Authors:** M. La Morgia, A. Mei, F. Sassi, J. Stefa
- **License:** See original repository (research use)

## Files

| File | Description | Rows |
|------|-------------|------|
| `pump_telegram.csv` | Raw labeled pump events from Telegram groups | ~900 |
| `group.csv` | Telegram group metadata | ~20 |
| `pump_events_clean.csv` | Cleaned and parsed pump events | ~900 |
| `labeled_trades.csv` | Generated trade-chunk dataset with features + labels | ~80K |

## Schema

### pump_telegram.csv (original from Sapienza)
| Column | Type | Description |
|--------|------|-------------|
| symbol | string | Pumped coin symbol (e.g. "BTC") |
| group | string | Telegram group code |
| date | date | Pump date (UTC) |
| hour | time | Pump hour (UTC) |
| exchange | string | Exchange (usually "binance") |

### labeled_trades.csv (generated)
| Column | Type | Description |
|--------|------|-------------|
| timestamp | datetime | UTC timestamp of chunk |
| symbol | string | Trading pair symbol |
| price | float | Closing price (SYM/BTC) |
| volume | float | Trade volume (BTC) |
| n_trades | int | Number of trades in chunk |
| rush_orders | int | Number of rapid buy orders |
| buy_ratio | float | Fraction of buy-side trades |
| is_pump | int | Ground truth label (0=normal, 1=pump) |
| event_id | string | Unique event identifier |
| group | string | Telegram group code |
| exchange | string | Exchange |

## Download

```bash
python3 data/download.py
```

Network access is required to fetch `pump_telegram.csv` and `group.csv`.
If offline, the script generates a synthetic-but-structurally-identical fallback.

## Attribution

If you use this dataset, cite the original paper:

```bibtex
@INPROCEEDINGS{9209660,
  author={M. {La Morgia} and A. {Mei} and F. {Sassi} and J. {Stefa}},
  booktitle={2020 29th International Conference on Computer Communications and Networks (ICCCN)},
  title={Pump and Dumps in the Bitcoin Era: Real Time Detection of Cryptocurrency Market Manipulations},
  year={2020},
  pages={1-9},
  doi={10.1109/ICCCN49398.2020.9209660}
}
```
