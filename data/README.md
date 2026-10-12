# Bundled benchmark data

## Source and attribution

CryptoSentinel uses the [SystemsLab–Sapienza Pump-and-Dump Dataset](https://github.com/SystemsLab-Sapienza/pump-and-dump-dataset), associated with the paper:

> M. La Morgia, A. Mei, F. Sassi, and J. Stefa, “Pump and Dumps in the Bitcoin Era: Real Time Detection of Cryptocurrency Market Manipulations,” *2020 29th International Conference on Computer Communications and Networks (ICCCN)*, 2020, pp. 1–9. DOI: [10.1109/ICCCN49398.2020.9209660](https://doi.org/10.1109/ICCCN49398.2020.9209660).

The upstream GitHub repository declares the MIT License. Its copyright and license notice is included at `pump_dump_dataset/LICENSE` and should accompany redistributed benchmark files.

## Included files

| File | Description |
|---|---|
| `pump_dump_dataset/labeled_trades.csv` | Project-prepared labeled five-second trade chunks used by the replay and XGBoost inference pipeline: 96,000 rows, including 4,800 pump-labeled and 91,200 normal-labeled rows. |
| `pump_dump_dataset/pump_events_clean.csv` | Cleaned benchmark pump-event records used for attribution and event/window context. |
| `pump_dump_dataset/pump_telegram.csv` | Benchmark event/group records derived from the source dataset. |
| `pump_dump_dataset/LICENSE` | Upstream MIT license and copyright notice. |

The supplied `labeled_trades.csv` is historical benchmark data, not a feed of current exchange trades. Its source `is_pump` field is ground truth for retrospective evaluation; it is not used as an inference feature.

## Citation

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
