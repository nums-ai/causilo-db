# Example tables

Two public tables. Column names were shortened to plain identifiers, and the
target was blanked in about a third of the rows (`strength` in 343 rows,
`price_eur` in 513) so that there is something to predict out of the box. Every
other value is as published.

`scripts/live_check.py` and the results under
[Tested](https://github.com/nums-ai/causilo-db#tested) use the rows that still
have an answer and hide their own subset (every third `strength`, every fourth
`trim`), so that the predictions can be scored.

| File | Rows | Source | Licence |
|---|---|---|---|
| `concrete.csv` | 1,030 | I-Cheng Yeh, "Concrete Compressive Strength", UCI Machine Learning Repository, [doi:10.24432/C5PK67](https://doi.org/10.24432/C5PK67). Yeh, I-C., "Modeling of strength of high-performance concrete using artificial neural networks", Cement and Concrete Research 28(12), 1797–1808 (1998). | [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) |
| `fiat500.csv` | 1,538 | Paolocons, "Another Dataset on Used Fiat 500 (1538 Rows)", Kaggle, 2020, [kaggle.com/datasets/paolocons/another-fiat-500-dataset-1538-rows](https://www.kaggle.com/datasets/paolocons/another-fiat-500-dataset-1538-rows). | [CC0 1.0](https://creativecommons.org/publicdomain/zero/1.0/) |
