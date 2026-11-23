"""A deterministic stand-in for the Causilo client."""

from __future__ import annotations

import numpy as np
import pandas as pd


class FakeCausilo:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def predict_with_metadata(self, context: pd.DataFrame, query: pd.DataFrame, *, target, model,
                              output_type=None, quantiles=None):
        self.calls.append({"n_context": len(context), "n_query": len(query), "model": model,
                           "columns": list(query.columns), "target_in_context": target in context.columns})
        first = query.iloc[:, 0]
        x = pd.to_numeric(first, errors="coerce").fillna(0).to_numpy(dtype="float64")
        if model == "causilo-reg":
            qs = quantiles or [0.5]
            return np.column_stack([x * 2 + (q - 0.5) * 10 for q in qs]), {}
        classes = sorted(str(c) for c in context[target].dropna().unique())
        p = 1 / (1 + np.exp(-(x - x.mean())))
        if len(classes) == 2:
            return np.column_stack([1 - p, p]), {"classes": classes}
        m = np.full((len(x), len(classes)), 1 / len(classes))
        return m, {"classes": classes}
