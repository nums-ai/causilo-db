"""Run Causilo on database tables: fill the blank cells of a column."""

from .core import CausiloDBError, PartialPredictionError, Prediction, infer_task, predict_frame
from .sql import predict_table, read_table

__version__ = "0.2.0"
__all__ = ["CausiloDBError", "PartialPredictionError", "Prediction", "infer_task", "predict_frame", "predict_table", "read_table"]
