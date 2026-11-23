"""Run Causilo on database tables: fill the blank cells of a column."""

from .core import CausiloDBError, Prediction, infer_task, predict_frame
from .sql import predict_table, read_table

__version__ = "0.1.0"
__all__ = ["CausiloDBError", "Prediction", "infer_task", "predict_frame", "predict_table", "read_table"]
