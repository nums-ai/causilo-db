"""causilo-db command line: predict the blank cells of a column of a database table."""

from __future__ import annotations

import argparse
import sys

from . import __version__
from .core import CausiloDBError
from .sql import predict_table

EPILOG = """example:
  export CAUSILO_API_KEY=csl_...
  causilo-db predict postgresql+psycopg://user@host/shop customers churn \\
      --id customer_id --out customers_churn_prediction

The API key is read from CAUSILO_API_KEY; calls are billed to it."""


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="causilo-db", description="Predict the blank cells of a database column with the hosted Causilo API.",
        epilog=EPILOG, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--version", action="version", version=f"causilo-db {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("predict", help="predict the blank cells of a column", epilog=EPILOG,
                       formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("url", help="SQLAlchemy URL, e.g. postgresql+psycopg://user@host/db "
                               "(a password here shows in shell history; prefer the driver's password file)")
    p.add_argument("table", help="the table to read (see --schema)")
    p.add_argument("target", help="the column whose blank cells are predicted")
    p.add_argument("--schema", help="the schema of the table")
    p.add_argument("--where", help="SQL condition choosing the rows to read")
    p.add_argument("--id", dest="id_column", help="a column that identifies rows; not used as a feature")
    p.add_argument("--features", help="comma-separated feature columns (default: all others)")
    p.add_argument("--task", choices=["clf", "reg"], help="classification or regression (default: inferred)")
    p.add_argument("--out", dest="out_table", help="write the result to this table")
    p.add_argument("--out-schema", help="the schema of the result table")
    p.add_argument("--if-exists", choices=["fail", "replace", "append"], default="fail",
                   help="when the result table exists (default: fail, checked before predicting)")
    args = parser.parse_args(argv)

    try:
        result, pred = predict_table(
            args.url, args.table, args.target, schema=args.schema, where=args.where,
            id_column=args.id_column, task=args.task,
            features=[f.strip() for f in args.features.split(",") if f.strip()] if args.features else None,
            out_table=args.out_table, out_schema=args.out_schema, if_exists=args.if_exists,
        )
    except CausiloDBError as exc:
        print(f"causilo-db: {exc}", file=sys.stderr)
        saved = getattr(exc, "result", None)
        if saved is not None:
            # Paid for but not written: keep it on stdout rather than lose it.
            saved.to_csv(sys.stdout, index=False)
            print("causilo-db: the predictions were printed to stdout instead", file=sys.stderr)
        return 2
    except ModuleNotFoundError as exc:
        print(f"causilo-db: the driver {exc.name!r} is not installed; install the matching extra, "
              'e.g. pip install "causilo-db[postgres]", and check the URL prefix', file=sys.stderr)
        return 2
    except Exception as exc:  # database and API errors: one line, not a traceback
        first = str(exc).strip().splitlines()[0] if str(exc).strip() else ""
        print(f"causilo-db: {type(exc).__name__}: {first}", file=sys.stderr)
        return 2
    summary = (f"{pred.n_query:,} blank cells of {pred.target} predicted from {pred.n_context:,} rows "
               f"({'regression' if pred.task == 'reg' else 'classification'}, {pred.calls} call(s))")
    print(summary, file=sys.stderr)
    for note in pred.notes:
        print(f"note: {note}", file=sys.stderr)
    if args.out_table:
        print(f"written to {args.out_table}", file=sys.stderr)
    else:
        result.to_csv(sys.stdout, index=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
