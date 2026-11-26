#!/usr/bin/env bash
# Remove what bigquery/deploy.sh created. Takes the same settings:
#
#   PROJECT=my-project REGION=us-central1 DATASET=analytics bigquery/teardown.sh
#
# Deletes the two functions, and of the BigQuery connection, the Cloud Run
# service, the Artifact Registry repository with its images, the secret, the
# build bucket and the two service accounts only those that deploy.sh created:
# they carry the label causilo_db=deploy, or on a service account and the
# connection the description "Created by causilo-db bigquery/deploy.sh". One
# that existed before deploy.sh ran and was reused is kept, and so are the
# grants of a kept service account; the script says which it kept.
#
# A deployment made before deploy.sh marked what it created has no marks, so
# nothing would be deleted; FORCE=1 deletes every piece named after PREFIX,
# whoever created it, after you type the project id to confirm.
#
# The dataset and the tables in it are kept, prediction results included,
# unless DELETE_DATASET=1. Missing pieces are skipped, so it is safe to run twice.

set -uo pipefail

: "${PROJECT:?set PROJECT}" "${REGION:?set REGION}" "${DATASET:?set DATASET}"
BQ_LOCATION="${BQ_LOCATION:-$REGION}"
PREFIX="${PREFIX:-causilo}"
ALLOWED_DATASETS="${ALLOWED_DATASETS:-$PROJECT.$DATASET}"
FORCE="${FORCE:-0}"
MARK="Created by causilo-db bigquery/deploy.sh"

RUN_SA="$PREFIX-run@$PROJECT.iam.gserviceaccount.com"
BUILD_SA="$PREFIX-build@$PROJECT.iam.gserviceaccount.com"
BUCKET="$PROJECT-$PREFIX-build"
gc() { gcloud --project "$PROJECT" --quiet "$@"; }
step() { echo "== $*"; }

if [ "$FORCE" = 1 ]; then
  echo "FORCE=1 deletes every resource named after PREFIX=$PREFIX in $PROJECT, including ones deploy.sh did not create."
  read -r -p "Type the project id to go on: " answer
  [ "$answer" = "$PROJECT" ] || { echo "Stopped." >&2; exit 1; }
fi

# owned WHAT VALUE: whether a piece is deploy.sh's, from its label or description
# (VALUE, empty when the piece is missing). Prints why a piece is kept.
owned() {
  local what="$1" value="$2"
  [ "$FORCE" = 1 ] && return 0
  if [ "$value" = deploy ] || [ "$value" = "$MARK" ]; then
    return 0
  fi
  echo "   kept $what: not created by deploy.sh (or missing)"
  return 1
}

step "Functions"
for name in causilo_predict causilo_predict_with_options; do
  bq --project_id="$PROJECT" query --location="$BQ_LOCATION" --use_legacy_sql=false --quiet \
    "DROP FUNCTION IF EXISTS \`$PROJECT.$DATASET.$name\`" >/dev/null || true
done

step "BigQuery connection $PREFIX"
desc="$(bq --project_id="$PROJECT" show --format=prettyjson --connection "$PROJECT.$BQ_LOCATION.$PREFIX" 2>/dev/null \
  | python3 -c 'import json,sys; print(json.load(sys.stdin).get("description", ""))' 2>/dev/null)"
if owned "connection $PREFIX" "$desc"; then
  bq --project_id="$PROJECT" rm -f --connection "$PROJECT.$BQ_LOCATION.$PREFIX" || true
fi

step "Cloud Run service $PREFIX-predict"
label="$(gc run services describe "$PREFIX-predict" --region "$REGION" \
  --format 'value(metadata.labels.causilo_db)' 2>/dev/null)"
if owned "service $PREFIX-predict" "$label"; then
  gc run services delete "$PREFIX-predict" --region "$REGION" || true
fi

step "Artifact Registry repository $PREFIX"
label="$(gc artifacts repositories describe "$PREFIX" --location "$REGION" \
  --format 'value(labels.causilo_db)' 2>/dev/null)"
if owned "repository $PREFIX" "$label"; then
  gc artifacts repositories delete "$PREFIX" --location "$REGION" || true
fi

step "Secret $PREFIX-api-key"
label="$(gc secrets describe "$PREFIX-api-key" --format 'value(labels.causilo_db)' 2>/dev/null)"
if owned "secret $PREFIX-api-key" "$label"; then
  gc secrets delete "$PREFIX-api-key" || true
fi

step "Service account $RUN_SA and its grants"
desc="$(gc iam service-accounts describe "$RUN_SA" --format 'value(description)' 2>/dev/null)"
if owned "service account $RUN_SA" "$desc"; then
  gc projects remove-iam-policy-binding "$PROJECT" --member "serviceAccount:$RUN_SA" \
    --role roles/bigquery.jobUser --condition None >/dev/null 2>&1 || true
  if [ "${DELETE_DATASET:-0}" != 1 ]; then
    IFS=',' read -r -a DATASETS <<< "$ALLOWED_DATASETS"
    for ds in "${DATASETS[@]}"; do
      ds="$(echo "$ds" | tr -d ' ')"
      [ -n "$ds" ] || continue
      loc="$(bq --project_id="$PROJECT" show --format=prettyjson --dataset "${ds/./:}" 2>/dev/null \
        | python3 -c 'import json,sys; print(json.load(sys.stdin)["location"])' 2>/dev/null)" || continue
      bq --project_id="$PROJECT" query --location="$loc" --use_legacy_sql=false --quiet \
        "REVOKE \`roles/bigquery.dataEditor\` ON SCHEMA \`${ds%%.*}\`.${ds#*.} FROM \"serviceAccount:$RUN_SA\"" \
        >/dev/null 2>&1 || true
    done
  fi
  gc iam service-accounts delete "$RUN_SA" || true
fi

step "Build bucket gs://$BUCKET"
label="$(gcloud storage buckets describe "gs://$BUCKET" --project "$PROJECT" \
  --format 'value(labels.causilo_db)' 2>/dev/null)"
if owned "bucket gs://$BUCKET" "$label"; then
  gcloud storage rm -r "gs://$BUCKET" --project "$PROJECT" --quiet || true
fi

step "Service account $BUILD_SA and its grant"
desc="$(gc iam service-accounts describe "$BUILD_SA" --format 'value(description)' 2>/dev/null)"
if owned "service account $BUILD_SA" "$desc"; then
  gc projects remove-iam-policy-binding "$PROJECT" --member "serviceAccount:$BUILD_SA" \
    --role roles/logging.logWriter --condition None >/dev/null 2>&1 || true
  gc iam service-accounts delete "$BUILD_SA" || true
fi

if [ "${DELETE_DATASET:-0}" = 1 ]; then
  step "Dataset $DATASET and every table in it"
  bq --project_id="$PROJECT" rm -r -f --dataset "$PROJECT:$DATASET" || true
fi
echo "Done."
