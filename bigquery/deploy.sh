#!/usr/bin/env bash
# Deploy the Causilo BigQuery remote function into your own GCP project.
#
#   PROJECT=my-project REGION=us-central1 DATASET=analytics KEY_FILE=~/causilo.key \
#       bigquery/deploy.sh
#
# Run from the repository root, logged in with gcloud as someone who can enable
# APIs, create service accounts and grant roles in PROJECT. It creates, all
# named after PREFIX (default "causilo"):
#
#   - an Artifact Registry repository PREFIX and the service image in it;
#   - with BUILD=cloudbuild, a service account PREFIX-build that the build runs
#     as and a bucket PROJECT-PREFIX-build for the build's source upload;
#   - a Secret Manager secret PREFIX-api-key holding the key from KEY_FILE;
#   - a service account PREFIX-run, which the Cloud Run service runs as. It gets
#     BigQuery Job User on the project and BigQuery Data Editor on each dataset
#     in ALLOWED_DATASETS, and can read the secret, nothing else;
#   - a Cloud Run service PREFIX-predict that only authenticated callers reach;
#   - a BigQuery connection PREFIX (Cloud resource) whose service account may
#     invoke that service, and nothing else;
#   - the functions DATASET.causilo_predict and DATASET.causilo_predict_with_options.
#
# Settings, from the environment:
#   PROJECT           the project (required)
#   REGION            Cloud Run, Artifact Registry and secret region (required)
#   BQ_LOCATION       BigQuery location of DATASET and the connection (default REGION)
#   DATASET           dataset that holds the functions; created if missing (required)
#   ALLOWED_DATASETS  comma-separated project.dataset names the function may read
#                     and write (default PROJECT.DATASET)
#   KEY_FILE          file holding the Causilo API key (required). The key is
#                     read into Secret Manager and never printed
#   PREFIX            resource name prefix (default causilo)
#   BUILD             cloudbuild (default) or docker (build locally and push)
#   MAX_INSTANCES     Cloud Run instance cap (default 3)
#
# Run it again to deploy a new image or a new key; it reuses what exists.
# What it creates is marked as its own (the label causilo_db=deploy, or on a
# service account and the connection the description below), and
# bigquery/teardown.sh deletes only what carries the mark. A repository,
# secret, bucket, service account or connection that existed already is used
# as it is and never marked. The build bucket is the one thing labelled after
# it is created rather than with it; should that step fail, a rerun finds the
# bucket unlabelled, leaves it so (teardown.sh then keeps it) and prints the
# command that labels it.

set -euo pipefail

: "${PROJECT:?set PROJECT}" "${REGION:?set REGION}" "${DATASET:?set DATASET}" "${KEY_FILE:?set KEY_FILE}"
BQ_LOCATION="${BQ_LOCATION:-$REGION}"
PREFIX="${PREFIX:-causilo}"
ALLOWED_DATASETS="${ALLOWED_DATASETS:-$PROJECT.$DATASET}"
BUILD="${BUILD:-cloudbuild}"
MAX_INSTANCES="${MAX_INSTANCES:-3}"

LABEL="causilo_db=deploy"
MARK="Created by causilo-db bigquery/deploy.sh"
REPO="$PREFIX"
IMAGE="$REGION-docker.pkg.dev/$PROJECT/$REPO/predict:$(date +%Y%m%d%H%M%S)"
SECRET="$PREFIX-api-key"
RUN_SA_NAME="$PREFIX-run"
RUN_SA="$RUN_SA_NAME@$PROJECT.iam.gserviceaccount.com"
SERVICE="$PREFIX-predict"
CONNECTION="$PREFIX"
BUILD_SA_NAME="$PREFIX-build"
BUILD_SA="$BUILD_SA_NAME@$PROJECT.iam.gserviceaccount.com"
BUCKET="$PROJECT-$PREFIX-build"

[ -f bigquery/main.py ] || { echo "Run from the repository root." >&2; exit 1; }
[ -s "$KEY_FILE" ] || { echo "KEY_FILE $KEY_FILE is missing or empty." >&2; exit 1; }

gc() { gcloud --project "$PROJECT" --quiet "$@"; }
step() { echo "== $*"; }
retry() {  # IAM changes take a little while to reach every service.
  local n
  for n in 1 2 3 4 5 6; do "$@" && return 0; echo "   not yet, retrying in $((n * 10))s"; sleep $((n * 10)); done
  return 1
}

step "Enabling APIs"
gc services enable run.googleapis.com artifactregistry.googleapis.com cloudbuild.googleapis.com \
  secretmanager.googleapis.com iam.googleapis.com bigquery.googleapis.com bigqueryconnection.googleapis.com

step "Dataset $DATASET ($BQ_LOCATION)"
if ! bq --project_id="$PROJECT" show --dataset "$PROJECT:$DATASET" >/dev/null 2>&1; then
  bq --project_id="$PROJECT" mk --dataset --location="$BQ_LOCATION" "$PROJECT:$DATASET"
fi

step "Artifact Registry repository $REPO"
if ! gc artifacts repositories describe "$REPO" --location "$REGION" >/dev/null 2>&1; then
  gc artifacts repositories create "$REPO" --repository-format docker --location "$REGION" --labels "$LABEL"
fi

step "Building $IMAGE ($BUILD)"
if [ "$BUILD" = docker ]; then
  gcloud auth configure-docker "$REGION-docker.pkg.dev" --quiet >/dev/null
  docker build --platform linux/amd64 -f bigquery/Dockerfile -t "$IMAGE" .
  docker push "$IMAGE"
else
  # The build runs as its own service account, which may only read the source
  # bucket, push to the repository and write logs, rather than the project's
  # default build account, which may hold broad roles or not exist at all.
  if ! gc iam service-accounts describe "$BUILD_SA" >/dev/null 2>&1; then
    gc iam service-accounts create "$BUILD_SA_NAME" --display-name "Causilo BigQuery function image build" \
      --description "$MARK"
  fi
  # A bucket cannot be labelled as it is created, so the label follows at
  # once, retried. A bucket found without it is never labelled later: its name
  # alone does not show that this script made it.
  if ! bucket_label="$(gcloud storage buckets describe "gs://$BUCKET" --project "$PROJECT" \
                       --format 'value(labels.causilo_db)' 2>/dev/null)"; then
    gcloud storage buckets create "gs://$BUCKET" --project "$PROJECT" --location "$REGION" \
      --uniform-bucket-level-access --quiet
    retry gcloud storage buckets update "gs://$BUCKET" --project "$PROJECT" --update-labels "$LABEL" --quiet
  elif [ "$bucket_label" != deploy ]; then
    echo "   gs://$BUCKET exists without the label $LABEL; it is used as it is and teardown.sh keeps it."
    echo "   If an earlier run of this script created it, label it to have teardown.sh delete it:"
    echo "     gcloud storage buckets update gs://$BUCKET --project $PROJECT --update-labels $LABEL"
  fi
  retry gcloud storage buckets add-iam-policy-binding "gs://$BUCKET" --member "serviceAccount:$BUILD_SA" \
    --role roles/storage.objectViewer --quiet >/dev/null
  retry gc artifacts repositories add-iam-policy-binding "$REPO" --location "$REGION" \
    --member "serviceAccount:$BUILD_SA" --role roles/artifactregistry.writer >/dev/null
  retry gc projects add-iam-policy-binding "$PROJECT" --member "serviceAccount:$BUILD_SA" \
    --role roles/logging.logWriter --condition None >/dev/null
  retry gc builds submit --region "$REGION" --config bigquery/cloudbuild.yaml --substitutions "_IMAGE=$IMAGE" \
    --service-account "projects/$PROJECT/serviceAccounts/$BUILD_SA" \
    --gcs-source-staging-dir "gs://$BUCKET/source" .
fi

step "Secret $SECRET"
if ! gc secrets describe "$SECRET" >/dev/null 2>&1; then
  gc secrets create "$SECRET" --replication-policy user-managed --locations "$REGION" --labels "$LABEL"
fi
# Strip a trailing newline: it would otherwise travel in an HTTP header.
SECRET_VERSION="$(tr -d '\r\n' < "$KEY_FILE" | gc secrets versions add "$SECRET" --data-file=- --format='value(name)')"
SECRET_VERSION="${SECRET_VERSION##*/}"

step "Service account $RUN_SA"
if ! gc iam service-accounts describe "$RUN_SA" >/dev/null 2>&1; then
  gc iam service-accounts create "$RUN_SA_NAME" --display-name "Causilo BigQuery remote function" \
    --description "$MARK"
fi
retry gc projects add-iam-policy-binding "$PROJECT" --member "serviceAccount:$RUN_SA" \
  --role roles/bigquery.jobUser --condition None >/dev/null
retry gc secrets add-iam-policy-binding "$SECRET" --member "serviceAccount:$RUN_SA" \
  --role roles/secretmanager.secretAccessor >/dev/null
IFS=',' read -r -a DATASETS <<< "$ALLOWED_DATASETS"
for ds in "${DATASETS[@]}"; do
  ds="$(echo "$ds" | tr -d ' ')"
  [ -n "$ds" ] || continue
  loc="$(bq --project_id="$PROJECT" show --format=prettyjson --dataset "${ds/./:}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["location"])')"
  echo "   BigQuery Data Editor on $ds"
  retry bq --project_id="$PROJECT" query --location="$loc" --use_legacy_sql=false --quiet \
    "GRANT \`roles/bigquery.dataEditor\` ON SCHEMA \`${ds%%.*}\`.${ds#*.} TO \"serviceAccount:$RUN_SA\"" >/dev/null
done

step "Cloud Run service $SERVICE"
# BigQuery allows a remote call 20 minutes; the service stops at the same point.
gc run deploy "$SERVICE" --region "$REGION" --image "$IMAGE" --service-account "$RUN_SA" \
  --no-allow-unauthenticated --timeout 1200 --concurrency 1 --max-instances "$MAX_INSTANCES" \
  --cpu 2 --memory 4Gi \
  --set-secrets "CAUSILO_API_KEY=$SECRET:$SECRET_VERSION" --update-labels "$LABEL" \
  --set-env-vars "^@^CAUSILO_ALLOWED_DATASETS=$ALLOWED_DATASETS"
URL="$(gc run services describe "$SERVICE" --region "$REGION" --format 'value(status.url)')"

step "BigQuery connection $CONNECTION"
if ! bq --project_id="$PROJECT" show --connection "$PROJECT.$BQ_LOCATION.$CONNECTION" >/dev/null 2>&1; then
  bq --project_id="$PROJECT" mk --connection --location="$BQ_LOCATION" --connection_type=CLOUD_RESOURCE \
    --description "$MARK" "$CONNECTION"
fi
CONN_SA="$(bq --project_id="$PROJECT" show --format=prettyjson --connection "$PROJECT.$BQ_LOCATION.$CONNECTION" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["cloudResource"]["serviceAccountId"])')"
retry gc run services add-iam-policy-binding "$SERVICE" --region "$REGION" \
  --member "serviceAccount:$CONN_SA" --role roles/run.invoker >/dev/null

step "Functions in $PROJECT.$DATASET"
for sig in "causilo_predict:" "causilo_predict_with_options:, options STRING"; do
  name="${sig%%:*}"; extra="${sig#*:}"
  bq --project_id="$PROJECT" query --location="$BQ_LOCATION" --use_legacy_sql=false --quiet "
CREATE OR REPLACE FUNCTION \`$PROJECT.$DATASET.$name\`(
  source_table STRING, target STRING, id_column STRING, output_table STRING$extra)
RETURNS STRING
REMOTE WITH CONNECTION \`$PROJECT.$BQ_LOCATION.$CONNECTION\`
OPTIONS (endpoint = '$URL', max_batching_rows = 1)" >/dev/null
done

echo
echo "Deployed. Try:"
echo "  SELECT \`$PROJECT.$DATASET\`.causilo_predict('$PROJECT.$DATASET.my_table', 'target', 'id', '$PROJECT.$DATASET.my_table_pred');"
