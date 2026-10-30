#!/usr/bin/env bash
set -euo pipefail

# --- Keep the window open on Windows when something fails ---
pause_on_exit() {
  local code=$?
  if [ "$code" -ne 0 ]; then
    echo ""
    echo "❌ Failed (exit $code) at line ${BASH_LINENO[0]:-?}: ${BASH_COMMAND:-?}"
  fi
  case "$(uname -s)" in
    MINGW*|MSYS*|CYGWIN*) read -r -p "Press Enter to close..." _ || true ;;
  esac
}
trap pause_on_exit EXIT

# Windows CLIs (aws, go) can print CRLF; strip \r from captured output
strip_cr() { tr -d '\r'; }

# Never open the AWS CLI pager (it hangs/kills Git Bash windows)
export AWS_PAGER=""

# --- Locate directories ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${PROJECT_ROOT}"

ENV_FILE="${PROJECT_ROOT}/.env"
if [ ! -f "$ENV_FILE" ]; then
  echo "❌ .env file not found at ${ENV_FILE}!"
  exit 1
fi

# --- Preflight: Docker engine must be running (Linux containers) ---
if ! docker info >/dev/null 2>&1; then
  echo "❌ Docker engine is not reachable."
  echo "   • Start Docker Desktop and wait until it says 'Engine running'"
  echo "   • Make sure it is in Linux containers mode (tray icon → 'Switch to Linux containers...')"
  exit 1
fi

# --- Bump app version (pure bash: no Go needed on the build PC) ---
echo "🔼 Bumping app version..."
bump_version() {
  local cur new major minor patch
  cur=$(grep -m1 '^APP_VERSION=' "$ENV_FILE" | cut -d= -f2- | tr -d '\r"'"'"' ' || true)
  if [[ "$cur" =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]; then
    major=${BASH_REMATCH[1]}; minor=${BASH_REMATCH[2]}; patch=${BASH_REMATCH[3]}
    new="${major}.${minor}.$((patch + 1))"
  else
    echo "⚠️  APP_VERSION missing/invalid ('${cur}'), starting at 0.0.1"
    new="0.0.1"
  fi
  # keep CRLF if the .env uses it
  local eol=""; grep -q $'\r$' "$ENV_FILE" && eol=$'\r'
  if grep -q '^APP_VERSION=' "$ENV_FILE"; then
    sed -i "s/^APP_VERSION=.*/APP_VERSION=${new}${eol}/" "$ENV_FILE"
  else
    [ -n "$(tail -c1 "$ENV_FILE")" ] && printf '%s\n' "$eol" >> "$ENV_FILE"
    printf 'APP_VERSION=%s%s\n' "$new" "$eol" >> "$ENV_FILE"
  fi
  # optional "// version X.Y.Z" placeholder in go.mod
  [ -f "${PROJECT_ROOT}/go.mod" ] && sed -i -E "s#^(//[[:space:]]*version[[:space:]]*)[0-9]+\.[0-9]+\.[0-9]+#\1${new}#" "${PROJECT_ROOT}/go.mod"
  echo "🔼 Bumped version ${cur:-none} → ${new}"
}
bump_version

# --- Load .env (CRLF-safe, handles spaces/quotes/blank lines) ---
while IFS= read -r line || [ -n "$line" ]; do
  line="${line%$'\r'}"
  [[ -z "$line" || "$line" =~ ^[[:space:]]*# ]] && continue
  [[ "$line" != *=* ]] && continue
  key="${line%%=*}"; val="${line#*=}"
  key="$(echo "$key" | xargs)"
  # drop surrounding quotes
  if [[ "$val" =~ ^\"(.*)\"$ || "$val" =~ ^\'(.*)\'$ ]]; then val="${BASH_REMATCH[1]}"; fi
  [[ "$key" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] && export "$key=$val"
done < "$ENV_FILE"

# --- Extract version ---
VERSION=${APP_VERSION:-"0.0.1"}

# --- AWS & ECR setup ---
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text | strip_cr)
REGION=${AWS_REGION:-"ap-southeast-1"}
REPO_NAME=${ECR_REPO_NAME:-}
if [ -z "$REPO_NAME" ]; then
  echo "❌ ECR_REPO_NAME is not set in .env"
  exit 1
fi

REGISTRY="${AWS_ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com"
IMAGE_TAG="${REGISTRY}/${REPO_NAME}:v${VERSION}"
IMAGE_TAG_LATEST="${REGISTRY}/${REPO_NAME}:latest"

echo "📦 Building and pushing image..."
echo "🔹 Repository: ${REPO_NAME}"
echo "🔹 Region: ${REGION}"
echo "🔹 Version: v${VERSION}"

# --- Ensure ECR repo exists ---
aws ecr describe-repositories \
  --repository-names "${REPO_NAME}" \
  --region "${REGION}" >/dev/null 2>&1 || {
  echo "📁 Creating ECR repository ${REPO_NAME}..."
  aws ecr create-repository --repository-name "${REPO_NAME}" --region "${REGION}" >/dev/null
}

# --- Authenticate Docker to ECR ---
echo "🔐 Logging into ECR..."
aws ecr get-login-password --region "${REGION}" | strip_cr | docker login \
  --username AWS \
  --password-stdin "${REGISTRY}"

# --- Ensure a multi-arch capable builder (Docker Desktop default driver can't do this) ---
BUILDER=multiarch
if ! docker buildx inspect "$BUILDER" --bootstrap >/dev/null 2>&1; then
  echo "🧰 (Re)creating buildx builder '${BUILDER}'..."
  docker buildx rm "$BUILDER" >/dev/null 2>&1 || true
  docker buildx create --name "$BUILDER" --driver docker-container >/dev/null
  docker buildx inspect "$BUILDER" --bootstrap >/dev/null
fi

# --- Build multi-arch image ---
echo "🏗️  Building multi-arch image..."
docker buildx build \
  --builder "$BUILDER" \
  --platform linux/amd64,linux/arm64 \
  -t "${IMAGE_TAG}" \
  -t "${IMAGE_TAG_LATEST}" \
  --push \
  "${PROJECT_ROOT}"

# --- Verify ---
echo "🔎 Verifying manifest..."
docker buildx imagetools inspect "${IMAGE_TAG_LATEST}" | grep 'Platform:' || true

# --- Done ---
echo "✅ Multi-arch image pushed successfully!"
echo "🖇️ Tags:"
echo "   - ${IMAGE_TAG}"
echo "   - ${IMAGE_TAG_LATEST}"