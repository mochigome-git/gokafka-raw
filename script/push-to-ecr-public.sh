#!/usr/bin/env bash
set -euo pipefail

# ─── Keep the window open on Windows (Git Bash) ──────────────────────────────
TMP_DOCKER_CONFIG=""
pause_on_exit() {
  local code=$?
  [ -n "$TMP_DOCKER_CONFIG" ] && rm -rf "$TMP_DOCKER_CONFIG"
  if [ "$code" -ne 0 ]; then
    echo ""
    echo "❌ Failed (exit $code): ${BASH_COMMAND:-?}"
  fi
  case "$(uname -s)" in
    MINGW*|MSYS*|CYGWIN*) read -r -p "Press Enter to close..." _ || true ;;
  esac
}
trap pause_on_exit EXIT

strip_cr() { tr -d '\r'; }   # Windows aws.exe prints CRLF
export AWS_PAGER=""

# ─── Always run from project root (works when double-clicked) ────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${PROJECT_ROOT}"

# ─── Load .env (CRLF-safe) ───────────────────────────────────────────────────
if [ ! -f .env ]; then
  echo "❌ .env file not found at ${PROJECT_ROOT}/.env"
  exit 1
fi
while IFS= read -r line || [ -n "$line" ]; do
  line="${line%$'\r'}"
  [[ -z "$line" || "$line" =~ ^[[:space:]]*# || "$line" != *=* ]] && continue
  key="${line%%=*}"; val="${line#*=}"
  key="$(echo "$key" | xargs)"
  if [[ "$val" =~ ^\"(.*)\"$ || "$val" =~ ^\'(.*)\'$ ]]; then val="${BASH_REMATCH[1]}"; fi
  [[ "$key" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] && export "$key=$val"
done < .env

# ─── Validate required vars ──────────────────────────────────────────────────
: "${APP_VERSION:?❌ APP_VERSION is not set in .env}"
: "${ECR_REPO_NAME:?❌ ECR_REPO_NAME is not set in .env}"
: "${ECR_PUBLIC_ALIAS:?❌ ECR_PUBLIC_ALIAS is not set in .env}"
: "${ECR_PUBLIC_REPO_NAME:?❌ ECR_PUBLIC_REPO_NAME is not set in .env}"
ECR_REGION="${ECR_REGION:-${AWS_REGION:-ap-southeast-1}}"
ECR_PUBLIC_REGION="us-east-1"

# ─── Docker must be running ──────────────────────────────────────────────────
if ! docker info >/dev/null 2>&1; then
  echo "❌ Docker engine is not reachable. Start Docker Desktop (Linux containers mode)."
  exit 1
fi

# Log in to a registry. On Windows, `docker login` always hands the token to
# Credential Manager (even with no credsStore), which rejects the long ECR
# Public token. So we write the auth straight into a temp config.json instead.
registry_login() {   # $1 = host, token on stdin
  local host="$1" token
  token="$(strip_cr)"
  if [ "${SKIP_DOCKER_LOGIN:-0}" = "1" ]; then
    echo "⏭️  Skipping login to ${host} (using Docker's existing login)"
    return 0
  fi
  if [ -n "$TMP_DOCKER_CONFIG" ]; then
    # kept in a file: this function runs in a pipeline subshell
    echo "${host}|$(printf 'AWS:%s' "$token" | base64 | tr -d '\n')" >> "$TMP_DOCKER_CONFIG/.auths"
    {
      printf '{\n  "auths": {'
      local i=0 e
      while IFS= read -r e; do
        [ $i -gt 0 ] && printf ','
        printf '\n    "%s": { "auth": "%s" }' "${e%%|*}" "${e#*|}"
        i=$((i+1))
      done < "$TMP_DOCKER_CONFIG/.auths"
      printf '\n  }'
      [ -n "${CTX:-}" ] && printf ',\n  "currentContext": "%s"' "$CTX"
      printf '\n}\n'
    } > "$TMP_DOCKER_CONFIG/config.json"
    echo "Login Succeeded (stored for this run only)"
  else
    printf '%s' "$token" | docker login --username AWS --password-stdin "$host"
  fi
}

# ─── Windows: bypass Credential Manager ──────────────────────────────────────
# wincred / desktop helper rejects the long ECR Public token with
# "The stub received bad data". Use a throwaway Docker config without a
# credsStore for this run only (deleted on exit), keeping contexts/buildx.
case "$( [ "${SKIP_DOCKER_LOGIN:-0}" = "1" ] && echo skip || uname -s)" in
  MINGW*|MSYS*|CYGWIN*)
    SRC_CFG="${DOCKER_CONFIG:-$HOME/.docker}"
    TMP_DOCKER_CONFIG="$(mktemp -d)"
    for d in contexts buildx cli-plugins; do
      [ -d "$SRC_CFG/$d" ] && cp -r "$SRC_CFG/$d" "$TMP_DOCKER_CONFIG/"
    done
    CTX=$(grep -o '"currentContext"[[:space:]]*:[[:space:]]*"[^"]*"' "$SRC_CFG/config.json" 2>/dev/null \
          | sed 's/.*"\([^"]*\)"$/\1/' || true)
    printf '{ "auths": {}%s }\n' "${CTX:+, \"currentContext\": \"$CTX\"}" > "$TMP_DOCKER_CONFIG/config.json"
    export DOCKER_CONFIG="$TMP_DOCKER_CONFIG"
    ;;
esac

# ─── AWS profiles: source (private) and destination (public) can differ ──────
#   ECR_SOURCE_PROFILE -> account that owns the private ECR repo
#   ECR_PUBLIC_PROFILE -> account that owns the public alias
SRC=(); PUB=()
[ -n "${ECR_SOURCE_PROFILE:-}" ] && SRC=(--profile "${ECR_SOURCE_PROFILE}")
[ -n "${ECR_PUBLIC_PROFILE:-}" ] && PUB=(--profile "${ECR_PUBLIC_PROFILE}")

AWS_ACCOUNT_ID=$(aws ${SRC[@]+"${SRC[@]}"} sts get-caller-identity --query Account --output text | strip_cr)
PUBLIC_ACCOUNT_ID=$(aws ${PUB[@]+"${PUB[@]}"} sts get-caller-identity --query Account --output text | strip_cr)

REGISTRY="${AWS_ACCOUNT_ID}.dkr.ecr.${ECR_REGION}.amazonaws.com"
PRIVATE_IMAGE="${REGISTRY}/${ECR_REPO_NAME}:v${APP_VERSION}"
PUBLIC_IMAGE="public.ecr.aws/${ECR_PUBLIC_ALIAS}/${ECR_PUBLIC_REPO_NAME}:v${APP_VERSION}"
PUBLIC_IMAGE_LATEST="public.ecr.aws/${ECR_PUBLIC_ALIAS}/${ECR_PUBLIC_REPO_NAME}:latest"

echo ""
echo "  Private : ${PRIVATE_IMAGE}"
echo "  Public  : ${PUBLIC_IMAGE}"
echo "  Public  : ${PUBLIC_IMAGE_LATEST}"
echo "  Source account : ${AWS_ACCOUNT_ID} (${ECR_SOURCE_PROFILE:-${AWS_PROFILE:-default}})"
echo "  Public account : ${PUBLIC_ACCOUNT_ID} (${ECR_PUBLIC_PROFILE:-${AWS_PROFILE:-default}})"
echo ""

# ─── Login to Private ECR ────────────────────────────────────────────────────
echo "🔐 Logging into Private ECR..."
aws ${SRC[@]+"${SRC[@]}"} ecr get-login-password --region "${ECR_REGION}" | registry_login "${REGISTRY}"

# ─── Verify the source image exists ──────────────────────────────────────────
echo "🔍 Inspecting source manifest..."
if ! docker buildx imagetools inspect "${PRIVATE_IMAGE}" > /dev/null 2>&1; then
  echo "❌ Could not find or inspect ${PRIVATE_IMAGE} in private ECR."
  echo "   Run ./script/deploy-ecr.sh first, or check ECR_SOURCE_PROFILE."
  exit 1
fi
docker buildx imagetools inspect "${PRIVATE_IMAGE}" | grep -E 'Name:|Platform:' || true

# ─── Login to Public ECR ─────────────────────────────────────────────────────
echo ""
echo "🔐 Logging into Public ECR..."
aws ${PUB[@]+"${PUB[@]}"} ecr-public get-login-password --region "${ECR_PUBLIC_REGION}" | registry_login public.ecr.aws

if ! aws ${PUB[@]+"${PUB[@]}"} ecr-public describe-repositories --region "${ECR_PUBLIC_REGION}" \
     --repository-names "${ECR_PUBLIC_REPO_NAME}" >/dev/null 2>&1; then
  echo "❌ Account ${PUBLIC_ACCOUNT_ID} has no public repo '${ECR_PUBLIC_REPO_NAME}'."
  echo "   public.ecr.aws/${ECR_PUBLIC_ALIAS} belongs to another account."
  echo "   Set ECR_PUBLIC_PROFILE in .env to the profile that owns it."
  exit 1
fi

# ─── Copy multi-platform manifest list, registry-to-registry ─────────────────
# imagetools create copies the full manifest list (all platforms) directly;
# docker pull/tag/push would silently drop every arch except the host's.
echo ""
echo "📤 Copying multi-arch image to Public ECR..."
docker buildx imagetools create \
  -t "${PUBLIC_IMAGE}" \
  -t "${PUBLIC_IMAGE_LATEST}" \
  "${PRIVATE_IMAGE}"

# ─── Verify ──────────────────────────────────────────────────────────────────
echo ""
echo "🔍 Verifying public manifest..."
docker buildx imagetools inspect "${PUBLIC_IMAGE}" | grep 'Platform:' || true

echo ""
echo "✅ Done! ${PUBLIC_IMAGE}"