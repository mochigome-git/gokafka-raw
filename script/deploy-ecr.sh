#!/usr/bin/env bash
set -euo pipefail

# --- Locate directories ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}/.."

# --- Bump Go app version ---
echo "🔼 Bumping Go app version..."
go run "${SCRIPT_DIR}/bump_version.go"

# --- Load .env ---
ENV_FILE="${PROJECT_ROOT}/.env"
if [ -f "$ENV_FILE" ]; then
  export $(grep -v '^#' "$ENV_FILE" | xargs)
else
  echo "❌ .env file not found at ${ENV_FILE}!"
  exit 1
fi

# --- Extract version ---
VERSION=${APP_VERSION:-"0.0.1"}

# --- AWS & ECR setup ---
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REGION=${AWS_REGION:-"ap-southeast-1"}
REPO_NAME=${ECR_REPO_NAME:-"pixelsofts/gokafka-raw"}

# Derived tags
IMAGE_TAG="${AWS_ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${REPO_NAME}:v${VERSION}"
IMAGE_TAG_LATEST="${AWS_ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${REPO_NAME}:latest"

echo "📦 Building and pushing image..."
echo "🔹 Repository: ${REPO_NAME}"
echo "🔹 Region: ${REGION}"
echo "🔹 Version: v${VERSION}"

# --- Ensure ECR repo exists ---
aws ecr describe-repositories \
  --repository-names "${REPO_NAME}" \
  --region "${REGION}" >/dev/null 2>&1 || {
  echo "📁 Creating ECR repository ${REPO_NAME}..."
  aws ecr create-repository --repository-name "${REPO_NAME}" --region "${REGION}"
}

# --- Authenticate Docker to ECR ---
echo "🔐 Logging into ECR..."
aws ecr get-login-password --region "${REGION}" | docker login \
  --username AWS \
  --password-stdin "${AWS_ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com"

# --- Build multi-arch image ---
echo "🏗️  Building multi-arch image..."
docker buildx build \
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
