#!/bin/bash
set -euo pipefail

mkdir -p /logs/verifier
echo 0 > /logs/verifier/reward.txt

app_root="${APP_ROOT:-/app}"
test_root="${TEST_ROOT:-/tests}"
backup_root="$(mktemp -d)"
original_tests="$backup_root/upstream-tests"
mkdir -p "$original_tests" || exit 1

restore() {
  local status=0
  local relative
  rm -f "$app_root/zz_activity161888_root_test.go" || status=1
  rm -f "$app_root/zz_activity161888_extended_test.go" || status=1
  rm -f "$app_root/zz_activity161888_lifecycle_test.go" || status=1
  rm -f "$app_root/internal/surgeon/zz_activity161888_surgeon_test.go" || status=1

  if [[ -f "$backup_root/go.mod" ]]; then
    cp "$backup_root/go.mod" "$app_root/go.mod" || status=1
  fi
  if [[ -f "$backup_root/go.sum" ]]; then
    cp "$backup_root/go.sum" "$app_root/go.sum" || status=1
  fi

  while IFS= read -r -d '' saved; do
    relative="${saved#"$original_tests"/}"
    mkdir -p "$(dirname "$app_root/$relative")" || status=1
    mv "$saved" "$app_root/$relative" || status=1
  done < <(find "$original_tests" -type f -name '*_test.go' -print0)
  rm -rf "$backup_root" || status=1
  if (( status != 0 )); then
    echo 0 > /logs/verifier/reward.txt || true
  fi
  return "$status"
}
trap restore EXIT

cp "$app_root/go.mod" "$backup_root/go.mod" || exit 1
cp "$app_root/go.sum" "$backup_root/go.sum" || exit 1

while IFS= read -r -d '' existing; do
  relative="${existing#"$app_root"/}"
  mkdir -p "$(dirname "$original_tests/$relative")" || exit 1
  mv "$existing" "$original_tests/$relative" || exit 1
done < <(find "$app_root" -type f -name '*_test.go' -print0)

cp "$test_root/activity161888_root_test.go" "$app_root/zz_activity161888_root_test.go" || exit 1
cp "$test_root/activity161888_extended_test.go" "$app_root/zz_activity161888_extended_test.go" || exit 1
cp "$test_root/activity161888_lifecycle_test.go" "$app_root/zz_activity161888_lifecycle_test.go" || exit 1
cp "$test_root/activity161888_surgeon_test.go" "$app_root/internal/surgeon/zz_activity161888_surgeon_test.go" || exit 1

cd "$app_root" || exit 1
export GOTOOLCHAIN=local
export GOWORK=off
export GOPROXY=off
export GOSUMDB=off

if ! go mod edit -replace="golang.org/x/sys=$test_root/deps/xsys"; then
  exit 1
fi

test_output="$backup_root/go-test.json"
if ! timeout 240s go test -json -count=1 -run '^TestActivity161888' . ./internal/surgeon | tee "$test_output"; then
  exit 1
fi
if ! grep -Eq '"Action":"run".*"Test":"TestActivity161888' "$test_output"; then
  echo "no TestActivity161888 test was executed" >&2
  exit 1
fi

if ! timeout 120s go build . ./internal/surgeon; then
  exit 1
fi

if ! restore; then
  exit 1
fi
trap - EXIT
echo 1 > /logs/verifier/reward.txt
