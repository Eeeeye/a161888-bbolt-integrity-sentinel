#!/bin/bash
set -uo pipefail

mkdir -p /logs/verifier
echo 0 > /logs/verifier/reward.txt

app_root="${APP_ROOT:-/app}"
test_root="${TEST_ROOT:-/tests}"
backup_root="$(mktemp -d)"
original_tests="$backup_root/upstream-tests"
mkdir -p "$original_tests"

restore() {
  rm -f "$app_root/zz_activity161888_root_test.go"
  rm -f "$app_root/zz_activity161888_extended_test.go"
  rm -f "$app_root/internal/surgeon/zz_activity161888_surgeon_test.go"

  if [[ -f "$backup_root/go.mod" ]]; then
    cp "$backup_root/go.mod" "$app_root/go.mod"
  fi
  if [[ -f "$backup_root/go.sum" ]]; then
    cp "$backup_root/go.sum" "$app_root/go.sum"
  fi

  while IFS= read -r -d '' saved; do
    relative="${saved#"$original_tests"/}"
    mkdir -p "$(dirname "$app_root/$relative")"
    mv "$saved" "$app_root/$relative"
  done < <(find "$original_tests" -type f -name '*_test.go' -print0)
  rm -rf "$backup_root"
}
trap restore EXIT

cp "$app_root/go.mod" "$backup_root/go.mod"
cp "$app_root/go.sum" "$backup_root/go.sum"

while IFS= read -r -d '' existing; do
  relative="${existing#"$app_root"/}"
  mkdir -p "$(dirname "$original_tests/$relative")"
  mv "$existing" "$original_tests/$relative"
done < <(find "$app_root" -type f -name '*_test.go' -print0)

cp "$test_root/activity161888_root_test.go" "$app_root/zz_activity161888_root_test.go"
cp "$test_root/activity161888_extended_test.go" "$app_root/zz_activity161888_extended_test.go"
cp "$test_root/activity161888_surgeon_test.go" "$app_root/internal/surgeon/zz_activity161888_surgeon_test.go"

cd "$app_root" || exit 1
export GOTOOLCHAIN=local
export GOWORK=off
export GOPROXY=off
export GOSUMDB=off

if ! go mod edit -replace="golang.org/x/sys=$test_root/deps/xsys"; then
  exit 1
fi

if ! timeout 240s go test -count=1 -run '^TestActivity161888' . ./internal/surgeon; then
  exit 1
fi

if ! timeout 120s go build . ./internal/surgeon; then
  exit 1
fi

echo 1 > /logs/verifier/reward.txt
