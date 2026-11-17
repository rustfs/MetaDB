#!/usr/bin/env bash
# Package the release metadbd and metadb-ctl binaries into a versioned tarball.
# The caller builds first:
#   cargo build --release --locked -p metadb-server --bins
# EXPECT_TRIPLE, when set, must match `rustc -vV` host (the native runner target).

set -euo pipefail

root=$(cd "$(dirname "$0")/../.." && pwd)
cd "$root"

version=$(
  awk '
    $0 == "[workspace.package]" { in_pkg = 1; next }
    in_pkg && $0 ~ /^version = "/ {
      sub(/^version = "/, "")
      sub(/".*$/, "")
      print
      exit
    }
    in_pkg && $0 ~ /^\[/ { exit }
  ' Cargo.toml
)
if [[ -z "$version" ]]; then
  echo "workspace.package version not found in Cargo.toml" >&2
  exit 1
fi
if [[ ! "$version" =~ ^[0-9A-Za-z][0-9A-Za-z.+_-]*$ ]]; then
  echo "refusing unsafe package version: ${version}" >&2
  exit 1
fi

host=$(rustc -vV | awk '/^host: / { print $2 }')
if [[ -z "$host" ]]; then
  echo "could not determine rustc host triple" >&2
  exit 1
fi
if [[ -n "${EXPECT_TRIPLE:-}" && "$host" != "$EXPECT_TRIPLE" ]]; then
  echo "rustc host ${host} != expected ${EXPECT_TRIPLE}" >&2
  exit 1
fi

for bin in metadbd metadb-ctl; do
  path="target/release/${bin}"
  if [[ ! -x "$path" ]]; then
    echo "missing release binary: ${path}" >&2
    exit 1
  fi
  version_out=$("$path" --version)
  printf '%s\n' "$version_out"
  if [[ "$version_out" != "${bin} ${version}" ]]; then
    echo "expected '${bin} ${version}' from --version, got: ${version_out}" >&2
    exit 1
  fi
  help_out=$("$path" --help)
  if [[ "$help_out" != *"$bin"* ]]; then
    echo "${bin} --help did not mention ${bin}" >&2
    exit 1
  fi
done

desc=$(file -b target/release/metadbd)
printf '%s\n' "$desc"
case "$host" in
  x86_64-*)
    if [[ "$desc" != *x86-64* ]]; then
      echo "metadbd is not an x86-64 binary" >&2
      exit 1
    fi
    ;;
  aarch64-*)
    if [[ "$desc" != *aarch64* ]]; then
      echo "metadbd is not an aarch64 binary" >&2
      exit 1
    fi
    ;;
  *)
    echo "no ELF arch check for host ${host}" >&2
    ;;
esac

name="metadb-${version}-${host}"
stage=$(mktemp -d)
trap 'rm -rf "$stage"' EXIT
mkdir -p "${stage}/${name}"
install -m 0755 target/release/metadbd target/release/metadb-ctl "${stage}/${name}/"
install -m 0644 LICENSE "${stage}/${name}/"
(
  cd "${stage}/${name}"
  sha256sum metadbd metadb-ctl LICENSE > SHA256SUMS
  sha256sum -c SHA256SUMS
)

mkdir -p dist
tar -C "$stage" -czf "dist/${name}.tar.gz" "$name"
(
  cd dist
  sha256sum "${name}.tar.gz" > "${name}.tar.gz.sha256"
  sha256sum -c "${name}.tar.gz.sha256"
)

list=$(tar -tzf "dist/${name}.tar.gz")
for entry in "${name}/metadbd" "${name}/metadb-ctl" "${name}/LICENSE" "${name}/SHA256SUMS"; do
  if ! grep -F -x -q "$entry" <<<"$list"; then
    echo "archive missing ${entry}" >&2
    printf '%s\n' "$list" >&2
    exit 1
  fi
done

check_dir=$(mktemp -d)
tar -C "$check_dir" -xzf "dist/${name}.tar.gz"
(
  cd "${check_dir}/${name}"
  sha256sum -c SHA256SUMS
)
rm -rf "$check_dir"

if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
  echo "artifact=${name}" >> "$GITHUB_OUTPUT"
fi

echo "packaged dist/${name}.tar.gz"
