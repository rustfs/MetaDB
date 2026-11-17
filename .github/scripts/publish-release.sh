#!/usr/bin/env bash
# Attach Linux release archives and checksums to the GitHub Release for this tag.
# Does not publish crates.io packages. Requires GH_TOKEN and GITHUB_REF_NAME.

set -euo pipefail

if [[ -z "${GITHUB_REF_NAME:-}" ]]; then
  echo "GITHUB_REF_NAME is required" >&2
  exit 1
fi
if [[ -z "${GH_TOKEN:-}" ]]; then
  echo "GH_TOKEN is required" >&2
  exit 1
fi

root=$(cd "$(dirname "$0")/../.." && pwd)
cd "$root"

if [[ ! -d dist ]]; then
  echo "dist/ is missing; download the package artifacts first" >&2
  exit 1
fi

mapfile -t archives < <(find dist -type f -name 'metadb-*.tar.gz' ! -name '*.sha256' | sort)
if [[ ${#archives[@]} -lt 2 ]]; then
  echo "expected metadb tarballs for x86_64 and aarch64 under dist/" >&2
  find dist -type f -print >&2 || true
  exit 1
fi

for needle in x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu; do
  found=0
  for archive in "${archives[@]}"; do
    if [[ "$archive" == *"$needle"* ]]; then
      found=1
    fi
  done
  if [[ "$found" -ne 1 ]]; then
    echo "missing archive for ${needle}" >&2
    printf '%s\n' "${archives[@]}" >&2
    exit 1
  fi
done

stage=$(mktemp -d)
trap 'rm -rf "$stage"' EXIT
for archive in "${archives[@]}"; do
  sidecar="${archive}.sha256"
  if [[ ! -f "$sidecar" ]]; then
    echo "missing checksum sidecar: ${sidecar}" >&2
    exit 1
  fi
  cp -a "$archive" "$sidecar" "$stage/"
done

(
  cd "$stage"
  : > SHA256SUMS
  for sidecar in metadb-*.tar.gz.sha256; do
    cat "$sidecar" >> SHA256SUMS
  done
  sha256sum -c SHA256SUMS
)

notes=$(mktemp)
trap 'rm -rf "$stage"; rm -f "$notes"' EXIT
cat > "$notes" <<EOF
MetaDB ${GITHUB_REF_NAME}

Linux glibc packages built with \`cargo build --release --locked\` (not musl, not crates.io):

- \`x86_64-unknown-linux-gnu\` (\`ubuntu-24.04\`)
- \`aarch64-unknown-linux-gnu\` (\`ubuntu-24.04-arm\`)

Each \`metadb-<version>-<target>.tar.gz\` contains \`metadbd\`, \`metadb-ctl\`, \`LICENSE\`, and \`SHA256SUMS\` for those three files. \`<archive>.sha256\` checksums that tarball. The release \`SHA256SUMS\` checksums every tarball.

Archive names use the workspace package version from \`Cargo.toml\`. \`metadbd --version\` and \`metadb-ctl --version\` report that same version.

\`\`\`bash
sha256sum -c SHA256SUMS
tar -xzf metadb-<version>-<target>.tar.gz
cd metadb-<version>-<target>
sha256sum -c SHA256SUMS
./metadbd --version
./metadb-ctl --version
\`\`\`
EOF

shopt -s nullglob
assets=("$stage"/metadb-*.tar.gz "$stage"/metadb-*.tar.gz.sha256 "$stage"/SHA256SUMS)
if [[ ${#assets[@]} -lt 5 ]]; then
  echo "expected two archives, two sidecars, and SHA256SUMS" >&2
  printf '%s\n' "${assets[@]}" >&2
  exit 1
fi

create_args=(--title "$GITHUB_REF_NAME" --notes-file "$notes" --verify-tag)
if [[ "$GITHUB_REF_NAME" == *-* ]]; then
  create_args+=(--prerelease)
fi

if gh release view "$GITHUB_REF_NAME" >/dev/null 2>&1; then
  gh release upload "$GITHUB_REF_NAME" "${assets[@]}" --clobber
else
  gh release create "$GITHUB_REF_NAME" "${assets[@]}" "${create_args[@]}"
fi
