#!/usr/bin/env sh

set -eu

msg_file="${1:?commit message path is required}"
header="$(sed -n '1p' "$msg_file" | tr -d '\r')"

# Allow Git-generated merge commits and standard revert/fixup flows.
case "$header" in
  Merge\ *|Revert\ *|fixup!\ *|squash!\ *)
    exit 0
    ;;
esac

pattern='^(feat|fix|docs|style|refactor|perf|test|build|ci|chore|revert)(\([a-z0-9._/-]+\))?(!)?: .+'

if printf '%s\n' "$header" | grep -Eq "$pattern"; then
  exit 0
fi

cat >&2 <<'EOF'
Invalid commit message.

Expected format:
  type(scope): subject
  type: subject
  type(scope)!: subject

Allowed types:
  feat, fix, docs, style, refactor, perf, test, build, ci, chore, revert

Examples:
  feat(worker): add ordering key visibility check
  fix(http): reject unsafe redirect targets
  chore(ci): tighten PR merge gate
EOF

exit 1
