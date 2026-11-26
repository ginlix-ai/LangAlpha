#!/usr/bin/env bash
# The tags that keep version order and commit order together on the feed: the
# newest stable release, and the commit the nightly tag names. Every lookup that
# fails stops the caller rather than reading as "none", since "none" is what
# lifts the ordering checks.
#
#   nightly-refs.sh stable        the newest stable version, empty when there is none
#   nightly-refs.sh commit TAG    the commit TAG names, empty when the tag does not exist
#   nightly-refs.sh nightly       the commit the nightly names, empty when there never was one
#
#   env  GH_TOKEN, REPO
#        FEED   the update feed's address, empty when none is configured
set -eo pipefail

NIGHTLY_TAG=desktop-oss-nightly
err=$(mktemp)
trap 'rm -f "$err"' EXIT

# A failed lookup prints its error body to stdout, past --jq, so only the exit
# status and the message on stderr say what happened.
commit_of () {
  local ref at
  if ! ref=$(gh api "repos/$REPO/git/ref/tags/$1" --jq '.object.type + " " + .object.sha' 2>"$err"); then
    grep -q 'HTTP 404' "$err" && return 0
    cat "$err" >&2
    echo "::error::could not read where $1 points" >&2
    return 1
  fi
  at=${ref#* }
  # An annotated tag is peeled to its commit.
  if [ "${ref%% *}" = tag ]; then
    at=$(gh api "repos/$REPO/git/tags/$at" --jq '.object.sha' 2>"$err") || {
      cat "$err" >&2
      echo "::error::could not peel $1 to its commit" >&2
      return 1
    }
  fi
  echo "$at"
}

case "$1" in
  stable)
    # Stable tags only: a nightly or an rc is never the base.
    refs=$(gh api "repos/$REPO/git/matching-refs/tags/desktop-oss-v" --paginate --jq '.[].ref' 2>"$err") || {
      cat "$err" >&2
      echo "::error::could not list the stable releases" >&2
      exit 1
    }
    printf '%s\n' "$refs" | sed 's#^refs/tags/desktop-oss-v##' | { grep -E '^[0-9]+\.[0-9]+\.[0-9]+$' || true; } \
      | sort -t. -k1,1n -k2,2n -k3,3n | tail -n1
    ;;
  commit)
    commit_of "$2"
    ;;
  nightly)
    at=$(commit_of "$NIGHTLY_TAG")
    if [ -n "$at" ] || [ -z "$FEED" ]; then
      echo "$at"
      exit 0
    fi
    # No tag, but the nightly run replaces its release by deleting it, tag and
    # all, so a run that failed before making the new one leaves none behind.
    # Only a feed with no nightly in it says there never was one.
    for m in nightly.yml nightly-mac.yml nightly-linux.yml; do
      code=$(curl -sSL -o /dev/null -w '%{http_code}' --max-time 60 "$FEED/$m" 2>"$err" || true)
      case "$code" in
        404) ;;
        200)
          echo "::error::$NIGHTLY_TAG is missing but the feed serves $m, so nothing says which commit the live nightly is. Rerun the publish job of the nightly run that failed, which puts the tag back at the commit it built. Tag by hand only a commit at or past every nightly ever released, not the one the feed serves: a release whose feed upload failed was still downloaded." >&2
          exit 1
          ;;
        *)
          cat "$err" >&2
          echo "::error::could not tell whether the feed serves a nightly: $FEED/$m answered ${code:-nothing}" >&2
          exit 1
          ;;
      esac
    done
    ;;
  *)
    echo "usage: nightly-refs.sh stable | commit TAG | nightly" >&2
    exit 2
    ;;
esac
