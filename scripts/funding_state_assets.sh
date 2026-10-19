#!/usr/bin/env bash
# The funding state release asset, shared by release-data.yml and funding-catchup.yml.
#
#   latest-tag          print the newest data-YYYY-MM-DD release tag (fails if gh does)
#   restore             restore the newest gmx-funding-state-<stamp>.tar.gz; always exits 0
#   names TAG           print TAG's state asset names (fails if TAG cannot be inspected)
#   pack DIR            tar user_data/data/gmx/funding/ into DIR/gmx-funding-state-<stamp>.tar.gz
#   publish TAG FILE [--no-delete]
#                       upload FILE under its own name, then delete the older state assets
#                       of TAG once `gh release view` confirms FILE's size
#
# Needs GITHUB_REPOSITORY and GH_TOKEN. `restore` reads PREVIOUS_TAG and, on success,
# appends FUNDING_STATE_RELEASE / FUNDING_STATE_ASSET to GITHUB_ENV so a later step
# can put the successor back in the release the state came from.
#
# Safety rules (the state may be the only resumable copy): a state asset is never
# replaced in place, the name is unique per upload, nothing is deleted until the new
# asset is confirmed present with the right size, and a failed inspection is
# "unknown", never "no state" -- it deletes nothing.

STATE_RE='^gmx-funding-state-[0-9]{8}T[0-9]{6}Z\.tar\.gz$'
DATA_TAG_JQ='[.[].tagName | select(test("^data-[0-9]{4}-[0-9]{2}-[0-9]{2}$"))] | sort | reverse | .[]'

state_names() {
  local names
  names=$(gh release view "$1" --repo "${GITHUB_REPOSITORY}" --json assets \
      --jq '.assets[].name') || return 1
  printf '%s\n' "${names}" | grep -E "${STATE_RE}" || true
}

cmd_latest_tag() {
  local tags
  tags=$(gh release list --repo "${GITHUB_REPOSITORY}" --limit 100 --json tagName \
      --jq "${DATA_TAG_JQ}") || return 1
  printf '%s\n' "${tags}" | head -1
}

# Unpacked beside the tree and moved in whole, so a failed restore can never leave
# a half-restored funding/ directory that would then be re-published. Fail-soft: an
# absent or unrestorable state only means the funding companions are withheld.
cmd_restore() {
  local found="" asset="" tag names cand tmp ok attempt
  if [ -z "${PREVIOUS_TAG}" ]; then
    echo "No prior data release (allow_fresh_seed): no funding state to restore."
    return 0
  fi
  for tag in $(gh release list --repo "${GITHUB_REPOSITORY}" --limit 100 --json tagName \
      --jq "${DATA_TAG_JQ}"); do
    names=$(gh release view "${tag}" --repo "${GITHUB_REPOSITORY}" --json assets \
        --jq '.assets[].name' 2>/dev/null) || continue
    cand=$(printf '%s\n' "${names}" | grep -E "${STATE_RE}" | sort | tail -1)
    if [ -n "${cand}" ]; then
      found="${tag}"
      asset="${cand}"
      break
    fi
  done
  if [ -z "${found}" ]; then
    echo "::warning::no gmx-funding-state asset in any retained release; funding state starts absent (seed it once with seed_funding_from)."
    return 0
  fi
  if [ "${found}" != "${PREVIOUS_TAG}" ]; then
    echo "::warning::${PREVIOUS_TAG} has no funding state asset; restoring ${asset} from ${found}."
  fi

  tmp=$(mktemp -d)
  ok=0
  for attempt in 1 2 3; do
    if gh release download "${found}" --repo "${GITHUB_REPOSITORY}" \
        --pattern "${asset}" --dir "${tmp}" --clobber \
        && mkdir -p "${tmp}/x" \
        && tar -xzf "${tmp}/${asset}" -C "${tmp}/x" \
        && [ -d "${tmp}/x/user_data/data/gmx/funding" ]; then
      ok=1
      break
    fi
    echo "funding state restore attempt ${attempt} failed; retrying."
    rm -rf "${tmp}/x"
    sleep $((attempt * ${RESTORE_RETRY_SECONDS:-10}))
  done
  if [ "${ok}" = "1" ] && [ ! -e user_data/data/gmx/funding ]; then
    mkdir -p user_data/data/gmx
    mv "${tmp}/x/user_data/data/gmx/funding" user_data/data/gmx/funding
    find user_data/data/gmx/funding -name '._*' -delete 2>/dev/null || true
    echo "Restored funding state from ${found}."
    if [ -n "${GITHUB_ENV:-}" ]; then
      printf 'FUNDING_STATE_RELEASE=%s\nFUNDING_STATE_ASSET=%s\n' "${found}" "${asset}" >> "${GITHUB_ENV}"
    fi
  else
    echo "::error::could not restore ${asset} from ${found}; funding state is absent this run."
  fi
  rm -rf "${tmp}"
  return 0
}

# A unique name per run: it is uploaded beside any older state asset, which is
# deleted only once this one is confirmed.
cmd_pack() {
  local dir="$1" name
  if [ -d user_data/data/gmx/funding/arbitrum/raw ]; then
    name="gmx-funding-state-$(date -u +%Y%m%dT%H%M%SZ).tar.gz"
    tar -czf "${dir}/${name}" user_data/data/gmx/funding/
  else
    echo "::warning::no funding state on disk; publishing without a funding state asset"
  fi
}

# Returns 1 if the upload, its confirmation, or a delete failed. A name inspection
# that fails (or --no-delete) leaves the older assets in place without failing.
cmd_publish() {
  local tag="$1" file="$2" delete=1 name want got old rc=0 other
  [ "${3:-}" = "--no-delete" ] && delete=0
  name=$(basename "${file}")
  if ! gh release upload "${tag}" --repo "${GITHUB_REPOSITORY}" "${file}"; then
    echo "::error::uploading ${name} failed; the older state asset is untouched."
    return 1
  fi
  want=$(stat -c %s "${file}")
  got=$(gh release view "${tag}" --repo "${GITHUB_REPOSITORY}" --json assets \
      --jq ".assets[] | select(.name==\"${name}\") | .size") || got=""
  if [ "${got}" != "${want}" ]; then
    echo "::error::${name} is not present with the expected size (want ${want}, got '${got}'); keeping the older state."
    return 1
  fi
  if [ "${delete}" = "1" ] && old=$(state_names "${tag}"); then
    for other in ${old}; do
      if [ "${other}" != "${name}" ]; then
        gh release delete-asset "${tag}" "${other}" --repo "${GITHUB_REPOSITORY}" --yes || rc=1
      fi
    done
  else
    echo "::warning::could not inspect ${tag} (or deletes are off); older state assets were left in place."
  fi
  return "${rc}"
}

case "${1:-}" in
  latest-tag) cmd_latest_tag ;;
  restore) cmd_restore ;;
  names) state_names "$2" ;;
  pack) cmd_pack "$2" ;;
  publish) cmd_publish "$2" "$3" "${4:-}" ;;
  *) echo "usage: $0 {latest-tag|restore|names TAG|pack DIR|publish TAG FILE [--no-delete]}" >&2; exit 2 ;;
esac
