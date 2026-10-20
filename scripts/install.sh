#!/usr/bin/env bash
# One-step install and run: deps, .env, schema, doctor, then the launchd agent that keeps
# `usfin serve` (scheduler + web UI on http://127.0.0.1:8660) running from login.
#
#   scripts/install.sh               install (or reinstall) and start
#   scripts/install.sh --no-agent    everything but the launchd agent
#   scripts/install.sh --print-plist print the agent plist for this checkout and exit
#   scripts/install.sh --uninstall   stop and remove the agent (data/, raw/, .env are kept)
#
# serve catches up at startup: every scheduled job whose last due fire is later than its last
# ok run is run once, so a first install (or a Mac that was off for days) fills in by itself.
set -euo pipefail

LABEL=com.tuntun.usfin.serve
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEMPLATE="$REPO/deploy/$LABEL.plist.example"
AGENT="$HOME/Library/LaunchAgents/$LABEL.plist"
DOMAIN="gui/$(id -u)"
PORT=8660

say() { printf '==> %s\n' "$*"; }
die() { printf 'error: %s\n' "$*" >&2; exit 1; }

uv_bin() {
  command -v uv || die "uv not found: install it (https://docs.astral.sh/uv/), then rerun"
}

# The example with this checkout's uv, repo and log paths filled in.
render_plist() {
  local uv dir
  uv="$(uv_bin)"
  dir="$(dirname "$uv")"
  sed -e "s#/opt/homebrew/bin/uv#$uv#" \
      -e "s#/opt/homebrew/bin:#$dir:#" \
      -e "s#/Users/tomtomtomtom/Projects/sec-edgar#$REPO#g" \
      "$TEMPLATE"
}

stop_agent() {
  if launchctl print "$DOMAIN/$LABEL" >/dev/null 2>&1; then
    launchctl bootout "$DOMAIN/$LABEL"
    # bootout returns before the process is gone; wait for the DB lock to be released
    for _ in $(seq 1 20); do
      launchctl print "$DOMAIN/$LABEL" >/dev/null 2>&1 || break
      sleep 0.5
    done
  fi
}

# Required keys must be non-empty in .env or the environment (real env wins, as in usfin).
check_env() {
  local missing=() key val
  for key in SEC_USER_AGENT BLS_CONTACT_EMAIL; do
    val="${!key:-}"
    [[ -z "$val" ]] && val="$(sed -n "s/^$key=//p" "$REPO/.env" | tail -1)"
    [[ -z "$val" ]] && missing+=("$key")
  done
  if (( ${#missing[@]} )); then
    die "fill in ${missing[*]} in $REPO/.env, then rerun"
  fi
  val="${FRED_API_KEY:-$(sed -n 's/^FRED_API_KEY=//p' "$REPO/.env" | tail -1)}"
  [[ -z "$val" ]] && say "warning: FRED_API_KEY is empty — job fred (layer-0 series) will be skipped"
  return 0
}

case "${1:-}" in
  --print-plist) render_plist; exit 0 ;;
  --uninstall)
    say "stopping $LABEL"
    stop_agent
    rm -f "$AGENT"
    say "removed $AGENT (data/, raw/ and .env kept)"
    exit 0 ;;
  ""|--no-agent) ;;
  *) die "unknown option $1 (see the header of $0)" ;;
esac

cd "$REPO"
UV="$(uv_bin)"

say "uv sync"
"$UV" sync --frozen

if [[ ! -f .env ]]; then
  cp .env.example .env
  die "created .env from .env.example: fill in SEC_USER_AGENT and BLS_CONTACT_EMAIL, then rerun"
fi
check_env

# A running serve holds the DB between jobs only briefly, but migrate must not race it.
[[ "${1:-}" == "--no-agent" ]] || stop_agent

say "usfin db migrate"
"$UV" run usfin db migrate
say "usfin doctor"
"$UV" run usfin doctor

if [[ "${1:-}" == "--no-agent" ]]; then
  say "done (no agent): start it by hand with 'uv run usfin serve'"
  exit 0
fi

say "installing $AGENT"
mkdir -p "$(dirname "$AGENT")" data
render_plist > "$AGENT"
plutil -lint "$AGENT" >/dev/null
launchctl bootstrap "$DOMAIN" "$AGENT"
# RunAtLoad on a Background agent can stay a pended "speculative" spawn; start it now
launchctl kickstart "$DOMAIN/$LABEL"

say "waiting for http://127.0.0.1:$PORT"
for _ in $(seq 1 60); do
  if curl -fsS -o /dev/null "http://127.0.0.1:$PORT/" 2>/dev/null; then
    say "running: http://127.0.0.1:$PORT (log: $REPO/data/usfin-serve.log)"
    say "startup catch-up runs any job that missed its due fire; watch it on the jobs page"
    exit 0
  fi
  sleep 1
done
die "serve did not answer on port $PORT within 60 s; see $REPO/data/usfin-serve.log"
