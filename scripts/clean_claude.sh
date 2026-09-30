#!/usr/bin/env bash
# Start Claude Code clean: no memories, CLAUDE.md, skills, plugins, claude.ai connectors or session history, wired to a
# tallyman checkout on a fresh, empty data dir. For watching how Claude handles a dataset it has never seen.
#
# Usage:
#   scripts/clean_claude.sh <tallyman-checkout> [dataset-file-or-dir ...] [-- claude-args ...]
#
# e.g. scripts/clean_claude.sh ~/tallyman ~/data/taxi.parquet -- --permission-mode acceptEdits
#
# What it does:
#   1. Stops every running `tallyman run` companion (any checkout, any port) and the Buckaroo it spawned. `tallyman
#      mcp` servers are left alone: each belongs to a live Claude session, and killing one drops that session's tools.
#   2. Makes a run dir $CLEAN_RUNS_DIR/<timestamp>/ holding
#        home/  a fresh TALLYMAN_HOME with one empty project (no fixture)
#        work/  Claude's cwd, with any dataset files copied in
#      The run dir sits outside every repo, so no CLAUDE.md is discovered, and a new cwd means an empty auto-memory.
#   3. Starts a companion from the checkout on that data dir, so the UI shows only this run.
#   4. Runs claude from work/ with its own config dir (log in once; it is kept across runs) and only the tallyman MCP
#      server, pointed at the fresh data dir.
#   When claude exits the companion is stopped. The run dir is kept so the catalog can be looked at afterwards.
#
# Overrides (env):
#   CLEAN_PROJECT            project name in the fresh data dir (default blind)
#   CLEAN_RUNS_DIR           where run dirs go (default ~/.tallyman-clean)
#   CLEAN_CLAUDE_CONFIG_DIR  claude's config dir (default ~/.claude-clean)
#   TALLYMAN_PORT            companion port (default 7860)
#   TALLYMAN_BUCKAROO_PORT   buckaroo port (default 8700)
set -uo pipefail

usage() { sed -n '4,8p' "$0" | sed 's/^# \{0,1\}//'; exit 2; }

(( $# )) || usage
[[ "$1" == "-h" || "$1" == "--help" ]] && usage
CHECKOUT="$(cd "$1" 2>/dev/null && pwd)" || { echo "not a directory: $1"; exit 2; }
shift
[[ -f "$CHECKOUT/src/tallyman_cli/main.py" ]] || { echo "not a tallyman checkout: $CHECKOUT"; exit 2; }

DATASETS=()
while (( $# )) && [[ "$1" != "--" ]]; do
  [[ -e "$1" ]] || { echo "no such dataset: $1"; exit 2; }
  DATASETS+=("$1")
  shift
done
[[ "${1:-}" == "--" ]] && shift
CLAUDE_ARGS=("$@")

PROJECT="${CLEAN_PROJECT:-blind}"
RUNS_DIR="${CLEAN_RUNS_DIR:-$HOME/.tallyman-clean}"
CLAUDE_DIR="${CLEAN_CLAUDE_CONFIG_DIR:-$HOME/.claude-clean}"
PORT="${TALLYMAN_PORT:-7860}"
BK_PORT="${TALLYMAN_BUCKAROO_PORT:-8700}"

# A project path or root left in this shell would point the fresh servers at some other project.
unset TALLYMAN_PROJECT_PATH TALLYMAN_PROJECT_ROOT

# Only ever returns the pid(s) LISTENING on a port, never client or browser connections.
listener() { lsof -ti TCP:"$1" -sTCP:LISTEN 2>/dev/null; }

wait_exit() {  # pid, timeout_s
  local p="$1" t="$2" i=0
  while kill -0 "$p" 2>/dev/null; do
    (( i++ >= t * 2 )) && return 1
    sleep 0.5
  done
}

wait_up() {  # port, timeout_s
  local p="$1" t="$2" i=0
  while [[ -z "$(listener "$p")" ]]; do
    (( i++ >= t * 2 )) && return 1
    sleep 0.5
  done
}

stop_pid() {  # pid, label
  local pid="$1" label="$2"
  echo "  stopping $label pid=$pid: $(ps -ww -o command= -p "$pid" 2>/dev/null)"
  kill -TERM "$pid" 2>/dev/null
  if ! wait_exit "$pid" 12; then
    echo "    still running after 12s, SIGKILL"
    kill -KILL "$pid" 2>/dev/null
    wait_exit "$pid" 5 || { echo "    FAILED to stop pid $pid"; exit 1; }
  fi
}

# ---- 1. Stop previous servers ----------------------------------------------
echo "stopping previous tallyman and buckaroo servers ..."
# The python process, not its `uv run` wrapper: SIGTERM there runs the companion's shutdown, which stops its Buckaroo
# and releases its data dir, and the wrapper exits with it.
for pid in $(pgrep -f 'bin/tallyman run( |$)'); do
  stop_pid "$pid" "companion"
done
# A Buckaroo orphaned by a companion that was SIGKILLed, found by the flag only a tallyman-spawned one runs with, or by
# the port.
for pid in $(pgrep -f 'buckaroo\.server .*--stdio-control') $(listener "$BK_PORT"); do
  kill -0 "$pid" 2>/dev/null && stop_pid "$pid" "buckaroo"
done
for p in "$PORT" "$BK_PORT"; do
  holder="$(listener "$p" | head -1)"
  if [[ -n "$holder" ]]; then
    echo "  :$p is still held by pid=$holder: $(ps -ww -o command= -p "$holder")"
    echo "  not a tallyman server, so not killing it. Free the port or set TALLYMAN_PORT / TALLYMAN_BUCKAROO_PORT."
    exit 1
  fi
done

# ---- 2. Fresh data dir and work dir ----------------------------------------
RUN_DIR="$RUNS_DIR/$(date +%Y%m%d-%H%M%S)"
HOME_DIR="$RUN_DIR/home"
WORK_DIR="$RUN_DIR/work"
mkdir -p "$HOME_DIR" "$WORK_DIR" "$CLAUDE_DIR" || exit 1
for d in ${DATASETS[@]+"${DATASETS[@]}"}; do
  cp -R "$d" "$WORK_DIR/" || exit 1
done
echo "run dir: $RUN_DIR"

echo "creating project '$PROJECT' in the fresh data dir ..."
( cd "$CHECKOUT" && TALLYMAN_HOME="$HOME_DIR" uv run tallyman init "$PROJECT" --no-fixture ) || exit 1

# ---- 3. Companion on the fresh data dir ------------------------------------
LOG="$RUN_DIR/companion.log"
echo "starting companion: (cd $CHECKOUT && uv run tallyman run --project $PROJECT --port $PORT --buckaroo-port $BK_PORT) -> $LOG"
# `;`, not `&&`, so `&` backgrounds nohup alone and this script's stdout isn't held open by a subshell.
( cd "$CHECKOUT" || exit 1; TALLYMAN_HOME="$HOME_DIR" TALLYMAN_PROJECT="$PROJECT" nohup uv run tallyman run --project "$PROJECT" --port "$PORT" --buckaroo-port "$BK_PORT" >"$LOG" 2>&1 </dev/null & )
if ! wait_up "$PORT" 60; then
  echo "FAILED: companion did not come up on :$PORT within 60s"
  echo "--- last 30 log lines ($LOG) ---"
  tail -n 30 "$LOG"
  exit 1
fi
COMPANION_PID="$(listener "$PORT" | head -1)"
stop_companion() {
  if kill -0 "$COMPANION_PID" 2>/dev/null; then
    echo "stopping companion pid=$COMPANION_PID ..."
    kill -TERM "$COMPANION_PID" 2>/dev/null
    wait_exit "$COMPANION_PID" 12 || kill -KILL "$COMPANION_PID" 2>/dev/null
  fi
}
trap stop_companion EXIT
trap 'exit 130' INT TERM
CODE="$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$PORT/catalog")"
echo "OK  companion pid=$COMPANION_PID on :$PORT (GET /catalog -> $CODE), http://127.0.0.1:$PORT"
wait_up "$BK_PORT" 20 && echo "OK  buckaroo on :$BK_PORT" || echo "WARN  buckaroo not up on :$BK_PORT (see $LOG)"

# ---- 4. Claude --------------------------------------------------------------
MCP_CONFIG="$(jq -cn --arg cwd "$CHECKOUT" --arg home "$HOME_DIR" --arg project "$PROJECT" '{mcpServers: {tallyman: {command: "uv", args: ["run", "tallyman", "mcp"], cwd: $cwd, env: {TALLYMAN_HOME: $home, TALLYMAN_PROJECT: $project}}}}')"
echo "starting claude in $WORK_DIR (config dir $CLAUDE_DIR) ..."
cd "$WORK_DIR" || exit 1
# CLAUDE_CONFIG_DIR drops ~/.claude (global CLAUDE.md, memories, skills, plugins, history); --strict-mcp-config drops
# every MCP server but the one given; ENABLE_CLAUDEAI_MCP_SERVERS=false drops the claude.ai account connectors.
CLAUDE_CONFIG_DIR="$CLAUDE_DIR" ENABLE_CLAUDEAI_MCP_SERVERS=false claude --strict-mcp-config --mcp-config "$MCP_CONFIG" ${CLAUDE_ARGS[@]+"${CLAUDE_ARGS[@]}"}
echo "claude exited. Run dir kept: $RUN_DIR"
