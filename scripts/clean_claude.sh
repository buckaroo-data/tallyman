#!/usr/bin/env bash
# Start Claude Code clean against a tallyman checkout: no memories, CLAUDE.md, skills, plugins, claude.ai connectors
# or session history, on a fresh, empty tallyman data dir. For watching how Claude handles a dataset it has never seen.
# Run with --help for the flags.
set -uo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/clean_claude.sh [options] [-- claude-args ...]

Starts a tallyman companion on a fresh data dir and runs claude next to it with nothing but the tallyman MCP server.

Options:
  --data PATH           File or dir copied into Claude's working dir. Repeatable.
  --checkout DIR        tallyman checkout that runs the companion and MCP server. Default: the one this script is in.
  --project NAME        Project created in the fresh data dir. Default: blind
  --perms LEVEL         How much Claude may do without asking. Default: tallyman
                          ask       prompt for everything (claude's defaults)
                          tallyman  tallyman MCP tools pre-approved; Bash and edits still prompt
                          accept    tallyman, plus file edits auto-accepted
                          bypass    nothing prompts. Fine for an unattended demo; Bash can still reach your real files
  --keep-servers        Don't stop other running tallyman companions first. Pick free ports with the two flags below.
  --port N              Companion port. Default: 7860
  --buckaroo-port N     Buckaroo port. Default: 8700
  --runs-dir DIR        Where run dirs go. Default: ~/.tallyman-clean
  --config-dir DIR      claude's config dir; log in once and it is kept across runs. Default: ~/.claude-clean
  -h, --help            Show this help.

Everything after `--` is passed to claude as is.

Examples:
  scripts/clean_claude.sh --data ~/code/tallyman_nfl_demo
  scripts/clean_claude.sh --data a.parquet --data b.parquet --perms bypass -- --model sonnet
  scripts/clean_claude.sh --keep-servers --port 7861 --buckaroo-port 8701 --data ~/data/taxi.parquet

Each run gets its own dir, kept after claude exits so the catalog can be looked at:
  <runs-dir>/<timestamp>/home/           the fresh TALLYMAN_HOME
  <runs-dir>/<timestamp>/work/           Claude's cwd, holding the --data copies
  <runs-dir>/<timestamp>/companion.log
The run dir sits outside every repo, so no CLAUDE.md is found, and a new cwd means an empty auto-memory.
EOF
}

die() { echo "$*" >&2; exit 2; }

# ---- Arguments ---------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CHECKOUT="$(dirname "$SCRIPT_DIR")"
DATASETS=()
PROJECT="blind"
PERMS="tallyman"
KEEP_SERVERS=0
PORT=7860
BK_PORT=8700
RUNS_DIR="$HOME/.tallyman-clean"
CLAUDE_DIR="$HOME/.claude-clean"
CLAUDE_ARGS=()

while (( $# )); do
  # Options that take a value must have one. Checked here, not in a helper called as $(...), where `die` would only
  # leave the subshell.
  case "$1" in
    --data|--checkout|--project|--perms|--port|--buckaroo-port|--runs-dir|--config-dir)
      [[ $# -ge 2 && -n "$2" ]] || die "$1 needs a value (see --help)" ;;
  esac
  case "$1" in
    --data)          DATASETS+=("$2"); shift 2 ;;
    --checkout)      CHECKOUT="$2"; shift 2 ;;
    --project)       PROJECT="$2"; shift 2 ;;
    --perms)         PERMS="$2"; shift 2 ;;
    --keep-servers)  KEEP_SERVERS=1; shift ;;
    --port)          PORT="$2"; shift 2 ;;
    --buckaroo-port) BK_PORT="$2"; shift 2 ;;
    --runs-dir)      RUNS_DIR="$2"; shift 2 ;;
    --config-dir)    CLAUDE_DIR="$2"; shift 2 ;;
    -h|--help)       usage; exit 0 ;;
    --)              shift; CLAUDE_ARGS=("$@"); break ;;
    *)               die "unknown argument: $1 (see --help)" ;;
  esac
done

CHECKOUT="$(cd "$CHECKOUT" 2>/dev/null && pwd)" || die "--checkout is not a directory"
[[ -f "$CHECKOUT/src/tallyman_cli/main.py" ]] || die "not a tallyman checkout: $CHECKOUT"
for d in ${DATASETS[@]+"${DATASETS[@]}"}; do
  [[ -e "$d" ]] || die "no such --data path: $d"
done
case "$PERMS" in ask|tallyman|accept|bypass) ;; *) die "--perms must be ask, tallyman, accept or bypass" ;; esac

RUN_DIR="$RUNS_DIR/$(date +%Y%m%d-%H%M%S)"
HOME_DIR="$RUN_DIR/home"
WORK_DIR="$RUN_DIR/work"
LOG="$RUN_DIR/companion.log"

# A project path or root left in this shell would point the fresh servers at some other project.
unset TALLYMAN_PROJECT_PATH TALLYMAN_PROJECT_ROOT

# ---- Helpers -----------------------------------------------------------------

# The pid(s) LISTENING on a port. Never client or browser connections, so it is safe to kill what this returns.
listener() { lsof -ti TCP:"$1" -sTCP:LISTEN 2>/dev/null; }

is_running() { kill -0 "$1" 2>/dev/null; }
is_stopped() { ! is_running "$1"; }
port_is_up() { [[ -n "$(listener "$1")" ]]; }

# wait_until SECONDS COMMAND...: rerun COMMAND every half second until it succeeds; fail after SECONDS.
wait_until() {
  local seconds="$1"; shift
  local deadline=$(( SECONDS + seconds ))
  until "$@"; do
    (( SECONDS >= deadline )) && return 1
    sleep 0.5
  done
}

# stop_pid PID LABEL: SIGTERM, then SIGKILL if it is still up after 12s.
stop_pid() {
  local pid="$1" label="$2"
  echo "  stopping $label pid=$pid: $(ps -ww -o command= -p "$pid" 2>/dev/null)"
  kill -TERM "$pid" 2>/dev/null
  wait_until 12 is_stopped "$pid" && return
  echo "    still running after 12s, SIGKILL"
  kill -KILL "$pid" 2>/dev/null
  wait_until 5 is_stopped "$pid" || { echo "    FAILED to stop pid $pid"; exit 1; }
}

# ---- Steps -------------------------------------------------------------------

print_summary() {
  echo "checkout:  $CHECKOUT"
  echo "run dir:   $RUN_DIR"
  echo "project:   $PROJECT"
  echo "data:      ${DATASETS[*]+${DATASETS[*]}}"
  echo "perms:     $PERMS"
  echo "ports:     companion :$PORT, buckaroo :$BK_PORT"
  echo "config:    $CLAUDE_DIR"
  echo
}

# Stop every `tallyman run` companion (any checkout, any port) and the Buckaroo it spawned. `tallyman mcp` servers are
# left alone: each belongs to a live Claude session, and killing one drops that session's tools.
stop_old_servers() {
  echo "stopping previous tallyman and buckaroo servers ..."
  # The python process, not its `uv run` wrapper: SIGTERM there runs the companion's shutdown, which stops its
  # Buckaroo and releases its data dir, and the wrapper exits with it.
  local pid
  for pid in $(pgrep -f 'bin/tallyman run( |$)'); do
    stop_pid "$pid" "companion"
  done
  # A Buckaroo orphaned by a SIGKILLed companion: found by the flag only a tallyman-spawned one runs with, or by port.
  for pid in $(pgrep -f 'buckaroo\.server .*--stdio-control') $(listener "$BK_PORT"); do
    is_running "$pid" && stop_pid "$pid" "buckaroo"
  done
}

# Refuse to start if something we don't own still holds one of our ports.
check_ports_free() {
  local p holder
  for p in "$PORT" "$BK_PORT"; do
    holder="$(listener "$p" | head -1)"
    [[ -z "$holder" ]] && continue
    echo ":$p is held by pid=$holder: $(ps -ww -o command= -p "$holder")"
    echo "Not killing it. Free the port, or pick others with --port / --buckaroo-port."
    exit 1
  done
}

# Make the run dir, copy the datasets into work/, and create the empty project (no fixture) in home/.
make_run_dir() {
  mkdir -p "$HOME_DIR" "$WORK_DIR" "$CLAUDE_DIR" || exit 1
  local d
  for d in ${DATASETS[@]+"${DATASETS[@]}"}; do
    cp -R "$d" "$WORK_DIR/" || exit 1
  done
  echo "creating project '$PROJECT' in $HOME_DIR ..."
  ( cd "$CHECKOUT" && TALLYMAN_HOME="$HOME_DIR" uv run tallyman init "$PROJECT" --no-fixture ) || exit 1
}

# Start a companion from the checkout on the fresh data dir, so the UI shows only this run. It is stopped when this
# script exits, however it exits.
start_companion() {
  echo "starting companion on :$PORT, log $LOG"
  # `;`, not `&&`, so `&` backgrounds nohup alone and this script's stdout isn't held open by a subshell.
  ( cd "$CHECKOUT" || exit 1; TALLYMAN_HOME="$HOME_DIR" TALLYMAN_PROJECT="$PROJECT" nohup uv run tallyman run --project "$PROJECT" --port "$PORT" --buckaroo-port "$BK_PORT" >"$LOG" 2>&1 </dev/null & )
  if ! wait_until 60 port_is_up "$PORT"; then
    echo "FAILED: companion did not come up on :$PORT within 60s. Last 30 log lines:"
    tail -n 30 "$LOG"
    exit 1
  fi
  COMPANION_PID="$(listener "$PORT" | head -1)"
  trap stop_companion EXIT
  trap 'exit 130' INT TERM
  # A 503 here usually means the checkout has no React build; the UI is down but the MCP tools still work.
  local body code
  body="$(curl -s -w '\n%{http_code}' "http://127.0.0.1:$PORT/catalog")"
  code="${body##*$'\n'}"
  if [[ "$code" == 200 ]]; then
    echo "OK  companion pid=$COMPANION_PID, http://127.0.0.1:$PORT"
  else
    echo "WARN  companion pid=$COMPANION_PID is up but GET /catalog -> $code: ${body%$'\n'*}"
  fi
  if wait_until 20 port_is_up "$BK_PORT"; then
    echo "OK  buckaroo on :$BK_PORT"
  else
    echo "WARN  buckaroo not up on :$BK_PORT (see $LOG)"
  fi
}

stop_companion() {
  is_running "$COMPANION_PID" || return
  echo "stopping companion pid=$COMPANION_PID ..."
  kill -TERM "$COMPANION_PID" 2>/dev/null
  wait_until 12 is_stopped "$COMPANION_PID" || kill -KILL "$COMPANION_PID" 2>/dev/null
}

# The MCP server claude will spawn. `--project` matters: claude ignores a `cwd` here and starts the server in work/,
# where a bare `uv run tallyman` finds no project and fails with "Failed to spawn: tallyman".
mcp_config() {
  jq -cn --arg checkout "$CHECKOUT" --arg home "$HOME_DIR" --arg project "$PROJECT" '{mcpServers: {tallyman: {
    command: "uv",
    args: ["run", "--project", $checkout, "tallyman", "mcp"],
    env: {TALLYMAN_HOME: $home, TALLYMAN_PROJECT: $project}}}}'
}

# Start the MCP server the way claude will, from work/, and fail now rather than inside claude if it can't.
check_mcp() {
  local out
  if ! out="$(cd "$WORK_DIR" && uv run --project "$CHECKOUT" tallyman mcp --help 2>&1)"; then
    echo "FAILED: the tallyman MCP server won't start from $WORK_DIR:"
    echo "$out"
    exit 1
  fi
  echo "OK  tallyman MCP server starts"
}

# Session settings for --perms. Deny rules hold in every mode, bypass included, but only for claude's Read tool: they
# keep it out of your real ~/.claude, other tallyman projects and the checkout's own CLAUDE.md and docs. Bash is not
# covered, so a determined `cat` still gets there.
# A bare tool name in deny removes that tool from claude's tool list, description included. A run has one project, so
# project_switch and project_new are only noise, and project_switch's description names a stale data dir
# (~/.tallyman) that sends the model looking for it on disk.
# claudeMdExcludes: claude looks for CLAUDE.md in every dir above its cwd and in each one's .claude/, so a run dir under
# $HOME picks up ~/.claude/CLAUDE.md, your global rules, as if it were a project file. CLAUDE_CONFIG_DIR doesn't stop
# that walk; excluding every CLAUDE.md does.
claude_settings() {
  local allow='[]'
  [[ "$PERMS" != "ask" ]] && allow='["mcp__tallyman"]'
  jq -cn --argjson allow "$allow" --arg checkout "$CHECKOUT" '{permissions: {
    allow: $allow,
    deny: ["Read(~/.claude/**)", "Read(~/.tallyman-notebooks/**)", ("Read(/" + $checkout + "/**)"),
      "mcp__tallyman__project_switch", "mcp__tallyman__project_new"]},
    claudeMdExcludes: ["**/CLAUDE.md", "**/CLAUDE.local.md", "**/.claude/rules/**"]}'
}

permission_mode() {
  case "$PERMS" in
    ask|tallyman) echo default ;;
    accept)       echo acceptEdits ;;
    bypass)       echo bypassPermissions ;;
  esac
}

# Run claude from work/ with its own config dir and only the tallyman MCP server. CLAUDE_CONFIG_DIR drops ~/.claude
# (global CLAUDE.md, memories, skills, plugins, history); --strict-mcp-config drops every MCP server but ours;
# ENABLE_CLAUDEAI_MCP_SERVERS=false drops the claude.ai account connectors.
# ENABLE_TOOL_SEARCH=false sends the tallyman tools up front instead of behind ToolSearch. Haiku doesn't take a tool
# that arrives as a ToolSearch result as callable: it runs `mcp__tallyman__catalog_import_source --outside_path ...`
# through Bash, or invents an `mcp-tallyman` CLI and wraps it in python, and each try is a Bash permission prompt.
run_claude() {
  echo "starting claude in $WORK_DIR ..."
  cd "$WORK_DIR" || exit 1
  local args=(--strict-mcp-config --mcp-config "$(mcp_config)")
  args+=(--settings "$(claude_settings)" --permission-mode "$(permission_mode)")
  args+=(${CLAUDE_ARGS[@]+"${CLAUDE_ARGS[@]}"})
  CLAUDE_CONFIG_DIR="$CLAUDE_DIR" ENABLE_CLAUDEAI_MCP_SERVERS=false ENABLE_TOOL_SEARCH=false claude "${args[@]}"
  echo "claude exited. Run dir kept: $RUN_DIR"
}

# ---- Main --------------------------------------------------------------------

print_summary
(( KEEP_SERVERS )) || stop_old_servers
check_ports_free
make_run_dir
check_mcp
start_companion
run_claude
