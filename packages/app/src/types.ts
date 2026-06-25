export interface Entry {
  content_hash: string;
  alias: string | null;
  version: number | null;
  is_current: boolean;
  version_count: number;
  row_count: number;
  execute_seconds: number;
  prompt: string | null;
  created_at: string;
}

export interface AppError {
  id: string;
  tool: string | null;
  created_at: string;
  message: string;
  code: string;
  prompt: string | null;
}

export interface SchemaField {
  name: string;
  type: string;
}

export interface Schema {
  fields: SchemaField[];
  row_count: number;
}

export interface EntryDetail {
  project: string;
  content_hash: string;
  manifest: {
    content_hash: string;
    row_count: number;
    execute_seconds: number;
    created_at: string;
    prompt: string | null;
    // Recorded tracked_expr_from_alias parent edges (raw manifest shape — no
    // resolved alias). Used to linkify the code's catalog refs (#135).
    parents?: { hash: string; ref: string; follow: boolean }[] | null;
  };
  schema: Schema;
  code: string;
  alias: string | null;
  version: number | null;
  forensic_history: Array<{ hash: string; version: number; is_current: boolean }>;
  prompt_history: Array<{ at: string; prompt: string }>;
  chart_spec: Record<string, unknown> | null;
  display_config: {
    column_config_overrides: Record<string, unknown>;
    diff_provenance?: {
      source_alias: string;
      va: number;
      vb: number;
      a_hash: string;
      b_hash: string;
      keys: string[];
    };
  } | null;
  build_artifacts: Array<{ name: string; text: string }>;
  total_rows: number;
  buckaroo_session: string | null;
  buckaroo_ws_base: string | null;
}

export interface NotebookCell {
  cell_id: string;
  alias: string;
  markdown: string;
  order: number;
  latest_hash: string | null;
  version: number | null;
  entry_meta: {
    content_hash: string;
    row_count: number;
    execute_seconds: number;
    prompt: string | null;
    created_at: string;
  } | null;
  schema: Schema | null;
  total_rows: number;
  chart_spec: Record<string, unknown> | null;
  buckaroo_session: string | null;
}

export interface NotebookFull {
  project: string;
  cells: NotebookCell[];
  buckaroo_ws_base: string | null;
  buckaroo_available: boolean;
}

export interface DiffData {
  project: string;
  alias: string;
  va: number;
  vb: number;
  a_hash: string;
  b_hash: string;
  hashes: string[];
  diff: {
    schema: {
      row_count: { before: number; after: number };
      added: string[];
      removed: string[];
      changed_type: Array<{ name: string; before: string; after: string }>;
    };
    stats: Array<{
      name: string;
      before: { count: number; nulls: number; distinct: number; numeric?: { mean: number } } | null;
      after: { count: number; nulls: number; distinct: number; numeric?: { mean: number } } | null;
    }>;
    keyed?: {
      keys: string[];
      matched: number;
      only_before: number;
      only_after: number;
      table_html: string;
    };
    head: {
      n: number;
      a_total: number;
      b_total: number;
      before: string;
      after: string;
    };
    code: string;
  };
  compare_session: string | null;
  buckaroo_ws_base: string | null;
}

export interface CacheEntry {
  hash: string;
  size: number;
  size_formatted: string;
  row_count: number;
  created: string;
  alias: string | null;
  version: number | null;
  is_current: boolean;
  prompt: string | null;
}

export interface ResultCache {
  project: string;
  entries: CacheEntry[];
  total: number;
  total_formatted: string;
}

export interface EntryCacheComponent {
  key: string;
  label: string;
  kind: "cache" | "artifact";
  reclaimable: boolean;
  exists: boolean;
  bytes: number;
  formatted: string;
  detail: string;
  /** Only present on the snapshot_cache component. */
  applicable?: boolean;
}

export interface EntryDiffCache {
  pair: string;
  other_hash_prefix: string;
  other_hash: string | null;
  other_alias: string | null;
  bytes: number;
  formatted: string;
}

export interface EntrySource {
  path: string;
  bytes: number;
  formatted: string;
  exists: boolean;
}

export interface EntryParent {
  hash: string;
  ref: string;
  follow: boolean;
  alias: string | null;
}

export interface EntryChild {
  hash: string;
  alias: string | null;
}

export interface EntryCache {
  project: string;
  content_hash: string;
  cache_worthy: boolean;
  cache_worthy_why: string | null;
  created_at: string | null;
  modified_at: string | null;
  components: EntryCacheComponent[];
  diff_caches: EntryDiffCache[];
  cache_bytes: number;
  cache_formatted: string;
  artifact_bytes: number;
  artifact_formatted: string;
  diff_cache_bytes: number;
  diff_cache_formatted: string;
  source_bytes: number;
  source_formatted: string;
  source_tracked: boolean;
  sources: EntrySource[];
  parents: EntryParent[];
  children: EntryChild[];
  total_bytes: number;
  total_formatted: string;
}

export interface SessionResult {
  status: "ok" | "unavailable" | "no_build" | "timeout" | "error";
  ws_url: string | null;
  detail: string;
}

export interface LogEvent {
  ts: string;
  kind: "build_ok" | "build_error" | "alias_set" | "buckaroo" | string;
  category: "mcp" | "alias" | "buckaroo" | string;
  session?: string | null;
  origin?: string | null;
  tool?: string | null;
  alias?: string | null;
  version?: number | null;
  hash?: string | null;
  prompt?: string | null;
  code?: string | null;
  message?: string | null;
  traceback?: string | null;
  status?: string | null;
  detail?: string | null;
  error_id?: string | null;
  load_ms?: number | null;
  load_expr_ms?: number | null;
}

export interface LogSession {
  session: string;
  count: number;
  last_ts: string;
}

export interface ActivityLog {
  project: string;
  events: LogEvent[];
  sessions: LogSession[];
}

export interface Projects {
  active: string | null;
  available: string[];
}

export interface SSEEvent {
  kind: string;
  hash?: string;
  alias?: string;
  version?: number;
  error_id?: string;
  name?: string;
  project?: string;
  [key: string]: unknown;
}
