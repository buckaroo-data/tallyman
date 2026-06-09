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

export interface DagNode {
  hash: string;
  row_count: number;
  execute_seconds: number;
  created_at: string;
}

export interface CatalogDagLayout {
  project: string;
  nodes: DagNode[];
  edges: Array<{ from: string; to: string }>;
  positions: Record<string, [number, number]>;
  width: number;
  height: number;
  annotated: Record<string, { alias: string | null; version: number | null }>;
}

export interface LineageLayout {
  project: string;
  content_hash: string;
  lineage: {
    nodes: Array<{ id: string; type: string; label: string | null }>;
    edges: Array<[string, string]>;
    root: string;
  };
  positions: Record<string, [number, number]>;
  width: number;
  height: number;
  column_trees: Record<string, string>;
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

export interface CustomizationItem {
  name: string;
  path: string;
  source: string;
  disabled: boolean;
}

export interface Customizations {
  project: string;
  summary_stats: CustomizationItem[];
  display_klasses: CustomizationItem[];
  post_processings: CustomizationItem[];
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
