import type {
  Entry,
  AppError,
  EntryDetail,
  EntryCache,
  ActivityLog,
  SessionResult,
  NotebookFull,
  DiffData,
  ResultCache,
  Projects,
} from "./types";

async function get<T>(url: string): Promise<T> {
  const r = await fetch(url);
  if (!r.ok) {
    const text = await r.text().catch(() => `HTTP ${r.status}`);
    let msg = text;
    try { msg = JSON.parse(text).detail ?? text; } catch { /* not JSON */ }
    throw new Error(msg);
  }
  return r.json();
}

async function post<T>(url: string, body: unknown): Promise<T> {
  const r = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!r.ok) {
    const d = await r.json().catch(() => ({}));
    throw new Error((d as { detail?: string }).detail ?? `HTTP ${r.status}`);
  }
  return r.json();
}

export const api = {
  projects: (): Promise<Projects> => get("/api/projects"),

  switchProject: (name: string) =>
    post<{ previous: string | null; active: string }>("/api/projects/switch", { name }),

  createProject: (name: string, withFixture: boolean) =>
    post<{ name: string; active: string }>("/api/projects/new", { name, with_fixture: withFixture }),

  deleteProjects: (names: string[]) =>
    post<{
      deleted: string[];
      errors: { name: string; error: string }[];
      active: string | null;
      remaining: string[];
    }>("/api/projects/delete", { names }),

  entries: (project: string): Promise<{ project: string; entries: Entry[] }> =>
    get(`/${project}/api/entries`),

  errors: (project: string, limit = 20): Promise<{ project: string; errors: AppError[] }> =>
    get(`/${project}/api/errors?limit=${limit}`),

  entryDetail: (project: string, hash: string): Promise<EntryDetail> =>
    get(`/${project}/api/entry/${hash}`),

  entryCache: (project: string, hash: string): Promise<EntryCache> =>
    get(`/${project}/api/entry_cache/${hash}`),

  log: (project: string, opts?: { categories?: string[]; sessions?: string[] }): Promise<ActivityLog> => {
    const p = new URLSearchParams();
    if (opts?.categories?.length) p.set("categories", opts.categories.join(","));
    if (opts?.sessions?.length) p.set("sessions", opts.sessions.join(","));
    const qs = p.toString();
    return get(`/${project}/api/log${qs ? `?${qs}` : ""}`);
  },

  diskUsage: (project: string): Promise<{ formatted: Record<string, string> }> =>
    get(`/${project}/api/disk_usage`),

  notebookFull: (project: string): Promise<NotebookFull> =>
    get(`/${project}/api/notebook_full`),

  patchNotebook: (
    project: string,
    action: "reorder" | "remove",
    cellId: string,
    newIndex?: number,
  ) =>
    fetch(`/${project}/api/notebook`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ action, cell_id: cellId, new_index: newIndex }),
    }).then((r) => {
      if (!r.ok) throw new Error("PATCH notebook failed");
      return r.json() as Promise<{ ok: boolean }>;
    }),

  putMarkdown: (project: string, cellId: string, markdown: string) =>
    fetch(`/${project}/api/markdown/${cellId}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ markdown }),
    }).then((r) => {
      if (!r.ok) throw new Error("save failed");
      return r.json() as Promise<{ markdown: string; html: string; cell_id: string }>;
    }),

  putCode: (project: string, alias: string, code: string, prompt?: string) =>
    fetch(`/${project}/api/code/${alias}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ code, prompt: prompt ?? "edited in browser" }),
    }).then((r) => {
      if (!r.ok)
        return r.text().then((t) => Promise.reject(new Error(t)));
      return r.json() as Promise<{ hash: string; alias: string; version: number; row_count: number }>;
    }),

  diffData: (project: string, alias: string, va: number, vb: number): Promise<DiffData> =>
    get(`/${project}/api/diff_data/${alias}/${va}/${vb}`),

  promoteDiff: (
    project: string,
    alias: string,
    va: number,
    vb: number,
  ): Promise<{ alias: string; hash: string; source_alias: string; va: number; vb: number; keys: string[]; row_count: number }> =>
    post(`/${project}/api/promote_diff/${alias}/${va}/${vb}`, {}),

  errorDetail: (project: string, errorId: string): Promise<{ project: string; error: AppError }> =>
    get(`/${project}/api/error/${errorId}`),

  clearErrors: (project: string): Promise<{ project: string; cleared: number }> =>
    fetch(`/${project}/api/errors`, { method: "DELETE" }).then((r) => {
      if (!r.ok) throw new Error(`clear failed: HTTP ${r.status}`);
      return r.json() as Promise<{ project: string; cleared: number }>;
    }),

  session: (project: string, hash: string): Promise<SessionResult> =>
    get(`/${project}/api/session/${hash}`),

  resultCache: (project: string): Promise<ResultCache> =>
    get(`/${project}/api/result_cache`),

  deleteResultCache: (project: string, hash: string): Promise<{ ok: boolean; hash: string }> =>
    fetch(`/${project}/api/result_cache/${hash}`, { method: "DELETE" }).then((r) => {
      if (!r.ok) throw new Error(`delete failed: HTTP ${r.status}`);
      return r.json() as Promise<{ ok: boolean; hash: string }>;
    }),
};
