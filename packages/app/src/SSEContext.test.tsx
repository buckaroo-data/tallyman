// @vitest-environment happy-dom
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { SSEProvider, useSSE } from "./SSEContext";
import { api } from "./api";
import { CachePage } from "./pages/CachePage";
import { CatalogPage } from "./pages/CatalogPage";
import { NotebookPage } from "./pages/NotebookPage";

// The companion sends each event with its `kind` as the SSE event name, and an
// EventSource hands a named event only to the listeners registered for that
// name: one nobody listens for is dropped. The pages refetch when the provider's
// `version` advances, so a kind without a listener leaves an open page stale
// (#235). These tests drive the provider through a fake EventSource that drops
// unlistened names the same way.

vi.mock("./api", () => ({
  api: {
    entries: vi.fn(async () => ({ project: "p1", entries: [] })),
    errors: vi.fn(async () => ({ project: "p1", errors: [] })),
    resultCache: vi.fn(async () => ({ project: "p1", entries: [], total_bytes: 0, total_formatted: "0 B" })),
    notebookFull: vi.fn(async () => ({ project: "p1", cells: [], buckaroo_available: false })),
  },
}));
// The notebook page imports the grid and chart embeds; an empty notebook renders neither.
vi.mock("./components/BuckarooEmbed", () => ({ LazyBuckarooEmbed: () => null, BuckarooEmbed: () => null }));
vi.mock("./components/VegaChart", () => ({ VegaChart: () => null }));

class FakeEventSource {
  static last: FakeEventSource | null = null;
  readonly url: string;
  readyState = 0;
  onopen: ((e: Event) => void) | null = null;
  onerror: ((e: Event) => void) | null = null;
  private listeners = new Map<string, Array<(e: MessageEvent) => void>>();

  constructor(url: string) {
    this.url = url;
    FakeEventSource.last = this;
  }

  addEventListener(kind: string, fn: (e: MessageEvent) => void) {
    this.listeners.set(kind, [...(this.listeners.get(kind) ?? []), fn]);
  }

  removeEventListener(kind: string, fn: (e: MessageEvent) => void) {
    this.listeners.set(kind, (this.listeners.get(kind) ?? []).filter((f) => f !== fn));
  }

  close() {
    this.readyState = 2;
  }

  // Deliver a named event the way the browser does: to that name's listeners, or to nobody.
  emit(kind: string, data: Record<string, unknown> = {}) {
    const event = new MessageEvent(kind, { data: JSON.stringify({ kind, ...data }) });
    for (const fn of this.listeners.get(kind) ?? []) fn(event);
  }
}

// Every kind the companion publishes and a page refetches on. The last six are
// the ones #235 found dropped.
const REFETCH_KINDS = [
  "new_entry",
  "build_failed",
  "notebook_changed",
  "chart_attached",
  "post_processing_changed",
  "summary_stat_changed",
  "recalc",
  "project_reset",
  "unfaithful_heal",
  "entry_added",
  "alias_changed",
  "alias_renamed",
  "display_changed",
];

let root: Root | null = null;
let seen: ReturnType<typeof useSSE> | null = null;

function Probe() {
  seen = useSSE();
  return null;
}

async function mount(path: string) {
  const container = document.createElement("div");
  root = createRoot(container);
  await act(async () => {
    root!.render(
      <MemoryRouter initialEntries={[path]}>
        <SSEProvider project="p1">
          <Probe />
          <Routes>
            <Route path="/:project/catalog" element={<CatalogPage />} />
            <Route path="/:project/notebook" element={<NotebookPage />} />
            <Route path="/:project/cache" element={<CachePage />} />
            <Route path="*" element={null} />
          </Routes>
        </SSEProvider>
      </MemoryRouter>,
    );
  });
  const es = FakeEventSource.last!;
  expect(es.url).toBe("/p1/api/sse");
  await act(async () => es.emit("hello", { project: "p1" }));
  return es;
}

beforeEach(() => {
  (globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
  vi.stubGlobal("EventSource", FakeEventSource);
  vi.spyOn(console, "log").mockImplementation(() => {});
  vi.clearAllMocks();
  seen = null;
});

afterEach(async () => {
  await act(async () => root?.unmount());
  root = null;
  FakeEventSource.last = null;
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("SSEProvider", () => {
  it.each(REFETCH_KINDS)("advances version on %s", async (kind) => {
    const es = await mount("/p1/none");
    expect(seen!.version).toBe(0);

    await act(async () => es.emit(kind, { hash: "abc" }));

    expect(seen!.version).toBe(1);
    expect(seen!.lastEvent?.kind).toBe(kind);
  });

  // The fake must drop an unlistened name, or the cases above would pass for any kind.
  it("leaves version alone for a kind it has no listener for", async () => {
    const es = await mount("/p1/none");

    await act(async () => es.emit("no_such_kind"));

    expect(seen!.version).toBe(0);
  });
});

// The pages that must refetch for each event, per #235: a reset rewinds the
// catalog, the notebook and the snapshot list; an unfaithful heal records a
// build error (the catalog's error banner) and pins the snapshot (the Cache
// page's pin reason).
describe("pages refetch", () => {
  const cases: Array<[string, string, keyof typeof api]> = [
    ["project_reset", "/p1/catalog", "entries"],
    ["project_reset", "/p1/notebook", "notebookFull"],
    ["project_reset", "/p1/cache", "resultCache"],
    ["unfaithful_heal", "/p1/catalog", "errors"],
    ["unfaithful_heal", "/p1/cache", "resultCache"],
  ];

  it.each(cases)("%s refetches %s (api.%s)", async (kind, path, fn) => {
    const es = await mount(path);
    const fetcher = vi.mocked(api[fn]);
    const before = fetcher.mock.calls.length;
    expect(before).toBeGreaterThan(0);

    await act(async () => es.emit(kind, { hash: "abc", step: 3 }));

    expect(fetcher.mock.calls.length).toBe(before + 1);
  });
});
