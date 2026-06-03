import { useEffect, useState, useRef } from "react";
import { Link, useParams } from "react-router-dom";
import {
  DndContext,
  closestCenter,
  KeyboardSensor,
  PointerSensor,
  useSensor,
  useSensors,
  type DragEndEvent,
} from "@dnd-kit/core";
import {
  SortableContext,
  sortableKeyboardCoordinates,
  verticalListSortingStrategy,
  useSortable,
  arrayMove,
} from "@dnd-kit/sortable";
import { CSS } from "@dnd-kit/utilities";
import { marked } from "marked";
import { api } from "../api";
import { useSSE } from "../SSEContext";
import { LazyBuckarooEmbed } from "../components/BuckarooEmbed";
import { VegaChart } from "../components/VegaChart";
import type { NotebookCell } from "../types";

// ── Markdown editor ───────────────────────────────────────────────────────────

function MarkdownCell({
  cell,
  project,
  onSaved,
}: {
  cell: NotebookCell;
  project: string;
  onSaved: (markdown: string) => void;
}) {
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState(cell.markdown);
  const taRef = useRef<HTMLTextAreaElement>(null);

  const save = async () => {
    try {
      const res = await api.putMarkdown(project, cell.cell_id, draft);
      onSaved(res.markdown);
    } catch {}
    setEditing(false);
  };

  const cancel = () => {
    setDraft(cell.markdown);
    setEditing(false);
  };

  // marked v12 returns string synchronously; markdown is user-authored local content only.
  const rendered = cell.markdown ? marked.parse(cell.markdown) as string : "";

  if (editing) {
    return (
      <>
        <textarea
          ref={taRef}
          className="nb-markdown-edit"
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
          onBlur={save}
          onKeyDown={(e) => {
            if (e.key === "Enter" && (e.metaKey || e.ctrlKey)) save();
            if (e.key === "Escape") cancel();
          }}
          autoFocus
        />
      </>
    );
  }

  if (rendered) {
    return (
      <div
        className="nb-markdown"
        title="click to edit"
        style={{ cursor: "text" }}
        onClick={() => setEditing(true)}
        dangerouslySetInnerHTML={{ __html: rendered }}
      />
    );
  }
  return (
    <div className="nb-markdown" style={{ cursor: "text" }} onClick={() => setEditing(true)}>
      <em className="meta">(click to add a note)</em>
    </div>
  );
}

// ── Sortable cell ─────────────────────────────────────────────────────────────

function SortableCell({
  cell,
  project,
  buckarooAvailable,
  onRemove,
  onMarkdownSaved,
}: {
  cell: NotebookCell;
  project: string;
  buckarooAvailable: boolean;
  onRemove: (cellId: string) => void;
  onMarkdownSaved: (cellId: string, markdown: string) => void;
}) {
  const { attributes, listeners, setNodeRef, transform, transition, isDragging } = useSortable({
    id: cell.cell_id,
  });
  const [showMeta, setShowMeta] = useState(false);

  const style: React.CSSProperties = {
    transform: CSS.Transform.toString(transform),
    transition,
    opacity: isDragging ? 0.4 : 1,
  };

  const rowCount = cell.entry_meta?.row_count ?? 10;
  const embedPx = Math.min(rowCount * 36 + 130, 420);

  return (
    <article
      ref={setNodeRef}
      style={style}
      className="nb-cell"
      id={`cell-${cell.cell_id}`}
    >
      <div className="nb-head">
        <span className="nb-drag" title="drag to reorder" {...attributes} {...listeners}>
          ⋮⋮
        </span>
        <span className="alias">{cell.alias}</span>
        {cell.version != null && <span className="vchip">V{cell.version}</span>}
        {cell.latest_hash && (
          <Link className="pill" to={`/${project}/catalog/${cell.latest_hash}`}>
            {cell.latest_hash}
          </Link>
        )}
        <span className="nb-actions">
          {cell.entry_meta && (
            <button type="button" onClick={() => setShowMeta((v) => !v)}>
              {showMeta ? "hide metadata" : "expand metadata"}
            </button>
          )}
          <button
            type="button"
            onClick={() => {
              if (confirm("remove this cell from the notebook? (the catalog entry stays.)")) {
                onRemove(cell.cell_id);
              }
            }}
          >
            ×
          </button>
        </span>
      </div>

      <MarkdownCell
        cell={cell}
        project={project}
        onSaved={(md) => onMarkdownSaved(cell.cell_id, md)}
      />

      {cell.chart_spec && cell.latest_hash && (
        <VegaChart
          spec={cell.chart_spec}
          dataHash={cell.latest_hash}
          project={project}
          className="chart-panel nb-chart"
        />
      )}

      {buckarooAvailable && cell.latest_hash ? (
        <LazyBuckarooEmbed
          hash={cell.latest_hash}
          project={project}
          style={{ height: embedPx }}
        />
      ) : cell.latest_hash ? (
        <div className="nb-empty">
          no result.parquet for {cell.latest_hash}
        </div>
      ) : (
        <div className="nb-empty">
          alias <code>{cell.alias}</code> has no current entry (was it pruned?)
        </div>
      )}

      {cell.entry_meta && showMeta && (
        <div className="nb-meta">
          {cell.entry_meta.row_count} rows · {cell.entry_meta.execute_seconds}s · first prompt:{" "}
          <em>{cell.entry_meta.prompt ?? "(none)"}</em>
        </div>
      )}
    </article>
  );
}

// ── Notebook page ─────────────────────────────────────────────────────────────

export function NotebookPage() {
  const { project } = useParams<{ project: string }>();
  const { version } = useSSE();
  const [cells, setCells] = useState<NotebookCell[]>([]);
  const [buckarooAvailable, setBuckarooAvailable] = useState(false);

  useEffect(() => {
    if (!project) return;
    api.notebookFull(project).then((d) => {
      setCells(d.cells);
      setBuckarooAvailable(d.buckaroo_available);
    }).catch(() => {});
  }, [project, version]);

  const sensors = useSensors(
    useSensor(PointerSensor),
    useSensor(KeyboardSensor, { coordinateGetter: sortableKeyboardCoordinates }),
  );

  const handleDragEnd = async (event: DragEndEvent) => {
    const { active, over } = event;
    if (!over || active.id === over.id || !project) return;
    const oldIndex = cells.findIndex((c) => c.cell_id === active.id);
    const newIndex = cells.findIndex((c) => c.cell_id === over.id);
    if (oldIndex === -1 || newIndex === -1) return;
    const reordered = arrayMove(cells, oldIndex, newIndex);
    setCells(reordered);
    await api.patchNotebook(project, "reorder", String(active.id), newIndex);
  };

  const handleRemove = async (cellId: string) => {
    if (!project) return;
    await api.patchNotebook(project, "remove", cellId);
    setCells((prev) => prev.filter((c) => c.cell_id !== cellId));
  };

  const handleMarkdownSaved = (cellId: string, markdown: string) => {
    setCells((prev) =>
      prev.map((c) => (c.cell_id === cellId ? { ...c, markdown } : c))
    );
  };

  if (!project) return null;

  return (
    <main style={{ overflow: "auto" }}>
      <section style={{ padding: "4px 0" }}>
        <h2 style={{ margin: "0 16px 2px" }}>notebook · {project}</h2>
        <p className="meta" style={{ margin: "0 16px 4px" }}>
          {cells.length} cell{cells.length === 1 ? "" : "s"}. Cells are anchored on aliases;
          revising an alias updates the cell in place. Drag a cell by its handle to reorder.{" "}
          <a href={`/${project}/export/marimo`} style={{ marginLeft: 12 }}>
            ↓ export to marimo
          </a>
        </p>

        {cells.length === 0 && (
          <div className="nb-empty">
            no named entries yet. Use <code>catalog_create</code> to add a cell.
          </div>
        )}

        <DndContext
          sensors={sensors}
          collisionDetection={closestCenter}
          onDragEnd={handleDragEnd}
        >
          <SortableContext
            items={cells.map((c) => c.cell_id)}
            strategy={verticalListSortingStrategy}
          >
            {cells.map((cell) => (
              <SortableCell
                key={cell.cell_id}
                cell={cell}
                project={project}
                buckarooAvailable={buckarooAvailable}
                onRemove={handleRemove}
                onMarkdownSaved={handleMarkdownSaved}
              />
            ))}
          </SortableContext>
        </DndContext>
      </section>
    </main>
  );
}
