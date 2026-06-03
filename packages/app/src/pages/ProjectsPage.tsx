import { useEffect, useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import { api } from "../api";

export function ProjectsPage() {
  const navigate = useNavigate();
  const [available, setAvailable] = useState<string[]>([]);
  const [active, setActive] = useState<string | null>(null);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [msg, setMsg] = useState("");
  const [err, setErr] = useState(false);
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    api.projects()
      .then((d) => {
        setAvailable(d.available);
        setActive(d.active);
      })
      .catch(() => {});
  }, []);

  // The active project can't be deleted, so only non-active projects count.
  const deletable = available.filter((p) => p !== active);

  const toggle = (name: string) => {
    setSelected((s) => {
      const n = new Set(s);
      if (n.has(name)) n.delete(name);
      else n.add(name);
      return n;
    });
  };

  const toggleAll = () => {
    setSelected((s) => (s.size === deletable.length ? new Set() : new Set(deletable)));
  };

  const handleDelete = async () => {
    const names = [...selected];
    if (!names.length) return;
    const noun = names.length === 1 ? `project "${names[0]}"` : `${names.length} projects`;
    if (!confirm(`Permanently delete ${noun}? This cannot be undone.`)) return;

    setBusy(true);
    setErr(false);
    setMsg("deleting…");
    try {
      const data = await api.deleteProjects(names);
      if (data.errors.length) {
        setErr(true);
        setMsg(data.errors.map((e) => `${e.name}: ${e.error}`).join("; "));
      }
      // If the active project was deleted, the server promoted a survivor (or
      // none). Land where it now points: the new project, or the empty state.
      if (active && !data.remaining.includes(active)) {
        navigate(data.active ? `/${data.active}/catalog` : "/");
        return;
      }
      setAvailable(data.remaining);
      setActive(data.active);
      setSelected(new Set());
      if (!data.errors.length) setMsg("");
    } catch (e) {
      setErr(true);
      setMsg(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const count = selected.size;

  return (
    <main>
      <section className="projects-page">
        <h2>projects</h2>

        {available.length === 0 ? (
          <p className="meta">
            no projects yet — <Link to="/">create one</Link>.
          </p>
        ) : (
          <>
            <ul className="project-list">
              {available.map((name) => {
                const isActive = name === active;
                return (
                  <li key={name} className={isActive ? "active-project" : ""}>
                    <input
                      type="checkbox"
                      checked={selected.has(name)}
                      disabled={isActive}
                      title={isActive ? "cannot delete the active project" : undefined}
                      onChange={() => toggle(name)}
                    />
                    <span className="proj-name">
                      <Link to={`/${name}/catalog`}>{name}</Link>
                    </span>
                    {isActive && <span className="active-badge">active</span>}
                  </li>
                );
              })}
            </ul>

            <div className="projects-actions">
              <button
                type="button"
                className="ghost-btn"
                onClick={toggleAll}
                disabled={deletable.length === 0}
              >
                {count === deletable.length && count > 0 ? "deselect all" : "select all"}
              </button>
              <button
                type="button"
                className="danger-btn"
                onClick={handleDelete}
                disabled={count === 0 || busy}
              >
                {count > 0 ? `delete ${count} project${count > 1 ? "s" : ""}` : "delete selected"}
              </button>
              {msg && <span className={`meta${err ? " err" : ""}`}>{msg}</span>}
            </div>
          </>
        )}
      </section>
    </main>
  );
}
