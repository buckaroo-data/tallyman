import { useEffect, useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import { api } from "../api";
import { useSSE } from "../SSEContext";

export function Header() {
  const { project } = useParams<{ project?: string }>();
  const navigate = useNavigate();
  const { status, version } = useSSE();
  const [projects, setProjects] = useState<string[]>([]);
  const [diskFormatted, setDiskFormatted] = useState<string | null>(null);

  useEffect(() => {
    api.projects().then((d) => setProjects(d.available)).catch(() => {});
  }, [project]);

  // version bumps on every SSE event (new entry, notebook change, …); the
  // backend coalesces the resulting burst of disk-usage walks behind a short
  // TTL cache, so refetching here keeps the pill fresh without re-walking.
  useEffect(() => {
    if (!project) return;
    api.diskUsage(project)
      .then((d) => setDiskFormatted(d.formatted.total))
      .catch(() => {});
  }, [project, version]);

  const handleSwitch = async (name: string) => {
    if (name === project) return;
    try {
      await api.switchProject(name);
      navigate(`/${name}/catalog`);
    } catch (e) {
      alert(`switch failed: ${e instanceof Error ? e.message : e}`);
    }
  };

  const handleNewProject = async () => {
    const name = prompt(
      "New project name (lowercase letters, digits, hyphens, underscores; up to 32):",
    );
    if (!name) return;
    try {
      await api.createProject(name, false);
      navigate(`/${name}/catalog`);
    } catch (e) {
      alert(`create failed: ${e instanceof Error ? e.message : e}`);
    }
  };

  return (
    <header>
      <span className="brand">pydata</span>

      {project ? (
        <span className="project-switcher">
          <label className="meta" htmlFor="project-select" style={{ marginRight: 4 }}>
            project:
          </label>
          <select
            id="project-select"
            value={project}
            onChange={(e) => handleSwitch(e.target.value)}
            aria-label="Active project"
          >
            {projects.length === 0 && <option value={project}>{project}</option>}
            {projects.map((p) => (
              <option key={p} value={p}>
                {p}
              </option>
            ))}
          </select>
          <button type="button" id="new-project-btn" onClick={handleNewProject} title="Create a new project">
            + new
          </button>
        </span>
      ) : null}

      {project && (
        <nav>
          <Link to={`/${project}/catalog`}>catalog</Link>
          <Link to={`/${project}/notebook`}>notebook</Link>
          <Link to={`/${project}/lineage`}>lineage</Link>
          <Link to={`/${project}/cache`}>cache</Link>
        </nav>
      )}

      {diskFormatted && (
        <span className="pill" style={{ cursor: "default" }}>
          disk: {diskFormatted}
        </span>
      )}

      <span className={`pill${status === "live" ? " live" : ""}`}>
        {status === "live" ? "live" : status === "offline" ? "offline" : "connecting…"}
      </span>
    </header>
  );
}
