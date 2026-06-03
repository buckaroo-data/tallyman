import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { api } from "../api";

export function EmptyStatePage() {
  const navigate = useNavigate();
  const [name, setName] = useState("");
  const [withFixture, setWithFixture] = useState(false);
  const [error, setError] = useState("");

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError("");
    try {
      await api.createProject(name, withFixture);
      navigate(`/${name}/catalog`);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  };

  return (
    <main>
      <section className="empty-state">
        <h2>create a project</h2>
        <p className="meta">
          No projects on disk yet. Name one to get started — lowercase letters, digits, hyphens,
          underscores; up to 32 characters.
        </p>
        <form onSubmit={handleSubmit}>
          <input
            type="text"
            value={name}
            onChange={(e) => setName(e.target.value)}
            pattern="[a-z0-9][a-z0-9_-]{0,31}"
            placeholder="project_name"
            required
            autoFocus
            autoComplete="off"
          />
          <label className="meta">
            <input
              type="checkbox"
              checked={withFixture}
              onChange={(e) => setWithFixture(e.target.checked)}
            />{" "}
            seed with the shoe-orders demo fixture
          </label>
          <button type="submit">create</button>
        </form>
        {error && (
          <div role="alert" className="meta" style={{ color: "#ffb4b4", marginTop: 8 }}>
            {error}
          </div>
        )}
      </section>
    </main>
  );
}
