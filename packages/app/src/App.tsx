import { useEffect } from "react";
import { Routes, Route, Navigate, useNavigate, useParams } from "react-router-dom";
import { Header } from "./components/Header";
import { SSEProvider } from "./SSEContext";
import { CatalogPage } from "./pages/CatalogPage";
import { NotebookPage } from "./pages/NotebookPage";
import { LineagePage } from "./pages/LineagePage";
import { LineageEntryPage } from "./pages/LineageEntryPage";
import { DiffPage } from "./pages/DiffPage";
import { CachePage } from "./pages/CachePage";
import { EmptyStatePage } from "./pages/EmptyStatePage";
import { api } from "./api";

function RootRedirect() {
  const navigate = useNavigate();

  useEffect(() => {
    api
      .projects()
      .then(({ active }) => {
        if (active) navigate(`/${active}/catalog`, { replace: true });
      })
      .catch(() => {});
  }, [navigate]);

  return <EmptyStatePage />;
}

function ProjectLayout({ children }: { children: React.ReactNode }) {
  const { project } = useParams<{ project: string }>();
  return <SSEProvider project={project ?? null}>{children}</SSEProvider>;
}

export default function App() {
  return (
    <>
      <Routes>
        <Route
          path="/:project/*"
          element={
            <ProjectLayout>
              <Header />
              <Routes>
                <Route path="catalog" element={<CatalogPage />} />
                <Route path="catalog/:hash" element={<CatalogPage />} />
                <Route path="errors/:errorId" element={<CatalogPage />} />
                <Route path="notebook" element={<NotebookPage />} />
                <Route path="lineage" element={<LineagePage />} />
                <Route path="lineage/:hash" element={<LineageEntryPage />} />
                <Route path="cache" element={<CachePage />} />
                <Route path="diff/:alias" element={<DiffPage />} />
                <Route path="diff/:alias/:va/:vb" element={<DiffPage />} />
                <Route path="*" element={<Navigate to="catalog" replace />} />
              </Routes>
            </ProjectLayout>
          }
        />
        <Route path="/" element={<><Header /><RootRedirect /></>} />
        <Route path="*" element={<><Header /><EmptyStatePage /></>} />
      </Routes>
    </>
  );
}
