import { useEffect, useRef, useState } from "react";
import { BuckarooServerView } from "buckaroo-js-core";
import "buckaroo-js-core/style.css";
import { api } from "../api";

interface Props {
  wsUrl: string;
  autoHeight?: boolean;
  className?: string;
  style?: React.CSSProperties;
}

export function BuckarooEmbed({ wsUrl, autoHeight, className = "buckaroo-embed", style }: Props) {
  return (
    <div className={className} style={style}>
      <BuckarooServerView
        wsUrl={wsUrl}
        autoHeight={autoHeight}
        style={{ width: "100%", height: autoHeight ? undefined : "100%" }}
      />
    </div>
  );
}

interface LazyProps {
  hash: string;
  project: string;
  className?: string;
  style?: React.CSSProperties;
  autoHeight?: boolean;
}

export function LazyBuckarooEmbed({ hash, project, className = "buckaroo-embed nb-buckaroo", style, autoHeight }: LazyProps) {
  const [wsUrl, setWsUrl] = useState<string | null>(null);
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const el = containerRef.current;
    if (!el || !hash) return;
    const observer = new IntersectionObserver(
      (entries) => {
        if (!entries[0].isIntersecting) return;
        observer.disconnect();
        api
          .session(project, hash)
          .then((data) => { if (data.ws_url) setWsUrl(data.ws_url); })
          .catch(() => {});
      },
      { rootMargin: "300px" },
    );
    observer.observe(el);
    return () => observer.disconnect();
  }, [hash, project]);

  return (
    <div ref={containerRef} className={className} style={style}>
      {wsUrl && (
        <BuckarooServerView
          wsUrl={wsUrl}
          autoHeight={autoHeight}
          style={{ width: "100%", height: autoHeight ? undefined : "100%" }}
        />
      )}
    </div>
  );
}
