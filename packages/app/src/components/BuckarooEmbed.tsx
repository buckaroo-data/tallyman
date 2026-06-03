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

    // A Buckaroo session can take a moment to spin up after an entry is first
    // opened, so a single fetch often races ahead of it and the viewer never
    // appears. Poll the session endpoint until ws_url is ready (~40 × 400ms ≈
    // 16s, with a slightly longer backoff on transient fetch errors).
    let cancelled = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const MAX_ATTEMPTS = 40;

    const poll = (attempt: number) => {
      api
        .session(project, hash)
        .then((data) => {
          if (cancelled) return;
          if (data.ws_url) setWsUrl(data.ws_url);
          else if (attempt < MAX_ATTEMPTS) timer = setTimeout(() => poll(attempt + 1), 400);
        })
        .catch(() => {
          if (!cancelled && attempt < MAX_ATTEMPTS) timer = setTimeout(() => poll(attempt + 1), 600);
        });
    };

    const observer = new IntersectionObserver(
      (entries) => {
        if (!entries[0].isIntersecting) return;
        observer.disconnect();
        poll(0);
      },
      { rootMargin: "300px" },
    );
    observer.observe(el);
    return () => {
      cancelled = true;
      if (timer) clearTimeout(timer);
      observer.disconnect();
    };
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
