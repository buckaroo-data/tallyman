// Mount `BuckarooServerView` into every [data-ws-url] div on the page.
//
// The companion's templates render <div class="buckaroo-embed"
// data-ws-url="ws://127.0.0.1:8700/ws/<session>"></div> placeholders where
// it used to render <iframe src=".../s/<session>">. This script scans for
// those divs (on initial load and on SPA-lite nav swaps) and mounts the
// React embed into each. Buckaroo's Tornado server stays running — it's
// the WebSocket data source — but the page chrome is gone.
import * as React from "react";
import { createRoot, Root } from "react-dom/client";
import { BuckarooServerView } from "buckaroo-js-core";
import "buckaroo-js-core/style.css";

interface MountedEl extends HTMLElement {
    __buckarooRoot?: Root;
}

function mount(el: MountedEl): void {
    if (el.__buckarooRoot) return;
    const wsUrl = el.dataset.wsUrl;
    if (!wsUrl) {
        console.warn("[buckaroo-embed] mount target has no data-ws-url", el);
        return;
    }
    const root = createRoot(el);
    el.__buckarooRoot = root;
    root.render(
        React.createElement(BuckarooServerView, {
            wsUrl,
            style: { width: "100%", height: "100%" },
        }),
    );
}

function unmount(el: MountedEl): void {
    if (!el.__buckarooRoot) return;
    el.__buckarooRoot.unmount();
    delete el.__buckarooRoot;
}

function scanAndMount(root: ParentNode = document): void {
    root.querySelectorAll<HTMLElement>("[data-ws-url]").forEach(mount);
}

scanAndMount();

// SPA-lite navigation in base.html swaps the main content in place; pick up
// new mount points and tear down ones that are no longer attached.
const observer = new MutationObserver((records) => {
    for (const r of records) {
        r.addedNodes.forEach((n) => {
            if (!(n instanceof HTMLElement)) return;
            if (n.matches?.("[data-ws-url]")) mount(n);
            scanAndMount(n);
        });
        r.removedNodes.forEach((n) => {
            if (!(n instanceof HTMLElement)) return;
            if (n.matches?.("[data-ws-url]")) unmount(n);
            n.querySelectorAll?.<HTMLElement>("[data-ws-url]").forEach(unmount);
        });
    }
});
observer.observe(document.body, { childList: true, subtree: true });
