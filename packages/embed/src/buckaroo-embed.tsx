// Mount `BuckarooServerView` into every [data-ws-url] div on the page.
//
// The companion's templates render placeholder divs with data-hash (lazy) or
// data-ws-url (immediate, e.g. entry-detail). This script mounts React embeds
// for data-ws-url divs on load, and re-mounts when data-ws-url is set
// dynamically (e.g. by the notebook's IntersectionObserver lazy-load).
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
    if (!wsUrl) return;
    const autoHeight = el.hasAttribute("data-auto-height");
    const root = createRoot(el);
    el.__buckarooRoot = root;
    root.render(
        React.createElement(BuckarooServerView, {
            wsUrl,
            autoHeight: autoHeight || undefined,
            style: { width: "100%", height: autoHeight ? undefined : "100%" },
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

// Watch for:
//   childList — SPA-lite nav swaps add/remove whole subtrees
//   attributes — lazy-load sets data-ws-url on an existing placeholder div
const observer = new MutationObserver((records) => {
    for (const r of records) {
        if (r.type === "attributes") {
            const el = r.target;
            if (el instanceof HTMLElement && el.dataset.wsUrl) mount(el);
            continue;
        }
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
observer.observe(document.body, {
    childList: true,
    subtree: true,
    attributes: true,
    attributeFilter: ["data-ws-url"],
});
