function initMermaid() {
    if (typeof mermaid === "undefined") {
        return;
    }

    const isDark = document.body.getAttribute("data-md-color-scheme") === "slate";
    mermaid.initialize({
        startOnLoad: false,
        securityLevel: "loose",
        theme: isDark ? "dark" : "default"
    });

    // Pymdownx outputs <pre class="mermaid"><code>...</code></pre>
    // Unwrap code elements into div.mermaid containers so Mermaid renders clean SVGs
    document.querySelectorAll("pre.mermaid").forEach((pre) => {
        const code = pre.querySelector("code");
        const div = document.createElement("div");
        div.className = "mermaid";
        div.textContent = code ? code.textContent : pre.textContent;
        pre.parentNode.replaceChild(div, pre);
    });

    mermaid.run({
        querySelector: ".mermaid:not([data-processed='true'])"
    }).catch((err) => {
        console.warn("Mermaid render notice:", err);
    });
}

if (typeof document$ !== "undefined") {
    document$.subscribe(function() {
        initMermaid();
    });
} else {
    document.addEventListener("DOMContentLoaded", initMermaid);
}
