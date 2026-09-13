// Render the bot's real Discord message payloads (content + embeds, as dumped by
// `cargo test --lib screenshot_payloads -- --ignored`) into PNGs that look like
// Discord's dark theme, using @skyra/discord-components and headless Chrome.
//
//   SCREENSHOT_PAYLOAD_DIR=/tmp/picket-render/payloads \
//     cargo test --lib screenshot_payloads -- --ignored --nocapture
//   node scripts/render-screenshots.mjs        # writes docs/screenshots/*.png
//
// These are renders of real payloads, not captures of the Discord client.
// Env: RENDER_DIR (scratch, default /tmp/picket-render), OUT_DIR (default
// docs/screenshots), W (viewport width, default 680), AVATAR_URL.
import { readdirSync, readFileSync, writeFileSync, mkdirSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { join, basename } from "node:path";

const D = process.env.RENDER_DIR ?? "/tmp/picket-render";
const OUT = process.env.OUT_DIR ?? "docs/screenshots";
const CHROME = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome";
const AVATAR = process.env.AVATAR_URL ?? "https://cdn.discordapp.com/embed/avatars/0.png";

const esc = (s) => String(s ?? "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
// Minimal Discord markdown: links, bold, italics, inline code, line breaks.
function md(s) {
  // Fenced code blocks first so their contents are left alone.
  const blocks = [];
  let src = String(s ?? "").replace(/```(?:[a-z]*\n)?([\s\S]*?)```/g, (_, code) => {
    blocks.push(`<pre class="cb"><code>${esc(code.replace(/\n$/, ""))}</code></pre>`);
    return `\u0000${blocks.length - 1}\u0000`;
  });
  let h = esc(src);
  h = h.replace(/\[(\[?[^\]]+\]?)\]\((https?:[^)\s]+)\)/g, '<a href="$2">$1</a>');
  h = h.replace(/\*\*([^*]+)\*\*/g, "<strong>$1</strong>");
  h = h.replace(/(^|[^*])\*([^*\n]+)\*/g, "$1<em>$2</em>");
  h = h.replace(/`([^`]+)`/g, "<code>$1</code>");
  h = h.replace(/&lt;(#|@&amp;|@)(\d+)&gt;/g, (_, k) => `<span class="mention">${k === "#" ? "#channel" : k.startsWith("@&") ? "@role" : "@user"}</span>`);
  h = h.replace(/@(here|everyone)/g, '<span class="mention">@$1</span>');
  h = h.replace(/\n/g, "<br>");
  return h.replace(/\u0000(\d+)\u0000/g, (_, i) => blocks[i]);
}
const fmtTs = (t) => { const d = new Date(t); return isNaN(d) ? t : d.toLocaleString("en-GB", { day: "2-digit", month: "2-digit", year: "numeric", hour: "2-digit", minute: "2-digit", timeZone: "UTC" }).replace(",", ""); };
const hex = (c) => (c == null ? "" : `#${Number(c).toString(16).padStart(6, "0")}`);

function embedHtml(e) {
  const fields = (e.fields ?? []).map((f, i) =>
    `<discord-embed-field field-title="${esc(f.name)}" ${f.inline ? `inline inline-index="${(i % 3) + 1}"` : ""}>${md(f.value)}</discord-embed-field>`).join("");
  const footer = e.footer || e.timestamp ? `<discord-embed-footer slot="footer" ${e.timestamp ? `timestamp="${esc(fmtTs(e.timestamp))}"` : ""} ${e.footer?.icon_url ? `footer-image="${esc(e.footer.icon_url)}"` : ""}>${esc(e.footer?.text ?? "")}</discord-embed-footer>` : "";
  const attrs = [
    e.color != null ? `color="${hex(e.color)}"` : "",
    e.title ? `embed-title="${esc(e.title)}"` : "",
    e.url ? `url="${esc(e.url)}"` : "",
    e.thumbnail?.url ? `thumbnail="${esc(e.thumbnail.url)}"` : "",
    e.image?.url ? `image="${esc(e.image.url)}"` : "",
    e.author?.name ? `author-name="${esc(e.author.name)}"` : "",
    e.author?.icon_url ? `author-image="${esc(e.author.icon_url)}"` : "",
    e.author?.url ? `author-url="${esc(e.author.url)}"` : "",
  ].join(" ");
  return `<discord-embed slot="embeds" ${attrs}>
    ${e.description ? `<discord-embed-description slot="description">${md(e.description)}</discord-embed-description>` : ""}
    ${fields ? `<discord-embed-fields slot="fields">${fields}</discord-embed-fields>` : ""}
    ${footer}
  </discord-embed>`;
}

function page(p) {
  const embeds = (p.embeds ?? (p.embed ? [p.embed] : [])).map(embedHtml).join("");
  return `<!doctype html><html><head><meta charset="utf-8">
<script type="module" src="https://esm.sh/@skyra/discord-components-core@4"></script>
<style>body{margin:0;background:#313338;padding:12px 0} discord-messages{border:0;width:640px}
a{color:#00a8fc;text-decoration:none} code{background:#1e1f22;padding:1px 4px;border-radius:3px;font-size:.9em} pre.cb{background:#1e1f22;border:1px solid #1e1f22;border-radius:4px;padding:8px;margin:4px 0;white-space:pre;font-size:.85em;line-height:1.25} .mention{background:#3c4270;color:#c9cdfb;border-radius:3px;padding:0 2px;font-weight:500}</style></head>
<body><discord-messages><discord-message author="Picket" bot avatar="${AVATAR}" timestamp="${new Date().toISOString().slice(0,10)}">
${p.content ? md(p.content) : ""}${embeds}</discord-message></discord-messages><script>setTimeout(()=>{document.title='H='+Math.ceil(document.body.scrollHeight)},6000)</script></body></html>`;
}

for (const d of ["pages"]) mkdirSync(join(D, d), { recursive: true });
mkdirSync(OUT, { recursive: true });
for (const f of readdirSync(join(D, "payloads")).filter((f) => f.endsWith(".json"))) {
  const p = JSON.parse(readFileSync(join(D, "payloads", f), "utf8"));
  const name = basename(f, ".json");
  const html = join(D, "pages", `${name}.html`);
  writeFileSync(html, page(p));
  const out = join(OUT, `${name}.png`);
  const W = process.env.W ?? 680;
  const dom = execFileSync(CHROME, ["--headless=new", "--disable-gpu", "--hide-scrollbars", "--virtual-time-budget=9000",
    `--window-size=${W},4000`, "--dump-dom", `file://${html}`], { encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] });
  const H = Number((dom.match(/<title>H=(\d+)<\/title>/) || [])[1] || 900) + 8;
  execFileSync(CHROME, ["--headless=new", "--disable-gpu", "--hide-scrollbars", "--force-device-scale-factor=2",
    "--virtual-time-budget=9000", `--window-size=${W},${H}`, `--screenshot=${out}`, `file://${html}`], { stdio: "ignore" });
  console.log("rendered", out);
}
