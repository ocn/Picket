// Render the bot's real Discord message payloads (content + embeds, as dumped by
// `cargo test --lib screenshot_payloads -- --ignored`) into PNGs that look like
// Discord's dark theme, using headless Chrome. The message chrome (avatar,
// username, bot tag) comes from @skyra/discord-components; the embed itself is a
// hand-written HTML/CSS clone of Discord's embed layout so the field grid and
// multi-line author render exactly as the real client shows them.
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
// Escaped text with hard line breaks preserved (author names carry a `\n`).
const escBr = (s) => esc(s).replace(/\n/g, "<br>");
// Minimal Discord markdown: fenced code blocks, links (incl. nested brackets),
// bold, italics, inline code, mentions, line breaks.
function md(s) {
  // Fenced code blocks first so their contents are left alone; render as a
  // block (<pre>), never inline code. Strip the leading/trailing newline the
  // ```-fence syntax leaves around the body.
  const blocks = [];
  let src = String(s ?? "").replace(/```(?:[a-z0-9]*\n)?([\s\S]*?)```/gi, (_, code) => {
    blocks.push(`<pre class="cb"><code>${esc(code.replace(/^\n/, "").replace(/\n+$/, ""))}</code></pre>`);
    return `\u0000${blocks.length - 1}\u0000`;
  });
  let h = esc(src);
  // Links: label may itself be bracketed, e.g. `[[B0SS]](https://…)`.
  h = h.replace(/\[(\[[^\][]*\]|[^\]]+)\]\((https?:[^)\s]+)\)/g, '<a href="$2">$1</a>');
  h = h.replace(/\*\*([^*]+)\*\*/g, "<strong>$1</strong>");
  h = h.replace(/(^|[^*])\*([^*\n]+)\*/g, "$1<em>$2</em>");
  h = h.replace(/`([^`]+)`/g, "<code>$1</code>");
  h = h.replace(/&lt;(#|@&amp;|@)(\d+)&gt;/g, (_, k) => `<span class="mention">${k === "#" ? "#channel" : k.startsWith("@&") ? "@role" : "@user"}</span>`);
  h = h.replace(/@(here|everyone)/g, '<span class="mention">@$1</span>');
  h = h.replace(/\n/g, "<br>");
  return h.replace(/\u0000(\d+)\u0000/g, (_, i) => blocks[i]);
}
// The client renders the embed timestamp in the viewer's locale; match the
// "11/10/2025 05:12" (MM/DD/YYYY HH:MM, 24h) shape the ticket asks for.
const fmtTs = (t) => { const d = new Date(t); return isNaN(d) ? t : d.toLocaleString("en-GB", { day: "2-digit", month: "2-digit", year: "numeric", hour: "2-digit", minute: "2-digit", hour12: false, timeZone: "UTC" }).replace(",", ""); };
const hex = (c) => (c == null ? "#202225" : `#${(Number(c) & 0xffffff).toString(16).padStart(6, "0")}`);

// Group fields into Discord's 12-column grid: a non-inline field spans all 12
// columns; consecutive inline fields share a row (up to 3), each taking 12/n.
function fieldsHtml(fields) {
  if (!fields || !fields.length) return "";
  const cells = [];
  const cell = (f, span) =>
    `<div class="field" style="grid-column:span ${span}">` +
    `<div class="fname">${esc(f.name)}</div>` +
    `<div class="fval">${md(f.value)}</div></div>`;
  for (let i = 0; i < fields.length; ) {
    if (!fields[i].inline) { cells.push(cell(fields[i], 12)); i++; continue; }
    const run = [];
    while (i < fields.length && fields[i].inline) run.push(fields[i++]);
    for (let j = 0; j < run.length; j += 3) {
      const row = run.slice(j, j + 3);
      const span = 12 / row.length; // 1->12, 2->6, 3->4
      for (const rf of row) cells.push(cell(rf, span));
    }
  }
  return `<div class="efields">${cells.join("")}</div>`;
}

function embedHtml(e) {
  const hasThumb = !!e.thumbnail?.url;
  const author = e.author?.name
    ? `<div class="eauthor">${e.author.icon_url ? `<img class="aicon" src="${esc(e.author.icon_url)}">` : ""}` +
      `<span class="aname">${e.author.url ? `<a href="${esc(e.author.url)}">${escBr(e.author.name)}</a>` : escBr(e.author.name)}</span></div>`
    : "";
  // Title text is the bot's real output and may contain literal backticks; show
  // it verbatim (escaped), not run through markdown.
  const title = e.title
    ? `<div class="etitle">${e.url ? `<a href="${esc(e.url)}">${esc(e.title)}</a>` : esc(e.title)}</div>`
    : "";
  const desc = e.description ? `<div class="edesc">${md(e.description)}</div>` : "";
  const fields = fieldsHtml(e.fields);
  const footerBits = [];
  if (e.footer?.text) footerBits.push(esc(e.footer.text));
  if (e.timestamp) footerBits.push(esc(fmtTs(e.timestamp)));
  const footer = footerBits.length
    ? `<div class="efooter">${e.footer?.icon_url ? `<img class="ficon" src="${esc(e.footer.icon_url)}">` : ""}` +
      `<span>${footerBits.join(" • ")}</span></div>`
    : "";
  const thumb = hasThumb ? `<div class="ethumb"><img src="${esc(e.thumbnail.url)}"></div>` : "";
  return `<div class="embed" style="border-left-color:${hex(e.color)}">` +
    `<div class="egrid${hasThumb ? " thumb" : ""}">` +
    `<div class="emain">${author}${title}${desc}${fields}${footer}</div>${thumb}` +
    `</div></div>`;
}

function page(p) {
  const embeds = (p.embeds ?? (p.embed ? [p.embed] : [])).map(embedHtml).join("");
  return `<!doctype html><html><head><meta charset="utf-8">
<script type="module" src="https://esm.sh/@skyra/discord-components-core@4"></script>
<style>
:root{font-family:"gg sans","Noto Sans","Helvetica Neue",Helvetica,Arial,sans-serif}
body{margin:0;background:#313338;padding:12px 0;font-family:"gg sans","Noto Sans","Helvetica Neue",Helvetica,Arial,sans-serif}
discord-messages{border:0;width:640px}
a{color:#00a8fc;text-decoration:none}
/* Embed clone */
.embed{max-width:520px;background:#2b2d31;border-radius:4px;border-left:4px solid #202225;margin-top:8px;box-sizing:border-box}
.egrid{display:grid;grid-template-columns:516px;padding:8px 16px 16px 12px}
.egrid.thumb{grid-template-columns:420px 80px;column-gap:16px}
.emain{grid-column:1;min-width:0}
.ethumb{grid-column:2;grid-row:1;justify-self:end}
.ethumb img{width:80px;height:80px;object-fit:cover;border-radius:4px;display:block}
.eauthor{display:flex;align-items:flex-start;gap:8px;margin-top:8px}
.eauthor .aicon{width:24px;height:24px;border-radius:50%;object-fit:cover;flex:0 0 auto}
.eauthor .aname{font-size:14px;font-weight:500;line-height:22px;color:#fff}
.eauthor .aname a{color:#fff}
.etitle{margin-top:8px;font-size:16px;font-weight:600;line-height:22px;color:#fff}
.etitle a{color:#00a8fc}
.edesc{margin-top:8px;font-size:14px;line-height:1.375;color:#dbdee1;white-space:normal;word-wrap:break-word}
.efields{display:grid;grid-template-columns:repeat(12,1fr);column-gap:8px;row-gap:8px;margin-top:8px}
.field{min-width:0}
.field .fname{font-size:14px;font-weight:600;color:#fff;line-height:18px;margin-bottom:8px}
.field .fval{font-size:14px;line-height:1.25;color:#dbdee1;word-wrap:break-word}
.efooter{display:flex;align-items:center;gap:8px;margin-top:8px;font-size:12px;line-height:16px;color:#949ba4}
.efooter .ficon{width:20px;height:20px;border-radius:50%;object-fit:cover;flex:0 0 auto}
code{background:#1e1f22;padding:1px 4px;border-radius:3px;font-family:"gg mono",ui-monospace,Menlo,Consolas,monospace;font-size:.85em}
pre.cb{background:#1e1f22;border:1px solid #1e1f22;border-radius:4px;padding:16px;margin:6px 0;white-space:pre;overflow:auto;font-family:"gg mono",ui-monospace,Menlo,Consolas,monospace;font-size:12px;line-height:1.35}
pre.cb code{background:none;padding:0;border-radius:0;font-size:inherit}
.mention{background:#3c4270;color:#c9cdfb;border-radius:3px;padding:0 2px;font-weight:500}
</style></head>
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
