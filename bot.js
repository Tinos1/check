// Discord username checker bot (discord.js v14, Node 18+)
// Slash commands: /check /scan /status /stop /resume /hits /reset
// Runs scans in the background and posts available names in the channel.

try { require("dotenv").config(); } catch {}

const fs = require("fs");
const path = require("path");
const http = require("http");
const {
  Client, GatewayIntentBits, Events, REST, Routes, SlashCommandBuilder,
  EmbedBuilder, AttachmentBuilder, MessageFlags,
} = require("discord.js");
const { fetch: ufetch, ProxyAgent } = require("undici");

// ---------- config ----------
const TOKEN = process.env.DISCORD_TOKEN;
const CLIENT_ID = process.env.CLIENT_ID;
const GUILD_ID = process.env.GUILD_ID; // optional: instant command registration in one server
const DELAY = Number(process.env.DELAY_MS || 1500); // min delay between requests PER IP
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, "data");
const ALLOWED = (process.env.ALLOWED_USER_IDS || "").split(",").map((s) => s.trim()).filter(Boolean);
const PROXY_LIST = (process.env.PROXIES || "").split(/[\s,]+/).filter(Boolean);

if (!TOKEN || !CLIENT_ID) {
  console.error("Missing DISCORD_TOKEN or CLIENT_ID env vars.");
  process.exit(1);
}

fs.mkdirSync(DATA_DIR, { recursive: true });
const META_FILE = path.join(DATA_DIR, "job.json");
const NAMES_FILE = path.join(DATA_DIR, "names.txt");
const RESULTS_FILE = path.join(DATA_DIR, "results.csv");
const AVAILABLE_FILE = path.join(DATA_DIR, "available.txt");

// ---------- helpers ----------
const ENDPOINT = "https://discord.com/api/v9/unique-username/username-attempt-unauthed";
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function validate(name) {
  if (name.length < 2 || name.length > 32) return "must be 2-32 chars";
  if (!/^[a-z0-9_.]+$/.test(name)) return "only a-z, 0-9, _ and . allowed";
  if (name.includes("..")) return "no consecutive dots";
  return null;
}

function formatDuration(ms) {
  if (!isFinite(ms) || ms < 0) return "?";
  const m = Math.round(ms / 60000);
  if (m < 60) return `${m}m`;
  const h = Math.floor(m / 60);
  if (h < 48) return `${h}h ${m % 60}m`;
  return `${Math.floor(h / 24)}d ${h % 24}h`;
}

function generateNames(length, digits, special) {
  let chars = "abcdefghijklmnopqrstuvwxyz";
  if (digits) chars += "0123456789";
  if (special) chars += "_.";
  const total = chars.length ** length;
  const idx = new Array(length).fill(0);
  const out = [];
  for (let n = 0; n < total; n++) {
    const name = idx.map((i) => chars[i]).join("");
    if (!name.includes("..")) out.push(name);
    for (let p = length - 1; p >= 0; p--) {
      idx[p]++;
      if (idx[p] < chars.length) break;
      idx[p] = 0;
    }
  }
  // shuffle so partial runs aren't all "aaa, aab, aac..."
  for (let i = out.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [out[i], out[j]] = [out[j], out[i]];
  }
  return out;
}

// ---------- checker core (one "slot" per exit IP, paced independently) ----------
const slots = PROXY_LIST.length
  ? PROXY_LIST.map((url) => ({
      name: url.replace(/\/\/.*@/, "//***@"),
      dispatcher: new ProxyAgent(url),
      next: 0,
    }))
  : [{ name: "direct", dispatcher: undefined, next: 0 }];

async function pickSlot() {
  const slot = slots.reduce((a, b) => (a.next <= b.next ? a : b));
  const wait = slot.next - Date.now();
  if (wait > 0) await sleep(wait);
  return slot;
}

async function check(username, aborted = () => false) {
  let lastErr = "unknown";
  for (let attempt = 0; attempt < 8; attempt++) {
    if (aborted()) throw new Error("stopped");
    const slot = await pickSlot();
    slot.next = Date.now() + DELAY;
    try {
      const res = await ufetch(ENDPOINT, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ username }),
        dispatcher: slot.dispatcher,
      });

      if (res.status === 429) {
        const data = await res.json().catch(() => ({}));
        slot.next = Date.now() + Math.ceil((data.retry_after ?? 5) * 1000) + 500; // park this IP
        lastErr = "rate limited";
        continue;
      }
      if (!res.ok) {
        await res.body?.cancel?.().catch(() => {});
        throw new Error(`HTTP ${res.status}`);
      }
      const data = await res.json();
      return !data.taken; // true = available
    } catch (err) {
      lastErr = err.message;
      slot.next = Date.now() + 10000; // dead/slow proxy: back off
    }
  }
  throw new Error(`gave up (${lastErr})`);
}

// ---------- job state ----------
let job = null; // {status, channelId, total, checked, hits, errors, startedAt, names, cursor, pending}

function saveMeta() {
  if (!job) return;
  const { status, channelId, total, checked, hits, errors, startedAt } = job;
  fs.writeFileSync(META_FILE, JSON.stringify({ status, channelId, total, checked, hits, errors, startedAt }));
}

function loadDone() {
  const done = new Set();
  if (fs.existsSync(RESULTS_FILE)) {
    for (const line of fs.readFileSync(RESULTS_FILE, "utf8").split("\n")) {
      if (line) done.add(line.split(",")[0]);
    }
  }
  return done;
}

function loadRemaining() {
  const done = loadDone();
  return fs.readFileSync(NAMES_FILE, "utf8").split("\n").filter(Boolean).filter((n) => !done.has(n));
}

function loadJobFromDisk() {
  if (!fs.existsSync(META_FILE) || !fs.existsSync(NAMES_FILE)) return null;
  try {
    const meta = JSON.parse(fs.readFileSync(META_FILE, "utf8"));
    const names = loadRemaining();
    return {
      ...meta,
      names,
      cursor: 0,
      pending: [],
      checked: meta.total - names.length,
    };
  } catch {
    return null;
  }
}

function currentRate(j) {
  if (!j.runStart) return null;
  const s = (Date.now() - j.runStart) / 1000;
  return s > 5 ? (j.checked - j.checkedAtStart) / s : null;
}

function statusEmbed(j) {
  const perSec = currentRate(j) ?? (slots.length * 1000) / DELAY;
  const left = j.total - j.checked;
  const pct = j.total ? ((j.checked / j.total) * 100).toFixed(1) : "0";
  const icon = { running: "🟢", stopped: "⏸️", done: "✅" }[j.status] || "";
  return new EmbedBuilder()
    .setTitle(`${icon} Username scan: ${j.status}`)
    .addFields(
      { name: "Progress", value: `${j.checked.toLocaleString()} / ${j.total.toLocaleString()} (${pct}%)`, inline: true },
      { name: "Available", value: String(j.hits), inline: true },
      { name: "Errors", value: String(j.errors), inline: true },
      { name: "Speed", value: `${perSec.toFixed(2)}/s on ${slots.length} IP(s)`, inline: true },
      { name: "ETA", value: j.status === "running" ? formatDuration((left / perSec) * 1000) : "-", inline: true },
    );
}

async function runJob(client) {
  const j = job;
  const channel = await client.channels.fetch(j.channelId).catch(() => null);
  if (!channel) {
    j.status = "stopped";
    saveMeta();
    return;
  }

  j.status = "running";
  j.runStart = Date.now();
  j.checkedAtStart = j.checked;
  saveMeta();

  let lastFlush = Date.now();
  const flush = async (force = false) => {
    if (!j.pending.length) return;
    if (!force && j.pending.length < 15 && Date.now() - lastFlush < 15000) return;
    const batch = j.pending.splice(0, j.pending.length);
    lastFlush = Date.now();
    for (let i = 0; i < batch.length; i += 50) {
      const chunk = batch.slice(i, i + 50).map((n) => `\`${n}\``).join(", ");
      await channel.send(`✅ Available: ${chunk}`).catch(() => {});
    }
  };

  const statusMsg = await channel.send({ embeds: [statusEmbed(j)] }).catch(() => null);
  const timer = setInterval(() => {
    statusMsg?.edit({ embeds: [statusEmbed(j)] }).catch(() => {});
    flush().catch(() => {});
  }, 30000);

  let errorsInRow = 0;
  try {
    while (j.status === "running" && j.cursor < j.names.length) {
      const name = j.names[j.cursor];
      try {
        const free = await check(name, () => j.status !== "running");
        fs.appendFileSync(RESULTS_FILE, `${name},${free ? 1 : 0}\n`);
        j.cursor++;
        j.checked++;
        errorsInRow = 0;
        if (free) {
          j.hits++;
          j.pending.push(name);
          fs.appendFileSync(AVAILABLE_FILE, name + "\n");
        }
      } catch (err) {
        if (err.message === "stopped") break;
        j.cursor++; // skipped; not recorded, so it's retried on /resume
        j.errors++;
        errorsInRow++;
        console.log(`error on ${name}: ${err.message}`);
        if (errorsInRow >= 5) {
          j.status = "stopped";
          await channel.send(`⚠️ 5 errors in a row (${err.message}). Scan paused, check your proxies/IP then /resume.`).catch(() => {});
        }
      }
      if (j.checked % 100 === 0) saveMeta();
      if (j.pending.length >= 15) await flush();
    }
  } finally {
    clearInterval(timer);
  }

  const finished = j.status === "running" && j.cursor >= j.names.length;
  if (finished) j.status = "done";
  saveMeta();
  await flush(true);
  await statusMsg?.edit({ embeds: [statusEmbed(j)] }).catch(() => {});

  if (finished) {
    const payload = { content: `🏁 Scan finished. **${j.hits}** available name(s) found.` };
    if (j.hits > 0 && fs.existsSync(AVAILABLE_FILE)) {
      payload.files = [new AttachmentBuilder(AVAILABLE_FILE, { name: "available.txt" })];
    }
    await channel.send(payload).catch(() => {});
  } else if (j.status === "stopped") {
    await channel.send(`⏸️ Scan paused at ${j.checked.toLocaleString()}/${j.total.toLocaleString()}. Use /resume to continue.`).catch(() => {});
  }
}

// ---------- commands ----------
const commands = [
  new SlashCommandBuilder()
    .setName("check").setDescription("Check if a single username is available")
    .addStringOption((o) => o.setName("username").setDescription("Username to check").setRequired(true)),
  new SlashCommandBuilder()
    .setName("scan").setDescription("Scan many usernames in the background")
    .addIntegerOption((o) => o.setName("length").setDescription("Generate all names of this length").setMinValue(2).setMaxValue(4))
    .addBooleanOption((o) => o.setName("digits").setDescription("Include 0-9"))
    .addBooleanOption((o) => o.setName("special").setDescription("Include _ and ."))
    .addAttachmentOption((o) => o.setName("file").setDescription("Or upload a .txt, one username per line")),
  new SlashCommandBuilder().setName("status").setDescription("Show scan progress"),
  new SlashCommandBuilder().setName("stop").setDescription("Pause the running scan (progress is saved)"),
  new SlashCommandBuilder().setName("resume").setDescription("Resume a paused scan"),
  new SlashCommandBuilder().setName("hits").setDescription("Get all available usernames found so far as a file"),
  new SlashCommandBuilder().setName("reset").setDescription("Delete saved progress and results"),
].map((c) => c.toJSON());

function isAllowed(i) {
  if (ALLOWED.length) return ALLOWED.includes(i.user.id);
  return !!i.memberPermissions?.has("Administrator");
}

const EPH = MessageFlags.Ephemeral;

const handlers = {
  async check(i) {
    await i.deferReply();
    const name = i.options.getString("username").trim().toLowerCase().replace(/^@/, "");
    const problem = validate(name);
    if (problem) return i.editReply(`⚠️ \`${name}\` is invalid: ${problem}`);
    try {
      const free = await check(name);
      await i.editReply(free ? `✅ \`${name}\` is **available**` : `❌ \`${name}\` is **taken**`);
    } catch (err) {
      await i.editReply(`❓ Couldn't check \`${name}\`: ${err.message}`);
    }
  },

  async scan(i, client) {
    if (job && job.status === "running") {
      return i.reply({ content: "A scan is already running. Use /status or /stop.", flags: EPH });
    }
    await i.deferReply();

    let names;
    const file = i.options.getAttachment("file");
    const length = i.options.getInteger("length");
    if (file) {
      if (file.size > 5 * 1024 * 1024) return i.editReply("File too big (max 5 MB).");
      const txt = await (await ufetch(file.url)).text();
      names = txt.split(/\r?\n/).map((s) => s.trim().toLowerCase()).filter(Boolean);
    } else if (length) {
      names = generateNames(length, !!i.options.getBoolean("digits"), !!i.options.getBoolean("special"));
    } else {
      return i.editReply("Give me a `length` (2-4) or upload a `file`.");
    }

    names = [...new Set(names)].filter((n) => !validate(n));
    if (!names.length) return i.editReply("No valid usernames found.");

    const done = loadDone();
    const remaining = names.filter((n) => !done.has(n));
    if (!remaining.length) {
      return i.editReply(`All ${names.length.toLocaleString()} names were already checked. Use /reset to start fresh.`);
    }

    fs.writeFileSync(NAMES_FILE, remaining.join("\n") + "\n");
    job = {
      status: "running", channelId: i.channelId, total: remaining.length, checked: 0,
      hits: 0, errors: 0, startedAt: Date.now(), names: remaining, cursor: 0, pending: [],
    };
    saveMeta();

    const perSec = (slots.length * 1000) / DELAY;
    await i.editReply(
      `🔎 Scan started: **${remaining.length.toLocaleString()}** names` +
      (names.length > remaining.length ? ` (${names.length - remaining.length} skipped, already checked)` : "") +
      ` on ${slots.length} IP(s), ETA ~${formatDuration((remaining.length / perSec) * 1000)}. Hits will be posted here.`
    );
    runJob(client).catch((e) => console.error("runJob crashed:", e));
  },

  async status(i) {
    if (!job) return i.reply({ content: "No scan yet. Start one with /scan.", flags: EPH });
    return i.reply({ embeds: [statusEmbed(job)] });
  },

  async stop(i) {
    if (!job || job.status !== "running") return i.reply({ content: "No scan is running.", flags: EPH });
    job.status = "stopped";
    saveMeta();
    return i.reply("⏸️ Stopping... progress is saved, use /resume to continue.");
  },

  async resume(i, client) {
    if (job && job.status === "running") return i.reply({ content: "Already running.", flags: EPH });
    if (!job || job.status === "done") return i.reply({ content: "Nothing to resume.", flags: EPH });
    job.channelId = i.channelId;
    job.names = loadRemaining();
    job.cursor = 0;
    job.checked = job.total - job.names.length;
    if (!job.names.length) return i.reply({ content: "Nothing left to check.", flags: EPH });
    await i.reply(`▶️ Resuming: ${job.names.length.toLocaleString()} names left.`);
    runJob(client).catch((e) => console.error("runJob crashed:", e));
  },

  async hits(i) {
    if (!fs.existsSync(AVAILABLE_FILE) || !fs.statSync(AVAILABLE_FILE).size) {
      return i.reply({ content: "No available names found yet.", flags: EPH });
    }
    return i.reply({ files: [new AttachmentBuilder(AVAILABLE_FILE, { name: "available.txt" })] });
  },

  async reset(i) {
    if (job && job.status === "running") return i.reply({ content: "Stop the scan first with /stop.", flags: EPH });
    for (const f of [META_FILE, NAMES_FILE, RESULTS_FILE, AVAILABLE_FILE]) fs.rmSync(f, { force: true });
    job = null;
    return i.reply("🗑️ Progress and results cleared.");
  },
};

// ---------- boot ----------
const client = new Client({ intents: [GatewayIntentBits.Guilds] });

client.once(Events.ClientReady, async (c) => {
  console.log(`Logged in as ${c.user.tag} | ${slots.length} IP slot(s) | delay ${DELAY}ms`);
  const rest = new REST({ version: "10" }).setToken(TOKEN);
  await rest.put(
    GUILD_ID ? Routes.applicationGuildCommands(CLIENT_ID, GUILD_ID) : Routes.applicationCommands(CLIENT_ID),
    { body: commands }
  );
  console.log("Slash commands registered.");

  job = loadJobFromDisk();
  if (job && job.status === "running" && job.names.length) {
    console.log("Resuming scan after restart...");
    const ch = await client.channels.fetch(job.channelId).catch(() => null);
    await ch?.send("🔄 Bot restarted, resuming the scan...").catch(() => {});
    runJob(client).catch((e) => console.error("runJob crashed:", e));
  } else if (job && job.status === "running") {
    job.status = "done";
    saveMeta();
  }
});

client.on(Events.InteractionCreate, async (i) => {
  if (!i.isChatInputCommand()) return;
  if (!isAllowed(i)) return i.reply({ content: "You're not allowed to use this bot.", flags: EPH });
  try {
    await handlers[i.commandName]?.(i, client);
  } catch (err) {
    console.error(err);
    const msg = `Something broke: ${err.message}`;
    if (i.deferred || i.replied) i.editReply(msg).catch(() => {});
    else i.reply({ content: msg, flags: EPH }).catch(() => {});
  }
});

// Optional tiny HTTP server (needed if you deploy as a Render *web service* + uptime pinger)
if (process.env.PORT) {
  http.createServer((_, res) => res.end("ok")).listen(process.env.PORT, () => console.log("HTTP keep-alive on", process.env.PORT));
}

// On deploy/restart Render sends SIGTERM: save state, status stays "running" so it auto-resumes on boot
process.on("SIGTERM", () => {
  saveMeta();
  process.exit(0);
});

client.login(TOKEN);
