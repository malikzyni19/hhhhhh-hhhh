# TradingView MCP — local setup

Connects Claude Code to the TradingView **Desktop** app so it can read charts,
push Pine scripts, and read real compiler errors back.

Source: <https://github.com/tradesdontlie/tradingview-mcp>

## It only works on the machine running TradingView

The server talks to TradingView Desktop over Chrome DevTools Protocol on
`127.0.0.1:9222`. It is a **localhost** connection to an app on your own
computer.

That means it cannot work from a Claude Code session running in a remote
container — there is no TradingView Desktop there and no route to your
localhost. **Run Claude Code locally, in this repo, on the machine where
TradingView Desktop is installed.**

## Install (one time)

This repo gitignores `.mcp.json` and `.claude/`, so the config **cannot be
shipped through git** — it has to be created on your machine. That is what the
script does:

```bash
bash scripts/install_tv_mcp.sh
```

It clones the server into `tradingview-mcp/`, runs `npm install`, writes a
`.mcp.json` pointing at `./tradingview-mcp/src/server.js` (a relative path, so no
username or absolute path needs editing), and copies the bundled skills into
`.claude/skills/`. If a `.mcp.json` already exists it refuses to overwrite it and
prints the entry to merge by hand.

On Windows, run it from Git Bash or WSL — or do the four steps manually, they are
short.

**Restart Claude Code afterwards.** MCP servers are only read at startup.

## Launch TradingView with the debug port

The debug port is **off by default** and has to be enabled deliberately.

Easiest: ask Claude to run the `tv_launch` tool once the server is connected —
it auto-detects the install on Windows, Mac and Linux.

Manual, Windows (TradingView now ships as an MSIX package):

```bat
tradingview-mcp\scripts\launch_tv_debug.bat
```

If Windows blocks launching from `WindowsApps` with *"Access is denied"*,
`tv_launch` copies the package to `%LOCALAPPDATA%\tradingview-mcp\` once (~330 MB)
and launches from there, preserving login and layout. Do **not** try to change
permissions on `WindowsApps` with `icacls` — it fails and can break app servicing.

Manual, Mac:

```bash
/Applications/TradingView.app/Contents/MacOS/TradingView --remote-debugging-port=9222
```

## Why this is worth setting up for this project

Every script in `indicators/` has shipped **compile-untested**, because a remote
session has no Pine compiler. That cost several round trips — the line-
continuation error, the leftover `dUpS` references, the 74-plot limit — each one
needing a paste, a screenshot, and a fix.

With this connected locally, Claude can push a script, read the actual compiler
errors, and fix them before you ever see them. The bundled `pine-develop` skill
describes exactly that loop.

## What is available without it

One piece works offline: the repo's `analyze()` function is a pure static
checker — array bounds, zero-size arrays, `strategy.*` without a `strategy()`
declaration, and Pine version. It is **not** a compiler and would not have caught
any of the errors listed above, but it is a cheap pre-flight check.

```bash
node -e "import('./tradingview-mcp/src/core/pine.js').then(m=>
  console.log(JSON.stringify(m.analyze({source:require('fs').readFileSync(process.argv[1],'utf8')}),null,2)))" \
  indicators/cvd_engine_p5.pine
```

## Bundled skills

Copied into `.claude/skills/`; they load automatically when Claude Code runs in
this repo:

| Skill | Purpose |
|---|---|
| `pine-develop` | write → compile → read errors → fix, in a loop |
| `chart-analysis` | read the current chart and its indicators |
| `multi-symbol-scan` | run the same read across a watchlist |
| `replay-practice` | drive TradingView's bar replay |
| `strategy-report` | pull strategy tester output |

They call the MCP tools, so they are inert until the server is connected.

## Notes

- Requires a valid TradingView subscription and the Desktop app. It reads the app
  you are already running; it does not connect to TradingView's servers.
- It uses undocumented internal APIs and can break on any TradingView update.
- An entry in `.mcp.json` starts automatically when Claude Code opens this repo.
  Remove the `tradingview` block from `.mcp.json` to disable it.
