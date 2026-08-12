#!/usr/bin/env bash
# Installs the TradingView MCP server for LOCAL Claude Code use.
# Run from the repo root, on the machine where TradingView Desktop is installed.
#
# .mcp.json and .claude/ are gitignored in this repo, so they cannot be shipped
# through git — this script recreates them locally instead.
set -euo pipefail
cd "$(dirname "$0")/.."

if [ ! -d tradingview-mcp ]; then
  git clone https://github.com/tradesdontlie/tradingview-mcp.git tradingview-mcp
fi
( cd tradingview-mcp && npm install )

# Relative server path, so no username or absolute path needs editing.
if [ -f .mcp.json ]; then
  echo "!! .mcp.json exists — merge this entry by hand instead of overwriting:"
  echo '   "tradingview": { "command": "node", "args": ["./tradingview-mcp/src/server.js"] }'
else
  cat > .mcp.json <<'JSON'
{
  "mcpServers": {
    "tradingview": {
      "command": "node",
      "args": ["./tradingview-mcp/src/server.js"]
    }
  }
}
JSON
  echo "wrote .mcp.json"
fi

mkdir -p .claude/skills
for d in tradingview-mcp/skills/*/; do
  cp -r "$d" ".claude/skills/$(basename "$d")"
  echo "skill: $(basename "$d")"
done

echo
echo "Done. Restart Claude Code — MCP servers are only read at startup."
echo "Then launch TradingView with the debug port (see indicators/TRADINGVIEW_MCP.md)."
