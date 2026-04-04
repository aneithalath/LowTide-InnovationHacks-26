
const fs = require("fs");
const code = fs.readFileSync("mapping-demo/src/App.tsx", "utf8");
const match = code.match(/<div className="terminal-sidebar__panel">[\s\S]*?(?=<\/aside>)/);
fs.writeFileSync("sidebar_match.txt", match ? match[0] : "no match");

