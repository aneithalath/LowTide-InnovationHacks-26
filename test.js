
const fs = require("fs");
let code = fs.readFileSync("mapping-demo/src/App.tsx", "utf8");
console.log(code.match(/<div className="panel-stats">[\s\S]*?<span>Incident Feeds<\/span>/) !== null);
console.log(code.match(/<div className="panel-heading">[\s\S]*?<h1>Unit Tracking<\/h1>[\s\S]*?<\/div>/) !== null);

