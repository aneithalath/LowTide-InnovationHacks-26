const fs = require('fs');
let code = fs.readFileSync('mapping-demo/src/App.tsx', 'utf8');

const oldPattern = /const speedPulse =.*?Math\.max\(4, Math\.round\(4 \+ speedPulse \* 9\)\)/s;

const newSnippet = \const speedPulse = (Math.sin(timestamp / 7_600 + runtime.speedPhaseSeed) + 1) / 2
    const statusPulse = Math.sin(timestamp / 10_400 + runtime.statusPhaseSeed)
    const originalStatus = resolveEmergencyStatusFromPulse(statusPulse)
    const status = runtime.overrideStatus ?? originalStatus
    const speedMetersPerSecond = runtime.baseSpeedMetersPerSecond * (0.72 + speedPulse * 0.42)
    const defaultPosition = sampleEmergencyRoutePosition(route, runtime.distanceAlongRouteMeters)
    const sampledPosition = runtime.overrideLocation
      ? Object.assign({}, defaultPosition, { latitude: runtime.overrideLocation.latitude, longitude: runtime.overrideLocation.longitude })
      : defaultPosition
    const speedMph = (status === 'staged' && runtime.overrideLocation) ? 0 : metersPerSecondToMph(speedMetersPerSecond)
    const etaMinutes =
      status === 'responding'
        ? Math.max(2, Math.round(2 + (1 - speedPulse) * 6))
        : status === 'staged'
          ? Math.max(7, Math.round(7 + speedPulse * 7))
          : Math.max(4, Math.round(4 + speedPulse * 9))\;

code = code.replace(oldPattern, newSnippet);
fs.writeFileSync('mapping-demo/src/App.tsx', code);
console.log('Update complete.');
