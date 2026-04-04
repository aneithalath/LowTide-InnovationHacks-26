
const fs = require("fs");
let code = fs.readFileSync("src/App.tsx", "utf8");

// 1. type EmergencyVehicleRuntime
code = code.replace(/crew: string\n\}/, `crew: string\n  overrideStatus?: EmergencyVehicleStatus\n  overrideLocation?: { latitude: number; longitude: number; headingDegrees: number }\n}`);
code = code.replace(/crew: string\r\n\}/, `crew: string\r\n  overrideStatus?: EmergencyVehicleStatus\r\n  overrideLocation?: { latitude: number; longitude: number; headingDegrees: number }\r\n}`);

// 2. buildEmergencyVehicleSnapshot
code=code.replace("const status = resolveEmergencyStatusFromPulse(statusPulse)", "const originalStatus = resolveEmergencyStatusFromPulse(statusPulse);\n    const status = runtime.overrideStatus ?? originalStatus;");
code=code.replace("const sampledPosition = sampleEmergencyRoutePosition(route, runtime.distanceAlongRouteMeters)", "const defaultPosition = sampleEmergencyRoutePosition(route, runtime.distanceAlongRouteMeters);\n    const sampledPosition = runtime.overrideLocation ? Object.assign({}, defaultPosition, { latitude: runtime.overrideLocation.latitude, longitude: runtime.overrideLocation.longitude }) : defaultPosition;");
code=code.replace("const speedMph = metersPerSecondToMph(speedMetersPerSecond)", "const speedMph = (status === \"staged\" && runtime.overrideLocation) ? 0 : metersPerSecondToMph(speedMetersPerSecond);");

// 3. Adding hooks
const hookSite = /const \[isUnitWidgetDismissed, setIsUnitWidgetDismissed\] = useState\(false\)/;
const injectionHooks = `const [isUnitWidgetDismissed, setIsUnitWidgetDismissed] = useState(false)
  const [isLayersSectionOpen, setIsLayersSectionOpen] = useState(true)
  const [isEmergencySectionOpen, setIsEmergencySectionOpen] = useState(false)
  const [dispatchFormState, setDispatchFormState] = useState<Record<string, { lat: string, lng: string }>>({})
  const [expandedControlVehicle, setExpandedControlVehicle] = useState<string | null>(null)

  const handleUpdateDispatchForm = (key: string, field: "lat"|"lng", value: string) => {
    setDispatchFormState(prev => ({ ...prev, [key]: { ...prev[key], [field]: value } }))
  }

  const manuallyDispatchVehicle = (key: string) => {
    const form = dispatchFormState[key]
    if (!form || !form.lat || !form.lng) return
    const lat = parseFloat(form.lat)
    const lng = parseFloat(form.lng)
    if (isNaN(lat) || isNaN(lng)) return
    const runtime = emergencyVehicleRuntimeRef.current.find(r => r.key === key)
    if (runtime) {
      runtime.overrideLocation = { latitude: lat, longitude: lng, headingDegrees: 0 }
      runtime.overrideStatus = "responding"
    }
  }

  const manuallyStageVehicle = (key: string) => {
    const runtime = emergencyVehicleRuntimeRef.current.find(r => r.key === key)
    const snapshot = emergencyVehicles.find(r => r.key === key)
    if (runtime && snapshot) {
      runtime.overrideLocation = { latitude: snapshot.latitude, longitude: snapshot.longitude, headingDegrees: snapshot.headingDegrees }
      runtime.overrideStatus = "staged"
    }
  }

  const manuallyPatrolVehicle = (key: string) => {
    const runtime = emergencyVehicleRuntimeRef.current.find(r => r.key === key)
    if (runtime) {
      runtime.overrideLocation = undefined
      runtime.overrideStatus = "patrolling"
    }
  }
`;
code=code.replace(hookSite, injectionHooks);

// 4. Rewriting the UI
// Wrapping Command Layers inside isLayersSectionOpen
code = code.replace(/<div className="panel-heading">[\s\S]*?<h1>Unit Tracking<\/h1>[\s\S]*?<\/div>/, `<div className="panel-heading">
            <p>Command Layers</p>
            <h1>Unit Tracking</h1>
          </div>
          
          <div className="section-heading" onClick={() => setIsLayersSectionOpen(o => !o)} style={{ cursor: "pointer", marginTop: "1rem", borderBottom: "1px solid #333", paddingBottom: "4px" }}>
            <h2 style={{ fontSize: "14px", margin: 0, textTransform: "uppercase", letterSpacing: "0.05em", color: "#ccc" }}>Layers {isLayersSectionOpen ? "?" : "?"}</h2>
          </div>
          
          {isLayersSectionOpen && (
            <>`);

// Emergency vehicles panel injected right above the very FIRST panel-metrics
const emergencyPanel = `  </>
          )}

          <div className="section-heading" onClick={() => setIsEmergencySectionOpen(o => !o)} style={{ cursor: "pointer", marginTop: "1rem", borderBottom: "1px solid #333", paddingBottom: "4px" }}>
            <h2 style={{ fontSize: "14px", margin: 0, textTransform: "uppercase", letterSpacing: "0.05em", color: "#ccc" }}>Emergency Vehicles {isEmergencySectionOpen ? "?" : "?"}</h2>
          </div>
          
          {isEmergencySectionOpen && (
            <div className="emergency-vehicles-section" style={{ marginTop: "0.5rem" }}>
              {emergencyVehicles.slice().sort((a,b) => a.vehicleType.localeCompare(b.vehicleType)).map(vehicle => {
                const statusColor = vehicle.status === "patrolling" ? "green" : vehicle.status === "responding" ? "yellow" : "red";
                const isExpanded = expandedControlVehicle === vehicle.key;
                return (
                  <div key={vehicle.key} style={{ padding: "0.5rem", borderBottom: "1px dotted #333" }}>
                    <div 
                      style={{ display: "flex", alignItems: "center", cursor: "pointer" }} 
                      onClick={() => setExpandedControlVehicle(isExpanded ? null : vehicle.key)}
                    >
                      <span style={{ width: "10px", height: "10px", borderRadius: "50%", backgroundColor: statusColor, marginRight: "8px", flexShrink: 0 }}></span>
                      <strong>{vehicle.unitCode} ({vehicle.vehicleType}) - {vehicle.status}</strong>
                    </div>
                    
                    {isExpanded && (
                      <div style={{ marginTop: "0.5rem", paddingLeft: "18px", display: "flex", flexDirection: "column", gap: "4px" }}>
                        <div style={{ display: "flex", gap: "4px", marginBottom: "4px" }}>
                          <input 
                            type="text" 
                            placeholder="Lat" 
                            value={dispatchFormState[vehicle.key]?.lat || ""} 
                            onChange={(e) => handleUpdateDispatchForm(vehicle.key, "lat", e.target.value)}
                            style={{ flex: 1, minWidth: 0, backgroundColor: "#1e1e1e", color: "white", border: "1px solid #444", padding: "2px" }}
                          />
                          <input 
                            type="text" 
                            placeholder="Lng" 
                            value={dispatchFormState[vehicle.key]?.lng || ""} 
                            onChange={(e) => handleUpdateDispatchForm(vehicle.key, "lng", e.target.value)}
                            style={{ flex: 1, minWidth: 0, backgroundColor: "#1e1e1e", color: "white", border: "1px solid #444", padding: "2px" }}
                          />
                          <button onClick={(e) => { e.stopPropagation(); manuallyDispatchVehicle(vehicle.key); }} style={{ padding: "2px 6px", backgroundColor: "#333", color: "white", border: "1px solid #555" }}>Go</button>
                        </div>
                        <div style={{ display: "flex", gap: "4px" }}>
                          <button onClick={(e) => { e.stopPropagation(); manuallyStageVehicle(vehicle.key); }} style={{ padding: "4px", backgroundColor: "#333", color: "white", border: "1px solid #555", flex: 1 }}>Stage Here</button>
                          <button onClick={(e) => { e.stopPropagation(); manuallyPatrolVehicle(vehicle.key); }} style={{ padding: "4px", backgroundColor: "#333", color: "white", border: "1px solid #555", flex: 1 }}>Patrol</button>
                        </div>
                      </div>
                    )}
                  </div>
                )
              })}
            </div>
          )}

          <div className="panel-metrics">`;

code = code.replace(/<div className="panel-metrics">/, emergencyPanel);

fs.writeFileSync("src/App.tsx", code);
console.log("All fixes applied");

