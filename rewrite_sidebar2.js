
const fs = require("fs");
let code = fs.readFileSync("mapping-demo/src/App.tsx", "utf8");

const replacement = `  </>
          )}

          <div className="section-heading" onClick={() => setIsEmergencySectionOpen(o => !o)} style={{ cursor: "pointer", marginTop: "1rem", borderBottom: "1px solid #333", paddingBottom: "4px" }}>
            <h2 style={{ fontSize: "14px", margin: 0, textTransform: "uppercase", letterSpacing: "0.05em", color: "#ccc" }}>Emergency Vehicles {isEmergencySectionOpen ? "Gû+" : "Gû¦"}</h2>
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
                        <div style={{ display: "flex", gap: "4px" }}>
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

code = code.replace(/<div className="panel-metrics">/, replacement);
fs.writeFileSync("mapping-demo/src/App.tsx", code);
console.log("Panel metrics replaced.");

