
const fs=require("fs");
let code=fs.readFileSync("mapping-demo/src/App.tsx","utf8");
const hookSite = /const \[isUnitWidgetDismissed, setIsUnitWidgetDismissed\] = useState\(false\)/;
const injection = `const [isUnitWidgetDismissed, setIsUnitWidgetDismissed] = useState(false)
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

code=code.replace(hookSite, injection);
fs.writeFileSync("mapping-demo/src/App.tsx", code);

