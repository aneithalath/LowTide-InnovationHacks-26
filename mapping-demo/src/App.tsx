import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type PointerEvent as ReactPointerEvent,
} from 'react'
import * as maptilersdk from '@maptiler/sdk'
import Hls from 'hls.js'
import '@maptiler/sdk/dist/maptiler-sdk.css'
import './App.css'

type IncidentUpdate = {
  text: string
  ts: number
  type: string
}

type IncidentResult = {
  key: string
  title: string
  address?: string
  rawLocation?: string
  location?: string
  latitude: number
  longitude: number
  severity?: string
  level?: number
  updates?: Record<string, IncidentUpdate>
}

type IncidentApiResponse = {
  results?: IncidentResult[]
}

type PredictionApiResponse = {
  prediction_id: string
  timestamp: string
  risk_assessment: {
    level: string
    coordinates: {
      latitude: number
      longitude: number
    }
    location_name: string
    risk_factors: string[]
    explanation: string
  }
  mitigation_strategy: {
    police_dispatch: {
      action: string
      assigned_units: {
        vehicle_id: string
        status: string
      }[]
    }
    medical_standby: {
      unit_id: string
      instruction: string
      message: string
      standby_location: {
        latitude: number
        longitude: number
      }
    }
    traffic_control: {
      're-routing': string
    }
  }
}

type ThreatActionItem = {
  id: string
  label: string
  detail: string
  kind: 'dispatch' | 'medical' | 'traffic'
  dispatchVehicleId?: string
}

type ThreatActionStatus = 'idle' | 'active' | 'complete' | 'error'

type OpenSkyApiResponse = {
  states?: unknown[]
}

type AircraftPinTheme = 'rotorcraft' | 'heavy' | 'light' | 'glider' | 'uav' | 'surface' | 'unknown'

type RotorcraftUnit = {
  key: string
  icao24: string
  callsign: string
  callsignRaw: string | null
  originCountry: string
  timePosition: number | null
  latitude: number
  longitude: number
  baroAltitude: number | null
  onGround: boolean
  velocityMetersPerSecond: number | null
  trueTrack: number | null
  verticalRate: number | null
  sensors: number[]
  geoAltitude: number | null
  squawk: string
  spi: boolean
  positionSource: number | null
  category: number | null
  categoryLabel: string
  icon: string
  pinTheme: AircraftPinTheme
  speedKnots: number | null
  altitudeMeters: number | null
  lastContact: number | null
}

type RotorcraftFeedStats = {
  totalStates: number
  statesWithCategory: number
  statesWithCoordinates: number
  plottedStates: number
}

type RotorcraftParseResult = {
  rotorcraft: RotorcraftUnit[]
  feedStats: RotorcraftFeedStats
}

type AircraftCacheSnapshot = {
  fetchedAt: number
  bounds: ViewportBounds
  feedStats: RotorcraftFeedStats
  aircraft: RotorcraftUnit[]
}

type ViewportBounds = {
  lowerLatitude: number
  lowerLongitude: number
  upperLatitude: number
  upperLongitude: number
}

type LayerState = {
  incidentPins: boolean
  congregationPins: boolean
  cctvPins: boolean
  emergencyVehiclePins: boolean
  rotorcraftPins: boolean
  trafficFlow: boolean
  riskHeatmap: boolean
  tacticalGrid: boolean
  scanLines: boolean
}

type CongregationSubLayerKey = 'worship' | 'school' | 'stadium' | 'arena' | 'hospital'

type CongregationSubLayerState = Record<CongregationSubLayerKey, boolean>

type PinSeverity = 'yellow' | 'red'

type IncidentUnit = IncidentResult & {
  level: number
  severity: PinSeverity
}

type UnitWidgetPosition = {
  x: number
  y: number
}

type UnitWidgetDragState = {
  pointerId: number
  pointerOffsetX: number
  pointerOffsetY: number
}

type ReligionGroup =
  | 'christian'
  | 'muslim'
  | 'jewish'
  | 'buddhist'
  | 'hindu'
  | 'sikh'
  | 'other'

type CongregationCategory = string

type CongregationPinTheme = ReligionGroup | 'school' | 'stadium' | 'arena' | 'hospital'

type CongregationPlace = {
  key: string
  latitude: number
  longitude: number
  name: string
  osmType: 'node' | 'way' | 'relation'
  osmId: string
  category: CongregationCategory
  categoryLabel: string
  pinTheme: CongregationPinTheme
  religionRaw: string
  denominationRaw: string
  religionLabel: string
  icon: string
  tags: Record<string, string>
}

type CctvStreamType = 'youtube' | 'hls' | 'iframe'

type CctvCamera = {
  key: string
  name: string
  latitude: number
  longitude: number
  streamType: CctvStreamType
  streamUrl: string
  streamLabel: string
}

type EmergencyVehicleType = 'police' | 'ambulance' | 'firetruck'

type EmergencyVehicleStatus = 'patrolling' | 'responding' | 'staged'

type EmergencyVehicleRouteBlueprint = {
  id: string
  label: string
  points: [number, number][]
}

type EmergencyVehicleRoute = {
  id: string
  label: string
  points: [number, number][]
  segmentLengths: number[]
  totalLength: number
}

type EmergencyVehicleRuntime = {
  key: string
  unitCode: string
  vehicleType: EmergencyVehicleType
  routeIndex: number
  distanceAlongRouteMeters: number
  baseSpeedMetersPerSecond: number
  speedPhaseSeed: number
  statusPhaseSeed: number
  fuelPhaseSeed: number
  assignment: string
  district: string
  crew: string
  dispatchRoute: EmergencyVehicleRoute | null
  dispatchDistanceMeters: number
  dispatchTargetLabel: string
  dispatchActionId: string
  isStationedAtDispatchTarget: boolean
  overrideLocation?: { latitude: number, longitude: number, headingDegrees: number }
  overrideStatus?: 'patrolling' | 'responding' | 'staged'
}

type EmergencyFleetUnitSeed = {
  id: string
  vehicleType: EmergencyVehicleType
}

type EmergencyFleetSnapshot = {
  targetSpeedMph: number
  routes: EmergencyVehicleRouteBlueprint[]
  units: EmergencyFleetUnitSeed[]
}

type RoadWaypoint = {
  latitude: number
  longitude: number
}

type RoadRouteApiResponse = {
  coordinates: [number, number][]
  distance_meters: number
}

type EmergencyVehicleUnit = {
  key: string
  unitCode: string
  vehicleType: EmergencyVehicleType
  vehicleLabel: string
  icon: string
  status: EmergencyVehicleStatus
  latitude: number
  longitude: number
  headingDegrees: number
  speedMph: number
  assignment: string
  district: string
  routeLabel: string
  crew: string
  etaMinutes: number
  fuelLevelPercent: number
  lastUpdate: number
  telemetry: string[]
  dispatchRoutePoints?: [number, number][]
}

type SelectedUnit =
  | {
      kind: 'incident'
      key: string
    }
  | {
      kind: 'congregation'
      key: string
    }
  | {
      kind: 'aircraft'
      key: string
    }
  | {
      kind: 'cctv'
      key: string
    }
  | {
      kind: 'emergencyVehicle'
      key: string
    }

type AircraftDetailField = {
  index: number
  property: string
  type: string
  value: string
  description: string
}

type CongregationCacheSnapshot = {
  coveredTilesByCategory: Record<string, Set<string>>
  places: CongregationPlace[]
}

type RiskHeatmapLevel = 'low' | 'medium' | 'high'

type RiskGridCell = {
  key: string
  west: number
  east: number
  south: number
  north: number
  centerLatitude: number
  centerLongitude: number
}

type RiskHeatmapFeature = {
  type: 'Feature'
  geometry: {
    type: 'Polygon'
    coordinates: number[][][]
  }
  properties: {
    riskScore: number
    riskLevel: RiskHeatmapLevel
    trafficScore: number
    highRiskSignal: number
    safetySignal: number
  }
}

type RiskHeatmapFeatureCollection = {
  type: 'FeatureCollection'
  features: RiskHeatmapFeature[]
}

type TomTomFlowSegmentResponse = {
  flowSegmentData?: {
    currentSpeed?: number
    freeFlowSpeed?: number
    confidence?: number
    roadClosure?: boolean
  }
}

type TrafficSampleCacheEntry = {
  fetchedAt: number
  congestionScore: number | null
}

const MAPTILER_KEY = import.meta.env.VITE_MAPTILER_API_KEY ?? '2LM4tlOmYm48CvePNf70'
const TOMTOM_TRAFFIC_API_KEY =
  import.meta.env.VITE_TOMTOM_TRAFFIC_API_KEY ?? 'Bim3HMLn0bFpUvzdbhR7tm5HIbQXtm8K'
const TOMTOM_TRAFFIC_FLOW_STYLE = 'relative0-dark'
const TOMTOM_TRAFFIC_SOURCE_ID = 'tomtom-traffic-flow-raster-source'
const TOMTOM_TRAFFIC_LAYER_ID = 'tomtom-traffic-flow-raster-layer'
const TOMTOM_TRAFFIC_SEGMENT_BASE =
  'https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json'
const TOMTOM_TRAFFIC_FLOW_TILE_TEMPLATE = `https://api.tomtom.com/traffic/map/4/tile/flow/${TOMTOM_TRAFFIC_FLOW_STYLE}/{z}/{x}/{y}.png?key=${TOMTOM_TRAFFIC_API_KEY}&tileSize=256`
const RISK_HEATMAP_SOURCE_ID = 'risk-heatmap-grid-source'
const RISK_HEATMAP_FILL_LAYER_ID = 'risk-heatmap-grid-fill-layer'
const RISK_HEATMAP_OUTLINE_LAYER_ID = 'risk-heatmap-grid-outline-layer'
const RISK_HEATMAP_TRAFFIC_TTL_MS = 120_000
const RISK_HEATMAP_TRAFFIC_QUANTIZATION_DEGREES = 0.004
const RISK_HEATMAP_TRAFFIC_CACHE_LIMIT = 280
const RISK_HEATMAP_TARGET_CELL_SIZE_METERS = 920
const RISK_HEATMAP_MIN_GRID_COLUMNS = 4
const RISK_HEATMAP_MAX_GRID_COLUMNS = 6
const RISK_HEATMAP_MIN_GRID_ROWS = 4
const RISK_HEATMAP_MAX_GRID_ROWS = 6
const RISK_HEATMAP_PROXIMITY_RADIUS_METERS = 780
const INCIDENT_LIMIT = 20
const CONGREGATION_LIMIT = 220
const AIRCRAFT_LIMIT = 180
const CITIZEN_DIRECT_BASE = 'https://citizen.com/api/incident/trending'
const CITIZEN_PROXY_BASE = '/api/citizen/incident/trending'
const OPENSKY_STATES_PROXY_BASE = '/api/opensky/states/all'
const OVERPASS_ENDPOINT = 'https://overpass-api.de/api/interpreter'
const CONGREGATION_CACHE_API = '/api/congregation-cache'
const CONGREGATION_CACHE_FILE = '/congregation-cache.json'
const CONGREGATION_CACHE_STORAGE_KEY = 'mapping-demo:congregation-cache:v1'
const CCTV_SOURCE_FILE = '/cams.json'
const EMERGENCY_FLEET_SOURCE_FILE = '/emergency-fleet.json'
const EMERGENCY_ROUTE_DISPATCH_API =
  import.meta.env.VITE_EMERGENCY_ROUTE_DISPATCH_API ?? 'http://127.0.0.1:8000/route/dispatch'
const EMERGENCY_ROUTE_WAYPOINTS_API =
  import.meta.env.VITE_EMERGENCY_ROUTE_WAYPOINTS_API ?? 'http://127.0.0.1:8000/route/waypoints'
const AIRCRAFT_CACHE_API = '/api/aircraft-cache'
const AIRCRAFT_CACHE_FILE = '/aircraft-cache.json'
const AIRCRAFT_CACHE_STORAGE_KEY = 'mapping-demo:aircraft-cache:v1'
const AIRCRAFT_CACHE_TTL_MS = 120_000
const AIRCRAFT_CACHE_MIN_VIEWPORT_OVERLAP = 0.82
const CONGREGATION_CACHE_TILE_SIZE = 0.025
const CONGREGATION_CACHE_MAX_TILE_SCAN = 4_000
const EMERGENCY_SIMULATION_UPDATE_MS = 1_000
const EMERGENCY_PATROL_SPEED_MPH = 45
const KNOTS_PER_METER_PER_SECOND = 1.943844
const FEET_PER_METER = 3.28084
const METERS_PER_MILE = 1_609.344
const METERS_PER_DEGREE_LATITUDE = 111_320
const RISK_PREDICTION_API =
  import.meta.env.VITE_RISK_PREDICTION_API ?? 'http://127.0.0.1:8000/prediction'
const EMPTY_ROTORCRAFT_FEED_STATS: RotorcraftFeedStats = {
  totalStates: 0,
  statesWithCategory: 0,
  statesWithCoordinates: 0,
  plottedStates: 0,
}
const DEFAULT_RISK_PREDICTION: PredictionApiResponse = {
  prediction_id: 'risk-analysis-2026-04-04-8832',
  timestamp: '2026-04-04T13:25:00Z',
  risk_assessment: {
    level: 'High',
    coordinates: {
      latitude: 33.4255,
      longitude: -111.94,
    },
    location_name: 'Mill Avenue & University Drive',
    risk_factors: [
      'Heavy congestion following a stadium event',
      'Historical data indicating high pedestrian-vehicle conflict at this hour',
      'Recent social media reports of an unsanctioned street gathering nearby',
    ],
    explanation:
      "A high-risk event is predicted due to the convergence of 'after-stadium' foot traffic and peak-hour vehicle congestion. The risk is compounded by recent citizen incident reports of aggressive driving in the immediate vicinity and a scheduled large-scale street festival nearby that has exceeded its planned capacity, creating a high probability of crowd crush or pedestrian-involved collisions.",
  },
  mitigation_strategy: {
    police_dispatch: {
      action: 'Deploy for traffic calming and crowd monitoring',
      assigned_units: [
        {
          vehicle_id: 'P-104',
          status: 'En route',
        },
        {
          vehicle_id: 'P-212',
          status: 'En route',
        },
      ],
    },
    medical_standby: {
      unit_id: 'AMB-09',
      instruction: 'Pre-notification',
      message:
        'Potential high-density incident at Mill & University. No immediate dispatch required; remain on standby at Station 4 for rapid response if situation escalates.',
      standby_location: {
        latitude: 33.422,
        longitude: -111.935,
      },
    },
    traffic_control: {
      're-routing':
        'Automated signal timing adjustment implemented for Northbound traffic to reduce pedestrian dwell time.',
    },
  },
}
const EMPTY_RISK_HEATMAP_FEATURE_COLLECTION: RiskHeatmapFeatureCollection = {
  type: 'FeatureCollection',
  features: [],
}
const DEFAULT_CENTER: [number, number] = [-111.93439253258266, 33.41770559781321]
const DEFAULT_ZOOM = 12.4
const DEFAULT_PITCH = 54
const DEFAULT_BEARING = -16

const DEFAULT_LAYER_STATE: LayerState = {
  incidentPins: false,
  congregationPins: false,
  cctvPins: false,
  emergencyVehiclePins: false,
  rotorcraftPins: false,
  trafficFlow: false,
  riskHeatmap: false,
  tacticalGrid: false,
  scanLines: false,
}

const DEFAULT_CONGREGATION_SUBLAYERS: CongregationSubLayerState = {
  worship: false,
  school: false,
  stadium: false,
  arena: false,
  hospital: false,
}

const CHRISTIAN_DENOMINATION_HINTS = [
  'adventist',
  'anglican',
  'apostolic',
  'baptist',
  'catholic',
  'church',
  'episcopal',
  'evangelical',
  'lutheran',
  'methodist',
  'orthodox',
  'pentecostal',
  'presbyterian',
  'protestant',
]

const RELIGION_ICON_BY_GROUP: Record<ReligionGroup, string> = {
  christian: '?',
  muslim: '?',
  jewish: '?',
  buddhist: '?',
  hindu: '?',
  sikh: '?',
  other: '?',
}

const RELIGION_LABEL_BY_GROUP: Record<ReligionGroup, string> = {
  christian: 'Christian',
  muslim: 'Muslim',
  jewish: 'Jewish',
  buddhist: 'Buddhist',
  hindu: 'Hindu',
  sikh: 'Sikh',
  other: 'Other',
}

const RELIGION_GROUPS: ReligionGroup[] = [
  'christian',
  'muslim',
  'jewish',
  'buddhist',
  'hindu',
  'sikh',
  'other',
]

const CONGREGATION_SUBLAYER_KEYS: CongregationSubLayerKey[] = [
  'worship',
  'school',
  'stadium',
  'arena',
  'hospital',
]

const CONGREGATION_SUBLAYER_LABELS: Record<CongregationSubLayerKey, string> = {
  worship: 'Places of Worship',
  school: 'Schools',
  stadium: 'Stadiums',
  arena: 'Arenas',
  hospital: 'Hospital',
}

const CONGREGATION_ICON_BY_CATEGORY: Record<CongregationSubLayerKey, string> = {
  worship: '?',
  school: '??',
  stadium: '??',
  arena: '??',
  hospital: '??',
}

const EMERGENCY_VEHICLE_LABEL_BY_TYPE: Record<EmergencyVehicleType, string> = {
  police: 'Police Patrol',
  ambulance: 'Ambulance',
  firetruck: 'Firetruck',
}

const EMERGENCY_VEHICLE_ICON_BY_TYPE: Record<EmergencyVehicleType, string> = {
  police: '??',
  ambulance: '??',
  firetruck: '??',
}

const EMERGENCY_STATUS_LABEL_BY_CODE: Record<EmergencyVehicleStatus, string> = {
  patrolling: 'Patrolling',
  responding: 'Responding',
  staged: 'Staged',
}

const EMERGENCY_VEHICLE_TYPES: EmergencyVehicleType[] = ['police', 'ambulance', 'firetruck']

const EMERGENCY_UNIT_COUNTS: Record<EmergencyVehicleType, number> = {
  police: 20,
  ambulance: 6,
  firetruck: 11,
}

const EMERGENCY_ASSIGNMENTS_BY_TYPE: Record<EmergencyVehicleType, string[]> = {
  police: [
    'Patrol sweep of priority corridor',
    'Traffic collision response support',
    'Downtown visibility patrol',
    'Perimeter assist for active incident',
  ],
  ambulance: [
    'Medical response transport standby',
    'Urgent care transfer routing',
    'Priority EMS call coverage',
    'Hospital transfer support',
  ],
  firetruck: [
    'Engine company hazard assessment',
    'Structure alarm staging route',
    'Hydrant readiness patrol',
    'Rescue support positioning',
  ],
}

const EMERGENCY_DISTRICT_LABELS = [
  'North Sector',
  'Central Sector',
  'River Corridor',
  'University District',
  'Industrial Belt',
  'South Sector',
]

const EMERGENCY_CREW_CODES = [
  'Team Atlas',
  'Team Beacon',
  'Team Cobalt',
  'Team Delta',
  'Team Echo',
  'Team Falcon',
  'Team Guardian',
  'Team Helix',
]

const EMERGENCY_ROUTE_BLUEPRINTS: EmergencyVehicleRouteBlueprint[] = [
  {
    id: 'mill-loop',
    label: 'Mill / Broadway Loop',
    points: [
      [-0.0065, 0.014],
      [-0.0035, 0.0125],
      [0.0008, 0.0105],
      [0.0052, 0.0085],
      [0.0072, 0.0042],
      [0.0062, -0.0018],
      [0.0032, -0.0048],
      [-0.0016, -0.0058],
      [-0.0058, -0.0034],
      [-0.0078, 0.0018],
      [-0.0065, 0.014],
    ],
  },
  {
    id: 'scottsdale-corridor',
    label: 'Scottsdale Corridor',
    points: [
      [0.0125, 0.017],
      [0.0142, 0.0112],
      [0.0158, 0.0048],
      [0.0154, -0.0024],
      [0.0138, -0.0094],
      [0.0105, -0.0122],
      [0.0072, -0.0088],
      [0.0092, -0.0016],
      [0.0114, 0.0064],
      [0.0125, 0.017],
    ],
  },
  {
    id: 'university-grid',
    label: 'University Grid',
    points: [
      [-0.015, 0.009],
      [-0.0095, 0.0102],
      [-0.0038, 0.0094],
      [0.0028, 0.0087],
      [0.0086, 0.0072],
      [0.0108, 0.0024],
      [0.0088, -0.0026],
      [0.0024, -0.0035],
      [-0.004, -0.0028],
      [-0.0108, -0.0012],
      [-0.015, 0.0034],
      [-0.015, 0.009],
    ],
  },
  {
    id: 'freeway-frontage',
    label: 'Freeway Frontage Sweep',
    points: [
      [-0.022, 0.0068],
      [-0.0155, 0.0056],
      [-0.0094, 0.004],
      [-0.0036, 0.0026],
      [0.0028, 0.0012],
      [0.0088, 0.0002],
      [0.0144, -0.0008],
      [0.0208, -0.0026],
      [0.0178, -0.0068],
      [0.0108, -0.0078],
      [0.0034, -0.007],
      [-0.0048, -0.006],
      [-0.0132, -0.0036],
      [-0.0198, 0.0016],
      [-0.022, 0.0068],
    ],
  },
  {
    id: 'river-channel',
    label: 'River Channel Patrol',
    points: [
      [-0.0102, 0.0204],
      [-0.0034, 0.0208],
      [0.0038, 0.0202],
      [0.0104, 0.0194],
      [0.0146, 0.0148],
      [0.0122, 0.0098],
      [0.0064, 0.0086],
      [-0.0006, 0.0084],
      [-0.0074, 0.0098],
      [-0.0112, 0.0146],
      [-0.0102, 0.0204],
    ],
  },
  {
    id: 'industrial-south',
    label: 'Industrial South Grid',
    points: [
      [-0.0164, -0.0102],
      [-0.0106, -0.0114],
      [-0.0042, -0.0118],
      [0.0024, -0.0124],
      [0.0082, -0.0112],
      [0.0118, -0.0074],
      [0.0106, -0.0028],
      [0.0052, -0.0008],
      [-0.0014, -0.0016],
      [-0.0078, -0.003],
      [-0.0138, -0.0054],
      [-0.0164, -0.0102],
    ],
  },
]

maptilersdk.config.apiKey = MAPTILER_KEY

const levelFromIncident = (incident: IncidentResult) => {
  if (!Number.isFinite(incident.level)) {
    return 0
  }

  return Math.max(0, Math.floor(incident.level as number))
}

const severityFromLevel = (level: number): PinSeverity => {
  if (level === 0) {
    return 'yellow'
  }

  return 'red'
}

const levelLabel = (level: number) => {
  if (level === 0) {
    return 'LEVEL 0'
  }

  return 'LEVEL 1+'
}

const threatActionStatusLabel = (status: ThreatActionStatus) => {
  if (status === 'active') {
    return 'Active'
  }

  if (status === 'complete') {
    return 'Complete'
  }

  if (status === 'error') {
    return 'Error'
  }

  return 'Pending'
}

const formatUpdateAge = (timestamp: number) => {
  const elapsedMs = Date.now() - timestamp

  if (elapsedMs < 60_000) {
    return 'JUST NOW'
  }

  const minutes = Math.round(elapsedMs / 60_000)
  if (minutes < 60) {
    return `${minutes}M AGO`
  }

  const hours = Math.round(minutes / 60)
  return `${hours}H AGO`
}

const buildCitizenTrendingUrl = (baseUrl: string, bounds: ViewportBounds) => {
  const params = new URLSearchParams({
    lowerLatitude: bounds.lowerLatitude.toFixed(6),
    lowerLongitude: bounds.lowerLongitude.toFixed(6),
    upperLatitude: bounds.upperLatitude.toFixed(6),
    upperLongitude: bounds.upperLongitude.toFixed(6),
    fullResponse: 'true',
    limit: `${INCIDENT_LIMIT}`,
  })

  return `${baseUrl}?${params.toString()}`
}

const buildOpenSkyStatesUrl = (bounds: ViewportBounds) => {
  const normalizedBounds = normalizeViewportBounds(bounds)
  const params = new URLSearchParams({
    lamin: normalizedBounds.south.toFixed(6),
    lomin: normalizedBounds.west.toFixed(6),
    lamax: normalizedBounds.north.toFixed(6),
    lomax: normalizedBounds.east.toFixed(6),
    extended: '1',
  })

  return `${OPENSKY_STATES_PROXY_BASE}?${params.toString()}`
}

const toFiniteNumber = (value: unknown) => {
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value : null
  }

  if (typeof value === 'string' && value.trim()) {
    const parsedValue = Number(value)
    return Number.isFinite(parsedValue) ? parsedValue : null
  }

  return null
}

const toFiniteInteger = (value: unknown) => {
  const finiteValue = toFiniteNumber(value)

  if (finiteValue === null) {
    return null
  }

  return Math.trunc(finiteValue)
}

const toBooleanValue = (value: unknown) => {
  if (typeof value === 'boolean') {
    return value
  }

  if (typeof value === 'number') {
    return value !== 0
  }

  if (typeof value === 'string') {
    const normalizedValue = value.trim().toLowerCase()

    if (normalizedValue === 'true' || normalizedValue === '1') {
      return true
    }

    if (normalizedValue === 'false' || normalizedValue === '0') {
      return false
    }
  }

  return null
}

const AIRCRAFT_CATEGORY_LABELS: Record<number, string> = {
  0: 'No category information',
  1: 'No ADS-B category information',
  2: 'Light',
  3: 'Small',
  4: 'Large',
  5: 'High vortex large',
  6: 'Heavy',
  7: 'High performance',
  8: 'Rotorcraft',
  9: 'Glider / sailplane',
  10: 'Lighter-than-air',
  11: 'Parachutist / skydiver',
  12: 'Ultralight / hang-glider / paraglider',
  13: 'Reserved',
  14: 'Unmanned aerial vehicle',
  15: 'Space / trans-atmospheric',
  16: 'Surface vehicle - emergency',
  17: 'Surface vehicle - service',
  18: 'Point obstacle',
  19: 'Cluster obstacle',
  20: 'Line obstacle',
}

const AIRCRAFT_ICON_BY_THEME: Record<AircraftPinTheme, string> = {
  rotorcraft: '??',
  heavy: '?',
  light: '??',
  glider: '??',
  uav: '??',
  surface: '??',
  unknown: '?',
}

const AIRCRAFT_PIN_THEMES: AircraftPinTheme[] = [
  'rotorcraft',
  'heavy',
  'light',
  'glider',
  'uav',
  'surface',
  'unknown',
]

const isAircraftPinTheme = (value: string): value is AircraftPinTheme =>
  AIRCRAFT_PIN_THEMES.includes(value as AircraftPinTheme)

const AIRCRAFT_POSITION_SOURCE_LABELS: Record<number, string> = {
  0: 'ADS-B',
  1: 'ASTERIX',
  2: 'MLAT',
  3: 'FLARM',
}

const resolveOpenSkyCategory = (value: unknown) => {
  const categoryCandidate = toFiniteNumber(value)

  if (categoryCandidate === null) {
    return null
  }

  return Math.max(0, Math.floor(categoryCandidate))
}

const resolveAircraftCategoryLabel = (category: number | null) => {
  if (category === null) {
    return 'Category unavailable'
  }

  return AIRCRAFT_CATEGORY_LABELS[category] ?? `Category ${category}`
}

const resolveAircraftPinTheme = (category: number | null): AircraftPinTheme => {
  if (category === 8) {
    return 'rotorcraft'
  }

  if (category === 14) {
    return 'uav'
  }

  if (category !== null && category >= 16) {
    return 'surface'
  }

  if (category === 9 || category === 10 || category === 11 || category === 12 || category === 13) {
    return 'glider'
  }

  if (category === 4 || category === 5 || category === 6 || category === 7 || category === 15) {
    return 'heavy'
  }

  if (category === 2 || category === 3) {
    return 'light'
  }

  return 'unknown'
}

const formatAircraftHeading = (trueTrack: number | null) =>
  trueTrack === null ? 'TRK N/A' : `TRK ${Math.round(trueTrack)}�`

const formatAircraftSpeed = (speedKnots: number | null) =>
  speedKnots === null ? 'GS N/A' : `GS ${Math.round(speedKnots)} KT`

const formatAircraftAltitude = (altitudeMeters: number | null) =>
  altitudeMeters === null ? 'ALT N/A' : `ALT ${Math.round(altitudeMeters * FEET_PER_METER)} FT`

const formatEmptyAircraftMessage = (feedStats: RotorcraftFeedStats) => {
  if (feedStats.totalStates <= 0) {
    return 'OpenSky returned no aircraft for this viewport.'
  }

  if (feedStats.statesWithCoordinates <= 0) {
    return `OpenSky returned ${feedStats.totalStates} states, but none had mappable coordinates.`
  }

  if (feedStats.statesWithCategory <= 0) {
    return `OpenSky returned ${feedStats.statesWithCoordinates} mappable aircraft, but none included category data (index 17).`
  }

  return `OpenSky returned ${feedStats.statesWithCoordinates} mappable aircraft, but none passed rendering filters.`
}

const formatUnixSeconds = (timestampSeconds: number | null) => {
  if (timestampSeconds === null) {
    return 'null'
  }

  return `${timestampSeconds} (${new Date(timestampSeconds * 1_000).toISOString()})`
}

const formatDetailNumber = (value: number | null, fractionDigits = 3) => {
  if (value === null) {
    return 'null'
  }

  return `${Number(value.toFixed(fractionDigits))}`
}

const formatDetailBoolean = (value: boolean) => (value ? 'true' : 'false')

const formatSensorsList = (sensors: number[]) =>
  sensors.length ? `[${sensors.map((sensorId) => `${sensorId}`).join(', ')}]` : 'null'

const resolvePositionSourceLabel = (positionSource: number | null) => {
  if (positionSource === null) {
    return 'null'
  }

  const sourceLabel = AIRCRAFT_POSITION_SOURCE_LABELS[positionSource]
  return sourceLabel ? `${positionSource} (${sourceLabel})` : `${positionSource}`
}

const buildAircraftDetailFields = (aircraft: RotorcraftUnit): AircraftDetailField[] =>
  (
    [
      {
        index: 0,
        property: 'icao24',
        raw: aircraft.icao24,
        type: 'string',
        value: aircraft.icao24,
        description: 'Unique ICAO 24-bit transponder address (hex).',
      },
      {
        index: 1,
        property: 'callsign',
        raw: aircraft.callsignRaw,
        type: 'string',
        value: aircraft.callsignRaw ?? 'null',
        description: 'Vehicle callsign (8 chars), may be null.',
      },
      {
        index: 2,
        property: 'origin_country',
        raw: aircraft.originCountry,
        type: 'string',
        value: aircraft.originCountry,
        description: 'Country inferred from ICAO 24-bit address.',
      },
      {
        index: 3,
        property: 'time_position',
        raw: aircraft.timePosition,
        type: 'int',
        value: formatUnixSeconds(aircraft.timePosition),
        description: 'Unix seconds of last position update.',
      },
      {
        index: 4,
        property: 'last_contact',
        raw: aircraft.lastContact,
        type: 'int',
        value: formatUnixSeconds(aircraft.lastContact),
        description: 'Unix seconds of last valid message.',
      },
      {
        index: 5,
        property: 'longitude',
        raw: aircraft.longitude,
        type: 'float',
        value: formatDetailNumber(aircraft.longitude, 6),
        description: 'WGS-84 longitude (decimal degrees).',
      },
      {
        index: 6,
        property: 'latitude',
        raw: aircraft.latitude,
        type: 'float',
        value: formatDetailNumber(aircraft.latitude, 6),
        description: 'WGS-84 latitude (decimal degrees).',
      },
      {
        index: 7,
        property: 'baro_altitude',
        raw: aircraft.baroAltitude,
        type: 'float',
        value: formatDetailNumber(aircraft.baroAltitude),
        description: 'Barometric altitude in meters.',
      },
      {
        index: 8,
        property: 'on_ground',
        raw: aircraft.onGround,
        type: 'boolean',
        value: formatDetailBoolean(aircraft.onGround),
        description: 'Whether the aircraft is on the ground.',
      },
      {
        index: 9,
        property: 'velocity',
        raw: aircraft.velocityMetersPerSecond,
        type: 'float',
        value: formatDetailNumber(aircraft.velocityMetersPerSecond),
        description: 'Ground speed in m/s.',
      },
      {
        index: 10,
        property: 'true_track',
        raw: aircraft.trueTrack,
        type: 'float',
        value: formatDetailNumber(aircraft.trueTrack),
        description: 'Heading in degrees clockwise from north.',
      },
      {
        index: 11,
        property: 'vertical_rate',
        raw: aircraft.verticalRate,
        type: 'float',
        value: formatDetailNumber(aircraft.verticalRate),
        description: 'Vertical rate in m/s.',
      },
      {
        index: 12,
        property: 'sensors',
        raw: aircraft.sensors.length ? aircraft.sensors : null,
        type: 'int[]',
        value: formatSensorsList(aircraft.sensors),
        description: 'Receiver IDs contributing to this state.',
      },
      {
        index: 13,
        property: 'geo_altitude',
        raw: aircraft.geoAltitude,
        type: 'float',
        value: formatDetailNumber(aircraft.geoAltitude),
        description: 'Geometric altitude in meters.',
      },
      {
        index: 14,
        property: 'squawk',
        raw: aircraft.squawk,
        type: 'string',
        value: aircraft.squawk || 'null',
        description: 'Transponder code (squawk).',
      },
      {
        index: 15,
        property: 'spi',
        raw: aircraft.spi,
        type: 'boolean',
        value: formatDetailBoolean(aircraft.spi),
        description: 'Special purpose indicator from flight status.',
      },
      {
        index: 16,
        property: 'position_source',
        raw: aircraft.positionSource,
        type: 'int',
        value: resolvePositionSourceLabel(aircraft.positionSource),
        description: 'Position source: 0 ADS-B, 1 ASTERIX, 2 MLAT, 3 FLARM.',
      },
      {
        index: 17,
        property: 'category',
        raw: aircraft.category,
        type: 'int',
        value:
          aircraft.category === null
            ? 'null'
            : `${aircraft.category} (${resolveAircraftCategoryLabel(aircraft.category)})`,
        description: 'Aircraft emitter category.',
      },
    ] as (AircraftDetailField & { raw: unknown })[]
  ).filter((field) => field.raw !== null && field.raw !== undefined)


const parseOpenSkySensors = (value: unknown) => {
  if (!Array.isArray(value)) {
    return []
  }

  return value
    .map((sensorId) => toFiniteInteger(sensorId))
    .filter((sensorId): sensorId is number => sensorId !== null)
}

const parseOpenSkyAircraft = (payload: unknown): RotorcraftParseResult => {
  if (!payload || typeof payload !== 'object') {
    return {
      rotorcraft: [],
      feedStats: EMPTY_ROTORCRAFT_FEED_STATS,
    }
  }

  const source = payload as OpenSkyApiResponse

  if (!Array.isArray(source.states)) {
    return {
      rotorcraft: [],
      feedStats: EMPTY_ROTORCRAFT_FEED_STATS,
    }
  }

  const rotorcraftUnits: RotorcraftUnit[] = []
  let statesWithCategory = 0
  let statesWithCoordinates = 0

  source.states.forEach((stateEntry, index) => {
    if (!Array.isArray(stateEntry)) {
      return
    }

    const category = resolveOpenSkyCategory(stateEntry[17])

    if (category !== null) {
      statesWithCategory += 1
    }

    const longitude = toFiniteNumber(stateEntry[5])
    const latitude = toFiniteNumber(stateEntry[6])

    if (longitude === null || latitude === null) {
      return
    }

    statesWithCoordinates += 1

    const icao24 = toTrimmedString(stateEntry[0]).toLowerCase() || 'unknown'

    const callsignRaw = toTrimmedString(stateEntry[1]) || null
    const callsign = callsignRaw ?? 'UNIDENTIFIED'
    const originCountry = toTrimmedString(stateEntry[2]) || 'Unknown'
    const timePosition = toFiniteInteger(stateEntry[3])
    const lastContact = toFiniteInteger(stateEntry[4])
    const baroAltitude = toFiniteNumber(stateEntry[7])
    const onGround = toBooleanValue(stateEntry[8]) ?? false
    const velocityMetersPerSecond = toFiniteNumber(stateEntry[9])
    const pinTheme = resolveAircraftPinTheme(category)
    const trueTrack = toFiniteNumber(stateEntry[10])
    const verticalRate = toFiniteNumber(stateEntry[11])
    const sensors = parseOpenSkySensors(stateEntry[12])
    const geoAltitude = toFiniteNumber(stateEntry[13])
    const squawk = toTrimmedString(stateEntry[14])
    const spi = toBooleanValue(stateEntry[15]) ?? false
    const positionSource = toFiniteInteger(stateEntry[16])
    const speedKnots =
      velocityMetersPerSecond === null
        ? null
        : Math.max(0, velocityMetersPerSecond * KNOTS_PER_METER_PER_SECOND)
    const altitudeMeters = geoAltitude ?? baroAltitude

    rotorcraftUnits.push({
      key: `${icao24}-${lastContact ?? index}-${index}`,
      icao24,
      callsign,
      callsignRaw,
      originCountry,
      timePosition,
      latitude,
      longitude,
      baroAltitude,
      onGround,
      velocityMetersPerSecond,
      verticalRate,
      sensors,
      geoAltitude,
      squawk,
      spi,
      positionSource,
      category,
      categoryLabel: resolveAircraftCategoryLabel(category),
      icon: AIRCRAFT_ICON_BY_THEME[pinTheme],
      pinTheme,
      trueTrack,
      speedKnots,
      altitudeMeters,
      lastContact,
    })
  })

  return {
    rotorcraft: rotorcraftUnits.slice(0, AIRCRAFT_LIMIT),
    feedStats: {
      totalStates: source.states.length,
      statesWithCategory,
      statesWithCoordinates,
      plottedStates: Math.min(rotorcraftUnits.length, AIRCRAFT_LIMIT),
    },
  }
}

const hydrateAircraftUnitFromCacheEntry = (entry: unknown, index: number): RotorcraftUnit | null => {
  if (!entry || typeof entry !== 'object') {
    return null
  }

  const source = entry as Record<string, unknown>
  const latitude = toFiniteNumber(source.latitude)
  const longitude = toFiniteNumber(source.longitude)

  if (latitude === null || longitude === null) {
    return null
  }

  const category = resolveOpenSkyCategory(source.category)
  const pinThemeValue = toTrimmedString(source.pinTheme).toLowerCase()
  const pinTheme = isAircraftPinTheme(pinThemeValue)
    ? pinThemeValue
    : resolveAircraftPinTheme(category)
  const onGroundRaw = source.onGround
  const cachedSpeedKnots = toFiniteNumber(source.speedKnots)
  const cachedVelocityMetersPerSecond = toFiniteNumber(source.velocityMetersPerSecond)
  const resolvedVelocityMetersPerSecond =
    cachedVelocityMetersPerSecond ??
    (cachedSpeedKnots === null ? null : cachedSpeedKnots / KNOTS_PER_METER_PER_SECOND)
  const resolvedSpeedKnots =
    cachedSpeedKnots ??
    (cachedVelocityMetersPerSecond === null
      ? null
      : Math.max(0, cachedVelocityMetersPerSecond * KNOTS_PER_METER_PER_SECOND))
  const cachedBaroAltitude = toFiniteNumber(source.baroAltitude)
  const cachedGeoAltitude = toFiniteNumber(source.geoAltitude)

  return {
    key:
      toTrimmedString(source.key) ||
      `${toTrimmedString(source.icao24).toLowerCase() || 'unknown'}-cache-${index + 1}`,
    icao24: toTrimmedString(source.icao24).toLowerCase() || 'unknown',
    callsign: toTrimmedString(source.callsign) || 'UNIDENTIFIED',
    callsignRaw: toTrimmedString(source.callsignRaw || source.callsign) || null,
    originCountry: toTrimmedString(source.originCountry) || 'Unknown',
    timePosition: toFiniteInteger(source.timePosition),
    latitude,
    longitude,
    baroAltitude: cachedBaroAltitude,
    onGround:
      typeof onGroundRaw === 'boolean' ? onGroundRaw : toBooleanValue(onGroundRaw) ?? false,
    velocityMetersPerSecond: resolvedVelocityMetersPerSecond,
    verticalRate: toFiniteNumber(source.verticalRate),
    sensors: parseOpenSkySensors(source.sensors),
    geoAltitude: cachedGeoAltitude,
    squawk: toTrimmedString(source.squawk),
    spi: toBooleanValue(source.spi) ?? false,
    positionSource: toFiniteInteger(source.positionSource),
    category,
    categoryLabel: resolveAircraftCategoryLabel(category),
    icon: toTrimmedString(source.icon) || AIRCRAFT_ICON_BY_THEME[pinTheme],
    pinTheme,
    trueTrack: toFiniteNumber(source.trueTrack),
    speedKnots: resolvedSpeedKnots,
    altitudeMeters: toFiniteNumber(source.altitudeMeters) ?? cachedGeoAltitude ?? cachedBaroAltitude,
    lastContact: toFiniteInteger(source.lastContact),
  }
}

const hydrateViewportBoundsFromCache = (value: unknown): ViewportBounds | null => {
  if (!value || typeof value !== 'object') {
    return null
  }

  const source = value as Record<string, unknown>
  const lowerLatitude = toFiniteNumber(source.lowerLatitude)
  const lowerLongitude = toFiniteNumber(source.lowerLongitude)
  const upperLatitude = toFiniteNumber(source.upperLatitude)
  const upperLongitude = toFiniteNumber(source.upperLongitude)

  if (
    lowerLatitude === null ||
    lowerLongitude === null ||
    upperLatitude === null ||
    upperLongitude === null
  ) {
    return null
  }

  return {
    lowerLatitude,
    lowerLongitude,
    upperLatitude,
    upperLongitude,
  }
}

const hydrateAircraftFeedStatsFromCache = (value: unknown, plottedStates: number): RotorcraftFeedStats => {
  if (!value || typeof value !== 'object') {
    return {
      totalStates: plottedStates,
      statesWithCategory: plottedStates,
      statesWithCoordinates: plottedStates,
      plottedStates,
    }
  }

  const source = value as Record<string, unknown>
  const parsedPlottedStates = toFiniteNumber(source.plottedStates)
  const parsedCoordinates = toFiniteNumber(source.statesWithCoordinates)
  const parsedCategories = toFiniteNumber(source.statesWithCategory)
  const parsedTotalStates = toFiniteNumber(source.totalStates)
  const normalizedPlottedStates = Math.max(
    plottedStates,
    Math.floor(parsedPlottedStates ?? plottedStates),
  )
  const normalizedStatesWithCoordinates = Math.max(
    normalizedPlottedStates,
    Math.floor(parsedCoordinates ?? normalizedPlottedStates),
  )
  const normalizedTotalStates = Math.max(
    normalizedStatesWithCoordinates,
    Math.floor(parsedTotalStates ?? normalizedStatesWithCoordinates),
  )
  const normalizedStatesWithCategory = Math.min(
    normalizedTotalStates,
    Math.max(0, Math.floor(parsedCategories ?? normalizedPlottedStates)),
  )

  return {
    totalStates: normalizedTotalStates,
    statesWithCategory: normalizedStatesWithCategory,
    statesWithCoordinates: normalizedStatesWithCoordinates,
    plottedStates: normalizedPlottedStates,
  }
}

const parseAircraftCachePayload = (payload: unknown): AircraftCacheSnapshot | null => {
  if (!payload || typeof payload !== 'object') {
    return null
  }

  const source = payload as Record<string, unknown>
  const fetchedAt = toFiniteNumber(source.fetchedAt)
  const bounds = hydrateViewportBoundsFromCache(source.bounds)

  if (fetchedAt === null || !bounds) {
    return null
  }

  const aircraft = Array.isArray(source.aircraft)
    ? source.aircraft
        .map((entry, index) => hydrateAircraftUnitFromCacheEntry(entry, index))
        .filter((entry): entry is RotorcraftUnit => entry !== null)
    : []

  return {
    fetchedAt,
    bounds,
    feedStats: hydrateAircraftFeedStatsFromCache(source.feedStats, aircraft.length),
    aircraft,
  }
}

const serializeAircraftCachePayload = (snapshot: AircraftCacheSnapshot) => ({
  fetchedAt: snapshot.fetchedAt,
  bounds: snapshot.bounds,
  feedStats: snapshot.feedStats,
  aircraft: snapshot.aircraft.map((aircraft) => ({
    key: aircraft.key,
    icao24: aircraft.icao24,
    callsign: aircraft.callsign,
    callsignRaw: aircraft.callsignRaw,
    originCountry: aircraft.originCountry,
    timePosition: aircraft.timePosition,
    latitude: aircraft.latitude,
    longitude: aircraft.longitude,
    baroAltitude: aircraft.baroAltitude,
    velocityMetersPerSecond: aircraft.velocityMetersPerSecond,
    verticalRate: aircraft.verticalRate,
    sensors: aircraft.sensors,
    geoAltitude: aircraft.geoAltitude,
    squawk: aircraft.squawk,
    spi: aircraft.spi,
    positionSource: aircraft.positionSource,
    category: aircraft.category,
    pinTheme: aircraft.pinTheme,
    icon: aircraft.icon,
    onGround: aircraft.onGround,
    trueTrack: aircraft.trueTrack,
    speedKnots: aircraft.speedKnots,
    altitudeMeters: aircraft.altitudeMeters,
    lastContact: aircraft.lastContact,
  })),
})

const calculateViewportOverlapRatio = (firstBounds: ViewportBounds, secondBounds: ViewportBounds) => {
  const first = normalizeViewportBounds(firstBounds)
  const second = normalizeViewportBounds(secondBounds)
  const firstWidth = Math.max(0, first.east - first.west)
  const firstHeight = Math.max(0, first.north - first.south)
  const secondWidth = Math.max(0, second.east - second.west)
  const secondHeight = Math.max(0, second.north - second.south)
  const firstArea = firstWidth * firstHeight
  const secondArea = secondWidth * secondHeight

  if (firstArea <= 0 || secondArea <= 0) {
    return 0
  }

  const overlapSouth = Math.max(first.south, second.south)
  const overlapNorth = Math.min(first.north, second.north)
  const overlapWest = Math.max(first.west, second.west)
  const overlapEast = Math.min(first.east, second.east)

  if (overlapNorth <= overlapSouth || overlapEast <= overlapWest) {
    return 0
  }

  const overlapArea = (overlapNorth - overlapSouth) * (overlapEast - overlapWest)
  return overlapArea / Math.min(firstArea, secondArea)
}

const isAircraftCacheFresh = (cacheSnapshot: AircraftCacheSnapshot) =>
  Date.now() - cacheSnapshot.fetchedAt <= AIRCRAFT_CACHE_TTL_MS

const shouldReuseAircraftCacheSnapshot = (
  cacheSnapshot: AircraftCacheSnapshot,
  viewportBounds: ViewportBounds,
) =>
  isAircraftCacheFresh(cacheSnapshot) &&
  calculateViewportOverlapRatio(cacheSnapshot.bounds, viewportBounds) >=
    AIRCRAFT_CACHE_MIN_VIEWPORT_OVERLAP

const resolveReligionGroup = (religionValue: string, denominationValue: string): ReligionGroup => {
  const normalizedReligion = religionValue.trim().toLowerCase()
  const normalizedDenomination = denominationValue.trim().toLowerCase()

  if (normalizedReligion.includes('christ')) {
    return 'christian'
  }

  if (normalizedReligion.includes('muslim') || normalizedReligion.includes('islam')) {
    return 'muslim'
  }

  if (normalizedReligion.includes('jew') || normalizedReligion.includes('jud')) {
    return 'jewish'
  }

  if (normalizedReligion.includes('buddh')) {
    return 'buddhist'
  }

  if (normalizedReligion.includes('hindu')) {
    return 'hindu'
  }

  if (normalizedReligion.includes('sikh')) {
    return 'sikh'
  }

  if (CHRISTIAN_DENOMINATION_HINTS.some((hint) => normalizedDenomination.includes(hint))) {
    return 'christian'
  }

  return 'other'
}

const buildOverpassClause = (
  key: string,
  value: string,
  south: string,
  west: string,
  north: string,
  east: string,
) => `  node["${key}"="${value}"](${south},${west},${north},${east});
  way["${key}"="${value}"](${south},${west},${north},${east});
  relation["${key}"="${value}"](${south},${west},${north},${east});`

const buildOverpassCongregationQuery = (
  bounds: ViewportBounds,
  enabledLayers: Set<CongregationSubLayerKey>,
) => {
  const south = bounds.lowerLatitude.toFixed(6)
  const west = bounds.lowerLongitude.toFixed(6)
  const north = bounds.upperLatitude.toFixed(6)
  const east = bounds.upperLongitude.toFixed(6)

  const clauses: string[] = []

  if (enabledLayers.has('worship')) {
    clauses.push(buildOverpassClause('amenity', 'place_of_worship', south, west, north, east))
  }

  if (enabledLayers.has('school')) {
    clauses.push(buildOverpassClause('amenity', 'school', south, west, north, east))
  }

  if (enabledLayers.has('hospital')) {
    clauses.push(buildOverpassClause('amenity', 'hospital', south, west, north, east))
  }

  if (enabledLayers.has('stadium')) {
    clauses.push(buildOverpassClause('leisure', 'stadium', south, west, north, east))
  }

  if (enabledLayers.has('arena')) {
    clauses.push(buildOverpassClause('leisure', 'arena', south, west, north, east))
  }

  if (!clauses.length) {
    return ''
  }

  return `[out:xml][timeout:25];
(
${clauses.join('\n')}
);
out tags center;`
}

const normalizeViewportBounds = (bounds: ViewportBounds) => {
  const south = Math.max(-90, Math.min(bounds.lowerLatitude, bounds.upperLatitude))
  const north = Math.min(90, Math.max(bounds.lowerLatitude, bounds.upperLatitude))
  const west = Math.max(-180, Math.min(bounds.lowerLongitude, bounds.upperLongitude))
  const east = Math.min(180, Math.max(bounds.lowerLongitude, bounds.upperLongitude))

  return {
    south,
    north,
    west,
    east,
  }
}

const toRadians = (degrees: number) => (degrees * Math.PI) / 180

const metersPerDegreeLongitude = (latitude: number) =>
  Math.max(1, Math.cos(toRadians(latitude)) * METERS_PER_DEGREE_LATITUDE)

const calculateGroundDistanceMeters = (
  from: [number, number],
  to: [number, number],
) => {
  const latitudeDeltaMeters = (to[1] - from[1]) * METERS_PER_DEGREE_LATITUDE
  const averageLatitude = (from[1] + to[1]) / 2
  const longitudeDeltaMeters = (to[0] - from[0]) * metersPerDegreeLongitude(averageLatitude)

  return Math.hypot(latitudeDeltaMeters, longitudeDeltaMeters)
}

const calculateHeadingDegrees = (from: [number, number], to: [number, number]) => {
  const latitudeDeltaMeters = (to[1] - from[1]) * METERS_PER_DEGREE_LATITUDE
  const averageLatitude = (from[1] + to[1]) / 2
  const longitudeDeltaMeters = (to[0] - from[0]) * metersPerDegreeLongitude(averageLatitude)
  const heading = (Math.atan2(longitudeDeltaMeters, latitudeDeltaMeters) * 180) / Math.PI

  return (heading + 360) % 360
}

const interpolateRouteCoordinate = (
  from: [number, number],
  to: [number, number],
  fraction: number,
): [number, number] => {
  const clampedFraction = Math.min(1, Math.max(0, fraction))

  return [
    from[0] + (to[0] - from[0]) * clampedFraction,
    from[1] + (to[1] - from[1]) * clampedFraction,
  ]
}

const normalizeLoopedDistance = (distanceMeters: number, totalLengthMeters: number) => {
  if (totalLengthMeters <= 0) {
    return 0
  }

  const wrappedDistance = distanceMeters % totalLengthMeters
  return wrappedDistance >= 0 ? wrappedDistance : wrappedDistance + totalLengthMeters
}

const clampNumber = (value: number, minimum: number, maximum: number) =>
  Math.min(maximum, Math.max(minimum, value))

const clampUnitInterval = (value: number) => clampNumber(value, 0, 1)

const resolveRiskHeatmapLevel = (riskScore: number): RiskHeatmapLevel => {
  if (riskScore >= 0.67) {
    return 'high'
  }

  if (riskScore <= 0.34) {
    return 'low'
  }

  return 'medium'
}

const buildRiskHeatmapTrafficSampleKey = (latitude: number, longitude: number) => {
  const quantizedLatitude =
    Math.round(latitude / RISK_HEATMAP_TRAFFIC_QUANTIZATION_DEGREES) *
    RISK_HEATMAP_TRAFFIC_QUANTIZATION_DEGREES
  const quantizedLongitude =
    Math.round(longitude / RISK_HEATMAP_TRAFFIC_QUANTIZATION_DEGREES) *
    RISK_HEATMAP_TRAFFIC_QUANTIZATION_DEGREES

  return `${quantizedLatitude.toFixed(4)}:${quantizedLongitude.toFixed(4)}`
}

const buildTomTomFlowSegmentUrl = (latitude: number, longitude: number) => {
  const params = new URLSearchParams({
    point: `${latitude.toFixed(6)},${longitude.toFixed(6)}`,
    unit: 'KMPH',
    key: TOMTOM_TRAFFIC_API_KEY,
  })

  return `${TOMTOM_TRAFFIC_SEGMENT_BASE}?${params.toString()}`
}

const trimTrafficSampleCache = (cache: Map<string, TrafficSampleCacheEntry>) => {
  if (cache.size <= RISK_HEATMAP_TRAFFIC_CACHE_LIMIT) {
    return
  }

  const sortedEntries = Array.from(cache.entries()).sort(
    (firstEntry, secondEntry) => firstEntry[1].fetchedAt - secondEntry[1].fetchedAt,
  )
  const removeCount = Math.max(1, cache.size - RISK_HEATMAP_TRAFFIC_CACHE_LIMIT)

  for (let index = 0; index < removeCount; index += 1) {
    cache.delete(sortedEntries[index][0])
  }
}

const buildRiskGridCells = (bounds: ViewportBounds): RiskGridCell[] => {
  const normalizedBounds = normalizeViewportBounds(bounds)
  const averageLatitude = (normalizedBounds.south + normalizedBounds.north) / 2
  const widthMeters =
    Math.max(0.0001, normalizedBounds.east - normalizedBounds.west) *
    metersPerDegreeLongitude(averageLatitude)
  const heightMeters =
    Math.max(0.0001, normalizedBounds.north - normalizedBounds.south) * METERS_PER_DEGREE_LATITUDE
  const columns = clampNumber(
    Math.round(widthMeters / RISK_HEATMAP_TARGET_CELL_SIZE_METERS),
    RISK_HEATMAP_MIN_GRID_COLUMNS,
    RISK_HEATMAP_MAX_GRID_COLUMNS,
  )
  const rows = clampNumber(
    Math.round(heightMeters / RISK_HEATMAP_TARGET_CELL_SIZE_METERS),
    RISK_HEATMAP_MIN_GRID_ROWS,
    RISK_HEATMAP_MAX_GRID_ROWS,
  )
  const longitudeStep = (normalizedBounds.east - normalizedBounds.west) / columns
  const latitudeStep = (normalizedBounds.north - normalizedBounds.south) / rows
  const cells: RiskGridCell[] = []

  for (let rowIndex = 0; rowIndex < rows; rowIndex += 1) {
    for (let columnIndex = 0; columnIndex < columns; columnIndex += 1) {
      const south = normalizedBounds.south + latitudeStep * rowIndex
      const north =
        rowIndex === rows - 1
          ? normalizedBounds.north
          : normalizedBounds.south + latitudeStep * (rowIndex + 1)
      const west = normalizedBounds.west + longitudeStep * columnIndex
      const east =
        columnIndex === columns - 1
          ? normalizedBounds.east
          : normalizedBounds.west + longitudeStep * (columnIndex + 1)

      cells.push({
        key: `${rowIndex}-${columnIndex}`,
        west,
        east,
        south,
        north,
        centerLatitude: (south + north) / 2,
        centerLongitude: (west + east) / 2,
      })
    }
  }

  return cells
}

const calculateProximitySignal = (
  center: [number, number],
  nearbyPoints: [number, number][],
  radiusMeters: number,
  saturationThreshold: number,
) => {
  if (!nearbyPoints.length || saturationThreshold <= 0) {
    return 0
  }

  let weightedPresence = 0

  for (const point of nearbyPoints) {
    const distance = calculateGroundDistanceMeters(center, point)

    if (distance > radiusMeters) {
      continue
    }

    const distanceFactor = 1 - distance / radiusMeters
    weightedPresence += distanceFactor * distanceFactor
  }

  return clampUnitInterval(weightedPresence / saturationThreshold)
}

const estimateFallbackTrafficCongestion = (
  congregationSignal: number,
  levelOneIncidentSignal: number,
  levelZeroIncidentSignal: number,
) =>
  clampUnitInterval(
    congregationSignal * 0.42 +
      levelOneIncidentSignal * 0.43 +
      levelZeroIncidentSignal * 0.15,
  )

const buildEmergencyRouteFromPoints = (
  routeId: string,
  routeLabel: string,
  points: [number, number][],
  closeLoop: boolean,
): EmergencyVehicleRoute | null => {
  const routePoints = points.map(([longitude, latitude]) => [
    Number(longitude.toFixed(7)),
    Number(latitude.toFixed(7)),
  ]) as [number, number][]

  if (closeLoop && routePoints.length >= 2) {
    const firstPoint = routePoints[0]
    const lastPoint = routePoints[routePoints.length - 1]

    if (firstPoint[0] !== lastPoint[0] || firstPoint[1] !== lastPoint[1]) {
      routePoints.push([firstPoint[0], firstPoint[1]])
    }
  }

  if (routePoints.length < 2) {
    return null
  }

  const segmentLengths: number[] = []
  let totalLength = 0

  for (let segmentIndex = 0; segmentIndex < routePoints.length - 1; segmentIndex += 1) {
    const start = routePoints[segmentIndex]
    const end = routePoints[segmentIndex + 1]
    const segmentLength = calculateGroundDistanceMeters(start, end)

    segmentLengths.push(segmentLength)
    totalLength += segmentLength
  }

  if (totalLength <= 0) {
    return null
  }

  return {
    id: routeId,
    label: routeLabel,
    points: routePoints,
    segmentLengths,
    totalLength,
  }
}

const buildEmergencyRoutes = (
  center: [number, number],
  routeBlueprints: EmergencyVehicleRouteBlueprint[] = EMERGENCY_ROUTE_BLUEPRINTS,
  pointsAreOffsets = true,
): EmergencyVehicleRoute[] => {
  const routes = routeBlueprints.map((blueprint): EmergencyVehicleRoute | null => {
    const absolutePoints = blueprint.points.map(([longitudeInput, latitudeInput]) => {
      const longitude = pointsAreOffsets ? center[0] + longitudeInput : longitudeInput
      const latitude = pointsAreOffsets ? center[1] + latitudeInput : latitudeInput

      return [longitude, latitude] as [number, number]
    })

    return buildEmergencyRouteFromPoints(blueprint.id, blueprint.label, absolutePoints, true)
  }).filter((route): route is EmergencyVehicleRoute => route !== null)

  if (routes.length) {
    return routes
  }

  const fallbackLoop = buildEmergencyRouteFromPoints(
    'fallback-loop',
    'Fallback Patrol Loop',
    [
      [center[0] - 0.008, center[1] + 0.008],
      [center[0] + 0.008, center[1] + 0.008],
      [center[0] + 0.008, center[1] - 0.008],
      [center[0] - 0.008, center[1] - 0.008],
    ],
    true,
  )

  return fallbackLoop ? [fallbackLoop] : []
}

const sampleEmergencyRoutePosition = (
  route: EmergencyVehicleRoute,
  distanceMeters: number,
  isLoopRoute: boolean,
) => {
  if (!route.points.length) {
    return {
      longitude: DEFAULT_CENTER[0],
      latitude: DEFAULT_CENTER[1],
      headingDegrees: 0,
    }
  }

  const boundedDistance = isLoopRoute
    ? normalizeLoopedDistance(distanceMeters, route.totalLength)
    : Math.min(Math.max(0, distanceMeters), route.totalLength)
  let remainingDistance = boundedDistance

  for (let segmentIndex = 0; segmentIndex < route.segmentLengths.length; segmentIndex += 1) {
    const segmentLength = route.segmentLengths[segmentIndex]

    if (remainingDistance <= segmentLength || segmentIndex === route.segmentLengths.length - 1) {
      const segmentStart = route.points[segmentIndex]
      const segmentEnd = route.points[segmentIndex + 1]
      const segmentProgress = segmentLength <= 0 ? 0 : remainingDistance / segmentLength
      const [longitude, latitude] = interpolateRouteCoordinate(
        segmentStart,
        segmentEnd,
        segmentProgress,
      )

      return {
        longitude,
        latitude,
        headingDegrees: calculateHeadingDegrees(segmentStart, segmentEnd),
      }
    }

    remainingDistance -= segmentLength
  }

  const fallbackPoint = route.points[0]

  return {
    longitude: fallbackPoint[0],
    latitude: fallbackPoint[1],
    headingDegrees: 0,
  }
}

const resolveEmergencyRuntimeRouteContext = (
  runtime: EmergencyVehicleRuntime,
  routes: EmergencyVehicleRoute[],
) => {
  if (runtime.dispatchRoute) {
    return {
      route: runtime.dispatchRoute,
      distanceMeters: runtime.dispatchDistanceMeters,
      isLoopRoute: false,
    }
  }

  const patrolRoute = routes[runtime.routeIndex] ?? routes[0]

  if (!patrolRoute) {
    return null
  }

  return {
    route: patrolRoute,
    distanceMeters: runtime.distanceAlongRouteMeters,
    isLoopRoute: true,
  }
}

const getEmergencyRuntimeCoordinates = (
  runtime: EmergencyVehicleRuntime,
  routes: EmergencyVehicleRoute[],
) => {
  const routeContext = resolveEmergencyRuntimeRouteContext(runtime, routes)

  if (!routeContext) {
    return {
      longitude: DEFAULT_CENTER[0],
      latitude: DEFAULT_CENTER[1],
    }
  }

  const sampledPosition = sampleEmergencyRoutePosition(
    routeContext.route,
    routeContext.distanceMeters,
    routeContext.isLoopRoute,
  )

  return {
    longitude: sampledPosition.longitude,
    latitude: sampledPosition.latitude,
  }
}

const resolveEmergencyStatusFromPulse = (_pulse: number): EmergencyVehicleStatus => {
  return 'patrolling'
}

const metersPerSecondToMph = (metersPerSecond: number) => (metersPerSecond * 3600) / METERS_PER_MILE

const buildEmergencyTelemetry = (
  vehicleType: EmergencyVehicleType,
  status: EmergencyVehicleStatus,
  routeLabel: string,
  assignment: string,
  speedMph: number,
  headingDegrees: number,
  etaMinutes: number,
) => {
  const vehicleLabel = EMERGENCY_VEHICLE_LABEL_BY_TYPE[vehicleType]
  const statusLabel = EMERGENCY_STATUS_LABEL_BY_CODE[status].toUpperCase()

  return [
    `${vehicleLabel} ${statusLabel} on ${routeLabel}. ${assignment}.`,
    `Position lock stable. Speed ${Math.round(speedMph)} MPH, heading ${Math.round(headingDegrees)}�.`,
    `Dispatch queue synchronized. Next status checkpoint in ${etaMinutes} min.`,
  ]
}

const createEmergencyVehicleRuntimes = (
  routes: EmergencyVehicleRoute[],
  fleetSnapshot: EmergencyFleetSnapshot,
): EmergencyVehicleRuntime[] => {
  if (!routes.length || !fleetSnapshot.units.length) {
    return []
  }

  const runtimes: EmergencyVehicleRuntime[] = []
  const speedMetersPerSecond = fleetSnapshot.targetSpeedMph / 2.23693629
  const unitsPerRoute = Math.max(1, Math.ceil(fleetSnapshot.units.length / routes.length))

  fleetSnapshot.units.forEach((fleetUnit, unitIndex) => {
    const routeIndex = unitIndex % routes.length
    const route = routes[routeIndex]
    const routeSlot = Math.floor(unitIndex / routes.length)
    const startDistance = route.totalLength * ((routeSlot + 1) / (unitsPerRoute + 1))
    const assignmentOptions = EMERGENCY_ASSIGNMENTS_BY_TYPE[fleetUnit.vehicleType]
    const assignment = assignmentOptions[unitIndex % assignmentOptions.length]
    const district = EMERGENCY_DISTRICT_LABELS[unitIndex % EMERGENCY_DISTRICT_LABELS.length]
    const crew = EMERGENCY_CREW_CODES[unitIndex % EMERGENCY_CREW_CODES.length]
    const normalizedUnitId = fleetUnit.id
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, '-')
      .replace(/(^-|-$)/g, '')

    runtimes.push({
      key: `emergency-${normalizedUnitId || `${fleetUnit.vehicleType}-${unitIndex + 1}`}`,
      unitCode: fleetUnit.id,
      vehicleType: fleetUnit.vehicleType,
      routeIndex,
      distanceAlongRouteMeters: startDistance,
      baseSpeedMetersPerSecond: speedMetersPerSecond,
      speedPhaseSeed: (unitIndex + 1) * 0.71,
      statusPhaseSeed: (unitIndex + 1) * 0.53,
      fuelPhaseSeed: (unitIndex + 1) * 0.37,
      assignment,
      district,
      crew,
      dispatchRoute: null,
      dispatchDistanceMeters: 0,
      dispatchTargetLabel: '',
      dispatchActionId: '',
      isStationedAtDispatchTarget: false,
    })
  })

  return runtimes
}

const buildEmergencyVehicleSnapshot = (
  runtime: EmergencyVehicleRuntime,
  routes: EmergencyVehicleRoute[],
  timestamp: number,
): EmergencyVehicleUnit => {
  const routeContext = resolveEmergencyRuntimeRouteContext(runtime, routes)
  const statusPulse = Math.sin(timestamp / 10_400 + runtime.statusPhaseSeed)
  const sampledPosition = routeContext
    ? sampleEmergencyRoutePosition(
        routeContext.route,
        routeContext.distanceMeters,
        routeContext.isLoopRoute,
      )
    : {
        longitude: DEFAULT_CENTER[0],
        latitude: DEFAULT_CENTER[1],
        headingDegrees: 0,
      }
  const isDispatchRouteActive = runtime.dispatchRoute !== null
  const originalStatus = isDispatchRouteActive
    ? runtime.isStationedAtDispatchTarget
      ? 'staged'
      : 'responding'
    : resolveEmergencyStatusFromPulse(statusPulse)
  const status = runtime.overrideStatus ?? originalStatus
  
  const finalPosition = runtime.overrideLocation 
    ? { longitude: runtime.overrideLocation.longitude, latitude: runtime.overrideLocation.latitude, headingDegrees: runtime.overrideLocation.headingDegrees }
    : sampledPosition
    
  const speedMetersPerSecond =
    (isDispatchRouteActive && runtime.isStationedAtDispatchTarget) || status === 'staged'
      ? 0
      : status === 'responding'
      ? (60 * 1609.344) / 3600
      : runtime.baseSpeedMetersPerSecond
  const speedMph = metersPerSecondToMph(speedMetersPerSecond)
  const etaPulse = (Math.sin(timestamp / 12_400 + runtime.speedPhaseSeed) + 1) / 2
  const etaMinutes =
    isDispatchRouteActive && runtime.isStationedAtDispatchTarget
      ? 0
      : status === 'responding'
      ? Math.max(2, Math.round(2 + etaPulse * 3))
      : status === 'staged'
        ? Math.max(7, Math.round(7 + etaPulse * 4))
        : Math.max(4, Math.round(4 + etaPulse * 5))
  const fuelPulse = (Math.sin(timestamp / 21_000 + runtime.fuelPhaseSeed) + 1) / 2
  const fuelLevelPercent = Math.round(35 + fuelPulse * 60)
  const routeLabel = runtime.dispatchRoute
    ? runtime.isStationedAtDispatchTarget
      ? `Stationed � ${runtime.dispatchTargetLabel}`
      : `Dispatch � ${runtime.dispatchTargetLabel}`
    : (routeContext?.route.label ?? 'Patrol Route')

  return {
    key: runtime.key,
    unitCode: runtime.unitCode,
    vehicleType: runtime.vehicleType,
    vehicleLabel: EMERGENCY_VEHICLE_LABEL_BY_TYPE[runtime.vehicleType],
    icon: EMERGENCY_VEHICLE_ICON_BY_TYPE[runtime.vehicleType],
    status,
    latitude: finalPosition.latitude,
    longitude: finalPosition.longitude,
    headingDegrees: finalPosition.headingDegrees,
    speedMph,
    assignment: runtime.assignment,
    district: runtime.district,
    routeLabel,
    crew: runtime.crew,
    etaMinutes,
    fuelLevelPercent,
    lastUpdate: timestamp,
    dispatchRoutePoints: isDispatchRouteActive && status === 'responding' ? runtime.dispatchRoute?.points : undefined,
    telemetry: buildEmergencyTelemetry(
      runtime.vehicleType,
      status,
      routeLabel,
      runtime.assignment,
      speedMph,
      finalPosition.headingDegrees,
      etaMinutes,
    ),
  }
}

const advanceEmergencyVehicleSimulation = (
  runtimes: EmergencyVehicleRuntime[],
  routes: EmergencyVehicleRoute[],
  elapsedSeconds: number,
  timestamp: number,
) => {
  if (!runtimes.length || !routes.length) {
    return []
  }

  return runtimes.map((runtime) => {
    const isResponding = runtime.dispatchRoute && !runtime.isStationedAtDispatchTarget
    const currentSpeed = isResponding ? (60 * 1609.344) / 3600 : runtime.baseSpeedMetersPerSecond
    const movementMeters = currentSpeed * elapsedSeconds

    if (runtime.dispatchRoute) {
      if (!runtime.isStationedAtDispatchTarget) {
        runtime.dispatchDistanceMeters = Math.min(
          runtime.dispatchDistanceMeters + movementMeters,
          runtime.dispatchRoute.totalLength,
        )

        if (runtime.dispatchDistanceMeters >= runtime.dispatchRoute.totalLength - 2) {
          runtime.dispatchDistanceMeters = runtime.dispatchRoute.totalLength
          runtime.isStationedAtDispatchTarget = true
        }
      }
    } else {
      const patrolRoute = routes[runtime.routeIndex] ?? routes[0]

      if (patrolRoute) {
        runtime.distanceAlongRouteMeters = normalizeLoopedDistance(
          runtime.distanceAlongRouteMeters + movementMeters,
          patrolRoute.totalLength,
        )
      }
    }

    return buildEmergencyVehicleSnapshot(runtime, routes, timestamp)
  })
}

const dedupeCongregationPlaces = (places: CongregationPlace[]) => {
  const deduplicatedPlaces: CongregationPlace[] = []
  const seen = new Set<string>()

  for (const place of places) {
    const dedupeKey = `${place.category}|${place.latitude.toFixed(5)}|${place.longitude.toFixed(5)}|${place.name.toLowerCase()}`

    if (seen.has(dedupeKey)) {
      continue
    }

    seen.add(dedupeKey)
    deduplicatedPlaces.push(place)
  }

  return deduplicatedPlaces
}

const toTrimmedString = (value: unknown) => (typeof value === 'string' ? value.trim() : '')

const parseEmergencyFleetPayload = (payload: unknown): EmergencyFleetSnapshot | null => {
  if (!payload || typeof payload !== 'object') {
    return null
  }

  const source = payload as Record<string, unknown>
  const routePayload = Array.isArray(source.routes) ? source.routes : []
  const routes: EmergencyVehicleRouteBlueprint[] = []

  routePayload.forEach((routeEntry, routeIndex) => {
    if (!routeEntry || typeof routeEntry !== 'object') {
      return
    }

    const routeSource = routeEntry as Record<string, unknown>
    const routePointsPayload = Array.isArray(routeSource.points) ? routeSource.points : []
    const routePoints: [number, number][] = []

    routePointsPayload.forEach((pointCandidate) => {
      if (!Array.isArray(pointCandidate) || pointCandidate.length < 2) {
        return
      }

      const longitude = toFiniteNumber(pointCandidate[0])
      const latitude = toFiniteNumber(pointCandidate[1])

      if (longitude === null || latitude === null) {
        return
      }

      routePoints.push([Number(longitude.toFixed(7)), Number(latitude.toFixed(7))])
    })

    if (routePoints.length < 2) {
      return
    }

    const firstPoint = routePoints[0]
    const lastPoint = routePoints[routePoints.length - 1]

    if (firstPoint[0] !== lastPoint[0] || firstPoint[1] !== lastPoint[1]) {
      routePoints.push([firstPoint[0], firstPoint[1]])
    }

    routes.push({
      id: toTrimmedString(routeSource.id) || `tempe-route-${routeIndex + 1}`,
      label: toTrimmedString(routeSource.label) || `Tempe Route ${routeIndex + 1}`,
      points: routePoints,
    })
  })

  if (!routes.length) {
    return null
  }

  const fleetSource = source.fleet && typeof source.fleet === 'object'
    ? (source.fleet as Record<string, unknown>)
    : null

  if (!fleetSource) {
    return null
  }

  const units: EmergencyFleetUnitSeed[] = []

  for (const vehicleType of EMERGENCY_VEHICLE_TYPES) {
    const typeIds = Array.isArray(fleetSource[vehicleType]) ? fleetSource[vehicleType] : []

    if (!Array.isArray(typeIds)) {
      return null
    }

    const dedupedIds: string[] = []
    const seenIds = new Set<string>()

    typeIds.forEach((idCandidate) => {
      const unitId = toTrimmedString(idCandidate)

      if (!unitId) {
        return
      }

      const normalizedKey = unitId.toLowerCase()

      if (seenIds.has(normalizedKey)) {
        return
      }

      seenIds.add(normalizedKey)
      dedupedIds.push(unitId)
    })

    if (dedupedIds.length !== EMERGENCY_UNIT_COUNTS[vehicleType]) {
      return null
    }

    dedupedIds.forEach((unitId) => {
      units.push({
        id: unitId,
        vehicleType,
      })
    })
  }

  const targetSpeedCandidate = toFiniteNumber(source.targetSpeedMph ?? source.target_speed_mph)
  const targetSpeedMph =
    targetSpeedCandidate === null
      ? EMERGENCY_PATROL_SPEED_MPH
      : Math.max(25, Math.min(60, targetSpeedCandidate))

  return {
    targetSpeedMph,
    routes,
    units,
  }
}

const extractIframeSource = (value: string) => {
  const iframeSourceMatch = value.match(/<iframe[^>]+src=["']([^"']+)["']/i)

  if (iframeSourceMatch?.[1]) {
    return iframeSourceMatch[1].trim()
  }

  return value
}

const normalizeYouTubeEmbedUrl = (value: string) => {
  try {
    const parsedUrl = new URL(value)
    const hostname = parsedUrl.hostname.toLowerCase()

    if (hostname.includes('youtu.be')) {
      const videoId = parsedUrl.pathname.split('/').filter(Boolean)[0]
      return videoId ? `https://www.youtube.com/embed/${videoId}` : null
    }

    if (!hostname.includes('youtube.com') && !hostname.includes('youtube-nocookie.com')) {
      return null
    }

    if (parsedUrl.pathname.startsWith('/embed/')) {
      return `${parsedUrl.origin}${parsedUrl.pathname}${parsedUrl.search}`
    }

    const watchId = parsedUrl.searchParams.get('v')

    if (watchId) {
      return `https://www.youtube.com/embed/${watchId}`
    }

    const pathSegments = parsedUrl.pathname.split('/').filter(Boolean)

    if (pathSegments.length >= 2 && (pathSegments[0] === 'shorts' || pathSegments[0] === 'live')) {
      return `https://www.youtube.com/embed/${pathSegments[1]}`
    }
  } catch {
    // Ignore malformed URLs and continue with regex fallback.
  }

  const embedMatch = value.match(/youtube\.com\/embed\/([\w-]+)/i)

  if (embedMatch?.[1]) {
    return `https://www.youtube.com/embed/${embedMatch[1]}`
  }

  return null
}

const resolveCctvStream = (streamLink: string) => {
  const source = extractIframeSource(streamLink.trim())

  if (!source) {
    return null
  }

  const youtubeEmbedUrl = normalizeYouTubeEmbedUrl(source)

  if (youtubeEmbedUrl) {
    return {
      streamType: 'youtube' as const,
      streamUrl: youtubeEmbedUrl,
      streamLabel: 'YouTube live feed',
    }
  }

  if (/\.m3u8($|\?)/i.test(source)) {
    return {
      streamType: 'hls' as const,
      streamUrl: source,
      streamLabel: 'M3U8 live stream',
    }
  }

  if (/^https?:\/\//i.test(source)) {
    return {
      streamType: 'iframe' as const,
      streamUrl: source,
      streamLabel: 'Embedded camera feed',
    }
  }

  return null
}

const parseCctvCamerasPayload = (payload: unknown) => {
  if (!Array.isArray(payload)) {
    return []
  }

  const seenKeys = new Set<string>()
  const cameras: CctvCamera[] = []

  payload.forEach((entry, index) => {
    if (!entry || typeof entry !== 'object') {
      return
    }

    const source = entry as Record<string, unknown>
    const coordinates = Array.isArray(source.Coordinates) ? source.Coordinates : []
    const latitude = toFiniteNumber(coordinates[0])
    const longitude = toFiniteNumber(coordinates[1])

    if (latitude === null || longitude === null) {
      return
    }

    const stream = resolveCctvStream(toTrimmedString(source.StreamLink))

    if (!stream) {
      return
    }

    const name = toTrimmedString(source.Name) || `Camera ${index + 1}`
    const dedupeKey = `${name.toLowerCase()}|${latitude.toFixed(6)}|${longitude.toFixed(6)}`

    if (seenKeys.has(dedupeKey)) {
      return
    }

    seenKeys.add(dedupeKey)

    cameras.push({
      key: `cctv-${index + 1}-${Math.round(latitude * 10_000)}-${Math.round(longitude * 10_000)}`,
      name,
      latitude,
      longitude,
      streamType: stream.streamType,
      streamUrl: stream.streamUrl,
      streamLabel: stream.streamLabel,
    })
  })

  return cameras
}

const isReligionGroup = (value: string): value is ReligionGroup =>
  RELIGION_GROUPS.includes(value as ReligionGroup)

const isCongregationSubLayerKey = (value: string): value is CongregationSubLayerKey =>
  CONGREGATION_SUBLAYER_KEYS.includes(value as CongregationSubLayerKey)

const buildEmptyCongregationTileCoverage = (): Record<string, Set<string>> => {
  const coverage: Record<string, Set<string>> = {}

  for (const layerKey of CONGREGATION_SUBLAYER_KEYS) {
    coverage[layerKey] = new Set<string>()
  }

  return coverage
}

const ensureCongregationCategoryCoverage = (
  coverage: Record<string, Set<string>>,
  category: string,
) => {
  if (!coverage[category]) {
    coverage[category] = new Set<string>()
  }

  return coverage[category]
}

const normalizeCongregationCategory = (value: unknown) =>
  toTrimmedString(value).toLowerCase() || 'worship'

const resolveCongregationCategoryLabel = (category: string) => {
  if (isCongregationSubLayerKey(category)) {
    return CONGREGATION_SUBLAYER_LABELS[category]
  }

  const normalizedCategory = category.trim()

  if (!normalizedCategory) {
    return 'Congregation'
  }

  return normalizedCategory
    .replace(/[\-_]+/g, ' ')
    .replace(/\s+/g, ' ')
    .replace(/\b\w/g, (character) => character.toUpperCase())
}

const resolveCongregationCategoryIcon = (category: string) =>
  isCongregationSubLayerKey(category) ? CONGREGATION_ICON_BY_CATEGORY[category] : '??'

const resolveCongregationCategoryTheme = (category: string): CongregationPinTheme => {
  if (category === 'school' || category === 'stadium' || category === 'arena') {
    return category
  }

  return 'other'
}

const buildCongregationTileKey = (latitude: number, longitude: number) => {
  const latTile = Math.floor(latitude / CONGREGATION_CACHE_TILE_SIZE)
  const lonTile = Math.floor(longitude / CONGREGATION_CACHE_TILE_SIZE)

  return `${latTile}:${lonTile}`
}

const normalizeOsmElementType = (value: unknown): 'node' | 'way' | 'relation' => {
  const normalizedType = toTrimmedString(value).toLowerCase()

  if (normalizedType === 'way' || normalizedType === 'relation') {
    return normalizedType
  }

  return 'node'
}

const normalizeCongregationTags = (value: unknown) => {
  if (!value || typeof value !== 'object') {
    return {}
  }

  const tagsSource = value as Record<string, unknown>
  const normalizedTags: Record<string, string> = {}

  Object.entries(tagsSource).forEach(([key, tagValue]) => {
    const normalizedKey = toTrimmedString(key)
    const normalizedValue = toTrimmedString(tagValue)

    if (normalizedKey && normalizedValue) {
      normalizedTags[normalizedKey] = normalizedValue
    }
  })

  return normalizedTags
}

const hydrateCongregationPlaceFromCacheEntry = (
  entry: unknown,
  index: number,
): CongregationPlace | null => {
  if (!entry || typeof entry !== 'object') {
    return null
  }

  const source = entry as Record<string, unknown>
  const latitude = Number(source.latitude)
  const longitude = Number(source.longitude)

  if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) {
    return null
  }

  const category = normalizeCongregationCategory(source.category)
  const categoryLabel = resolveCongregationCategoryLabel(category)
  const osmType = normalizeOsmElementType(source.osmType)
  const osmId = toTrimmedString(source.osmId) || `${index + 1}`
  const tags = normalizeCongregationTags(source.tags)

  if (category !== 'worship') {
    const categoryKeyPrefix = category.replace(/[^a-z0-9_-]/g, '-') || 'congregation'

    return {
      key: toTrimmedString(source.key) || `${categoryKeyPrefix}-cache-${index + 1}`,
      latitude,
      longitude,
      name:
        toTrimmedString(source.name) ||
        (isCongregationSubLayerKey(category)
          ? category === 'school'
            ? 'Unnamed School'
            : category === 'stadium'
              ? 'Unnamed Stadium'
              : category === 'arena'
                ? 'Unnamed Arena'
                : 'Unnamed Congregation Area'
          : `Unnamed ${categoryLabel}`),
      osmType,
      osmId,
      category,
      categoryLabel,
      pinTheme: resolveCongregationCategoryTheme(category),
      religionRaw: '',
      denominationRaw: '',
      religionLabel: '',
      icon: resolveCongregationCategoryIcon(category),
      tags,
    }
  }

  const religionRaw = toTrimmedString(source.religionRaw)
  const denominationRaw = toTrimmedString(source.denominationRaw)
  const normalizedReligionGroup = toTrimmedString(source.religionGroup).toLowerCase()
  const religionGroup = isReligionGroup(normalizedReligionGroup)
    ? normalizedReligionGroup
    : resolveReligionGroup(religionRaw, denominationRaw)

  return {
    key: toTrimmedString(source.key) || `congregation-worship-cache-${index + 1}`,
    latitude,
    longitude,
    name: toTrimmedString(source.name) || 'Unnamed Place of Worship',
    osmType,
    osmId,
    category,
    categoryLabel,
    pinTheme: religionGroup,
    religionRaw: religionRaw || RELIGION_LABEL_BY_GROUP[religionGroup],
    denominationRaw,
    religionLabel: RELIGION_LABEL_BY_GROUP[religionGroup],
    icon: RELIGION_ICON_BY_GROUP[religionGroup],
    tags,
  }
}

const parseCongregationCachePayload = (payload: unknown): CongregationCacheSnapshot => {
  const coveredTilesByCategory = buildEmptyCongregationTileCoverage()

  if (!payload || typeof payload !== 'object') {
    return {
      coveredTilesByCategory,
      places: [],
    }
  }

  const source = payload as Record<string, unknown>
  const appendCoverageTiles = (categoryKey: string, layerTiles: unknown) => {
    if (!Array.isArray(layerTiles)) {
      return
    }

    const categoryCoverage = ensureCongregationCategoryCoverage(
      coveredTilesByCategory,
      normalizeCongregationCategory(categoryKey),
    )

    for (const tileCandidate of layerTiles) {
      const tileKey = toTrimmedString(tileCandidate)

      if (tileKey) {
        categoryCoverage.add(tileKey)
      }
    }
  }

  const coveredTilesByCategoryPayload = source.coveredTilesByCategory

  if (coveredTilesByCategoryPayload && typeof coveredTilesByCategoryPayload === 'object') {
    const categorySource = coveredTilesByCategoryPayload as Record<string, unknown>

    for (const [categoryKey, categoryTiles] of Object.entries(categorySource)) {
      appendCoverageTiles(categoryKey, categoryTiles)
    }
  }

  // Backward compatibility for previous payload key.
  const coveredTilesByLayerPayload = source.coveredTilesByLayer

  if (coveredTilesByLayerPayload && typeof coveredTilesByLayerPayload === 'object') {
    const layerSource = coveredTilesByLayerPayload as Record<string, unknown>

    for (const [layerKey, layerTiles] of Object.entries(layerSource)) {
      appendCoverageTiles(layerKey, layerTiles)
    }
  }

  // Backward compatibility for older single-layer cache shape.
  if (Array.isArray(source.coveredTiles)) {
    for (const tileCandidate of source.coveredTiles) {
      const tileKey = toTrimmedString(tileCandidate)

      if (tileKey) {
        ensureCongregationCategoryCoverage(coveredTilesByCategory, 'worship').add(tileKey)
      }
    }
  }

  const places = Array.isArray(source.places)
    ? source.places
        .map((entry, index) => hydrateCongregationPlaceFromCacheEntry(entry, index))
        .filter((place): place is CongregationPlace => Boolean(place))
    : []

  for (const place of places) {
    ensureCongregationCategoryCoverage(coveredTilesByCategory, place.category).add(
      buildCongregationTileKey(place.latitude, place.longitude),
    )
  }

  return {
    coveredTilesByCategory,
    places: dedupeCongregationPlaces(places),
  }
}

const serializeCongregationCachePayload = (
  coveredTilesByCategory: Record<string, Set<string>>,
  places: CongregationPlace[],
) => ({
  coveredTilesByCategory: Object.fromEntries(
    Object.entries(coveredTilesByCategory).map(([categoryKey, tileSet]) => [
      categoryKey,
      Array.from(tileSet.values()),
    ]),
  ),
  places: places.map((place) => ({
    key: place.key,
    latitude: place.latitude,
    longitude: place.longitude,
    name: place.name,
    osmType: place.osmType,
    osmId: place.osmId,
    category: place.category,
    religionRaw: place.religionRaw,
    denominationRaw: place.denominationRaw,
    religionGroup: place.pinTheme,
    tags: place.tags,
  })),
})

const getCongregationPlacesInBounds = (
  places: CongregationPlace[],
  bounds: ViewportBounds,
  enabledLayers: Set<CongregationSubLayerKey>,
) => {
  const normalizedBounds = normalizeViewportBounds(bounds)

  return dedupeCongregationPlaces(
    places.filter(
      (place) =>
        isCongregationSubLayerKey(place.category) &&
        enabledLayers.has(place.category) &&
        place.latitude >= normalizedBounds.south &&
        place.latitude <= normalizedBounds.north &&
        place.longitude >= normalizedBounds.west &&
        place.longitude <= normalizedBounds.east,
    ),
  ).slice(0, CONGREGATION_LIMIT)
}

const buildViewportTileKeys = (bounds: ViewportBounds) => {
  const normalizedBounds = normalizeViewportBounds(bounds)
  const minLatTile = Math.floor(normalizedBounds.south / CONGREGATION_CACHE_TILE_SIZE)
  const maxLatTile = Math.floor(normalizedBounds.north / CONGREGATION_CACHE_TILE_SIZE)
  const minLonTile = Math.floor(normalizedBounds.west / CONGREGATION_CACHE_TILE_SIZE)
  const maxLonTile = Math.floor(normalizedBounds.east / CONGREGATION_CACHE_TILE_SIZE)
  const latSteps = maxLatTile - minLatTile + 1
  const lonSteps = maxLonTile - minLonTile + 1

  if (latSteps <= 0 || lonSteps <= 0 || latSteps * lonSteps > CONGREGATION_CACHE_MAX_TILE_SCAN) {
    return null
  }

  const tileKeys = new Set<string>()

  for (let latTile = minLatTile; latTile <= maxLatTile; latTile += 1) {
    for (let lonTile = minLonTile; lonTile <= maxLonTile; lonTile += 1) {
      tileKeys.add(`${latTile}:${lonTile}`)
    }
  }

  return tileKeys
}

const resolveCongregationCategoryFromTags = (
  tags: Map<string, string>,
): CongregationSubLayerKey | null => {
  const amenity = tags.get('amenity')?.trim().toLowerCase() ?? ''
  const leisure = tags.get('leisure')?.trim().toLowerCase() ?? ''

  if (amenity === 'place_of_worship') {
    return 'worship'
  }

  if (amenity === 'school') {
    return 'school'
  }

  if (amenity === 'hospital') {
    return 'hospital'
  }

  if (leisure === 'stadium') {
    return 'stadium'
  }

  if (leisure === 'arena') {
    return 'arena'
  }

  return null
}

const parseOverpassCongregationPlaces = (
  xmlPayload: string,
  enabledLayers: Set<CongregationSubLayerKey>,
) => {
  const parser = new DOMParser()
  const xmlDocument = parser.parseFromString(xmlPayload, 'application/xml')

  if (xmlDocument.querySelector('parsererror')) {
    throw new Error('Unable to parse Overpass XML payload.')
  }

  const sourceElements = Array.from(xmlDocument.querySelectorAll('node, way, relation'))

  const places = sourceElements
    .map((sourceElement, index): CongregationPlace | null => {
      const center = sourceElement.querySelector('center')
      const latitude = Number(sourceElement.getAttribute('lat') ?? center?.getAttribute('lat') ?? '')
      const longitude = Number(sourceElement.getAttribute('lon') ?? center?.getAttribute('lon') ?? '')

      if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) {
        return null
      }

      const tagElements = Array.from(sourceElement.getElementsByTagName('tag'))
      const tags = new Map<string, string>()

      for (const tagElement of tagElements) {
        const key = tagElement.getAttribute('k')
        const value = tagElement.getAttribute('v')

        if (!key || !value) {
          continue
        }

        tags.set(key, value)
      }

      const category = resolveCongregationCategoryFromTags(tags)

      if (!category || !enabledLayers.has(category)) {
        return null
      }

      const categoryLabel = CONGREGATION_SUBLAYER_LABELS[category]
      const name = tags.get('name')?.trim() || tags.get('operator')?.trim()
      const osmType = normalizeOsmElementType(sourceElement.tagName)
      const osmId = sourceElement.getAttribute('id') ?? `${index + 1}`
      const serializedTags = Object.fromEntries(tags.entries())

      if (category !== 'worship') {
        return {
          key: `${category}-${sourceElement.tagName}-${sourceElement.getAttribute('id') ?? index + 1}`,
          latitude,
          longitude,
          name:
            name ||
            (category === 'school'
              ? 'Unnamed School'
              : category === 'stadium'
                ? 'Unnamed Stadium'
                : category === 'hospital'
                  ? 'Unnamed Hospital'
                  : 'Unnamed Arena'),
          osmType,
          osmId,
          category,
          categoryLabel,
          pinTheme: category,
          religionRaw: '',
          denominationRaw: '',
          religionLabel: '',
          icon: CONGREGATION_ICON_BY_CATEGORY[category],
          tags: serializedTags,
        }
      }

      const religionRaw = tags.get('religion')?.trim() ?? ''
      const denominationRaw = tags.get('denomination')?.trim() ?? ''
      const religionGroup = resolveReligionGroup(religionRaw, denominationRaw)
      const religionLabel = RELIGION_LABEL_BY_GROUP[religionGroup]

      return {
        key: `worship-${sourceElement.tagName}-${sourceElement.getAttribute('id') ?? index + 1}`,
        latitude,
        longitude,
        name: name || 'Unnamed Place of Worship',
        osmType,
        osmId,
        category,
        categoryLabel,
        pinTheme: religionGroup,
        religionRaw: religionRaw || religionLabel,
        denominationRaw,
        religionLabel,
        icon: RELIGION_ICON_BY_GROUP[religionGroup],
        tags: serializedTags,
      }
    })
    .filter((place): place is CongregationPlace => place !== null)

  return dedupeCongregationPlaces(places).slice(0, CONGREGATION_LIMIT)
}

const hydrateIncident = (incident: IncidentResult, index: number): IncidentUnit => {
  const level = levelFromIncident(incident)
  const severity = severityFromLevel(level)
  const resolvedAddress =
    incident.address?.trim() ||
    incident.rawLocation?.trim() ||
    incident.location?.trim() ||
    'Location unavailable'

  return {
    ...incident,
    key: incident.key || `INCIDENT-${index + 1}`,
    title: incident.title?.trim() || incident.key || `INCIDENT-${index + 1}`,
    address: resolvedAddress,
    level,
    severity,
  }
}

const DISPATCH_ROUTE_SOURCE_ID = 'emergency-dispatch-route-source'
const DISPATCH_ROUTE_LAYER_ID = 'emergency-dispatch-route-layer'
const EMPTY_DISPATCH_ROUTE_FEATURE_COLLECTION: any = {
  type: 'FeatureCollection',
  features: [],
}

const ensureDispatchRouteLayer = (map: maptilersdk.Map) => {
  if (!map.getSource(DISPATCH_ROUTE_SOURCE_ID)) {
    map.addSource(DISPATCH_ROUTE_SOURCE_ID, {
      type: 'geojson',
      data: EMPTY_DISPATCH_ROUTE_FEATURE_COLLECTION,
    })
  }

  if (!map.getLayer(DISPATCH_ROUTE_LAYER_ID)) {
    map.addLayer({
      id: DISPATCH_ROUTE_LAYER_ID,
      type: 'line',
      source: DISPATCH_ROUTE_SOURCE_ID,
      layout: {
        'line-join': 'round',
        'line-cap': 'round',
      },
      paint: {
        'line-color': '#ffcc00',
        'line-width': 6,
        'line-dasharray': [1, 1],
      },
    })
  }
}

const ensureTrafficFlowLayer = (map: maptilersdk.Map) => {
  if (!map.getSource(TOMTOM_TRAFFIC_SOURCE_ID)) {
    map.addSource(TOMTOM_TRAFFIC_SOURCE_ID, {
      type: 'raster',
      tiles: [TOMTOM_TRAFFIC_FLOW_TILE_TEMPLATE],
      tileSize: 256,
      attribution: 'Traffic flow \u00a9 TomTom',
    })
  }

  if (!map.getLayer(TOMTOM_TRAFFIC_LAYER_ID)) {
    map.addLayer({
      id: TOMTOM_TRAFFIC_LAYER_ID,
      type: 'raster',
      source: TOMTOM_TRAFFIC_SOURCE_ID,
      paint: {
        'raster-opacity': 0.84,
      },
    })
  }
}

const ensureRiskHeatmapLayer = (map: maptilersdk.Map) => {
  if (!map.getSource(RISK_HEATMAP_SOURCE_ID)) {
    map.addSource(RISK_HEATMAP_SOURCE_ID, {
      type: 'geojson',
      data: EMPTY_RISK_HEATMAP_FEATURE_COLLECTION,
    })
  }

  if (!map.getLayer(RISK_HEATMAP_FILL_LAYER_ID)) {
    map.addLayer({
      id: RISK_HEATMAP_FILL_LAYER_ID,
      type: 'fill',
      source: RISK_HEATMAP_SOURCE_ID,
      paint: {
        'fill-color': [
          'interpolate',
          ['linear'],
          ['coalesce', ['get', 'riskScore'], 0],
          0,
          '#1f9d55',
          0.5,
          '#f3c737',
          1,
          '#d3342f',
        ],
        'fill-opacity': [
          'interpolate',
          ['linear'],
          ['coalesce', ['get', 'riskScore'], 0],
          0,
          0.16,
          0.5,
          0.32,
          1,
          0.58,
        ],
      },
    })
  }

  if (!map.getLayer(RISK_HEATMAP_OUTLINE_LAYER_ID)) {
    map.addLayer({
      id: RISK_HEATMAP_OUTLINE_LAYER_ID,
      type: 'line',
      source: RISK_HEATMAP_SOURCE_ID,
      paint: {
        'line-color': 'rgba(255,255,255,0.16)',
        'line-width': 0.6,
      },
    })
  }
}

function App() {
  const mapContainerRef = useRef<HTMLDivElement | null>(null)
  const unitWidgetRef = useRef<HTMLElement | null>(null)
  const cctvVideoRef = useRef<HTMLVideoElement | null>(null)
  const cctvHlsRef = useRef<Hls | null>(null)
  const mapRef = useRef<maptilersdk.Map | null>(null)
  const incidentMarkersRef = useRef<maptilersdk.Marker[]>([])
  const congregationMarkersRef = useRef<maptilersdk.Marker[]>([])
  const cctvMarkersRef = useRef<maptilersdk.Marker[]>([])
  const emergencyVehicleMarkersRef = useRef<maptilersdk.Marker[]>([])
  const rotorcraftMarkersRef = useRef<maptilersdk.Marker[]>([])
  const emergencyVehicleRuntimeRef = useRef<EmergencyVehicleRuntime[]>([])
  const emergencyVehicleRoutesRef = useRef<EmergencyVehicleRoute[]>([])
  const emergencyVehicleLastTickRef = useRef(0)
  const unitWidgetDragStateRef = useRef<UnitWidgetDragState | null>(null)
  const incidentFetchControllerRef = useRef<AbortController | null>(null)
  const congregationFetchControllerRef = useRef<AbortController | null>(null)
  const cctvFetchControllerRef = useRef<AbortController | null>(null)
  const rotorcraftFetchControllerRef = useRef<AbortController | null>(null)
  const riskHeatmapFetchControllerRef = useRef<AbortController | null>(null)
  const congregationCacheInitPromiseRef = useRef<Promise<void> | null>(null)
  const aircraftCacheInitPromiseRef = useRef<Promise<void> | null>(null)
  const aircraftCacheSnapshotRef = useRef<AircraftCacheSnapshot | null>(null)
  const trafficSampleCacheRef = useRef<Map<string, TrafficSampleCacheEntry>>(new Map())
  const congregationCachedTilesByCategoryRef = useRef<Record<string, Set<string>>>(
    buildEmptyCongregationTileCoverage(),
  )
  const congregationCachedPlacesRef = useRef<CongregationPlace[]>([])

  const [activeLayers, setActiveLayers] = useState<LayerState>(DEFAULT_LAYER_STATE)
  const [congregationSubLayers, setCongregationSubLayers] = useState<CongregationSubLayerState>(
    DEFAULT_CONGREGATION_SUBLAYERS,
  )
  const [viewportBounds, setViewportBounds] = useState<ViewportBounds | null>(null)
  const [incidents, setIncidents] = useState<IncidentUnit[]>([])
  const [congregationPlaces, setCongregationPlaces] = useState<CongregationPlace[]>([])
  const [cctvCameras, setCctvCameras] = useState<CctvCamera[]>([])
  const [emergencyVehicles, setEmergencyVehicles] = useState<EmergencyVehicleUnit[]>([])
  const [rotorcraft, setRotorcraft] = useState<RotorcraftUnit[]>([])
  const [riskHeatmapCollection, setRiskHeatmapCollection] =
    useState<RiskHeatmapFeatureCollection>(EMPTY_RISK_HEATMAP_FEATURE_COLLECTION)
  const [riskHeatmapFallbackCount, setRiskHeatmapFallbackCount] = useState(0)
  const [selectedUnit, setSelectedUnit] = useState<SelectedUnit | null>(null)
  const [isCommandSidebarOpen, setIsCommandSidebarOpen] = useState(true)
  const [isIncidentsPanelOpen, setIsIncidentsPanelOpen] = useState(true)
  const [isThreatMenuOpen, setIsThreatMenuOpen] = useState(false)
  const [riskPrediction, setRiskPrediction] =
    useState<PredictionApiResponse>(DEFAULT_RISK_PREDICTION)
  const [riskPredictionError, setRiskPredictionError] = useState('')
  const [threatActionStatusById, setThreatActionStatusById] = useState<
    Record<string, ThreatActionStatus>
  >({})
  const [isUnitWidgetDismissed, setIsUnitWidgetDismissed] = useState(false)
  const [isLayersSectionOpen, setIsLayersSectionOpen] = useState(true)
  const [isEmergencySectionOpen, setIsEmergencySectionOpen] = useState(false)
  const [dispatchFormState, setDispatchFormState] = useState<Record<string, { lat: string, lng: string }>>(
    {},
  )
  const [expandedControlVehicle, setExpandedControlVehicle] = useState<string | null>(null)

  const refreshEmergencyVehiclesFromRuntime = () => {
    const updateTimestamp = Date.now()
    emergencyVehicleLastTickRef.current = updateTimestamp

    setEmergencyVehicles(
      advanceEmergencyVehicleSimulation(
        emergencyVehicleRuntimeRef.current,
        emergencyVehicleRoutesRef.current,
        0,
        updateTimestamp,
      ),
    )
  }

  const resetDispatchActionToPending = (dispatchActionId: string) => {
    if (!dispatchActionId) {
      return
    }

    setThreatActionStatusById((current) => ({
      ...current,
      [dispatchActionId]: 'idle',
    }))
  }

  const handleUpdateDispatchForm = (key: string, field: 'lat' | 'lng', value: string) => {
    setDispatchFormState((prev) => ({ ...prev, [key]: { ...prev[key], [field]: value } }))
  }

  const manuallyDispatchVehicle = (key: string) => {
    const form = dispatchFormState[key]
    if (!form || !form.lat || !form.lng) {
      return
    }

    const lat = parseFloat(form.lat)
    const lng = parseFloat(form.lng)
    if (isNaN(lat) || isNaN(lng)) {
      return
    }

    const runtime = emergencyVehicleRuntimeRef.current.find((candidate) => candidate.key === key)

    if (runtime) {
      const canceledDispatchActionId = runtime.dispatchActionId

      runtime.dispatchRoute = null
      runtime.dispatchDistanceMeters = 0
      runtime.dispatchTargetLabel = 'Manual Dispatch'
      runtime.dispatchActionId = ''
      runtime.isStationedAtDispatchTarget = false
      runtime.overrideLocation = { latitude: lat, longitude: lng, headingDegrees: 0 }
      runtime.overrideStatus = 'responding'

      resetDispatchActionToPending(canceledDispatchActionId)
      refreshEmergencyVehiclesFromRuntime()
    }
  }

  const manuallyStageVehicle = (key: string) => {
    const runtime = emergencyVehicleRuntimeRef.current.find((candidate) => candidate.key === key)
    const snapshot = emergencyVehicles.find((candidate) => candidate.key === key)

    if (runtime && snapshot) {
      const canceledDispatchActionId = runtime.dispatchActionId

      runtime.dispatchRoute = null
      runtime.dispatchDistanceMeters = 0
      runtime.dispatchTargetLabel = ''
      runtime.dispatchActionId = ''
      runtime.isStationedAtDispatchTarget = false
      runtime.overrideLocation = {
        latitude: snapshot.latitude,
        longitude: snapshot.longitude,
        headingDegrees: snapshot.headingDegrees,
      }
      runtime.overrideStatus = 'staged'

      resetDispatchActionToPending(canceledDispatchActionId)
      refreshEmergencyVehiclesFromRuntime()
    }
  }

  const manuallyPatrolVehicle = (key: string) => {
    const runtime = emergencyVehicleRuntimeRef.current.find((candidate) => candidate.key === key)

    if (runtime) {
      const canceledDispatchActionId = runtime.dispatchActionId

      runtime.dispatchRoute = null
      runtime.dispatchDistanceMeters = 0
      runtime.dispatchTargetLabel = ''
      runtime.dispatchActionId = ''
      runtime.isStationedAtDispatchTarget = false
      runtime.overrideLocation = undefined
      runtime.overrideStatus = undefined

      resetDispatchActionToPending(canceledDispatchActionId)
      refreshEmergencyVehiclesFromRuntime()
    }
  }

  const [unitWidgetPosition, setUnitWidgetPosition] = useState<UnitWidgetPosition>({
    x: 18,
    y: 18,
  })
  const [incidentLoading, setIncidentLoading] = useState(false)
  const [incidentError, setIncidentError] = useState('')
  const [congregationLoading, setCongregationLoading] = useState(false)
  const [congregationError, setCongregationError] = useState('')
  const [cctvLoading, setCctvLoading] = useState(false)
  const [cctvError, setCctvError] = useState('')
  const [cctvPlaybackError, setCctvPlaybackError] = useState('')
  const [emergencyVehicleError, setEmergencyVehicleError] = useState('')
  const [rotorcraftLoading, setRotorcraftLoading] = useState(false)
  const [rotorcraftError, setRotorcraftError] = useState('')
  const [rotorcraftFeedStats, setRotorcraftFeedStats] = useState<RotorcraftFeedStats>(
    EMPTY_ROTORCRAFT_FEED_STATS,
  )
  const [initialCenter, setInitialCenter] = useState<[number, number]>(DEFAULT_CENTER)
  const [hasResolvedInitialCenter, setHasResolvedInitialCenter] = useState(false)

  const selectedIncidentKey = selectedUnit?.kind === 'incident' ? selectedUnit.key : ''
  const selectedCongregationKey = selectedUnit?.kind === 'congregation' ? selectedUnit.key : ''
  const selectedCctvKey = selectedUnit?.kind === 'cctv' ? selectedUnit.key : ''
  const selectedEmergencyVehicleKey = selectedUnit?.kind === 'emergencyVehicle' ? selectedUnit.key : ''
  const selectedAircraftKey = selectedUnit?.kind === 'aircraft' ? selectedUnit.key : ''

  const selectedIncident = useMemo(() => {
    if (selectedUnit?.kind !== 'incident') {
      return null
    }

    return incidents.find((incident) => incident.key === selectedUnit.key) ?? null
  }, [incidents, selectedUnit])

  const selectedCongregation = useMemo(() => {
    if (selectedUnit?.kind !== 'congregation') {
      return null
    }

    return congregationPlaces.find((place) => place.key === selectedUnit.key) ?? null
  }, [congregationPlaces, selectedUnit])

  const selectedAircraft = useMemo(() => {
    if (selectedUnit?.kind !== 'aircraft') {
      return null
    }

    return rotorcraft.find((aircraft) => aircraft.key === selectedUnit.key) ?? null
  }, [rotorcraft, selectedUnit])

  const selectedCctv = useMemo(() => {
    if (selectedUnit?.kind !== 'cctv') {
      return null
    }

    return cctvCameras.find((camera) => camera.key === selectedUnit.key) ?? null
  }, [cctvCameras, selectedUnit])

  const selectedEmergencyVehicle = useMemo(() => {
    if (selectedUnit?.kind !== 'emergencyVehicle') {
      return null
    }

    return emergencyVehicles.find((vehicle) => vehicle.key === selectedUnit.key) ?? null
  }, [emergencyVehicles, selectedUnit])

  const selectedAircraftDetailFields = useMemo(
    () => (selectedAircraft ? buildAircraftDetailFields(selectedAircraft) : []),
    [selectedAircraft],
  )

  const selectedCongregationTags = useMemo(() => {
    if (!selectedCongregation) {
      return []
    }

    return Object.entries(selectedCongregation.tags).sort(([firstKey], [secondKey]) =>
      firstKey.localeCompare(secondKey),
    )
  }, [selectedCongregation])

  const selectedUpdates = useMemo(() => {
    if (!selectedIncident?.updates) {
      return []
    }

    return Object.values(selectedIncident.updates)
      .sort((a, b) => b.ts - a.ts)
      .slice(0, 3)
  }, [selectedIncident])

  const threatActionItems = useMemo<ThreatActionItem[]>(() => {
    const policeDispatch = riskPrediction.mitigation_strategy.police_dispatch
    const medicalStandby = riskPrediction.mitigation_strategy.medical_standby
    const dispatchActions = policeDispatch.assigned_units.map((unit) => {
      const normalizedUnitId = unit.vehicle_id
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, '-')
        .replace(/(^-|-$)/g, '')

      return {
        id: `dispatch-${normalizedUnitId || 'unit'}`,
        label: `Dispatch ${unit.vehicle_id}`,
        detail: `${policeDispatch.action} � Feed status: ${unit.status}`,
        kind: 'dispatch' as const,
        dispatchVehicleId: unit.vehicle_id,
      }
    })

    return [
      ...dispatchActions,
      {
        id: 'medical-standby',
        label: 'Prepare Medical Standby',
        detail: `${medicalStandby.unit_id} ${medicalStandby.instruction}`,
        kind: 'medical',
      },
      {
        id: 'traffic-reroute',
        label: 'Apply Traffic Re-route',
        detail: riskPrediction.mitigation_strategy.traffic_control['re-routing'],
        kind: 'traffic',
      },
    ]
  }, [riskPrediction])

  useEffect(() => {
    setThreatActionStatusById((current) => {
      const validActionIds = new Set(threatActionItems.map((actionItem) => actionItem.id))
      const nextStatusById: Record<string, ThreatActionStatus> = {}

      Object.entries(current).forEach(([actionId, status]) => {
        if (validActionIds.has(actionId)) {
          nextStatusById[actionId] = status
        }
      })

      return nextStatusById
    })
  }, [threatActionItems])

  const incidentLevelSummary = useMemo(() => {
    const levelZero = incidents.filter((incident) => incident.level === 0).length
    const levelOnePlus = incidents.filter((incident) => incident.level > 0).length

    return {
      total: incidents.length,
      levelZero,
      levelOnePlus,
    }
  }, [incidents])

  const enabledCongregationSubLayers = useMemo(
    () =>
      new Set<CongregationSubLayerKey>(
        CONGREGATION_SUBLAYER_KEYS.filter((layerKey) => congregationSubLayers[layerKey]),
      ),
    [congregationSubLayers],
  )

  const hasEnabledCongregationSubLayers = enabledCongregationSubLayers.size > 0

  const congregationSummary = useMemo(() => {
    const trackedCategories = new Set<CongregationCategory>()
    const trackedReligions = new Set<string>()

    for (const place of congregationPlaces) {
      trackedCategories.add(place.category)

      if (place.category === 'worship' && place.religionLabel) {
        trackedReligions.add(place.religionLabel)
      }
    }

    return {
      total: congregationPlaces.length,
      trackedCategories: trackedCategories.size,
      trackedReligions: trackedReligions.size,
    }
  }, [congregationPlaces])

  const aircraftSummary = useMemo(() => {
    const grounded = rotorcraft.filter((aircraft) => aircraft.onGround).length

    return {
      total: rotorcraft.length,
      grounded,
      airborne: rotorcraft.length - grounded,
    }
  }, [rotorcraft])

  const cctvSummary = useMemo(
    () => ({
      total: cctvCameras.length,
    }),
    [cctvCameras],
  )

  const emergencyVehicleSummary = useMemo(() => {
    const police = emergencyVehicles.filter((vehicle) => vehicle.vehicleType === 'police').length
    const ambulances = emergencyVehicles.filter(
      (vehicle) => vehicle.vehicleType === 'ambulance',
    ).length
    const firetrucks = emergencyVehicles.filter(
      (vehicle) => vehicle.vehicleType === 'firetruck',
    ).length

    return {
      total: emergencyVehicles.length,
      police,
      ambulances,
      firetrucks,
    }
  }, [emergencyVehicles])

  const riskHeatmapSummary = useMemo(() => {
    const highRiskCells = riskHeatmapCollection.features.filter(
      (feature) => feature.properties.riskLevel === 'high',
    ).length
    const mediumRiskCells = riskHeatmapCollection.features.filter(
      (feature) => feature.properties.riskLevel === 'medium',
    ).length
    const lowRiskCells = riskHeatmapCollection.features.filter(
      (feature) => feature.properties.riskLevel === 'low',
    ).length
    const total = riskHeatmapCollection.features.length
    const averageScore =
      total <= 0
        ? 0
        : riskHeatmapCollection.features.reduce(
            (scoreTotal, feature) => scoreTotal + feature.properties.riskScore,
            0,
          ) / total

    return {
      total,
      highRiskCells,
      mediumRiskCells,
      lowRiskCells,
      averageScore,
    }
  }, [riskHeatmapCollection])

  const openIncident = useCallback((incidentKey: string) => {
    setIsUnitWidgetDismissed(false)
    setSelectedUnit({
      kind: 'incident',
      key: incidentKey,
    })
  }, [])

  const openCongregation = useCallback((placeKey: string) => {
    setIsUnitWidgetDismissed(false)
    setSelectedUnit({
      kind: 'congregation',
      key: placeKey,
    })
  }, [])

  const openAircraft = useCallback((aircraftKey: string) => {
    setIsUnitWidgetDismissed(false)
    setSelectedUnit({
      kind: 'aircraft',
      key: aircraftKey,
    })
  }, [])

  const openCctv = useCallback((cameraKey: string) => {
    setIsUnitWidgetDismissed(false)
    setSelectedUnit({
      kind: 'cctv',
      key: cameraKey,
    })
  }, [])

  const openEmergencyVehicle = useCallback((vehicleKey: string) => {
    setIsUnitWidgetDismissed(false)
    setSelectedUnit({
      kind: 'emergencyVehicle',
      key: vehicleKey,
    })
  }, [])

  const syncThreatDispatchStatuses = useCallback(() => {
    setThreatActionStatusById((current) => {
      const nextStatusById = { ...current }

      threatActionItems.forEach((actionItem) => {
        if (actionItem.kind === 'dispatch') {
          nextStatusById[actionItem.id] = 'idle'
        }
      })

      emergencyVehicleRuntimeRef.current.forEach((runtime) => {
        if (!runtime.dispatchActionId) {
          return
        }

        nextStatusById[runtime.dispatchActionId] = runtime.isStationedAtDispatchTarget
          ? 'complete'
          : 'active'
      })

      return nextStatusById
    })
  }, [threatActionItems])

  const dispatchEmergencyUnitToRisk = useCallback(
    async (actionItem: ThreatActionItem) => {
      if (actionItem.kind !== 'dispatch' || !actionItem.dispatchVehicleId) {
        return
      }

      setThreatActionStatusById((current) => ({
        ...current,
        [actionItem.id]: 'active',
      }))

      const runtime = emergencyVehicleRuntimeRef.current.find(
        (candidate) =>
          candidate.unitCode.toLowerCase() === actionItem.dispatchVehicleId?.toLowerCase(),
      )

      if (!runtime) {
        setThreatActionStatusById((current) => ({
          ...current,
          [actionItem.id]: 'error',
        }))
        return
      }

      const currentPosition = getEmergencyRuntimeCoordinates(
        runtime,
        emergencyVehicleRoutesRef.current,
      )

      try {
        const routeResponse = await fetch(EMERGENCY_ROUTE_DISPATCH_API, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            start: {
              latitude: currentPosition.latitude,
              longitude: currentPosition.longitude,
            },
            target: {
              latitude: riskPrediction.risk_assessment.coordinates.latitude,
              longitude: riskPrediction.risk_assessment.coordinates.longitude,
            },
          }),
        })

        if (!routeResponse.ok) {
          throw new Error('Unable to route emergency unit to risk zone.')
        }

        const routePayload = (await routeResponse.json()) as RoadRouteApiResponse
        const routePoints: [number, number][] = []

        routePayload.coordinates.forEach((coordinate) => {
          if (!Array.isArray(coordinate) || coordinate.length < 2) {
            return
          }

          const longitude = toFiniteNumber(coordinate[0])
          const latitude = toFiniteNumber(coordinate[1])

          if (longitude === null || latitude === null) {
            return
          }

          routePoints.push([longitude, latitude])
        })

        const dispatchRoute = buildEmergencyRouteFromPoints(
          `dispatch-${runtime.key}-${Date.now()}`,
          `Dispatch ${runtime.unitCode}`,
          routePoints,
          false,
        )

        if (!dispatchRoute) {
          throw new Error('No routable path was generated for this dispatch.')
        }

        runtime.dispatchRoute = dispatchRoute
        runtime.dispatchDistanceMeters = 0
        runtime.dispatchTargetLabel = riskPrediction.risk_assessment.location_name
        runtime.dispatchActionId = actionItem.id
        runtime.isStationedAtDispatchTarget = false

        const updateTimestamp = Date.now()
        emergencyVehicleLastTickRef.current = updateTimestamp

        setEmergencyVehicles(
          advanceEmergencyVehicleSimulation(
            emergencyVehicleRuntimeRef.current,
            emergencyVehicleRoutesRef.current,
            0,
            updateTimestamp,
          ),
        )

        syncThreatDispatchStatuses()
      } catch {
        setThreatActionStatusById((current) => ({
          ...current,
          [actionItem.id]: 'error',
        }))
      }
    },
    [riskPrediction, syncThreatDispatchStatuses],
  )

  const handleThreatActionPress = useCallback(
    (actionItem: ThreatActionItem) => {
      if (actionItem.kind !== 'dispatch') {
        return
      }

      void dispatchEmergencyUnitToRisk(actionItem)
    },
    [dispatchEmergencyUnitToRisk],
  )

  const closeUnitWidget = useCallback(() => {
    setIsUnitWidgetDismissed(true)
    setSelectedUnit(null)
  }, [])

  const clampUnitWidgetPosition = useCallback((x: number, y: number): UnitWidgetPosition => {
    const minOffset = 8
    const mapElement = mapContainerRef.current
    const widgetElement = unitWidgetRef.current

    if (!mapElement || !widgetElement) {
      return {
        x: Math.max(minOffset, x),
        y: Math.max(minOffset, y),
      }
    }

    const mapRect = mapElement.getBoundingClientRect()
    const widgetRect = widgetElement.getBoundingClientRect()
    const maxX = Math.max(minOffset, mapRect.width - widgetRect.width - minOffset)
    const maxY = Math.max(minOffset, mapRect.height - widgetRect.height - minOffset)

    return {
      x: Math.min(Math.max(x, minOffset), maxX),
      y: Math.min(Math.max(y, minOffset), maxY),
    }
  }, [])

  const handleUnitWidgetPointerDown = useCallback((event: ReactPointerEvent<HTMLDivElement>) => {
    if (event.button !== 0 || !unitWidgetRef.current) {
      return
    }

    const widgetRect = unitWidgetRef.current.getBoundingClientRect()

    unitWidgetDragStateRef.current = {
      pointerId: event.pointerId,
      pointerOffsetX: event.clientX - widgetRect.left,
      pointerOffsetY: event.clientY - widgetRect.top,
    }

    event.currentTarget.setPointerCapture(event.pointerId)
  }, [])

  const handleUnitWidgetPointerMove = useCallback(
    (event: ReactPointerEvent<HTMLDivElement>) => {
      const dragState = unitWidgetDragStateRef.current
      const mapElement = mapContainerRef.current

      if (!dragState || dragState.pointerId !== event.pointerId || !mapElement) {
        return
      }

      const mapRect = mapElement.getBoundingClientRect()
      const nextX = event.clientX - mapRect.left - dragState.pointerOffsetX
      const nextY = event.clientY - mapRect.top - dragState.pointerOffsetY

      setUnitWidgetPosition(clampUnitWidgetPosition(nextX, nextY))
    },
    [clampUnitWidgetPosition],
  )

  const handleUnitWidgetPointerUp = useCallback((event: ReactPointerEvent<HTMLDivElement>) => {
    const dragState = unitWidgetDragStateRef.current

    if (!dragState || dragState.pointerId !== event.pointerId) {
      return
    }

    unitWidgetDragStateRef.current = null

    if (event.currentTarget.hasPointerCapture(event.pointerId)) {
      event.currentTarget.releasePointerCapture(event.pointerId)
    }
  }, [])

  useEffect(() => {
    const fetchController = new AbortController()

    const syncRiskPrediction = async () => {
      try {
        const response = await fetch(RISK_PREDICTION_API, {
          signal: fetchController.signal,
        })

        if (!response.ok) {
          throw new Error('Risk prediction API request failed.')
        }

        const predictionResponse = (await response.json()) as PredictionApiResponse
        setRiskPrediction(predictionResponse)
        setRiskPredictionError('')
      } catch {
        if (fetchController.signal.aborted) {
          return
        }

        setRiskPrediction(DEFAULT_RISK_PREDICTION)
        setRiskPredictionError('Risk API unavailable. Showing fallback prediction payload.')
      }
    }

    void syncRiskPrediction()

    return () => {
      fetchController.abort()
    }
  }, [])

  useEffect(() => {
    const syncUnitWidgetBounds = () => {
      setUnitWidgetPosition((currentPosition) =>
        clampUnitWidgetPosition(currentPosition.x, currentPosition.y),
      )
    }

    window.addEventListener('resize', syncUnitWidgetBounds)

    return () => {
      window.removeEventListener('resize', syncUnitWidgetBounds)
    }
  }, [clampUnitWidgetPosition])

  useEffect(() => {
    if (!activeLayers.incidentPins) {
      setIsIncidentsPanelOpen(false)
    }

    setSelectedUnit((current) => {
      if (!current) {
        return current
      }

      if (current.kind === 'incident' && !activeLayers.incidentPins) {
        return null
      }

      if (current.kind === 'congregation' && !activeLayers.congregationPins) {
        return null
      }

      if (current.kind === 'cctv' && !activeLayers.cctvPins) {
        return null
      }

      if (current.kind === 'emergencyVehicle' && !activeLayers.emergencyVehiclePins) {
        return null
      }

      if (current.kind === 'aircraft' && !activeLayers.rotorcraftPins) {
        return null
      }

      return current
    })
  }, [
    activeLayers.incidentPins,
    activeLayers.congregationPins,
    activeLayers.cctvPins,
    activeLayers.emergencyVehiclePins,
    activeLayers.rotorcraftPins,
  ])

  const hydrateCongregationCache = useCallback(async () => {
    if (congregationCacheInitPromiseRef.current) {
      await congregationCacheInitPromiseRef.current
      return
    }

    const initTask = (async () => {
      const mergedTilesByCategory = buildEmptyCongregationTileCoverage()
      const mergedPlaces: CongregationPlace[] = []

      const mergeCacheSnapshot = (cacheSnapshot: CongregationCacheSnapshot) => {
        for (const [categoryKey, tileSet] of Object.entries(cacheSnapshot.coveredTilesByCategory)) {
          const categoryCoverage = ensureCongregationCategoryCoverage(
            mergedTilesByCategory,
            categoryKey,
          )

          tileSet.forEach((tileKey) => {
            categoryCoverage.add(tileKey)
          })
        }

        mergedPlaces.push(...cacheSnapshot.places)
      }

      try {
        const cacheSources = [CONGREGATION_CACHE_API, CONGREGATION_CACHE_FILE]

        for (const cacheSource of cacheSources) {
          try {
            const response = await fetch(cacheSource, {
              cache: 'no-store',
            })

            if (!response.ok) {
              continue
            }

            const cachePayload = (await response.json()) as unknown
            mergeCacheSnapshot(parseCongregationCachePayload(cachePayload))
            break
          } catch {
            // Try the next available source.
          }
        }
      } catch {
        // Ignore unavailable local seed file and continue with runtime cache.
      }

      if (typeof window !== 'undefined') {
        try {
          const localStoragePayload = window.localStorage.getItem(CONGREGATION_CACHE_STORAGE_KEY)

          if (localStoragePayload) {
            mergeCacheSnapshot(
              parseCongregationCachePayload(JSON.parse(localStoragePayload) as unknown),
            )
          }
        } catch {
          // Ignore malformed local cache payloads.
        }
      }

      congregationCachedTilesByCategoryRef.current = mergedTilesByCategory
      congregationCachedPlacesRef.current = dedupeCongregationPlaces(mergedPlaces)
    })()

    congregationCacheInitPromiseRef.current = initTask
    await initTask
  }, [])

  const persistCongregationCache = useCallback(() => {
    const payload = serializeCongregationCachePayload(
      congregationCachedTilesByCategoryRef.current,
      congregationCachedPlacesRef.current,
    )

    if (typeof window === 'undefined') {
      return
    }

    try {
      window.localStorage.setItem(CONGREGATION_CACHE_STORAGE_KEY, JSON.stringify(payload))
    } catch {
      // Ignore quota and storage access errors.
    }

    void fetch(CONGREGATION_CACHE_API, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
      },
      body: JSON.stringify(payload),
    }).catch(() => {
      // Ignore file-write failures in environments without write access.
    })
  }, [])

  const hydrateAircraftCache = useCallback(async () => {
    if (aircraftCacheInitPromiseRef.current) {
      await aircraftCacheInitPromiseRef.current
      return
    }

    const initTask = (async () => {
      let latestSnapshot: AircraftCacheSnapshot | null = null

      try {
        const cacheSources = [AIRCRAFT_CACHE_API, AIRCRAFT_CACHE_FILE]

        for (const cacheSource of cacheSources) {
          try {
            const response = await fetch(cacheSource, {
              cache: 'no-store',
            })

            if (!response.ok) {
              continue
            }

            const cachePayload = (await response.json()) as unknown
            const parsedSnapshot = parseAircraftCachePayload(cachePayload)

            if (parsedSnapshot) {
              latestSnapshot = parsedSnapshot
              break
            }
          } catch {
            // Try the next available source.
          }
        }
      } catch {
        // Ignore unavailable aircraft cache files and continue.
      }

      if (typeof window !== 'undefined') {
        try {
          const localStoragePayload = window.localStorage.getItem(AIRCRAFT_CACHE_STORAGE_KEY)

          if (localStoragePayload) {
            const parsedSnapshot = parseAircraftCachePayload(
              JSON.parse(localStoragePayload) as unknown,
            )

            if (parsedSnapshot && (!latestSnapshot || parsedSnapshot.fetchedAt > latestSnapshot.fetchedAt)) {
              latestSnapshot = parsedSnapshot
            }
          }
        } catch {
          // Ignore malformed temporary cache payloads.
        }
      }

      aircraftCacheSnapshotRef.current = latestSnapshot
    })()

    aircraftCacheInitPromiseRef.current = initTask
    await initTask
  }, [])

  const persistAircraftCache = useCallback((cacheSnapshot: AircraftCacheSnapshot) => {
    const payload = serializeAircraftCachePayload(cacheSnapshot)

    if (typeof window === 'undefined') {
      return
    }

    try {
      window.localStorage.setItem(AIRCRAFT_CACHE_STORAGE_KEY, JSON.stringify(payload))
    } catch {
      // Ignore quota and storage access errors.
    }

    void fetch(AIRCRAFT_CACHE_API, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
      },
      body: JSON.stringify(payload),
    }).catch(() => {
      // Ignore file-write failures in environments without write access.
    })
  }, [])

  useEffect(() => {
    let isResolved = false

    const resolveInitialCenter = (center: [number, number]) => {
      if (isResolved) {
        return
      }

      isResolved = true
      setInitialCenter(center)
      setHasResolvedInitialCenter(true)
    }

    if (typeof window === 'undefined' || typeof navigator === 'undefined' || !navigator.geolocation) {
      resolveInitialCenter(DEFAULT_CENTER)
      return () => {
        isResolved = true
      }
    }

    const fallbackTimer = window.setTimeout(() => {
      resolveInitialCenter(DEFAULT_CENTER)
    }, 11_000)

    navigator.geolocation.getCurrentPosition(
      ({ coords }) => {
        if (Number.isFinite(coords.latitude) && Number.isFinite(coords.longitude)) {
          resolveInitialCenter([coords.longitude, coords.latitude])
          return
        }

        resolveInitialCenter(DEFAULT_CENTER)
      },
      () => {
        resolveInitialCenter(DEFAULT_CENTER)
      },
      {
        enableHighAccuracy: true,
        timeout: 10_000,
        maximumAge: 0,
      },
    )

    return () => {
      isResolved = true
      window.clearTimeout(fallbackTimer)
    }
  }, [])

  useEffect(() => {
    if (!mapContainerRef.current || mapRef.current || !hasResolvedInitialCenter) {
      return
    }

    const map = new maptilersdk.Map({
      container: mapContainerRef.current,
      style: `https://api.maptiler.com/maps/dataviz-dark/style.json?key=${MAPTILER_KEY}`,
      center: initialCenter,
      zoom: DEFAULT_ZOOM,
      pitch: DEFAULT_PITCH,
      bearing: DEFAULT_BEARING,
      attributionControl: false,
      geolocate: false,
      navigationControl: false,
    })

    mapRef.current = map

    const syncViewportBounds = () => {
      const bounds = map.getBounds()

      setViewportBounds({
        lowerLatitude: bounds.getSouth(),
        lowerLongitude: bounds.getWest(),
        upperLatitude: bounds.getNorth(),
        upperLongitude: bounds.getEast(),
      })
    }

    const clearSelection = () => setSelectedUnit(null)

    map.on('load', syncViewportBounds)
    map.on('moveend', syncViewportBounds)
    map.on('click', clearSelection)

    return () => {
      map.off('load', syncViewportBounds)
      map.off('moveend', syncViewportBounds)
      map.off('click', clearSelection)

      incidentMarkersRef.current.forEach((marker) => marker.remove())
      incidentMarkersRef.current = []
      congregationMarkersRef.current.forEach((marker) => marker.remove())
      congregationMarkersRef.current = []
      cctvMarkersRef.current.forEach((marker) => marker.remove())
      cctvMarkersRef.current = []
      emergencyVehicleMarkersRef.current.forEach((marker) => marker.remove())
      emergencyVehicleMarkersRef.current = []
      rotorcraftMarkersRef.current.forEach((marker) => marker.remove())
      rotorcraftMarkersRef.current = []

      map.remove()
      mapRef.current = null
    }
  }, [hasResolvedInitialCenter, initialCenter])

  useEffect(() => {
    if (!viewportBounds || !activeLayers.incidentPins) {
      incidentFetchControllerRef.current?.abort()
      setIncidentLoading(false)
      setIncidentError('')
      setIncidents([])
      return
    }

    incidentFetchControllerRef.current?.abort()

    const controller = new AbortController()
    incidentFetchControllerRef.current = controller

    const fetchIncidents = async () => {
      setIncidentLoading(true)
      setIncidentError('')

      try {
        const directUrl = buildCitizenTrendingUrl(CITIZEN_DIRECT_BASE, viewportBounds)
        const proxyUrl = buildCitizenTrendingUrl(CITIZEN_PROXY_BASE, viewportBounds)
        const requestUrls = [directUrl, proxyUrl]

        let response: Response | null = null

        for (const requestUrl of requestUrls) {
          try {
            const candidate = await fetch(requestUrl, {
              signal: controller.signal,
            })

            if (candidate.ok) {
              response = candidate
              break
            }
          } catch {
            if (controller.signal.aborted) {
              return
            }
          }
        }

        if (!response) {
          throw new Error('Unable to load Citizen incident feed.')
        }

        const data = (await response.json()) as IncidentApiResponse
        const liveIncidents = (data.results ?? [])
          .filter(
            (incident) =>
              Number.isFinite(incident.latitude) &&
              Number.isFinite(incident.longitude) &&
              incident.latitude !== 0 &&
              incident.longitude !== 0,
          )
          .slice(0, INCIDENT_LIMIT)
          .map((incident, index) => hydrateIncident(incident, index))

        if (!controller.signal.aborted) {
          setIncidents(liveIncidents)

          if (!liveIncidents.length) {
            setIncidentError('No incidents returned by Citizen for this viewport.')
          }
        }
      } catch {
        if (!controller.signal.aborted) {
          setIncidentError('Live feed unavailable. Unable to load Citizen incidents.')
          setIncidents([])
        }
      } finally {
        if (!controller.signal.aborted) {
          setIncidentLoading(false)
        }
      }
    }

    void fetchIncidents()

    return () => {
      controller.abort()
    }
  }, [viewportBounds, activeLayers.incidentPins])

  useEffect(() => {
    const congregationLayerEnabled =
      activeLayers.congregationPins && hasEnabledCongregationSubLayers

    if (!viewportBounds || !congregationLayerEnabled) {
      congregationFetchControllerRef.current?.abort()
      setCongregationLoading(false)
      setCongregationError('')
      setCongregationPlaces([])
      return
    }

    congregationFetchControllerRef.current?.abort()

    const controller = new AbortController()
    congregationFetchControllerRef.current = controller

    const syncCongregationPlaces = async () => {
      setCongregationLoading(true)
      setCongregationError('')

      try {
        await hydrateCongregationCache()

        if (controller.signal.aborted) {
          return
        }

        const viewportTileKeys = buildViewportTileKeys(viewportBounds)
        const cachedPlacesInViewport = getCongregationPlacesInBounds(
          congregationCachedPlacesRef.current,
          viewportBounds,
          enabledCongregationSubLayers,
        )

        if (cachedPlacesInViewport.length) {
          setCongregationPlaces(cachedPlacesInViewport)
        }

        let hasFullTileCoverage = viewportTileKeys !== null

        if (viewportTileKeys) {
          for (const layerKey of enabledCongregationSubLayers) {
            const layerCache = congregationCachedTilesByCategoryRef.current[layerKey]

            if (!layerCache || !Array.from(viewportTileKeys).every((tileKey) => layerCache.has(tileKey))) {
              hasFullTileCoverage = false
              break
            }
          }
        }

        if (hasFullTileCoverage) {
          setCongregationPlaces(cachedPlacesInViewport)

          if (!cachedPlacesInViewport.length) {
            setCongregationError('No congregation areas available in local cache for this viewport.')
          }

          return
        }

        const query = buildOverpassCongregationQuery(viewportBounds, enabledCongregationSubLayers)

        if (!query) {
          setCongregationPlaces(cachedPlacesInViewport)
          return
        }

        const requestBody = new URLSearchParams({
          data: query,
        }).toString()

        const response = await fetch(OVERPASS_ENDPOINT, {
          method: 'POST',
          headers: {
            accept: '*/*',
            'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
          },
          body: requestBody,
          signal: controller.signal,
        })

        if (!response.ok) {
          throw new Error('Overpass congregation request failed.')
        }

        const payload = await response.text()
        const parsedPlaces = parseOverpassCongregationPlaces(payload, enabledCongregationSubLayers)

        if (!controller.signal.aborted) {
          congregationCachedPlacesRef.current = dedupeCongregationPlaces([
            ...congregationCachedPlacesRef.current,
            ...parsedPlaces,
          ])

          if (viewportTileKeys) {
            for (const layerKey of enabledCongregationSubLayers) {
              const layerCoverage = ensureCongregationCategoryCoverage(
                congregationCachedTilesByCategoryRef.current,
                layerKey,
              )

              viewportTileKeys.forEach((tileKey) => {
                layerCoverage.add(tileKey)
              })
            }
          }

          persistCongregationCache()

          const resolvedPlaces = getCongregationPlacesInBounds(
            congregationCachedPlacesRef.current,
            viewportBounds,
            enabledCongregationSubLayers,
          )

          setCongregationPlaces(resolvedPlaces)

          if (!resolvedPlaces.length) {
            setCongregationError('No congregation areas were returned for this viewport.')
          }
        }
      } catch {
        if (!controller.signal.aborted) {
          const fallbackPlaces = getCongregationPlacesInBounds(
            congregationCachedPlacesRef.current,
            viewportBounds,
            enabledCongregationSubLayers,
          )

          setCongregationPlaces(fallbackPlaces)
          setCongregationError(
            fallbackPlaces.length
              ? 'OpenStreetMap unavailable. Showing cached congregation areas.'
              : 'OpenStreetMap layer unavailable. Unable to load congregation areas.',
          )
        }
      } finally {
        if (!controller.signal.aborted) {
          setCongregationLoading(false)
        }
      }
    }

    void syncCongregationPlaces()

    return () => {
      controller.abort()
    }
  }, [
    viewportBounds,
    activeLayers.congregationPins,
    enabledCongregationSubLayers,
    hasEnabledCongregationSubLayers,
    hydrateCongregationCache,
    persistCongregationCache,
  ])

  useEffect(() => {
    if (!activeLayers.cctvPins) {
      cctvFetchControllerRef.current?.abort()
      setCctvLoading(false)
      setCctvError('')
      setCctvPlaybackError('')
      setCctvCameras([])
      return
    }

    cctvFetchControllerRef.current?.abort()

    const controller = new AbortController()
    cctvFetchControllerRef.current = controller

    const syncCctvCameras = async () => {
      setCctvLoading(true)
      setCctvError('')

      try {
        const response = await fetch(CCTV_SOURCE_FILE, {
          cache: 'no-store',
          signal: controller.signal,
        })

        if (!response.ok) {
          throw new Error('Unable to load CCTV camera payload.')
        }

        const payload = (await response.json()) as unknown
        const parsedCameras = parseCctvCamerasPayload(payload)

        if (!controller.signal.aborted) {
          setCctvCameras(parsedCameras)

          if (!parsedCameras.length) {
            setCctvError('No camera streams with valid coordinates were found in cams.json.')
          }
        }
      } catch {
        if (!controller.signal.aborted) {
          setCctvCameras([])
          setCctvError('CCTV layer unavailable. Unable to load camera stream definitions.')
        }
      } finally {
        if (!controller.signal.aborted) {
          setCctvLoading(false)
        }
      }
    }

    void syncCctvCameras()

    return () => {
      controller.abort()
    }
  }, [activeLayers.cctvPins])

  useEffect(() => {
    if (!hasResolvedInitialCenter || !activeLayers.emergencyVehiclePins) {
      emergencyVehicleRuntimeRef.current = []
      emergencyVehicleRoutesRef.current = []
      emergencyVehicleLastTickRef.current = 0
      setEmergencyVehicleError('')
      setEmergencyVehicles([])
      setThreatActionStatusById({})
      return
    }

    const controller = new AbortController()
    let isDisposed = false
    let simulationTimerId: number | null = null

    const startEmergencySimulation = async () => {
      setEmergencyVehicleError('')

      try {
        const response = await fetch(EMERGENCY_FLEET_SOURCE_FILE, {
          cache: 'no-store',
          signal: controller.signal,
        })

        if (!response.ok) {
          throw new Error('Unable to load emergency fleet payload.')
        }

        const payload = (await response.json()) as unknown
        const fleetSnapshot = parseEmergencyFleetPayload(payload)

        if (!fleetSnapshot) {
          throw new Error('Emergency fleet payload is invalid.')
        }

        if (controller.signal.aborted || isDisposed) {
          return
        }

        const snappedFleetRoutes = await Promise.all(
          fleetSnapshot.routes.map(async (routeBlueprint) => {
            const waypointPayload: RoadWaypoint[] = routeBlueprint.points.map(
              ([longitude, latitude]) => ({
                latitude,
                longitude,
              }),
            )

            try {
              const routeResponse = await fetch(EMERGENCY_ROUTE_WAYPOINTS_API, {
                method: 'POST',
                headers: {
                  'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                  waypoints: waypointPayload,
                  is_loop: true,
                }),
                signal: controller.signal,
              })

              if (!routeResponse.ok) {
                return routeBlueprint
              }

              const routePayload = (await routeResponse.json()) as RoadRouteApiResponse
              const routePoints: [number, number][] = []

              routePayload.coordinates.forEach((coordinate) => {
                if (!Array.isArray(coordinate) || coordinate.length < 2) {
                  return
                }

                const longitude = toFiniteNumber(coordinate[0])
                const latitude = toFiniteNumber(coordinate[1])

                if (longitude === null || latitude === null) {
                  return
                }

                routePoints.push([longitude, latitude])
              })

              return routePoints.length >= 2
                ? {
                    ...routeBlueprint,
                    points: routePoints,
                  }
                : routeBlueprint
            } catch {
              return routeBlueprint
            }
          }),
        )

        if (controller.signal.aborted || isDisposed) {
          return
        }

        const routes = buildEmergencyRoutes(DEFAULT_CENTER, snappedFleetRoutes, false)
        const runtimes = createEmergencyVehicleRuntimes(routes, fleetSnapshot)
        const timestamp = Date.now()

        emergencyVehicleRoutesRef.current = routes
        emergencyVehicleRuntimeRef.current = runtimes
        emergencyVehicleLastTickRef.current = timestamp

        setEmergencyVehicles(advanceEmergencyVehicleSimulation(runtimes, routes, 0, timestamp))
        syncThreatDispatchStatuses()

        simulationTimerId = window.setInterval(() => {
          const tickTimestamp = Date.now()
          const elapsedSeconds = Math.max(
            0.35,
            Math.min(2.8, (tickTimestamp - emergencyVehicleLastTickRef.current) / 1_000),
          )

          emergencyVehicleLastTickRef.current = tickTimestamp

          setEmergencyVehicles(
            advanceEmergencyVehicleSimulation(
              emergencyVehicleRuntimeRef.current,
              emergencyVehicleRoutesRef.current,
              elapsedSeconds,
              tickTimestamp,
            ),
          )
          syncThreatDispatchStatuses()
        }, EMERGENCY_SIMULATION_UPDATE_MS)
      } catch {
        if (controller.signal.aborted || isDisposed) {
          return
        }

        emergencyVehicleRuntimeRef.current = []
        emergencyVehicleRoutesRef.current = []
        emergencyVehicleLastTickRef.current = 0
        setEmergencyVehicles([])
        setEmergencyVehicleError(
          'Emergency vehicle layer unavailable. Verify emergency-fleet.json IDs and GraphML routing API.',
        )
      }
    }

    void startEmergencySimulation()

    return () => {
      isDisposed = true
      controller.abort()

      if (simulationTimerId !== null) {
        window.clearInterval(simulationTimerId)
      }
    }
  }, [activeLayers.emergencyVehiclePins, hasResolvedInitialCenter, syncThreatDispatchStatuses])

  useEffect(() => {
    if (!viewportBounds || !activeLayers.rotorcraftPins) {
      rotorcraftFetchControllerRef.current?.abort()
      setRotorcraftLoading(false)
      setRotorcraftError('')
      setRotorcraft([])
      setRotorcraftFeedStats(EMPTY_ROTORCRAFT_FEED_STATS)
      return
    }

    rotorcraftFetchControllerRef.current?.abort()

    const controller = new AbortController()
    rotorcraftFetchControllerRef.current = controller

    const syncRotorcraftFeed = async () => {
      setRotorcraftLoading(true)
      setRotorcraftError('')

      try {
        await hydrateAircraftCache()

        if (controller.signal.aborted) {
          return
        }

        const cachedSnapshot = aircraftCacheSnapshotRef.current

        if (cachedSnapshot && shouldReuseAircraftCacheSnapshot(cachedSnapshot, viewportBounds)) {
          setRotorcraftFeedStats(cachedSnapshot.feedStats)
          setRotorcraft(cachedSnapshot.aircraft)

          if (!cachedSnapshot.aircraft.length) {
            setRotorcraftError(formatEmptyAircraftMessage(cachedSnapshot.feedStats))
          }

          return
        }

        const requestUrl = buildOpenSkyStatesUrl(viewportBounds)
        const response = await fetch(requestUrl, {
          signal: controller.signal,
          headers: {
            accept: 'application/json',
          },
        })

        if (!response.ok) {
          throw new Error('OpenSky aircraft request failed.')
        }

        const payload = (await response.json()) as unknown
        const parsedRotorcraft = parseOpenSkyAircraft(payload)
        const cacheSnapshot: AircraftCacheSnapshot = {
          fetchedAt: Date.now(),
          bounds: viewportBounds,
          feedStats: parsedRotorcraft.feedStats,
          aircraft: parsedRotorcraft.rotorcraft,
        }

        if (!controller.signal.aborted) {
          aircraftCacheSnapshotRef.current = cacheSnapshot
          persistAircraftCache(cacheSnapshot)
          setRotorcraftFeedStats(parsedRotorcraft.feedStats)
          setRotorcraft(parsedRotorcraft.rotorcraft)

          if (!parsedRotorcraft.rotorcraft.length) {
            setRotorcraftError(formatEmptyAircraftMessage(parsedRotorcraft.feedStats))
          }
        }
      } catch {
        if (!controller.signal.aborted) {
          const fallbackSnapshot = aircraftCacheSnapshotRef.current

          if (fallbackSnapshot && isAircraftCacheFresh(fallbackSnapshot)) {
            setRotorcraftFeedStats(fallbackSnapshot.feedStats)
            setRotorcraft(fallbackSnapshot.aircraft)
            setRotorcraftError('OpenSky unavailable. Showing recent cached aircraft feed.')
          } else {
            setRotorcraft([])
            setRotorcraftFeedStats(EMPTY_ROTORCRAFT_FEED_STATS)
            setRotorcraftError('OpenSky layer unavailable. Unable to load aircraft feed.')
          }
        }
      } finally {
        if (!controller.signal.aborted) {
          setRotorcraftLoading(false)
        }
      }
    }

    void syncRotorcraftFeed()

    return () => {
      controller.abort()
    }
  }, [
    viewportBounds,
    activeLayers.rotorcraftPins,
    hydrateAircraftCache,
    persistAircraftCache,
  ])

  useEffect(() => {
    const map = mapRef.current

    if (!map) {
      return
    }

    const applyTrafficFlowLayer = () => {
      if (!map.isStyleLoaded()) {
        return
      }

      if (!activeLayers.trafficFlow) {
        if (map.getLayer(TOMTOM_TRAFFIC_LAYER_ID)) {
          map.setLayoutProperty(TOMTOM_TRAFFIC_LAYER_ID, 'visibility', 'none')
        }

        return
      }

      ensureTrafficFlowLayer(map)

      if (map.getLayer(TOMTOM_TRAFFIC_LAYER_ID)) {
        map.setLayoutProperty(TOMTOM_TRAFFIC_LAYER_ID, 'visibility', 'visible')
      }
    }

    applyTrafficFlowLayer()
    map.on('load', applyTrafficFlowLayer)

    return () => {
      map.off('load', applyTrafficFlowLayer)
    }
  }, [activeLayers.trafficFlow])

  useEffect(() => {
    const map = mapRef.current

    if (!map) {
      return
    }

    const applyDispatchRouteLayer = () => {
      if (!map.isStyleLoaded()) {
        return
      }

      ensureDispatchRouteLayer(map)

      const vehicle = emergencyVehicles.find((v) => v.key === selectedEmergencyVehicleKey)

      if (vehicle && vehicle.status === 'responding' && vehicle.dispatchRoutePoints) {
        const source = map.getSource(DISPATCH_ROUTE_SOURCE_ID) as 
          | { setData: (data: any) => void } 
          | undefined
        if (source) {
          source.setData({
            type: 'FeatureCollection',
            features: [
              {
                type: 'Feature',
                properties: {},
                geometry: {
                  type: 'LineString',
                  coordinates: vehicle.dispatchRoutePoints,
                },
              },
            ],
          })
        }
        
        if (map.getLayer(DISPATCH_ROUTE_LAYER_ID)) {
          map.setLayoutProperty(DISPATCH_ROUTE_LAYER_ID, 'visibility', 'visible')
        }
      } else {
        const source = map.getSource(DISPATCH_ROUTE_SOURCE_ID) as 
          | { setData: (data: any) => void } 
          | undefined
        if (source) {
          source.setData(EMPTY_DISPATCH_ROUTE_FEATURE_COLLECTION)
        }
        if (map.getLayer(DISPATCH_ROUTE_LAYER_ID)) {
          map.setLayoutProperty(DISPATCH_ROUTE_LAYER_ID, 'visibility', 'none')
        }
      }
    }

    applyDispatchRouteLayer()
    map.on('load', applyDispatchRouteLayer)

    return () => {
      map.off('load', applyDispatchRouteLayer)
    }
  }, [emergencyVehicles, selectedEmergencyVehicleKey])

  useEffect(() => {
    if (!viewportBounds || !activeLayers.riskHeatmap) {
      riskHeatmapFetchControllerRef.current?.abort()
      setRiskHeatmapCollection(EMPTY_RISK_HEATMAP_FEATURE_COLLECTION)
      setRiskHeatmapFallbackCount(0)
      return
    }

    riskHeatmapFetchControllerRef.current?.abort()

    const controller = new AbortController()
    riskHeatmapFetchControllerRef.current = controller

    const congregationPoints = congregationPlaces.map(
      (place) => [place.longitude, place.latitude] as [number, number],
    )
    const levelOneIncidentPoints = incidents
      .filter((incident) => incident.level > 0)
      .map((incident) => [incident.longitude, incident.latitude] as [number, number])
    const levelZeroIncidentPoints = incidents
      .filter((incident) => incident.level === 0)
      .map((incident) => [incident.longitude, incident.latitude] as [number, number])
    const cctvPoints = cctvCameras.map((camera) => [camera.longitude, camera.latitude] as [number, number])
    const emergencyPoints = emergencyVehicles.map(
      (vehicle) => [vehicle.longitude, vehicle.latitude] as [number, number],
    )

    const fetchTrafficCongestionScore = async (latitude: number, longitude: number) => {
      const cacheKey = buildRiskHeatmapTrafficSampleKey(latitude, longitude)
      const cachedSample = trafficSampleCacheRef.current.get(cacheKey)

      if (cachedSample && Date.now() - cachedSample.fetchedAt <= RISK_HEATMAP_TRAFFIC_TTL_MS) {
        return cachedSample.congestionScore
      }

      try {
        const response = await fetch(buildTomTomFlowSegmentUrl(latitude, longitude), {
          cache: 'no-store',
          signal: controller.signal,
        })

        if (!response.ok) {
          throw new Error('Traffic segment request failed.')
        }

        const payload = (await response.json()) as TomTomFlowSegmentResponse
        const segmentData = payload.flowSegmentData
        const currentSpeed = toFiniteNumber(segmentData?.currentSpeed)
        const freeFlowSpeed = toFiniteNumber(segmentData?.freeFlowSpeed)
        const confidence = clampUnitInterval(toFiniteNumber(segmentData?.confidence) ?? 1)
        const roadClosure = Boolean(segmentData?.roadClosure)
        let congestionScore: number | null = null

        if (roadClosure) {
          congestionScore = 1
        } else if (currentSpeed !== null && freeFlowSpeed !== null && freeFlowSpeed > 0) {
          const baselineCongestion = clampUnitInterval(1 - currentSpeed / freeFlowSpeed)
          congestionScore = clampUnitInterval(baselineCongestion * (0.55 + confidence * 0.45))
        }

        trafficSampleCacheRef.current.set(cacheKey, {
          fetchedAt: Date.now(),
          congestionScore,
        })
        trimTrafficSampleCache(trafficSampleCacheRef.current)

        return congestionScore
      } catch {
        if (controller.signal.aborted) {
          return null
        }

        trafficSampleCacheRef.current.set(cacheKey, {
          fetchedAt: Date.now(),
          congestionScore: null,
        })
        trimTrafficSampleCache(trafficSampleCacheRef.current)

        return null
      }
    }

    const syncRiskHeatmap = async () => {
      try {
        const gridCells = buildRiskGridCells(viewportBounds)
        const scoredCells = await Promise.all(
          gridCells.map(async (cell) => {
            const cellCenter: [number, number] = [cell.centerLongitude, cell.centerLatitude]
            const congregationSignal = calculateProximitySignal(
              cellCenter,
              congregationPoints,
              RISK_HEATMAP_PROXIMITY_RADIUS_METERS,
              2.2,
            )
            const levelOneIncidentSignal = calculateProximitySignal(
              cellCenter,
              levelOneIncidentPoints,
              RISK_HEATMAP_PROXIMITY_RADIUS_METERS,
              1.45,
            )
            const levelZeroIncidentSignal = calculateProximitySignal(
              cellCenter,
              levelZeroIncidentPoints,
              RISK_HEATMAP_PROXIMITY_RADIUS_METERS,
              1.9,
            )
            const cctvSignal = calculateProximitySignal(
              cellCenter,
              cctvPoints,
              RISK_HEATMAP_PROXIMITY_RADIUS_METERS,
              1.7,
            )
            const emergencySignal = calculateProximitySignal(
              cellCenter,
              emergencyPoints,
              RISK_HEATMAP_PROXIMITY_RADIUS_METERS,
              1.7,
            )

            let trafficHighSignal = await fetchTrafficCongestionScore(
              cell.centerLatitude,
              cell.centerLongitude,
            )
            let usedFallbackTraffic = false

            if (trafficHighSignal === null) {
              usedFallbackTraffic = true
              trafficHighSignal = estimateFallbackTrafficCongestion(
                congregationSignal,
                levelOneIncidentSignal,
                levelZeroIncidentSignal,
              )
            }

            const trafficLowSignal = 1 - trafficHighSignal
            const highRiskSignal = congregationSignal * levelOneIncidentSignal * trafficHighSignal
            const safetySignal = emergencySignal * cctvSignal * trafficLowSignal
            const dangerScore =
              levelOneIncidentSignal * 0.35 +
              levelZeroIncidentSignal * 0.12 +
              congregationSignal * 0.24 +
              trafficHighSignal * 0.21 +
              highRiskSignal * 0.36
            const mitigationScore =
              emergencySignal * 0.29 +
              cctvSignal * 0.22 +
              trafficLowSignal * 0.16 +
              safetySignal * 0.36
            const riskScore = clampUnitInterval(0.37 + dangerScore - mitigationScore)
            const riskLevel = resolveRiskHeatmapLevel(riskScore)

            return {
              usedFallbackTraffic,
              feature: {
                type: 'Feature' as const,
                geometry: {
                  type: 'Polygon' as const,
                  coordinates: [
                    [
                      [cell.west, cell.south],
                      [cell.east, cell.south],
                      [cell.east, cell.north],
                      [cell.west, cell.north],
                      [cell.west, cell.south],
                    ],
                  ],
                },
                properties: {
                  riskScore: Number(riskScore.toFixed(4)),
                  riskLevel,
                  trafficScore: Number(trafficHighSignal.toFixed(4)),
                  highRiskSignal: Number(highRiskSignal.toFixed(4)),
                  safetySignal: Number(safetySignal.toFixed(4)),
                },
              },
            }
          }),
        )

        if (controller.signal.aborted) {
          return
        }

        setRiskHeatmapCollection({
          type: 'FeatureCollection',
          features: scoredCells.map((cell) => cell.feature),
        })
        setRiskHeatmapFallbackCount(scoredCells.filter((cell) => cell.usedFallbackTraffic).length)
      } catch {
        if (!controller.signal.aborted) {
          setRiskHeatmapCollection(EMPTY_RISK_HEATMAP_FEATURE_COLLECTION)
          setRiskHeatmapFallbackCount(0)
        }
      }
    }

    void syncRiskHeatmap()

    return () => {
      controller.abort()
    }
  }, [
    viewportBounds,
    activeLayers.riskHeatmap,
    incidents,
    congregationPlaces,
    cctvCameras,
    emergencyVehicles,
  ])

  useEffect(() => {
    const map = mapRef.current

    if (!map) {
      return
    }

    const applyRiskHeatmapLayer = () => {
      if (!map.isStyleLoaded()) {
        return
      }

      ensureRiskHeatmapLayer(map)

      const visibility = activeLayers.riskHeatmap ? 'visible' : 'none'

      if (map.getLayer(RISK_HEATMAP_FILL_LAYER_ID)) {
        map.setLayoutProperty(RISK_HEATMAP_FILL_LAYER_ID, 'visibility', visibility)
      }

      if (map.getLayer(RISK_HEATMAP_OUTLINE_LAYER_ID)) {
        map.setLayoutProperty(RISK_HEATMAP_OUTLINE_LAYER_ID, 'visibility', visibility)
      }

      const riskHeatmapSource = map.getSource(RISK_HEATMAP_SOURCE_ID) as
        | { setData: (data: RiskHeatmapFeatureCollection) => void }
        | undefined

      riskHeatmapSource?.setData(riskHeatmapCollection)
    }

    applyRiskHeatmapLayer()
    map.on('load', applyRiskHeatmapLayer)

    return () => {
      map.off('load', applyRiskHeatmapLayer)
    }
  }, [activeLayers.riskHeatmap, riskHeatmapCollection])

  useEffect(() => {
    setSelectedUnit((current) => {
      if (!current) {
        return current
      }

      if (current.kind === 'incident') {
        return incidents.some((incident) => incident.key === current.key) ? current : null
      }

      if (current.kind === 'congregation') {
        return congregationPlaces.some((place) => place.key === current.key) ? current : null
      }

      if (current.kind === 'cctv') {
        return cctvCameras.some((camera) => camera.key === current.key) ? current : null
      }

      if (current.kind === 'emergencyVehicle') {
        return emergencyVehicles.some((vehicle) => vehicle.key === current.key) ? current : null
      }

      if (current.kind === 'aircraft') {
        return rotorcraft.some((aircraft) => aircraft.key === current.key) ? current : null
      }

      return current
    })
  }, [incidents, congregationPlaces, cctvCameras, emergencyVehicles, rotorcraft])

  useEffect(() => {
    cctvHlsRef.current?.destroy()
    cctvHlsRef.current = null

    const videoElement = cctvVideoRef.current

    if (!selectedCctv || selectedCctv.streamType !== 'hls' || !videoElement) {
      setCctvPlaybackError('')

      if (videoElement) {
        videoElement.pause()
        videoElement.removeAttribute('src')
        videoElement.load()
      }

      return
    }

    setCctvPlaybackError('')

    if (videoElement.canPlayType('application/vnd.apple.mpegurl')) {
      videoElement.src = selectedCctv.streamUrl
      void videoElement.play().catch(() => {
        setCctvPlaybackError('Live stream loaded. Press play if autoplay is blocked.')
      })

      return () => {
        videoElement.pause()
        videoElement.removeAttribute('src')
        videoElement.load()
      }
    }

    if (!Hls.isSupported()) {
      setCctvPlaybackError('This browser cannot play HLS streams.')
      return
    }

    const hls = new Hls({
      enableWorker: true,
      lowLatencyMode: true,
    })
    cctvHlsRef.current = hls

    hls.attachMedia(videoElement)
    hls.on(Hls.Events.MEDIA_ATTACHED, () => {
      hls.loadSource(selectedCctv.streamUrl)
    })
    hls.on(Hls.Events.ERROR, (_event, data) => {
      if (data.fatal) {
        setCctvPlaybackError('Unable to play this live stream in the browser.')
      }
    })

    return () => {
      hls.destroy()
      cctvHlsRef.current = null
      videoElement.pause()
      videoElement.removeAttribute('src')
      videoElement.load()
    }
  }, [selectedCctv])

  useEffect(() => {
    if (!activeLayers.incidentPins || !incidents.length || isUnitWidgetDismissed) {
      return
    }

    setSelectedUnit((current) => {
      if (current) {
        return current
      }

      return {
        kind: 'incident',
        key: incidents[0].key,
      }
    })
  }, [incidents, activeLayers.incidentPins, isUnitWidgetDismissed])

  useEffect(() => {
    incidentMarkersRef.current.forEach((marker) => marker.remove())
    incidentMarkersRef.current = []

    if (!mapRef.current || !activeLayers.incidentPins) {
      return
    }

    const markers = incidents.map((incident) => {
      const markerElement = document.createElement('button')
      markerElement.type = 'button'
      markerElement.className = `tactical-pin tactical-pin--${incident.severity}${
        incident.key === selectedIncidentKey ? ' tactical-pin--active' : ''
      }`
      markerElement.title = incident.title

      markerElement.addEventListener('click', (event) => {
        event.stopPropagation()
        openIncident(incident.key)
      })

      return new maptilersdk.Marker({ element: markerElement, anchor: 'center' })
        .setLngLat([incident.longitude, incident.latitude])
        .addTo(mapRef.current as maptilersdk.Map)
    })

    incidentMarkersRef.current = markers

    return () => {
      markers.forEach((marker) => marker.remove())
    }
  }, [incidents, selectedIncidentKey, activeLayers.incidentPins, openIncident])

  useEffect(() => {
    congregationMarkersRef.current.forEach((marker) => marker.remove())
    congregationMarkersRef.current = []

    if (!mapRef.current || !activeLayers.congregationPins || !hasEnabledCongregationSubLayers) {
      return
    }

    const markers = congregationPlaces.map((place) => {
      const markerElement = document.createElement('button')
      markerElement.type = 'button'
      markerElement.className = `congregation-pin congregation-pin--${place.pinTheme}${
        place.key === selectedCongregationKey ? ' congregation-pin--active' : ''
      }`
      markerElement.title = `${place.name} (${place.categoryLabel})`
      markerElement.setAttribute('aria-label', `${place.categoryLabel}: ${place.name}`)

      const iconElement = document.createElement('span')
      iconElement.className = 'congregation-pin__icon'
      iconElement.textContent = place.icon
      markerElement.append(iconElement)

      markerElement.addEventListener('click', (event) => {
        event.stopPropagation()
        openCongregation(place.key)
      })

      const popupRoot = document.createElement('div')
      popupRoot.className = 'congregation-popup'

      const popupTitle = document.createElement('strong')
      popupTitle.textContent = place.name
      popupRoot.append(popupTitle)

      const popupMeta = document.createElement('span')
      popupMeta.textContent =
        place.category === 'worship'
          ? place.denominationRaw
            ? `${place.categoryLabel} � ${place.religionLabel} � ${place.denominationRaw}`
            : `${place.categoryLabel} � ${place.religionRaw}`
          : place.categoryLabel
      popupRoot.append(popupMeta)

      const popup = new maptilersdk.Popup({
        closeButton: false,
        offset: 14,
      }).setDOMContent(popupRoot)

      return new maptilersdk.Marker({
        element: markerElement,
        anchor: 'center',
      })
        .setLngLat([place.longitude, place.latitude])
        .setPopup(popup)
        .addTo(mapRef.current as maptilersdk.Map)
    })

    congregationMarkersRef.current = markers

    return () => {
      markers.forEach((marker) => marker.remove())
    }
  }, [
    congregationPlaces,
    activeLayers.congregationPins,
    hasEnabledCongregationSubLayers,
    selectedCongregationKey,
    openCongregation,
  ])

  useEffect(() => {
    cctvMarkersRef.current.forEach((marker) => marker.remove())
    cctvMarkersRef.current = []

    if (!mapRef.current || !activeLayers.cctvPins) {
      return
    }

    const markers = cctvCameras.map((camera) => {
      const markerElement = document.createElement('button')
      markerElement.type = 'button'
      markerElement.className = `cctv-pin${camera.key === selectedCctvKey ? ' cctv-pin--active' : ''}`
      markerElement.title = `${camera.name} (${camera.streamLabel})`
      markerElement.setAttribute('aria-label', `CCTV camera ${camera.name}`)

      const iconElement = document.createElement('span')
      iconElement.className = 'cctv-pin__icon'
      iconElement.textContent = '??'
      markerElement.append(iconElement)

      markerElement.addEventListener('click', (event) => {
        event.stopPropagation()
        openCctv(camera.key)
      })

      const popupRoot = document.createElement('div')
      popupRoot.className = 'cctv-popup'

      const popupTitle = document.createElement('strong')
      popupTitle.textContent = camera.name
      popupRoot.append(popupTitle)

      const popupMeta = document.createElement('span')
      popupMeta.textContent = camera.streamLabel
      popupRoot.append(popupMeta)

      const popup = new maptilersdk.Popup({
        closeButton: false,
        offset: 14,
      }).setDOMContent(popupRoot)

      return new maptilersdk.Marker({
        element: markerElement,
        anchor: 'center',
      })
        .setLngLat([camera.longitude, camera.latitude])
        .setPopup(popup)
        .addTo(mapRef.current as maptilersdk.Map)
    })

    cctvMarkersRef.current = markers

    return () => {
      markers.forEach((marker) => marker.remove())
    }
  }, [cctvCameras, activeLayers.cctvPins, selectedCctvKey, openCctv])

  useEffect(() => {
    emergencyVehicleMarkersRef.current.forEach((marker) => marker.remove())
    emergencyVehicleMarkersRef.current = []

    if (!mapRef.current || !activeLayers.emergencyVehiclePins) {
      return
    }

    const markers = emergencyVehicles.map((vehicle) => {
      const markerElement = document.createElement('button')
      markerElement.type = 'button'
      markerElement.className = `emergency-vehicle-pin emergency-vehicle-pin--${vehicle.vehicleType}${
        vehicle.key === selectedEmergencyVehicleKey ? ' emergency-vehicle-pin--active' : ''
      }${vehicle.status === 'responding' ? ' emergency-vehicle-pin--flashing' : ''}`
      markerElement.title = `${vehicle.unitCode} � ${vehicle.vehicleLabel}`
      markerElement.setAttribute('aria-label', `${vehicle.vehicleLabel} ${vehicle.unitCode}`)

      const iconElement = document.createElement('span')
      iconElement.className = 'emergency-vehicle-pin__icon'
      iconElement.textContent = vehicle.icon
      markerElement.append(iconElement)

      markerElement.addEventListener('click', (event) => {
        event.stopPropagation()
        openEmergencyVehicle(vehicle.key)
      })

      const popupRoot = document.createElement('div')
      popupRoot.className = 'emergency-vehicle-popup'

      const popupTitle = document.createElement('strong')
      popupTitle.textContent = `${vehicle.unitCode} � ${vehicle.vehicleLabel}`
      popupRoot.append(popupTitle)

      const popupStatus = document.createElement('span')
      popupStatus.textContent = `${EMERGENCY_STATUS_LABEL_BY_CODE[vehicle.status]} � ETA ${vehicle.etaMinutes} MIN`
      popupRoot.append(popupStatus)

      const popupRoute = document.createElement('span')
      popupRoute.textContent = `${vehicle.routeLabel} � HDG ${Math.round(vehicle.headingDegrees)}�`
      popupRoot.append(popupRoute)

      const popupAssignment = document.createElement('span')
      popupAssignment.textContent = vehicle.assignment
      popupRoot.append(popupAssignment)

      const popup = new maptilersdk.Popup({
        closeButton: false,
        offset: 14,
      }).setDOMContent(popupRoot)

      return new maptilersdk.Marker({
        element: markerElement,
        anchor: 'center',
      })
        .setLngLat([vehicle.longitude, vehicle.latitude])
        .setPopup(popup)
        .addTo(mapRef.current as maptilersdk.Map)
    })

    emergencyVehicleMarkersRef.current = markers

    return () => {
      markers.forEach((marker) => marker.remove())
    }
  }, [
    emergencyVehicles,
    activeLayers.emergencyVehiclePins,
    selectedEmergencyVehicleKey,
    openEmergencyVehicle,
  ])

  useEffect(() => {
    rotorcraftMarkersRef.current.forEach((marker) => marker.remove())
    rotorcraftMarkersRef.current = []

    if (!mapRef.current || !activeLayers.rotorcraftPins) {
      return
    }

    const markers = rotorcraft.map((aircraft) => {
      const markerElement = document.createElement('button')
      markerElement.type = 'button'
      markerElement.className = `rotorcraft-pin rotorcraft-pin--${aircraft.pinTheme}${
        aircraft.key === selectedAircraftKey ? ' rotorcraft-pin--active' : ''
      }`
      markerElement.title = `${aircraft.callsign} (${aircraft.icao24.toUpperCase()}) � ${aircraft.categoryLabel}`
      markerElement.setAttribute('aria-label', `Aircraft ${aircraft.callsign}`)

      const iconElement = document.createElement('span')
      iconElement.className = 'rotorcraft-pin__icon'
      iconElement.textContent = aircraft.icon
      markerElement.append(iconElement)

      markerElement.addEventListener('click', (event) => {
        event.stopPropagation()
        openAircraft(aircraft.key)
      })

      const popupRoot = document.createElement('div')
      popupRoot.className = 'rotorcraft-popup'

      const popupTitle = document.createElement('strong')
      popupTitle.textContent = aircraft.callsign
      popupRoot.append(popupTitle)

      const popupIdentity = document.createElement('span')
      popupIdentity.textContent = `${aircraft.icao24.toUpperCase()} � ${aircraft.originCountry}`
      popupRoot.append(popupIdentity)

      const popupCategory = document.createElement('span')
      popupCategory.textContent =
        aircraft.category === null
          ? aircraft.categoryLabel
          : `${aircraft.categoryLabel} � CATEGORY ${aircraft.category}`
      popupRoot.append(popupCategory)

      const popupFlight = document.createElement('span')
      popupFlight.textContent = `${formatAircraftSpeed(aircraft.speedKnots)} � ${formatAircraftHeading(
        aircraft.trueTrack,
      )}`
      popupRoot.append(popupFlight)

      const popupAltitude = document.createElement('span')
      popupAltitude.textContent = `${formatAircraftAltitude(aircraft.altitudeMeters)} � ${
        aircraft.onGround ? 'ON GROUND' : 'AIRBORNE'
      }`
      popupRoot.append(popupAltitude)

      const popupLastContact = document.createElement('span')
      popupLastContact.textContent =
        aircraft.lastContact === null
          ? 'LAST CONTACT N/A'
          : `LAST CONTACT ${formatUpdateAge(aircraft.lastContact * 1_000)}`
      popupRoot.append(popupLastContact)

      const popup = new maptilersdk.Popup({
        closeButton: false,
        offset: 14,
      }).setDOMContent(popupRoot)

      return new maptilersdk.Marker({
        element: markerElement,
        anchor: 'center',
      })
        .setLngLat([aircraft.longitude, aircraft.latitude])
        .setPopup(popup)
        .addTo(mapRef.current as maptilersdk.Map)
    })

    rotorcraftMarkersRef.current = markers

    return () => {
      markers.forEach((marker) => marker.remove())
    }
  }, [rotorcraft, activeLayers.rotorcraftPins, selectedAircraftKey, openAircraft])

  useEffect(() => {
    return () => {
      incidentFetchControllerRef.current?.abort()
      congregationFetchControllerRef.current?.abort()
      cctvFetchControllerRef.current?.abort()
      rotorcraftFetchControllerRef.current?.abort()
      riskHeatmapFetchControllerRef.current?.abort()
      cctvHlsRef.current?.destroy()
      cctvHlsRef.current = null
    }
  }, [])

  const toggleLayer = (layer: keyof LayerState) => {
    setActiveLayers((current) => ({
      ...current,
      [layer]: !current[layer],
    }))
  }

  const toggleCongregationSubLayer = (layer: CongregationSubLayerKey) => {
    setCongregationSubLayers((current) => ({
      ...current,
      [layer]: !current[layer],
    }))
  }

  const handleZoomIn = () => {
    mapRef.current?.zoomIn({ duration: 200 })
  }

  const handleZoomOut = () => {
    mapRef.current?.zoomOut({ duration: 200 })
  }

  const handleResetView = () => {
    mapRef.current?.flyTo({
      center: initialCenter,
      zoom: DEFAULT_ZOOM,
      pitch: DEFAULT_PITCH,
      bearing: DEFAULT_BEARING,
      essential: true,
      duration: 700,
    })
  }

  const focusIncidentFromPanel = useCallback(
    (incident: IncidentUnit) => {
      openIncident(incident.key)

      const map = mapRef.current

      if (!map) {
        return
      }

      const panelOffsetX = isIncidentsPanelOpen
        ? Math.round(Math.min(180, map.getContainer().clientWidth * 0.16))
        : 0

      map.flyTo({
        center: [incident.longitude, incident.latitude],
        offset: [-panelOffsetX, 0],
        essential: true,
        duration: 650,
      })
    },
    [isIncidentsPanelOpen, openIncident],
  )

  const shouldRenderUnitWidget =
    !isUnitWidgetDismissed &&
    Boolean(
      (selectedIncident && activeLayers.incidentPins) ||
        (selectedCongregation && activeLayers.congregationPins) ||
        (selectedCctv && activeLayers.cctvPins) ||
        (selectedEmergencyVehicle && activeLayers.emergencyVehiclePins) ||
        (selectedAircraft && activeLayers.rotorcraftPins),
    )

  return (
    <div className="terminal-shell">
      <aside
        className={`terminal-sidebar${isCommandSidebarOpen ? '' : ' terminal-sidebar--collapsed'}`}
      >
        <button
          type="button"
          className="terminal-sidebar__toggle"
          onClick={() => setIsCommandSidebarOpen((current) => !current)}
          aria-label={
            isCommandSidebarOpen ? 'Collapse command layers sidebar' : 'Expand command layers sidebar'
          }
        >
          {isCommandSidebarOpen ? '<' : '>'}
        </button>

        {isCommandSidebarOpen ? (
          <div className="terminal-sidebar__panel">
          <div className="panel-heading">
            <p>Command Layers</p>
            <h1>Unit Tracking</h1>
          </div>
          <p className="panel-subtext">Toggle live overlays and tactical instrumentation.</p>

          <div className="section-heading" onClick={() => setIsLayersSectionOpen(o => !o)} style={{ cursor: "pointer", marginTop: "1rem", borderBottom: "1px solid #333", paddingBottom: "4px" }}>
            <h2 style={{ fontSize: "14px", margin: 0, textTransform: "uppercase", letterSpacing: "0.05em", color: "#ccc" }}>Layers {isLayersSectionOpen ? "[-]" : "[+]"}</h2>
          </div>

          {isLayersSectionOpen && ( <>
            <div className="layer-stack" style={{ marginTop: "0.5rem" }}>
              <label className="layer-toggle">
              <input
                type="checkbox"
                checked={activeLayers.incidentPins}
                onChange={() => toggleLayer('incidentPins')}
              />
              <span className="layer-toggle__content">
                <strong>Incident Pins</strong>
                <small>Unit markers from field telemetry</small>
              </span>
            </label>

            <label className="layer-toggle">
              <input
                type="checkbox"
                checked={activeLayers.congregationPins}
                onChange={() => toggleLayer('congregationPins')}
              />
              <span className="layer-toggle__content">
                <strong>Congregation Areas</strong>
                <small>Parent layer for high-congregation OpenStreetMap sublayers</small>
              </span>
            </label>

            {activeLayers.congregationPins ? (
              <div className="layer-sublayer-stack">
                {CONGREGATION_SUBLAYER_KEYS.map((layerKey) => (
                  <label key={layerKey} className="layer-toggle layer-toggle--sub">
                    <input
                      type="checkbox"
                      checked={congregationSubLayers[layerKey]}
                      onChange={() => toggleCongregationSubLayer(layerKey)}
                    />
                    <span className="layer-toggle__content">
                      <strong>{CONGREGATION_SUBLAYER_LABELS[layerKey]}</strong>
                      <small>
                        {layerKey === 'worship'
                          ? 'amenity=place_of_worship'
                          : layerKey === 'school'
                            ? 'amenity=school'
                            : layerKey === 'hospital'
                              ? 'amenity=hospital'
                              : layerKey === 'stadium'
                                ? 'leisure=stadium'
                                : 'leisure=arena'}
                      </small>
                    </span>
                  </label>
                ))}
              </div>
            ) : null}

            <label className="layer-toggle">
              <input
                type="checkbox"
                checked={activeLayers.cctvPins}
                onChange={() => toggleLayer('cctvPins')}
              />
              <span className="layer-toggle__content">
                <strong>CCTV Cameras</strong>
                <small>Static camera pins from cams.json with live embedded streams</small>
              </span>
            </label>

            <label className="layer-toggle">
              <input
                type="checkbox"
                checked={activeLayers.emergencyVehiclePins}
                onChange={() => toggleLayer('emergencyVehiclePins')}
              />
              <span className="layer-toggle__content">
                <strong>Emergency Vehicles</strong>
                <small>Simulated police, EMS, and fire geolocation feed moving along road corridors</small>
              </span>
            </label>

            <label className="layer-toggle">
              <input
                type="checkbox"
                checked={activeLayers.rotorcraftPins}
                onChange={() => toggleLayer('rotorcraftPins')}
              />
              <span className="layer-toggle__content">
                <strong>Aircraft</strong>
                <small>OpenSky Network aircraft with category-based icons</small>
              </span>
            </label>

            <label className="layer-toggle">
              <input
                type="checkbox"
                checked={activeLayers.trafficFlow}
                onChange={() => toggleLayer('trafficFlow')}
              />
              <span className="layer-toggle__content">
                <strong>Traffic Flow</strong>
                <small>TomTom raster flow tiles showing road congestion</small>
              </span>
            </label>

            <label className="layer-toggle">
              <input
                type="checkbox"
                checked={activeLayers.riskHeatmap}
                onChange={() => toggleLayer('riskHeatmap')}
              />
              <span className="layer-toggle__content">
                <strong>Risk Heatmap</strong>
                <small>Grid-scored risk model, green safer and red higher risk</small>
              </span>
            </label>

            <label className="layer-toggle">
              <input
                type="checkbox"
                checked={activeLayers.tacticalGrid}
                onChange={() => toggleLayer('tacticalGrid')}
              />
              <span className="layer-toggle__content">
                <strong>Tactical Grid</strong>
                <small>Reference grid for district alignment</small>
              </span>
            </label>

            <label className="layer-toggle">
              <input
                type="checkbox"
                checked={activeLayers.scanLines}
                onChange={() => toggleLayer('scanLines')}
              />
              <span className="layer-toggle__content">
                <strong>Scan Lines</strong>
                <small>Terminal scan texture overlay</small>
              </span>
            </label>
          </div>

          <div className="pin-legend">
            <span className="legend-label">Pin Legend</span>
            <div>
              <span className="legend-dot legend-dot--yellow" />
              Level 0
            </div>
            <div>
              <span className="legend-dot legend-dot--red" />
              Level 1+
            </div>

            <span className="legend-label">CCTV Layer</span>
            <div className="pin-legend__religion">
              <span className="legend-glyph">??</span>
              Camera feed (YouTube, m3u8, or embed URL)
            </div>

            <span className="legend-label">Emergency Layer</span>
            <div className="pin-legend__religion">
              <span className="legend-glyph">??</span>
              Police patrol car
            </div>
            <div className="pin-legend__religion">
              <span className="legend-glyph">??</span>
              Ambulance
            </div>
            <div className="pin-legend__religion">
              <span className="legend-glyph">??</span>
              Firetruck
            </div>

            <span className="legend-label">Congregation Icons</span>
            <div className="pin-legend__religion">
              <span className="legend-glyph">??</span>
              School
            </div>
            <div className="pin-legend__religion">
              <span className="legend-glyph">??</span>
              Stadium
            </div>
            <div className="pin-legend__religion">
              <span className="legend-glyph">??</span>
              Arena
            </div>

            <span className="legend-label">Traffic Overlay</span>
            <div className="pin-legend__religion">Flow raster style: relative0-dark (TomTom)</div>
            <div className="pin-legend__religion">Traffic flow \u00a9 TomTom</div>

            <span className="legend-label">Risk Heatmap</span>
            <div className="risk-legend-bar" />
            <div className="pin-legend__religion">
              Green = safer, yellow = medium risk, red = higher risk.
            </div>
            <div className="pin-legend__religion">
              High risk: congregation + high traffic + level 1+ incident nearby.
            </div>
            <div className="pin-legend__religion">
              Low risk: emergency vehicles + CCTV + low traffic nearby.
            </div>

            <span className="legend-label">Aviation Layer</span>
            <div className="pin-legend__religion">
              <span className="legend-glyph">??</span>
              Rotorcraft
            </div>
            <div className="pin-legend__religion">
              <span className="legend-glyph">?</span>
              Heavy / Large / High-performance
            </div>
            <div className="pin-legend__religion">
              <span className="legend-glyph">??</span>
              Light / Small
            </div>
            <div className="pin-legend__religion">
              <span className="legend-glyph">??</span>
              Glider / Lighter-than-air / Ultralight
            </div>
            <div className="pin-legend__religion">
              <span className="legend-glyph">??</span>
              Unmanned aerial vehicle
            </div>
            <div className="pin-legend__religion">
              <span className="legend-glyph">??</span>
              Surface categories / obstacles
            </div>

            <span className="legend-label">Worship Icons</span>
            <div className="pin-legend__religion">
              <span className="legend-glyph">?</span>
              Christian
            </div>
            <div className="pin-legend__religion">
              <span className="legend-glyph">?</span>
              Muslim
            </div>
            <div className="pin-legend__religion">
              <span className="legend-glyph">?</span>
              Jewish
            </div>
            <div className="pin-legend__religion">
              <span className="legend-glyph">?</span>
              Buddhist
            </div>
            <div className="pin-legend__religion">
              <span className="legend-glyph">?</span>
              Hindu
            </div>
            <div className="pin-legend__religion">
              <span className="legend-glyph">☬</span>
              Sikh
            </div>
          </div>

            </>
          )}

          <div className="section-heading" onClick={() => setIsEmergencySectionOpen(o => !o)} style={{ cursor: "pointer", marginTop: "1rem", borderBottom: "1px solid #333", paddingBottom: "4px" }}>
            <h2 style={{ fontSize: "14px", margin: 0, textTransform: "uppercase", letterSpacing: "0.05em", color: "#ccc" }}>Emergency Vehicles {isEmergencySectionOpen ? "[-]" : "[+]"}</h2>
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

          <div className="panel-metrics">
            <div>
              <span>Total Incidents</span>
              <strong>{incidentLevelSummary.total.toString().padStart(2, '0')}</strong>
            </div>
            <div>
              <span>Level 0</span>
              <strong>{incidentLevelSummary.levelZero.toString().padStart(2, '0')}</strong>
            </div>
            <div>
              <span>Level 1+</span>
              <strong>{incidentLevelSummary.levelOnePlus.toString().padStart(2, '0')}</strong>
            </div>
            <div>
              <span>Risk Grid Cells</span>
              <strong>{riskHeatmapSummary.total.toString().padStart(2, '0')}</strong>
            </div>
            <div>
              <span>High-Risk Cells</span>
              <strong>{riskHeatmapSummary.highRiskCells.toString().padStart(2, '0')}</strong>
            </div>
            <div>
              <span>Medium-Risk Cells</span>
              <strong>{riskHeatmapSummary.mediumRiskCells.toString().padStart(2, '0')}</strong>
            </div>
            <div>
              <span>Low-Risk Cells</span>
              <strong>{riskHeatmapSummary.lowRiskCells.toString().padStart(2, '0')}</strong>
            </div>
            <div>
              <span>Average Risk</span>
              <strong>{`${Math.round(riskHeatmapSummary.averageScore * 100)}%`}</strong>
            </div>
            <div>
              <span>Congregation Pins</span>
              <strong>{congregationSummary.total.toString().padStart(2, '0')}</strong>
            </div>
            <div>
              <span>Types Tracked</span>
              <strong>{congregationSummary.trackedCategories.toString().padStart(2, '0')}</strong>
            </div>
            <div>
              <span>Religions Tracked</span>
              <strong>{congregationSummary.trackedReligions.toString().padStart(2, '0')}</strong>
            </div>
            <div>
              <span>CCTV Cameras</span>
              <strong>{cctvSummary.total.toString().padStart(2, '0')}</strong>
            </div>
            <div>
              <span>Emergency Units</span>
              <strong>{emergencyVehicleSummary.total.toString().padStart(2, '0')}</strong>
            </div>
            <div>
              <span>Police Units</span>
              <strong>{emergencyVehicleSummary.police.toString().padStart(2, '0')}</strong>
            </div>
            <div>
              <span>Ambulances</span>
              <strong>{emergencyVehicleSummary.ambulances.toString().padStart(2, '0')}</strong>
            </div>
            <div>
              <span>Firetrucks</span>
              <strong>{emergencyVehicleSummary.firetrucks.toString().padStart(2, '0')}</strong>
            </div>
            <div>
              <span>Aircraft Tracked</span>
              <strong>{aircraftSummary.total.toString().padStart(2, '0')}</strong>
            </div>
            <div>
              <span>Airborne Aircraft</span>
              <strong>{aircraftSummary.airborne.toString().padStart(2, '0')}</strong>
            </div>
            <div>
              <span>OpenSky States</span>
              <strong>{rotorcraftFeedStats.totalStates.toString().padStart(2, '0')}</strong>
            </div>
            <div>
              <span>Categorized States</span>
              <strong>{rotorcraftFeedStats.statesWithCategory.toString().padStart(2, '0')}</strong>
            </div>
            <div>
              <span>Mappable States</span>
              <strong>{rotorcraftFeedStats.statesWithCoordinates.toString().padStart(2, '0')}</strong>
            </div>
          </div>

          {incidentError ? <p className="panel-warning">{incidentError}</p> : null}
          {activeLayers.congregationPins && !hasEnabledCongregationSubLayers ? (
            <p className="panel-warning">Enable one or more congregation sublayers to display pins.</p>
          ) : null}
          {activeLayers.congregationPins && hasEnabledCongregationSubLayers && congregationLoading ? (
            <p className="panel-warning">Syncing OpenStreetMap congregation areas...</p>
          ) : null}
          {activeLayers.congregationPins && hasEnabledCongregationSubLayers && congregationError ? (
            <p className="panel-warning">{congregationError}</p>
          ) : null}
          {activeLayers.cctvPins && cctvLoading ? (
            <p className="panel-warning">Syncing CCTV camera layer...</p>
          ) : null}
          {activeLayers.cctvPins && cctvError ? <p className="panel-warning">{cctvError}</p> : null}
          {activeLayers.emergencyVehiclePins && emergencyVehicleError ? (
            <p className="panel-warning">{emergencyVehicleError}</p>
          ) : null}
          {activeLayers.emergencyVehiclePins && !emergencyVehicleError ? (
            <p className="panel-warning">
              Local Tempe fleet mode: IDs are loaded from emergency-fleet.json (20 patrol, 6
              ambulances, 11 firetrucks), routes are snapped to tempe_road_graph.graphml road
              geometry, and dispatch actions auto-track active/completed states.
            </p>
          ) : null}
          {activeLayers.riskHeatmap ? (
            <p className="panel-warning">
              Risk heatmap active: each grid cell blends incidents, congregation density, traffic,
              CCTV, and emergency proximity.
            </p>
          ) : null}
          {activeLayers.riskHeatmap && riskHeatmapFallbackCount > 0 ? (
            <p className="panel-warning">
              {`Traffic fallback applied in ${riskHeatmapFallbackCount} cell${
                riskHeatmapFallbackCount === 1 ? '' : 's'
              }; local pin-density estimates replaced missing live traffic samples.`}
            </p>
          ) : null}
          {activeLayers.rotorcraftPins && rotorcraftLoading ? (
            <p className="panel-warning">Syncing OpenSky aircraft feed...</p>
          ) : null}
          {activeLayers.rotorcraftPins && rotorcraftError ? (
            <p className="panel-warning">{rotorcraftError}</p>
          ) : null}
          </div>
        ) : null}
      </aside>

      <main
        className={`terminal-main${
          activeLayers.incidentPins && isIncidentsPanelOpen
            ? ' terminal-main--incident-panel-open'
            : ''
        }`}
      >
        <div ref={mapContainerRef} className="map-canvas" />
        {activeLayers.tacticalGrid ? <div className="tactical-grid-overlay" /> : null}
        {activeLayers.scanLines ? <div className="scanline-overlay" /> : null}

        <div className="map-utility-controls">
          <button type="button" onClick={handleZoomIn} aria-label="Zoom in">
            +
          </button>
          <button type="button" onClick={handleZoomOut} aria-label="Zoom out">
            -
          </button>
          <button type="button" onClick={handleResetView} aria-label="Reset view">
            []
          </button>
        </div>

        {shouldRenderUnitWidget ? (
          <section
            ref={unitWidgetRef}
            className="unit-widget"
            style={{ left: `${unitWidgetPosition.x}px`, top: `${unitWidgetPosition.y}px` }}
          >
            <span className="unit-widget__corner unit-widget__corner--tl" />
            <span className="unit-widget__corner unit-widget__corner--tr" />
            <span className="unit-widget__corner unit-widget__corner--bl" />
            <span className="unit-widget__corner unit-widget__corner--br" />

            <div
              className="unit-widget__header"
              onPointerDown={handleUnitWidgetPointerDown}
              onPointerMove={handleUnitWidgetPointerMove}
              onPointerUp={handleUnitWidgetPointerUp}
              onPointerCancel={handleUnitWidgetPointerUp}
            >
              <div className="unit-widget__identity">
                {selectedIncident ? (
                  <span className={`status-dot status-dot--${selectedIncident.severity}`} />
                ) : selectedAircraft ? (
                  <span className="unit-widget__badge unit-widget__badge--aircraft">Aircraft</span>
                ) : selectedEmergencyVehicle ? (
                  <span className="unit-widget__badge unit-widget__badge--emergency">Emergency</span>
                ) : selectedCctv ? (
                  <span className="unit-widget__badge unit-widget__badge--cctv">CCTV</span>
                ) : (
                  <span className="unit-widget__badge unit-widget__badge--congregation">OSM</span>
                )}
                <span>
                  {selectedIncident
                    ? levelLabel(selectedIncident.level)
                    : selectedAircraft
                      ? `ICAO24 ${selectedAircraft.icao24.toUpperCase()}`
                      : selectedEmergencyVehicle
                        ? `${selectedEmergencyVehicle.unitCode} � ${EMERGENCY_STATUS_LABEL_BY_CODE[selectedEmergencyVehicle.status].toUpperCase()}`
                      : selectedCctv
                        ? `${selectedCctv.streamType.toUpperCase()} STREAM`
                      : `${selectedCongregation?.osmType.toUpperCase()} ${selectedCongregation?.osmId}`}
                </span>
                <h3>
                  {selectedIncident?.title ??
                    selectedAircraft?.callsign ??
                    selectedEmergencyVehicle?.unitCode ??
                    selectedCctv?.name ??
                    selectedCongregation?.name ??
                    'Selected Unit'}
                </h3>
              </div>
              <button
                type="button"
                onPointerDown={(event) => event.stopPropagation()}
                onClick={closeUnitWidget}
                aria-label="Close unit details"
              >
                X
              </button>
            </div>

            {selectedIncident ? <p className="unit-widget__address">{selectedIncident.address}</p> : null}

            {selectedIncident ? (
              <div className="unit-widget__updates">
                {selectedUpdates.length ? (
                  selectedUpdates.map((update, index) => (
                    <p key={`${update.ts}-${index}`}>
                      <span>{formatUpdateAge(update.ts)}</span>
                      {update.text}
                    </p>
                  ))
                ) : (
                  <p className="unit-widget__empty">No telemetry messages available.</p>
                )}
              </div>
            ) : null}

            {selectedAircraft ? (
              <>
                <p className="unit-widget__address">
                  {selectedAircraft.originCountry} � {selectedAircraft.categoryLabel}
                </p>

                <div className="unit-widget__stats">
                  <div>
                    <span>Status</span>
                    <strong>{selectedAircraft.onGround ? 'On Ground' : 'Airborne'}</strong>
                  </div>
                  <div>
                    <span>Speed</span>
                    <strong>{formatAircraftSpeed(selectedAircraft.speedKnots)}</strong>
                  </div>
                  <div>
                    <span>Altitude</span>
                    <strong>{formatAircraftAltitude(selectedAircraft.altitudeMeters)}</strong>
                  </div>
                </div>

                <div className="unit-widget__table-wrap">
                  <table className="unit-widget__table">
                    <thead>
                      <tr>
                        <th>Index</th>
                        <th>Property</th>
                        <th>Type</th>
                        <th>Value</th>
                        <th>Description</th>
                      </tr>
                    </thead>
                    <tbody>
                      {selectedAircraftDetailFields.map((field) => (
                        <tr key={`${selectedAircraft.key}-${field.index}`}>
                          <td>{field.index}</td>
                          <td>{field.property}</td>
                          <td>{field.type}</td>
                          <td>{field.value}</td>
                          <td>{field.description}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </>
            ) : null}

            {selectedEmergencyVehicle ? (
              <>
                <p className="unit-widget__address">
                  {selectedEmergencyVehicle.assignment} � {selectedEmergencyVehicle.district}
                </p>

                <div className="unit-widget__stats">
                  <div>
                    <span>Status</span>
                    <strong>{EMERGENCY_STATUS_LABEL_BY_CODE[selectedEmergencyVehicle.status]}</strong>
                  </div>
                  <div>
                    <span>Speed</span>
                    <strong>{`${Math.round(selectedEmergencyVehicle.speedMph)} MPH`}</strong>
                  </div>
                  <div>
                    <span>ETA</span>
                    <strong>{`${selectedEmergencyVehicle.etaMinutes} MIN`}</strong>
                  </div>
                </div>

                <div className="unit-widget__updates">
                  <p>
                    <span>Coordinates</span>
                    {`${selectedEmergencyVehicle.latitude.toFixed(6)}, ${selectedEmergencyVehicle.longitude.toFixed(6)}`}
                  </p>
                  <p>
                    <span>Route</span>
                    {selectedEmergencyVehicle.routeLabel}
                  </p>
                  <p>
                    <span>Heading</span>
                    {`HDG ${Math.round(selectedEmergencyVehicle.headingDegrees)}�`}
                  </p>
                  <p>
                    <span>Crew</span>
                    {selectedEmergencyVehicle.crew}
                  </p>
                  <p>
                    <span>Fuel</span>
                    {`${selectedEmergencyVehicle.fuelLevelPercent}%`}
                  </p>
                  <p>
                    <span>Updated</span>
                    {formatUpdateAge(selectedEmergencyVehicle.lastUpdate)}
                  </p>
                </div>

                <div className="unit-widget__updates">
                  {selectedEmergencyVehicle.telemetry.map((message, index) => (
                    <p key={`${selectedEmergencyVehicle.key}-telemetry-${index}`}>
                      <span>{index === 0 ? 'Dispatch' : index === 1 ? 'Navigation' : 'Ops'}</span>
                      {message}
                    </p>
                  ))}
                </div>
              </>
            ) : null}

            {selectedCctv ? (
              <>
                <p className="unit-widget__address">{selectedCctv.streamLabel}</p>

                <div className="unit-widget__video-wrap">
                  {selectedCctv.streamType === 'hls' ? (
                    <video
                      ref={cctvVideoRef}
                      className="unit-widget__video"
                      controls
                      autoPlay
                      muted
                      playsInline
                    />
                  ) : (
                    <iframe
                      className="unit-widget__video"
                      src={selectedCctv.streamUrl}
                      title={`${selectedCctv.name} live stream`}
                      loading="lazy"
                      allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share"
                      referrerPolicy="strict-origin-when-cross-origin"
                      allowFullScreen
                    />
                  )}
                </div>

                {cctvPlaybackError ? <p className="unit-widget__empty">{cctvPlaybackError}</p> : null}

                <div className="unit-widget__updates">
                  <p>
                    <span>Coordinates</span>
                    {`${selectedCctv.latitude.toFixed(6)}, ${selectedCctv.longitude.toFixed(6)}`}
                  </p>
                  <p>
                    <span>Source</span>
                    {selectedCctv.streamUrl}
                  </p>
                </div>
              </>
            ) : null}

            {selectedCongregation ? (
              <>
                <p className="unit-widget__address">
                  {selectedCongregation.categoryLabel}
                  {selectedCongregation.religionLabel ? ` � ${selectedCongregation.religionLabel}` : ''}
                </p>

                <div className="unit-widget__updates">
                  <p>
                    <span>OSM</span>
                    {`<${selectedCongregation.osmType} id="${selectedCongregation.osmId}">`}
                  </p>
                  <p>
                    <span>Center</span>
                    {`lat="${selectedCongregation.latitude.toFixed(6)}" lon="${selectedCongregation.longitude.toFixed(6)}"`}
                  </p>
                </div>

                <div className="unit-widget__tag-list">
                  {selectedCongregationTags.length ? (
                    selectedCongregationTags.map(([tagKey, tagValue]) => (
                      <div key={`${selectedCongregation.key}-${tagKey}`}>
                        <span>{`tag k="${tagKey}"`}</span>
                        <strong>{`v="${tagValue}"`}</strong>
                      </div>
                    ))
                  ) : (
                    <p className="unit-widget__empty">No OpenStreetMap tags available.</p>
                  )}
                </div>
              </>
            ) : null}
          </section>
        ) : null}

        <section className={`threat-menu${isThreatMenuOpen ? ' threat-menu--open' : ''}`}>
          <button
            type="button"
            className="threat-menu__toggle"
            onClick={() => setIsThreatMenuOpen((current) => !current)}
            aria-expanded={isThreatMenuOpen}
            aria-controls="threat-menu-panel"
          >
            <span className="threat-menu__toggle-label">Threat Notification</span>
            <strong>{`${riskPrediction.risk_assessment.level.toUpperCase()} RISK`}</strong>
            <span className="threat-menu__toggle-state">
              {isThreatMenuOpen ? 'Collapse' : 'Expand'}
            </span>
          </button>

          {isThreatMenuOpen ? (
            <div className="threat-menu__panel" id="threat-menu-panel">
              <div className="threat-menu__section">
                <h3>{riskPrediction.risk_assessment.location_name}</h3>
                <p className="threat-menu__meta">
                  <span>{riskPrediction.prediction_id}</span>
                  <span>{new Date(riskPrediction.timestamp).toLocaleString()}</span>
                </p>
                {riskPredictionError ? <p className="threat-menu__notice">{riskPredictionError}</p> : null}
                <p className="threat-menu__copy">{riskPrediction.risk_assessment.explanation}</p>
              </div>

              <div className="threat-menu__section">
                <h4>Threat Factors</h4>
                <ul className="threat-menu__list">
                  {riskPrediction.risk_assessment.risk_factors.map((factor) => (
                    <li key={factor}>{factor}</li>
                  ))}
                </ul>
              </div>

              <div className="threat-menu__section">
                <h4>Possible Solutions</h4>
                <div className="threat-menu__copy-grid">
                  <p>
                    <span>Police Dispatch</span>
                    {riskPrediction.mitigation_strategy.police_dispatch.action}
                  </p>
                  <p>
                    <span>Medical Standby</span>
                    {riskPrediction.mitigation_strategy.medical_standby.message}
                  </p>
                  <p>
                    <span>Traffic Control</span>
                    {riskPrediction.mitigation_strategy.traffic_control['re-routing']}
                  </p>
                </div>
                <div className="threat-menu__actions">
                  {threatActionItems.map((actionItem) => {
                    const actionStatus = threatActionStatusById[actionItem.id] ?? 'idle'

                    return (
                      <button
                        key={actionItem.id}
                        type="button"
                        className={`threat-menu__action-button threat-menu__action-button--${actionStatus}`}
                        onClick={() => handleThreatActionPress(actionItem)}
                      >
                        <strong>{actionItem.label}</strong>
                        <span>{actionItem.detail}</span>
                        <em className={`threat-menu__action-status threat-menu__action-status--${actionStatus}`}>
                          {threatActionStatusLabel(actionStatus)}
                        </em>
                      </button>
                    )
                  })}
                </div>
              </div>
            </div>
          ) : null}
        </section>

        {activeLayers.incidentPins ? (
          <section className={`unit-strip${isIncidentsPanelOpen ? '' : ' unit-strip--collapsed'}`}>
            <button
              type="button"
              className="unit-strip__toggle"
              onClick={() => setIsIncidentsPanelOpen((current) => !current)}
              aria-label={isIncidentsPanelOpen ? 'Collapse incidents panel' : 'Expand incidents panel'}
            >
              {isIncidentsPanelOpen ? '>' : '<'}
            </button>

            <div className="unit-strip__header">
              <strong>Incidents</strong>
              <button
                type="button"
                onClick={() => setIsIncidentsPanelOpen(false)}
                aria-label="Close incidents panel"
              >
                X
              </button>
            </div>

            <div className="unit-strip__list">
              {incidents.map((incident) => (
                <button
                  key={incident.key}
                  type="button"
                  className={`unit-row ${
                    incident.key === selectedIncidentKey ? 'unit-row--selected' : ''
                  }`}
                  onClick={() => focusIncidentFromPanel(incident)}
                >
                  <span className={`status-dot status-dot--${incident.severity}`} />
                  <span className="unit-row__status">{levelLabel(incident.level)}</span>
                  <strong>{incident.title}</strong>
                  <span className="unit-row__action">Open</span>
                </button>
              ))}

              {!incidents.length ? (
                <p className="unit-strip__empty">
                  {incidentLoading
                    ? 'Syncing telemetry feed...'
                    : 'No incident data in the current viewport.'}
                </p>
              ) : null}
            </div>
          </section>
        ) : null}

        <span className="frame-corner frame-corner--tl" />
        <span className="frame-corner frame-corner--tr" />
        <span className="frame-corner frame-corner--bl" />
        <span className="frame-corner frame-corner--br" />
      </main>
    </div>
  )
}

export default App
