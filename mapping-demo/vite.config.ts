import { mkdir, readFile, writeFile } from 'node:fs/promises'
import path from 'node:path'
import { defineConfig, loadEnv, type Plugin } from 'vite'
import react from '@vitejs/plugin-react'

const CONGREGATION_CACHE_RELATIVE_PATH = path.join('public', 'congregation-cache.json')
const AIRCRAFT_CACHE_RELATIVE_PATH = path.join('public', 'aircraft-cache.json')
const OPENSKY_TOKEN_ENDPOINT =
  'https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token'
const OPENSKY_STATES_ENDPOINT = 'https://opensky-network.org/api/states/all'
const OPENSKY_TOKEN_REFRESH_MARGIN_MS = 30_000

type OpenSkyTokenResponse = {
  access_token?: string
  expires_in?: number
}

const DEFAULT_CONGREGATION_CACHE_PAYLOAD = {
  coveredTilesByCategory: {
    worship: [],
    school: [],
    stadium: [],
    arena: [],
  },
  places: [],
}

const DEFAULT_AIRCRAFT_CACHE_PAYLOAD = {
  fetchedAt: 0,
  bounds: null,
  feedStats: {
    totalStates: 0,
    statesWithCategory: 0,
    statesWithCoordinates: 0,
    plottedStates: 0,
  },
  aircraft: [],
}

const readRequestBody = async (request: NodeJS.ReadableStream) => {
  const chunks: Uint8Array[] = []

  for await (const chunk of request) {
    if (typeof chunk === 'string') {
      chunks.push(Buffer.from(chunk))
      continue
    }

    chunks.push(chunk)
  }

  return Buffer.concat(chunks).toString('utf8')
}

const congregationCacheApiPlugin = (): Plugin => ({
  name: 'congregation-cache-api',
  configureServer(server) {
    const cacheFilePath = path.resolve(server.config.root, CONGREGATION_CACHE_RELATIVE_PATH)

    server.middlewares.use('/api/congregation-cache', async (request, response) => {
      if (request.method === 'GET') {
        try {
          const cachePayload = await readFile(cacheFilePath, 'utf8')

          response.statusCode = 200
          response.setHeader('content-type', 'application/json; charset=utf-8')
          response.end(cachePayload)
          return
        } catch {
          const fallbackPayload = `${JSON.stringify(DEFAULT_CONGREGATION_CACHE_PAYLOAD, null, 2)}\n`

          try {
            await mkdir(path.dirname(cacheFilePath), { recursive: true })
            await writeFile(cacheFilePath, fallbackPayload, 'utf8')
          } catch {
            // Ignore file-creation failures and still return defaults.
          }

          response.statusCode = 200
          response.setHeader('content-type', 'application/json; charset=utf-8')
          response.end(fallbackPayload)
          return
        }
      }

      if (request.method === 'POST') {
        try {
          const rawPayload = await readRequestBody(request)
          const parsedPayload = JSON.parse(rawPayload) as unknown

          if (!parsedPayload || typeof parsedPayload !== 'object') {
            response.statusCode = 400
            response.setHeader('content-type', 'application/json; charset=utf-8')
            response.end('{"error":"Invalid cache payload."}')
            return
          }

          const serializedPayload = `${JSON.stringify(parsedPayload, null, 2)}\n`

          await mkdir(path.dirname(cacheFilePath), { recursive: true })
          await writeFile(cacheFilePath, serializedPayload, 'utf8')

          response.statusCode = 200
          response.setHeader('content-type', 'application/json; charset=utf-8')
          response.end('{"ok":true}')
          return
        } catch {
          response.statusCode = 400
          response.setHeader('content-type', 'application/json; charset=utf-8')
          response.end('{"error":"Unable to persist congregation cache payload."}')
          return
        }
      }

      response.statusCode = 405
      response.setHeader('allow', 'GET, POST')
      response.end()
    })
  },
})

const aircraftCacheApiPlugin = (): Plugin => ({
  name: 'aircraft-cache-api',
  configureServer(server) {
    const cacheFilePath = path.resolve(server.config.root, AIRCRAFT_CACHE_RELATIVE_PATH)

    server.middlewares.use('/api/aircraft-cache', async (request, response) => {
      if (request.method === 'GET') {
        try {
          const cachePayload = await readFile(cacheFilePath, 'utf8')

          response.statusCode = 200
          response.setHeader('content-type', 'application/json; charset=utf-8')
          response.end(cachePayload)
          return
        } catch {
          const fallbackPayload = `${JSON.stringify(DEFAULT_AIRCRAFT_CACHE_PAYLOAD, null, 2)}\n`

          try {
            await mkdir(path.dirname(cacheFilePath), { recursive: true })
            await writeFile(cacheFilePath, fallbackPayload, 'utf8')
          } catch {
            // Ignore file-creation failures and still return defaults.
          }

          response.statusCode = 200
          response.setHeader('content-type', 'application/json; charset=utf-8')
          response.end(fallbackPayload)
          return
        }
      }

      if (request.method === 'POST') {
        try {
          const rawPayload = await readRequestBody(request)
          const parsedPayload = JSON.parse(rawPayload) as unknown

          if (!parsedPayload || typeof parsedPayload !== 'object') {
            response.statusCode = 400
            response.setHeader('content-type', 'application/json; charset=utf-8')
            response.end('{"error":"Invalid aircraft cache payload."}')
            return
          }

          const serializedPayload = `${JSON.stringify(parsedPayload, null, 2)}\n`

          await mkdir(path.dirname(cacheFilePath), { recursive: true })
          await writeFile(cacheFilePath, serializedPayload, 'utf8')

          response.statusCode = 200
          response.setHeader('content-type', 'application/json; charset=utf-8')
          response.end('{"ok":true}')
          return
        } catch {
          response.statusCode = 400
          response.setHeader('content-type', 'application/json; charset=utf-8')
          response.end('{"error":"Unable to persist aircraft cache payload."}')
          return
        }
      }

      response.statusCode = 405
      response.setHeader('allow', 'GET, POST')
      response.end()
    })
  },
})

const openSkyApiPlugin = (clientId: string, clientSecret: string): Plugin => {
  let accessToken = ''
  let accessTokenExpiryMs = 0

  const resolveAccessToken = async () => {
    if (!clientId || !clientSecret) {
      return ''
    }

    if (accessToken && Date.now() < accessTokenExpiryMs - OPENSKY_TOKEN_REFRESH_MARGIN_MS) {
      return accessToken
    }

    const requestBody = new URLSearchParams({
      grant_type: 'client_credentials',
      client_id: clientId,
      client_secret: clientSecret,
    }).toString()

    const tokenResponse = await fetch(OPENSKY_TOKEN_ENDPOINT, {
      method: 'POST',
      headers: {
        accept: 'application/json',
        'content-type': 'application/x-www-form-urlencoded',
      },
      body: requestBody,
    })

    if (!tokenResponse.ok) {
      throw new Error(`OpenSky OAuth2 token request failed (${tokenResponse.status}).`)
    }

    const tokenPayload = (await tokenResponse.json()) as OpenSkyTokenResponse
    const resolvedToken = tokenPayload.access_token?.trim() ?? ''

    if (!resolvedToken) {
      throw new Error('OpenSky OAuth2 token response missing access_token.')
    }

    const expiresInSeconds =
      typeof tokenPayload.expires_in === 'number' && Number.isFinite(tokenPayload.expires_in)
        ? Math.max(60, Math.floor(tokenPayload.expires_in))
        : 1_800

    accessToken = resolvedToken
    accessTokenExpiryMs = Date.now() + expiresInSeconds * 1_000

    return accessToken
  }

  return {
    name: 'opensky-api-oauth2',
    configureServer(server) {
      server.middlewares.use('/api/opensky/states/all', async (request, response) => {
        if (request.method !== 'GET') {
          response.statusCode = 405
          response.setHeader('allow', 'GET')
          response.end()
          return
        }

        const requestUrl = request.url ?? ''
        const queryIndex = requestUrl.indexOf('?')
        const queryString = queryIndex >= 0 ? requestUrl.slice(queryIndex) : ''
        const targetUrl = `${OPENSKY_STATES_ENDPOINT}${queryString}`

        try {
          const requestHeaders: Record<string, string> = {
            accept: 'application/json',
          }

          if (clientId && clientSecret) {
            requestHeaders.authorization = `Bearer ${await resolveAccessToken()}`
          }

          const upstreamResponse = await fetch(targetUrl, {
            method: 'GET',
            headers: requestHeaders,
          })

          const contentType = upstreamResponse.headers.get('content-type')

          if (contentType) {
            response.setHeader('content-type', contentType)
          }

          const remainingCredits = upstreamResponse.headers.get('x-rate-limit-remaining')
          const retryAfterSeconds = upstreamResponse.headers.get('x-rate-limit-retry-after-seconds')

          if (remainingCredits) {
            response.setHeader('x-rate-limit-remaining', remainingCredits)
          }

          if (retryAfterSeconds) {
            response.setHeader('x-rate-limit-retry-after-seconds', retryAfterSeconds)
          }

          response.statusCode = upstreamResponse.status
          response.end(await upstreamResponse.text())
        } catch {
          response.statusCode = 502
          response.setHeader('content-type', 'application/json; charset=utf-8')
          response.end('{"error":"Unable to reach OpenSky API."}')
        }
      })
    },
  }
}

// https://vite.dev/config/
export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '')
  const openSkyClientId = env.OPENSKY_CLIENT_ID?.trim() ?? ''
  const openSkyClientSecret = env.OPENSKY_CLIENT_SECRET?.trim() ?? ''

  return {
    plugins: [
      react(),
      congregationCacheApiPlugin(),
      aircraftCacheApiPlugin(),
      openSkyApiPlugin(openSkyClientId, openSkyClientSecret),
    ],
    server: {
      proxy: {
        '/api/citizen': {
          target: 'https://citizen.com/api',
          changeOrigin: true,
          rewrite: (path) => path.replace(/^\/api\/citizen/, ''),
        },
      },
    },
  }
})
