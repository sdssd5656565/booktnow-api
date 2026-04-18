import { createHash } from 'node:crypto'
import travelpayoutsPrograms from '../../src/services/travelpayouts-programs.json' with { type: 'json' }

function normalizeFlightLookupKey(value) {
  return String(value || '')
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[\u064B-\u065F\u0670\u0640]/g, '')
    .replace(/[أإآ]/g, 'ا')
    .replace(/ؤ/g, 'و')
    .replace(/ئ/g, 'ي')
    .replace(/ى/g, 'ي')
    .replace(/ة/g, 'ه')
    .replace(/[^a-zA-Z0-9\u0600-\u06FF]+/g, '')
    .trim()
    .toLowerCase()
}

function normalizeIataCode(value) {
  const normalized = String(value || '').trim().toUpperCase()
  return /^[A-Z]{3}$/.test(normalized) ? normalized : ''
}

function buildMarketIataLookup() {
  const markets = Array.isArray(travelpayoutsPrograms?.markets) ? travelpayoutsPrograms.markets : []
  const lookup = Object.create(null)
  for (const item of markets) {
    const iataCode = normalizeIataCode(item?.iataCode)
    if (!iataCode) continue
    const aliases = [item?.destinationLabel, item?.cityLabel, item?.city, item?.country]
    for (const alias of aliases) {
      const key = normalizeFlightLookupKey(alias)
      if (key) lookup[key] = iataCode
    }
  }
  return Object.freeze(lookup)
}

const MARKET_IATA_LOOKUP = buildMarketIataLookup()

function toIataCode(value, fallback = 'RUH') {
  const normalized = String(value || '').trim()
  if (!normalized) return fallback
  const upper = normalized.toUpperCase()
  if (/^[A-Z]{3}$/.test(upper)) return upper
  const lookupKey = normalizeFlightLookupKey(normalized)
  if (lookupKey && MARKET_IATA_LOOKUP[lookupKey]) return MARKET_IATA_LOOKUP[lookupKey]
  const compact = normalized.toLowerCase().replace(/\s+/g, ' ')
  return IATA_FALLBACKS[compact] || fallback
}

function toPositiveNumber(value) {
  const numeric = Number(value)
  return Number.isFinite(numeric) && numeric > 0 ? numeric : null
}

function normalizeFlightDate(value) {
  const raw = String(value || '').trim()
  if (!raw) return ''
  const isoDate = raw.match(/^\d{4}-\d{2}-\d{2}/)
  return isoDate ? isoDate[0] : ''
}

function buildMockFlightEntries(origin, destination, dateFrom, currency, limit) {
  const MOCK_AIRLINES = [
    { code: 'SV', name: 'Saudia' },
    { code: 'EK', name: 'Emirates' },
    { code: 'FZ', name: 'flydubai' },
    { code: 'XY', name: 'flynas' },
    { code: 'G9', name: 'Air Arabia' },
    { code: 'WY', name: 'Oman Air' },
    { code: 'GF', name: 'Gulf Air' },
    { code: 'QR', name: 'Qatar Airways' }
  ]
  const originCode = String(origin || 'RUH').trim().toUpperCase()
  const destinationCode = String(destination || 'DXB').trim().toUpperCase()
  // Always price mock flights in USD regardless of requested currency.
  // convertPrice() will convert to the display currency correctly.
  const baseCurrency = 'USD'

  const baseDate = (() => {
    const d = new Date(normalizeFlightDate(dateFrom) ? `${normalizeFlightDate(dateFrom)}T00:00:00Z` : Date.now())
    return Number.isFinite(d.getTime()) ? d : new Date()
  })()

  const count = Math.min(Math.max(Number(limit) || 6, 4), 10)
  const entries = []
  // USD prices for mock flights — realistic regional short/medium-haul range
  const basePrices = [92, 118, 79, 148, 105, 135, 74, 172, 89, 155]
  const departureHours = [6, 9, 12, 15, 18, 21, 7, 14, 10, 20]

  for (let i = 0; i < count; i++) {
    const airline = MOCK_AIRLINES[i % MOCK_AIRLINES.length]
    const seed = createSeed('mock-flight', originCode, destinationCode, i)
    const daysOffset = Math.floor(i / 3)
    const departDate = new Date(baseDate)
    departDate.setUTCDate(departDate.getUTCDate() + daysOffset)
    departDate.setUTCHours(departureHours[i % departureHours.length], (seed % 4) * 15, 0, 0)
    const price = basePrices[i % basePrices.length] + (seed % 40) - 20
    const durationHours = 1 + (seed % 3)
    const durationMins = (seed % 4) * 15
    entries.push({
      price: Math.max(50, price),
      airline: airline.code,
      airlineName: airline.name,
      departureAt: departDate.toISOString(),
      returnAt: '',
      expiresAt: '',
      originCode,
      destinationCode,
      flightNumber: `${airline.code}${400 + (seed % 500)}`,
      transfers: i % 5 === 0 ? 1 : 0,
      durationMinutes: durationHours * 60 + durationMins,
      partnerName: 'Aviasales Flights',
      currency: baseCurrency
    })
  }
  return entries
}

function normalizeFlightMonth(value) {
  const normalizedDate = normalizeFlightDate(value)
  return normalizedDate ? normalizedDate.slice(0, 7) : ''
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms))
}

function normalizeForwardedList(value) {
  if (Array.isArray(value)) return value.flatMap(item => normalizeForwardedList(item))
  return String(value || '')
    .split(',')
    .map(item => item.trim())
    .filter(Boolean)
}

function isLocalIpAddress(value) {
  const raw = String(value || '').trim()
  if (!raw) return true
  if (raw === '::1' || raw === 'localhost') return true
  if (raw.startsWith('127.')) return true
  if (raw.startsWith('10.')) return true
  if (raw.startsWith('192.168.')) return true
  if (/^172\.(1[6-9]|2\d|3[0-1])\./.test(raw)) return true
  if (raw.startsWith('fc') || raw.startsWith('fd')) return true
  return false
}

function isLocalHostName(value) {
  const host = String(value || '')
    .trim()
    .toLowerCase()
    .replace(/^https?:\/\//, '')
    .split('/')[0]
    .split(':')[0]
  if (!host) return true
  if (host === 'localhost' || host === '127.0.0.1' || host === '::1') return true
  return host.endsWith('.local')
}

function resolvePublicRequestIp(requestContext = {}) {
  const candidates = [
    ...normalizeForwardedList(requestContext.forwardedFor),
    String(requestContext.userIp || '').trim()
  ]
  return candidates.find(candidate => candidate && !isLocalIpAddress(candidate)) || ''
}

function resolveRealHost(requestContext = {}) {
  const configuredHost =
    process.env.TRAVELPAYOUTS_REAL_HOST ||
    process.env.DOMAIN_NAME ||
    process.env.APP_DOMAIN ||
    process.env.CADDY_DOMAIN ||
    ''
  const requestHost = String(requestContext.host || '').trim()
  return String(configuredHost || requestHost).trim()
}

function normalizeTravelpayoutsLocale(value = 'ar') {
  const language = String(value || 'ar').trim().toLowerCase()
  if (language.startsWith('ar')) return 'ar'
  if (language.startsWith('en-gb')) return 'en-gb'
  return 'en-us'
}

function resolveTravelpayoutsMarketCode(originCode, locale = 'ar') {
  const configured = String(process.env.TRAVELPAYOUTS_MARKET_CODE || '').trim().toUpperCase()
  if (/^[A-Z]{2}$/.test(configured)) return configured
  if (['RUH', 'JED', 'DMM'].includes(String(originCode || '').trim().toUpperCase())) return 'SA'
  if (String(originCode || '').trim().toUpperCase() === 'CAI') return 'EG'
  return locale === 'ar' ? 'SA' : 'US'
}

function collectSignatureValues(input, bucket = []) {
  if (input === null || input === undefined) return bucket
  if (Array.isArray(input)) {
    for (const item of input) collectSignatureValues(item, bucket)
    return bucket
  }
  if (typeof input === 'object') {
    for (const key of Object.keys(input).sort()) {
      if (key === 'signature') continue
      collectSignatureValues(input[key], bucket)
    }
    return bucket
  }
  bucket.push(String(input))
  return bucket
}

function createTravelpayoutsSignature(token, marker, payload) {
  const values = collectSignatureValues(payload, []).sort((a, b) => a.localeCompare(b))
  const base = [String(token || '').trim(), String(marker || '').trim(), ...values].join(':')
  return createHash('md5').update(base).digest('hex')
}

function buildFlightSearchRequestHeaders(requestContext, signature) {
  const token = String(process.env.TRAVELPAYOUTS_TOKEN || '').trim()
  const realHost = resolveRealHost(requestContext)
  const userIp = resolvePublicRequestIp(requestContext)
  return {
    'Content-Type': 'application/json',
    'x-real-host': realHost,
    'x-user-ip': userIp,
    'x-signature': signature,
    'x-affiliate-user-id': token
  }
}

function buildFlightClickRequestHeaders(requestContext, marker, signature) {
  return {
    ...buildFlightSearchRequestHeaders(requestContext, signature),
    'x-marker': String(marker || '').trim()
  }
}

function resolveFlightSearchConfig(requestContext = {}) {
  const token = String(process.env.TRAVELPAYOUTS_TOKEN || '').trim()
  const marker = String(
    process.env.TRAVELPAYOUTS_MARKER || process.env.VITE_TRAVELPAYOUTS_MARKER || ''
  ).trim()
  const realHost = resolveRealHost(requestContext)
  const userIp = resolvePublicRequestIp(requestContext)

  if (String(process.env.TRAVELPAYOUTS_FLIGHT_SEARCH_ENABLED || '').trim().toLowerCase() === 'false') {
    return { ready: false, reason: 'disabled_by_env', token, marker, realHost, userIp }
  }
  if (!token) return { ready: false, reason: 'missing_token', token, marker, realHost, userIp }
  if (!marker) return { ready: false, reason: 'missing_marker', token, marker, realHost, userIp }
  if (!realHost || isLocalHostName(realHost)) {
    return { ready: false, reason: 'invalid_real_host', token, marker, realHost, userIp }
  }
  if (!userIp || isLocalIpAddress(userIp)) {
    return { ready: false, reason: 'invalid_user_ip', token, marker, realHost, userIp }
  }

  return { ready: true, reason: 'ready', token, marker, realHost, userIp }
}

function buildAffiliateFlightSearchBody({
  marker,
  origin,
  destination,
  currency,
  passengers,
  dateFrom,
  dateTo,
  locale = 'ar'
}) {
  const directions = [
    {
      origin,
      destination,
      date: dateFrom
    }
  ]
  if (dateTo) {
    directions.push({
      origin: destination,
      destination: origin,
      date: dateTo
    })
  }

  return {
    marker,
    locale,
    currency_code: String(currency || 'USD').trim().toUpperCase(),
    market_code: resolveTravelpayoutsMarketCode(origin, locale),
    search_params: {
      trip_class: 'Y',
      passengers: {
        adults: Math.max(1, Math.min(Number(passengers) || 1, 9)),
        children: 0,
        infants: 0
      },
      directions
    }
  }
}

function formatLocalDateTime(value = '') {
  const parsed = Date.parse(String(value || ''))
  if (!Number.isFinite(parsed)) return ''
  return new Date(parsed).toISOString()
}

function extractCarrierCodeFromLeg(leg = {}) {
  const designator = String(leg.operating_carrier_designator || '').trim().toUpperCase()
  const match = designator.match(/^[A-Z0-9]{2}/)
  return match ? match[0] : ''
}

function indexBy(items, resolver) {
  const map = new Map()
  for (const item of Array.isArray(items) ? items : []) {
    const key = resolver(item)
    if (key) map.set(key, item)
  }
  return map
}

function mapAffiliateTicketToFlightEntry(ticket, context) {
  const { airlinesByCode, agentsById, flightLegs, origin, destination, currency } = context
  const proposals = Array.isArray(ticket?.proposals) ? ticket.proposals : []
  if (proposals.length === 0) return null

  const cheapestProposal = proposals.reduce((best, current) => {
    const currentAmount = Number(current?.price?.amount || Infinity)
    const bestAmount = Number(best?.price?.amount || Infinity)
    return currentAmount < bestAmount ? current : best
  }, proposals[0])

  const segments = Array.isArray(ticket?.segments) ? ticket.segments : []
  const outboundIndexes = Array.isArray(segments[0]?.flights) ? segments[0].flights : []
  const returnIndexes = Array.isArray(segments[1]?.flights) ? segments[1].flights : []
  const outboundLegs = outboundIndexes.map(index => flightLegs[index]).filter(Boolean)
  const returnLegs = returnIndexes.map(index => flightLegs[index]).filter(Boolean)
  const firstOutboundLeg = outboundLegs[0] || null
  const firstReturnLeg = returnLegs[0] || null
  const airlineCode = extractCarrierCodeFromLeg(firstOutboundLeg || {})
  const airlineLabel = airlinesByCode.get(airlineCode)?.name || airlineCode || 'Unknown airline'
  const agent = agentsById.get(String(cheapestProposal?.agent_id || ''))
  const stopsCount =
    Math.max(0, outboundLegs.length - 1) + Math.max(0, returnLegs.length - 1)

  return {
    price: Number(cheapestProposal?.price?.amount || 0),
    airline: airlineCode || airlineLabel,
    airlineName: airlineLabel,
    departureAt:
      formatLocalDateTime(firstOutboundLeg?.local_departure_date_time) ||
      String(firstOutboundLeg?.local_departure_date_time || '').trim(),
    returnAt:
      formatLocalDateTime(firstReturnLeg?.local_departure_date_time) ||
      String(firstReturnLeg?.local_departure_date_time || '').trim(),
    expiresAt: '',
    originCode: String(firstOutboundLeg?.origin || origin || '').trim() || origin,
    destinationCode:
      String(firstOutboundLeg?.destination || destination || '').trim() || destination,
    flightNumber: String(firstOutboundLeg?.operating_carrier_designator || '').trim(),
    transfers: stopsCount,
    proposalId: String(cheapestProposal?.id || '').trim(),
    partnerName: String(agent?.label || agent?.gate_name || 'Aviasales Flights').trim(),
    currency:
      String(cheapestProposal?.price?.currency || currency || 'USD').trim().toUpperCase() || 'USD'
  }
}

async function fetchAffiliateFlightSearchResults({
  origin,
  destination,
  currency = 'USD',
  passengers = 1,
  dateFrom = '',
  dateTo = '',
  requestContext = {},
  limit = 100
}) {
  const config = resolveFlightSearchConfig(requestContext)
  if (!config.ready) {
    return { flights: [], source: 'affiliate_search_unavailable', reason: config.reason }
  }

  const normalizedDateFrom = normalizeFlightDate(dateFrom)
  const normalizedDateTo = normalizeFlightDate(dateTo)
  if (!normalizedDateFrom) {
    return { flights: [], source: 'affiliate_search_unavailable', reason: 'missing_departure_date' }
  }

  const locale = normalizeTravelpayoutsLocale(requestContext.locale || 'ar')
  const body = buildAffiliateFlightSearchBody({
    marker: config.marker,
    origin,
    destination,
    currency,
    passengers,
    dateFrom: normalizedDateFrom,
    dateTo: normalizedDateTo,
    locale
  })
  const signature = createTravelpayoutsSignature(config.token, config.marker, body)
  const startBody = {
    ...body,
    signature
  }
  const startHeaders = buildFlightSearchRequestHeaders(requestContext, signature)

  const startResponse = await fetch('https://tickets-api.travelpayouts.com/search/affiliate/start', {
    method: 'POST',
    headers: startHeaders,
    body: JSON.stringify(startBody)
  })
  if (!startResponse.ok) {
    return { flights: [], source: 'affiliate_search_failed', reason: `start_${startResponse.status}` }
  }

  const startPayload = await startResponse.json().catch(() => ({}))
  const searchId = String(startPayload?.search_id || '').trim()
  const resultsUrl = String(startPayload?.results_url || '').trim().replace(/\/+$/, '')
  if (!searchId || !resultsUrl) {
    return { flights: [], source: 'affiliate_search_failed', reason: 'missing_search_context' }
  }

  let lastUpdateTimestamp = 0
  let tickets = []
  let agents = []
  let airlines = []
  let flightLegs = []
  let isOver = false

  for (let attempt = 0; attempt < 6 && !isOver; attempt += 1) {
    const resultsBody = {
      search_id: searchId,
      last_update_timestamp: lastUpdateTimestamp
    }
    const resultsSignature = createTravelpayoutsSignature(
      config.token,
      config.marker,
      resultsBody
    )
    const resultsHeaders = buildFlightSearchRequestHeaders(requestContext, resultsSignature)
    const resultsResponse = await fetch(`${resultsUrl}/search/affiliate/results`, {
      method: 'POST',
      headers: resultsHeaders,
      body: JSON.stringify(resultsBody)
    })

    if (resultsResponse.status === 304) {
      await sleep(1200)
      continue
    }
    if (!resultsResponse.ok) {
      return {
        flights: [],
        source: 'affiliate_search_failed',
        reason: `results_${resultsResponse.status}`
      }
    }

    const resultsPayload = await resultsResponse.json().catch(() => ({}))
    if (Array.isArray(resultsPayload?.tickets) && resultsPayload.tickets.length > 0) {
      tickets = resultsPayload.tickets
    }
    if (Array.isArray(resultsPayload?.agents) && resultsPayload.agents.length > 0) {
      agents = resultsPayload.agents
    }
    if (Array.isArray(resultsPayload?.airlines) && resultsPayload.airlines.length > 0) {
      airlines = resultsPayload.airlines
    }
    if (Array.isArray(resultsPayload?.flight_legs) && resultsPayload.flight_legs.length > 0) {
      flightLegs = resultsPayload.flight_legs
    }
    lastUpdateTimestamp = Number(resultsPayload?.last_update_timestamp || lastUpdateTimestamp || 0)
    isOver = Boolean(resultsPayload?.is_over)
    if (!isOver) {
      await sleep(1200)
    }
  }

  const airlinesByCode = indexBy(airlines, item => String(item?.iata || '').trim().toUpperCase())
  const agentsById = indexBy(agents, item => String(item?.id || '').trim())
  const mappedFlights = tickets
    .map(ticket =>
      mapAffiliateTicketToFlightEntry(ticket, {
        airlinesByCode,
        agentsById,
        flightLegs,
        origin,
        destination,
        currency
      })
    )
    .filter(Boolean)

  const uniqueFlights = mergeUniqueFlights(mappedFlights).slice(0, Math.max(1, Number(limit) || 100))
  return {
    flights: uniqueFlights,
    source: 'travelpayouts_affiliate_search',
    reason: isOver ? 'search_completed' : 'search_partial',
    searchId,
    resultsUrl
  }
}

export async function buildAffiliateFlightBookingLink(
  { searchId = '', proposalId = '', resultsUrl = '' },
  requestContext = {}
) {
  const config = resolveFlightSearchConfig(requestContext)
  const normalizedSearchId = String(searchId || '').trim()
  const normalizedProposalId = String(proposalId || '').trim()
  const normalizedResultsUrl = String(resultsUrl || '').trim().replace(/\/+$/, '')

  if (!config.ready) {
    return { ok: false, reason: config.reason, url: '' }
  }
  if (!normalizedSearchId || !normalizedProposalId || !normalizedResultsUrl) {
    return { ok: false, reason: 'missing_click_context', url: '' }
  }

  const signature = createTravelpayoutsSignature(config.token, config.marker, {
    search_id: normalizedSearchId,
    proposal_id: normalizedProposalId
  })

  const response = await fetch(
    `${normalizedResultsUrl}/searches/${encodeURIComponent(normalizedSearchId)}/clicks/${encodeURIComponent(normalizedProposalId)}`,
    {
      method: 'GET',
      headers: buildFlightClickRequestHeaders(requestContext, config.marker, signature)
    }
  )

  if (!response.ok) {
    return {
      ok: false,
      reason: `click_${response.status}`,
      url: ''
    }
  }

  const payload = await response.json().catch(() => ({}))
  const bookingUrl = String(payload?.url || '').trim()
  const method = String(payload?.method || 'GET').trim().toUpperCase() || 'GET'

  if (!bookingUrl) {
    return {
      ok: false,
      reason: 'missing_click_url',
      url: ''
    }
  }

  return {
    ok: true,
    reason: 'ready',
    url: bookingUrl,
    method,
    params: payload?.params && typeof payload.params === 'object' ? payload.params : {},
    agentId: String(payload?.agent_id || '').trim(),
    gateId: String(payload?.gate_id || '').trim(),
    clickId: String(payload?.str_click_id || payload?.click_id || '').trim(),
    expireAtUnixSec: Number(payload?.expire_at_unix_sec || 0) || 0
  }
}

function buildFlightResultKey(flight) {
  return [
    String(flight?.airline || '').trim(),
    String(flight?.departureAt || '').trim(),
    String(flight?.returnAt || '').trim(),
    String(flight?.price || '').trim(),
    String(flight?.transfers || '').trim(),
    String(flight?.flightNumber || '').trim()
  ].join('|')
}

function normalizeTravelpayoutsFlightEntry(entry, origin, destination) {
  const item = entry && typeof entry === 'object' ? entry : {}
  const price = toPositiveNumber(item.price)
  if (!price) return null

  const airline = String(item.airline || '').trim() || 'Unknown airline'
  const departureAt = String(item.departure_at || '').trim()
  const returnAt = String(item.return_at || '').trim()
  const expiresAt = String(item.expires_at || '').trim()
  const destinationCode = toIataCode(item.destination || item.destination_airport || destination, destination)
  const originCode = toIataCode(item.origin || item.origin_airport || origin, origin)
  const flightNumber = String(item.flight_number || '').trim()
  const transfers = Number(item.transfers || item.number_of_changes || 0)

  return {
    price,
    airline,
    departureAt,
    returnAt,
    expiresAt,
    originCode,
    destinationCode,
    flightNumber,
    transfers
  }
}

function mergeUniqueFlights(...flightGroups) {
  const uniqueFlights = []
  const seen = new Set()
  for (const group of flightGroups) {
    for (const flight of Array.isArray(group) ? group : []) {
      const key = buildFlightResultKey(flight)
      if (!seen.has(key)) {
        seen.add(key)
        uniqueFlights.push(flight)
      }
    }
  }
  return uniqueFlights
}

function getFlightDepartureDistanceScore(flight, requestedDate) {
  const requestedTs = Date.parse(`${requestedDate}T00:00:00Z`)
  const departureTs = Date.parse(String(flight?.departureAt || ''))
  if (!Number.isFinite(requestedTs) || !Number.isFinite(departureTs)) return Number.POSITIVE_INFINITY
  return Math.abs(departureTs - requestedTs)
}

async function fetchFlightPricesForDates({
  origin,
  destination,
  currency,
  departureAt,
  returnAt = '',
  oneWay = true,
  limit = 1000,
  page = 1,
  signal
}) {
  const token = String(process.env.TRAVELPAYOUTS_TOKEN || '').trim()
  if (!token) return []

  const params = new URLSearchParams({
    origin,
    destination,
    departure_at: departureAt,
    direct: 'false',
    one_way: oneWay ? 'true' : 'false',
    sorting: 'price',
    unique: 'false',
    currency: String(currency || 'USD').trim().toLowerCase(),
    limit: String(Math.min(Math.max(Number(limit) || 1000, 1), 1000)),
    page: String(Math.max(Number(page) || 1, 1)),
    token
  })
  if (returnAt && !oneWay) params.set('return_at', returnAt)

  const response = await fetch(
    `${TRAVELPAYOUTS_API_BASE}/aviasales/v3/prices_for_dates?${params.toString()}`,
    {
      method: 'GET',
      signal
    }
  )
  if (!response.ok) return []

  const payload = await response.json().catch(() => ({}))
  const items = Array.isArray(payload?.data) ? payload.data : []
  return items
    .map(item => normalizeTravelpayoutsFlightEntry(item, origin, destination))
    .filter(Boolean)
}

async function fetchLiveFlightPrices({
  origin,
  destination,
  currency = 'USD',
  limit = 30,
  dateFrom = '',
  dateTo = ''
}) {
  const token = String(process.env.TRAVELPAYOUTS_TOKEN || '').trim()
  if (!token) return []

  const normalizedDateFrom = normalizeFlightDate(dateFrom)
  const normalizedDateTo = normalizeFlightDate(dateTo)
  const today = new Date().toISOString().split('T')[0]
  const exactDepartureAt = normalizedDateFrom
  const fallbackDepartureAt = normalizeFlightMonth(normalizedDateFrom || today)
  const fallbackReturnAt = normalizeFlightMonth(normalizedDateTo)
  const oneWay = !normalizedDateTo
  const fetchLimit = Math.min(Math.max(Number(limit) || 100, 100), 1000)

  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), 5500)
  try {
    const exactFlights = exactDepartureAt
      ? await fetchFlightPricesForDates({
          origin,
          destination,
          currency,
          departureAt: exactDepartureAt,
          returnAt: normalizedDateTo,
          oneWay,
          limit: fetchLimit,
          page: 1,
          signal: controller.signal
        })
      : []

    const needsFallbackMonth =
      !exactDepartureAt || exactFlights.length < Math.max(5, Math.min(fetchLimit, 12))

    const fallbackFlights = needsFallbackMonth
      ? await fetchFlightPricesForDates({
          origin,
          destination,
          currency,
          departureAt: fallbackDepartureAt || normalizeFlightMonth(today),
          returnAt: fallbackReturnAt,
          oneWay,
          limit: fetchLimit,
          page: 1,
          signal: controller.signal
        })
      : []

    const mergedFlights = mergeUniqueFlights(exactFlights, fallbackFlights)

    if (normalizedDateFrom) {
      mergedFlights.sort((a, b) => {
        const dateDistance = getFlightDepartureDistanceScore(a, normalizedDateFrom) - getFlightDepartureDistanceScore(b, normalizedDateFrom)
        if (dateDistance !== 0) return dateDistance
        return Number(a.price || 0) - Number(b.price || 0)
      })
    }

    return mergedFlights
  } catch {
    return []
  } finally {
    clearTimeout(timer)
  }
}

const ACTIVE_PROGRAM_STATUSES = new Set(['active', 'approved', 'joined', 'enabled'])
const TRAVELPAYOUTS_API_BASE = 'https://api.travelpayouts.com'

const IATA_FALLBACKS = Object.freeze({
  riyadh: 'RUH',
  الرياض: 'RUH',
  jeddah: 'JED',
  جدة: 'JED',
  dammam: 'DMM',
  الدمام: 'DMM',
  dubai: 'DXB',
  دبي: 'DXB',
  abu_dhabi: 'AUH',
  'abu dhabi': 'AUH',
  أبوظبي: 'AUH',
  doha: 'DOH',
  الدوحة: 'DOH',
  cairo: 'CAI',
  القاهرة: 'CAI',
  istanbul: 'IST',
  إسطنبول: 'IST',
  london: 'LON',
  لندن: 'LON',
  paris: 'PAR',
  باريس: 'PAR',
  amman: 'AMM',
  عمّان: 'AMM',
  bangkok: 'BKK',
  بانكوك: 'BKK',
  singapore: 'SIN',
  سنغافورة: 'SIN',
  muscat: 'MCT',
  مسقط: 'MCT',
  manama: 'BAH',
  المنامة: 'BAH',
  kuwait: 'KWI',
  الكويت: 'KWI'
})

const CATEGORY_ALIASES = Object.freeze({
  flight: 'flights',
  flights: 'flights',
  hotel: 'hotels',
  hotels: 'hotels',
  car: 'cars',
  cars: 'cars',
  taxi: 'taxi',
  transfers: 'taxi',
  activity: 'activities',
  activities: 'activities',
  event: 'events',
  events: 'events',
  attraction: 'attractions',
  attractions: 'attractions'
})

const PARTNER_KIND_BY_CATEGORY = Object.freeze({
  flights: 'flight',
  hotels: 'hotel',
  cars: 'car',
  taxi: 'taxi',
  activities: 'activity',
  events: 'event',
  attractions: 'attraction'
})

const CATEGORY_LABELS = Object.freeze({
  flights: 'Flight',
  hotels: 'Hotel',
  cars: 'Car',
  taxi: 'Ride',
  activities: 'Activity',
  events: 'Event',
  attractions: 'Attraction'
})

const CATEGORY_IMAGES = Object.freeze({
  // Flight card backgrounds: destination city skylines (one per common route)
  flights: [
    'https://images.unsplash.com/photo-1512453979798-5ea266f8880c?auto=format&fit=crop&w=1200&q=80', // Dubai skyline night
    'https://images.unsplash.com/photo-1520250497591-112f2f40a3f4?auto=format&fit=crop&w=1200&q=80', // city aerial blue
    'https://images.unsplash.com/photo-1436491865332-7a61a109cc05?auto=format&fit=crop&w=1200&q=80', // aircraft wing sky
    'https://images.unsplash.com/photo-1556388158-158ea5ccacbd?auto=format&fit=crop&w=1200&q=80', // airport terminal glass
    'https://images.unsplash.com/photo-1469854523086-cc02fe5d8800?auto=format&fit=crop&w=1200&q=80', // road trip vista
    'https://images.unsplash.com/photo-1530521954074-e64f6810b32d?auto=format&fit=crop&w=1200&q=80'  // travel passports map
  ],
  hotels: [
    'https://images.unsplash.com/photo-1631049307264-da0ec9d70304?auto=format&fit=crop&w=1200&q=80', // luxury room marina view
    'https://images.unsplash.com/photo-1611892440504-42a792e24d32?auto=format&fit=crop&w=1200&q=80', // premium bedroom
    'https://images.unsplash.com/photo-1566073771259-6a8506099945?auto=format&fit=crop&w=1200&q=80', // infinity pool sunset
    'https://images.unsplash.com/photo-1568084680786-a84f91d1153c?auto=format&fit=crop&w=1200&q=80', // grand lobby
    'https://images.unsplash.com/photo-1564501049412-61c2a3083791?auto=format&fit=crop&w=1200&q=80', // lobby chandelier
    'https://images.unsplash.com/photo-1582719478250-c89cae4dc85b?auto=format&fit=crop&w=1200&q=80', // penthouse suite
    'https://images.unsplash.com/photo-1540518614846-7eded433c457?auto=format&fit=crop&w=1200&q=80', // modern double room
    'https://images.unsplash.com/photo-1551882547-ff40c63fe5fa?auto=format&fit=crop&w=1200&q=80', // hotel exterior facade
    'https://images.unsplash.com/photo-1537640538966-79f369143f8f?auto=format&fit=crop&w=1200&q=80', // spa pool garden
    'https://images.unsplash.com/photo-1578683010236-d716f9a3f461?auto=format&fit=crop&w=1200&q=80', // suite sea view
    'https://images.unsplash.com/photo-1596394516093-501ba68a0ba6?auto=format&fit=crop&w=1200&q=80', // boutique bed
    'https://images.unsplash.com/photo-1520250497591-112f2f40a3f4?auto=format&fit=crop&w=1200&q=80'  // resort aerial
  ],
  // Cars: one image per vehicle class matching product names order
  cars: [
    'https://images.unsplash.com/photo-1492144534655-ae79c964c9d7?auto=format&fit=crop&w=1200&q=80', // economy coupe (Yaris)
    'https://images.unsplash.com/photo-1503376780353-7e6692767b70?auto=format&fit=crop&w=1200&q=80', // silver SUV road (Tucson)
    'https://images.unsplash.com/photo-1558618666-fcd25c85cd64?auto=format&fit=crop&w=1200&q=80', // white mpv/minivan (Alphard)
    'https://images.unsplash.com/photo-1555215695-3004980ad54e?auto=format&fit=crop&w=1200&q=80', // BMW premium (5 Series)
    'https://images.unsplash.com/photo-1511919884226-fd3cad34687c?auto=format&fit=crop&w=1200&q=80', // rugged 4x4 (Wrangler)
    'https://images.unsplash.com/photo-1485291571150-772bcfc10da5?auto=format&fit=crop&w=1200&q=80'  // executive sedan (E-Class)
  ],
  // Taxi: per service type matching product names order
  taxi: [
    'https://images.unsplash.com/photo-1519003722824-194d4455a60c?auto=format&fit=crop&w=1200&q=80', // airport terminal arrivals
    'https://images.unsplash.com/photo-1449965408869-eaa3f722e40d?auto=format&fit=crop&w=1200&q=80', // private car city night
    'https://images.unsplash.com/photo-1544636331-e26879cd4d9b?auto=format&fit=crop&w=1200&q=80', // business black car
    'https://images.unsplash.com/photo-1464219789935-c2d9d9aba644?auto=format&fit=crop&w=1200&q=80', // vip luxury car door
    'https://images.unsplash.com/photo-1494976388531-d1058494cdd8?auto=format&fit=crop&w=1200&q=80', // minivan road
    'https://images.unsplash.com/photo-1525609004556-c46c7d6cf023?auto=format&fit=crop&w=1200&q=80'  // premium car interior
  ],
  // Activities: desert and cultural experiences (Dubai)
  activities: [
    'https://images.unsplash.com/photo-1509316785289-025f5b846b35?auto=format&fit=crop&w=1200&q=80', // desert dunes safari
    'https://images.unsplash.com/photo-1466611653911-95081537e5b7?auto=format&fit=crop&w=1200&q=80', // desert sunset dune bashing
    'https://images.unsplash.com/photo-1473116763249-2faaef81ccda?auto=format&fit=crop&w=1200&q=80', // hot air balloon desert
    'https://images.unsplash.com/photo-1516026672322-bc52d61a55d5?auto=format&fit=crop&w=1200&q=80', // camel ride wadi
    'https://images.unsplash.com/photo-1549144511-f099e773c147?auto=format&fit=crop&w=1200&q=80', // desert stargazing night
    'https://images.unsplash.com/photo-1556910103-1c02745aae4d?auto=format&fit=crop&w=1200&q=80'   // cooking class cultural
  ],
  // Events: TicketNetwork — concert/sports/show
  events: [
    'https://images.unsplash.com/photo-1513151233558-d860c5398176?auto=format&fit=crop&w=1200&q=80', // concert stage lights
    'https://images.unsplash.com/photo-1493225457124-a3eb161ffa5f?auto=format&fit=crop&w=1200&q=80', // singer live mic
    'https://images.unsplash.com/photo-1524117074681-31bd4de22ad3?auto=format&fit=crop&w=1200&q=80', // sports stadium crowd
    'https://images.unsplash.com/photo-1501281668745-f7f57925c3b4?auto=format&fit=crop&w=1200&q=80', // concert crowd aerial
    'https://images.unsplash.com/photo-1514525253161-7a46d19cd819?auto=format&fit=crop&w=1200&q=80', // dj festival night
    'https://images.unsplash.com/photo-1471478331149-c72f17e33c73?auto=format&fit=crop&w=1200&q=80'  // outdoor festival lights
  ],
  // Attractions: Tiqets — landmark/museum entry tickets (Dubai-specific verified images)
  attractions: [
    'https://images.unsplash.com/photo-1512453979798-5ea266f8880c?auto=format&fit=crop&w=1200&q=80', // Burj Khalifa at night
    'https://images.unsplash.com/photo-1518684079-3c830dcef090?auto=format&fit=crop&w=1200&q=80', // Dubai Frame / modern architecture
    'https://images.unsplash.com/photo-1545324418-cc1a3fa10c00?auto=format&fit=crop&w=1200&q=80', // Dubai aquarium blue water
    'https://images.unsplash.com/photo-1512100356356-de1b84283e18?auto=format&fit=crop&w=1200&q=80', // landmark architecture
    'https://images.unsplash.com/photo-1598970434795-0c54fe7c0648?auto=format&fit=crop&w=1200&q=80', // theme park rides
    'https://images.unsplash.com/photo-1506905925346-21bda4d32df4?auto=format&fit=crop&w=1200&q=80'  // colourful festival/global village
  ]
})

const CATEGORY_CONFIG = Object.freeze({
  // Prices are in USD — convertPrice() converts to any display currency
  flights: {
    basePrice: 128,
    priceStep: 42,
    minResults: 10,
    stars: [0, 0, 0, 0, 0, 0],
    namesEn: [
      'Economy Saver',
      'Flex Economy',
      'Standard Economy',
      'Business Light',
      'Premium Economy',
      'Business Class'
    ],
    namesAr: [
      'اقتصادي موفر',
      'اقتصادي مرن',
      'اقتصادي عادي',
      'أعمال خفيف',
      'اقتصادي بريميوم',
      'درجة أعمال'
    ],
    featuresEn: [
      ['Direct flight', 'Carry-on included', 'Online check-in'],
      ['Flexible change', 'Carry-on included', 'Seat selection'],
      ['1 checked bag', 'Standard seat', 'Online check-in'],
      ['Priority boarding', 'Extra legroom', '1 checked bag'],
      ['Seat selection', 'Extra legroom', 'Carry-on + bag'],
      ['Lie-flat seat', 'Lounge access', 'Priority boarding']
    ],
    featuresAr: [
      ['رحلة مباشرة', 'حقيبة يد مشمولة', 'تسجيل إلكتروني'],
      ['تغيير مرن', 'حقيبة يد مشمولة', 'اختيار المقعد'],
      ['حقيبة مسجّلة', 'مقعد عادي', 'تسجيل إلكتروني'],
      ['أولوية الصعود', 'مساحة أوسع', 'حقيبة مسجّلة'],
      ['اختيار المقعد', 'مساحة أوسع', 'حقيبتان'],
      ['مقعد قابل للفرد', 'صالة مطار', 'أولوية الصعود']
    ]
  },
  hotels: {
    // USD 105–355/night — Aviasales Hotels (Hotellook)
    basePrice: 105,
    priceStep: 22,
    minResults: 10,
    stars: [5, 4, 4, 5, 4, 5, 4, 3, 5, 4],
    namesEn: [
      'The Ritz-Carlton',
      'Four Seasons Hotel',
      'Hyatt Regency Olaya',
      'Hilton Hotel & Residences',
      'Narcissus Hotel & Spa',
      'Novotel Suites Olaya',
      'Crowne Plaza RDC',
      'Holiday Inn Olaya',
      'Marriott Diplomatic Quarter',
      'Radisson Blu Plaza'
    ],
    namesAr: [
      'ذا ريتز كارلتون',
      'فندق فور سيزونز',
      'حياة ريجنسي العليا',
      'هيلتون للإقامة الفندقية',
      'فندق نرسيس وسبا',
      'أجنحة نوفوتيل العليا',
      'كراون بلازا RDC',
      'هوليداي إن العليا',
      'ماريوت الحي الدبلوماسي',
      'راديسون بلو بلازا'
    ],
    featuresEn: [
      ['Breakfast included', 'Free cancellation', 'Spa & wellness'],
      ['Free cancellation', 'Private balcony', 'Late checkout 14:00'],
      ['Breakfast included', 'Rooftop pool', 'City view'],
      ['Free cancellation', 'Executive lounge', 'Fitness center'],
      ['Breakfast included', 'Spa & hammam', 'Express check-in'],
      ['Free cancellation', 'Family suite', 'Airport shuttle'],
      ['Breakfast included', 'Meeting rooms', 'Free parking'],
      ['Free cancellation', 'Kids stay free', 'Outdoor pool'],
      ['Breakfast included', 'Club lounge', 'Valet parking'],
      ['Free cancellation', 'Rooftop restaurant', 'City view']
    ],
    featuresAr: [
      ['إفطار مشمول', 'إلغاء مجاني', 'سبا وعافية'],
      ['إلغاء مجاني', 'شرفة خاصة', 'مغادرة متأخرة 14:00'],
      ['إفطار مشمول', 'مسبح علوي', 'إطلالة على المدينة'],
      ['إلغاء مجاني', 'صالة تنفيذية', 'مركز لياقة'],
      ['إفطار مشمول', 'سبا وحمام', 'تسجيل وصول سريع'],
      ['إلغاء مجاني', 'جناح عائلي', 'مواصلات مطار'],
      ['إفطار مشمول', 'قاعات اجتماعات', 'موقف مجاني'],
      ['إلغاء مجاني', 'إقامة مجانية للأطفال', 'مسبح خارجي'],
      ['إفطار مشمول', 'صالة النادي', 'خدمة صف السيارات'],
      ['إلغاء مجاني', 'مطعم علوي', 'إطلالة على المدينة']
    ]
  },
  cars: {
    // USD 42–87/day — Auto Europe, QEEQ, EconomyBookings
    basePrice: 42,
    priceStep: 7,
    minResults: 10,
    stars: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    namesEn: [
      'Toyota Yaris — Economy',
      'Hyundai Tucson — SUV',
      'Toyota Alphard — MPV',
      'BMW 5 Series — Premium',
      'Jeep Wrangler — 4x4',
      'Mercedes E-Class — Executive',
      'Nissan Sunny — Compact',
      'Toyota Camry — Midsize',
      'Kia Carnival — Family Van',
      'Lexus ES — Luxury'
    ],
    namesAr: [
      'تويوتا يارس — اقتصادية',
      'هيونداي توسان — دفع رباعي',
      'تويوتا ألفارد — MPV',
      'بي إم دبليو الفئة 5 — بريميوم',
      'جيب رانجلر — 4x4',
      'مرسيدس E-Class — تنفيذية',
      'نيسان صني — مدمجة',
      'تويوتا كامري — متوسطة',
      'كيا كرنفال — فان عائلي',
      'لكزس ES — فاخرة'
    ],
    featuresEn: [
      ['Automatic', 'Unlimited mileage', 'Free cancellation'],
      ['SUV', 'Airport delivery', 'Unlimited mileage'],
      ['7 seats', 'Family friendly', 'Automatic'],
      ['Premium interior', 'Automatic', 'Airport delivery'],
      ['4x4 off-road', 'Unlimited mileage', 'GPS included'],
      ['Executive class', 'Chauffeur option', 'Airport delivery'],
      ['Compact', 'Fuel efficient', 'Free cancellation'],
      ['Midsize sedan', 'Bluetooth', 'Unlimited mileage'],
      ['8 seats', 'Sliding doors', 'Airport delivery'],
      ['Luxury interior', 'Automatic', 'Free cancellation']
    ],
    featuresAr: [
      ['أوتوماتيك', 'أميال غير محدودة', 'إلغاء مجاني'],
      ['دفع رباعي', 'توصيل للمطار', 'أميال غير محدودة'],
      ['7 مقاعد', 'مناسب للعائلة', 'أوتوماتيك'],
      ['داخلية بريميوم', 'أوتوماتيك', 'توصيل للمطار'],
      ['4x4 خارج الطرق', 'أميال غير محدودة', 'GPS مشمول'],
      ['درجة تنفيذية', 'خيار سائق خاص', 'توصيل للمطار'],
      ['مدمجة', 'موفرة للوقود', 'إلغاء مجاني'],
      ['سيدان متوسطة', 'بلوتوث', 'أميال غير محدودة'],
      ['8 مقاعد', 'أبواب منزلقة', 'توصيل للمطار'],
      ['داخلية فاخرة', 'أوتوماتيك', 'إلغاء مجاني']
    ]
  },
  taxi: {
    // USD 22–67 per transfer — Intui, Kiwitaxi, GetTransfer
    basePrice: 22,
    priceStep: 5,
    minResults: 10,
    stars: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    namesEn: [
      'Airport Standard Transfer',
      'Private City Sedan',
      'Business Class Transfer',
      'Meet & Greet VIP',
      'Family Minivan Transfer',
      'Express City Ride',
      'Shared Airport Shuttle',
      'SUV Private Transfer',
      'Hourly Chauffeur Service',
      'Luxury Limousine Transfer'
    ],
    namesAr: [
      'نقل مطار عادي',
      'سيدان خاص داخل المدينة',
      'نقل درجة أعمال',
      'استقبال VIP ومرافقة',
      'ميني فان عائلي',
      'توصيل سريع داخل المدينة',
      'شاتل مطار مشترك',
      'نقل خاص SUV',
      'سائق خاص بالساعة',
      'نقل ليموزين فاخر'
    ],
    featuresEn: [
      ['Airport pickup', 'Fixed fare', 'Flight tracking'],
      ['Private sedan', 'Meet & greet', '24/7 support'],
      ['Business class car', 'Fixed fare', 'Flight tracking'],
      ['Meet & greet', 'Name sign', 'Fixed fare'],
      ['7-seat minivan', 'Extra luggage', 'Airport pickup'],
      ['Express pickup', 'Fixed fare', 'Driver on time'],
      ['Shared ride', 'Low cost', 'Airport route'],
      ['SUV comfort', 'Extra luggage', 'Meet & greet'],
      ['Hourly booking', 'Multiple stops', 'Private driver'],
      ['Premium vehicle', 'VIP service', 'Refreshments']
    ],
    featuresAr: [
      ['استقبال مطار', 'سعر ثابت', 'تتبع الرحلة'],
      ['سيدان خاص', 'استقبال ومرافقة', 'دعم 24/7'],
      ['سيارة درجة أعمال', 'سعر ثابت', 'تتبع الرحلة'],
      ['استقبال باللوحة', 'سعر ثابت', 'خدمة VIP'],
      ['ميني فان 7 مقاعد', 'أمتعة إضافية', 'استقبال مطار'],
      ['استلام سريع', 'سعر ثابت', 'سائق في الوقت'],
      ['رحلة مشتركة', 'تكلفة منخفضة', 'خط المطار'],
      ['راحة SUV', 'أمتعة إضافية', 'استقبال ومرافقة'],
      ['حجز بالساعة', 'وقفات متعددة', 'سائق خاص'],
      ['سيارة فاخرة', 'خدمة VIP', 'مرطبات']
    ]
  },
  activities: {
    // USD 39–139/person — Klook
    basePrice: 39,
    priceStep: 11,
    minResults: 10,
    stars: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    namesEn: [
      'Desert Safari & Sunset',
      'Quad Biking Adventure',
      'Full-Day Cultural Tour',
      'Family Camel Ride Experience',
      'Stargazing Desert Night',
      'Dune Buggy & BBQ Dinner',
      'Horse Riding in the Desert',
      'Cooking Class — Saudi Cuisine',
      'Falconry Experience',
      'Off-Road Wadi Adventure'
    ],
    namesAr: [
      'سفاري صحراوية وغروب',
      'مغامرة دبابات رملية',
      'جولة ثقافية ليوم كامل',
      'تجربة ركوب الجمال العائلية',
      'ليلة رصد النجوم في الصحراء',
      'باقي رملي وعشاء مشوي',
      'ركوب خيل في الصحراء',
      'دورة طبخ — المطبخ السعودي',
      'تجربة الصقارة',
      'مغامرة الأودية الجبلية'
    ],
    featuresEn: [
      ['3-hour safari', 'Soft drinks included', 'Instant confirmation'],
      ['Private quad bike', 'Up to 8 guests', 'Safety gear included'],
      ['8-hour tour', 'Lunch included', 'Private guide'],
      ['Family friendly', 'Photo stops', 'Instant confirmation'],
      ['Overnight camp', 'Dinner included', 'Private experience'],
      ['Dune buggy', 'BBQ dinner', 'Up to 6 guests'],
      ['1-hour ride', 'Instructor included', 'All levels'],
      ['3-hour class', 'Ingredients included', 'Recipe booklet'],
      ['Private falcon', 'Photo opportunity', 'Tea & dates'],
      ['4x4 guided tour', 'Hiking included', 'Lunch stop']
    ],
    featuresAr: [
      ['سفاري 3 ساعات', 'مشروبات مشمولة', 'تأكيد فوري'],
      ['دباب خاص', 'حتى 8 أشخاص', 'معدات سلامة مشمولة'],
      ['جولة 8 ساعات', 'غداء مشمول', 'مرشد خاص'],
      ['مناسب للعائلة', 'وقفات تصوير', 'تأكيد فوري'],
      ['مخيم ليلي', 'عشاء مشمول', 'تجربة خاصة'],
      ['باقي رملي', 'عشاء مشوي', 'حتى 6 أشخاص'],
      ['ركوب ساعة', 'مدرب مشمول', 'جميع المستويات'],
      ['دورة 3 ساعات', 'المكونات مشمولة', 'كتيب وصفات'],
      ['صقر خاص', 'فرصة تصوير', 'شاي وتمر'],
      ['جولة 4x4 مع مرشد', 'مشي مشمول', 'وقفة غداء']
    ]
  },
  events: {
    // USD 65–165/ticket — TicketNetwork
    basePrice: 65,
    priceStep: 13,
    minResults: 10,
    stars: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    namesEn: [
      'Live Concert — General Admission',
      'International Show — Premium Seat',
      'Sports Event — Stadium Ticket',
      'Comedy Night — Front Row',
      'Music Festival — VIP Pass',
      'Cultural Show — Reserved Seat',
      'Theater Performance — Balcony',
      'Food Festival — Day Pass',
      'Art Exhibition — Timed Entry',
      'Family Show — Group Ticket'
    ],
    namesAr: [
      'حفل موسيقي — دخول عام',
      'عرض دولي — مقعد مميز',
      'فعالية رياضية — تذكرة ملعب',
      'ليلة كوميدية — الصفوف الأمامية',
      'مهرجان موسيقي — تصريح VIP',
      'عرض ثقافي — مقعد محجوز',
      'عرض مسرحي — شرفة',
      'مهرجان طعام — تصريح يومي',
      'معرض فني — دخول موقوت',
      'عرض عائلي — تذكرة جماعية'
    ],
    featuresEn: [
      ['Mobile ticket', 'Instant delivery', 'General seating'],
      ['Mobile ticket', 'Premium section', 'Instant delivery'],
      ['Mobile ticket', 'Stadium seating', 'Fast entry'],
      ['Front section', 'Mobile ticket', 'Instant delivery'],
      ['VIP access', 'Backstage option', 'Mobile ticket'],
      ['Reserved seat', 'Mobile ticket', 'Instant delivery'],
      ['Balcony seat', 'Printed program', 'Mobile ticket'],
      ['All-day access', 'Tasting vouchers', 'Mobile ticket'],
      ['Timed entry', 'Audio guide', 'Instant delivery'],
      ['4 tickets', 'Family friendly', 'Mobile ticket']
    ],
    featuresAr: [
      ['تذكرة جوال', 'تسليم فوري', 'مقاعد عامة'],
      ['تذكرة جوال', 'قسم مميز', 'تسليم فوري'],
      ['تذكرة جوال', 'مقاعد الملعب', 'دخول سريع'],
      ['صفوف أمامية', 'تذكرة جوال', 'تسليم فوري'],
      ['VIP دخول', 'خيار كواليس', 'تذكرة جوال'],
      ['مقعد محجوز', 'تذكرة جوال', 'تسليم فوري'],
      ['مقعد شرفة', 'برنامج مطبوع', 'تذكرة جوال'],
      ['دخول طوال اليوم', 'قسائم تذوق', 'تذكرة جوال'],
      ['دخول موقوت', 'دليل صوتي', 'تسليم فوري'],
      ['4 تذاكر', 'مناسب للعائلة', 'تذكرة جوال']
    ]
  },
  attractions: {
    // USD 26–76/ticket — Tiqets
    basePrice: 26,
    priceStep: 7,
    minResults: 10,
    stars: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    namesEn: [
      'Kingdom Centre Sky Bridge',
      'National Museum — Entry',
      'Masmak Fortress — Guided Tour',
      'Boulevard City — Day Pass',
      'Riyadh Zoo — Family Ticket',
      'Edge of the World — Day Trip',
      'Diriyah Gate — Heritage Walk',
      'Riyadh Season — Event Pass',
      'Via Riyadh — Observation Deck',
      'King Abdulaziz Historical Center'
    ],
    namesAr: [
      'جسر برج المملكة',
      'المتحف الوطني — دخول',
      'قصر المصمك — جولة مع مرشد',
      'بوليفارد سيتي — تصريح يومي',
      'حديقة الحيوان — تذكرة عائلية',
      'حافة العالم — رحلة يومية',
      'بوابة الدرعية — جولة تراثية',
      'موسم الرياض — تصريح فعاليات',
      'فيا الرياض — منصة المشاهدة',
      'مركز الملك عبدالعزيز التاريخي'
    ],
    featuresEn: [
      ['Skip the line', 'Sky bridge access', 'Mobile ticket'],
      ['Timed entry', 'Audio guide', 'Instant confirmation'],
      ['Guided tour', 'Historical exhibit', 'Mobile voucher'],
      ['All-day access', 'Entertainment zones', 'Mobile ticket'],
      ['Family friendly', 'All exhibits', 'Mobile ticket'],
      ['Transport included', 'Guided hike', 'Lunch included'],
      ['Heritage tour', 'UNESCO site', 'Mobile voucher'],
      ['Multi-venue access', 'Priority entry', 'Mobile ticket'],
      ['360° view', 'Skip the line', 'Instant confirmation'],
      ['Guided tour', 'Museum access', 'Mobile ticket']
    ],
    featuresAr: [
      ['تجاوز الطابور', 'دخول الجسر المعلق', 'تذكرة جوال'],
      ['دخول موقوت', 'دليل صوتي', 'تأكيد فوري'],
      ['جولة مع مرشد', 'معرض تاريخي', 'قسيمة جوال'],
      ['دخول طوال اليوم', 'مناطق ترفيهية', 'تذكرة جوال'],
      ['مناسب للعائلة', 'جميع المعارض', 'تذكرة جوال'],
      ['نقل مشمول', 'مشي مع مرشد', 'غداء مشمول'],
      ['جولة تراثية', 'موقع يونسكو', 'قسيمة جوال'],
      ['دخول متعدد المواقع', 'أولوية الدخول', 'تذكرة جوال'],
      ['إطلالة 360°', 'تجاوز الطابور', 'تأكيد فوري'],
      ['جولة مع مرشد', 'دخول المتحف', 'تذكرة جوال']
    ]
  }
})

function createResultTruthMap() {
  return {
    provider: 'partner_program',
    partnerName: 'partner_program',
    name: 'generated',
    price: 'generated',
    originalPrice: 'generated',
    rating: 'generated',
    reviews: 'generated',
    stars: 'generated',
    image: 'generated',
    location: 'search_request',
    lastBooking: 'generated',
    features: 'generated',
    discount: 'generated',
    badge: 'generated',
    smartScore: 'generated',
    priceChange: 'generated',
    viewers: 'generated',
    flexTier: 'generated',
    flightData: 'generated',
    productId: 'generated',
    category: 'partner_program',
    bookingUrl: 'partner_program',
    deeplinkUrl: 'partner_program',
    deeplink: 'partner_program',
    currency: 'partner_program',
    priceIntel: 'generated'
  }
}

function createOfferTruthMap() {
  return {
    partnerId: 'partner_program',
    partnerName: 'partner_program',
    kind: 'partner_program',
    productId: 'generated',
    productName: 'generated',
    price: 'generated',
    originalPrice: 'generated',
    deeplinkUrl: 'partner_program',
    badge: 'generated',
    paymentMode: 'generated',
    commissionModel: 'partner_program',
    commissionRate: 'partner_program'
  }
}

function normalizePartnerCategory(category = 'hotels') {
  const value = String(category || 'hotels')
    .trim()
    .toLowerCase()
  return CATEGORY_ALIASES[value] || 'hotels'
}

function containsArabic(value = '') {
  return /[\u0600-\u06FF]/.test(String(value || ''))
}

function createSeed(...parts) {
  return String(parts.join('|'))
    .split('')
    .reduce((acc, char) => (acc * 31 + char.charCodeAt(0)) >>> 0 || 7, 17)
}

function dedupeBy(items, getKey) {
  const seen = new Set()
  return items.filter(item => {
    const key = getKey(item)
    if (!key || seen.has(key)) return false
    seen.add(key)
    return true
  })
}

function toCommissionRate(value) {
  const match = String(value || '').match(/(\d+(?:\.\d+)?)\s*%/)
  return match ? Number(match[1]) : undefined
}

function resolveProgramCurrency(program, fallback = 'USD') {
  const candidate = String(program?.currency || '')
    .trim()
    .toUpperCase()
  return candidate || fallback
}

function listPrograms() {
  return Array.isArray(travelpayoutsPrograms?.programs) ? travelpayoutsPrograms.programs : []
}

function normalizeRuleText(value = '') {
  return String(value || '')
    .trim()
    .toLowerCase()
}

const PARTNER_CATEGORY_RULES = Object.freeze({
  flights: ['aviasales-flights', 'aviasales flights', 'aviasales.tpk.lu'],
  hotels: ['aviasales-hotels', 'aviasales hotels', 'search.hotellook.com', 'hotellook'],
  cars: ['autoeurope', 'economybookings', 'getrentacar', 'bikesbooking', 'qeeq', 'localrent'],
  taxi: ['intui', 'kiwitaxi', 'gettransfer', 'welcome pickups', 'welcomepickups'],
  activities: ['searadar', 'klook'],
  events: ['ticketnetwork'],
  attractions: ['tiqets']
})

const PARTNER_CATEGORY_PRIORITY = Object.freeze({
  flights: ['aviasales'],
  hotels: ['hotellook', 'aviasales'],
  cars: ['autoeurope', 'economybookings', 'getrentacar', 'bikesbooking', 'qeeq', 'localrent'],
  taxi: ['intui', 'kiwitaxi', 'gettransfer'],
  activities: ['searadar'],
  events: ['ticketnetwork'],
  attractions: ['tiqets']
})

function buildProgramIdentity(program) {
  return [
    program?.id,
    program?.name,
    program?.url,
    program?.description,
    Array.isArray(program?.categories) ? program.categories.join(' ') : ''
  ]
    .map(value => normalizeRuleText(value))
    .filter(Boolean)
    .join(' ')
}

function matchesProgramCategoryRule(category, program) {
  const normalizedCategory = normalizePartnerCategory(category)
  const rules = PARTNER_CATEGORY_RULES[normalizedCategory]
  if (!Array.isArray(rules) || rules.length === 0) return true
  const identity = buildProgramIdentity(program)
  return rules.some(rule => identity.includes(normalizeRuleText(rule)))
}

function getProgramCategoryPriority(category, program) {
  // Prefer explicit priority from the JSON registry — no code change needed when adding partners
  if (typeof program?.priority === 'number') {
    return Number.MAX_SAFE_INTEGER - program.priority
  }
  // Fallback: legacy name-based matching (kept for backward compatibility)
  const normalizedCategory = normalizePartnerCategory(category)
  const priorityRules = PARTNER_CATEGORY_PRIORITY[normalizedCategory]
  if (!Array.isArray(priorityRules) || priorityRules.length === 0) return Number.MAX_SAFE_INTEGER
  const identity = buildProgramIdentity(program)
  const index = priorityRules.findIndex(rule => identity.includes(normalizeRuleText(rule)))
  return index === -1 ? priorityRules.length + 10 : index
}

export function listActiveProgramsForCategory(category = 'hotels') {
  const normalizedCategory = normalizePartnerCategory(category)
  return dedupeBy(
    listPrograms()
      .filter(program =>
        ACTIVE_PROGRAM_STATUSES.has(
          String(program?.status || '')
            .trim()
            .toLowerCase()
        )
      )
      .filter(program => Array.isArray(program?.categories))
      .filter(program =>
        program.categories.some(
          item => normalizePartnerCategory(String(item || '')) === normalizedCategory
        )
      )
      // NOTE: category assignment is authoritative from the JSON `categories[]` field.
      // No secondary name/domain matching — new partners need only a correct JSON entry.
      .sort((left, right) => {
        const priorityDelta =
          getProgramCategoryPriority(normalizedCategory, left) -
          getProgramCategoryPriority(normalizedCategory, right)
        if (priorityDelta !== 0) return priorityDelta
        return String(left?.name || '').localeCompare(String(right?.name || ''))
      })
      .map(program => ({
        id: String(program?.id || ''),
        name: String(program?.name || 'Partner program').trim(),
        url: String(program?.url || '').trim(),
        destination: String(program?.destination || 'global')
          .trim()
          .toLowerCase(),
        description: String(program?.description || '').trim(),
        currency: resolveProgramCurrency(program),
        commissionRate: toCommissionRate(program?.commission)
      }))
      .filter(program => program.id && program.name && program.url),
    program => `${program.id}:${program.url}`
  )
}

export function auditPartnerCategoryAssignments() {
  return Object.keys(PARTNER_CATEGORY_RULES).reduce((acc, category) => {
    acc[category] = listActiveProgramsForCategory(category).map(program => ({
      id: program.id,
      name: program.name,
      url: program.url
    }))
    return acc
  }, {})
}

function buildUrl(baseUrl, params = {}) {
  try {
    const url = new URL(baseUrl)
    Object.entries(params).forEach(([key, value]) => {
      const normalizedValue = String(value || '').trim()
      if (!normalizedValue) return
      url.searchParams.set(key, normalizedValue)
    })
    return url.toString()
  } catch {
    return baseUrl
  }
}

function pickVariant(list, index, fallback = '') {
  if (!Array.isArray(list) || list.length === 0) return fallback
  return list[index % list.length] || fallback
}

function resolveVariantText(config, field, index, isArabic) {
  const list = isArabic ? config?.[`${field}Ar`] : config?.[`${field}En`]
  const fallbackList = isArabic ? config?.[`${field}En`] : config?.[`${field}Ar`]
  return pickVariant(list, index, pickVariant(fallbackList, index, ''))
}

/*
 * DATA SOURCE: demo
 * Generates realistic price intelligence from seed-based deterministic data.
 * When live partner APIs are connected, replace the body of this function
 * with a call to the real price-history API while keeping the same return shape.
 */
function createPriceIntel(price, seed, currency, isArabic) {
  const avg30 = Math.round(price * (1.08 + (seed % 5) * 0.015))
  const avg7 = Math.round(avg30 * (0.94 + (seed % 4) * 0.02))
  const min30 = Math.max(1, Math.round(price * (0.9 + (seed % 3) * 0.015)))
  const pctVsAvg30 = avg30 > 0 ? Number((((price - avg30) / avg30) * 100).toFixed(1)) : 0
  const pctVsMin30 = min30 > 0 ? Number((((price - min30) / min30) * 100).toFixed(1)) : 0
  const sampleCount = 24 + (seed % 18)
  return {
    dataSource: 'demo',
    reason:
      pctVsAvg30 <= -5
        ? isArabic
          ? 'أقل من متوسط السعر المعتاد'
          : 'Below the usual average'
        : isArabic
          ? 'ضمن النطاق الطبيعي الحالي'
          : 'Within the current expected range',
    insufficientData: false,
    avg7,
    avg30,
    min30,
    pctVsAvg30,
    pctVsMin30,
    sampleCount,
    analysisBase: currency,
    avg30usd: currency === 'USD' ? avg30 : null,
    min30usd: currency === 'USD' ? min30 : null,
    currentCurrency: currency,
    fxRate: 1,
    stdDev30usd: currency === 'USD' ? Math.round(avg30 * 0.08) : null,
    volatilityRatio: Number((0.08 + (seed % 4) * 0.02).toFixed(2)),
    volatilityNeutral: false
  }
}

function buildFlightData(seed, destination, isArabic) {
  const demandHeat = 68 + (seed % 24)
  const seatOccupancyPct = 62 + (seed % 28)
  return {
    demandHeat,
    seatOccupancy: isArabic ? `${seatOccupancyPct}% إشغال` : `${seatOccupancyPct}% occupied`,
    seatOccupancyColor: seatOccupancyPct >= 80 ? '#fda4af' : '#6ee7b7',
    hasWifi: seed % 2 === 0,
    wifiSpeed: isArabic ? 'واي فاي مناسب للتصفح' : 'Wi-Fi for browsing',
    carbonKg: 112 + (seed % 75),
    carbonColor: '#93c5fd',
    airportRating: isArabic ? 'انطلاق سلس' : 'Smooth departure',
    airportName: destination || (isArabic ? 'المطار الرئيسي' : 'Primary airport'),
    priceHistory: Array.from({ length: 7 }, (_, index) => 180 + ((seed + index * 11) % 95)),
    transferEase: isArabic ? 'تحويل واضح' : 'Easy transfer',
    transferColor: '#6ee7b7',
    transferTime: isArabic ? `${1 + (seed % 3)} ساعة` : `${1 + (seed % 3)}h`,
    isNightFlight: seed % 3 === 0,
    comfortScore: isArabic ? 'راحة جيدة' : 'Good comfort',
    checkpointMin: 18 + (seed % 22),
    airQuality: isArabic ? 'جيد' : 'Good',
    airQualityColor: '#6ee7b7',
    aqiIndex: 32 + (seed % 18)
  }
}

function ensureMinimumProgramAssignments(programs, minimum) {
  if (programs.length === 0) return []
  const expanded = [...programs]
  while (expanded.length < minimum) {
    expanded.push(programs[expanded.length % programs.length])
  }
  return expanded
}

function createResultName(category, destination, index, isArabic) {
  const config = CATEGORY_CONFIG[category] || CATEGORY_CONFIG.hotels
  const baseName = resolveVariantText(config, 'names', index, isArabic)
  if (!destination) return baseName
  // Avoid duplicating the destination if the product name already contains it
  const destLower = String(destination || '').toLowerCase()
  const baseNameLower = String(baseName || '').toLowerCase()
  const alreadyContainsDest = baseNameLower.includes(destLower)
  if (isArabic) return alreadyContainsDest ? baseName : `${baseName} في ${destination}`
  return alreadyContainsDest ? baseName : `${destination} ${baseName}`
}

function createLastBooking(index, isArabic) {
  const minutes = 6 + index * 4
  return isArabic ? `تم الحجز قبل ${minutes} دقيقة` : `Booked ${minutes} min ago`
}

function createBadge(index, isArabic) {
  const badges = isArabic
    ? ['موصى به', 'أفضل قيمة', 'الأرخص', 'مرن']
    : ['Recommended', 'Best value', 'Cheapest', 'Flexible']
  return badges[index % badges.length]
}

function createResult(program, category, destination, passengers, index) {
  const config = CATEGORY_CONFIG[category] || CATEGORY_CONFIG.hotels
  const isArabic = containsArabic(destination)
  const seed = createSeed(category, destination, program.id, index)
  // Prices are in USD — convertPrice() in the frontend converts to any display currency
  const currency = 'USD'
  const basePrice = config.basePrice + index * config.priceStep + (seed % 9)
  const originalPrice = Math.round(basePrice * (1.16 + (seed % 3) * 0.03))
  const productId = `${category}:${program.id}:${index + 1}`
  const deeplink = buildUrl(program.url, {
    utm_source: 'booktnow',
    utm_medium: 'partner_search',
    category,
    destination,
    product_id: productId,
    passengers: String(Math.max(1, Number(passengers || 1)))
  })
  const priceIntel = createPriceIntel(basePrice, seed, currency, isArabic)
  const pct = Number(priceIntel?.pctVsAvg30 || 0)
  return {
    id: 1000 + (seed % 900000),
    name: createResultName(category, destination, index, isArabic),
    provider: program.name,
    partnerName: program.name,
    price: basePrice,
    originalPrice,
    rating: Number((8 + (seed % 19) / 10).toFixed(1)),
    reviews: 120 + (seed % 4600),
    stars: pickVariant(config.stars, index, 0) || undefined,
    image: pickVariant(
      CATEGORY_IMAGES[category],
      index,
      pickVariant(CATEGORY_IMAGES.hotels, index)
    ),
    location: destination,
    lastBooking: createLastBooking(index, isArabic),
    features: resolveVariantText(config, 'features', index, isArabic),
    discount: Math.max(4, Math.round(((originalPrice - basePrice) / originalPrice) * 100)),
    badge: createBadge(index, isArabic),
    smartScore: isArabic ? 'مؤشر BOOKTNOW' : 'BOOKTNOW score',
    priceChange: pct < -3 ? 'down' : pct > 3 ? 'up' : 'stable',
    viewers: 8 + (seed % 95),
    flexTier: seed % 3,
    flightData: category === 'flights' ? buildFlightData(seed, destination, isArabic) : null,
    productId,
    category,
    bookingUrl: deeplink,
    deeplinkUrl: deeplink,
    deeplink,
    currency,
    priceIntel,
    truth: createResultTruthMap()
  }
}

function createOffer(program, category, productId, index) {
  const seed = createSeed(category, productId, program.id, index)
  const currency = resolveProgramCurrency(program, 'USD')
  const basePriceMap = {
    flights: 225,
    hotels: 102,
    cars: 45,
    taxi: 21,
    activities: 37,
    events: 62,
    attractions: 26
  }
  const basePrice = (basePriceMap[category] || 50) + index * 9 + (seed % 8)
  const originalPrice = Math.round(basePrice * (1.14 + (seed % 4) * 0.02))
  return {
    partnerId: program.id,
    partnerName: program.name,
    kind: PARTNER_KIND_BY_CATEGORY[category] || 'hotel',
    productId: String(productId || ''),
    productName: `${CATEGORY_LABELS[category] || 'Offer'} ${productId || index + 1}`,
    price: {
      amount: basePrice,
      currency
    },
    originalPrice: {
      amount: originalPrice,
      currency
    },
    deeplinkUrl: buildUrl(program.url, {
      utm_source: 'booktnow',
      utm_medium: 'partner_offer',
      category,
      product_id: String(productId || ''),
      partner_id: program.id
    }),
    paymentMode: 'redirect',
    commissionModel: 'cps',
    commissionRate: program.commissionRate,
    priority: typeof program.priority === 'number' ? program.priority : undefined,
    truth: createOfferTruthMap()
  }
}

function markOfferBadges(offers) {
  if (offers.length === 0) return offers
  const cheapestAmount = Math.min(...offers.map(offer => Number(offer.price?.amount || 0)))
  const preferredPartnerId = offers[0]?.partnerId || ''
  return offers.map(offer => ({
    ...offer,
    badge:
      offer.partnerId === preferredPartnerId
        ? 'Recommended'
        : Number(offer.price?.amount || 0) === cheapestAmount
          ? 'Cheapest'
          : offer.badge
  }))
}

export async function buildRealPartnerSearchPayload(query = {}, requestContext = {}) {
  const category = normalizePartnerCategory(String(query.category || 'hotels'))
  const destination = String(query.destination || query.query || '').trim() || 'دبي'
  const passengers = Math.max(1, Number(query.passengers || 1) || 1)
  const requestedLimit = Number(query.limit || 100) || 100
  if (category === 'flights') {
    const origin = toIataCode(query.origin || query.from || 'RUH', 'RUH')
    const destinationCode = toIataCode(destination, 'DXB')
    const currency =
      String(query.currency || 'USD')
        .trim()
        .toUpperCase() || 'USD'
    const affiliateSearch = await fetchAffiliateFlightSearchResults({
      origin,
      destination: destinationCode,
      currency,
      passengers,
      dateFrom: query.dateFrom,
      dateTo: query.dateTo,
      requestContext,
      limit: requestedLimit
    })
    const fallbackFlights = await fetchLiveFlightPrices({
      origin,
      destination: destinationCode,
      currency,
      limit: requestedLimit,
      dateFrom: query.dateFrom,
      dateTo: query.dateTo
    })
    const liveFlights =
      Array.isArray(affiliateSearch.flights) && affiliateSearch.flights.length > 0
        ? affiliateSearch.flights
        : fallbackFlights
    const flightSource =
      Array.isArray(affiliateSearch.flights) && affiliateSearch.flights.length > 0
        ? affiliateSearch.source
        : 'travelpayouts_live_flights'
    const flightReason =
      Array.isArray(affiliateSearch.flights) && affiliateSearch.flights.length > 0
        ? affiliateSearch.reason
        : 'travelpayouts_live_flights'

    const resolvedFlights = liveFlights.length > 0
      ? liveFlights
      : buildMockFlightEntries(origin, destinationCode, query.dateFrom, currency, Math.min(requestedLimit, 10))
    const resolvedFlightSource = liveFlights.length > 0 ? flightSource : 'mock_flight_catalog'
    const resolvedFlightReason = liveFlights.length > 0 ? flightReason : 'mock_flight_catalog'

    const flightsProgram = listActiveProgramsForCategory('flights')[0] || {
      name: 'Aviasales Flights',
      url: 'https://aviasales.tpk.lu/KjTN4Rrw',
      id: 'aviasales-flights-1'
    }

    const assignments = resolvedFlights.slice(0, requestedLimit).map((entry, index) => {
      const baseId = `${entry.originCode}-${entry.destinationCode}-${entry.airline}-${entry.price}-${entry.departureAt}`
      const idSeed = createSeed('flights', baseId, flightsProgram.id, index)
      const productId = `flights:${entry.originCode}-${entry.destinationCode}:${index + 1}`
      const deeplink = buildUrl(flightsProgram.url, {
        utm_source: 'booktnow',
        utm_medium:
          resolvedFlightSource === 'travelpayouts_affiliate_search'
            ? 'partner_search_affiliate'
            : 'partner_search_live',
        origin: entry.originCode,
        destination: entry.destinationCode,
        depart_date: normalizeFlightDate(entry.departureAt),
        return_date: normalizeFlightDate(entry.returnAt),
        passengers: String(passengers)
      })
      const requestLocale = String(requestContext.locale || query.locale || 'ar').trim().toLowerCase()
      const isArabic = requestLocale.startsWith('ar')
      const formattedDeparture = entry.departureAt
        ? entry.departureAt.split('T')[0] + ' ' + (entry.departureAt.split('T')[1] || '').slice(0, 5)
        : ''
      const formattedReturn = entry.returnAt
        ? entry.returnAt.split('T')[0] + ' ' + (entry.returnAt.split('T')[1] || '').slice(0, 5)
        : ''
      const transfersLabel = entry.transfers === 0
        ? (isArabic ? 'بدون توقف' : 'Direct')
        : (isArabic ? `توقف ${entry.transfers}` : `${entry.transfers} stop${entry.transfers > 1 ? 's' : ''}`)

      const flightRating = Number((7.8 + (idSeed % 22) / 10).toFixed(1))
      const flightReviews = 180 + (idSeed % 2800)
      const flightViewers = 6 + (idSeed % 38)
      const badgesAr = ['موصى به', 'أفضل قيمة', 'الأرخص', 'سعر جيد', 'مباشرة', 'أسرع وقت']
      const badgesEn = ['Recommended', 'Best value', 'Lowest fare', 'Good price', 'Direct', 'Fastest']
      const flightBadge = isArabic ? badgesAr[idSeed % badgesAr.length] : badgesEn[idSeed % badgesEn.length]
      const bookingMinsAgo = 4 + (idSeed % 28)
      const flightLastBooking = isArabic ? `تم الحجز قبل ${bookingMinsAgo} دقيقة` : `Booked ${bookingMinsAgo} min ago`
      const pctVsAvg = Math.round(((idSeed % 21) - 8))   // -8% to +12%
      const avg30Price = Math.round(entry.price * (1 + 0.07 + (idSeed % 9) * 0.01))
      const min30Price = Math.round(entry.price * (0.88 + (idSeed % 8) * 0.01))
      return {
        id: 300000 + (idSeed % 600000),
        name: `${entry.originCode} → ${entry.destinationCode}`,
        provider: entry.airlineName || entry.airline,
        partnerName: entry.partnerName || flightsProgram.name,
        airlineName: entry.airlineName || entry.airline,
        airlineCode: String(entry.airline || '').trim().toUpperCase().slice(0, 4),
        price: entry.price,
        originalPrice: Math.round(entry.price * (1.08 + (idSeed % 5) * 0.02)),
        rating: flightRating,
        reviews: flightReviews,
        stars: undefined,
        image: pickVariant(CATEGORY_IMAGES.flights, index, pickVariant(CATEGORY_IMAGES.flights, 0)),
        location: destination,
        lastBooking: flightLastBooking,
        features: [
          transfersLabel,
          formattedDeparture ? (isArabic ? `ذهاب: ${formattedDeparture}` : `Depart: ${formattedDeparture}`) : '',
          formattedReturn ? (isArabic ? `عودة: ${formattedReturn}` : `Return: ${formattedReturn}`) : ''
        ].filter(Boolean),
        discount: Math.max(3, Math.round(((entry.price * (1.08 + (idSeed % 5) * 0.02) - entry.price) / (entry.price * (1.08 + (idSeed % 5) * 0.02))) * 100)),
        badge: flightBadge,
        smartScore: isArabic ? 'مؤشر BOOKTNOW' : 'BOOKTNOW score',
        priceChange: pctVsAvg < -4 ? 'down' : pctVsAvg > 4 ? 'up' : 'stable',
        viewers: flightViewers,
        flexTier: idSeed % 3,
        flightData: entry.durationMinutes > 0 ? { checkpointMin: entry.durationMinutes } : null,
        productId,
        category,
        bookingUrl: deeplink,
        deeplinkUrl: deeplink,
        deeplink,
        currency,
        originCode: entry.originCode,
        destinationCode: entry.destinationCode,
        departureAt: entry.departureAt || '',
        returnAt: entry.returnAt || '',
        stopsCount: entry.transfers,
        affiliateSearchId: affiliateSearch.searchId || '',
        affiliateResultsUrl: affiliateSearch.resultsUrl || '',
        affiliateProposalId: entry.proposalId || '',
        priceIntel: {
          reason: 'live_partner_price',
          insufficientData: false,
          avg7: Math.round(entry.price * (1.04 + (idSeed % 7) * 0.01)),
          avg30: avg30Price,
          min30: min30Price,
          pctVsAvg30: pctVsAvg,
          pctVsMin30: Math.round(((entry.price - min30Price) / min30Price) * 100),
          sampleCount: 28 + (idSeed % 60),
          analysisBase: 'USD',
          avg30usd: avg30Price,
          min30usd: min30Price,
          currentCurrency: 'USD',
          fxRate: 1,
          stdDev30usd: Math.round(entry.price * 0.07),
          volatilityRatio: Number((0.07 + (idSeed % 4) * 0.02).toFixed(2)),
          volatilityNeutral: false
        },
        truth: {
          provider: 'partner_offer',
          partnerName: 'partner_program',
          name: 'search_request',
          price: 'partner_offer',
          originalPrice: 'partner_offer',
          rating: 'generated',
          reviews: 'generated',
          stars: 'generated',
          image: 'generated',
          location: 'search_request',
          lastBooking: 'partner_offer',
          features: 'partner_offer',
          discount: 'generated',
          badge: 'generated',
          smartScore: 'generated',
          priceChange: 'generated',
          viewers: 'generated',
          flexTier: 'generated',
          flightData: 'generated',
          productId: 'generated',
          category: 'search_request',
          bookingUrl: 'partner_program',
          deeplinkUrl: 'partner_program',
          deeplink: 'partner_program',
          currency: 'partner_offer',
          priceIntel: 'generated'
        }
      }
    })

    return {
      results: assignments,
      meta: {
        source: resolvedFlightSource,
        query: String(query.query || ''),
        category,
        destination,
        passengers,
        budget: Number(query.budget || 0),
        sortBy: String(query.sortBy || 'recommended'),
        count: assignments.length,
        total: assignments.length,
        offset: 0,
        limit: requestedLimit,
        hasMore: false,
        cursor: String(query.cursor || '') || null,
        nextCursor: null
      },
      insights: {
        status: 200,
        reason: resolvedFlightReason,
        partners: [{ id: flightsProgram.id, name: flightsProgram.name }]
      }
    }
  }

  if (!['hotels', 'cars', 'taxi', 'activities', 'events', 'attractions'].includes(category)) {
    return {
      results: [],
      meta: {
        source: 'partner_data_pending',
        query: String(query.query || ''),
        category,
        destination,
        passengers,
        budget: Number(query.budget || 0),
        sortBy: String(query.sortBy || 'recommended'),
        count: 0,
        total: 0,
        offset: 0,
        limit: requestedLimit,
        hasMore: false,
        cursor: String(query.cursor || '') || null,
        nextCursor: null
      },
      insights: {
        status: 204,
        reason: 'partner_data_pending'
      }
    }
  }
  const programs = listActiveProgramsForCategory(category)
  if (programs.length === 0) {
    return {
      results: [],
      meta: {
        source: 'no_real_partner_data',
        query: String(query.query || ''),
        category,
        destination,
        passengers,
        budget: Number(query.budget || 0),
        sortBy: String(query.sortBy || 'recommended'),
        count: 0,
        total: 0,
        offset: 0,
        limit: requestedLimit,
        hasMore: false,
        cursor: String(query.cursor || '') || null,
        nextCursor: null
      },
      insights: {
        status: 204,
        reason: 'no_real_partner_data'
      }
    }
  }

  const categoryMaxResults = CATEGORY_CONFIG[category]?.minResults || 6
  const displayLimit = Math.max(
    1,
    Math.min(requestedLimit, categoryMaxResults)
  )
  const assignments = ensureMinimumProgramAssignments(programs.slice(0, displayLimit), displayLimit)
    .map((program, index) => createResult(program, category, destination, passengers, index))

  return {
    results: assignments,
    meta: {
      source: 'real_partner_catalog',
      query: String(query.query || destination),
      category,
      destination,
      passengers,
      budget: Number(query.budget || 0),
      sortBy: String(query.sortBy || 'recommended'),
      count: assignments.length,
      total: assignments.length,
      offset: 0,
      limit: requestedLimit,
      hasMore: false,
      cursor: String(query.cursor || '') || null,
      nextCursor: null
    },
    insights: {
      status: 200,
      reason: 'real_partner_catalog',
      partners: dedupeBy(programs, program => program.id).map(program => ({
        id: program.id,
        name: program.name
      }))
    }
  }
}

export function buildRealPartnerOffersPayload(query = {}) {
  const category = normalizePartnerCategory(String(query.category || 'hotels'))
  const productId = String(query.productId || '')
  if (!['hotels', 'cars', 'taxi', 'activities', 'events', 'attractions'].includes(category)) {
    return {
      offers: [],
      meta: {
        source: 'partner_data_pending',
        category,
        productId,
        count: 0
      },
      insights: {
        status: 204,
        reason: 'partner_data_pending'
      }
    }
  }
  const programs = listActiveProgramsForCategory(category)
  if (programs.length === 0) {
    return {
      offers: [],
      meta: {
        source: 'no_real_partner_data',
        category,
        productId,
        count: 0
      },
      insights: {
        status: 204,
        reason: 'no_real_partner_data'
      }
    }
  }

  const displayLimit = Math.max(1, Math.min(category === 'hotels' ? 6 : 4, programs.length || 6))
  const offers = markOfferBadges(
    ensureMinimumProgramAssignments(programs.slice(0, displayLimit), displayLimit).map(
      (program, index) => createOffer(program, category, productId, index)
    )
  ).sort((left, right) => Number(left.price.amount) - Number(right.price.amount))

  return {
    offers,
    meta: {
      source: 'real_partner_catalog',
      category,
      productId,
      count: offers.length
    },
    insights: {
      status: 200,
      reason: 'real_partner_catalog'
    }
  }
}

export { normalizePartnerCategory }
