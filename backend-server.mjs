import cors from '@fastify/cors'
import rateLimit from '@fastify/rate-limit'
import dotenv from 'dotenv'
import Fastify from 'fastify'
import { randomBytes, randomUUID, scryptSync, timingSafeEqual } from 'node:crypto'
import { resolveBackendRuntime } from './server/config/runtime.mjs'
import {
  bookingDB,
  deviceDB,
  eventDB,
  favoriteDB,
  identityDB,
  referralDB,
  sessionDB,
  ticketDB,
  ticketMessageDB,
  userDB,
  watchDB
} from './server/db/sqlite.mjs'
import { registerAuthRoutes } from './server/routes/auth.mjs'
import { registerOffersRoutes } from './server/routes/offers.mjs'
import { registerPartnerRoutes } from './server/routes/partners.mjs'
import { registerSearchRoutes } from './server/routes/search.mjs'
import { registerTicketRoutes } from './server/routes/tickets.mjs'
import { buildPasswordResetRequestResponse } from './server/services/authService.mjs'
import {
  buildNoRealPartnerDataOffersPayload,
  buildRealPartnerDataOffersPayload
} from './server/services/offerService.mjs'
import {
  buildFlightBookingLinkPayload,
  buildNoRealPartnerDataSearchPayload,
  buildRealPartnerDataSearchPayload
} from './server/services/searchService.mjs'
import runtimeDefaults from './src/shared/runtime-defaults.json' with { type: 'json' }

dotenv.config({ path: '.env' })
dotenv.config({ path: '.env.local' })

const localRuntime = runtimeDefaults.local
const backendRuntime = resolveBackendRuntime(localRuntime)
const IS_DEV_RUNTIME = backendRuntime.nodeEnv !== 'production'
const IS_LOCAL_RUNTIME = ['127.0.0.1', 'localhost', '::1'].includes(
  String(backendRuntime.host || '').trim()
)

const fastify = Fastify({ logger: true })

function normalizeEmail(value = '') {
  return String(value || '')
    .trim()
    .toLowerCase()
}

function createSessionToken() {
  return randomBytes(32).toString('hex')
}

// Authentication helpers
function hashPassword(password, salt = randomBytes(16).toString('hex')) {
  const passwordHash = scryptSync(String(password), salt, 64).toString('hex')
  return { passwordHash, passwordSalt: salt }
}

function verifyPassword(password, passwordHash, passwordSalt) {
  if (!passwordHash || !passwordSalt) return false
  const derived = scryptSync(String(password), passwordSalt, 64)
  const expected = Buffer.from(passwordHash, 'hex')
  if (derived.length !== expected.length) return false
  return timingSafeEqual(derived, expected)
}

function createSession(user) {
  const sessionUser =
    typeof user === 'string'
      ? {
          id: user,
          profileId: 'default',
          email: `${user}@booktnow.local`
        }
      : user
  const token = createSessionToken()
  const expiresAt = new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString()
  sessionDB.create({
    token,
    userId: sessionUser.id,
    profileId: sessionUser.profileId || 'default',
    email:
      normalizeEmail(sessionUser.email) || `${String(sessionUser.id || 'session')}@booktnow.local`,
    expiresAt
  })
  return token
}

function getDeviceMetadata(request) {
  const userAgent = String(request.headers['user-agent'] || '').trim()
  const platform = String(
    request.headers['sec-ch-ua-platform'] || request.headers['x-platform'] || ''
  ).replace(/"/g, '')
  const browser = userAgent.includes('Firefox')
    ? 'Firefox'
    : userAgent.includes('Edg')
      ? 'Edge'
      : userAgent.includes('Chrome')
        ? 'Chrome'
        : userAgent.includes('Safari')
          ? 'Safari'
          : 'Browser'
  const language =
    String(request.headers['accept-language'] || request.headers['x-language'] || '')
      .split(',')[0]
      .trim() || 'en'
  const name = [browser, platform].filter(Boolean).join(' · ') || 'Device'
  return { userAgent, platform, browser, language, name }
}

function createSessionDevice(request, userId, sessionToken) {
  const metadata = getDeviceMetadata(request)
  const deviceId = `device_${randomUUID().replace(/-/g, '')}`
  deviceDB.clearCurrentForUser(userId)
  return deviceDB.upsert({
    id: deviceId,
    userId,
    sessionToken,
    name: metadata.name,
    platform: metadata.platform,
    browser: metadata.browser,
    language: metadata.language,
    biometricEnabled: 0,
    current: 1,
    lastSeenAt: new Date().toISOString()
  })
}

function getRequestToken(request) {
  const authHeader = request.headers.authorization
  if (authHeader?.startsWith('Bearer ')) {
    return authHeader.replace('Bearer ', '')
  }
  const sessionToken = request.headers['x-session-token']
  if (sessionToken) {
    return sessionToken
  }
  return null
}

function requireSession(request, reply) {
  const token = getRequestToken(request)
  if (!token) {
    reply.code(401)
    return null
  }
  const session = sessionDB.findByToken(token)
  if (!session) {
    reply.code(401)
    return null
  }
  const device = deviceDB.findBySessionToken(token)
  if (device) {
    deviceDB.upsert({
      ...device,
      lastSeenAt: new Date().toISOString(),
      current: 1
    })
  }
  if (!sessionDB.touch(token)) {
    reply.code(401)
    return null
  }
  return session
}

function buildAuthUser(user) {
  return {
    id: user.id,
    email: user.email,
    name: user.name
  }
}

function issueAuthResponse(user) {
  const token = createSession(user)
  return {
    token,
    user: buildAuthUser(user)
  }
}

function toEpochMs(value) {
  if (!value) return null
  const numeric = Number(value)
  if (Number.isFinite(numeric) && numeric > 0) return numeric
  const parsed = Date.parse(String(value))
  return Number.isFinite(parsed) ? parsed : null
}

function serializeReferral(referral) {
  if (!referral) return null
  return {
    referralId: referral.referralId,
    partnerId: referral.partnerId,
    partnerName: referral.partnerName,
    productId: referral.productId,
    productName: referral.productName,
    amount: Number(referral.amount || 0),
    currency: referral.currency || 'SAR',
    commissionModel: referral.commissionModel || 'cps',
    commissionRate: Number(referral.commissionRate || 0),
    status: referral.status || 'created',
    createdAt: toEpochMs(referral.createdAt) ?? Date.now(),
    handoverAt: toEpochMs(referral.handoverAt),
    conversionAt: toEpochMs(referral.conversionAt),
    commission: Number(referral.commission || 0)
  }
}

function serializeEvent(event) {
  if (!event) return null
  return {
    id: event.id,
    userId: event.userId,
    name: event.name,
    payload: event.payload || {},
    eventAt: Number(event.eventAt || Date.now())
  }
}

function issuePasswordResetCode(userId) {
  const currentIdentity = identityDB.get(userId) || {}
  const requestedAt = Date.now()
  const expiresAt = requestedAt + 15 * 60 * 1000
  const resetCode = String(Math.floor(100000 + Math.random() * 900000)).padStart(6, '0')
  identityDB.upsert(userId, {
    ...currentIdentity,
    passwordReset: {
      code: resetCode,
      requestedAt,
      expiresAt
    }
  })
  return { resetCode, requestedAt, expiresAt }
}

function readPasswordResetState(userId) {
  const identity = identityDB.get(userId) || {}
  const passwordReset =
    identity && typeof identity.passwordReset === 'object' && identity.passwordReset
      ? identity.passwordReset
      : null
  if (!passwordReset) return null
  return {
    identity,
    code: String(passwordReset.code || ''),
    requestedAt: Number(passwordReset.requestedAt) || 0,
    expiresAt: Number(passwordReset.expiresAt) || 0
  }
}

function clearPasswordResetState(userId) {
  const identity = identityDB.get(userId) || {}
  if (!identity || typeof identity !== 'object' || !identity.passwordReset) {
    return false
  }
  const nextIdentity = { ...identity }
  delete nextIdentity.passwordReset
  identityDB.upsert(userId, nextIdentity)
  return true
}

// Enable CORS
// In production, only allow the configured public origins (comma-separated).
// Defaults cover the official booktnow.com domain; extend via CORS_ORIGINS env.
const productionOrigins = String(
  process.env.CORS_ORIGINS ||
    'https://booktnow.com,https://www.booktnow.com'
)
  .split(',')
  .map((o) => o.trim())
  .filter(Boolean)

fastify.register(cors, {
  origin: IS_DEV_RUNTIME ? true : productionOrigins,
  credentials: true,
  methods: ['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS']
})

// Rate limiting for AI endpoints
fastify.register(rateLimit, {
  global: false,
  max: 200,
  timeWindow: '1 minute'
})

fastify.get('/api/health', async () => {
  return { ok: true }
})

registerAuthRoutes(fastify, {
  normalizeEmail,
  hashPassword,
  verifyPassword,
  buildAuthUser,
  issueAuthResponse,
  createSessionToken,
  createSessionDevice,
  requireSession,
  getRequestToken,
  issuePasswordResetCode,
  readPasswordResetState,
  clearPasswordResetState,
  buildPasswordResetRequestResponse,
  randomUUID,
  userDB,
  sessionDB,
  deviceDB
})

function buildSearchRuntimePayload(query = {}, requestContext = {}) {
  if (IS_DEV_RUNTIME && IS_LOCAL_RUNTIME) {
    return buildRealPartnerDataSearchPayload(query, requestContext)
  }
  return buildNoRealPartnerDataSearchPayload(query)
}

function buildOffersRuntimePayload(query = {}) {
  if (IS_DEV_RUNTIME && IS_LOCAL_RUNTIME) {
    return buildRealPartnerDataOffersPayload(query)
  }
  return buildNoRealPartnerDataOffersPayload(query)
}

registerSearchRoutes(fastify, {
  buildSearchRuntimePayload,
  buildFlightBookingLinkPayload
})

registerOffersRoutes(fastify, {
  buildOffersRuntimePayload
})

registerPartnerRoutes(fastify, {
  requireSession,
  randomUUID,
  bookingDB,
  favoriteDB,
  referralDB,
  eventDB,
  identityDB,
  watchDB,
  serializeReferral,
  serializeEvent
})

registerTicketRoutes(fastify, {
  requireSession,
  randomUUID,
  ticketDB,
  ticketMessageDB,
  referralDB,
})

function normalizeSmartText(value = '') {
  return String(value || '')
    .trim()
    .replace(/\s+/g, ' ')
}

function detectCategory(text = '') {
  const value = String(text || '').toLowerCase()
  if (/(hotel|hotels|فندق|فنادق)/i.test(value)) return 'hotels'
  if (/(flight|flights|طيران|رحلة)/i.test(value)) return 'flights'
  if (/(car|cars|سيارة|تأجير)/i.test(value)) return 'cars'
  if (/(taxi|transfer|transfers|تاكسي|توصيل)/i.test(value)) return 'taxi'
  if (/(activity|activities|نشاط|أنشطة)/i.test(value)) return 'activities'
  if (/(event|events|فعالية|فعاليات)/i.test(value)) return 'events'
  if (/(attraction|attractions|معلم|معالم)/i.test(value)) return 'attractions'
  return ''
}

function detectDestination(text = '') {
  const value = normalizeSmartText(text)
  const lowered = value.toLowerCase()
  // Each entry: [canonical Arabic label, ...aliases]
  const destinations = [
    ['دبي', 'dubai', 'dubái', 'dubaï', 'dubay'],
    ['الرياض', 'riyadh', 'riyad'],
    ['جدة', 'jeddah', 'jidda', 'jedda'],
    ['أبوظبي', 'abu dhabi', 'abu-dhabi', 'abudhabi', 'ابوظبي'],
    ['الدوحة', 'doha'],
    ['الكويت', 'kuwait', 'kuwait city'],
    ['المنامة', 'manama', 'bahrain'],
    ['مسقط', 'muscat', 'masqat'],
    ['الشارقة', 'sharjah'],
    ['الدمام', 'dammam'],
    ['تبوك', 'tabuk'],
    ['القاهرة', 'cairo', 'el cairo'],
    ['إسطنبول', 'istanbul', 'estambul', 'اسطنبول'],
    ['بيروت', 'beirut'],
    ['عمّان', 'amman'],
    ['دمشق', 'damascus'],
    ['بغداد', 'baghdad'],
    ['الدار البيضاء', 'casablanca'],
    ['مراكش', 'marrakech', 'marrakesh'],
    ['تونس', 'tunis'],
    ['الجزائر', 'algiers'],
    ['الخرطوم', 'khartoum'],
    ['باريس', 'paris', 'parigi'],
    ['لندن', 'london', 'londres'],
    ['أمستردام', 'amsterdam'],
    ['روما', 'rome', 'roma'],
    ['ميلانو', 'milan', 'milano'],
    ['برشلونة', 'barcelona'],
    ['مدريد', 'madrid'],
    ['فيينا', 'vienna', 'wien'],
    ['براغ', 'prague'],
    ['برلين', 'berlin'],
    ['زيورخ', 'zurich'],
    ['فرانكفورت', 'frankfurt'],
    ['بانكوك', 'bangkok'],
    ['طوكيو', 'tokyo', 'tokio'],
    ['سنغافورة', 'singapore'],
    ['كوالالمبور', 'kuala lumpur', 'kl'],
    ['بالي', 'bali'],
    ['المالديف', 'maldives', 'maldivas'],
    ['هونغ كونغ', 'hong kong'],
    ['شنغهاي', 'shanghai'],
    ['دلهي', 'delhi', 'new delhi'],
    ['مومباي', 'mumbai', 'bombay'],
    ['نيويورك', 'new york', 'nyc', 'new york city'],
    ['ميامي', 'miami'],
    ['لوس أنجلوس', 'los angeles', 'la'],
    ['تورنتو', 'toronto'],
    ['سيدني', 'sydney'],
    ['ملبورن', 'melbourne'],
    ['جوهانسبرغ', 'johannesburg']
  ]
  for (const variants of destinations) {
    const [canonical, ...aliases] = variants
    const allForms = [canonical.toLowerCase(), ...aliases.map(a => a.toLowerCase())]
    if (allForms.some(form => form && lowered.includes(form))) {
      return canonical
    }
  }
  return ''
}

function detectBudget(text = '') {
  const value = normalizeSmartText(text)
  const match =
    value.match(/(\d{2,7})\s*(ريال|sar|usd|دولار|درهم|aed|دينار|kwd|bhd|qar|omr|egp|جنيه|gbp)/i) ||
    value.match(/budget\s*(\d{2,7})/i)
  const numeric = match ? Number(match[1]) : 0
  return Number.isFinite(numeric) ? numeric : 0
}

function detectPassengers(text = '') {
  const value = normalizeSmartText(text)
  const match =
    value.match(/(\d{1,2})\s*(?:شخص|أشخاص|people|persons|passengers|pax)/i) ||
    value.match(/for\s+(\d{1,2})/i)
  const numeric = match ? Number(match[1]) : 0
  return Number.isFinite(numeric) ? numeric : 0
}

function buildSuggestions({ category, destination, budget, passengers }) {
  const suggestions = []
  if (!category) {
    suggestions.push({
      type: 'missing_category',
      label: 'حدد نوع البحث (فنادق/طيران/سيارات...)',
      icon: 'category'
    })
  }
  if (!destination) {
    suggestions.push({
      type: 'missing_destination',
      label: 'حدد الوجهة (مثال: دبي أو الرياض)',
      icon: 'map-pin'
    })
  }
  if (!budget) {
    suggestions.push({
      type: 'optional_budget',
      label: 'أضف ميزانية إن رغبت (مثال: 2500 ريال)',
      icon: 'wallet'
    })
  }
  if (!passengers) {
    suggestions.push({
      type: 'optional_passengers',
      label: 'أضف عدد المسافرين إن رغبت (مثال: لشخصين)',
      icon: 'users'
    })
  }
  return suggestions
}

function computeConfidence({ category, destination, budget, passengers }) {
  let score = 0.35
  if (category) score += 0.25
  if (destination) score += 0.25
  if (budget) score += 0.08
  if (passengers) score += 0.07
  return Math.max(0, Math.min(0.99, Number(score.toFixed(2))))
}

// Extra AI and utility endpoints
fastify.post('/api/ai/intent', { config: { rateLimit: { max: 20, timeWindow: '1 minute' } } }, async (request, reply) => {
  try {
    const payload = request.body || {}
    const query = normalizeSmartText(payload.query || '')
    const category = detectCategory(query) || 'hotels'
    const destination = detectDestination(query) || ''
    const budget = detectBudget(query) || 0
    const passengers = detectPassengers(query) || 0
    const extracted = { category, destination, budget, passengers }
    const suggestions = buildSuggestions(extracted)
    const confidence = computeConfidence(extracted)

    return {
      query,
      extracted,
      chips: suggestions.map(s => ({
        type: s.type,
        label: s.label,
        icon: s.icon
      })),
      confidence
    }
  } catch (err) {
    reply.code(500)
    return { error: 'ai_processing_failed', reason: String(err?.message || err) }
  }
})

fastify.post('/api/ai/voice/process', { config: { rateLimit: { max: 15, timeWindow: '1 minute' } } }, async (request, reply) => {
  try {
    const payload = request.body || {}
    const transcript = normalizeSmartText(payload.transcript || '')
    const category = detectCategory(transcript) || 'hotels'
    const destination = detectDestination(transcript) || ''
    const budget = detectBudget(transcript) || 0
    const passengers = detectPassengers(transcript) || 0
    const extracted = { category, destination, budget, passengers }
    const suggestions = buildSuggestions(extracted)
    const confidence = computeConfidence(extracted)

    return {
      transcript,
      understood: Boolean(category || destination),
      confidence,
      extracted,
      suggestions: suggestions.map(s => s.label)
    }
  } catch (err) {
    reply.code(500)
    return { error: 'ai_processing_failed', reason: String(err?.message || err) }
  }
})

/* ═══════════════════════════════════════
   نورة — المساعدة الذكية (معالجة محلية)
   الذكاء الأساسي في الفرونت إند — هذا الـ endpoint احتياطي فقط
═══════════════════════════════════════ */
fastify.post('/api/ai/chat', { config: { rateLimit: { max: 30, timeWindow: '1 minute' } } }, async (request, reply) => {
  try {
    const payload = request.body || {}
    const text = normalizeSmartText(payload.text || '')
    const ctx = payload.context || {}
    const category = detectCategory(text) || ctx.category || ''
    const destination = detectDestination(text) || ctx.destination || ''

    let content = ''
    let chips = ['فنادق', 'رحلات', 'تاكسي', 'أنشطة']

    if (category && destination) {
      content = `أبحث لك عن ${category} في ${destination}.`
      chips = [`${category} في ${destination}`, 'قارن الأسعار', 'نبهني بسعر', 'مساعدة أخرى']
    } else if (destination) {
      content = `وجهة رائعة — ${destination}! ما الذي تبحث عنه هناك؟`
      chips = [`فنادق ${destination}`, `رحلات ${destination}`, `تاكسي ${destination}`, `أنشطة ${destination}`]
    } else if (category) {
      content = `تبحث عن ${category}. ما الوجهة؟`
      chips = ['دبي', 'إسطنبول', 'لندن', 'المالديف']
    } else {
      content = 'أنا نورة من BOOKTNOW. بماذا أساعدك؟'
    }

    return { message: { role: 'assistant', content, chips, status: 'done' } }
  } catch (err) {
    reply.code(500)
    return { error: 'ai_chat_failed', reason: String(err?.message || err) }
  }
})

fastify.get('/api/metrics/basic', async (request, reply) => {
  const session = requireSession(request, reply)
  if (!session) {
    return { error: 'unauthorized' }
  }

  const bookingSummary = bookingDB.summaryByUserId(session.userId)
  const referralSummary = referralDB.summaryByUserId(session.userId)
  const eventCount = eventDB.countByUserId(session.userId)

  return {
    referrals: referralSummary.total,
    bookings: bookingSummary.total,
    revenue: bookingSummary.revenue,
    users: userDB.count(),
    conversion:
      referralSummary.total > 0
        ? Number((referralSummary.converted / referralSummary.total).toFixed(2))
        : 0,
    commissions: referralSummary.commission,
    events: eventCount
  }
})

const start = async () => {
  try {
    const host = backendRuntime.host
    const port = backendRuntime.port
    await fastify.listen({ port, host })
    console.log(`Backend server listening on http://${host}:${port}`)
  } catch (err) {
    fastify.log.error(err)
    process.exit(1)
  }
}

start()
