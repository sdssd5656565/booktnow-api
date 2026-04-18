export function registerAuthRoutes(fastify, deps) {
  const {
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
  } = deps

  fastify.post('/api/auth/signup', async (request, reply) => {
    const payload = request.body || {}
    const email = normalizeEmail(payload.email)
    const password = String(payload.password || '')
    const name = String(payload.name || email.split('@')[0] || 'User').trim()

    if (!email || !password || password.length < 6) {
      reply.code(400)
      return { error: 'invalid_payload' }
    }

    if (userDB.findByEmail(email)) {
      reply.code(409)
      return { error: 'email_exists' }
    }

    const { passwordHash, passwordSalt } = hashPassword(password)
    const user = userDB.create({
      id: `user_${randomUUID().replace(/-/g, '')}`,
      email,
      name,
      passwordHash,
      passwordSalt
    })
    const response = issueAuthResponse(user)
    createSessionDevice(request, user.id, response.token)
    reply.code(201)
    return response
  })

  fastify.post('/api/auth/login', async (request, reply) => {
    const payload = request.body || {}
    const email = normalizeEmail(payload.email)
    const password = String(payload.password || '')

    const user = userDB.findByEmail(email)
    if (!user || !verifyPassword(password, user.passwordHash, user.passwordSalt)) {
      reply.code(401)
      return { error: 'invalid_credentials' }
    }

    reply.code(200)
    const response = issueAuthResponse(user)
    createSessionDevice(request, user.id, response.token)
    return response
  })

  fastify.post('/api/auth/social/:provider', async (request, reply) => {
    const payload = request.body || {}
    const provider = String(request.params?.provider || '')
      .trim()
      .toLowerCase()
    const code = String(payload.code || '').trim()
    const state = String(payload.state || '').trim()
    const redirectUri = String(payload.redirectUri || '').trim()

    if (provider !== 'google' || !code || !state) {
      reply.code(400)
      return { success: false, error: 'invalid_payload' }
    }

    const clientId = process.env.VITE_GOOGLE_CLIENT_ID || ''
    const clientSecret = process.env.GOOGLE_CLIENT_SECRET || ''

    if (!clientId || !clientSecret) {
      reply.code(503)
      return { success: false, error: 'google_not_configured' }
    }

    // Exchange authorization code for tokens
    let googleTokens
    try {
      const tokenRes = await fetch('https://oauth2.googleapis.com/token', {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: new URLSearchParams({
          code,
          client_id: clientId,
          client_secret: clientSecret,
          redirect_uri: redirectUri,
          grant_type: 'authorization_code'
        }).toString()
      })
      googleTokens = await tokenRes.json()
      if (googleTokens.error) {
        reply.code(401)
        return { success: false, error: googleTokens.error_description || 'google_token_error' }
      }
    } catch {
      reply.code(502)
      return { success: false, error: 'google_token_fetch_failed' }
    }

    // Fetch Google user profile
    let googleUser
    try {
      const profileRes = await fetch('https://www.googleapis.com/oauth2/v2/userinfo', {
        headers: { Authorization: `Bearer ${googleTokens.access_token}` }
      })
      googleUser = await profileRes.json()
      if (!googleUser.email) {
        reply.code(401)
        return { success: false, error: 'google_profile_missing_email' }
      }
    } catch {
      reply.code(502)
      return { success: false, error: 'google_profile_fetch_failed' }
    }

    const email = normalizeEmail(googleUser.email)
    const displayName = String(googleUser.name || googleUser.email.split('@')[0] || 'Google User').trim()
    let user = userDB.findByEmail(email)

    if (!user) {
      const { passwordHash, passwordSalt } = hashPassword(`social-google-${randomUUID()}`)
      user = userDB.create({
        id: `user_${randomUUID().replace(/-/g, '')}`,
        email,
        name: displayName,
        passwordHash,
        passwordSalt
      })
    }

    const response = issueAuthResponse(user)
    createSessionDevice(request, user.id, response.token)
    reply.code(200)
    return {
      success: true,
      token: response.token,
      user: {
        ...buildAuthUser(user),
        provider: 'google',
        verified: Boolean(googleUser.verified_email)
      }
    }
  })

  fastify.put('/api/auth/profile', async (request, reply) => {
    const session = requireSession(request, reply)
    if (!session) return { error: 'unauthorized' }
    const payload = request.body || {}
    const name = String(payload.name || '').trim()
    if (!name) {
      reply.code(400)
      return { ok: false, error: 'name_required' }
    }
    userDB.updateName(session.userId, name)
    const user = userDB.findById(session.userId)
    reply.code(200)
    return { ok: true, user: buildAuthUser(user) }
  })

  fastify.get('/api/auth/me', async (request, reply) => {
    const session = requireSession(request, reply)
    if (!session) {
      return { error: 'unauthorized' }
    }
    const user = userDB.findById(session.userId)
    if (!user) {
      reply.code(401)
      return { error: 'unauthorized' }
    }
    reply.code(200)
    return { user: buildAuthUser(user) }
  })

  fastify.post('/api/auth/password-reset/request', async (request, reply) => {
    const payload = request.body || {}
    const email = normalizeEmail(payload.email)
    if (!email) {
      reply.code(400)
      return { ok: false, error: 'invalid_email' }
    }
    const user = userDB.findByEmail(email)
    if (!user) {
      reply.code(404)
      return { ok: false, error: 'account_not_found' }
    }
    const { resetCode, requestedAt, expiresAt } = issuePasswordResetCode(user.id)
    // TODO: send resetCode via email when email service is integrated
    return { ...buildPasswordResetRequestResponse({ requestedAt, expiresAt }), resetCode }
  })

  fastify.post('/api/auth/password-reset/confirm', async (request, reply) => {
    const payload = request.body || {}
    const email = normalizeEmail(payload.email)
    const code = String(payload.code || '').trim()
    const newPassword = String(payload.newPassword || '')

    if (!email || !code || !newPassword || newPassword.length < 6) {
      reply.code(400)
      return { ok: false, error: 'invalid_payload' }
    }

    const user = userDB.findByEmail(email)
    if (!user) {
      reply.code(404)
      return { ok: false, error: 'account_not_found' }
    }

    const resetState = readPasswordResetState(user.id)
    if (!resetState || !resetState.code) {
      reply.code(400)
      return { ok: false, error: 'reset_code_missing' }
    }

    if (resetState.expiresAt && resetState.expiresAt <= Date.now()) {
      clearPasswordResetState(user.id)
      reply.code(400)
      return { ok: false, error: 'reset_code_expired' }
    }

    if (resetState.code !== code) {
      reply.code(400)
      return { ok: false, error: 'invalid_reset_code' }
    }

    const { passwordHash, passwordSalt } = hashPassword(newPassword)
    const updated = userDB.updatePassword(user.id, passwordHash, passwordSalt)
    clearPasswordResetState(user.id)

    if (!updated) {
      reply.code(500)
      return { ok: false, error: 'password_update_failed' }
    }

    return { ok: true }
  })

  fastify.post('/api/auth/change-password', async (request, reply) => {
    const session = requireSession(request, reply)
    if (!session) {
      return { ok: false, error: 'unauthorized' }
    }

    const payload = request.body || {}
    const currentPassword = String(payload.currentPassword || '')
    const nextPassword = String(payload.newPassword || payload.nextPassword || '')

    if (!currentPassword || !nextPassword || nextPassword.length < 6) {
      reply.code(400)
      return { ok: false, error: 'invalid_payload' }
    }

    const user = userDB.findById(session.userId)
    if (!user) {
      reply.code(401)
      return { ok: false, error: 'unauthorized' }
    }

    const currentValid = verifyPassword(currentPassword, user.passwordHash, user.passwordSalt)
    if (!currentValid) {
      reply.code(401)
      return { ok: false, error: 'invalid_current_password' }
    }

    const { passwordHash, passwordSalt } = hashPassword(nextPassword)
    const updated = userDB.updatePassword(user.id, passwordHash, passwordSalt)
    if (!updated) {
      reply.code(500)
      return { ok: false, error: 'update_failed' }
    }
    reply.code(200)
    return { ok: true }
  })

  fastify.post('/api/session/init', async request => {
    const payload = request.body || {}
    const profileId = String(payload.profileId || 'default') || 'default'
    const token = createSessionToken()
    const expiresAt = new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString()
    sessionDB.create({
      token,
      userId: 'session-init',
      profileId,
      email: 'session@booktnow.local',
      expiresAt
    })
    return { token, profileId }
  })

  fastify.post('/api/auth/logout', async (request, reply) => {
    const session = requireSession(request, reply)
    if (!session) {
      return { error: 'unauthorized' }
    }

    const token = getRequestToken(request)
    if (token) {
      const device = deviceDB.findBySessionToken(token)
      if (device) {
        deviceDB.clearSessionToken(token)
        deviceDB.upsert({
          ...device,
          sessionToken: null,
          current: 0,
          lastSeenAt: new Date().toISOString()
        })
      }
      sessionDB.deleteByToken(token)
    }
    reply.code(200)
    return { ok: true }
  })

  fastify.get('/api/devices', async (request, reply) => {
    const session = requireSession(request, reply)
    if (!session) {
      return { error: 'unauthorized' }
    }
    reply.code(200)
    return {
      devices: deviceDB.findByUserId(session.userId).map(({ sessionToken, ...device }) => device)
    }
  })

  fastify.delete('/api/devices/:id', async (request, reply) => {
    const session = requireSession(request, reply)
    if (!session) {
      return { error: 'unauthorized' }
    }

    const { id } = request.params
    const device = deviceDB.findById(id, session.userId)
    if (!device) {
      reply.code(404)
      return { ok: false, error: 'not_found' }
    }

    const currentDeviceRemoved = Boolean(device.current)
    if (currentDeviceRemoved && device.sessionToken) {
      sessionDB.deleteByToken(device.sessionToken)
      deviceDB.clearSessionToken(device.sessionToken)
    }
    const success = deviceDB.deleteById(id, session.userId)
    if (!success) {
      reply.code(404)
      return { ok: false, error: 'not_found' }
    }
    reply.code(200)
    return { ok: true, currentDeviceRemoved }
  })
}
