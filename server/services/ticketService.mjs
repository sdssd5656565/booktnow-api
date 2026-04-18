/* ══════════════��════════════════════════════════════════════════
   ticketService.mjs — خدمة إدارة تذاكر خدمة العملاء
   ═══════════════════════════════════════════════════════════════ */

/**
 * إنشاء تذكرة جديدة مع رسالة أولى
 */
export function createTicket(deps, userId, data) {
  const { ticketDB, ticketMessageDB, referralDB, randomUUID } = deps
  const ticketId = `TK-${Date.now().toString(36).toUpperCase()}-${randomUUID().slice(0, 4).toUpperCase()}`
  const messageId = `msg_${randomUUID().replace(/-/g, '')}`

  // إذا فيه referralId نسحب بيانات الإحالة
  let referralData = null
  if (data.referralId && userId) {
    referralData = referralDB.findById(data.referralId, userId)
  }

  const ticket = ticketDB.create({
    id: ticketId,
    userId,
    type: data.type || 'other',
    category: data.category || referralData?.category || null,
    partnerId: data.partnerId || referralData?.partnerId || null,
    partnerName: data.partnerName || referralData?.partnerName || null,
    referralId: data.referralId || null,
    status: 'open',
    priority: data.priority || 'medium',
    summary: data.summary || null,
    payload: {
      ...(data.payload || {}),
      referralSnapshot: referralData || null,
      displayedPrice: data.displayedPrice || null,
      actualPrice: data.actualPrice || null,
      description: data.description || null,
    }
  })

  // الرسالة الأولى من العميل
  if (data.description) {
    ticketMessageDB.create({
      id: messageId,
      ticketId,
      sender: 'customer',
      content: data.description,
      metadata: { type: 'initial_report' }
    })
  }

  return ticket
}

/**
 * إضافة رسالة إلى تذكرة موجودة
 */
export function addTicketMessage(deps, ticketId, userId, sender, content, metadata) {
  const { ticketDB, ticketMessageDB, randomUUID } = deps
  const ticket = ticketDB.findById(ticketId, userId)
  if (!ticket) return null

  const messageId = `msg_${randomUUID().replace(/-/g, '')}`
  return ticketMessageDB.create({
    id: messageId,
    ticketId,
    sender,
    content,
    metadata: metadata || {}
  })
}

/**
 * تحديث حالة التذكرة
 */
export function updateTicketStatus(deps, ticketId, userId, status, resolution) {
  const { ticketDB } = deps
  return ticketDB.updateStatus(ticketId, userId, status, resolution)
}

/**
 * جلب تذاكر المستخدم
 */
export function getUserTickets(deps, userId) {
  const { ticketDB } = deps
  return ticketDB.findByUserId(userId)
}

/**
 * جلب تذكرة واحدة مع رسائله��
 */
export function getTicketWithMessages(deps, ticketId, userId) {
  const { ticketDB, ticketMessageDB } = deps
  const ticket = ticketDB.findById(ticketId, userId)
  if (!ticket) return null
  const messages = ticketMessageDB.findByTicketId(ticketId)
  return { ...ticket, messages }
}

/**
 * جلب التذاكر المفتوحة
 */
export function getOpenTickets(deps, userId) {
  const { ticketDB } = deps
  return ticketDB.findOpenByUserId(userId)
}
