/* ════���══════════════════════════════════════════════════════════
   tickets.mjs — مسارات API لتذاكر خدمة العملاء
   ═════════════════════════════════════════════════���═════════════ */

import {
  createTicket,
  addTicketMessage,
  updateTicketStatus,
  getUserTickets,
  getTicketWithMessages,
  getOpenTickets,
} from '../services/ticketService.mjs'

export function registerTicketRoutes(fastify, deps) {
  const { requireSession } = deps

  // إنشاء تذكرة جديدة
  fastify.post('/api/tickets', async (request, reply) => {
    const session = requireSession(request, reply)
    if (!session) return { error: 'unauthorized' }

    const body = request.body || {}
    const ticket = createTicket(deps, session.userId, {
      type: body.type,
      category: body.category,
      partnerId: body.partnerId,
      partnerName: body.partnerName,
      referralId: body.referralId,
      priority: body.priority,
      summary: body.summary,
      description: body.description,
      displayedPrice: body.displayedPrice,
      actualPrice: body.actualPrice,
      payload: body.payload,
    })

    return ticket || { error: 'failed_to_create_ticket' }
  })

  // جلب كل التذاكر
  fastify.get('/api/tickets', async (request, reply) => {
    const session = requireSession(request, reply)
    if (!session) return { error: 'unauthorized' }

    const status = request.query?.status
    if (status === 'open') {
      return getOpenTickets(deps, session.userId)
    }
    return getUserTickets(deps, session.userId)
  })

  // جلب تذكرة واحدة مع رسائلها
  fastify.get('/api/tickets/:id', async (request, reply) => {
    const session = requireSession(request, reply)
    if (!session) return { error: 'unauthorized' }

    const ticket = getTicketWithMessages(deps, request.params.id, session.userId)
    if (!ticket) {
      reply.code(404)
      return { error: 'ticket_not_found' }
    }
    return ticket
  })

  // إضافة رسالة إلى تذكرة
  fastify.post('/api/tickets/:id/messages', async (request, reply) => {
    const session = requireSession(request, reply)
    if (!session) return { error: 'unauthorized' }

    const body = request.body || {}
    const message = addTicketMessage(
      deps,
      request.params.id,
      session.userId,
      body.sender || 'customer',
      body.content || '',
      body.metadata
    )

    if (!message) {
      reply.code(404)
      return { error: 'ticket_not_found' }
    }
    return message
  })

  // تحديث حالة التذكرة
  fastify.put('/api/tickets/:id/status', async (request, reply) => {
    const session = requireSession(request, reply)
    if (!session) return { error: 'unauthorized' }

    const body = request.body || {}
    const updated = updateTicketStatus(
      deps,
      request.params.id,
      session.userId,
      body.status,
      body.resolution
    )

    if (!updated) {
      reply.code(404)
      return { error: 'ticket_not_found' }
    }
    return { ok: true }
  })
}
