import {
  markReferralHandover,
  markReferralConversion,
  createReferral,
} from "../services/handoffService.mjs";

export function registerPartnerRoutes(fastify, deps) {
  const {
    requireSession,
    randomUUID,
    bookingDB,
    favoriteDB,
    referralDB,
    eventDB,
    identityDB,
    watchDB,
    serializeReferral,
    serializeEvent,
  } = deps;

  fastify.get("/api/bookings", async (request, reply) => {
    const session = requireSession(request, reply);
    if (!session) {
      return { error: "unauthorized" };
    }
    return bookingDB.findByUserId(session.userId);
  });

  fastify.post("/api/bookings", async (request, reply) => {
    const session = requireSession(request, reply);
    if (!session) {
      return { error: "unauthorized" };
    }

    const payload = request.body || {};
    const booking = bookingDB.create({
      id: payload.id || `booking_${randomUUID().replace(/-/g, "")}`,
      userId: session.userId,
      category: payload.productCategory || payload.category || "hotels",
      destination: payload.productLocation || payload.destination || "",
      productName: payload.productName || payload.name || "",
      provider: payload.providerName || payload.provider || "",
      price: Number(payload.total ?? payload.price ?? 0),
      payload,
    });
    reply.code(201);
    return { booking };
  });

  fastify.get("/api/favorites", async (request, reply) => {
    const session = requireSession(request, reply);
    if (!session) {
      return { error: "unauthorized" };
    }

    return favoriteDB.findByUserId(session.userId);
  });

  fastify.put("/api/favorites", async (request, reply) => {
    const session = requireSession(request, reply);
    if (!session) {
      return { error: "unauthorized" };
    }

    const payload = request.body || {};
    return favoriteDB.replaceForUser(
      session.userId,
      payload.items || payload.favorites || [],
    );
  });

  fastify.post("/api/referrals", async (request, reply) => {
    const session = requireSession(request, reply);
    if (!session) {
      return { error: "unauthorized" };
    }

    const payload = request.body || {};
    const result = createReferral(
      referralDB,
      payload,
      session.userId,
      randomUUID,
      serializeReferral,
    );
    reply.code(201);
    return result;
  });

  fastify.post("/api/referrals/:id/handover", async (request, reply) => {
    const session = requireSession(request, reply);
    if (!session) {
      return { error: "unauthorized" };
    }

    const { id } = request.params;
    const result = markReferralHandover(
      referralDB,
      id,
      session.userId,
      serializeReferral,
    );
    if (!result.ok) {
      reply.code(404);
    }
    return result;
  });

  fastify.post("/api/referrals/:id/conversion", async (request, reply) => {
    const session = requireSession(request, reply);
    if (!session) {
      return { error: "unauthorized" };
    }

    const { id } = request.params;
    const payload = request.body || {};
    const result = markReferralConversion(
      referralDB,
      id,
      session.userId,
      Number(payload.commission || payload.commissionAmount || 0),
      serializeReferral,
    );
    if (!result.ok) {
      reply.code(404);
    }
    return result;
  });

  fastify.post("/api/events", async (request, reply) => {
    const session = requireSession(request, reply);
    if (!session) {
      return { error: "unauthorized" };
    }

    const payload = request.body || {};
    const event = eventDB.create({
      id: `evt_${randomUUID().replace(/-/g, "")}`,
      userId: session.userId,
      name: String(payload.name || ""),
      payload: payload.payload || {},
      eventAt: Number(payload.at || Date.now()),
    });
    reply.code(201);
    return { event: serializeEvent(event) };
  });

  fastify.put("/api/identity", async (request, reply) => {
    const session = requireSession(request, reply);
    if (!session) {
      return { error: "unauthorized" };
    }

    const payload = request.body || {};
    identityDB.upsert(session.userId, payload.identity || {});
    return { identity: identityDB.get(session.userId) || {} };
  });

  fastify.get("/api/identity", async (request, reply) => {
    const session = requireSession(request, reply);
    if (!session) {
      return { error: "unauthorized" };
    }

    return { identity: identityDB.get(session.userId) || {} };
  });

  fastify.get("/api/watch", async (request, reply) => {
    const session = requireSession(request, reply);
    if (!session) {
      return { error: "unauthorized" };
    }

    return watchDB.findByUserId(session.userId);
  });

  fastify.post("/api/watch", async (request, reply) => {
    const session = requireSession(request, reply);
    if (!session) {
      return { error: "unauthorized" };
    }

    const payload = request.body || {};
    const watch = watchDB.create({
      id: `watch_${randomUUID().replace(/-/g, "")}`,
      userId: session.userId,
      payload,
    });
    reply.code(201);
    return { ok: true, watch };
  });

  fastify.delete("/api/watch/:id", async (request, reply) => {
    const session = requireSession(request, reply);
    if (!session) {
      return { error: "unauthorized" };
    }

    const { id } = request.params;
    const success = watchDB.deleteById(id, session.userId);
    reply.code(200);
    return { ok: success };
  });
}
