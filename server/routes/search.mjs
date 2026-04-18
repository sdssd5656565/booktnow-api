export function registerSearchRoutes(fastify, deps) {
  const { buildSearchRuntimePayload, buildFlightBookingLinkPayload } = deps;

  fastify.get("/api/search", async (request) => {
    return buildSearchRuntimePayload(request.query || {}, {
      userIp: request.ip,
      forwardedFor: request.headers["x-forwarded-for"],
      host: request.headers.host,
      protocol: request.protocol,
      userAgent: request.headers["user-agent"],
    });
  });

  fastify.post("/api/search", async (request) => {
    return buildSearchRuntimePayload(request.body || {}, {
      userIp: request.ip,
      forwardedFor: request.headers["x-forwarded-for"],
      host: request.headers.host,
      protocol: request.protocol,
      userAgent: request.headers["user-agent"],
    });
  });

  fastify.post("/api/flights/booking-link", async (request, reply) => {
    if (typeof buildFlightBookingLinkPayload !== "function") {
      reply.code(503);
      return { ok: false, reason: "flight_booking_link_unavailable" };
    }

    const result = await buildFlightBookingLinkPayload(request.body || {}, {
      userIp: request.ip,
      forwardedFor: request.headers["x-forwarded-for"],
      host: request.headers.host,
      protocol: request.protocol,
      userAgent: request.headers["user-agent"],
    });

    if (!result?.ok) {
      reply.code(400);
      return result;
    }

    return result;
  });
}
