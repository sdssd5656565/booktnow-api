export function registerOffersRoutes(fastify, deps) {
  const { buildOffersRuntimePayload } = deps;

  fastify.get("/api/offers", async (request) => {
    return buildOffersRuntimePayload(request.query || {});
  });
}
