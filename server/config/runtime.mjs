function toNumber(value, fallback) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

export function resolveBackendRuntime(localDefaults = {}) {
  const defaultHost = String(localDefaults.host || "0.0.0.0");
  const defaultPort = toNumber(localDefaults.backendPort, 3000);

  // cPanel (Phusion Passenger) and most PaaS providers inject PORT.
  // Fall back to BACKEND_PORT (our dev convention) then to the local default.
  const resolvedPort = toNumber(
    process.env.PORT,
    toNumber(process.env.BACKEND_PORT, defaultPort)
  );

  return {
    nodeEnv: String(process.env.NODE_ENV || "development"),
    host: String(process.env.BACKEND_HOST || defaultHost),
    port: resolvedPort,
  };
}
