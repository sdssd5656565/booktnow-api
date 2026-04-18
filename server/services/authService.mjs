export function normalizeEmail(value = "") {
  return String(value || "")
    .trim()
    .toLowerCase();
}

export function issuePasswordResetCode(identityDB, userId) {
  const currentIdentity = identityDB.get(userId) || {};
  const requestedAt = Date.now();
  const expiresAt = requestedAt + 15 * 60 * 1000;
  const resetCode = String(
    Math.floor(100000 + Math.random() * 900000),
  ).padStart(6, "0");
  identityDB.upsert(userId, {
    ...currentIdentity,
    passwordReset: {
      code: resetCode,
      requestedAt,
      expiresAt,
    },
  });
  return { resetCode, requestedAt, expiresAt };
}

export function readPasswordResetState(identityDB, userId) {
  const identity = identityDB.get(userId) || {};
  const passwordReset =
    identity &&
    typeof identity.passwordReset === "object" &&
    identity.passwordReset
      ? identity.passwordReset
      : null;
  if (!passwordReset) return null;
  return {
    identity,
    code: String(passwordReset.code || ""),
    requestedAt: Number(passwordReset.requestedAt) || 0,
    expiresAt: Number(passwordReset.expiresAt) || 0,
  };
}

export function clearPasswordResetState(identityDB, userId) {
  const identity = identityDB.get(userId) || {};
  if (!identity || typeof identity !== "object" || !identity.passwordReset) {
    return false;
  }
  const nextIdentity = { ...identity };
  delete nextIdentity.passwordReset;
  identityDB.upsert(userId, nextIdentity);
  return true;
}

export function buildPasswordResetRequestResponse({ requestedAt, expiresAt }) {
  return {
    ok: true,
    requestedAt,
    expiresInMs: Math.max(0, expiresAt - requestedAt),
  };
}
