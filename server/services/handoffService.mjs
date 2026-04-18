export function markReferralHandover(
  referralDB,
  id,
  userId,
  serializeReferral,
) {
  const referral = referralDB.markHandover(id, userId);
  if (!referral) {
    return { ok: false, error: "not_found", referral: null };
  }
  return {
    ok: true,
    error: null,
    referral: serializeReferral(referral),
  };
}

export function markReferralConversion(
  referralDB,
  id,
  userId,
  commission,
  serializeReferral,
) {
  const referral = referralDB.markConversion(
    id,
    userId,
    Number(commission || 0),
  );
  if (!referral) {
    return { ok: false, error: "not_found", referral: null };
  }
  return {
    ok: true,
    error: null,
    referral: serializeReferral(referral),
  };
}

export function createReferral(
  referralDB,
  payload,
  userId,
  randomUUID,
  serializeReferral,
) {
  const referral = referralDB.create({
    referralId: payload.referralId || `ref_${randomUUID().replace(/-/g, "")}`,
    userId,
    partnerId: String(payload.partnerId || ""),
    partnerName: String(payload.partnerName || ""),
    productId: String(payload.productId || ""),
    productName: String(payload.productName || ""),
    amount: Number(payload.amount || 0),
    currency: String(payload.currency || "SAR"),
    commissionModel: String(payload.commissionModel || "cps"),
    commissionRate: Number(payload.commissionRate || 0.05),
    status: "created",
    commission: 0,
  });
  return { referral: serializeReferral(referral) };
}
