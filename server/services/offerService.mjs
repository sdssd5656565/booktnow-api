import { buildRealPartnerOffersPayload, normalizePartnerCategory } from "./partnerRuntimeService.mjs";

function normalizeOfferMeta(query = {}, source) {
  return {
    source,
    category: normalizePartnerCategory(String(query.category || "hotels")),
    productId: String(query.productId || ""),
    count: 0,
  };
}

export function buildPartnerDataPendingOffersPayload(query = {}) {
  return {
    offers: [],
    meta: normalizeOfferMeta(query, "partner_data_pending"),
    insights: {
      status: 204,
      reason: "partner_data_pending",
    },
  };
}

export function buildNoRealPartnerDataOffersPayload(query = {}) {
  return {
    offers: [],
    meta: normalizeOfferMeta(query, "no_real_partner_data"),
    insights: {
      status: 204,
      reason: "no_real_partner_data",
    },
  };
}

export function buildRealPartnerDataOffersPayload(query = {}) {
  return buildRealPartnerOffersPayload(query);
}
