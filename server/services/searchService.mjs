import {
  buildAffiliateFlightBookingLink,
  buildRealPartnerSearchPayload,
  normalizePartnerCategory,
} from "./partnerRuntimeService.mjs";

export function normalizeSearchCategory(category = "hotels") {
  return normalizePartnerCategory(category);
}

function normalizeSearchMeta(query = {}, source) {
  return {
    source,
    query: String(query.query || ""),
    category: normalizeSearchCategory(String(query.category || "hotels")),
    destination: String(query.destination || ""),
    passengers: Number(query.passengers || 1),
    budget: Number(query.budget || 0),
    sortBy: String(query.sortBy || "recommended"),
    count: 0,
    total: 0,
  };
}

export function buildPartnerDataPendingSearchPayload(query = {}) {
  return {
    results: [],
    meta: normalizeSearchMeta(query, "partner_data_pending"),
    insights: {
      status: 204,
      reason: "partner_data_pending",
    },
  };
}

export function buildNoRealPartnerDataSearchPayload(query = {}) {
  return {
    results: [],
    meta: normalizeSearchMeta(query, "no_real_partner_data"),
    insights: {
      status: 204,
      reason: "no_real_partner_data",
    },
  };
}

export function buildRealPartnerDataSearchPayload(query = {}, requestContext = {}) {
  return buildRealPartnerSearchPayload(query, requestContext);
}

export function buildFlightBookingLinkPayload(payload = {}, requestContext = {}) {
  return buildAffiliateFlightBookingLink(payload, requestContext);
}
