const HUNT_SCORE_LOGIC_COOKIE_PREFIX = "hunt-score-logic-";

export function getHuntScoreLogicCookieName(storeId) {
  return `${HUNT_SCORE_LOGIC_COOKIE_PREFIX}${String(storeId ?? "").trim()}`;
}

export function encodeHuntScoreLogicCookieValue(logicKey) {
  return encodeURIComponent(String(logicKey ?? "").trim());
}

export function decodeHuntScoreLogicCookieValue(value) {
  try {
    return decodeURIComponent(String(value ?? "").trim());
  } catch {
    return String(value ?? "").trim();
  }
}
