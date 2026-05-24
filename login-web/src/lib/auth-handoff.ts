import crypto from "crypto";

function signingKey(): Buffer {
  const pepper =
    process.env.DASHBOARD_SESSION_SECRET ??
    process.env.SUPABASE_KEY ??
    "ticket-dashboard-remember";
  return crypto.createHash("sha256").update(pepper).digest();
}

/** Signed token Streamlit ``_try_react_auth_handoff`` can verify (no Fernet). */
export function createAuthHandoffToken(
  username: string,
  operatorId: string,
): string {
  const payload = JSON.stringify({
    u: username,
    op: operatorId,
    ts: Date.now() / 1000,
  });
  const body = Buffer.from(payload, "utf8").toString("base64url");
  const sig = crypto.createHmac("sha256", signingKey()).update(body).digest("hex");
  return `${body}.${sig}`;
}
