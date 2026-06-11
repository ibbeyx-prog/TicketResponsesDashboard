/** Digits-only ticket id when 9 or 16 digits (matches Streamlit lookup). */
export function normalizeTicketLookup(raw: string): string {
  const digits = raw.replace(/\D/g, "");
  if (digits.length === 9 || digits.length === 16) return digits;
  return "";
}
