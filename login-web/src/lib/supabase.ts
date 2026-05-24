import { createClient, type SupabaseClient } from "@supabase/supabase-js";

let client: SupabaseClient | null = null;

export function getSupabase(): SupabaseClient {
  if (client) return client;

  const url = process.env.NEXT_PUBLIC_SUPABASE_URL;
  const key =
    process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY ??
    process.env.NEXT_PUBLIC_SUPABASE_KEY;

  if (!url || !key) {
    throw new Error(
      "Set NEXT_PUBLIC_SUPABASE_URL and NEXT_PUBLIC_SUPABASE_ANON_KEY in login-web/.env.local",
    );
  }

  client = createClient(url, key);
  return client;
}

export type DashboardLoginResult = {
  ok: boolean;
  username?: string;
  operator_id?: string;
  error?: string;
};

export async function verifyDashboardLogin(
  username: string,
  password: string,
): Promise<DashboardLoginResult> {
  const supabase = getSupabase();
  const { data, error } = await supabase.rpc("dashboard_verify_login", {
    p_username: username.trim().toLowerCase(),
    p_password: password,
  });

  if (error) {
    throw new Error(error.message);
  }

  return (data ?? { ok: false }) as DashboardLoginResult;
}
