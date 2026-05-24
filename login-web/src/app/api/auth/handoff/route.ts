import { NextResponse } from "next/server";

import { createAuthHandoffToken } from "@/lib/auth-handoff";
import { verifyDashboardLogin } from "@/lib/supabase";

export async function POST(request: Request) {
  try {
    const body = (await request.json()) as {
      username?: string;
      password?: string;
    };
    const username = String(body.username ?? "").trim();
    const password = String(body.password ?? "");

    if (!username || !password) {
      return NextResponse.json(
        { ok: false, error: "missing_credentials" },
        { status: 400 },
      );
    }

    const result = await verifyDashboardLogin(username, password);
    if (!result.ok) {
      return NextResponse.json(
        { ok: false, error: "invalid_credentials" },
        { status: 401 },
      );
    }

    const uname = result.username ?? username.toLowerCase();
    const op = result.operator_id ?? uname;
    const auth = createAuthHandoffToken(uname, op);

    return NextResponse.json({ ok: true, auth, username: uname, operator_id: op });
  } catch (err) {
    const message = err instanceof Error ? err.message : "handoff_failed";
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}
