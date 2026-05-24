"use client";

import { useState } from "react";
import { Loader2, LogIn } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";

const REMEMBER_KEY = "bon_dashboard_remember";

export function LoginForm() {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [remember, setRemember] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  async function onSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError(null);
    setLoading(true);

    try {
      if (remember && typeof window !== "undefined") {
        localStorage.setItem(
          REMEMBER_KEY,
          JSON.stringify({ username: username.trim().toLowerCase() }),
        );
      } else if (typeof window !== "undefined") {
        localStorage.removeItem(REMEMBER_KEY);
      }

      const handoffRes = await fetch("/api/auth/handoff", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ username, password }),
      });
      const handoff = (await handoffRes.json()) as {
        ok?: boolean;
        auth?: string;
      };
      if (handoffRes.status === 401) {
        setError("Incorrect username or password.");
        return;
      }
      if (!handoffRes.ok || !handoff.ok || !handoff.auth) {
        setError("Could not start dashboard session.");
        return;
      }

      const dashboardUrl =
        process.env.NEXT_PUBLIC_DASHBOARD_URL ?? "http://localhost:8501";
      const params = new URLSearchParams({ _auth: handoff.auth });
      window.location.href = `${dashboardUrl.replace(/\/$/, "")}?${params.toString()}`;
    } catch (err) {
      setError(
        err instanceof Error ? err.message : "Could not verify login.",
      );
    } finally {
      setLoading(false);
    }
  }

  return (
    <div
      className="w-full max-w-sm rounded-2xl border border-white/10 bg-black/50 p-6 shadow-xl backdrop-blur-md"
      style={{ boxShadow: "0 0 40px rgba(0, 179, 198, 0.08)" }}
    >
      <form onSubmit={onSubmit} className="space-y-4 text-left">
        <div className="space-y-2">
          <Label htmlFor="username" className="text-foreground/90">
            Username
          </Label>
          <Input
            id="username"
            name="username"
            autoComplete="username"
            placeholder="your login name"
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            required
          />
        </div>

        <div className="space-y-2">
          <Label htmlFor="password" className="text-foreground/90">
            Password
          </Label>
          <Input
            id="password"
            name="password"
            type="password"
            autoComplete="current-password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            required
          />
        </div>

        <label className="flex cursor-pointer items-center gap-2 text-sm text-muted-foreground">
          <input
            type="checkbox"
            checked={remember}
            onChange={(e) => setRemember(e.target.checked)}
            className="rounded border-input accent-bon-cyan"
          />
          Save username on this device
        </label>

        {error ? (
          <p className="text-sm text-bon-red" role="alert">
            {error}
          </p>
        ) : null}

        <div className="inline-block w-full rounded-2xl bg-gradient-to-b from-bon-orange/20 to-bon-cyan/10 p-px">
          <Button
            type="submit"
            disabled={loading}
            className="h-11 w-full rounded-[0.95rem] border border-white/10 bg-black/80 text-base font-semibold hover:bg-black"
          >
            {loading ? (
              <Loader2 className="mr-2 h-4 w-4 animate-spin" />
            ) : (
              <LogIn className="mr-2 h-4 w-4" />
            )}
            Sign In
          </Button>
        </div>
      </form>

      <p className="mt-4 text-center text-xs text-muted-foreground">
        <a
          href={`${process.env.NEXT_PUBLIC_DASHBOARD_URL ?? "http://localhost:8501"}?native=1`}
          className="text-bon-cyan hover:underline"
        >
          Forgot password
        </a>
      </p>
    </div>
  );
}
