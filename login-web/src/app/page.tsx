import { BackgroundPaths } from "@/components/ui/background-paths";
import { LoginForm } from "@/components/login-form";

export default function LoginPage() {
  return (
    <BackgroundPaths title="Field Ticket Operations">
      <div className="mx-auto max-w-sm">
        <p className="mb-6 text-sm text-muted-foreground">
          Ticket responses dashboard — sign in with your team account.
        </p>
        <LoginForm />
      </div>
    </BackgroundPaths>
  );
}
