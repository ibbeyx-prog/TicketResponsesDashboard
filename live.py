"""
Entry point for running the Ticket Responses Dashboard in production or locally.

Railway sets PORT; default 8501 for local runs.
"""

import os
import sys


def main() -> None:
    port = os.environ.get("PORT", "8501")
    args = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        "dashboard.py",
        "--server.port",
        str(port),
        "--server.address",
        "0.0.0.0",
        "--browser.gatherUsageStats",
        "false",
    ]
    os.execvp(sys.executable, args)


if __name__ == "__main__":
    main()
