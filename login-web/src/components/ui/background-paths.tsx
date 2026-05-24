"use client";

import type { ReactNode } from "react";
import { motion } from "framer-motion";

import { Button } from "@/components/ui/button";

/** BON brand palette for animated path strokes */
const BON_PATH_COLORS = [
  "rgb(241, 90, 41)", /* #F15A29 orange */
  "rgb(0, 179, 198)", /* #00B3C6 cyan_teal */
  "rgb(226, 35, 26)", /* #E2231A red */
  "rgb(247, 147, 30)", /* #F7931E light_orange */
] as const;

function FloatingPaths({ position }: { position: number }) {
  const paths = Array.from({ length: 36 }, (_, i) => ({
    id: i,
    d: `M-${380 - i * 5 * position} -${189 + i * 6}C-${
      380 - i * 5 * position
    } -${189 + i * 6} -${312 - i * 5 * position} ${216 - i * 6} ${
      152 - i * 5 * position
    } ${343 - i * 6}C${616 - i * 5 * position} ${470 - i * 6} ${
      684 - i * 5 * position
    } ${875 - i * 6} ${684 - i * 5 * position} ${875 - i * 6}`,
    stroke: BON_PATH_COLORS[i % BON_PATH_COLORS.length],
    width: 0.5 + i * 0.03,
    opacity: 0.22 + (i % 9) * 0.04,
  }));

  return (
    <motion.div className="pointer-events-none absolute inset-0">
      <svg className="h-full w-full" viewBox="0 0 696 316" fill="none">
        <title>Background Paths</title>
        {paths.map((path) => (
          <motion.path
            key={path.id}
            d={path.d}
            stroke={path.stroke}
            strokeWidth={path.width}
            strokeOpacity={path.opacity}
            initial={{ pathLength: 0.3, opacity: 0.6 }}
            animate={{
              pathLength: 1,
              opacity: [0.3, 0.6, 0.3],
              pathOffset: [0, 1, 0],
            }}
            transition={{
              duration: 20 + (path.id % 10),
              repeat: Number.POSITIVE_INFINITY,
              ease: "linear",
            }}
          />
        ))}
      </svg>
    </motion.div>
  );
}

const BON_TITLE_WORD_COLORS = {
  field: "#F15A29",
  ticket: "#F7931E",
  operations: "#00B3C6",
} as const;

function splitLoginTitleWords(title: string): string[] {
  return title.trim().split(/\s+/).filter(Boolean);
}

export function BackgroundPaths({
  title = "Field Ticket Operations",
  children,
}: {
  title?: string;
  children?: ReactNode;
}) {
  const words = splitLoginTitleWords(title);
  return (
    <motion.div className="relative flex min-h-screen w-full items-center justify-center overflow-hidden bg-black">
      <div className="absolute inset-0">
        <FloatingPaths position={1} />
        <FloatingPaths position={-1} />
      </div>

      <div className="container relative z-10 mx-auto px-4 text-center md:px-6">
        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          transition={{ duration: 2 }}
          className="mx-auto max-w-4xl"
        >
          <motion.h1
            initial={{ y: 48, opacity: 0 }}
            animate={{ y: 0, opacity: 1 }}
            transition={{
              duration: 0.9,
              type: "spring",
              stiffness: 120,
              damping: 22,
            }}
            className="mb-8 inline-block max-w-full text-4xl font-bold tracking-tight sm:text-5xl md:text-6xl lg:text-7xl"
          >
            {words.map((word, i) => {
              const key = word.toLowerCase();
              const color =
                key === "field"
                  ? BON_TITLE_WORD_COLORS.field
                  : key === "ticket"
                    ? BON_TITLE_WORD_COLORS.ticket
                    : key === "operations"
                      ? BON_TITLE_WORD_COLORS.operations
                      : "#ffffff";
              return (
                <span key={`${word}-${i}`} style={{ color }}>
                  {i > 0 ? " " : ""}
                  {word}
                </span>
              );
            })}
          </motion.h1>

          {children ?? (
            <div className="group relative inline-block overflow-hidden rounded-2xl bg-gradient-to-b from-white/10 to-black/10 p-px shadow-lg backdrop-blur-lg transition-shadow duration-300 hover:shadow-xl">
              <Button
                variant="ghost"
                className="rounded-[1.15rem] border border-white/10 bg-black/95 px-8 py-6 text-lg font-semibold text-white backdrop-blur-md transition-all duration-300 hover:bg-black group-hover:-translate-y-0.5 hover:shadow-neutral-800/50"
              >
                <span className="opacity-90 transition-opacity group-hover:opacity-100">
                  Discover Excellence
                </span>
                <span className="ml-3 opacity-70 transition-all duration-300 group-hover:translate-x-1.5 group-hover:opacity-100">
                  →
                </span>
              </Button>
            </div>
          )}
        </motion.div>
      </div>
    </motion.div>
  );
}
