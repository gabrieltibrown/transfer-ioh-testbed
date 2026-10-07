interface Props {
  label: string;
  value: string;
  tone?: "ok" | "warn" | "bad" | "muted";
  hint?: string;
}

export default function Indicator({ label, value, tone = "muted", hint }: Props) {
  return (
    <div className={`ind ind-${tone}`} title={hint}>
      <div className="ind-label">{label}</div>
      <div className="ind-value">{value}</div>
    </div>
  );
}

export function fmtS(v: number | null | undefined, digits = 2): string {
  if (v == null || Number.isNaN(v)) return "--";
  if (Math.abs(v) < 1) return `${(v * 1000).toFixed(0)} ms`;
  return `${v.toFixed(digits)} s`;
}

export function fmtPct(v: number | null | undefined): string {
  if (v == null || Number.isNaN(v)) return "--";
  return `${(v * 100).toFixed(v >= 0.995 ? 0 : 1)}%`;
}

export function toneFor(v: number | null | undefined, warn: number, bad: number, invert = false): Props["tone"] {
  if (v == null || Number.isNaN(v)) return "muted";
  if (invert) return v >= warn ? "ok" : v >= bad ? "warn" : "bad";
  return v <= warn ? "ok" : v <= bad ? "warn" : "bad";
}
