import { useEffect, useRef } from "react";
import { NUMERIC_WINDOW_S } from "../store";

interface Props {
  label: string;
  unit: string;
  series: [number, number][];
  horizon: number | null;
  color: string;
}

export default function NumericTile({ label, unit, series, horizon, color }: Props) {
  const ref = useRef<HTMLCanvasElement>(null);
  const last = series.length ? series[series.length - 1][1] : null;

  useEffect(() => {
    const canvas = ref.current;
    if (!canvas) return;
    const dpr = window.devicePixelRatio || 1;
    const w = canvas.clientWidth, h = canvas.clientHeight;
    if (canvas.width !== w * dpr || canvas.height !== h * dpr) { canvas.width = w * dpr; canvas.height = h * dpr; }
    const ctx = canvas.getContext("2d")!;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, w, h);
    if (!horizon || series.length < 2) return;
    const t1 = horizon, t0 = t1 - NUMERIC_WINDOW_S;
    let lo = Infinity, hi = -Infinity;
    for (const [t, v] of series) { if (t >= t0) { if (v < lo) lo = v; if (v > hi) hi = v; } }
    if (!isFinite(lo)) return;
    if (hi - lo < 1e-9) { lo -= 1; hi += 1; }
    const pad = (hi - lo) * 0.15; lo -= pad; hi += pad;
    ctx.strokeStyle = color; ctx.lineWidth = 1.25; ctx.beginPath();
    let started = false;
    for (const [t, v] of series) {
      if (t < t0) continue;
      const x = ((t - t0) / (t1 - t0)) * w, y = h - ((v - lo) / (hi - lo)) * h;
      if (!started) { ctx.moveTo(x, y); started = true; } else ctx.lineTo(x, y);
    }
    ctx.stroke();
  }, [series, horizon, color]);

  return (
    <div className="tile">
      <div className="tile-head">
        <span className="tile-label">{label}</span>
        <span className="tile-unit">{unit}</span>
      </div>
      <div className="tile-value" style={{ color }}>{last == null ? "--" : fmtValue(last)}</div>
      <canvas ref={ref} className="spark" />
    </div>
  );
}

function fmtValue(v: number): string {
  return Number.isInteger(v) ? v.toString() : Math.abs(v) >= 100 ? v.toFixed(0) : v.toFixed(1);
}
