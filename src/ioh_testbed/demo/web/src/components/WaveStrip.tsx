import { useEffect, useRef } from "react";
import type { WavePoint } from "../api";
import { WAVE_WINDOW_S } from "../store";

interface Props {
  label: string;
  unit: string;
  points: WavePoint[];
  horizon: number | null; // newest event time of the bed; the strip ends here
  color: string;
}

// A monitor-style strip: the newest sample is at the right edge, 10 s visible,
// drawn as a min/max band from the tap's 10 ms buckets with gaps where samples
// were invalid or unavailable.
export default function WaveStrip({ label, unit, points, horizon, color }: Props) {
  const ref = useRef<HTMLCanvasElement>(null);

  useEffect(() => {
    const canvas = ref.current;
    if (!canvas) return;
    const dpr = window.devicePixelRatio || 1;
    const w = canvas.clientWidth, h = canvas.clientHeight;
    if (canvas.width !== w * dpr || canvas.height !== h * dpr) {
      canvas.width = w * dpr;
      canvas.height = h * dpr;
    }
    const ctx = canvas.getContext("2d")!;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, w, h);
    if (!horizon || points.length === 0) {
      ctx.strokeStyle = "rgba(255,255,255,0.08)";
      ctx.beginPath(); ctx.moveTo(0, h / 2); ctx.lineTo(w, h / 2); ctx.stroke();
      return;
    }
    const t1 = horizon, t0 = t1 - WAVE_WINDOW_S;
    let lo = Infinity, hi = -Infinity;
    for (const p of points) {
      if (p[0] < t0 || p[1] == null || p[2] == null) continue;
      if (p[1] < lo) lo = p[1];
      if (p[2] > hi) hi = p[2];
    }
    if (!isFinite(lo)) { lo = 0; hi = 1; }
    if (hi - lo < 1e-9) { hi = lo + 1; }
    const pad = (hi - lo) * 0.1;
    lo -= pad; hi += pad;
    const x = (t: number) => ((t - t0) / (t1 - t0)) * w;
    const y = (v: number) => h - ((v - lo) / (hi - lo)) * h;

    // faint grid every second
    ctx.strokeStyle = "rgba(255,255,255,0.05)";
    ctx.lineWidth = 1;
    for (let s = Math.ceil(t0); s <= t1; s++) {
      ctx.beginPath(); ctx.moveTo(x(s) + 0.5, 0); ctx.lineTo(x(s) + 0.5, h); ctx.stroke();
    }
    // band
    ctx.fillStyle = color + "55";
    ctx.strokeStyle = color;
    ctx.lineWidth = 1.25;
    let run: WavePoint[] = [];
    const flush = () => {
      if (run.length === 0) return;
      ctx.beginPath();
      for (let i = 0; i < run.length; i++) ctx.lineTo(x(run[i][0]), y(run[i][2]!));
      for (let i = run.length - 1; i >= 0; i--) ctx.lineTo(x(run[i][0]), y(run[i][1]!));
      ctx.closePath(); ctx.fill();
      ctx.beginPath();
      for (let i = 0; i < run.length; i++) {
        const mid = (run[i][1]! + run[i][2]!) / 2;
        if (i === 0) ctx.moveTo(x(run[i][0]), y(mid)); else ctx.lineTo(x(run[i][0]), y(mid));
      }
      ctx.stroke();
      run = [];
    };
    for (const p of points) {
      if (p[0] < t0) continue;
      if (p[1] == null || p[2] == null) { flush(); continue; }
      run.push(p);
    }
    flush();
    // scale labels
    ctx.fillStyle = "rgba(255,255,255,0.45)";
    ctx.font = "10px 'IBM Plex Mono', monospace";
    ctx.textAlign = "right";
    ctx.fillText(fmt(hi - pad), w - 4, 11);
    ctx.fillText(fmt(lo + pad), w - 4, h - 3);
  }, [points, horizon, color]);

  return (
    <div className="strip">
      <div className="strip-label" style={{ color }}>
        <span>{label}</span>
        <span className="unit">{unit}</span>
      </div>
      <canvas ref={ref} />
    </div>
  );
}

function fmt(v: number): string {
  return Math.abs(v) >= 100 ? v.toFixed(0) : Math.abs(v) >= 10 ? v.toFixed(1) : v.toFixed(2);
}
