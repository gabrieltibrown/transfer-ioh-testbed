// Per-bed rolling buffers built from websocket frames.
import type { Frame, Prediction, WavePoint } from "./api";

export const WAVE_WINDOW_S = 10;
export const NUMERIC_WINDOW_S = 120;

export interface BedBuffers {
  bed: number;
  status: string;
  speed: number;
  firstEvent: number | null;
  lastEvent: number | null;
  elapsed: number;
  waves: Record<string, WavePoint[]>; // label -> points, sorted by t, trimmed
  numerics: Record<string, [number, number][]>;
  units: Record<string, string>;
  ingress: Frame["ingress"] | null;
  predictionStats: Frame["prediction_stats"] | null;
  predictions: Prediction[];
  progressLag: number | null;
  lastFrameWall: number;
}

export function emptyBed(bed: number): BedBuffers {
  return {
    bed, status: "idle", speed: 1, firstEvent: null, lastEvent: null, elapsed: 0,
    waves: {}, numerics: {}, units: {}, ingress: null, predictionStats: null,
    predictions: [], progressLag: null, lastFrameWall: 0,
  };
}

export function applyFrame(prev: BedBuffers, f: Frame, reset: boolean): BedBuffers {
  const b: BedBuffers = reset ? emptyBed(f.bed) : { ...prev, waves: { ...prev.waves }, numerics: { ...prev.numerics } };
  b.status = f.status;
  b.speed = f.speed;
  b.firstEvent = f.first_event;
  b.lastEvent = f.last_event;
  b.elapsed = f.elapsed_patient_s;
  b.units = { ...b.units, ...f.units };
  b.ingress = f.ingress;
  b.predictionStats = f.prediction_stats;
  b.progressLag = f.progress_lag_s;
  b.lastFrameWall = Date.now() / 1000;
  const horizon = f.last_event ?? 0;
  for (const pk of f.waves) {
    const arr = (b.waves[pk.label] ?? []).concat(pk.points);
    const cut = horizon - WAVE_WINDOW_S - 1;
    let i = 0;
    while (i < arr.length && arr[i][0] < cut) i++;
    b.waves[pk.label] = i ? arr.slice(i) : arr;
  }
  for (const [label, t, v] of f.numerics) {
    const arr = (b.numerics[label] ?? []).concat([[t, v]]);
    const cut = horizon - NUMERIC_WINDOW_S;
    let i = 0;
    while (i < arr.length && arr[i][0] < cut) i++;
    b.numerics[label] = i ? arr.slice(i) : arr;
  }
  if (f.predictions.length) {
    b.predictions = b.predictions.concat(f.predictions).slice(-60);
  }
  return b;
}
