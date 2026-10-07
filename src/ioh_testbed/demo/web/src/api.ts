// Types mirror the JSON the Python server emits (demo/server.py, demo/tap.py).

export type Grain = "dwc_10s" | "dei_256ms";
export type FailureStyle = "wait" | "shed" | "reject";

export interface Shared {
  speed: number;
  window_ms: number;
  slide_ms: number;
  service_ms: number;
  service_cv: number;
  workers: number;
  failure_style: FailureStyle;
  shed_timeout_ms: number;
  reject_queue_max: number;
  capacity: number;
  idleness_ms: number;
  watermark_bound_ms: number;
  parallelism: number;
}

export interface BedInfo {
  index: number;
  case: number | null;
  grain: Grain;
  start_at: number;
  status: "idle" | "running" | "done" | "error";
  key: string;
  started_wall: number;
  run_id: string;
  message: string;
}

export interface CaseInfo {
  caseid: number;
  duration_s: number;
  n_wave_tracks: number;
  n_numeric_tracks: number;
  wave_tracks: string[];
}

export interface PipelineStatus {
  kafka: boolean;
  flink: boolean;
  stub: boolean;
  job_state: string | null;
  job_id: string | null;
  message: string;
  clock_offset_s: number;
  event_origin: number;
  metrics: {
    source_backpressure_ms_s?: number | null;
    source_busy_ms_s?: number | null;
    source_records_in_s?: number | null;
    kafka_lag_max?: number | null;
    features_busy_ms_s?: number | null;
    features_backpressure_ms_s?: number | null;
    stub?: { in_flight: number; queued: number; served: number; rejected: number; max_queued: number };
  };
}

export interface State {
  beds: BedInfo[];
  shared: Shared;
  pipeline: PipelineStatus;
  grains: Grain[];
  failure_styles: FailureStyle[];
  profile: { name: string; waves: string[]; numerics: string[] };
}

export type WavePoint = [number, number | null, number | null];

export interface WavePacket {
  label: string;
  hz: number;
  unit: string;
  t_first: number;
  t_last: number;
  n: number;
  n_invalid: number;
  n_unavailable: number;
  points: WavePoint[];
}

export interface Prediction {
  window_end: number;
  window_start: number;
  partial: boolean;
  status: string;
  risk: number | null;
  t_receipt: number;
  t_pipeline: number;
  staleness: number;
  inference_s: number | null;
  n_records: number;
}

export interface Frame {
  bed: number;
  status: string;
  speed: number;
  key: string;
  first_event: number | null;
  last_event: number | null;
  elapsed_patient_s: number;
  waves: WavePacket[];
  numerics: [string, number, number][];
  units: Record<string, string>;
  ingress: {
    n_records: number;
    n_wave: number;
    n_numeric: number;
    records_per_s_30s: number;
    delay_p50: number | null;
    delay_p99: number | null;
    staleness_p50: number | null;
    completeness: number | null;
    invalid_fraction: number;
    channels: string[];
  };
  predictions: Prediction[];
  prediction_stats: {
    n: number;
    n_full: number;
    n_due: number;
    completeness: number | null;
    by_status: Record<string, number>;
    latency_p50: number | null;
    latency_p99: number | null;
    staleness_p50: number | null;
    last_risk: number | null;
  };
  progress_lag_s: number | null;
}

export async function getState(): Promise<State> {
  const r = await fetch("/api/state");
  return r.json();
}

export async function getCases(): Promise<CaseInfo[]> {
  const r = await fetch("/api/cases");
  return r.json();
}

export async function play(index: number, body: { case: number; grain: Grain; start_at: number }) {
  const r = await fetch(`/api/beds/${index}/play`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!r.ok) throw new Error((await r.json()).detail ?? r.statusText);
  return r.json() as Promise<BedInfo>;
}

export async function stop(index: number) {
  const r = await fetch(`/api/beds/${index}/stop`, { method: "POST" });
  return r.json() as Promise<BedInfo>;
}

export async function applyConfig(shared: Partial<Shared>): Promise<State> {
  const r = await fetch("/api/config", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(shared),
  });
  if (!r.ok) throw new Error((await r.json()).detail ?? r.statusText);
  return r.json();
}
