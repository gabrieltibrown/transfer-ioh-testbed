import { useEffect, useState } from "react";
import type { FailureStyle, Shared, State } from "../api";
import { applyConfig } from "../api";

interface Props {
  state: State;
  open: boolean;
  onClose: () => void;
  onApplied: (s: State) => void;
}

const PRESETS: { name: string; note: string; values: Partial<Shared> }[] = [
  { name: "Baseline", note: "50 ms model, 4 workers, waits on overload", values: { service_ms: 50, service_cv: 0, workers: 4, failure_style: "wait", capacity: 8 } },
  { name: "Stable under burst", note: "200 ms × 2 workers: enough capacity, bursty queue", values: { service_ms: 200, service_cv: 0, workers: 2, failure_style: "wait", capacity: 8 } },
  { name: "Saturated, wait", note: "2 s × 1 worker: back-pressure, lag climbs", values: { service_ms: 2000, service_cv: 0, workers: 1, failure_style: "wait", capacity: 8 } },
  { name: "Saturated, shed", note: "2 s × 1 worker, 10 s timeout: predictions lost", values: { service_ms: 2000, service_cv: 0, workers: 1, failure_style: "shed", shed_timeout_ms: 10000, capacity: 8 } },
  { name: "Saturated, reject", note: "2 s × 1 worker, queue of 4: 503s", values: { service_ms: 2000, service_cv: 0, workers: 1, failure_style: "reject", reject_queue_max: 4, capacity: 8 } },
];

export default function SettingsDrawer({ state, open, onClose, onApplied }: Props) {
  const [form, setForm] = useState<Shared>(state.shared);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => setForm(state.shared), [state.shared]);

  const set = <K extends keyof Shared>(k: K, v: Shared[K]) => setForm((f) => ({ ...f, [k]: v }));
  const anyRunning = state.beds.some((b) => b.status === "running");

  const apply = async () => {
    setBusy(true); setError(null);
    try { onApplied(await applyConfig(form)); onClose(); } catch (e) { setError(String((e as Error).message)); } finally { setBusy(false); }
  };

  return (
    <aside className={`drawer ${open ? "drawer-open" : ""}`}>
      <div className="drawer-head">
        <div className="drawer-title">Shared pipeline settings</div>
        <button className="btn btn-ghost" onClick={onClose}>close</button>
      </div>
      <p className="muted small">
        All beds feed one Flink job and one inference service. Applying restarts both and stops any running bed.
      </p>
      <div className="presets">
        {PRESETS.map((p) => (
          <button key={p.name} className="preset" onClick={() => setForm((f) => ({ ...f, ...p.values }))} title={p.note}>
            <div>{p.name}</div><div className="muted small">{p.note}</div>
          </button>
        ))}
      </div>
      <div className="form">
        <Field label="replay speed" hint="event time runs at this multiple of wall time on every bed">
          <select value={form.speed} onChange={(e) => set("speed", Number(e.target.value))}>
            {[1, 2, 5, 10].map((s) => <option key={s} value={s}>{s}x</option>)}
          </select>
        </Field>
        <Field label="window / slide (s)" hint="trailing window per case and how often a prediction is made, in patient time">
          <input type="number" value={form.window_ms / 1000} min={20} step={10} onChange={(e) => set("window_ms", Number(e.target.value) * 1000)} />
          <input type="number" value={form.slide_ms / 1000} min={5} step={5} onChange={(e) => set("slide_ms", Number(e.target.value) * 1000)} />
        </Field>
        <Field label="inference time (ms)" hint="mean service time per prediction; cv adds lognormal variability">
          <input type="number" value={form.service_ms} min={0} step={10} onChange={(e) => set("service_ms", Number(e.target.value))} />
          <span className="muted small">cv</span>
          <input type="number" value={form.service_cv} min={0} max={2} step={0.1} onChange={(e) => set("service_cv", Number(e.target.value))} />
        </Field>
        <Field label="workers" hint="predictions the service runs concurrently">
          <input type="number" value={form.workers} min={1} max={32} onChange={(e) => set("workers", Number(e.target.value))} />
        </Field>
        <Field label="when inference falls behind" hint="wait: Flink holds requests and back-pressures the source; shed: Flink gives up after the timeout; reject: the service refuses beyond a queue length">
          <select value={form.failure_style} onChange={(e) => set("failure_style", e.target.value as FailureStyle)}>
            <option value="wait">wait (back-pressure)</option>
            <option value="shed">shed (timeout)</option>
            <option value="reject">reject (503)</option>
          </select>
          {form.failure_style === "shed" && (
            <><span className="muted small">timeout ms</span>
              <input type="number" value={form.shed_timeout_ms} min={500} step={500} onChange={(e) => set("shed_timeout_ms", Number(e.target.value))} /></>
          )}
          {form.failure_style === "reject" && (
            <><span className="muted small">queue max</span>
              <input type="number" value={form.reject_queue_max} min={0} onChange={(e) => set("reject_queue_max", Number(e.target.value))} /></>
          )}
        </Field>
        <Field label="async capacity per subtask" hint="in-flight inference calls Flink allows per parallel instance; total is capacity × parallelism">
          <input type="number" value={form.capacity} min={1} max={256} onChange={(e) => set("capacity", Number(e.target.value))} />
          <span className="muted small">× {form.parallelism} = {form.capacity * form.parallelism}</span>
        </Field>
        <Field label="watermark bound / idleness (ms)" hint="out-of-orderness tolerated before a window closes; how long a quiet stream holds the watermark">
          <input type="number" value={form.watermark_bound_ms} min={0} step={100} onChange={(e) => set("watermark_bound_ms", Number(e.target.value))} />
          <input type="number" value={form.idleness_ms} min={100} step={100} onChange={(e) => set("idleness_ms", Number(e.target.value))} />
        </Field>
      </div>
      {error && <div className="bed-error">{error}</div>}
      <div className="drawer-actions">
        {anyRunning && <span className="muted small">applying stops all running beds</span>}
        <button className="btn btn-play" onClick={apply} disabled={busy}>{busy ? "applying…" : "Apply and restart pipeline"}</button>
      </div>
    </aside>
  );
}

function Field({ label, hint, children }: { label: string; hint?: string; children: React.ReactNode }) {
  return (
    <label className="field" title={hint}>
      <span className="field-label">{label}</span>
      <span className="field-inputs">{children}</span>
    </label>
  );
}
