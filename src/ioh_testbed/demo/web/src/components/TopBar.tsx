import type { PipelineStatus, Shared } from "../api";

interface Props {
  pipeline: PipelineStatus | null;
  shared: Shared | null;
  connected: boolean;
  onOpenSettings: () => void;
}

export default function TopBar({ pipeline, shared, connected, onOpenSettings }: Props) {
  const m = pipeline?.metrics ?? {};
  const bp = m.source_backpressure_ms_s ?? null;
  const busy = m.features_busy_ms_s ?? null;
  const stub = m.stub;
  return (
    <header className="topbar">
      <div className="brand">
        <div className="brand-title">IOH testbed console</div>
        <div className="brand-sub">replay → Kafka → Flink → inference → interface</div>
      </div>
      <div className="lights">
        <Light on={connected} label="console" />
        <Light on={!!pipeline?.kafka} label="Kafka" />
        <Light on={!!pipeline?.flink} label={`Flink ${pipeline?.job_state ?? ""}`} />
        <Light on={!!pipeline?.stub} label="stub" />
      </div>
      <div className="gauges">
        <Gauge label="back-pressure" value={bp} max={1000} unit="ms/s" hint="time per second the Kafka source spends blocked by downstream" />
        <Gauge label="window+inference busy" value={busy} max={1000} unit="ms/s" />
        <div className="gauge-text">
          <div className="muted small">stub</div>
          <div>{stub ? `${stub.in_flight} in flight · ${stub.queued} queued · ${stub.rejected} rejected` : "--"}</div>
        </div>
      </div>
      <div className="shared-summary" onClick={onOpenSettings} role="button">
        {shared && (
          <>
            <span>{shared.speed}x</span>
            <span>{shared.window_ms / 1000} s / {shared.slide_ms / 1000} s</span>
            <span>{shared.service_ms} ms × {shared.workers}</span>
            <span className={`pill pill-${shared.failure_style}`}>{shared.failure_style}</span>
          </>
        )}
        <span className="link">settings</span>
      </div>
      {pipeline?.message && <div className="topbar-msg">{pipeline.message}</div>}
    </header>
  );
}

function Light({ on, label }: { on: boolean; label: string }) {
  return (
    <div className="light">
      <span className={`dot ${on ? "dot-on" : "dot-off"}`} />
      <span>{label}</span>
    </div>
  );
}

function Gauge({ label, value, max, unit, hint }: { label: string; value: number | null; max: number; unit: string; hint?: string }) {
  const frac = value == null ? 0 : Math.min(1, value / max);
  const tone = frac > 0.8 ? "bad" : frac > 0.3 ? "warn" : "ok";
  return (
    <div className="gauge" title={hint}>
      <div className="muted small">{label}</div>
      <div className="bar"><div className={`bar-fill bar-${tone}`} style={{ width: `${frac * 100}%` }} /></div>
      <div className="small">{value == null ? "--" : `${Math.round(value)} ${unit}`}</div>
    </div>
  );
}
