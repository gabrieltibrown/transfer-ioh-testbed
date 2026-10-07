import { useState } from "react";
import type { BedInfo, CaseInfo, Grain, State } from "../api";
import { play, stop } from "../api";
import type { BedBuffers } from "../store";
import Indicator, { fmtPct, fmtS, toneFor } from "./Indicator";
import NumericTile from "./NumericTile";
import WaveStrip from "./WaveStrip";

const WAVE_COLORS: Record<string, string> = {
  ECG_II: "#3ddc84", ART: "#ff5a5f", PLETH: "#4cc9f0", AWP: "#f9c74f", CO2: "#f8961e",
};
const NUMERIC_COLORS: Record<string, string> = {
  HR: "#3ddc84", ART_MBP: "#ff5a5f", ART_SBP: "#ff8a8d", ART_DBP: "#ff8a8d", SPO2: "#4cc9f0",
  ETCO2: "#f8961e", RR: "#f9c74f", TEMP: "#c77dff",
};

interface Props {
  info: BedInfo;
  buf: BedBuffers;
  cases: CaseInfo[];
  state: State;
  busy: boolean;
}

export default function BedCard({ info, buf, cases, state, busy }: Props) {
  const [caseId, setCaseId] = useState<number>(info.case ?? cases[info.index % cases.length]?.caseid ?? 1);
  const [grain, setGrain] = useState<Grain>(info.grain ?? "dwc_10s");
  const [startAt, setStartAt] = useState<number>(info.start_at ?? 0);
  const [error, setError] = useState<string | null>(null);
  const running = info.status === "running";
  const horizon = buf.lastEvent;
  const sel = cases.find((c) => c.caseid === caseId);

  const onPlay = async () => {
    setError(null);
    try { await play(info.index, { case: caseId, grain, start_at: startAt }); } catch (e) { setError(String((e as Error).message)); }
  };
  const onStop = async () => { await stop(info.index); };

  const ing = buf.ingress;
  const ps = buf.predictionStats;
  const risk = ps?.last_risk;

  return (
    <section className={`bed bed-${info.status}`}>
      <header className="bed-head">
        <div className="bed-title">
          <span className="bed-num">Bed {info.index + 1}</span>
          <span className={`pill pill-${info.status}`}>{info.status}</span>
          {running && <span className="elapsed">{fmtClock(buf.elapsed)} patient time{buf.speed !== 1 ? ` at ${buf.speed}x` : ""}</span>}
        </div>
        <div className="bed-controls">
          <label>
            case
            <select value={caseId} disabled={running} onChange={(e) => setCaseId(Number(e.target.value))}>
              {cases.map((c) => (
                <option key={c.caseid} value={c.caseid}>
                  {c.caseid} · {(c.duration_s / 3600).toFixed(1)} h · {c.n_wave_tracks}w/{c.n_numeric_tracks}n
                </option>
              ))}
            </select>
          </label>
          <label>
            grain
            <select value={grain} disabled={running} onChange={(e) => setGrain(e.target.value as Grain)}>
              {state.grains.map((g) => <option key={g} value={g}>{g === "dwc_10s" ? "10 s packets (DWC)" : "256 ms packets (DEI)"}</option>)}
            </select>
          </label>
          <label>
            start at
            <input type="number" min={0} step={60} value={startAt} disabled={running}
                   max={sel ? Math.max(0, Math.floor(sel.duration_s - 120)) : undefined}
                   onChange={(e) => setStartAt(Number(e.target.value))} /> s
          </label>
          {running
            ? <button className="btn btn-stop" onClick={onStop}>Stop</button>
            : <button className="btn btn-play" onClick={onPlay} disabled={busy}>Play</button>}
        </div>
      </header>
      {(error || info.message) && <div className="bed-error">{error ?? info.message}</div>}

      <div className="bed-body">
        <div className="waves">
          {state.profile.waves.map((label) => (
            <WaveStrip key={label} label={label} unit={buf.units[label] ?? ""} points={buf.waves[label] ?? []}
                       horizon={horizon} color={WAVE_COLORS[label] ?? "#ddd"} />
          ))}
        </div>
        <div className="side">
          <div className="tiles">
            {state.profile.numerics.map((label) => (
              <NumericTile key={label} label={label} unit={buf.units[label] ?? ""} series={buf.numerics[label] ?? []}
                           horizon={horizon} color={NUMERIC_COLORS[label] ?? "#ddd"} />
            ))}
          </div>
          <div className="panel">
            <div className="panel-title">Ingress <span className="muted">replay to Kafka</span></div>
            <div className="inds">
              <Indicator label="delay p50" value={fmtS(ing?.delay_p50)} tone={toneFor(ing?.delay_p50, 0.05, 0.5)}
                         hint="broker append time minus scheduled emission time, last 30 s" />
              <Indicator label="delay p99" value={fmtS(ing?.delay_p99)} tone={toneFor(ing?.delay_p99, 0.1, 1)} />
              <Indicator label="complete" value={fmtPct(ing?.completeness)} tone={toneFor(ing?.completeness, 0.98, 0.9, true)}
                         hint="records received versus records the schedule implies for the elapsed patient time" />
              <Indicator label="invalid" value={fmtPct(ing?.invalid_fraction)} tone={toneFor(ing?.invalid_fraction, 0.001, 0.01)}
                         hint="fraction of waveform samples flagged invalid or unavailable" />
              <Indicator label="records/s" value={ing ? ing.records_per_s_30s.toFixed(1) : "--"} />
              <Indicator label="channels" value={ing ? `${ing.channels.length}/${state.profile.waves.length + state.profile.numerics.length}` : "--"}
                         tone={ing ? (ing.channels.length === state.profile.waves.length + state.profile.numerics.length ? "ok" : "warn") : "muted"} />
            </div>
          </div>
          <div className="panel">
            <div className="panel-title">Prediction <span className="muted">Flink window to interface</span></div>
            <div className="risk-row">
              <div className="risk-gauge" style={{ "--risk": risk ?? 0 } as React.CSSProperties}>
                <div className="risk-fill" />
                <div className="risk-num">{risk == null ? "--" : (risk * 100).toFixed(0)}</div>
              </div>
              <div className="risk-text">
                <div>IOH risk</div>
                <div className="muted small">{ps?.n ?? 0} predictions, {ps?.n_full ?? 0} full</div>
              </div>
            </div>
            <div className="inds">
              <Indicator label="latency p50" value={fmtS(ps?.latency_p50)} tone={toneFor(ps?.latency_p50, 3, 10)}
                         hint="receipt at the interface minus broker ingress of the newest record in the window" />
              <Indicator label="latency p99" value={fmtS(ps?.latency_p99)} tone={toneFor(ps?.latency_p99, 5, 20)} />
              <Indicator label="complete" value={fmtPct(ps?.completeness)} tone={toneFor(ps?.completeness, 0.95, 0.7, true)}
                         hint="full windows delivered versus windows due so far" />
              <Indicator label="staleness" value={fmtS(ps?.staleness_p50)} tone={toneFor(ps?.staleness_p50, 3, 10)} />
              <Indicator label="lag" value={fmtS(buf.progressLag)} tone={toneFor(buf.progressLag, 2, 10)}
                         hint="per-case progress lag: wall time minus newest event time seen by Flink" />
              <Indicator label="status" value={fmtStatus(ps?.by_status)} tone={statusTone(ps?.by_status)} />
            </div>
            <PredictionTimeline preds={buf.predictions} />
          </div>
        </div>
      </div>
    </section>
  );
}

function PredictionTimeline({ preds }: { preds: BedBuffers["predictions"] }) {
  const shown = preds.slice(-24);
  return (
    <div className="timeline" title="last predictions: height is risk, colour is status">
      {shown.map((p, i) => (
        <div key={i} className={`tl tl-${p.status}${p.partial ? " tl-partial" : ""}`}
             style={{ height: `${Math.max(6, (p.risk ?? 0) * 100)}%` }}
             title={`${p.status}${p.partial ? " (partial)" : ""}  risk ${p.risk == null ? "--" : (p.risk * 100).toFixed(0)}  latency ${fmtS(p.t_pipeline)}`} />
      ))}
    </div>
  );
}

function fmtClock(s: number): string {
  const m = Math.floor(s / 60), sec = Math.floor(s % 60);
  return `${m}:${sec.toString().padStart(2, "0")}`;
}

function fmtStatus(by?: Record<string, number>): string {
  if (!by) return "--";
  const parts = Object.entries(by).filter(([k]) => k !== "ok").map(([k, v]) => `${v} ${k}`);
  return parts.length ? parts.join(", ") : `${by.ok ?? 0} ok`;
}

function statusTone(by?: Record<string, number>): "ok" | "warn" | "bad" | "muted" {
  if (!by) return "muted";
  const bad = Object.entries(by).filter(([k]) => k !== "ok").reduce((a, [, v]) => a + v, 0);
  if (bad === 0) return "ok";
  return bad > (by.ok ?? 0) ? "bad" : "warn";
}
