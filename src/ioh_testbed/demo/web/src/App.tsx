import { useEffect, useState } from "react";
import type { CaseInfo } from "./api";
import { getCases, getState } from "./api";
import BedCard from "./components/BedCard";
import SettingsDrawer from "./components/SettingsDrawer";
import TopBar from "./components/TopBar";
import { useLive } from "./useLive";

export default function App() {
  const live = useLive();
  const [cases, setCases] = useState<CaseInfo[]>([]);
  const [drawer, setDrawer] = useState(false);

  useEffect(() => {
    getCases().then(setCases).catch(() => setCases([]));
    getState().then(live.setState).catch(() => undefined);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const state = live.state;
  const applying = !!state && state.pipeline.message === "applying settings";

  return (
    <div className="app">
      <TopBar pipeline={live.pipeline} shared={state?.shared ?? null} connected={live.connected} onOpenSettings={() => setDrawer(true)} />
      {state && cases.length > 0 ? (
        <main className="grid">
          {live.bedInfo.map((info) => (
            <BedCard key={info.index} info={info} buf={live.beds[info.index]} cases={cases} state={state} busy={applying || !state.pipeline.flink} />
          ))}
        </main>
      ) : (
        <main className="empty">
          <div>{live.connected ? "loading cases…" : "connecting to ioh-demo on this host…"}</div>
        </main>
      )}
      {state && <SettingsDrawer state={state} open={drawer} onClose={() => setDrawer(false)} onApplied={live.setState} />}
      {drawer && <div className="scrim" onClick={() => setDrawer(false)} />}
    </div>
  );
}
