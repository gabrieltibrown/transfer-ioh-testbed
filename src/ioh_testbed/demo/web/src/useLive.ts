import { useEffect, useRef, useState } from "react";
import type { BedInfo, Frame, PipelineStatus, State } from "./api";
import { applyFrame, emptyBed, type BedBuffers } from "./store";

interface Live {
  state: State | null;
  beds: BedBuffers[];
  bedInfo: BedInfo[];
  pipeline: PipelineStatus | null;
  connected: boolean;
  setState: (s: State) => void;
}

export function useLive(): Live {
  const [state, setState] = useState<State | null>(null);
  const [bedInfo, setBedInfo] = useState<BedInfo[]>([]);
  const [pipeline, setPipeline] = useState<PipelineStatus | null>(null);
  const [connected, setConnected] = useState(false);
  const buffers = useRef<BedBuffers[]>([0, 1, 2, 3].map(emptyBed));
  const keys = useRef<string[]>(["", "", "", ""]);
  const [, tick] = useState(0);

  useEffect(() => {
    let sock: WebSocket | null = null;
    let closed = false;
    let timer: number | undefined;

    const connect = () => {
      const proto = location.protocol === "https:" ? "wss" : "ws";
      sock = new WebSocket(`${proto}://${location.host}/ws`);
      sock.onopen = () => setConnected(true);
      sock.onclose = () => {
        setConnected(false);
        if (!closed) timer = window.setTimeout(connect, 1000);
      };
      sock.onmessage = (ev) => {
        const msg = JSON.parse(ev.data);
        if (msg.type === "snapshot") {
          setState(msg.state);
          setBedInfo(msg.state.beds);
          setPipeline(msg.state.pipeline);
          for (const f of msg.frames as Frame[]) {
            buffers.current[f.bed] = applyFrame(buffers.current[f.bed], f, true);
            keys.current[f.bed] = f.key;
          }
        } else if (msg.type === "frame") {
          setBedInfo(msg.beds);
          setPipeline(msg.pipeline);
          const seen = new Set<number>();
          for (const f of msg.frames as Frame[]) {
            const reset = keys.current[f.bed] !== f.key;
            buffers.current[f.bed] = applyFrame(buffers.current[f.bed], f, reset);
            keys.current[f.bed] = f.key;
            seen.add(f.bed);
          }
          for (const b of msg.beds as BedInfo[]) {
            if (b.status === "idle" && !seen.has(b.index) && buffers.current[b.index].status !== "idle") {
              buffers.current[b.index] = emptyBed(b.index);
              keys.current[b.index] = "";
            }
          }
        }
        tick((n) => n + 1);
      };
    };
    connect();
    return () => {
      closed = true;
      if (timer) clearTimeout(timer);
      sock?.close();
    };
  }, []);

  return { state, beds: buffers.current, bedInfo, pipeline, connected, setState };
}
