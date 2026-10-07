"""Compare LIVIA's relative-sleep pacing against absolute-deadline pacing.

Mimics the real shape: N concurrent (patient,label) coroutines, 256 ms packet
cadence, JSON-serializing a 128-sample payload each tick like build_record does.
"""
import asyncio, json, time, statistics

GAP = 0.256
TICKS = 40
STREAMS = 400
PAYLOAD = {"c_value": list(range(128)), "c_label": "ART", "p_patnr": "X",
           "c_sample_period": 2, "c_hz": 500, "c_n_samples": 128,
           **{f"f{i}": 0 for i in range(20)}}

async def livia_style(errs):
    """await asyncio.sleep(gap) between sends, as in replay_stream()."""
    t0 = time.monotonic()
    for i in range(TICKS):
        json.dumps(PAYLOAD).encode()
        await asyncio.sleep(GAP)
    errs.append(time.monotonic() - t0 - TICKS * GAP)

async def deadline_style(errs):
    """Absolute schedule from t0; sleep until the next deadline."""
    t0 = time.monotonic()
    for i in range(TICKS):
        json.dumps(PAYLOAD).encode()
        target = t0 + (i + 1) * GAP
        d = target - time.monotonic()
        if d > 0:
            await asyncio.sleep(d)
    errs.append(time.monotonic() - t0 - TICKS * GAP)

async def run(fn, label):
    errs = []
    await asyncio.gather(*(fn(errs) for _ in range(STREAMS)))
    per_tick = [e / TICKS for e in errs]
    print(f"{label}")
    print(f"   total drift after {TICKS} ticks: "
          f"median {statistics.median(errs)*1000:8.1f} ms   max {max(errs)*1000:8.1f} ms")
    print(f"   per-packet overshoot:            "
          f"median {statistics.median(per_tick)*1000:8.3f} ms")
    return statistics.median(per_tick)

async def main():
    print(f"{STREAMS} concurrent streams, {TICKS} ticks at {GAP*1000:.0f} ms\n")
    a = await run(livia_style,    "LIVIA pattern: await asyncio.sleep(gap)")
    print()
    b = await run(deadline_style, "Absolute deadline schedule")
    print(f"\nextrapolated to a 3 h case ({int(3*3600/GAP):,} packets per stream):")
    print(f"   LIVIA pattern   : {a*3*3600/GAP:9.1f} s of accumulated drift")
    print(f"   deadline schedule: {b*3*3600/GAP:9.1f} s")

asyncio.run(main())
