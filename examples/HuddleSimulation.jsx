import { useState, useEffect, useRef, useCallback } from "react";

//  Constants 
const HEAT_THRESHOLD    = 0.75;
const COOL_THRESHOLD    = 0.30;
const EMA_ALPHA         = 0.25;
const MAX_INNER         = 4;
const MIN_INNER         = 2;
const MIN_OUTER_DWELL   = 4000;  // ms
const ROTATION_COOLDOWN = 3000;  // ms

const PENGUIN_NAMES = ["Alpha","Bravo","Charlie","Delta","Echo","Foxtrot","Golf","Hotel"];

function tempColor(t) {
  const r = Math.round(30  + t * 225);
  const g = Math.round(180 - t * 170);
  const b = Math.round(220 - t * 200);
  return `rgb(${r},${g},${b})`;
}

function tempLabel(t) {
  if (t < 0.3)  return "COOL";
  if (t < 0.55) return "WARM";
  if (t < 0.75) return "HOT";
  return "CRITICAL";
}

function updateTemp(current, m) {
  const raw = m.cpu*0.35 + m.mem*0.25 + Math.min(m.conn/100,1)*0.20
            + Math.min(m.resp/5000,1)*0.15 + m.err*0.05;
  return EMA_ALPHA * Math.max(0, Math.min(1, raw)) + (1 - EMA_ALPHA) * current;
}

function driftMetrics(m, stressed) {
  const drift = (v, min, max, speed) => {
    const d = (Math.random() - 0.5) * speed + (stressed ? 0.04 : -0.02);
    return Math.max(min, Math.min(max, v + d));
  };
  return {
    cpu:  drift(m.cpu,  0, 1,     stressed ? 0.12 : 0.06),
    mem:  drift(m.mem,  0, 1,     stressed ? 0.08 : 0.04),
    conn: drift(m.conn, 0, 200,   stressed ? 15   : 8),
    resp: drift(m.resp, 50, 4000, stressed ? 300  : 100),
    err:  drift(m.err,  0, 0.5,   stressed ? 0.04 : 0.02),
  };
}

function makeServer(id, startInner) {
  return {
    id,
    name: PENGUIN_NAMES[id],
    position: startInner ? "inner" : "outer",
    temperature: Math.random() * 0.2,
    metrics: { cpu: Math.random()*0.2, mem: Math.random()*0.2, conn: 10, resp: 100, err: 0.01 },
    rotationCount: 0,
    innerTimeSec: 0,
    outerTimeSec: 0,
    lastRotated: Date.now(),
    consecutiveEvictions: 0,
    stressed: false,
  };
}

//  Main Component 
export default function HuddleSimulation() {
  const [servers, setServers]       = useState(() =>
    Array.from({ length: 6 }, (_, i) => makeServer(i, i < MAX_INNER))
  );
  const [running, setRunning]       = useState(false);
  const [speed, setSpeed]           = useState(1);
  const [log, setLog]               = useState([]);
  const [tick, setTick]             = useState(0);
  const [totalRotations, setTotalRotations] = useState(0);
  const [requestCount, setRequestCount]     = useState(0);
  const [lastServedId, setLastServedId]     = useState(null);

  // FIX: refs live outside setState so no stale-closure issues
  const innerCursorRef  = useRef(0);
  const rafRef          = useRef(null);
  const lastTickRef     = useRef(Date.now());
  const lastServedTimer = useRef(null);

  const addLog = useCallback((msg, type = "info") => {
    setLog(prev => [
      { id: Date.now() + Math.random(), msg, type, ts: new Date().toLocaleTimeString() },
      ...prev,
    ].slice(0, 40));
  }, []);

  //  Simulation Tick 
  const simulateTick = useCallback((dt) => {
    const now = Date.now();

    // FIX: compute all side-effect data OUTSIDE setServers, then apply atomically
    setServers(prev => {
      const timeDelta = dt / 1000;

      // 1. Drift metrics + EMA temperature
      const next = prev.map(s => {
        const nm = driftMetrics(s.metrics, s.stressed);
        const nt = updateTemp(s.temperature, nm);
        return {
          ...s,
          metrics: nm,
          temperature: nt,
          innerTimeSec: s.position === "inner" ? s.innerTimeSec + timeDelta : s.innerTimeSec,
          outerTimeSec: s.position === "outer" ? s.outerTimeSec + timeDelta : s.outerTimeSec,
        };
      });

      const inner = next.filter(s => s.position === "inner");
      const outer  = next.filter(s => s.position === "outer")
                         .sort((a, b) => a.temperature - b.temperature);

      // 2. Eviction candidates
      const candidates = inner.filter(s =>
        s.temperature >= HEAT_THRESHOLD &&
        (now - s.lastRotated) >= ROTATION_COOLDOWN
      );
      const maxEvict  = Math.max(1, Math.floor(inner.length / 3));
      const safeEvict = inner.length - MIN_INNER;
      const toEvict   = candidates.slice(0, Math.min(maxEvict, Math.max(0, safeEvict)));
      const evictedIds = new Set(toEvict.map(s => s.id));

      // 3. Pull coolest outer servers
      const remainingInner = inner.length - toEvict.length;
      let slots = MAX_INNER - remainingInner;
      const toEnter = [];
      for (const s of outer) {
        if (slots <= 0) break;
        if ((now - s.lastRotated) >= MIN_OUTER_DWELL && s.temperature <= COOL_THRESHOLD) {
          toEnter.push(s);
          slots--;
        }
      }
      const enterIds = new Set(toEnter.map(s => s.id));

      // 4. Apply transitions
      const rotDelta = evictedIds.size + enterIds.size;
      const updated = next.map(s => {
        if (evictedIds.has(s.id)) {
          return { ...s, position: "outer", lastRotated: now,
                   rotationCount: s.rotationCount + 1,
                   consecutiveEvictions: s.consecutiveEvictions + 1 };
        }
        if (enterIds.has(s.id)) {
          return { ...s, position: "inner", lastRotated: now,
                   rotationCount: s.rotationCount + 1,
                   consecutiveEvictions: 0 };
        }
        return s;
      });

      // FIX: schedule side-effects via setTimeout(0) so they run AFTER
      // React finishes this render batch — avoids setState-inside-setState
      if (rotDelta > 0) {
        setTimeout(() => {
          setTotalRotations(r => r + rotDelta);
          toEvict.forEach(s =>
            addLog(`🔥 ${s.name} → outer  (temp ${(s.temperature * 100).toFixed(0)}%)`, "evict")
          );
          toEnter.forEach(s =>
            addLog(`❄️  ${s.name} → inner  (temp ${(s.temperature * 100).toFixed(0)}%)`, "enter")
          );
        }, 0);
      }

      return updated;
    });

    setTick(t => t + 1);
  }, [addLog]);

  //  Animation Loop 
  useEffect(() => {
    if (!running) return;
    const INTERVAL = 800 / speed;

    const loop = () => {
      const now = Date.now();
      const dt  = now - lastTickRef.current;
      if (dt >= INTERVAL) {
        simulateTick(dt);
        lastTickRef.current = now;
      }
      rafRef.current = requestAnimationFrame(loop);
    };
    rafRef.current = requestAnimationFrame(loop);
    return () => cancelAnimationFrame(rafRef.current);
  }, [running, speed, simulateTick]);

  //  Request Simulation 
  // FIX: cursor mutation and lastServedId update moved OUTSIDE setServers
  const sendRequest = useCallback(() => {
    setServers(prev => {
      const inner = prev.filter(s => s.position === "inner");
      if (!inner.length) return prev;

      // FIX: read & mutate ref outside the pure updater (it's a ref, not state)
      const idx    = innerCursorRef.current % inner.length;
      const target = inner[idx];
      innerCursorRef.current += 1;

      // FIX: side effects scheduled after render
      setTimeout(() => {
        setRequestCount(r => r + 1);
        setLastServedId(target.id);
        addLog(`📨 Request → ${target.name}  (temp ${(target.temperature * 100).toFixed(0)}%)`, "req");

        clearTimeout(lastServedTimer.current);
        lastServedTimer.current = setTimeout(() => setLastServedId(null), 600);
      }, 0);

      return prev; // servers state itself doesn't change on a request
    });
  }, [addLog]);

  //  Stress Toggle 
  const toggleStress = useCallback((id) => {
    setServers(prev => prev.map(s => s.id === id ? { ...s, stressed: !s.stressed } : s));
  }, []);

  //  Derived State 
  const inner = servers.filter(s => s.position === "inner");
  const outer  = servers.filter(s => s.position === "outer")
                         .sort((a, b) => a.temperature - b.temperature);

  const fairness = (() => {
    const times = servers.map(s => s.innerTimeSec);
    const total = times.reduce((a, b) => a + b, 0);
    if (total === 0) return 0;
    const mean = total / times.length;
    const dev  = times.reduce((a, b) => a + Math.abs(b - mean), 0);
    return dev / (2 * times.length * mean);
  })();

  //  Render 
  return (
    <div style={{
      background: "#06090f",
      minHeight: "100vh",
      fontFamily: "'Courier New', monospace",
      color: "#c8d8e8",
      overflow: "hidden",
    }}>
      <style>{`
        @keyframes pulse       { 0%,100%{opacity:1} 50%{opacity:0.4} }
        @keyframes glow        { 0%,100%{box-shadow:0 0 8px rgba(80,200,255,0.3)} 50%{box-shadow:0 0 24px rgba(80,200,255,0.9)} }
        @keyframes requestFlash{ 0%{box-shadow:0 0 0 0 rgba(255,220,60,0.9)} 100%{box-shadow:0 0 0 20px rgba(255,220,60,0)} }
        @keyframes snowfall    { from{transform:translateY(-10px);opacity:0} to{transform:translateY(0);opacity:1} }
        .server-card { transition: all 0.4s cubic-bezier(0.34,1.56,0.64,1); }
        .server-card:hover { transform: scale(1.05) !important; }
        .btn { cursor:pointer; border:none; outline:none; transition:all 0.15s; }
        .btn:active { transform:scale(0.94); }
        ::-webkit-scrollbar { width:4px; }
        ::-webkit-scrollbar-track { background:#0a0e17; }
        ::-webkit-scrollbar-thumb { background:#1e3050; border-radius:2px; }
      `}</style>

      {/* Header */}
      <div style={{ background:"#080c14", borderBottom:"1px solid #0e1e35", padding:"14px 24px",
                    display:"flex", alignItems:"center", justifyContent:"space-between" }}>
        <div style={{ display:"flex", alignItems:"center", gap:"12px" }}>
          <span style={{ fontSize:"28px" }}>🐧</span>
          <div>
            <div style={{ fontSize:"15px", fontWeight:"bold", letterSpacing:"3px", color:"#50c8ff", textTransform:"uppercase" }}>HuddleCluster</div>
            <div style={{ fontSize:"10px", color:"#3a5570", letterSpacing:"2px" }}>PENGUIN-INSPIRED LOAD BALANCER</div>
          </div>
        </div>

        <div style={{ display:"flex", gap:"24px", alignItems:"center" }}>
          {[["ROTATIONS", totalRotations], ["REQUESTS", requestCount],
            ["FAIRNESS", (fairness*100).toFixed(1)+"%"], ["TICK", tick]
          ].map(([k, v]) => (
            <div key={k} style={{ textAlign:"center" }}>
              <div style={{ fontSize:"9px", color:"#2a4060", letterSpacing:"2px" }}>{k}</div>
              <div style={{ fontSize:"16px", color:"#50c8ff", fontWeight:"bold" }}>{v}</div>
            </div>
          ))}
        </div>

        <div style={{ display:"flex", gap:"8px", alignItems:"center" }}>
          <span style={{ fontSize:"10px", color:"#2a4060", letterSpacing:"1px" }}>SPEED</span>
          {[0.5, 1, 2, 3].map(s => (
            <button key={s} className="btn" onClick={() => setSpeed(s)} style={{
              background: speed===s ? "#0e3060" : "#080c14",
              border: `1px solid ${speed===s ? "#50c8ff" : "#0e1e35"}`,
              color: speed===s ? "#50c8ff" : "#3a5570",
              padding:"4px 10px", borderRadius:"4px", fontSize:"11px",
            }}>{s}×</button>
          ))}

          <button className="btn" onClick={() => {
            setRunning(r => {
              if (!r) addLog("▶ Simulation started", "system");
              else     addLog("⏸ Simulation paused",  "system");
              return !r;
            });
          }} style={{
            background: running ? "#0a2a10" : "#0e3060",
            border: `1px solid ${running ? "#30c860" : "#50c8ff"}`,
            color: running ? "#30c860" : "#50c8ff",
            padding:"6px 18px", borderRadius:"4px", fontSize:"12px", letterSpacing:"1px",
          }}>
            {running ? "⏸ PAUSE" : "▶ START"}
          </button>

          <button className="btn" onClick={sendRequest} style={{
            background:"#1a1200", border:"1px solid #c8a020",
            color:"#c8a020", padding:"6px 14px", borderRadius:"4px", fontSize:"12px",
          }}>📨 REQUEST</button>
        </div>
      </div>

      {/* Main Layout */}
      <div style={{ display:"grid", gridTemplateColumns:"1fr 280px", height:"calc(100vh - 61px)" }}>

        {/* LEFT: Rings */}
        <div style={{ padding:"20px 24px", display:"flex", flexDirection:"column", gap:"20px", overflowY:"auto" }}>

          {/* Legend */}
          <div style={{ display:"flex", alignItems:"center", gap:"12px", fontSize:"10px", color:"#2a5070", letterSpacing:"1px" }}>
            <span>TEMP:</span>
            {[["COOL","#40b8ff"],["WARM","#80d090"],["HOT","#e08040"],["CRITICAL","#ff4040"]].map(([l,c]) => (
              <span key={l} style={{ display:"flex", alignItems:"center", gap:"4px" }}>
                <span style={{ width:8, height:8, background:c, borderRadius:"50%", display:"inline-block" }}/>
                {l}
              </span>
            ))}
            <div style={{ flex:1, height:"1px", background:"#0e1e35" }}/>
            <span>HEAT={HEAT_THRESHOLD*100}%</span>
            <span>COOL={COOL_THRESHOLD*100}%</span>
          </div>

          {/* Inner Ring */}
          <div>
            <div style={{ display:"flex", alignItems:"center", gap:"10px", marginBottom:"12px" }}>
              <div style={{ width:"8px", height:"8px", borderRadius:"50%", background:"#50c8ff",
                            animation: running ? "pulse 2s infinite" : "none" }}/>
              <span style={{ fontSize:"11px", letterSpacing:"3px", color:"#50c8ff" }}>INNER RING</span>
              <span style={{ fontSize:"10px", color:"#1a3050" }}>─── ACTIVE ───</span>
              <span style={{ fontSize:"10px", color:"#204070" }}>{inner.length} / {MAX_INNER} servers</span>
            </div>
            <div style={{ border:"1px solid #0e2840", borderRadius:"12px", background:"#070b13",
                          padding:"20px", position:"relative", minHeight:"140px" }}>
              <div style={{ position:"absolute", top:"50%", left:"50%", transform:"translate(-50%,-50%)",
                            width:"90%", height:"90%", border:"1px dashed #0a1e30", borderRadius:"50%",
                            pointerEvents:"none", opacity:0.5 }}/>
              <div style={{ display:"flex", gap:"16px", flexWrap:"wrap", justifyContent:"center", position:"relative", zIndex:1 }}>
                {inner.length === 0 && (
                  <div style={{ color:"#ff4040", fontSize:"12px", letterSpacing:"2px",
                                animation:"pulse 1s infinite", padding:"20px" }}>
                    ⚠ INNER RING EMPTY — EMERGENCY MODE
                  </div>
                )}
                {inner.map(s => (
                  <ServerCard key={s.id} server={s} isActive={s.id === lastServedId} onStress={() => toggleStress(s.id)} />
                ))}
              </div>
            </div>
          </div>

          <div style={{ textAlign:"center", color:"#0e2840", fontSize:"20px", lineHeight:"1" }}>⟳ rotation</div>

          {/* Outer Ring */}
          <div>
            <div style={{ display:"flex", alignItems:"center", gap:"10px", marginBottom:"12px" }}>
              <div style={{ width:"8px", height:"8px", borderRadius:"50%", background:"#204060" }}/>
              <span style={{ fontSize:"11px", letterSpacing:"3px", color:"#204060" }}>OUTER RING</span>
              <span style={{ fontSize:"10px", color:"#1a3050" }}>─── RESTING ─── sorted by temp ↑</span>
              <span style={{ fontSize:"10px", color:"#143050" }}>{outer.length} servers</span>
            </div>
            <div style={{ border:"1px solid #0a1a28", borderRadius:"12px", background:"#050810",
                          padding:"20px", minHeight:"120px" }}>
              <div style={{ display:"flex", gap:"16px", flexWrap:"wrap" }}>
                {outer.length === 0 && (
                  <div style={{ color:"#1a3050", fontSize:"11px", letterSpacing:"2px", padding:"10px" }}>
                    ALL SERVERS IN INNER RING
                  </div>
                )}
                {outer.map(s => (
                  <ServerCard key={s.id} server={s} isActive={false} onStress={() => toggleStress(s.id)} />
                ))}
              </div>
            </div>
          </div>

          {/* Fairness Bar */}
          <div>
            <div style={{ display:"flex", justifyContent:"space-between", marginBottom:"8px",
                          fontSize:"10px", letterSpacing:"2px", color:"#2a5070" }}>
              <span>INNER TIME DISTRIBUTION (FAIRNESS)</span>
              <span style={{ color: fairness > 0.3 ? "#ff6040" : "#30c860" }}>
                SCORE: {(fairness*100).toFixed(1)}% {fairness > 0.3 ? "⚠" : "✓"}
              </span>
            </div>
            <div style={{ display:"flex", gap:"4px", height:"24px" }}>
              {servers.map(s => {
                const total = servers.reduce((a, b) => a + b.innerTimeSec, 0);
                const frac  = total > 0 ? s.innerTimeSec / total : 1 / servers.length;
                return (
                  <div key={s.id} title={s.name} style={{
                    flex: frac, background: s.position === "inner"
                      ? `linear-gradient(180deg, ${tempColor(s.temperature)}, ${tempColor(s.temperature)}88)`
                      : "#0a1a28",
                    borderRadius:"3px", transition:"flex 0.5s ease",
                    border:"1px solid #0e1e35", position:"relative", cursor:"default", minWidth:"8px",
                  }}>
                    <div style={{ position:"absolute", bottom:"2px", left:"50%", transform:"translateX(-50%)",
                                  fontSize:"8px", color:"rgba(200,220,240,0.6)", whiteSpace:"nowrap" }}>
                      {s.name.slice(0,1)}
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        </div>

        {/* RIGHT: Event Log */}
        <div style={{ borderLeft:"1px solid #0a1828", background:"#050810", display:"flex", flexDirection:"column" }}>
          <div style={{ padding:"12px 16px", borderBottom:"1px solid #0a1828",
                        display:"flex", alignItems:"center", gap:"8px" }}>
            <div style={{ width:"6px", height:"6px", background:"#30c860", borderRadius:"50%",
                          animation: running ? "pulse 1.5s infinite" : "none" }}/>
            <span style={{ fontSize:"10px", letterSpacing:"3px", color:"#204060" }}>EVENT LOG</span>
          </div>
          <div style={{ flex:1, overflowY:"auto", padding:"8px" }}>
            {log.length === 0 && (
              <div style={{ color:"#1a3050", fontSize:"10px", padding:"12px", letterSpacing:"1px" }}>
                Press START to begin simulation…
              </div>
            )}
            {log.map(e => (
              <div key={e.id} style={{
                padding:"5px 8px", marginBottom:"2px", borderRadius:"4px",
                fontSize:"10px", lineHeight:"1.4",
                background: e.type==="evict" ? "#1a0808" : e.type==="enter" ? "#071510"
                           : e.type==="req"  ? "#0e0e06" : "#080c14",
                borderLeft: `2px solid ${
                  e.type==="evict" ? "#802020" : e.type==="enter" ? "#206040"
                : e.type==="req"   ? "#806020" : "#0e2840"}`,
                animation: "snowfall 0.2s ease-out",
              }}>
                <span style={{ color:"#1a3050", marginRight:"6px" }}>{e.ts}</span>
                <span style={{ color: e.type==="evict" ? "#e06060" : e.type==="enter" ? "#50c880"
                              : e.type==="req" ? "#c8a840" : "#3a6080" }}>{e.msg}</span>
              </div>
            ))}
          </div>
          <div style={{ padding:"10px 12px", borderTop:"1px solid #0a1828",
                        fontSize:"9px", color:"#1a3050", letterSpacing:"1px", lineHeight:"1.8" }}>
            CLICK SERVER → TOGGLE STRESS<br/>
            HEAT ≥ {HEAT_THRESHOLD*100}% → EVICT TO OUTER<br/>
            COOL ≤ {COOL_THRESHOLD*100}% + DWELL → ENTER INNER
          </div>
        </div>
      </div>
    </div>
  );
}

//  Server Card 
function ServerCard({ server, isActive, onStress }) {
  const { name, temperature, metrics, position, rotationCount, innerTimeSec, stressed } = server;
  const col = tempColor(temperature);
  const pct = (temperature * 100).toFixed(0);

  return (
    <div
      className="server-card"
      onClick={onStress}
      title={`${name} — Click to toggle stress\nCPU: ${(metrics.cpu*100).toFixed(0)}%\nMem: ${(metrics.mem*100).toFixed(0)}%\nConns: ${metrics.conn.toFixed(0)}\nResp: ${metrics.resp.toFixed(0)}ms`}
      style={{
        width:"110px", background:"#080d18",
        border:`1px solid ${isActive ? "#c8a020" : stressed ? "#802020" : col+"66"}`,
        borderRadius:"8px", padding:"10px 8px", cursor:"pointer", position:"relative",
        animation: isActive ? "requestFlash 0.6s ease-out" : "none",
        boxShadow: isActive   ? `0 0 20px ${col}66`
                 : stressed   ? "0 0 12px rgba(200,40,40,0.3)"
                 : position==="inner" ? `0 0 8px ${col}33` : "none",
      }}
    >
      {stressed && (
        <div style={{ position:"absolute", top:"-6px", right:"-6px", background:"#802020",
                      color:"#ffaaaa", fontSize:"8px", borderRadius:"4px",
                      padding:"1px 4px", letterSpacing:"1px" }}>STRESS</div>
      )}

      <div style={{ textAlign:"center", fontSize:"24px", marginBottom:"4px",
                    filter: position==="inner" ? "none" : "grayscale(60%)",
                    transform: stressed ? "scale(1.15)" : "scale(1)",
                    transition:"transform 0.3s" }}>🐧</div>

      <div style={{ textAlign:"center", fontSize:"9px", letterSpacing:"2px",
                    color:"#405070", marginBottom:"6px" }}>{name.toUpperCase()}</div>

      <div style={{ background:"#0a1020", borderRadius:"3px", height:"4px",
                    marginBottom:"4px", overflow:"hidden" }}>
        <div style={{
          width:`${pct}%`, height:"100%",
          background:`linear-gradient(90deg, ${col}, ${col}cc)`,
          transition:"width 0.4s ease, background 0.4s ease",
          boxShadow: temperature >= HEAT_THRESHOLD ? `0 0 6px ${col}` : "none",
        }}/>
      </div>

      <div style={{ display:"flex", justifyContent:"space-between", alignItems:"center" }}>
        <span style={{ fontSize:"8px", color:col, letterSpacing:"1px" }}>{tempLabel(temperature)}</span>
        <span style={{ fontSize:"10px", fontWeight:"bold", color:col }}>{pct}%</span>
      </div>

      <div style={{ marginTop:"6px", display:"grid", gridTemplateColumns:"1fr 1fr", gap:"2px" }}>
        {[
          ["CPU",  (metrics.cpu*100).toFixed(0)+"%"],
          ["MEM",  (metrics.mem*100).toFixed(0)+"%"],
          ["CONN", metrics.conn.toFixed(0)],
          ["ROT",  rotationCount],
        ].map(([k, v]) => (
          <div key={k} style={{ background:"#060a14", borderRadius:"2px", padding:"2px 4px" }}>
            <div style={{ fontSize:"7px", color:"#1a3050" }}>{k}</div>
            <div style={{ fontSize:"9px", color:"#304860" }}>{v}</div>
          </div>
        ))}
      </div>

      {position === "inner" && (
        <div style={{ marginTop:"5px", textAlign:"center", fontSize:"8px", color:"#204050",
                      borderTop:"1px solid #0a1828", paddingTop:"4px" }}>
          ⏱ {innerTimeSec.toFixed(0)}s active
        </div>
      )}
    </div>
  );
}