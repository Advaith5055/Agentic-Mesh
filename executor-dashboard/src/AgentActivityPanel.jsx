import { useState } from 'react';
import {
  Route,
  Brain,
  ShieldCheck,
  Hammer,
  Eye,
  Share2,
  User,
  Cpu,
  Gauge,
  MemoryStick,
  ArrowRight,
  CheckCircle2,
  XCircle,
  Loader2,
  MinusCircle,
  CircleDashed,
  Hourglass,
  Workflow,
  MessagesSquare,
  Server,
  ShieldAlert
} from 'lucide-react';

// Agents are identified by icon + name (never colour alone); colour is reserved for status.
const AGENTS = {
  user: { label: 'User', icon: User, role: 'Sends requests and approves plans' },
  router: { label: 'Router', icon: Route, role: 'Chooses fast path, AI path or vision' },
  planner: { label: 'Planner', icon: Brain, role: 'LLM turns requests into operations' },
  validator: { label: 'Validator', icon: ShieldCheck, role: 'Schema, policy and safety checks' },
  executor: { label: 'Executor', icon: Hammer, role: 'Commits to SQLite atomically' },
  vision: { label: 'Vision', icon: Eye, role: 'Vision model reads photos' },
  mesh: { label: 'Mesh', icon: Share2, role: 'Gossip replication to peers' },
  llm: { label: 'LLM', icon: Cpu, role: 'Local language model (Ollama)' }
};

const STATUS = {
  done: { label: 'Done', icon: CheckCircle2, cls: 'text-emerald-400' },
  running: { label: 'Running', icon: Loader2, cls: 'text-amber-300', spin: true },
  failed: { label: 'Failed', icon: XCircle, cls: 'text-rose-400' },
  skipped: { label: 'Skipped', icon: MinusCircle, cls: 'text-slate-500' },
  pending: { label: 'Pending', icon: CircleDashed, cls: 'text-slate-500' },
  waiting: { label: 'Awaiting approval', icon: Hourglass, cls: 'text-amber-300' },
  rejected: { label: 'Rejected', icon: ShieldAlert, cls: 'text-amber-300' }
};

const KIND_LABEL = {
  'fast-path': 'Fast path',
  'nl-plan': 'AI plan',
  execute: 'Execute',
  chat: 'Chat',
  vision: 'Vision',
  approval: 'Approval',
  replication: 'Replication'
};

function health(rate) {
  if (rate === null || rate === undefined) return { label: 'Idle', icon: CircleDashed, cls: 'text-slate-500' };
  if (rate >= 90) return { label: 'Healthy', icon: CheckCircle2, cls: 'text-emerald-400' };
  if (rate >= 60) return { label: 'Degraded', icon: Hourglass, cls: 'text-amber-300' };
  return { label: 'Failing', icon: XCircle, cls: 'text-rose-400' };
}

const fmtMs = (ms) => (ms === null || ms === undefined ? '—' : ms >= 1000 ? `${(ms / 1000).toFixed(1)} s` : `${Math.round(ms)} ms`);
const fmtTime = (t) => (t ? new Date(t).toLocaleTimeString() : '—');

/** Horizontal meter: one accent hue, value printed in neutral ink beside it. */
function Meter({ value, label }) {
  const pct = Math.max(0, Math.min(100, value ?? 0));
  return (
    <div className="w-full" role="meter" aria-valuenow={value ?? 0} aria-valuemin={0} aria-valuemax={100} aria-label={label}>
      <div className="h-1.5 w-full rounded-full bg-slate-800 overflow-hidden">
        <div className="h-full rounded-full bg-amber-400 transition-all duration-500" style={{ width: `${pct}%` }} />
      </div>
    </div>
  );
}

function StatusBadge({ status }) {
  const s = STATUS[status] || STATUS.pending;
  const Icon = s.icon;
  return (
    <span className={`inline-flex items-center gap-1 text-[10px] font-semibold ${s.cls}`}>
      <Icon size={12} className={s.spin ? 'animate-spin' : ''} /> {s.label}
    </span>
  );
}

function AgentTag({ agent }) {
  const a = AGENTS[agent] || { label: agent, icon: Server };
  const Icon = a.icon;
  return (
    <span className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded-md bg-slate-800/80 border border-slate-700 text-slate-200 text-[10px] font-semibold whitespace-nowrap">
      <Icon size={11} className="text-slate-400" /> {a.label}
    </span>
  );
}

function StatTile({ icon: Icon, label, value, sub, meter }) {
  return (
    <div className="p-3 rounded-xl bg-slate-900/70 border border-slate-800 flex flex-col gap-1.5 min-w-0">
      <div className="flex items-center gap-1.5 text-[10px] uppercase tracking-wider text-slate-400 font-mono">
        <Icon size={12} /> {label}
      </div>
      <div className="text-xl font-black text-slate-100 font-mono leading-none">{value}</div>
      {meter !== undefined && <Meter value={meter} label={label} />}
      {sub && <div className="text-[10px] text-slate-500 truncate" title={sub}>{sub}</div>}
    </div>
  );
}

export default function AgentActivityPanel({ activity, nodes = [], proposals = [], onApprove, onReject }) {
  const [agentFilter, setAgentFilter] = useState('all');
  const [busyId, setBusyId] = useState(null);
  const decide = async (fn, id) => {
    setBusyId(id);
    try { await fn?.(id); } finally { setBusyId(null); }
  };
  const { agents = [], tasks = [], messages = [], system } = activity || {};

  const totals = agents.reduce((acc, a) => ({ calls: acc.calls + a.calls, success: acc.success + a.success }), { calls: 0, success: 0 });
  const overall = totals.calls ? Math.round((totals.success / totals.calls) * 1000) / 10 : null;
  const finished = tasks.filter(t => ['done', 'rejected', 'failed'].includes(t.status));
  const taskRate = finished.length ? Math.round((finished.filter(t => t.status !== 'failed').length / finished.length) * 1000) / 10 : null;

  const feed = [...messages]
    .reverse()
    .filter(m => agentFilter === 'all' || m.from === agentFilter || m.to === agentFilter)
    .slice(0, 120);

  return (
    <div className="flex-1 flex flex-col bg-slate-950 overflow-y-auto p-5 space-y-5 select-text">

      {/* PERFORMANCE OVERVIEW */}
      <section aria-label="Performance overview" className="grid grid-cols-2 xl:grid-cols-5 gap-3">
        <StatTile icon={Gauge} label="Agent success" value={overall === null ? '—' : `${overall}%`} meter={overall ?? 0}
          sub={totals.calls ? `${totals.success} of ${totals.calls} agent steps succeeded` : 'No agent activity yet'} />
        <StatTile icon={Workflow} label="Task completion" value={taskRate === null ? '—' : `${taskRate}%`} meter={taskRate ?? 0}
          sub={finished.length ? `${finished.length} finished · ${finished.filter(t => t.status === 'rejected').length} blocked by checks` : 'No finished tasks yet'} />
        <StatTile icon={Cpu} label="Node CPU" value={system ? `${system.cpuPercent}%` : '—'} meter={system?.cpuPercent ?? 0}
          sub={system ? `System load ${system.systemLoadPercent}%` : 'waiting for stats…'} />
        <StatTile icon={MemoryStick} label="Node memory" value={system ? `${system.memoryMb} MB` : '—'}
          sub={system ? `Up ${Math.floor(system.uptimeSec / 60)} min · ${system.peers} peer(s) connected` : ''} />
        <StatTile icon={Brain} label="LLM" value={system?.llm?.processor || '—'}
          sub={system?.llm ? `${system.llm.model} @ ${system.llm.host.replace(/^https?:\/\//, '')}` : ''} />
      </section>

      {/* PLANS WAITING FOR HUMAN APPROVAL */}
      <section aria-label="Waiting for approval">
        <h3 className="text-xs font-bold text-slate-300 font-mono uppercase tracking-wider mb-2 flex items-center gap-1.5">
          <Hourglass size={14} className="text-amber-300" /> Waiting for execution approval ({proposals.length})
        </h3>
        {proposals.length === 0 ? (
          <div className="p-3 rounded-xl border border-dashed border-slate-800 text-[11px] text-slate-500">
            Incoming execution requests and high-risk schema mutations appear here.
          </div>
        ) : (
          <div className="space-y-2">
            {proposals.map(p => (
              <div key={p.id} className="p-3 rounded-xl bg-slate-900/70 border border-amber-500/30 space-y-2">
                <div className="flex items-center justify-between gap-3">
                  <div className="min-w-0">
                    <div className="text-xs font-bold text-slate-100 truncate" title={p.summary}>{p.summary || p.request || p.id}</div>
                    <div className="text-[10px] text-slate-500 font-mono">
                      {p.id} · origin {p.origin}{p.from ? ` · from ${p.from}` : ''} · expires {new Date(p.expiresAt).toLocaleTimeString()}
                    </div>
                  </div>
                  <div className="flex items-center gap-2 shrink-0">
                    <button
                      onClick={() => decide(onApprove, p.id)}
                      disabled={busyId === p.id}
                      className="px-3 py-1.5 rounded-lg bg-emerald-600 hover:bg-emerald-500 disabled:opacity-50 text-white text-[11px] font-bold inline-flex items-center gap-1 cursor-pointer shadow-md shadow-emerald-950"
                    >
                      {busyId === p.id ? <Loader2 size={12} className="animate-spin" /> : <CheckCircle2 size={12} />} Execute & Commit
                    </button>
                    <button
                      onClick={() => decide(onReject, p.id)}
                      disabled={busyId === p.id}
                      className="px-3 py-1.5 rounded-lg bg-slate-800 hover:bg-slate-700 disabled:opacity-50 text-slate-200 border border-slate-700 text-[11px] font-bold inline-flex items-center gap-1 cursor-pointer"
                    >
                      <XCircle size={12} /> Reject
                    </button>
                  </div>
                </div>
                <div className="space-y-1 font-mono text-[10px]">
                  {p.operations.map((op, i) => (
                    <div key={i} className="px-2 py-1 rounded bg-slate-950 border border-slate-800 text-slate-300 break-all">
                      <span className="text-slate-500">{i}.</span> <span className="font-bold text-amber-400">{op.operation}</span> {op.table} {JSON.stringify(op.data)}
                    </div>
                  ))}
                </div>
              </div>
            ))}
          </div>
        )}
      </section>

      {/* MESH NODES: CPU / RAM per laptop */}
      <section aria-label="Mesh nodes">
        <h3 className="text-xs font-bold text-slate-300 font-mono uppercase tracking-wider mb-2 flex items-center gap-1.5">
          <Server size={14} className="text-amber-400" /> Mesh nodes ({nodes.length})
        </h3>
        <div className="grid grid-cols-2 xl:grid-cols-3 gap-3">
          {nodes.map(n => (
            <div key={n.id} className="p-3 rounded-xl bg-slate-900/60 border border-slate-800 space-y-2">
              <div className="flex items-center justify-between gap-2">
                <div className="min-w-0">
                  <div className="text-xs font-bold text-slate-100 truncate">{n.name}{n.local ? ' (this node)' : ''}</div>
                  <div className="text-[10px] text-slate-500 font-mono truncate">{[n.role, n.model, n.ip].filter(Boolean).join(' · ')}</div>
                </div>
                {n.perf
                  ? <span className="inline-flex items-center gap-1 text-[10px] font-semibold text-emerald-400"><CheckCircle2 size={12} /> Reporting</span>
                  : <span className="inline-flex items-center gap-1 text-[10px] font-semibold text-slate-500"><CircleDashed size={12} /> No stats</span>}
              </div>
              {[['CPU', n.perf?.cpuPercent], ['RAM', n.perf?.memPercent]].map(([label, v]) => (
                <div key={label} className="space-y-1">
                  <div className="flex justify-between text-[10px] font-mono text-slate-400">
                    <span>{label}</span><span className="text-slate-200">{v === undefined || v === null ? '—' : `${v}%`}</span>
                  </div>
                  <Meter value={v ?? 0} label={`${n.name} ${label}`} />
                </div>
              ))}
            </div>
          ))}
        </div>
      </section>

      {/* PER-AGENT PERFORMANCE */}
      <section aria-label="Agent performance">
        <h3 className="text-xs font-bold text-slate-300 font-mono uppercase tracking-wider mb-2 flex items-center gap-1.5">
          <Gauge size={14} className="text-amber-400" /> Agent performance
        </h3>
        <div className="grid grid-cols-2 xl:grid-cols-3 gap-3">
          {agents.map(a => {
            const meta = AGENTS[a.agent] || { label: a.agent, icon: Server, role: '' };
            const Icon = meta.icon;
            const h = health(a.successRate);
            const HIcon = h.icon;
            return (
              <div key={a.agent} className="p-3 rounded-xl bg-slate-900/60 border border-slate-800 space-y-2">
                <div className="flex items-center justify-between gap-2">
                  <div className="flex items-center gap-2 min-w-0">
                    <div className="p-1.5 rounded-lg bg-slate-800 border border-slate-700"><Icon size={14} className="text-slate-300" /></div>
                    <div className="min-w-0">
                      <div className="text-xs font-bold text-slate-100">{meta.label}</div>
                      <div className="text-[10px] text-slate-500 truncate">{meta.role}</div>
                    </div>
                  </div>
                  <span className={`inline-flex items-center gap-1 text-[10px] font-semibold ${h.cls}`}><HIcon size={12} /> {h.label}</span>
                </div>
                <div className="flex items-end justify-between">
                  <span className="text-2xl font-black font-mono text-slate-100">{a.successRate === null ? '—' : `${a.successRate}%`}</span>
                  <span className="text-[10px] text-slate-500 font-mono">success rate</span>
                </div>
                <Meter value={a.successRate ?? 0} label={`${meta.label} success rate`} />
                <div className="grid grid-cols-3 text-[10px] font-mono text-slate-400">
                  <span>{a.calls} calls</span>
                  <span className="text-center">{a.rejected ? `${a.failed} failed · ${a.rejected} blocked` : `${a.failed} failed`}</span>
                  <span className="text-right">avg {fmtMs(a.avgMs)}</span>
                </div>
              </div>
            );
          })}
        </div>
      </section>

      {/* TASK PIPELINES */}
      <section aria-label="Task progress">
        <h3 className="text-xs font-bold text-slate-300 font-mono uppercase tracking-wider mb-2 flex items-center gap-1.5">
          <Workflow size={14} className="text-amber-400" /> Task progress ({tasks.length})
        </h3>
        {tasks.length === 0 ? (
          <div className="p-4 rounded-xl border border-dashed border-slate-800 text-xs text-slate-500 text-center">
            No executed tasks yet. Execute planned tasks or sync with peers to view activity.
          </div>
        ) : (
          <div className="space-y-2">
            {tasks.slice(0, 25).map(t => (
              <div key={t.id} className="p-3 rounded-xl bg-slate-900/60 border border-slate-800 space-y-2">
                <div className="flex items-center justify-between gap-3">
                  <div className="flex items-center gap-2 min-w-0">
                    <span className="px-1.5 py-0.5 rounded bg-slate-800 border border-slate-700 text-[10px] font-mono text-slate-300 shrink-0">{KIND_LABEL[t.kind] || t.kind}</span>
                    <span className="text-xs text-slate-100 font-semibold truncate" title={t.title}>{t.title}</span>
                  </div>
                  <div className="flex items-center gap-3 shrink-0">
                    <StatusBadge status={t.detail === 'awaiting approval' && t.status === 'done' ? 'waiting' : t.status} />
                    <span className="text-sm font-black font-mono text-slate-100 w-12 text-right">{t.progress}%</span>
                  </div>
                </div>
                <Meter value={t.progress} label={`${t.title} progress`} />
                <div className="flex items-center flex-wrap gap-1">
                  {t.stages.map((s, i) => (
                    <div key={s.name} className="flex items-center gap-1">
                      {i > 0 && <ArrowRight size={11} className="text-slate-600" />}
                      <div className="flex items-center gap-1 px-1.5 py-1 rounded-lg bg-slate-950 border border-slate-800" title={s.detail || s.name}>
                        <AgentTag agent={s.agent} />
                        <span className="text-[10px] text-slate-300">{s.name}</span>
                        <StatusBadge status={s.status} />
                        {s.durationMs !== null && s.durationMs !== undefined && <span className="text-[10px] text-slate-500 font-mono">{fmtMs(s.durationMs)}</span>}
                      </div>
                    </div>
                  ))}
                </div>
                {(t.detail || t.stages.find(s => s.status === 'failed' || s.status === 'rejected')?.detail) && (
                  <div className="text-[10px] text-slate-400 font-mono truncate" title={t.detail}>
                    {t.stages.find(s => s.status === 'failed' || s.status === 'rejected')?.detail || t.detail}
                  </div>
                )}
              </div>
            ))}
          </div>
        )}
      </section>

      {/* AGENT COMMUNICATIONS */}
      <section aria-label="Agent communications">
        <div className="flex items-center justify-between mb-2 gap-2 flex-wrap">
          <h3 className="text-xs font-bold text-slate-300 font-mono uppercase tracking-wider flex items-center gap-1.5">
            <MessagesSquare size={14} className="text-amber-400" /> Agent communications ({messages.length})
          </h3>
          <div className="flex items-center gap-1 flex-wrap">
            {['all', 'router', 'planner', 'llm', 'validator', 'executor', 'vision', 'mesh'].map(f => (
              <button
                key={f}
                onClick={() => setAgentFilter(f)}
                className={`px-2 py-0.5 rounded text-[10px] font-mono transition cursor-pointer ${agentFilter === f ? 'bg-slate-700 text-white font-bold' : 'text-slate-500 hover:text-slate-300'}`}
              >
                {f === 'all' ? 'ALL' : AGENTS[f].label}
              </button>
            ))}
          </div>
        </div>
        <div className="rounded-xl border border-slate-800 bg-slate-900/40 divide-y divide-slate-800/60 font-mono">
          {feed.length === 0 ? (
            <div className="p-4 text-xs text-slate-500 text-center">No messages yet.</div>
          ) : feed.map(m => (
            <div key={m.id} className="px-3 py-2 flex items-start gap-2 text-[11px] hover:bg-slate-800/30">
              <span className="text-slate-600 shrink-0">{fmtTime(m.at)}</span>
              <span className="flex items-center gap-1 shrink-0">
                <AgentTag agent={m.from} /><ArrowRight size={11} className="text-slate-500" /><AgentTag agent={m.to} />
              </span>
              <span className="text-slate-300 flex-1 break-words">{m.summary}</span>
              {m.durationMs !== null && m.durationMs !== undefined && <span className="text-slate-500 shrink-0">{fmtMs(m.durationMs)}</span>}
              {m.status === 'error' && <span className="shrink-0 inline-flex items-center gap-1 text-rose-400"><XCircle size={12} /> Error</span>}
              {m.status === 'warn' && <span className="shrink-0 inline-flex items-center gap-1 text-amber-300"><Hourglass size={12} /> Warning</span>}
            </div>
          ))}
        </div>
      </section>
    </div>
  );
}
