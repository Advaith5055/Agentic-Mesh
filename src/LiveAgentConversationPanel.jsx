import { useState, useMemo } from 'react';
import {
  MessageSquare,
  Sparkles,
  ArrowRight,
  ShieldCheck,
  CheckCircle2,
  Clock,
  Layers,
  Database,
  ArrowRightLeft,
  AlertTriangle,
  Play,
  Check,
  X,
  ChevronDown,
  ChevronRight,
  Filter,
  RefreshCw,
  Hash,
  Terminal,
  Activity,
  User,
  Route,
  Brain,
  Hammer
} from 'lucide-react';

/**
 * Role display metadata with explicit user-specified colors:
 * - Planner: blue
 * - Executor: green
 * - Router: orange
 * - Validator: purple
 * - User: white
 */
const ROLE_CONFIG = {
  planner: {
    label: 'Planner',
    icon: Brain,
    textCls: 'text-blue-400',
    borderCls: 'border-blue-500/40',
    bgCls: 'bg-blue-950/40',
    badgeCls: 'bg-blue-900/40 text-blue-300 border border-blue-500/50',
    dotCls: 'bg-blue-400'
  },
  executor: {
    label: 'Executor',
    icon: Hammer,
    textCls: 'text-emerald-400',
    borderCls: 'border-emerald-500/40',
    bgCls: 'bg-emerald-950/40',
    badgeCls: 'bg-emerald-900/40 text-emerald-300 border border-emerald-500/50',
    dotCls: 'bg-emerald-400'
  },
  router: {
    label: 'Router',
    icon: Route,
    textCls: 'text-orange-400',
    borderCls: 'border-orange-500/40',
    bgCls: 'bg-orange-950/40',
    badgeCls: 'bg-orange-900/40 text-orange-300 border border-orange-500/50',
    dotCls: 'bg-orange-400'
  },
  validator: {
    label: 'Validator',
    icon: ShieldCheck,
    textCls: 'text-purple-400',
    borderCls: 'border-purple-500/40',
    bgCls: 'bg-purple-950/40',
    badgeCls: 'bg-purple-900/40 text-purple-300 border border-purple-500/50',
    dotCls: 'bg-purple-400'
  },
  user: {
    label: 'User',
    icon: User,
    textCls: 'text-white',
    borderCls: 'border-slate-400/50',
    bgCls: 'bg-slate-800',
    badgeCls: 'bg-slate-800 text-white border border-slate-500',
    dotCls: 'bg-white'
  },
  peer: {
    label: 'Mesh Peer',
    icon: Activity,
    textCls: 'text-cyan-400',
    borderCls: 'border-cyan-500/40',
    bgCls: 'bg-cyan-950/40',
    badgeCls: 'bg-cyan-900/40 text-cyan-300 border border-cyan-500/50',
    dotCls: 'bg-cyan-400'
  }
};

function getRoleMeta(role) {
  const normalized = (role || 'user').toLowerCase();
  return ROLE_CONFIG[normalized] || ROLE_CONFIG.user;
}

export default function LiveAgentConversationPanel({
  messages = [],
  tasks = [],
  selectedTaskId = null,
  onSelectTask,
  onTriggerDemoTask,
  onApproveTask,
  approvals = [],
  isTriggeringDemo = false,
  onRefresh
}) {
  const [filterRole, setFilterRole] = useState('all');
  const [expandedPlanMsgIds, setExpandedPlanMsgIds] = useState(new Set());

  // Filter messages by selected task (if any) and role
  const filteredMessages = useMemo(() => {
    let list = Array.isArray(messages) ? [...messages] : [];

    // Chronological order (oldest first for natural conversational timeline)
    list.sort((a, b) => {
      const ta = new Date(a.timestamp || 0).getTime();
      const tb = new Date(b.timestamp || 0).getTime();
      return ta - tb;
    });

    if (selectedTaskId) {
      list = list.filter(m => m.task_id === selectedTaskId || m.taskId === selectedTaskId);
    }

    if (filterRole !== 'all') {
      list = list.filter(m => {
        const sRole = (m.sender_role || '').toLowerCase();
        const rRole = (m.receiver_role || '').toLowerCase();
        return sRole === filterRole || rRole === filterRole;
      });
    }

    return list;
  }, [messages, selectedTaskId, filterRole]);

  const toggleExpandPlan = (msgId) => {
    setExpandedPlanMsgIds(prev => {
      const next = new Set(prev);
      if (next.has(msgId)) next.delete(msgId);
      else next.add(msgId);
      return next;
    });
  };

  const selectedTask = useMemo(() => {
    if (!selectedTaskId) return null;
    return tasks.find(t => t.id === selectedTaskId) || null;
  }, [tasks, selectedTaskId]);

  return (
    <div className="flex flex-col h-full bg-slate-950 text-slate-100 rounded-2xl border border-slate-800 shadow-xl overflow-hidden">
      {/* ── TOP HEADER / CONTROLS ── */}
      <div className="px-6 py-4 bg-slate-900/70 border-b border-slate-800 flex flex-wrap items-center justify-between gap-4">
        <div className="flex items-center gap-3">
          <div className="p-2 rounded-xl bg-blue-500/10 border border-blue-500/30 text-blue-400">
            <MessageSquare size={20} />
          </div>
          <div>
            <div className="flex items-center gap-2">
              <h2 className="text-base font-bold text-slate-100 tracking-tight">Live Agent Conversation</h2>
              <span className="flex items-center gap-1 px-2 py-0.5 rounded-full text-[10px] font-mono bg-emerald-500/15 text-emerald-400 border border-emerald-500/30">
                <span className="w-1.5 h-1.5 rounded-full bg-emerald-400 animate-pulse" />
                Live Mesh Stream
              </span>
              <span className="text-xs text-slate-400 font-mono">
                ({filteredMessages.length} msg{filteredMessages.length === 1 ? '' : 's'})
              </span>
            </div>
            <p className="text-xs text-slate-400 mt-0.5">
              Auditable peer-to-peer collaboration: Planner decomposition, validation receipts, human sign-off & distributed execution
            </p>
          </div>
        </div>

        {/* Task Selection & Actions */}
        <div className="flex items-center gap-2 flex-wrap">
          {/* Task Dropdown */}
          <div className="flex items-center gap-1.5 bg-slate-900 border border-slate-700/80 rounded-xl px-2.5 py-1.5 text-xs">
            <Filter size={13} className="text-slate-400" />
            <span className="text-slate-400 text-[11px]">Task:</span>
            <select
              value={selectedTaskId || ''}
              onChange={(e) => onSelectTask && onSelectTask(e.target.value || null)}
              className="bg-transparent text-slate-200 text-xs font-mono focus:outline-none cursor-pointer max-w-[200px] truncate"
            >
              <option value="" className="bg-slate-900 text-slate-300">All Tasks (Mesh Wide)</option>
              {tasks.map(t => (
                <option key={t.id} value={t.id} className="bg-slate-900 text-slate-200">
                  [{t.status.toUpperCase()}] {t.title || t.id}
                </option>
              ))}
            </select>
            {selectedTaskId && (
              <button
                onClick={() => onSelectTask && onSelectTask(null)}
                className="text-slate-400 hover:text-slate-200 p-0.5"
                title="Clear task filter"
              >
                <X size={12} />
              </button>
            )}
          </div>

          {/* Role Filter */}
          <div className="flex items-center gap-1.5 bg-slate-900 border border-slate-700/80 rounded-xl px-2.5 py-1.5 text-xs">
            <span className="text-slate-400 text-[11px]">Role:</span>
            <select
              value={filterRole}
              onChange={(e) => setFilterRole(e.target.value)}
              className="bg-transparent text-slate-200 text-xs font-mono focus:outline-none cursor-pointer"
            >
              <option value="all" className="bg-slate-900">All Roles</option>
              <option value="planner" className="bg-slate-900">Planner (Blue)</option>
              <option value="executor" className="bg-slate-900">Executor (Green)</option>
              <option value="router" className="bg-slate-900">Router (Orange)</option>
              <option value="validator" className="bg-slate-900">Validator (Purple)</option>
              <option value="user" className="bg-slate-900">User (White)</option>
            </select>
          </div>

          {/* Demo Button */}
          <button
            onClick={onTriggerDemoTask}
            disabled={isTriggeringDemo}
            className="flex items-center gap-1.5 px-3 py-1.5 rounded-xl bg-gradient-to-r from-blue-600 to-indigo-600 hover:from-blue-500 hover:to-indigo-500 text-white font-medium text-xs shadow-md transition disabled:opacity-50 cursor-pointer"
            title="Launch multi-step autonomous collaboration demonstration"
          >
            <Sparkles size={14} className={isTriggeringDemo ? 'animate-spin' : ''} />
            <span>Collaborative Mesh Intelligence Task</span>
          </button>

          {onRefresh && (
            <button
              onClick={onRefresh}
              className="p-1.5 rounded-xl bg-slate-900 border border-slate-700/80 text-slate-400 hover:text-slate-200 hover:bg-slate-800 transition"
              title="Refresh conversation timeline"
            >
              <RefreshCw size={14} />
            </button>
          )}
        </div>
      </div>

      {/* ── ROLE COLOR LEGEND ── */}
      <div className="px-6 py-2 bg-slate-900/40 border-b border-slate-800/80 flex flex-wrap items-center justify-between text-xs gap-3">
        <div className="flex items-center gap-4 flex-wrap">
          <span className="text-[11px] uppercase tracking-wider font-mono text-slate-400 font-bold">Node Roles:</span>
          
          <div className="flex items-center gap-1.5">
            <span className="w-2.5 h-2.5 rounded-full bg-blue-500 shadow-[0_0_8px_rgba(59,130,246,0.6)]" />
            <span className="text-blue-400 font-semibold font-mono text-[11px]">Planner</span>
          </div>

          <div className="flex items-center gap-1.5">
            <span className="w-2.5 h-2.5 rounded-full bg-emerald-500 shadow-[0_0_8px_rgba(16,185,129,0.6)]" />
            <span className="text-emerald-400 font-semibold font-mono text-[11px]">Executor</span>
          </div>

          <div className="flex items-center gap-1.5">
            <span className="w-2.5 h-2.5 rounded-full bg-orange-500 shadow-[0_0_8px_rgba(249,115,22,0.6)]" />
            <span className="text-orange-400 font-semibold font-mono text-[11px]">Router</span>
          </div>

          <div className="flex items-center gap-1.5">
            <span className="w-2.5 h-2.5 rounded-full bg-purple-500 shadow-[0_0_8px_rgba(168,85,247,0.6)]" />
            <span className="text-purple-400 font-semibold font-mono text-[11px]">Validator</span>
          </div>

          <div className="flex items-center gap-1.5">
            <span className="w-2.5 h-2.5 rounded-full bg-white shadow-[0_0_8px_rgba(255,255,255,0.6)]" />
            <span className="text-slate-100 font-semibold font-mono text-[11px]">User</span>
          </div>
        </div>

        {selectedTask && (
          <div className="flex items-center gap-2 text-xs">
            <span className="text-slate-400 font-mono text-[11px]">Viewing Task:</span>
            <span className="px-2 py-0.5 rounded bg-slate-800 border border-slate-700 text-cyan-300 font-mono text-[11px] font-bold">
              {selectedTask.id}
            </span>
            <span className={`px-2 py-0.5 rounded text-[10px] font-mono uppercase font-bold ${
              selectedTask.status === 'completed' ? 'bg-emerald-500/20 text-emerald-300 border border-emerald-500/40' :
              selectedTask.status === 'awaiting_approval' ? 'bg-amber-500/20 text-amber-300 border border-amber-500/40' :
              selectedTask.status === 'executing' ? 'bg-cyan-500/20 text-cyan-300 border border-cyan-500/40' :
              'bg-blue-500/20 text-blue-300 border border-blue-500/40'
            }`}>
              {selectedTask.status}
            </span>
          </div>
        )}
      </div>

      {/* ── PLANNER BRAIN INTELLIGENCE & PERFORMANCE BAR ── */}
      <div className="px-6 py-2.5 bg-blue-950/30 border-b border-blue-900/40 flex flex-wrap items-center justify-between text-xs gap-3">
        <div className="flex items-center gap-3 flex-wrap">
          <div className="flex items-center gap-1.5 text-blue-400 font-mono font-bold text-[11px]">
            <Brain size={14} className="text-blue-400 animate-pulse" />
            <span>PLANNER BRAIN:</span>
            <span className="text-cyan-300 font-semibold bg-blue-900/50 px-2 py-0.5 rounded border border-blue-500/40">
              Ollama Local Agent (qwen2.5-coder:1.5b)
            </span>
          </div>
          <span className="text-slate-600 text-xs">•</span>
          <div className="flex items-center gap-1 text-[11px] text-slate-300 font-mono">
            <span className="text-slate-400">Host:</span>
            <span className="text-slate-200">127.0.0.1:11434</span>
          </div>
          <span className="text-slate-600 text-xs">•</span>
          <div className="flex items-center gap-1 text-[11px] text-slate-300 font-mono">
            <span className="text-slate-400">Specialization:</span>
            <span className="text-emerald-400 font-semibold">3NF Relational Decomposition & Foreign Key Chaining</span>
          </div>
          <span className="text-slate-600 text-xs">•</span>
          <div className="flex items-center gap-1 text-[11px] text-slate-300 font-mono">
            <span className="text-slate-400">Self-Correction:</span>
            <span className="text-purple-400 font-semibold">Reflection Active</span>
          </div>
        </div>

        <div className="flex items-center gap-2 text-[11px] font-mono text-slate-400">
          <span className="flex items-center gap-1.5 text-emerald-400 font-medium">
            <span className="w-2 h-2 rounded-full bg-emerald-400 animate-ping inline-block" />
            Memory Resident (Keep-Alive)
          </span>
          <span className="text-slate-600">|</span>
          <span className="text-slate-300 font-semibold">Strict Separation: Plan Only</span>
        </div>
      </div>

      {/* ── TIMELINE CONVERSATION MESSAGES STREAM ── */}
      <div className="flex-1 overflow-y-auto p-6 space-y-4">
        {filteredMessages.length === 0 ? (
          <div className="h-64 flex flex-col items-center justify-center text-center p-8 rounded-xl border border-dashed border-slate-800 bg-slate-900/20">
            <MessageSquare size={36} className="text-slate-600 mb-3" />
            <h3 className="text-sm font-semibold text-slate-300">No agent conversation messages yet</h3>
            <p className="text-xs text-slate-500 max-w-md mt-1 mb-4">
              {selectedTaskId
                ? `No recorded messages for task ${selectedTaskId}. Plan or execute the task to initiate communication.`
                : 'Start the Collaborative Mesh Intelligence Task or dispatch a task from the Task Board to watch Planner and Executor communicate in real time.'}
            </p>
            <button
              onClick={onTriggerDemoTask}
              disabled={isTriggeringDemo}
              className="px-4 py-2 rounded-xl bg-blue-600 hover:bg-blue-500 text-white font-medium text-xs flex items-center gap-2 cursor-pointer shadow-lg"
            >
              <Sparkles size={14} />
              <span>Run Collaborative Mesh Intelligence Task</span>
            </button>
          </div>
        ) : (
          filteredMessages.map((msg, index) => {
            const senderMeta = getRoleMeta(msg.sender_role);
            const receiverMeta = getRoleMeta(msg.receiver_role);
            const SenderIcon = senderMeta.icon;
            const ReceiverIcon = receiverMeta.icon;

            const isPlanMsg = msg.message_type?.includes('PLAN') || msg.payload_metadata?.plan || msg.payload_metadata?.steps;
            const planData = msg.payload_metadata?.plan || (typeof msg.plan === 'object' ? msg.plan : null);
            const steps = planData?.ordered_steps || msg.payload_metadata?.steps || [];
            const isPlanExpanded = expandedPlanMsgIds.has(msg.message_id || `msg-${index}`);

            const isValidationMsg = msg.message_type?.includes('VALIDAT');
            const isApprovalMsg = msg.message_type?.includes('APPROVAL');
            const isExecutionMsg = msg.message_type?.includes('EXECUTION');
            const isReplicationMsg = msg.message_type?.includes('REPLICATION');

            // Format timestamp
            const timeStr = msg.timestamp
              ? new Date(msg.timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })
              : '';

            return (
              <div
                key={msg.message_id || `msg-${index}-${msg.timestamp}`}
                className={`relative rounded-xl border transition-all p-4 ${
                  senderMeta.borderCls
                } ${senderMeta.bgCls} shadow-md hover:border-slate-600`}
              >
                {/* Header row: Sender → Receiver, Role Badges, Message Type, IDs */}
                <div className="flex flex-wrap items-center justify-between gap-2 border-b border-slate-800/60 pb-2.5 mb-2.5">
                  <div className="flex items-center gap-2 flex-wrap">
                    {/* Sender Node + Role */}
                    <div className="flex items-center gap-1.5">
                      <span className={`flex items-center gap-1 px-2 py-0.8 rounded-lg text-xs font-mono font-bold ${senderMeta.badgeCls}`}>
                        <SenderIcon size={12} />
                        <span>{msg.sender_node || 'node'}</span>
                        <span className="text-[10px] opacity-80 uppercase">({senderMeta.label})</span>
                      </span>
                    </div>

                    {/* Direction Arrow */}
                    <ArrowRight size={13} className="text-slate-500" />

                    {/* Receiver Node + Role */}
                    <div className="flex items-center gap-1.5">
                      <span className={`flex items-center gap-1 px-2 py-0.8 rounded-lg text-xs font-mono font-bold ${receiverMeta.badgeCls}`}>
                        <ReceiverIcon size={12} />
                        <span>{msg.receiver_node || 'peer'}</span>
                        <span className="text-[10px] opacity-80 uppercase">({receiverMeta.label})</span>
                      </span>
                    </div>

                    {/* Message Type Pill */}
                    <span className="px-2 py-0.5 rounded text-[10px] font-mono uppercase font-bold bg-slate-900 border border-slate-700 text-cyan-300">
                      {msg.message_type || 'AGENT_MESSAGE'}
                    </span>

                    {/* Brain Badge (for Planner messages or messages with brain meta) */}
                    {(msg.sender_role === 'planner' || msg.payload_metadata?.brain || planData?.brain) && (
                      <span className="flex items-center gap-1 px-1.5 py-0.5 rounded text-[10px] font-mono bg-blue-950 border border-blue-600/50 text-blue-300 shadow-sm" title="Formulated by Ollama Local Agent Brain">
                        <Brain size={11} className="text-blue-400" />
                        <span>Ollama Brain</span>
                        {(msg.payload_metadata?.brain?.latencyMs || planData?.brain?.latencyMs) && (
                          <span className="text-emerald-400 font-bold ml-0.5">
                            ⚡{(msg.payload_metadata?.brain?.latencyMs || planData?.brain?.latencyMs)}ms
                          </span>
                        )}
                      </span>
                    )}
                  </div>

                  {/* Metadata: Task ID, Request ID, Timestamp */}
                  <div className="flex items-center gap-2 text-[11px] font-mono text-slate-400">
                    {msg.task_id && (
                      <span className="px-1.5 py-0.5 rounded bg-slate-900 border border-slate-800 text-slate-300">
                        task:{msg.task_id}
                      </span>
                    )}
                    {msg.request_id && (
                      <span className="px-1.5 py-0.5 rounded bg-slate-900 border border-slate-800 text-slate-400">
                        reqId:{String(msg.request_id).slice(0, 12)}
                      </span>
                    )}
                    {timeStr && (
                      <span className="flex items-center gap-1 text-slate-400">
                        <Clock size={11} />
                        {timeStr}
                      </span>
                    )}
                  </div>
                </div>

                {/* Operational Summary */}
                <div className="text-xs text-slate-200 font-sans leading-relaxed">
                  <span className="font-semibold text-slate-100">{msg.summary}</span>
                </div>

                {/* ── STEP-BY-STEP PLAN CARD (IF AVAILABLE) ── */}
                {steps && steps.length > 0 && (
                  <div className="mt-3 pt-3 border-t border-slate-800/80">
                    <button
                      onClick={() => toggleExpandPlan(msg.message_id || `msg-${index}`)}
                      className="flex items-center gap-1.5 text-xs font-mono text-blue-400 hover:text-blue-300 font-semibold cursor-pointer mb-2"
                    >
                      {isPlanExpanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
                      <Layers size={13} />
                      <span>Planner Decomposed Multi-Step Plan ({steps.length} Steps)</span>
                    </button>

                    {isPlanExpanded && (
                      <div className="space-y-2 mt-2 bg-slate-900/80 p-3 rounded-xl border border-slate-800">
                        {planData?.goal && (
                          <div className="text-[11px] text-slate-300 font-mono mb-2 pb-2 border-b border-slate-800">
                            <span className="text-blue-400 font-bold uppercase">Plan Goal:</span> {planData.goal}
                          </div>
                        )}
                        {steps.map((st, sIdx) => {
                          const stepRoleMeta = getRoleMeta(st.responsible_role);
                          return (
                            <div
                              key={st.step_id || sIdx}
                              className="p-2.5 rounded-lg bg-slate-950/60 border border-slate-800 text-xs flex flex-col gap-1"
                            >
                              <div className="flex items-center justify-between">
                                <span className="font-mono font-bold text-slate-200">
                                  Step {sIdx + 1}: {st.title || st.goal}
                                </span>
                                <span className={`px-2 py-0.5 rounded text-[10px] font-mono ${stepRoleMeta.badgeCls}`}>
                                  {stepRoleMeta.label} → {st.assigned_node_id || 'auto'}
                                </span>
                              </div>
                              <div className="text-[11px] text-slate-400">{st.goal}</div>
                              {st.expected_outputs && (
                                <div className="text-[10px] font-mono text-cyan-400/90 mt-1">
                                  Expected: {JSON.stringify(st.expected_outputs)}
                                </div>
                              )}
                            </div>
                          );
                        })}
                      </div>
                    )}
                  </div>
                )}

                {/* ── VALIDATION MESSAGES ── */}
                {isValidationMsg && (
                  <div className="mt-2.5 flex items-center gap-2 p-2 rounded-lg bg-purple-950/30 border border-purple-800/40 text-[11px] text-purple-200 font-mono">
                    <ShieldCheck size={14} className="text-purple-400 shrink-0" />
                    <span>Local schema validation verified: Allowlist & 3NF integrity constraints satisfied.</span>
                  </div>
                )}

                {/* ── APPROVAL EVENT CARD ── */}
                {isApprovalMsg && (
                  <div className="mt-2.5 flex items-center justify-between gap-2 p-2.5 rounded-lg bg-amber-950/30 border border-amber-800/40 text-xs font-mono">
                    <div className="flex items-center gap-2 text-amber-200">
                      <AlertTriangle size={15} className="text-amber-400 shrink-0" />
                      <span>
                        Human Approval Policy: {msg.status?.toUpperCase() || 'REQUIRED'}
                      </span>
                    </div>

                    {msg.status === 'awaiting_approval' && onApproveTask && (
                      <button
                        onClick={() => onApproveTask(msg.payload_metadata?.approvalId, msg.task_id)}
                        className="px-2.5 py-1 rounded bg-emerald-600 hover:bg-emerald-500 text-white font-sans text-xs font-semibold cursor-pointer shadow transition"
                      >
                        Approve Now
                      </button>
                    )}
                  </div>
                )}

                {/* ── REPLICATION EVENT CARD ── */}
                {isReplicationMsg && (
                  <div className="mt-2.5 flex items-center gap-2 p-2 rounded-lg bg-cyan-950/30 border border-cyan-800/40 text-[11px] text-cyan-200 font-mono">
                    <ArrowRightLeft size={14} className="text-cyan-400 shrink-0" />
                    <span>Mesh Gossip Replication Published: Transaction committed, vector clock incremented across peers.</span>
                  </div>
                )}

                {/* ── EXECUTION RESULT CARD ── */}
                {msg.message_type === 'EXECUTION_RESULT' && (
                  <div className="mt-2.5 p-2.5 rounded-lg bg-emerald-950/40 border border-emerald-700/50 text-xs">
                    <div className="flex items-center gap-2 text-emerald-300 font-mono font-bold mb-1">
                      <CheckCircle2 size={14} className="text-emerald-400" />
                      <span>Execution Result: Task Successfully Completed</span>
                    </div>
                    {msg.payload_metadata?.results && (
                      <pre className="text-[11px] font-mono text-emerald-200/90 bg-slate-950/70 p-2 rounded border border-emerald-900/50 overflow-x-auto">
                        {typeof msg.payload_metadata.results === 'string'
                          ? msg.payload_metadata.results
                          : JSON.stringify(msg.payload_metadata.results, null, 2)}
                      </pre>
                    )}
                  </div>
                )}
              </div>
            );
          })
        )}
      </div>
    </div>
  );
}
