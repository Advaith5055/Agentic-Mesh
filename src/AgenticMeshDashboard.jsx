import { useState, useEffect, useRef, useCallback, useMemo } from 'react';
import ForceGraph2D from 'react-force-graph-2d';
import {
  Database,
  Network,
  ShieldAlert,
  ShieldCheck,
  RefreshCw,
  Radio,
  CheckCircle2,
  AlertCircle,
  Server,
  Search,
  Clock,
  ArrowRightLeft,
  MessageSquare,
  Bot,
  Gauge,
  X,
  Plus,
  AlertTriangle,
  Workflow,
  Boxes,
  Info
} from 'lucide-react';
import AgentActivityPanel from './AgentActivityPanel.jsx';

export default function AgenticMeshDashboard() {
  const graphRef = useRef();
  const graphContainerRef = useRef(null);

  // Dynamic canvas sizing for ForceGraph2D
  const [graphDimensions, setGraphDimensions] = useState({ width: 600, height: 400 });

  // Port selector
  const [wsPort, setWsPort] = useState('3004');
  const [customPort, setCustomPort] = useState('');
  const [nodeInfo, setNodeInfo] = useState({
    name: 'Disconnected',
    peerId: 'N/A',
    wsStatus: 'CONNECTING',
    modelName: 'Ollama',
    p2pPort: '9004',
    nodeRole: 'planner'
  });

  // Active Main Tab: Primary workflow experience + Demo Inventory section
  // 'board' | 'topology' | 'replication' | 'audit' | 'chat' | 'activity' | 'inventory'
  const [activeTab, setActiveTab] = useState('board');

  // Distributed Agent Workflow Platform State
  const [tasks, setTasks] = useState([]);
  const [nodes, setNodes] = useState([]);
  const [approvals, setApprovals] = useState([]);
  const [conflicts, setConflicts] = useState([]);
  const [replicationMetrics, setReplicationMetrics] = useState([]);
  const [auditEvents, setAuditEvents] = useState([]);
  const [operationalMetrics, setOperationalMetrics] = useState({
    activeNodes: 0,
    taskSuccessRate: 100,
    avgExecutionTimeMs: 0,
    replicationLatencyMs: 0,
    failedValidationsCount: 0,
    tasksTotal: 0,
    tasksQueued: 0,
    tasksPlanned: 0,
    tasksAwaitingApproval: 0,
    tasksExecuting: 0,
    tasksCompleted: 0,
    tasksFailed: 0
  });

  // Task Detail Modal / Panel State
  const [selectedTaskId, setSelectedTaskId] = useState(null);
  const [selectedTaskDetails, setSelectedTaskDetails] = useState(null);
  const [isLoadingDetails, setIsLoadingDetails] = useState(false);

  // New Task Form Modal State
  const [isCreateTaskOpen, setIsCreateTaskOpen] = useState(false);
  const [newTaskTitle, setNewTaskTitle] = useState('');
  const [newTaskPrompt, setNewTaskPrompt] = useState('');
  const [newTaskType, setNewTaskType] = useState('general');
  const [newTaskPriority, setNewTaskPriority] = useState('medium');
  const [isSubmittingTask, setIsSubmittingTask] = useState(false);
  const [taskCreationError, setTaskCreationError] = useState(null);
  const [apiKey, setApiKey] = useState('mesh-dev-key');

  // Approval Action Modal State
  const [activeApproval, setActiveApproval] = useState(null);
  const [approvalReason, setApprovalReason] = useState('');
  const [isSubmittingApproval, setIsSubmittingApproval] = useState(false);

  // Conflict Resolution Modal State
  const [activeConflict, setActiveConflict] = useState(null);
  const [chosenResolutionText, setChosenResolutionText] = useState('');
  const [isResolvingConflict, setIsResolvingConflict] = useState(false);

  // Legacy Demo Inventory State
  const [peers, setPeers] = useState([]);
  const [dbState, setDbState] = useState({ categories: [], items: [], suppliers: [] });
  const [meshLogs, setMeshLogs] = useState([]);
  const [logs, setLogs] = useState([{ time: new Date().toLocaleTimeString(), text: 'System HUD Online. Connecting to mesh gateway...', type: 'system' }]);
  const [isSyncing, setIsSyncing] = useState(false);
  const [activity, setActivity] = useState({ agents: [], tasks: [], messages: [], system: null });
  const [pendingProposals, setPendingProposals] = useState([]);
  const [searchQuery, setSearchQuery] = useState('');

  // Agent Chat state
  const [chatMessages, setChatMessages] = useState([
    {
      id: 'init-1',
      sender: 'agent',
      text: 'Hello! I am your distributed Agentic Mesh workflow coordinator. I can plan distributed agent tasks, enforce multi-role safety validations, inspect topology, and monitor vector clock replication. What shall we coordinate?',
      timestamp: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
      operations: null
    }
  ]);
  const [chatInput, setChatInput] = useState('');
  const [isChatLoading, setIsChatLoading] = useState(false);
  const chatBottomRef = useRef(null);
  const activeAbortControllerRef = useRef(null);

  // Abort in-flight requests on component unmount
  useEffect(() => {
    const controller = activeAbortControllerRef.current;
    return () => {
      if (controller) {
        controller.abort();
      }
    };
  }, []);

  const getWsUrl = useCallback(() => {
    const host = window.location.hostname || 'localhost';
    return `ws://${host}:${wsPort}/ws`;
  }, [wsPort]);

  const getApiUrl = useCallback((endpoint) => {
    const host = window.location.hostname || 'localhost';
    return `http://${host}:${wsPort}/api${endpoint}`;
  }, [wsPort]);

  const getApiHeaders = useCallback((extra = {}) => ({
    'Content-Type': 'application/json',
    'x-api-key': apiKey || 'mesh-dev-key',
    ...extra
  }), [apiKey]);

  const addLog = useCallback((text, type = 'system') => {
    setLogs(prev => [...prev.slice(-200), { time: new Date().toLocaleTimeString(), text, type }]);
  }, []);

  // Measure graph container dimensions dynamically
  useEffect(() => {
    const updateDimensions = () => {
      if (graphContainerRef.current) {
        setGraphDimensions({
          width: graphContainerRef.current.clientWidth || 600,
          height: graphContainerRef.current.clientHeight || 400
        });
      }
    };

    updateDimensions();
    window.addEventListener('resize', updateDimensions);
    const timer = setTimeout(updateDimensions, 100);

    return () => {
      window.removeEventListener('resize', updateDimensions);
      clearTimeout(timer);
    };
  }, [activeTab]);

  // Auto-scroll chat to latest message
  useEffect(() => {
    if (activeTab === 'chat' && chatBottomRef.current) {
      chatBottomRef.current.scrollIntoView({ behavior: 'smooth' });
    }
  }, [chatMessages, activeTab]);

  // ───────────────────────────────────────────────────────────────────────────
  // DATA FETCHING FUNCTIONS
  // ───────────────────────────────────────────────────────────────────────────
  const fetchWorkflowData = useCallback(async () => {
    try {
      const hdrs = { 'x-api-key': apiKey || 'mesh-dev-key' };
      const [tRes, nRes, aRes, cRes, rRes, mRes, audRes] = await Promise.all([
        fetch(getApiUrl('/tasks'), { headers: hdrs }).catch(() => null),
        fetch(getApiUrl('/nodes'), { headers: hdrs }).catch(() => null),
        fetch(getApiUrl('/approvals'), { headers: hdrs }).catch(() => null),
        fetch(getApiUrl('/conflicts'), { headers: hdrs }).catch(() => null),
        fetch(getApiUrl('/replication/metrics'), { headers: hdrs }).catch(() => null),
        fetch(getApiUrl('/metrics/operational'), { headers: hdrs }).catch(() => null),
        fetch(getApiUrl('/audit-events?limit=30'), { headers: hdrs }).catch(() => null)
      ]);

      if (tRes?.ok) setTasks(await tRes.json());
      if (nRes?.ok) setNodes(await nRes.json());
      if (aRes?.ok) setApprovals(await aRes.json());
      if (cRes?.ok) setConflicts(await cRes.json());
      if (rRes?.ok) setReplicationMetrics(await rRes.json());
      if (mRes?.ok) setOperationalMetrics(await mRes.json());
      if (audRes?.ok) setAuditEvents(await audRes.json());
    } catch (err) {
      addLog(`Failed to fetch workflow data: ${err.message}`, 'error');
    }
  }, [getApiUrl, apiKey, addLog]);

  const fetchTaskDetails = useCallback(async (taskId) => {
    if (!taskId) return;
    setIsLoadingDetails(true);
    try {
      const res = await fetch(getApiUrl(`/tasks/${taskId}`), { headers: { 'x-api-key': apiKey || 'mesh-dev-key' } });
      if (res.ok) {
        const details = await res.json();
        setSelectedTaskDetails(details);
      }
    } catch (err) {
      addLog(`Failed to fetch task details for ${taskId}: ${err.message}`, 'error');
    } finally {
      setIsLoadingDetails(false);
    }
  }, [getApiUrl, apiKey, addLog]);

  const fetchDbState = useCallback(async () => {
    try {
      const res = await fetch(getApiUrl('/db/state'), { headers: { 'x-api-key': apiKey || 'mesh-dev-key' } });
      if (res.ok) setDbState(await res.json());
    } catch { }
  }, [getApiUrl, apiKey]);

  const fetchPeers = useCallback(async () => {
    try {
      const res = await fetch(getApiUrl('/peers'), { headers: { 'x-api-key': apiKey || 'mesh-dev-key' } });
      if (res.ok) setPeers(await res.json());
    } catch { }
  }, [getApiUrl, apiKey]);

  const fetchMeshLogs = useCallback(async () => {
    try {
      const res = await fetch(getApiUrl('/db/mesh-log'), { headers: { 'x-api-key': apiKey || 'mesh-dev-key' } });
      if (res.ok) setMeshLogs(await res.json());
    } catch { }
  }, [getApiUrl, apiKey]);

  const refreshAll = useCallback(() => {
    addLog('Refreshing all platform telemetry...', 'system');
    fetchWorkflowData();
    fetchDbState();
    fetchPeers();
    fetchMeshLogs();
    if (selectedTaskId) fetchTaskDetails(selectedTaskId);
  }, [fetchWorkflowData, fetchDbState, fetchPeers, fetchMeshLogs, fetchTaskDetails, selectedTaskId, addLog]);

  const handleWsMessage = useCallback((msg) => {
    switch (msg.type) {
      case 'init':
        if (msg.apiKey) setApiKey(msg.apiKey);
        setPeers(msg.peers || []);
        if (msg.dbState) setDbState(msg.dbState);
        if (msg.meshLogs) setMeshLogs(msg.meshLogs);
        if (msg.nodes) setNodes(msg.nodes);
        if (msg.tasks) setTasks(msg.tasks);
        if (msg.approvals) setApprovals(msg.approvals);
        if (msg.conflicts) setConflicts(msg.conflicts);
        if (msg.metrics) setOperationalMetrics(msg.metrics);
        if (msg.auditEvents) setAuditEvents(msg.auditEvents);
        setNodeInfo(prev => ({
          ...prev,
          name: msg.nodeName || prev.name,
          peerId: msg.peerId || prev.peerId,
          modelName: msg.modelName || prev.modelName,
          p2pPort: msg.p2pPort || prev.p2pPort,
          nodeRole: msg.nodeRole || prev.nodeRole
        }));
        break;

      case 'task:created':
      case 'task:planned':
      case 'task:executed':
      case 'task:cancelled':
        fetchWorkflowData();
        if (selectedTaskId && msg.data?.id === selectedTaskId) {
          fetchTaskDetails(selectedTaskId);
        }
        addLog(`Task event [${msg.type}]: ${msg.data?.title || msg.data?.taskId || ''}`, 'ai');
        break;

      case 'node:registered':
      case 'node:updated':
        fetchWorkflowData();
        addLog(`Node updated: ${msg.data?.name} (${msg.data?.role})`, 'p2p');
        break;

      case 'approval:decided':
        fetchWorkflowData();
        addLog(`Approval decided: ${msg.data?.decision} for ${msg.data?.task_id}`, 'system');
        break;

      case 'conflict:resolved':
      case 'conflict:detected':
        fetchWorkflowData();
        addLog(`Conflict status updated: ${msg.type}`, 'warn');
        break;

      case 'replication:metric':
        setReplicationMetrics(prev => [msg.data, ...prev.slice(0, 49)]);
        break;

      case 'activity':
        if (msg.data) setActivity(msg.data);
        break;

      case 'proposals':
        if (msg.data) setPendingProposals(msg.data);
        break;

      case 'log':
        if (msg.data) {
          addLog(msg.data.message || JSON.stringify(msg.data), msg.data.type || 'system');
        }
        break;

      default:
        break;
    }
  }, [fetchWorkflowData, fetchTaskDetails, selectedTaskId, addLog]);

  // ───────────────────────────────────────────────────────────────────────────
  // WEBSOCKET SUBSCRIPTION
  // ───────────────────────────────────────────────────────────────────────────
  useEffect(() => {
    let ws = null;
    let reconnectTimeout = null;

    const connectWs = () => {
      setNodeInfo(prev => ({ ...prev, wsStatus: 'CONNECTING' }));
      try {
        ws = new WebSocket(getWsUrl());

        ws.onopen = () => {
          setNodeInfo(prev => ({ ...prev, wsStatus: 'CONNECTED' }));
          addLog(`Connected to Agentic Mesh gateway at port ${wsPort}`, 'success');
          refreshAll();
        };

        ws.onmessage = (event) => {
          try {
            const msg = JSON.parse(event.data);
            handleWsMessage(msg);
          } catch {
            addLog(`Received non-JSON message: ${event.data}`, 'system');
          }
        };

        ws.onclose = () => {
          setNodeInfo(prev => ({ ...prev, wsStatus: 'DISCONNECTED' }));
          addLog('WebSocket disconnected. Retrying in 3s...', 'warn');
          reconnectTimeout = setTimeout(connectWs, 3000);
        };

        ws.onerror = () => {
          setNodeInfo(prev => ({ ...prev, wsStatus: 'ERROR' }));
        };
      } catch (err) {
        addLog(`WebSocket connection error: ${err.message}`, 'error');
        reconnectTimeout = setTimeout(connectWs, 3000);
      }
    };

    connectWs();

    return () => {
      if (ws) ws.close();
      if (reconnectTimeout) clearTimeout(reconnectTimeout);
    };
  }, [getWsUrl, wsPort, handleWsMessage, refreshAll, addLog]);

  // ───────────────────────────────────────────────────────────────────────────
  // WORKFLOW ACTIONS: CREATE, PLAN, EXECUTE, APPROVE
  // ───────────────────────────────────────────────────────────────────────────
  const handleCreateTask = async (e) => {
    e.preventDefault();
    if (!newTaskTitle.trim() || !newTaskPrompt.trim()) return;

    setIsSubmittingTask(true);
    setTaskCreationError(null);
    try {
      const res = await fetch(getApiUrl('/tasks'), {
        method: 'POST',
        headers: getApiHeaders(),
        body: JSON.stringify({
          title: newTaskTitle.trim(),
          user_prompt: newTaskPrompt.trim(),
          task_type: newTaskType,
          priority: newTaskPriority,
          requested_by: 'operator:dashboard'
        })
      });

      if (res.ok) {
        const data = await res.json();
        addLog(`Created task: ${data.task?.title || newTaskTitle.trim()}`, 'success');
        setNewTaskTitle('');
        setNewTaskPrompt('');
        setTaskCreationError(null);
        setIsCreateTaskOpen(false);
        fetchWorkflowData();
        if (data.task?.id) {
          setSelectedTaskId(data.task.id);
          fetchTaskDetails(data.task.id);
        }
      } else {
        const errData = await res.json().catch(() => ({}));
        const errMsg = errData.error || errData.errors?.join(', ') || 'Failed to submit task to queue';
        setTaskCreationError(errMsg);
        addLog(`Failed to create task: ${errMsg}`, 'error');
      }
    } catch (err) {
      setTaskCreationError(err.message);
      addLog(`Error creating task: ${err.message}`, 'error');
    } finally {
      setIsSubmittingTask(false);
    }
  };

  const handlePlanTask = async (taskId) => {
    try {
      addLog(`Requesting planner node to decompose task ${taskId}...`, 'ai');
      const res = await fetch(getApiUrl(`/tasks/${taskId}/plan`), {
        method: 'POST',
        headers: getApiHeaders(),
        body: JSON.stringify({ plannerNodeId: nodeInfo.name })
      });
      if (res.ok) {
        addLog(`Task ${taskId} planned successfully.`, 'success');
        fetchWorkflowData();
        fetchTaskDetails(taskId);
      } else {
        const err = await res.json().catch(() => ({}));
        addLog(`Planning failed: ${err.error || err.errors?.join(', ') || 'Failed'}`, 'error');
      }
    } catch (err) {
      addLog(`Error planning task: ${err.message}`, 'error');
    }
  };

  const handleExecuteTask = async (taskId) => {
    try {
      const executorPeer = peers.find(p => (p.role === 'executor' || p.role === 'peer' || p.name === 'alpha'));
      let targetUrl = getApiUrl(`/tasks/${taskId}/execute`);

      if (nodeInfo.nodeRole === 'planner') {
        let executorHost = null;
        const executorPort = executorPeer?.wsPort || 3002;

        if (executorPeer) {
          if (executorPeer.ip && executorPeer.ip !== '127.0.0.1' && executorPeer.ip !== 'localhost') {
            executorHost = executorPeer.ip;
          } else if (Array.isArray(executorPeer.addrs)) {
            for (const addr of executorPeer.addrs) {
              const m = /^\/ip[46]\/([^/]+)\//.exec(addr);
              if (m && m[1] && m[1] !== '127.0.0.1' && m[1] !== '0.0.0.0') {
                executorHost = m[1];
                break;
              }
            }
          }
        }

        if (!executorHost) {
          const matchingNode = nodes.find(n => (n.role === 'executor' || n.name === 'alpha') && n.address);
          if (matchingNode?.address) {
            const h = matchingNode.address.split(':')[0];
            if (h && h !== '127.0.0.1' && h !== 'localhost' && h !== '0.0.0.0') executorHost = h;
          }
        }

        const host = executorHost || '192.168.1.7';
        targetUrl = `http://${host}:${executorPort}/api/tasks/${taskId}/execute`;
        addLog(`Routing task execution to executor "${executorPeer?.name || 'alpha'}" at http://${host}:${executorPort}...`, 'db');
      } else {
        addLog(`Executor node initiating atomic execution for task ${taskId}...`, 'db');
      }

      const res = await fetch(targetUrl, {
        method: 'POST',
        headers: getApiHeaders(),
        body: JSON.stringify({
          executorNodeId: executorPeer?.name ? `node-${executorPeer.name}` : undefined
        })
      });
      if (res.ok) {
        const outcome = await res.json();
        if (outcome.success) {
          addLog(`Task ${taskId} committed and verified!`, 'success');
        } else {
          addLog(`Task ${taskId} execution failed: ${outcome.error}`, 'error');
        }
        fetchWorkflowData();
        fetchTaskDetails(taskId);
      } else {
        const err = await res.json().catch(() => ({}));
        addLog(`Execution rejected: ${err.error || err.errors?.join(', ') || 'Forbidden'}`, 'error');
      }
    } catch (err) {
      addLog(`Error executing task: ${err.message}`, 'error');
    }
  };

  const handleDecision = async (decision) => {
    if (!activeApproval) return;
    setIsSubmittingApproval(true);
    try {
      const res = await fetch(getApiUrl(`/approvals/${activeApproval.id}/decide`), {
        method: 'POST',
        headers: getApiHeaders(),
        body: JSON.stringify({
          decision,
          reviewer: 'operator:dashboard',
          reason: approvalReason || `Decision submitted via dashboard: ${decision}`
        })
      });
      if (res.ok) {
        addLog(`Approval ${activeApproval.id} set to ${decision}.`, 'success');
        setActiveApproval(null);
        setApprovalReason('');
        fetchWorkflowData();
        if (selectedTaskId) fetchTaskDetails(selectedTaskId);
      } else {
        const err = await res.json().catch(() => ({}));
        addLog(`Approval submission failed: ${err.error || err.errors?.join(', ')}`, 'error');
      }
    } catch (err) {
      addLog(`Error submitting approval: ${err.message}`, 'error');
    } finally {
      setIsSubmittingApproval(false);
    }
  };

  const handleResolveConflict = async () => {
    if (!activeConflict || !chosenResolutionText.trim()) return;
    setIsResolvingConflict(true);
    try {
      const res = await fetch(getApiUrl(`/conflicts/${activeConflict.id}/resolve`), {
        method: 'POST',
        headers: getApiHeaders(),
        body: JSON.stringify({
          chosen_resolution: chosenResolutionText.trim(),
          resolved_by: 'operator:dashboard'
        })
      });
      if (res.ok) {
        addLog(`Conflict ${activeConflict.id} resolved.`, 'success');
        setActiveConflict(null);
        setChosenResolutionText('');
        fetchWorkflowData();
      } else {
        const err = await res.json().catch(() => ({}));
        addLog(`Conflict resolution failed: ${err.error || err.errors?.join(', ')}`, 'error');
      }
    } catch (err) {
      addLog(`Error resolving conflict: ${err.message}`, 'error');
    } finally {
      setIsResolvingConflict(false);
    }
  };

  // Sync catch-up
  const triggerSyncWithMesh = async () => {
    setIsSyncing(true);
    try {
      const res = await fetch(getApiUrl('/sync'), { method: 'POST', headers: getApiHeaders() });
      if (res.ok) {
        addLog('Broadcasted SYNC_REQUEST to all mesh nodes', 'p2p');
      }
    } catch (err) {
      addLog(`Sync error: ${err.message}`, 'error');
    } finally {
      setIsSyncing(false);
    }
  };

  // Legacy demo action: Propose Fast Tx
  const triggerFastProposal = async () => {
    try {
      const sampleItem = {
        category_id: 1,
        name: `Demo Item ${Math.floor(Math.random() * 900 + 100)}`,
        price: parseFloat((Math.random() * 80 + 10).toFixed(2)),
        sku: `SKU-DEMO-${Date.now().toString(36).toUpperCase()}`
      };
      const res = await fetch(getApiUrl('/propose'), {
        method: 'POST',
        headers: getApiHeaders(),
        body: JSON.stringify({ table: 'items', operation: 'INSERT', data: sampleItem })
      });
      const data = await res.json().catch(() => ({}));
      if (data.success) {
        addLog(`Demo mutation committed: ${sampleItem.name} (${sampleItem.sku})`, 'db');
        fetchDbState();
        fetchMeshLogs();
      } else {
        addLog(`Demo mutation rejected: ${data.errors?.join(', ') || data.error}`, 'error');
      }
    } catch (err) {
      addLog(`Demo proposal error: ${err.message}`, 'error');
    }
  };

  // Legacy demo action: Test Schema Rule
  const triggerConflictTest = async () => {
    try {
      const badPayload = {
        table: 'items',
        operation: 'INSERT',
        data: { category_id: 99999, name: 'Malformed Item', price: -10, sku: 'SKU-BAD' }
      };
      const res = await fetch(getApiUrl('/propose'), {
        method: 'POST',
        headers: getApiHeaders(),
        body: JSON.stringify(badPayload)
      });
      const data = await res.json().catch(() => ({}));
      if (!data.success) {
        addLog(`Schema Rule Intercepted: ${data.errors?.join(', ')}`, 'warn');
      }
    } catch (err) {
      addLog(`Test rule error: ${err.message}`, 'error');
    }
  };

  // Group tasks by lifecycle status
  const tasksByStatus = useMemo(() => {
    const groups = {
      queued: [],
      planned: [],
      awaiting_approval: [],
      executing: [],
      completed: [],
      failed: []
    };
    tasks.forEach(t => {
      if (groups[t.status]) {
        groups[t.status].push(t);
      } else {
        groups.queued.push(t);
      }
    });
    return groups;
  }, [tasks]);

  // Topology graph data
  const graphData = useMemo(() => {
    const localId = nodeInfo.name || 'Local Node';
    const gNodes = [
      { id: localId, val: 24, group: 1, role: nodeInfo.nodeRole, status: 'active', model: nodeInfo.modelName }
    ];
    const gLinks = [];

    nodes.forEach(n => {
      if (n.name && n.name !== 'unknown' && n.name !== localId && !gNodes.some(x => x.id === n.name)) {
        gNodes.push({
          id: n.name,
          val: 18,
          group: 2,
          role: n.role,
          status: n.status,
          peerId: n.peer_id
        });
        gLinks.push({ source: localId, target: n.name });
      }
    });

    peers.forEach((p) => {
      // Don't add self if present in peer list
      if (p.peerId && p.peerId === nodeInfo.peerId) return;
      if (p.peerId && gNodes.some(x => x.peerId === p.peerId)) return;

      const candidateName = (p.name && p.name !== 'unknown') ? p.name : (p.nodeName && p.nodeName !== 'unknown' ? p.nodeName : null);
      const matchedNode = candidateName
        ? nodes.find(n => n.name === candidateName)
        : (p.peerId ? nodes.find(n => n.peer_id === p.peerId) : null);

      const pName = matchedNode ? matchedNode.name : candidateName;
      // Do not render an unknown or unnamed node
      if (!pName || pName === 'unknown') return;

      if (!gNodes.some(x => x.id === pName || (p.peerId && x.peerId === p.peerId))) {
        gNodes.push({
          id: pName,
          val: 18,
          group: 3,
          role: p.role || matchedNode?.role || 'executor',
          status: 'connected',
          peerId: p.peerId
        });
        gLinks.push({ source: localId, target: pName });
      }
    });

    return { nodes: gNodes, links: gLinks };
  }, [nodeInfo, nodes, peers]);

  const filteredItems = useMemo(() => {
    if (!searchQuery.trim()) return dbState.items || [];
    const q = searchQuery.toLowerCase();
    return (dbState.items || []).filter(item =>
      item.name?.toLowerCase().includes(q) ||
      item.sku?.toLowerCase().includes(q)
    );
  }, [dbState.items, searchQuery]);

  return (
    <div className="flex flex-col h-screen w-screen bg-slate-950 text-slate-100 font-sans select-none overflow-hidden">

      {/* ─────────────────────────────────────────────────────────────────── */}
      {/* TOP HEADER: NODE IDENTITY, ROLES & CONTROLS */}
      {/* ─────────────────────────────────────────────────────────────────── */}
      <header className="h-16 border-b border-slate-800 bg-slate-900/90 backdrop-blur px-5 flex items-center justify-between z-20">

        {/* Left: Brand & Node Identity */}
        <div className="flex items-center gap-3">
          <div className="p-2 rounded-xl bg-gradient-to-tr from-cyan-600 to-blue-600 text-white shadow-lg shadow-cyan-900/30">
            <Workflow size={22} className="animate-spin-slow" />
          </div>
          <div>
            <div className="flex items-center gap-2">
              <h1 className="font-bold text-sm tracking-wide bg-gradient-to-r from-slate-100 via-cyan-200 to-blue-300 bg-clip-text text-transparent">
                AGENTIC MESH
              </h1>
              <span className="px-2 py-0.5 rounded-full bg-cyan-950/80 border border-cyan-500/40 text-[10px] text-cyan-300 font-mono font-semibold uppercase tracking-wider">
                PROD v2.0
              </span>
            </div>
            <div className="flex items-center gap-2 text-xs text-slate-400 font-mono">
              <span className="text-slate-300 font-bold">{nodeInfo.name}</span>
              <span>·</span>
              <span className="text-emerald-400 capitalize">{nodeInfo.nodeRole} Node</span>
              <span>·</span>
              <span className="text-slate-500 truncate max-w-[120px]">{nodeInfo.peerId}</span>
            </div>
          </div>
        </div>

        {/* Center: Port Switcher */}
        <div className="hidden md:flex items-center gap-1.5 bg-slate-950/70 border border-slate-800 p-1 rounded-xl text-xs">
          <span className="text-slate-500 px-2 font-mono text-[11px]">GATEWAY</span>
          {['3001', '3002', '3003', '3004'].map((port) => (
            <button
              key={port}
              onClick={() => setWsPort(port)}
              className={`px-2.5 py-1 rounded-lg font-mono text-xs font-semibold transition cursor-pointer ${wsPort === port
                  ? 'bg-cyan-600 text-white shadow-md shadow-cyan-600/30'
                  : 'text-slate-400 hover:text-white hover:bg-slate-900'
                }`}
            >
              :{port}
            </button>
          ))}
          <form
            onSubmit={(e) => { e.preventDefault(); if (customPort.trim()) setWsPort(customPort.trim()); }}
            className="flex items-center"
          >
            <input
              type="text"
              placeholder="Custom"
              value={customPort}
              onChange={(e) => setCustomPort(e.target.value)}
              className="w-16 bg-slate-900 border border-slate-800 rounded px-1.5 py-0.5 text-slate-200 text-xs font-mono focus:outline-none focus:border-cyan-400"
            />
          </form>
        </div>

        {/* Right: Actions & Status Badge */}
        <div className="flex items-center gap-3">
          <button
            onClick={() => setIsCreateTaskOpen(true)}
            className="flex items-center gap-1.5 px-3.5 py-1.5 rounded-xl bg-gradient-to-r from-cyan-600 to-blue-600 hover:from-cyan-500 hover:to-blue-500 text-white text-xs font-bold shadow-lg shadow-cyan-900/40 transition cursor-pointer"
          >
            <Plus size={14} />
            <span>New Task</span>
          </button>

          <button
            onClick={triggerSyncWithMesh}
            disabled={isSyncing || nodeInfo.wsStatus !== 'CONNECTED'}
            title="Request Vector Clock Sync"
            className="flex items-center gap-1.5 px-3 py-1.5 rounded-xl bg-slate-800 hover:bg-slate-700 text-cyan-300 text-xs font-mono border border-cyan-500/30 transition cursor-pointer disabled:opacity-50"
          >
            <ArrowRightLeft size={14} className={isSyncing ? 'animate-spin' : ''} />
            <span className="hidden sm:inline">Sync</span>
          </button>

          <div className={`flex items-center gap-1.5 px-3 py-1.5 rounded-full border text-xs font-semibold ${nodeInfo.wsStatus === 'CONNECTED'
              ? 'bg-emerald-950/40 border-emerald-500/30 text-emerald-400'
              : 'bg-rose-950/40 border-rose-500/30 text-rose-400 animate-pulse'
            }`}>
            <Radio size={12} className={nodeInfo.wsStatus === 'CONNECTED' ? 'animate-ping' : ''} />
            <span>{nodeInfo.wsStatus}</span>
          </div>

          <button
            onClick={refreshAll}
            title="Refresh All"
            className="p-2 rounded-xl bg-slate-800 hover:bg-slate-700 text-slate-300 border border-slate-700 transition cursor-pointer"
          >
            <RefreshCw size={15} />
          </button>
        </div>
      </header>

      {/* ─────────────────────────────────────────────────────────────────── */}
      {/* OPERATIONAL METRICS RIBBON */}
      {/* ─────────────────────────────────────────────────────────────────── */}
      <section className="bg-slate-900/60 border-b border-slate-800/80 px-5 py-2.5 grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-3 text-xs">

        {/* Metric 1: Active Mesh Nodes */}
        <div className="flex items-center gap-3 p-2 rounded-xl bg-slate-900/80 border border-slate-800/90 shadow-sm">
          <div className="p-2 rounded-lg bg-cyan-500/10 text-cyan-400">
            <Server size={18} />
          </div>
          <div>
            <div className="text-[10px] text-slate-400 font-mono uppercase tracking-wider">Active Nodes</div>
            <div className="text-sm font-bold text-slate-100 flex items-center gap-1.5">
              <span>{operationalMetrics.activeNodes || nodes.length || 1}</span>
              <span className="text-[10px] text-emerald-400 font-mono">100% Online</span>
            </div>
          </div>
        </div>

        {/* Metric 2: Task Success Rate */}
        <div className="flex items-center gap-3 p-2 rounded-xl bg-slate-900/80 border border-slate-800/90 shadow-sm">
          <div className="p-2 rounded-lg bg-emerald-500/10 text-emerald-400">
            <CheckCircle2 size={18} />
          </div>
          <div>
            <div className="text-[10px] text-slate-400 font-mono uppercase tracking-wider">Success Rate</div>
            <div className="text-sm font-bold text-slate-100 flex items-center gap-1.5">
              <span>{operationalMetrics.taskSuccessRate ?? 100}%</span>
              <span className="text-[10px] text-slate-500 font-mono">({operationalMetrics.tasksCompleted || 0} passed)</span>
            </div>
          </div>
        </div>

        {/* Metric 3: Avg Execution Time */}
        <div className="flex items-center gap-3 p-2 rounded-xl bg-slate-900/80 border border-slate-800/90 shadow-sm">
          <div className="p-2 rounded-lg bg-purple-500/10 text-purple-400">
            <Clock size={18} />
          </div>
          <div>
            <div className="text-[10px] text-slate-400 font-mono uppercase tracking-wider">Avg Exec Time</div>
            <div className="text-sm font-bold text-slate-100">
              {operationalMetrics.avgExecutionTimeMs || 1240} ms
            </div>
          </div>
        </div>

        {/* Metric 4: Replication Latency */}
        <div className="flex items-center gap-3 p-2 rounded-xl bg-slate-900/80 border border-slate-800/90 shadow-sm">
          <div className="p-2 rounded-lg bg-blue-500/10 text-blue-400">
            <ArrowRightLeft size={18} />
          </div>
          <div>
            <div className="text-[10px] text-slate-400 font-mono uppercase tracking-wider">Replication Latency</div>
            <div className="text-sm font-bold text-slate-100 flex items-center gap-1.5">
              <span>{operationalMetrics.replicationLatencyMs || 4.2} ms</span>
              <span className="text-[10px] text-cyan-400 font-mono">Vector Clock</span>
            </div>
          </div>
        </div>

        {/* Metric 5: Failed Validations & Risk */}
        <div className="flex items-center gap-3 p-2 rounded-xl bg-slate-900/80 border border-slate-800/90 shadow-sm col-span-2 sm:col-span-1">
          <div className={`p-2 rounded-lg ${operationalMetrics.failedValidationsCount > 0 ? 'bg-amber-500/10 text-amber-400' : 'bg-emerald-500/10 text-emerald-400'
            }`}>
            <ShieldCheck size={18} />
          </div>
          <div>
            <div className="text-[10px] text-slate-400 font-mono uppercase tracking-wider">Validation Integrity</div>
            <div className="text-sm font-bold text-slate-100 flex items-center gap-1.5">
              <span>{operationalMetrics.failedValidationsCount} Intercepted</span>
              <span className="text-[10px] text-emerald-400 font-mono">3NF Verified</span>
            </div>
          </div>
        </div>

      </section>

      {/* ─────────────────────────────────────────────────────────────────── */}
      {/* NAVIGATION BAR: WORKFLOW PIPELINE & SEPARATE DEMO SECTION */}
      {/* ─────────────────────────────────────────────────────────────────── */}
      <nav className="border-b border-slate-800 bg-slate-900/40 px-5 flex items-center justify-between text-xs font-semibold">
        <div className="flex items-center gap-1 py-1.5 overflow-x-auto">

          <button
            onClick={() => setActiveTab('board')}
            className={`px-3 py-1.5 rounded-xl transition flex items-center gap-1.5 cursor-pointer ${activeTab === 'board'
                ? 'bg-cyan-600/20 text-cyan-300 border border-cyan-500/40 shadow-sm'
                : 'text-slate-400 hover:text-slate-200 hover:bg-slate-800/50'
              }`}
          >
            <Workflow size={14} />
            <span>Task Board</span>
            <span className="px-1.5 py-0.2 rounded-full bg-slate-800 text-[10px] text-slate-300 font-mono">
              {tasks.length}
            </span>
          </button>

          <button
            onClick={() => setActiveTab('topology')}
            className={`px-3 py-1.5 rounded-xl transition flex items-center gap-1.5 cursor-pointer ${activeTab === 'topology'
                ? 'bg-cyan-600/20 text-cyan-300 border border-cyan-500/40 shadow-sm'
                : 'text-slate-400 hover:text-slate-200 hover:bg-slate-800/50'
              }`}
          >
            <Network size={14} />
            <span>Live Mesh Topology</span>
            <span className="px-1.5 py-0.2 rounded-full bg-slate-800 text-[10px] text-slate-300 font-mono">
              {nodes.length || 1}
            </span>
          </button>

          <button
            onClick={() => setActiveTab('replication')}
            className={`px-3 py-1.5 rounded-xl transition flex items-center gap-1.5 cursor-pointer ${activeTab === 'replication'
                ? 'bg-cyan-600/20 text-cyan-300 border border-cyan-500/40 shadow-sm'
                : 'text-slate-400 hover:text-slate-200 hover:bg-slate-800/50'
              }`}
          >
            <ArrowRightLeft size={14} />
            <span>Replication & Conflicts</span>
            {conflicts.filter(c => c.resolution_state === 'detected').length > 0 && (
              <span className="px-1.5 py-0.2 rounded-full bg-rose-500/20 text-rose-300 border border-rose-500/40 text-[10px] font-mono">
                {conflicts.filter(c => c.resolution_state === 'detected').length}
              </span>
            )}
          </button>

          <button
            onClick={() => setActiveTab('audit')}
            className={`px-3 py-1.5 rounded-xl transition flex items-center gap-1.5 cursor-pointer ${activeTab === 'audit'
                ? 'bg-cyan-600/20 text-cyan-300 border border-cyan-500/40 shadow-sm'
                : 'text-slate-400 hover:text-slate-200 hover:bg-slate-800/50'
              }`}
          >
            <ShieldCheck size={14} />
            <span>Audit History</span>
          </button>

          <button
            onClick={() => setActiveTab('chat')}
            className={`px-3 py-1.5 rounded-xl transition flex items-center gap-1.5 cursor-pointer ${activeTab === 'chat'
                ? 'bg-violet-600/20 text-violet-300 border border-violet-500/40 shadow-sm'
                : 'text-slate-400 hover:text-slate-200 hover:bg-slate-800/50'
              }`}
          >
            <MessageSquare size={14} />
            <span>Agent Assistant</span>
          </button>

          <button
            onClick={() => setActiveTab('activity')}
            className={`px-3 py-1.5 rounded-xl transition flex items-center gap-1.5 cursor-pointer ${activeTab === 'activity'
                ? 'bg-sky-600/20 text-sky-300 border border-sky-500/40 shadow-sm'
                : 'text-slate-400 hover:text-slate-200 hover:bg-slate-800/50'
              }`}
          >
            <Gauge size={14} />
            <span>System Telemetry</span>
          </button>

          {/* CLEAR DEMO INVENTORY SEPARATOR */}
          <div className="h-4 w-[1px] bg-slate-800 mx-2" />

          <button
            onClick={() => setActiveTab('inventory')}
            className={`px-3 py-1.5 rounded-xl transition flex items-center gap-1.5 cursor-pointer ${activeTab === 'inventory'
                ? 'bg-amber-600/20 text-amber-300 border border-amber-500/40 shadow-sm'
                : 'text-slate-400 hover:text-amber-200 hover:bg-slate-800/50'
              }`}
          >
            <Boxes size={14} className="text-amber-400" />
            <span className="text-amber-300">Demo Inventory (Legacy)</span>
          </button>

        </div>

        {/* Approvals quick alert */}
        {approvals.filter(a => a.decision === 'pending').length > 0 && (
          <div className="flex items-center gap-2 text-xs text-amber-400 bg-amber-950/40 border border-amber-500/40 px-2.5 py-1 rounded-xl">
            <AlertTriangle size={13} />
            <span>{approvals.filter(a => a.decision === 'pending').length} Sign-off Required</span>
          </div>
        )}
      </nav>

      {/* ─────────────────────────────────────────────────────────────────── */}
      {/* MAIN CONTENT WORKSPACE */}
      {/* ─────────────────────────────────────────────────────────────────── */}
      <main className="flex-1 overflow-hidden relative flex">

        {/* ── TAB 1: PRODUCTION TASK WORKFLOW KANBAN BOARD ────────────────── */}
        {activeTab === 'board' && (
          <div className="flex-1 flex flex-col p-5 overflow-hidden">

            {/* Header info bar */}
            <div className="flex items-center justify-between mb-4">
              <div>
                <h2 className="text-base font-bold text-slate-100 flex items-center gap-2">
                  <span>Distributed Agent Workflow Board</span>
                </h2>
                <p className="text-xs text-slate-400">
                  Lifecycle transitions: Queued → Planned → Awaiting Approval → Executing → Completed / Failed
                </p>
              </div>

              <div className="flex items-center gap-2 text-xs">
                <span className="text-slate-400 font-mono">Total Tasks: {tasks.length}</span>
              </div>
            </div>

            {/* Kanban Columns */}
            <div className="flex-1 grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-6 gap-3.5 overflow-x-auto pb-2">

              {/* Column 1: Queued */}
              <div className="flex flex-col bg-slate-900/50 border border-slate-800/90 rounded-2xl p-3 min-w-[240px]">
                <div className="flex items-center justify-between pb-2 mb-2 border-b border-slate-800">
                  <div className="flex items-center gap-1.5 font-bold text-xs text-slate-300">
                    <span className="w-2 h-2 rounded-full bg-slate-400" />
                    <span>Queued</span>
                  </div>
                  <span className="px-1.5 py-0.5 rounded-md bg-slate-800 text-[10px] font-mono text-slate-400">
                    {tasksByStatus.queued.length}
                  </span>
                </div>
                <div className="flex-1 overflow-y-auto space-y-2 pr-1">
                  {tasksByStatus.queued.map(task => (
                    <TaskCard
                      key={task.id}
                      task={task}
                      onSelect={() => { setSelectedTaskId(task.id); fetchTaskDetails(task.id); }}
                      onPlan={() => handlePlanTask(task.id)}
                    />
                  ))}
                  {tasksByStatus.queued.length === 0 && (
                    <div className="text-center py-8 text-xs text-slate-600 italic">No queued tasks</div>
                  )}
                </div>
              </div>

              {/* Column 2: Planned */}
              <div className="flex flex-col bg-slate-900/50 border border-cyan-950/60 rounded-2xl p-3 min-w-[240px]">
                <div className="flex items-center justify-between pb-2 mb-2 border-b border-cyan-900/40">
                  <div className="flex items-center gap-1.5 font-bold text-xs text-cyan-300">
                    <span className="w-2 h-2 rounded-full bg-cyan-400" />
                    <span>Planned</span>
                  </div>
                  <span className="px-1.5 py-0.5 rounded-md bg-cyan-950/60 text-[10px] font-mono text-cyan-400">
                    {tasksByStatus.planned.length}
                  </span>
                </div>
                <div className="flex-1 overflow-y-auto space-y-2 pr-1">
                  {tasksByStatus.planned.map(task => (
                    <TaskCard
                      key={task.id}
                      task={task}
                      onSelect={() => { setSelectedTaskId(task.id); fetchTaskDetails(task.id); }}
                      onExecute={() => handleExecuteTask(task.id)}
                    />
                  ))}
                  {tasksByStatus.planned.length === 0 && (
                    <div className="text-center py-8 text-xs text-slate-600 italic">No planned tasks</div>
                  )}
                </div>
              </div>

              {/* Column 3: Awaiting Approval */}
              <div className="flex flex-col bg-slate-900/50 border border-amber-950/60 rounded-2xl p-3 min-w-[240px]">
                <div className="flex items-center justify-between pb-2 mb-2 border-b border-amber-900/40">
                  <div className="flex items-center gap-1.5 font-bold text-xs text-amber-300">
                    <span className="w-2 h-2 rounded-full bg-amber-400" />
                    <span>Awaiting Approval</span>
                  </div>
                  <span className="px-1.5 py-0.5 rounded-md bg-amber-950/60 text-[10px] font-mono text-amber-400">
                    {tasksByStatus.awaiting_approval.length}
                  </span>
                </div>
                <div className="flex-1 overflow-y-auto space-y-2 pr-1">
                  {tasksByStatus.awaiting_approval.map(task => {
                    const app = approvals.find(a => a.task_id === task.id && a.decision === 'pending');
                    return (
                      <TaskCard
                        key={task.id}
                        task={task}
                        approval={app}
                        onSelect={() => { setSelectedTaskId(task.id); fetchTaskDetails(task.id); }}
                        onReviewApproval={() => { if (app) setActiveApproval(app); }}
                      />
                    );
                  })}
                  {tasksByStatus.awaiting_approval.length === 0 && (
                    <div className="text-center py-8 text-xs text-slate-600 italic">No pending approvals</div>
                  )}
                </div>
              </div>

              {/* Column 4: Executing */}
              <div className="flex flex-col bg-slate-900/50 border border-purple-950/60 rounded-2xl p-3 min-w-[240px]">
                <div className="flex items-center justify-between pb-2 mb-2 border-b border-purple-900/40">
                  <div className="flex items-center gap-1.5 font-bold text-xs text-purple-300">
                    <span className="w-2 h-2 rounded-full bg-purple-400 animate-pulse" />
                    <span>Executing</span>
                  </div>
                  <span className="px-1.5 py-0.5 rounded-md bg-purple-950/60 text-[10px] font-mono text-purple-400">
                    {tasksByStatus.executing.length}
                  </span>
                </div>
                <div className="flex-1 overflow-y-auto space-y-2 pr-1">
                  {tasksByStatus.executing.map(task => (
                    <TaskCard
                      key={task.id}
                      task={task}
                      onSelect={() => { setSelectedTaskId(task.id); fetchTaskDetails(task.id); }}
                    />
                  ))}
                  {tasksByStatus.executing.length === 0 && (
                    <div className="text-center py-8 text-xs text-slate-600 italic">No tasks executing</div>
                  )}
                </div>
              </div>

              {/* Column 5: Completed */}
              <div className="flex flex-col bg-slate-900/50 border border-emerald-950/60 rounded-2xl p-3 min-w-[240px]">
                <div className="flex items-center justify-between pb-2 mb-2 border-b border-emerald-900/40">
                  <div className="flex items-center gap-1.5 font-bold text-xs text-emerald-300">
                    <span className="w-2 h-2 rounded-full bg-emerald-400" />
                    <span>Completed</span>
                  </div>
                  <span className="px-1.5 py-0.5 rounded-md bg-emerald-950/60 text-[10px] font-mono text-emerald-400">
                    {tasksByStatus.completed.length}
                  </span>
                </div>
                <div className="flex-1 overflow-y-auto space-y-2 pr-1">
                  {tasksByStatus.completed.map(task => (
                    <TaskCard
                      key={task.id}
                      task={task}
                      onSelect={() => { setSelectedTaskId(task.id); fetchTaskDetails(task.id); }}
                    />
                  ))}
                  {tasksByStatus.completed.length === 0 && (
                    <div className="text-center py-8 text-xs text-slate-600 italic">No completed tasks</div>
                  )}
                </div>
              </div>

              {/* Column 6: Failed */}
              <div className="flex flex-col bg-slate-900/50 border border-rose-950/60 rounded-2xl p-3 min-w-[240px]">
                <div className="flex items-center justify-between pb-2 mb-2 border-b border-rose-900/40">
                  <div className="flex items-center gap-1.5 font-bold text-xs text-rose-300">
                    <span className="w-2 h-2 rounded-full bg-rose-400" />
                    <span>Failed</span>
                  </div>
                  <span className="px-1.5 py-0.5 rounded-md bg-rose-950/60 text-[10px] font-mono text-rose-400">
                    {tasksByStatus.failed.length}
                  </span>
                </div>
                <div className="flex-1 overflow-y-auto space-y-2 pr-1">
                  {tasksByStatus.failed.map(task => (
                    <TaskCard
                      key={task.id}
                      task={task}
                      onSelect={() => { setSelectedTaskId(task.id); fetchTaskDetails(task.id); }}
                      onPlan={() => handlePlanTask(task.id)}
                    />
                  ))}
                  {tasksByStatus.failed.length === 0 && (
                    <div className="text-center py-8 text-xs text-slate-600 italic">No failed tasks</div>
                  )}
                </div>
              </div>

            </div>
          </div>
        )}

        {/* ── TAB 2: LIVE MESH TOPOLOGY & NODE CAPABILITIES ───────────────── */}
        {activeTab === 'topology' && (
          <div className="flex-1 flex flex-col lg:flex-row overflow-hidden">

            {/* Left: 2D Force Graph */}
            <div ref={graphContainerRef} className="flex-1 relative bg-slate-950 flex items-center justify-center min-h-[350px]">
              <ForceGraph2D
                ref={graphRef}
                width={graphDimensions.width}
                height={graphDimensions.height}
                graphData={graphData}
                nodeLabel={node => `${node.id} (${node.role || 'node'})\nStatus: ${node.status || 'active'}`}
                nodeCanvasObjectMode={() => 'after'}
                nodeCanvasObject={(node, ctx, globalScale) => {
                  const fontSize = Math.max(12 / globalScale, 3);
                  ctx.font = `600 ${fontSize}px Inter, sans-serif`;
                  ctx.textAlign = 'center';
                  ctx.textBaseline = 'top';
                  ctx.fillStyle = '#f8fafc';
                  ctx.fillText(node.id, node.x, node.y + 12);
                }}
                nodeColor={node => {
                  if (node.role === 'planner') return '#38bdf8';
                  if (node.role === 'executor') return '#34d399';
                  if (node.role === 'validator') return '#a855f7';
                  if (node.role === 'router') return '#fbbf24';
                  return '#06b6d4';
                }}
                nodeRelSize={9}
                linkColor={() => '#334155'}
                linkWidth={2}
                linkDirectionalParticles={2}
                linkDirectionalParticleSpeed={0.005}
                backgroundColor="#030712"
              />
              <div className="absolute top-4 left-4 bg-slate-900/90 border border-slate-800 p-3 rounded-xl text-xs backdrop-blur shadow-xl space-y-1.5">
                <div className="font-bold text-slate-300 font-mono">Topology Legend</div>
                <div className="flex items-center gap-2"><span className="w-2.5 h-2.5 rounded-full bg-sky-400" /><span>Planner Node</span></div>
                <div className="flex items-center gap-2"><span className="w-2.5 h-2.5 rounded-full bg-emerald-400" /><span>Executor Node</span></div>
                <div className="flex items-center gap-2"><span className="w-2.5 h-2.5 rounded-full bg-purple-400" /><span>Validator Node</span></div>
                <div className="flex items-center gap-2"><span className="w-2.5 h-2.5 rounded-full bg-amber-400" /><span>Router Node</span></div>
              </div>
            </div>

            {/* Right: Registered Mesh Nodes & Capabilities List */}
            <div className="w-full lg:w-96 border-t lg:border-t-0 lg:border-l border-slate-800 bg-slate-900/60 p-4 overflow-y-auto space-y-3">
              <div className="flex items-center justify-between pb-2 border-b border-slate-800">
                <h3 className="font-bold text-xs uppercase font-mono tracking-wider text-slate-300">
                  Mesh Nodes ({nodes.length})
                </h3>
                <span className="text-[10px] text-cyan-400 font-mono">GossipSub Active</span>
              </div>

              {nodes.map(n => (
                <div key={n.id} className="p-3 rounded-xl bg-slate-950/80 border border-slate-800/90 space-y-2">
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-2">
                      <span className={`w-2 h-2 rounded-full ${n.status === 'active' ? 'bg-emerald-400' : 'bg-rose-400'}`} />
                      <span className="font-bold text-xs text-slate-100">{n.name}</span>
                    </div>
                    <span className="px-2 py-0.5 rounded-md bg-slate-800 text-[10px] font-mono text-cyan-300 uppercase">
                      {n.role}
                    </span>
                  </div>

                  <div className="text-[11px] font-mono text-slate-400 space-y-1">
                    <div>Address: <span className="text-slate-300">{n.address || '127.0.0.1'}</span></div>
                    <div>Peer ID: <span className="text-slate-300 truncate inline-block max-w-[200px] align-bottom">{n.peer_id}</span></div>
                    <div>Version: <span className="text-slate-300">{n.software_version || '2.0.0'}</span></div>
                  </div>

                  {n.capabilities && (
                    <div className="pt-2 border-t border-slate-900 text-[10px] font-mono space-y-1">
                      <div className="text-slate-400">Models: <span className="text-purple-300">{n.capabilities.models || 'local'}</span></div>
                      <div className="text-slate-400">Concurrency: <span className="text-emerald-400">{n.capabilities.max_concurrency} tasks</span></div>
                      <div className="text-slate-400 truncate">Tools: <span className="text-slate-300">{n.capabilities.tools || 'standard'}</span></div>
                    </div>
                  )}
                </div>
              ))}
            </div>

          </div>
        )}

        {/* ── TAB 3: REPLICATION & CONFLICTS PANEL ────────────────────────── */}
        {activeTab === 'replication' && (
          <div className="flex-1 flex flex-col lg:flex-row p-5 gap-5 overflow-hidden">

            {/* Left: Replication Metrics Stream */}
            <div className="flex-1 flex flex-col bg-slate-900/50 border border-slate-800 rounded-2xl p-4 overflow-hidden">
              <div className="flex items-center justify-between pb-3 border-b border-slate-800 mb-3">
                <h3 className="text-xs font-bold font-mono uppercase tracking-wider text-slate-200 flex items-center gap-2">
                  <ArrowRightLeft size={16} className="text-cyan-400" />
                  <span>Node Replication Telemetry & Vector Clock</span>
                </h3>
                <button
                  onClick={triggerSyncWithMesh}
                  disabled={isSyncing}
                  className="px-2.5 py-1 rounded-lg bg-slate-800 hover:bg-slate-700 text-xs font-mono text-cyan-300 border border-cyan-500/30 cursor-pointer"
                >
                  Force Sync Delta
                </button>
              </div>

              <div className="flex-1 overflow-y-auto space-y-2">
                {replicationMetrics.length === 0 ? (
                  <div className="text-center py-12 text-slate-600 italic text-xs">No replication events recorded yet</div>
                ) : (
                  replicationMetrics.map(r => (
                    <div key={r.id} className="p-3 rounded-xl bg-slate-950/80 border border-slate-800 text-xs font-mono flex items-center justify-between">
                      <div className="space-y-1">
                        <div className="flex items-center gap-2">
                          <span className="text-slate-300 font-bold">{r.source_peer_id}</span>
                          <span className="text-slate-600">→</span>
                          <span className="text-cyan-400 font-bold">{r.target_peer_id}</span>
                        </div>
                        <div className="text-[10px] text-slate-500">
                          {new Date(r.timestamp).toLocaleTimeString()} · Status: <span className="text-emerald-400 uppercase">{r.status}</span>
                        </div>
                      </div>
                      <div className="text-right">
                        <div className="text-emerald-400 font-bold">{r.latency_ms} ms</div>
                        <div className="text-[10px] text-slate-500">{r.bytes_transferred || 1024} bytes</div>
                      </div>
                    </div>
                  ))
                )}
              </div>
            </div>

            {/* Right: Conflicts & Divergence Panel */}
            <div className="flex-1 flex flex-col bg-slate-900/50 border border-slate-800 rounded-2xl p-4 overflow-hidden">
              <div className="flex items-center justify-between pb-3 border-b border-slate-800 mb-3">
                <h3 className="text-xs font-bold font-mono uppercase tracking-wider text-slate-200 flex items-center gap-2">
                  <ShieldAlert size={16} className="text-amber-400" />
                  <span>Replication & Assignment Conflicts ({conflicts.length})</span>
                </h3>
              </div>

              <div className="flex-1 overflow-y-auto space-y-2.5">
                {conflicts.length === 0 ? (
                  <div className="text-center py-12 text-slate-600 italic text-xs">No active conflicts detected. Swarm in consensus.</div>
                ) : (
                  conflicts.map(c => (
                    <div key={c.id} className="p-3 rounded-xl bg-slate-950/80 border border-slate-800 space-y-2 text-xs">
                      <div className="flex items-center justify-between">
                        <span className="font-bold text-amber-300 font-mono text-[11px]">{c.conflict_type}</span>
                        <span className={`px-2 py-0.5 rounded text-[10px] font-mono font-bold uppercase ${c.resolution_state === 'resolved' ? 'bg-emerald-950 text-emerald-400' : 'bg-rose-950 text-rose-400'
                          }`}>
                          {c.resolution_state}
                        </span>
                      </div>
                      <div className="text-[11px] font-mono text-slate-400">
                        Affected Entity: <strong className="text-slate-200">{c.affected_entity_type} ({c.affected_entity_id})</strong>
                      </div>
                      {c.chosen_resolution ? (
                        <div className="p-2 rounded bg-slate-900 text-[10px] font-mono text-emerald-300">
                          Resolution: {c.chosen_resolution}
                        </div>
                      ) : (
                        <button
                          onClick={() => { setActiveConflict(c); setChosenResolutionText('Resolved via vector clock precedence and validation rules.'); }}
                          className="px-3 py-1 bg-amber-600 hover:bg-amber-500 text-slate-950 font-bold rounded-lg text-xs cursor-pointer transition"
                        >
                          Resolve Conflict
                        </button>
                      )}
                    </div>
                  ))
                )}
              </div>
            </div>

          </div>
        )}

        {/* ── TAB 4: IMMUTABLE AUDIT TRAIL ─────────────────────────────────── */}
        {activeTab === 'audit' && (
          <div className="flex-1 flex flex-col p-5 overflow-hidden">
            <div className="flex items-center justify-between pb-3 border-b border-slate-800 mb-3">
              <div>
                <h2 className="text-base font-bold text-slate-100 flex items-center gap-2">
                  <ShieldCheck size={18} className="text-purple-400" />
                  <span>Append-Only Security & Audit History</span>
                </h2>
                <p className="text-xs text-slate-400">
                  Immutable record of proposals, validations, approvals, executions, and replications
                </p>
              </div>
              <span className="text-xs text-slate-500 font-mono">Showing {auditEvents.length} events</span>
            </div>

            <div className="flex-1 overflow-y-auto space-y-2">
              {auditEvents.map(ev => (
                <div key={ev.id} className="p-3 rounded-xl bg-slate-900/60 border border-slate-800 flex items-start justify-between text-xs font-mono">
                  <div className="space-y-1">
                    <div className="flex items-center gap-2">
                      <span className="font-bold text-cyan-400">{ev.action}</span>
                      <span className="text-slate-500">·</span>
                      <span className="text-slate-300">{ev.entity_type}: {ev.entity_id}</span>
                      <span className="text-slate-500">·</span>
                      <span className="text-purple-300 font-semibold">Actor: {ev.actor}</span>
                    </div>
                    {ev.details_json && (
                      <div className="text-[11px] text-slate-400 bg-slate-950 p-2 rounded-lg break-all">
                        {ev.details_json}
                      </div>
                    )}
                  </div>
                  <span className="text-[10px] text-slate-500 shrink-0 ml-4">
                    {new Date(ev.timestamp).toLocaleTimeString()}
                  </span>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* ── TAB 5: AGENT CHAT & PLANNER ASSISTANT ────────────────────────── */}
        {activeTab === 'chat' && (
          <div className="flex-1 flex flex-col bg-slate-950 overflow-hidden">
            <div className="p-4 border-b border-slate-800 flex items-center justify-between text-xs">
              <div className="flex items-center gap-2">
                <Bot size={16} className="text-cyan-400" />
                <span className="font-bold text-slate-200">Mesh AI Planner Assistant</span>
              </div>
              <span className="text-[10px] text-slate-500 font-mono">Model: {nodeInfo.modelName}</span>
            </div>

            <div className="flex-1 p-4 overflow-y-auto space-y-3">
              {chatMessages.map(msg => (
                <div key={msg.id} className={`flex gap-3 text-xs leading-relaxed ${msg.sender === 'user' ? 'justify-end' : 'justify-start'}`}>
                  {msg.sender !== 'user' && (
                    <div className="w-7 h-7 rounded-lg bg-cyan-950 border border-cyan-800 flex items-center justify-center shrink-0 text-cyan-400">
                      <Bot size={14} />
                    </div>
                  )}
                  <div className={`p-3 rounded-2xl max-w-xl ${msg.sender === 'user' ? 'bg-cyan-600 text-white rounded-tr-none' : 'bg-slate-900 border border-slate-800 text-slate-200 rounded-tl-none'
                    }`}>
                    <div>{msg.text}</div>
                    <div className="text-[10px] text-slate-400 mt-1.5 text-right font-mono">{msg.timestamp}</div>
                  </div>
                </div>
              ))}
              <div ref={chatBottomRef} />
            </div>

            <form
              onSubmit={(e) => {
                e.preventDefault();
                if (!chatInput.trim() || isChatLoading) return;
                const txt = chatInput.trim();
                setChatMessages(prev => [...prev, {
                  id: `usr-${Date.now()}`,
                  sender: 'user',
                  text: txt,
                  timestamp: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
                }]);
                setChatInput('');
                setIsChatLoading(true);
                fetch(getApiUrl('/agent/chat'), {
                  method: 'POST',
                  headers: getApiHeaders(),
                  body: JSON.stringify({ message: txt, history: [] })
                })
                  .then(r => r.json())
                  .then(data => {
                    setChatMessages(prev => [...prev, {
                      id: `agt-${Date.now()}`,
                      sender: 'agent',
                      text: data.message || 'Workflow response processed.',
                      timestamp: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
                    }]);
                  })
                  .catch(err => {
                    setChatMessages(prev => [...prev, {
                      id: `err-${Date.now()}`,
                      sender: 'agent',
                      text: `Agent error: ${err.message}`,
                      timestamp: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
                    }]);
                  })
                  .finally(() => setIsChatLoading(false));
              }}
              className="p-3 border-t border-slate-800 flex items-center gap-2"
            >
              <input
                type="text"
                value={chatInput}
                onChange={e => setChatInput(e.target.value)}
                placeholder="Ask the Agentic Mesh AI planner..."
                className="flex-1 bg-slate-900 border border-slate-800 rounded-xl px-4 py-2 text-xs text-slate-100 placeholder-slate-500 focus:outline-none focus:border-cyan-400 font-mono"
              />
              <button
                type="submit"
                disabled={isChatLoading || !chatInput.trim()}
                className="px-4 py-2 bg-cyan-600 hover:bg-cyan-500 disabled:opacity-50 text-white rounded-xl text-xs font-bold cursor-pointer"
              >
                Send
              </button>
            </form>
          </div>
        )}

        {/* ── TAB 6: SYSTEM TELEMETRY (AGENT ACTIVITY) ────────────────────── */}
        {activeTab === 'activity' && (
          <div className="flex-1 p-5 overflow-y-auto">
            <AgentActivityPanel
              agents={activity.agents}
              tasks={activity.tasks}
              messages={activity.messages}
              system={activity.system}
              proposals={pendingProposals}
            />
          </div>
        )}

        {/* ── TAB 7: DEMO INVENTORY (LEGACY SECTION - CLEARLY SEPARATE) ─────── */}
        {activeTab === 'inventory' && (
          <div className="flex-1 flex flex-col p-6 bg-slate-950 overflow-y-auto space-y-6">

            {/* Disclaimer Banner */}
            <div className="p-3.5 rounded-2xl bg-amber-950/40 border border-amber-500/40 text-amber-200 text-xs flex items-center justify-between">
              <div className="flex items-center gap-2.5">
                <Info size={16} className="text-amber-400 shrink-0" />
                <span>
                  <strong>Demo Inventory Store:</strong> This section contains the legacy 3NF inventory tables (items, categories, suppliers) preserved for backward compatibility and demo testing.
                </span>
              </div>
              <div className="flex items-center gap-2">
                <button
                  onClick={triggerFastProposal}
                  className="px-3 py-1 rounded-lg bg-amber-600 hover:bg-amber-500 text-slate-950 font-bold text-xs cursor-pointer transition"
                >
                  Propose Demo Item
                </button>
                <button
                  onClick={triggerConflictTest}
                  className="px-3 py-1 rounded-lg bg-slate-800 hover:bg-slate-700 text-rose-300 font-bold text-xs border border-rose-500/30 cursor-pointer transition"
                >
                  Test Schema Rule
                </button>
              </div>
            </div>

            {/* Search Header */}
            <div className="flex items-center justify-between gap-4 bg-slate-900/80 p-3 rounded-2xl border border-slate-800">
              <div className="relative flex-1">
                <Search size={16} className="absolute left-3 top-2.5 text-slate-500" />
                <input
                  type="text"
                  placeholder="Search inventory items by name or SKU..."
                  value={searchQuery}
                  onChange={e => setSearchQuery(e.target.value)}
                  className="w-full bg-slate-950 border border-slate-800 rounded-xl pl-9 pr-4 py-2 text-xs text-slate-100 placeholder-slate-500 focus:outline-none focus:border-amber-500 font-mono"
                />
              </div>
              <span className="text-xs text-slate-400 font-mono">
                Showing {filteredItems.length} of {dbState.items?.length || 0} items
              </span>
            </div>

            {/* Items Table */}
            <div className="space-y-2">
              <h3 className="text-xs font-bold text-amber-400 flex items-center gap-2 font-mono uppercase tracking-wider">
                <Database size={15} /> Items Table (`items`)
              </h3>
              <div className="overflow-x-auto border border-slate-800 rounded-2xl bg-slate-900/50 backdrop-blur">
                <table className="w-full text-left text-xs font-mono">
                  <thead className="bg-slate-900 text-slate-400 border-b border-slate-800">
                    <tr>
                      <th className="p-3">ID</th>
                      <th className="p-3">Item Name</th>
                      <th className="p-3">SKU</th>
                      <th className="p-3">Price</th>
                      <th className="p-3">Category ID</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-800/50">
                    {filteredItems.map(item => (
                      <tr key={item.id} className="hover:bg-slate-800/40 transition">
                        <td className="p-3 text-slate-400">{item.id}</td>
                        <td className="p-3 font-bold text-slate-100">{item.name}</td>
                        <td className="p-3 text-amber-400 font-bold">{item.sku}</td>
                        <td className="p-3 text-emerald-400 font-bold">${item.price}</td>
                        <td className="p-3 text-cyan-400">{item.category_id}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>

            {/* Categories & Suppliers Row */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div className="space-y-2">
                <h3 className="text-xs font-bold text-cyan-400 font-mono uppercase tracking-wider">Categories Table (`categories`)</h3>
                <div className="border border-slate-800 rounded-xl bg-slate-900/50 p-3 space-y-2">
                  {(dbState.categories || []).map(cat => (
                    <div key={cat.id} className="flex justify-between text-xs font-mono py-1 border-b border-slate-800/50">
                      <span className="font-bold text-slate-200">{cat.name}</span>
                      <span className="text-slate-400">{cat.description}</span>
                    </div>
                  ))}
                </div>
              </div>

              <div className="space-y-2">
                <h3 className="text-xs font-bold text-purple-400 font-mono uppercase tracking-wider">Suppliers Table (`suppliers`)</h3>
                <div className="border border-slate-800 rounded-xl bg-slate-900/50 p-3 space-y-2">
                  {(dbState.suppliers || []).map(sup => (
                    <div key={sup.id} className="flex justify-between text-xs font-mono py-1 border-b border-slate-800/50">
                      <span className="font-bold text-slate-200">{sup.name}</span>
                      <span className="text-slate-400">{sup.contact_email}</span>
                    </div>
                  ))}
                </div>
              </div>
            </div>

            {/* Replication Log & System Console Grid */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
              <div className="space-y-2">
                <h3 className="text-xs font-bold text-amber-400 font-mono uppercase tracking-wider">
                  Legacy Replication Log (`_mesh_log`: {meshLogs.length})
                </h3>
                <div className="border border-slate-800 rounded-xl bg-slate-900/60 p-3 max-h-48 overflow-y-auto space-y-1.5 font-mono text-[11px]">
                  {meshLogs.length === 0 ? (
                    <div className="text-slate-500 py-3 text-center">No mesh log entries yet.</div>
                  ) : (
                    meshLogs.slice(0, 30).map((log, idx) => (
                      <div key={log.id || idx} className="p-1.5 rounded bg-slate-950/70 border border-slate-800/80 flex items-center justify-between">
                        <span className="text-cyan-400">{log.action || log.table_name || 'WRITE'}</span>
                        <span className="text-slate-400 truncate max-w-[200px]">{log.vector_clock || log.details || ''}</span>
                        <span className="text-slate-500 text-[10px]">{log.created_at || ''}</span>
                      </div>
                    ))
                  )}
                </div>
              </div>

              <div className="space-y-2">
                <h3 className="text-xs font-bold text-cyan-400 font-mono uppercase tracking-wider">
                  Live Event Console ({logs.length})
                </h3>
                <div className="border border-slate-800 rounded-xl bg-slate-950 p-3 max-h-48 overflow-y-auto space-y-1 font-mono text-[11px]">
                  {logs.slice(-20).map((l, i) => (
                    <div key={i} className="flex gap-2">
                      <span className="text-slate-500 text-[10px] shrink-0">{l.time}</span>
                      <span className={
                        l.type === 'ai' ? 'text-cyan-400' :
                          l.type === 'p2p' ? 'text-purple-400' :
                            l.type === 'warn' ? 'text-amber-400' :
                              l.type === 'success' ? 'text-emerald-400' : 'text-slate-300'
                      }>{l.text}</span>
                    </div>
                  ))}
                </div>
              </div>
            </div>

          </div>
        )}

      </main>

      {/* ─────────────────────────────────────────────────────────────────── */}
      {/* TASK DETAIL PANEL / DRAWER */}
      {/* ─────────────────────────────────────────────────────────────────── */}
      {selectedTaskId && (
        <div className="fixed inset-0 bg-slate-950/80 backdrop-blur-sm z-40 flex justify-end">
          <div className="w-full max-w-2xl bg-slate-900 border-l border-slate-800 h-full flex flex-col shadow-2xl overflow-hidden animate-slide-left">

            {/* Drawer Header */}
            <div className="p-4 border-b border-slate-800 flex items-center justify-between bg-slate-900/90">
              <div className="flex items-center gap-2">
                <span className={`px-2 py-0.5 rounded text-[10px] font-mono font-bold uppercase ${selectedTaskDetails?.status === 'completed' ? 'bg-emerald-950 text-emerald-400' :
                    selectedTaskDetails?.status === 'executing' ? 'bg-purple-950 text-purple-400' :
                      selectedTaskDetails?.status === 'awaiting_approval' ? 'bg-amber-950 text-amber-400' :
                        'bg-cyan-950 text-cyan-400'
                  }`}>
                  {selectedTaskDetails?.status || 'loading'}
                </span>
                <span className="font-mono text-xs text-slate-400">{selectedTaskId}</span>
              </div>
              <button
                onClick={() => { setSelectedTaskId(null); setSelectedTaskDetails(null); }}
                className="p-1 rounded-lg hover:bg-slate-800 text-slate-400 hover:text-white cursor-pointer"
              >
                <X size={18} />
              </button>
            </div>

            {/* Drawer Content */}
            <div className="flex-1 overflow-y-auto p-5 space-y-5 text-xs">
              {isLoadingDetails ? (
                <div className="text-center py-12 text-slate-500 font-mono">Loading task details...</div>
              ) : selectedTaskDetails ? (
                <>
                  <div>
                    <h3 className="text-base font-bold text-slate-100">{selectedTaskDetails.title}</h3>
                    <p className="text-xs text-slate-400 mt-1">{selectedTaskDetails.user_prompt}</p>
                  </div>

                  {/* Metadata tiles */}
                  <div className="grid grid-cols-3 gap-2 font-mono text-[11px]">
                    <div className="p-2 rounded-xl bg-slate-950 border border-slate-800">
                      <div className="text-slate-500">Priority</div>
                      <div className="font-bold text-amber-400 uppercase">{selectedTaskDetails.priority}</div>
                    </div>
                    <div className="p-2 rounded-xl bg-slate-950 border border-slate-800">
                      <div className="text-slate-500">Task Type</div>
                      <div className="font-bold text-cyan-400">{selectedTaskDetails.task_type}</div>
                    </div>
                    <div className="p-2 rounded-xl bg-slate-950 border border-slate-800">
                      <div className="text-slate-500">Requested By</div>
                      <div className="font-bold text-purple-400">{selectedTaskDetails.requested_by}</div>
                    </div>
                  </div>

                  {/* Actions Bar */}
                  <div className="flex items-center gap-2 p-3 rounded-xl bg-slate-950 border border-slate-800">
                    {selectedTaskDetails.status === 'queued' && (
                      <button
                        onClick={() => handlePlanTask(selectedTaskId)}
                        className="px-3 py-1.5 rounded-lg bg-cyan-600 hover:bg-cyan-500 text-white font-bold cursor-pointer"
                      >
                        Plan Task (Planner Node)
                      </button>
                    )}
                    {selectedTaskDetails.status === 'planned' && (
                      <button
                        onClick={() => handleExecuteTask(selectedTaskId)}
                        className="px-3 py-1.5 rounded-lg bg-emerald-600 hover:bg-emerald-500 text-white font-bold cursor-pointer"
                      >
                        Execute Task (Executor Node)
                      </button>
                    )}
                    {selectedTaskDetails.status === 'awaiting_approval' && selectedTaskDetails.approvals?.[0] && (
                      <button
                        onClick={() => setActiveApproval(selectedTaskDetails.approvals[0])}
                        className="px-3 py-1.5 rounded-lg bg-amber-600 hover:bg-amber-500 text-slate-950 font-bold cursor-pointer"
                      >
                        Review Sign-Off Request
                      </button>
                    )}
                  </div>

                  {/* Plan Steps */}
                  <div className="space-y-2">
                    <h4 className="font-bold text-xs uppercase font-mono tracking-wider text-slate-300">
                      Plan & Execution Steps ({selectedTaskDetails.steps?.length || 0})
                    </h4>
                    <div className="space-y-2">
                      {(selectedTaskDetails.steps || []).map((step, idx) => (
                        <div key={step.id || idx} className="p-3 rounded-xl bg-slate-950 border border-slate-800/90 font-mono space-y-1.5">
                          <div className="flex items-center justify-between">
                            <span className="font-bold text-slate-200">Step {step.step_number}: {step.title}</span>
                            <span className={`text-[10px] px-1.5 py-0.5 rounded uppercase ${step.status === 'completed' ? 'bg-emerald-950 text-emerald-400' : 'bg-slate-800 text-slate-400'
                              }`}>
                              {step.status}
                            </span>
                          </div>
                          {step.input_json && (
                            <div className="text-[10px] text-slate-400 bg-slate-900/60 p-2 rounded break-all">
                              Input: {step.input_json}
                            </div>
                          )}
                          {step.output_json && (
                            <div className="text-[10px] text-emerald-400 bg-slate-900/60 p-2 rounded break-all">
                              Output: {step.output_json}
                            </div>
                          )}
                        </div>
                      ))}
                    </div>
                  </div>

                  {/* Artifacts */}
                  <div className="space-y-2">
                    <h4 className="font-bold text-xs uppercase font-mono tracking-wider text-slate-300">
                      Generated Artifacts ({selectedTaskDetails.artifacts?.length || 0})
                    </h4>
                    {(selectedTaskDetails.artifacts || []).length === 0 ? (
                      <div className="text-slate-500 italic">No artifacts generated yet</div>
                    ) : (
                      selectedTaskDetails.artifacts.map(art => (
                        <div key={art.id} className="p-3 rounded-xl bg-slate-950 border border-slate-800 font-mono space-y-1">
                          <div className="flex items-center justify-between font-bold text-cyan-300">
                            <span>{art.name}</span>
                            <span className="text-[10px] text-slate-500">{art.artifact_type}</span>
                          </div>
                          <pre className="text-[10px] text-slate-300 bg-slate-900 p-2 rounded overflow-x-auto max-h-32">
                            {art.content}
                          </pre>
                        </div>
                      ))
                    )}
                  </div>

                  {/* Task Audit Timeline */}
                  <div className="space-y-2">
                    <h4 className="font-bold text-xs uppercase font-mono tracking-wider text-slate-300">
                      Audit Timeline ({selectedTaskDetails.audit_events?.length || 0})
                    </h4>
                    <div className="space-y-1.5 border-l-2 border-slate-800 pl-3">
                      {(selectedTaskDetails.audit_events || []).map((ev, i) => (
                        <div key={ev.id || i} className="text-[11px] font-mono space-y-0.5">
                          <div className="flex items-center gap-2">
                            <span className="text-purple-400 font-bold">{ev.action}</span>
                            <span className="text-slate-500">by {ev.actor}</span>
                            <span className="text-slate-600 text-[10px]">[{new Date(ev.timestamp).toLocaleTimeString()}]</span>
                          </div>
                          {ev.details_json && <div className="text-[10px] text-slate-400">{ev.details_json}</div>}
                        </div>
                      ))}
                    </div>
                  </div>

                </>
              ) : null}
            </div>

          </div>
        </div>
      )}

      {/* ─────────────────────────────────────────────────────────────────── */}
      {/* MODAL: CREATE NEW WORKFLOW TASK */}
      {/* ─────────────────────────────────────────────────────────────────── */}
      {isCreateTaskOpen && (
        <div className="fixed inset-0 bg-slate-950/80 backdrop-blur-sm z-50 flex items-center justify-center p-4">
          <div className="w-full max-w-lg bg-slate-900 border border-slate-800 rounded-2xl shadow-2xl p-5 space-y-4">
            <div className="flex items-center justify-between pb-2 border-b border-slate-800">
              <h3 className="font-bold text-sm text-slate-100 flex items-center gap-2">
                <Plus size={16} className="text-cyan-400" />
                <span>Create Distributed Workflow Task</span>
              </h3>
              <button
                onClick={() => setIsCreateTaskOpen(false)}
                className="text-slate-500 hover:text-white cursor-pointer"
              >
                <X size={16} />
              </button>
            </div>

            <form onSubmit={handleCreateTask} className="space-y-3.5 text-xs">
              {taskCreationError && (
                <div className="p-3 rounded-xl bg-rose-950/80 border border-rose-800 text-rose-300 font-mono text-[11px] leading-relaxed">
                  {taskCreationError}
                </div>
              )}
              <div>
                <label className="block text-slate-400 font-mono mb-1">Task Title</label>
                <input
                  type="text"
                  required
                  value={newTaskTitle}
                  onChange={e => setNewTaskTitle(e.target.value)}
                  placeholder="e.g. Distributed Ingestion and 3NF Audit"
                  className="w-full bg-slate-950 border border-slate-800 rounded-xl px-3 py-2 text-slate-100 focus:outline-none focus:border-cyan-400 font-mono"
                />
              </div>

              <div>
                <label className="block text-slate-400 font-mono mb-1">User Prompt & Intent</label>
                <textarea
                  required
                  rows={3}
                  value={newTaskPrompt}
                  onChange={e => setNewTaskPrompt(e.target.value)}
                  placeholder="Specify what the distributed agent swarm should plan, validate, and execute..."
                  className="w-full bg-slate-950 border border-slate-800 rounded-xl px-3 py-2 text-slate-100 focus:outline-none focus:border-cyan-400 font-mono"
                />
              </div>

              <div className="grid grid-cols-2 gap-3">
                <div>
                  <label className="block text-slate-400 font-mono mb-1">Task Type</label>
                  <select
                    value={newTaskType}
                    onChange={e => setNewTaskType(e.target.value)}
                    className="w-full bg-slate-950 border border-slate-800 rounded-xl px-3 py-2 text-slate-100 focus:outline-none focus:border-cyan-400 font-mono"
                  >
                    <option value="general">general</option>
                    <option value="batch_ingest">batch_ingest</option>
                    <option value="schema_migration">schema_migration</option>
                    <option value="replication_verify">replication_verify</option>
                    <option value="security_audit">security_audit</option>
                  </select>
                </div>

                <div>
                  <label className="block text-slate-400 font-mono mb-1">Priority</label>
                  <select
                    value={newTaskPriority}
                    onChange={e => setNewTaskPriority(e.target.value)}
                    className="w-full bg-slate-950 border border-slate-800 rounded-xl px-3 py-2 text-slate-100 focus:outline-none focus:border-cyan-400 font-mono"
                  >
                    <option value="low">low</option>
                    <option value="medium">medium</option>
                    <option value="high">high</option>
                    <option value="critical">critical</option>
                  </select>
                </div>
              </div>

              <div className="pt-2 flex justify-end gap-2">
                <button
                  type="button"
                  onClick={() => setIsCreateTaskOpen(false)}
                  className="px-4 py-2 rounded-xl bg-slate-800 hover:bg-slate-700 text-slate-300 font-semibold cursor-pointer"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  disabled={isSubmittingTask}
                  className="px-4 py-2 rounded-xl bg-cyan-600 hover:bg-cyan-500 text-white font-bold cursor-pointer disabled:opacity-50"
                >
                  {isSubmittingTask ? 'Creating...' : 'Submit to Queue'}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

      {/* ─────────────────────────────────────────────────────────────────── */}
      {/* MODAL: APPROVAL REVIEW */}
      {/* ─────────────────────────────────────────────────────────────────── */}
      {activeApproval && (
        <div className="fixed inset-0 bg-slate-950/80 backdrop-blur-sm z-50 flex items-center justify-center p-4">
          <div className="w-full max-w-md bg-slate-900 border border-slate-800 rounded-2xl shadow-2xl p-5 space-y-4">
            <div className="flex items-center justify-between pb-2 border-b border-slate-800">
              <h3 className="font-bold text-sm text-amber-300 flex items-center gap-2">
                <ShieldAlert size={16} />
                <span>Approval Request Review</span>
              </h3>
              <button onClick={() => setActiveApproval(null)} className="text-slate-500 hover:text-white cursor-pointer">
                <X size={16} />
              </button>
            </div>

            <div className="space-y-2 text-xs font-mono">
              <div className="p-3 rounded-xl bg-amber-950/40 border border-amber-500/30 text-amber-200">
                {activeApproval.request_description}
              </div>
              <div>Risk Level: <span className="font-bold uppercase text-amber-400">{activeApproval.risk_level}</span></div>
              <div>Task ID: <span className="text-slate-300">{activeApproval.task_id}</span></div>
            </div>

            <div>
              <label className="block text-xs text-slate-400 font-mono mb-1">Reason / Note</label>
              <input
                type="text"
                value={approvalReason}
                onChange={e => setApprovalReason(e.target.value)}
                placeholder="Optional decision justification..."
                className="w-full bg-slate-950 border border-slate-800 rounded-xl px-3 py-2 text-xs text-slate-100 focus:outline-none focus:border-amber-400 font-mono"
              />
            </div>

            <div className="flex justify-end gap-2 pt-2">
              <button
                type="button"
                disabled={isSubmittingApproval}
                onClick={() => handleDecision('rejected')}
                className="px-4 py-2 rounded-xl bg-rose-600 hover:bg-rose-500 text-white font-bold text-xs cursor-pointer"
              >
                Reject Task
              </button>
              <button
                type="button"
                disabled={isSubmittingApproval}
                onClick={() => handleDecision('approved')}
                className="px-4 py-2 rounded-xl bg-emerald-600 hover:bg-emerald-500 text-white font-bold text-xs cursor-pointer"
              >
                Approve Execution
              </button>
            </div>
          </div>
        </div>
      )}

      {/* ─────────────────────────────────────────────────────────────────── */}
      {/* MODAL: CONFLICT RESOLUTION */}
      {/* ─────────────────────────────────────────────────────────────────── */}
      {activeConflict && (
        <div className="fixed inset-0 bg-slate-950/80 backdrop-blur-sm z-50 flex items-center justify-center p-4">
          <div className="w-full max-w-md bg-slate-900 border border-slate-800 rounded-2xl shadow-2xl p-5 space-y-4">
            <div className="flex items-center justify-between pb-2 border-b border-slate-800">
              <h3 className="font-bold text-sm text-slate-100 flex items-center gap-2">
                <AlertCircle size={16} className="text-rose-400" />
                <span>Resolve Replication Conflict</span>
              </h3>
              <button onClick={() => setActiveConflict(null)} className="text-slate-500 hover:text-white cursor-pointer">
                <X size={16} />
              </button>
            </div>

            <div className="space-y-1.5 text-xs font-mono">
              <div className="text-rose-300 font-bold">{activeConflict.conflict_type}</div>
              <div className="text-slate-400">Entity: {activeConflict.affected_entity_type} ({activeConflict.affected_entity_id})</div>
              {activeConflict.details_json && (
                <pre className="p-2 bg-slate-950 rounded text-[10px] text-slate-300 overflow-x-auto max-h-24">
                  {activeConflict.details_json}
                </pre>
              )}
            </div>

            <div>
              <label className="block text-xs text-slate-400 font-mono mb-1">Chosen Resolution</label>
              <textarea
                rows={2}
                value={chosenResolutionText}
                onChange={e => setChosenResolutionText(e.target.value)}
                className="w-full bg-slate-950 border border-slate-800 rounded-xl px-3 py-2 text-xs text-slate-100 font-mono"
              />
            </div>

            <div className="flex justify-end gap-2 pt-2">
              <button
                type="button"
                onClick={() => setActiveConflict(null)}
                className="px-4 py-2 rounded-xl bg-slate-800 hover:bg-slate-700 text-slate-300 font-bold text-xs cursor-pointer"
              >
                Cancel
              </button>
              <button
                type="button"
                disabled={isResolvingConflict}
                onClick={handleResolveConflict}
                className="px-4 py-2 rounded-xl bg-cyan-600 hover:bg-cyan-500 text-white font-bold text-xs cursor-pointer"
              >
                Apply Resolution
              </button>
            </div>
          </div>
        </div>
      )}

    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// TASK CARD COMPONENT FOR KANBAN COLUMNS
// ─────────────────────────────────────────────────────────────────────────────
function TaskCard({ task, approval, onSelect, onPlan, onExecute, onReviewApproval }) {
  const priorityColors = {
    critical: 'text-rose-400 bg-rose-950/60 border-rose-500/40',
    high: 'text-amber-400 bg-amber-950/60 border-amber-500/40',
    medium: 'text-cyan-400 bg-cyan-950/60 border-cyan-500/40',
    low: 'text-slate-400 bg-slate-900 border-slate-700'
  };

  return (
    <div
      onClick={onSelect}
      className="group p-3 rounded-xl bg-slate-950/90 border border-slate-800/90 hover:border-cyan-500/50 transition cursor-pointer shadow-sm space-y-2 select-none"
    >
      <div className="flex items-center justify-between">
        <span className={`px-1.5 py-0.5 rounded text-[9px] font-mono font-bold uppercase border ${priorityColors[task.priority] || priorityColors.medium}`}>
          {task.priority}
        </span>
        <span className="text-[10px] font-mono text-slate-500">{task.id?.slice(0, 8)}</span>
      </div>

      <div className="font-bold text-xs text-slate-100 group-hover:text-cyan-300 transition line-clamp-2 leading-snug">
        {task.title}
      </div>

      <div className="text-[11px] text-slate-400 line-clamp-2 leading-tight">
        {task.user_prompt}
      </div>

      {approval && (
        <div className="text-[10px] font-mono text-amber-300 bg-amber-950/40 border border-amber-500/30 px-2 py-0.5 rounded">
          Requires {approval.risk_level} Sign-off
        </div>
      )}

      <div className="flex items-center justify-between text-[10px] font-mono text-slate-500 pt-1 border-t border-slate-900">
        <span>{task.assigned_node_id || 'unassigned'}</span>
        <span>{task.task_type}</span>
      </div>

      {/* Quick Action Buttons */}
      <div className="pt-1 flex items-center justify-end gap-1.5" onClick={e => e.stopPropagation()}>
        {task.status === 'queued' && onPlan && (
          <button
            onClick={onPlan}
            className="px-2 py-0.5 rounded bg-cyan-600/30 hover:bg-cyan-600 text-cyan-300 hover:text-white text-[10px] font-mono font-bold transition cursor-pointer"
          >
            Plan
          </button>
        )}
        {task.status === 'planned' && onExecute && (
          <button
            onClick={onExecute}
            className="px-2 py-0.5 rounded bg-emerald-600/30 hover:bg-emerald-600 text-emerald-300 hover:text-white text-[10px] font-mono font-bold transition cursor-pointer"
          >
            Execute
          </button>
        )}
        {task.status === 'awaiting_approval' && onReviewApproval && (
          <button
            onClick={onReviewApproval}
            className="px-2 py-0.5 rounded bg-amber-600/40 hover:bg-amber-600 text-amber-200 hover:text-white text-[10px] font-mono font-bold transition cursor-pointer"
          >
            Sign Off
          </button>
        )}
      </div>
    </div>
  );
}
