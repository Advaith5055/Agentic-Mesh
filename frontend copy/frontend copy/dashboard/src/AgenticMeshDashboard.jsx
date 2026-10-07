import { useState, useEffect, useRef, useCallback } from 'react';
import ForceGraph2D from 'react-force-graph-2d';
import { 
  Activity, 
  Database, 
  Network, 
  Sparkles, 
  ShieldAlert, 
  ShieldCheck, 
  RefreshCw, 
  Send, 
  Cpu, 
  Terminal, 
  Radio, 
  CheckCircle2, 
  AlertCircle, 
  Server,
  Layers,
  Search,
  Copy,
  Clock,
  ArrowRightLeft,
  MessageSquare,
  Bot,
  User,
  Trash2,
  Gauge
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
    p2pPort: '9004'
  });
  const [peers, setPeers] = useState([]);
  const [dbState, setDbState] = useState({ categories: [], items: [], suppliers: [] });
  const [meshLogs, setMeshLogs] = useState([]);
  const [logs, setLogs] = useState([{ time: new Date().toLocaleTimeString(), text: 'System HUD Online. Connecting to mesh gateway...', type: 'system' }]);
  const [logFilter, setLogFilter] = useState('ALL');
  const [nlInput, setNlInput] = useState('');
  const [isSyncing, setIsSyncing] = useState(false);
  const [activeTab, setActiveTab] = useState('topology'); // 'topology' | 'database' | 'meshlog' | 'chat' | 'activity'
  const [activity, setActivity] = useState({ agents: [], tasks: [], messages: [], system: null });
  const [pendingProposals, setPendingProposals] = useState([]);
  const [searchQuery, setSearchQuery] = useState('');
  const [copiedPeerId, setCopiedPeerId] = useState(false);

  // Agent Chat state
  const [chatMode, setChatMode] = useState('fast'); // 'fast' | 'deep'
  const [loadedModels, setLoadedModels] = useState([]);
  const [chatMessages, setChatMessages] = useState([
    {
      id: 'init-1',
      sender: 'agent',
      text: 'Hello! I am your Coding Agent for Agentic Mesh node "delta". I can generate verified database plans, write integration code, inspect schema state, or explain distributed mesh internals. What can I code or plan for you today?',
      timestamp: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
      operations: null
    }
  ]);
  const [chatInput, setChatInput] = useState('');
  const [isChatLoading, setIsChatLoading] = useState(false);
  const [executingPlanId, setExecutingPlanId] = useState(null);
  const chatBottomRef = useRef(null);
  const activeAbortControllerRef = useRef(null);

  // Abort in-flight requests on component unmount
  useEffect(() => {
    return () => {
      if (activeAbortControllerRef.current) {
        activeAbortControllerRef.current.abort();
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

  const fetchDbState = useCallback(async () => {
    try {
      const res = await fetch(getApiUrl('/db/state'));
      if (res.ok) {
        const data = await res.json();
        setDbState(data);
      }
    } catch {
      // Ignore network errors
    }
  }, [getApiUrl]);

  const fetchPeers = useCallback(async () => {
    try {
      const res = await fetch(getApiUrl('/peers'));
      if (res.ok) {
        const data = await res.json();
        setPeers(data);
      }
    } catch {
      // Ignore network errors
    }
  }, [getApiUrl]);

  const fetchMeshLogs = useCallback(async () => {
    try {
      const res = await fetch(getApiUrl('/db/mesh-log'));
      if (res.ok) {
        const data = await res.json();
        setMeshLogs(data);
      }
    } catch {
      // Ignore network errors
    }
  }, [getApiUrl]);

  const fetchAgentStatus = useCallback(async () => {
    try {
      const res = await fetch(getApiUrl('/agent/status'));
      if (res.ok) {
        const data = await res.json();
        if (data.loadedModels) {
          setLoadedModels(data.loadedModels);
        }
        if (data.activeModel) {
          setNodeInfo(prev => ({ ...prev, modelName: data.activeModel }));
        }
      }
    } catch {
      // Ignore network errors
    }
  }, [getApiUrl]);

  const fetchActivity = useCallback(async () => {
    try {
      const res = await fetch(getApiUrl('/agents/activity'));
      if (res.ok) {
        setActivity(await res.json());
      }
    } catch {
      // Ignore network errors
    }
  }, [getApiUrl]);

  const fetchProposals = useCallback(async () => {
    try {
      const res = await fetch(getApiUrl('/proposals?status=pending'));
      if (res.ok) setPendingProposals(await res.json());
    } catch {
      // Ignore network errors
    }
  }, [getApiUrl]);

  const decideProposal = useCallback(async (id, decision) => {
    try {
      const res = await fetch(getApiUrl(`/proposals/${id}/${decision}`), { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}' });
      const data = await res.json();
      if (data.success) {
        addLog(`Proposal ${id} ${decision === 'approve' ? 'approved and executed' : 'rejected'}`, decision === 'approve' ? 'success' : 'system');
      } else {
        addLog(`Proposal ${id} not ${decision}d: ${data.errors?.join(' | ')}`, 'error');
      }
    } catch (e) {
      addLog(`Proposal ${decision} error: ${e.message}`, 'error');
    }
  }, [getApiUrl, addLog]);

  const refreshAll = useCallback(() => {
    fetchDbState();
    fetchPeers();
    fetchMeshLogs();
    fetchAgentStatus();
    fetchActivity();
    fetchProposals();
  }, [fetchDbState, fetchPeers, fetchMeshLogs, fetchAgentStatus, fetchActivity, fetchProposals]);

  // WebSocket Connection Lifecycle
  useEffect(() => {
    let socket = null;
    let reconnectTimer = null;

    const connect = () => {
      setNodeInfo(prev => ({ ...prev, wsStatus: 'CONNECTING' }));
      socket = new WebSocket(getWsUrl());

      socket.onopen = () => {
        setNodeInfo(prev => ({ ...prev, wsStatus: 'CONNECTED' }));
        addLog(`Connected to node gateway on port ${wsPort}`, 'success');
        refreshAll();
      };

      socket.onmessage = (event) => {
        try {
          const msg = JSON.parse(event.data);
          
          if (msg.type === 'init') {
            setNodeInfo({ 
              name: msg.nodeName || 'Unknown', 
              peerId: msg.peerId || 'N/A', 
              wsStatus: 'CONNECTED',
              modelName: msg.modelName || 'N/A',
              p2pPort: msg.p2pPort || '9001'
            });
            if (msg.peers) setPeers(msg.peers);
            if (msg.dbState) setDbState(msg.dbState);
            if (msg.meshLogs) setMeshLogs(msg.meshLogs);
            addLog(`Mesh Node Active: ${msg.nodeName} (${msg.peerId?.slice(0, 12)}...) [P2P :${msg.p2pPort || '9001'}]`, 'success');
          } else if (msg.type === 'log') {
            const { level, message, nodeName } = msg.data || {};
            let logType = 'system';
            if (level === 'P2P') logType = 'network';
            else if (level === 'DB') logType = 'success';
            else if (level === 'AI') logType = 'agent';
            else if (level === 'ERROR') logType = 'error';
            addLog(`[${nodeName || 'node'}] [${level || 'LOG'}] ${message}`, logType);
            if (level === 'DB') {
              fetchDbState();
              fetchMeshLogs();
            }
          } else if (msg.type === 'peer:updated') {
            const updated = msg.data || {};
            setPeers(prev => {
              const before = prev.find(p => p.peerId === updated.peerId);
              if (before && !before.name && updated.name) {
                addLog(`Peer identified: ${updated.name} (${updated.role || 'peer'}, ${updated.peerId?.slice(0, 12)}...)`, 'network');
              }
              return before ? prev.map(p => (p.peerId === updated.peerId ? { ...p, ...updated } : p)) : [...prev, updated];
            });
          } else if (msg.type === 'peer:joined' || msg.type === 'peer:left' || msg.type === 'peer:connected') {
            addLog(`Peer network event: ${msg.type} (${msg.data?.peerId?.slice(0, 12) || ''}...)`, 'network');
            fetchPeers();
          } else if (msg.type === 'tx:committed') {
            addLog(`Local transaction committed: ${msg.data?.operation} on ${msg.data?.tableName}`, 'success');
            fetchDbState();
            fetchMeshLogs();
          } else if (msg.type === 'tx:replicated') {
            addLog(`GossipSub transaction replicated from ${msg.data?.from?.slice(0, 8)}...: ${msg.data?.payload?.operation} on ${msg.data?.payload?.tableName}`, 'success');
            fetchDbState();
            fetchMeshLogs();
          } else if (msg.type === 'tx:conflict') {
            addLog(`Conflict detected on write: ${msg.data?.errors?.join(' | ') || 'Schema rule violation'}`, 'error');
          } else if (typeof msg.type === 'string' && msg.type.startsWith('proposal:')) {
            if (msg.type === 'proposal:created') addLog(`New plan waiting for approval: ${msg.data?.summary || msg.data?.id}`, 'agent');
            fetchProposals();
            fetchDbState();
          } else if (msg.type === 'agent:task') {
            setActivity(prev => ({
              ...prev,
              tasks: [msg.data, ...prev.tasks.filter(t => t.id !== msg.data.id)].slice(0, 60)
            }));
          } else if (msg.type === 'agent:message') {
            setActivity(prev => ({ ...prev, messages: [...prev.messages, msg.data].slice(-300) }));
          } else if (msg.type === 'agent:stats') {
            setActivity(prev => ({ ...prev, agents: msg.data.agents, system: msg.data.system }));
          } else if (msg.type === 'sync:completed') {
            const { applied = 0, duplicates = 0, conflicts = 0, total = 0 } = msg.data || {};
            addLog(`Sync catch-up completed: ${applied} applied, ${duplicates} skipped, ${conflicts} conflicts (total ${total} entries)`, 'network');
            fetchDbState();
            fetchMeshLogs();
          }
        } catch {
          // Ignore parse errors
        }
      };

      socket.onclose = () => {
        setNodeInfo(prev => ({ ...prev, wsStatus: 'DISCONNECTED' }));
        addLog(`WebSocket connection to port ${wsPort} lost. Retrying in 3s...`, 'error');
        reconnectTimer = setTimeout(connect, 3000);
      };

      socket.onerror = () => {
        socket.close();
      };
    };

    connect();

    return () => {
      if (socket) socket.close();
      if (reconnectTimer) clearTimeout(reconnectTimer);
    };
  }, [wsPort, getWsUrl, refreshAll, addLog, fetchDbState, fetchMeshLogs, fetchPeers, fetchProposals]);

  // Topology Graph Data
  const peerLabel = (p, idx) => p.name || (p.peerId ? `Node-${p.peerId.slice(0, 8)}` : `Peer-${idx + 1}`);
  const peerIp = (p) => (p.addrs || []).map(a => /^\/ip4\/([\d.]+)\//.exec(a)?.[1]).find(ip => ip && !ip.startsWith('127.')) || '—';
  const graphData = {
    nodes: [
      { id: nodeInfo.name || 'Local Node', label: `${nodeInfo.name || 'Local Node'} (this node)`, group: 1, val: 28, peerId: nodeInfo.peerId },
      ...peers.map((p, idx) => ({ 
        id: p.peerId ? `Node-${p.peerId.slice(0, 8)}` : `Peer-${idx + 1}`, 
        label: peerLabel(p, idx),
        group: 2, 
        val: 22,
        peerId: p.peerId
      }))
    ],
    links: peers.map((p, idx) => ({
      source: nodeInfo.name || 'Local Node',
      target: p.peerId ? `Node-${p.peerId.slice(0, 8)}` : `Peer-${idx + 1}`
    }))
  };

  // Keep nodes far enough apart that circles and labels don't overlap
  const peerCount = peers.length;
  useEffect(() => {
    const graph = graphRef.current;
    if (!graph || activeTab !== 'topology') return;
    graph.d3Force('link')?.distance(160);
    graph.d3Force('charge')?.strength(-500);
    graph.d3ReheatSimulation?.();
  }, [peerCount, activeTab]);

  // Fast-path direct transaction
  const triggerFastProposal = async () => {
    addLog('Proposing Fast-Path Transaction (<1ms JS schema validation)...', 'system');
    try {
      const sku = `SKU-W${Math.floor(1000 + Math.random() * 9000)}`;
      const res = await fetch(getApiUrl('/propose'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          table: 'items',
          operation: 'INSERT',
          data: { category_id: 1, name: `Quantum Sensor ${sku}`, price: 149.99, sku }
        })
      });
      const data = await res.json();
      if (data.success) {
        addLog(`Fast-path write committed & gossiped to mesh! SKU: ${sku}`, 'success');
        refreshAll();
      } else {
        addLog(`Fast-path validation error: ${data.errors?.join(' | ')}`, 'error');
      }
    } catch (e) {
      addLog(`Error connecting to backend: ${e.message}. Ensure node is running on port ${wsPort}.`, 'error');
    }
  };

  // Test malformed payload rejection
  const triggerConflictTest = async () => {
    addLog('Executing Malformed Payload (Price -50 & Invalid Category ID 999)...', 'system');
    try {
      const res = await fetch(getApiUrl('/propose'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          table: 'items',
          operation: 'INSERT',
          data: { category_id: 999, name: 'Malformed Item', price: -50, sku: 'MALFORMED' }
        })
      });
      const data = await res.json();
      if (!data.success) {
        addLog(`Schema Validator intercepted violation in <1ms: ${data.errors?.join(' | ')}`, 'error');
      } else {
        addLog('Unexpected success on malformed transaction', 'error');
      }
    } catch (e) {
      addLog(`Error connecting to backend: ${e.message}`, 'error');
    }
  };

  // Trigger manual sync request
  const triggerSyncWithMesh = async () => {
    setIsSyncing(true);
    addLog('Broadcasting SYNC_REQUEST with local vector clock...', 'network');
    try {
      const res = await fetch(getApiUrl('/sync'), { method: 'POST' });
      const data = await res.json();
      if (data.success) {
        addLog('Sync request broadcasted to mesh peers', 'success');
      } else {
        addLog(`Sync error: ${data.errors?.join(', ')}`, 'error');
      }
    } catch (e) {
      addLog(`Sync connection error: ${e.message}`, 'error');
    } finally {
      setIsSyncing(false);
    }
  };

  // Agent Chat message sender
  const sendChatMessage = useCallback(async (msgText) => {
    const textToSend = (msgText !== undefined ? msgText : chatInput).trim();
    if (!textToSend || isChatLoading) return;

    const reqId = Math.random().toString(36).slice(2, 9);
    const userMsgId = `msg-${Date.now()}`;
    const userMsg = {
      id: userMsgId,
      sender: 'user',
      text: textToSend,
      timestamp: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
      operations: null
    };

    setChatMessages(prev => [...prev, userMsg]);
    setChatInput('');
    setIsChatLoading(true);
    addLog(`[reqId:${reqId}] User to Agent: "${textToSend}"`, 'user');

    const controller = new AbortController();
    activeAbortControllerRef.current = controller;
    let timeoutTriggered = false;
    const timeoutId = setTimeout(() => {
      timeoutTriggered = true;
      controller.abort(new Error('TIMEOUT'));
    }, 90_000);

    try {
      const history = chatMessages.slice(-4).map(m => ({
        role: m.sender === 'user' ? 'user' : 'assistant',
        content: m.text
      }));

      const res = await fetch(getApiUrl('/agent/chat'), {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Accept': 'text/event-stream',
          'x-request-id': reqId
        },
        body: JSON.stringify({ message: textToSend, history, reqId, mode: chatMode }),
        signal: controller.signal
      });

      const contentType = res.headers.get('content-type') || '';

      if (contentType.includes('text/event-stream') && res.body) {
        const agentMsgId = `agent-${Date.now()}`;
        const initialAgentMsg = {
          id: agentMsgId,
          sender: 'agent',
          text: '',
          timestamp: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
          operations: null,
          status: 'pending',
          isStreaming: true,
          isGenerating: false
        };
        setChatMessages(prev => [...prev, initialAgentMsg]);

        // Show "Model is generating..." after 1 second if no tokens received yet
        const generatingTimer = setTimeout(() => {
          setChatMessages(prev => prev.map(m => (m.id === agentMsgId && !m.text) ? { ...m, isGenerating: true } : m));
        }, 1000);

        const reader = res.body.getReader();
        const decoder = new TextDecoder();
        let buffer = '';
        let accumulatedText = '';
        let receivedOperations = null;

        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          clearTimeout(generatingTimer);
          buffer += decoder.decode(value, { stream: true });
          const lines = buffer.split('\n');
          buffer = lines.pop();

          for (const line of lines) {
            const trimmed = line.trim();
            // Skip keepalive comments or empty lines
            if (!trimmed || trimmed.startsWith(':')) continue;
            if (!trimmed.startsWith('data:')) continue;
            const jsonStr = trimmed.replace(/^data:\s*/, '');
            if (!jsonStr) continue;

            try {
              const event = JSON.parse(jsonStr);
              if (event.type === 'token') {
                accumulatedText += event.content;
                setChatMessages(prev => prev.map(m => m.id === agentMsgId ? { ...m, text: accumulatedText, isGenerating: false } : m));
              } else if (event.type === 'error') {
                accumulatedText = event.message || 'Error occurred';
                setChatMessages(prev => prev.map(m => m.id === agentMsgId ? {
                  ...m,
                  text: accumulatedText,
                  isError: true,
                  isStreaming: false,
                  isGenerating: false
                } : m));
                addLog(`[reqId:${reqId}] Agent error: ${accumulatedText}`, 'error');
              } else if (event.type === 'done') {
                accumulatedText = event.message || accumulatedText;
                receivedOperations = event.operations || null;
                setChatMessages(prev => prev.map(m => m.id === agentMsgId ? {
                  ...m,
                  text: accumulatedText || 'Task processed successfully.',
                  operations: receivedOperations,
                  isStreaming: false,
                  isGenerating: false
                } : m));

                addLog(`[reqId:${reqId}] Agent: ${(accumulatedText || 'Response received').slice(0, 90)}${(accumulatedText || '').length > 90 ? '...' : ''}`, 'agent');
                if (receivedOperations && receivedOperations.length > 0) {
                  addLog(`[reqId:${reqId}] Agent proposed ${receivedOperations.length} verified database operation(s). Awaiting your approval.`, 'agent');
                }
              }
            } catch {
              // ignore parse errors on partial chunks
            }
          }
        }

        clearTimeout(generatingTimer);
        // Final safety check to stop streaming state and never leave empty message
        setChatMessages(prev => prev.map(m => m.id === agentMsgId ? {
          ...m,
          text: m.text || accumulatedText || 'The local model finished processing.',
          isStreaming: false,
          isGenerating: false
        } : m));
      } else {
        const data = await res.json();
        if (data.success) {
          const agentMsg = {
            id: `agent-${Date.now()}`,
            sender: 'agent',
            text: data.message || 'Task processed successfully.',
            timestamp: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
            operations: data.operations || null,
            status: 'pending'
          };
          setChatMessages(prev => [...prev, agentMsg]);
          addLog(`[reqId:${reqId}] Agent: ${(data.message || '').slice(0, 90)}${(data.message || '').length > 90 ? '...' : ''}`, 'agent');

          if (data.operations && data.operations.length > 0) {
            addLog(`[reqId:${reqId}] Agent proposed ${data.operations.length} verified database operation(s). Awaiting your approval in Chat.`, 'agent');
          }
        } else {
          const errorMsg = {
            id: `agent-err-${Date.now()}`,
            sender: 'agent',
            text: `Notice: ${data.errors?.join(' | ') || 'Failed to generate response'}`,
            timestamp: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
            operations: null,
            isError: true
          };
          setChatMessages(prev => [...prev, errorMsg]);
          addLog(`[reqId:${reqId}] Agent error: ${data.errors?.join(' | ')}`, 'error');
        }
      }
    } catch (err) {
      const isTimeout = timeoutTriggered || err.name === 'AbortError' || err.message?.includes('TIMEOUT') || controller.signal.aborted;
      const errorText = isTimeout
        ? 'The local model did not respond within 90 seconds. Check Ollama CPU/GPU usage and selected model.'
        : `Connection error: ${err.message}. Ensure node gateway is running on port ${wsPort}.`;
      const errorMsg = {
        id: `agent-err-${Date.now()}`,
        sender: 'agent',
        text: errorText,
        timestamp: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
        operations: null,
        isError: true
      };
      setChatMessages(prev => [...prev, errorMsg]);
      addLog(`[reqId:${reqId}] Agent error: ${errorText}`, 'error');
    } finally {
      clearTimeout(timeoutId);
      activeAbortControllerRef.current = null;
      setIsChatLoading(false);
    }
  }, [chatInput, isChatLoading, chatMessages, addLog, getApiUrl, wsPort, chatMode]);

  // Execute operations from approved plan card
  const executeApprovedPlan = async (messageId, operations) => {
    setExecutingPlanId(messageId);
    addLog(`Executing approved plan (${operations.length} operation(s))...`, 'system');

    try {
      const res = await fetch(getApiUrl('/ask/confirm'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ operations })
      });

      const data = await res.json();
      if (data.success) {
        addLog(`Plan successfully committed & gossiped! (${data.execResult?.completed || operations.length} ops applied)`, 'success');
        setChatMessages(prev => prev.map(m => m.id === messageId ? { ...m, status: 'executed' } : m));
        refreshAll();
      } else {
        addLog(`Plan execution failed: ${data.errors?.join(' | ')}`, 'error');
      }
    } catch (err) {
      addLog(`Execution error: ${err.message}`, 'error');
    } finally {
      setExecutingPlanId(null);
    }
  };

  // AI Prompt handler from left bar
  const handleAiPrompt = async (e) => {
    if (e) e.preventDefault();
    if (!nlInput.trim()) return;

    const userPrompt = nlInput.trim();
    setNlInput('');
    setActiveTab('chat');
    await sendChatMessage(userPrompt);
  };

  // Run AI 3NF Audit
  const triggerAiAudit = async () => {
    addLog('Running Background 3NF/BCNF AI Schema Audit...', 'system');
    try {
      const res = await fetch(getApiUrl('/audit'), { method: 'POST' });
      const data = await res.json();
      if (data.issues && data.issues.length === 0) {
        addLog('AI Audit Completed: 0 schema normalization or key violations detected.', 'success');
      } else {
        addLog(`AI Audit Findings: ${JSON.stringify(data.issues)}`, 'agent');
      }
    } catch (e) {
      addLog(`Audit connection error: ${e.message}`, 'error');
    }
  };

  const copyPeerId = () => {
    if (nodeInfo.peerId && nodeInfo.peerId !== 'N/A') {
      navigator.clipboard.writeText(nodeInfo.peerId);
      setCopiedPeerId(true);
      setTimeout(() => setCopiedPeerId(false), 2000);
    }
  };

  const filteredLogs = logs.filter(log => {
    if (logFilter === 'ALL') return true;
    if (logFilter === 'P2P') return log.type === 'network';
    if (logFilter === 'DB') return log.type === 'success';
    if (logFilter === 'AI') return log.type === 'agent';
    if (logFilter === 'ERRORS') return log.type === 'error';
    return true;
  });

  const getLogColor = (type) => {
    switch(type) {
      case 'user': return 'text-cyan-400 font-bold';
      case 'agent': return 'text-purple-400 font-semibold';
      case 'network': return 'text-amber-400 font-medium';
      case 'error': return 'text-rose-400 font-bold';
      case 'success': return 'text-emerald-400 font-semibold';
      default: return 'text-slate-300';
    }
  };

  const filteredItems = (dbState.items || []).filter(item => 
    item.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
    item.sku.toLowerCase().includes(searchQuery.toLowerCase())
  );

  return (
    <div className="flex flex-col h-screen w-full bg-slate-950 text-slate-100 font-sans overflow-hidden select-none">
      
      {/* FUTURISTIC HUD HEADER */}
      <header className="h-16 border-b border-slate-800/80 bg-slate-900/90 backdrop-blur-md px-6 flex items-center justify-between z-20 shadow-xl">
        
        {/* Brand & Node Status */}
        <div className="flex items-center gap-4">
          <div className="flex items-center gap-2.5">
            <div className="p-2 rounded-xl bg-gradient-to-tr from-cyan-500/20 to-blue-500/20 border border-cyan-500/30 shadow-lg shadow-cyan-500/10">
              <Network size={22} className="text-cyan-400 animate-pulse" />
            </div>
            <div>
              <h1 className="text-lg font-black tracking-wider bg-gradient-to-r from-cyan-400 via-teal-300 to-emerald-400 bg-clip-text text-transparent">
                AGENTIC MESH
              </h1>
              <p className="text-[10px] text-slate-400 font-mono tracking-widest uppercase">Autonomous P2P DB Swarm</p>
            </div>
          </div>

          <div className="h-6 w-[1px] bg-slate-800 mx-1 hidden md:block" />

          {/* Node Identity Badge */}
          <div className="hidden md:flex items-center gap-2 px-3 py-1.5 rounded-lg bg-slate-950 border border-slate-800 text-xs font-mono">
            <Server size={14} className="text-emerald-400" />
            <span className="text-slate-400">Node:</span>
            <span className="font-bold text-slate-200">{nodeInfo.name}</span>
            <span className="text-slate-500 text-[10px]">(P2P :{nodeInfo.p2pPort})</span>
            {nodeInfo.peerId && nodeInfo.peerId !== 'N/A' && (
              <button 
                onClick={copyPeerId}
                title="Copy Peer ID" 
                className="ml-1 text-slate-500 hover:text-cyan-400 transition cursor-pointer flex items-center gap-1"
              >
                {copiedPeerId ? <CheckCircle2 size={13} className="text-emerald-400" /> : <Copy size={13} />}
              </button>
            )}
          </div>
        </div>

        {/* Center Target Port Selector */}
        <div className="flex items-center gap-2 bg-slate-950/80 p-1 rounded-xl border border-slate-800 text-xs">
          <span className="text-slate-400 px-2 font-mono text-[11px] uppercase">Node Port:</span>
          {['3001', '3002', '3003', '3004'].map(port => (
            <button
              key={port}
              onClick={() => setWsPort(port)}
              className={`px-3 py-1 rounded-lg font-mono font-semibold transition cursor-pointer ${
                wsPort === port 
                  ? 'bg-gradient-to-r from-cyan-600 to-blue-600 text-white shadow-md shadow-cyan-600/30' 
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
              placeholder="Other..."
              value={customPort}
              onChange={(e) => setCustomPort(e.target.value)}
              className="w-16 bg-slate-900 border border-slate-700 rounded px-1.5 py-0.5 text-slate-200 text-xs font-mono focus:outline-none focus:border-cyan-400"
            />
          </form>
        </div>

        {/* Right Status Pill, Sync & Refresh */}
        <div className="flex items-center gap-3">
          <button
            onClick={triggerSyncWithMesh}
            disabled={isSyncing || nodeInfo.wsStatus !== 'CONNECTED'}
            title="Request Catch-Up Sync with Mesh Peers"
            className="flex items-center gap-1.5 px-3 py-1.5 rounded-xl bg-slate-800 hover:bg-slate-700 text-cyan-300 hover:text-white border border-cyan-500/30 text-xs font-mono transition cursor-pointer disabled:opacity-50"
          >
            <ArrowRightLeft size={14} className={isSyncing ? 'animate-spin' : ''} />
            <span className="hidden sm:inline">Sync Mesh</span>
          </button>

          <div className={`flex items-center gap-2 px-3 py-1.5 rounded-full border text-xs font-semibold ${
            nodeInfo.wsStatus === 'CONNECTED'
              ? 'bg-emerald-950/40 border-emerald-500/30 text-emerald-400 shadow-lg shadow-emerald-950/50'
              : 'bg-rose-950/40 border-rose-500/30 text-rose-400 animate-pulse'
          }`}>
            <Radio size={14} className={nodeInfo.wsStatus === 'CONNECTED' ? 'animate-ping' : ''} />
            <span>{nodeInfo.wsStatus}</span>
          </div>

          <button 
            onClick={refreshAll} 
            title="Refresh All Data" 
            className="p-2 rounded-xl bg-slate-800 hover:bg-slate-700 text-slate-300 hover:text-white border border-slate-700 transition cursor-pointer"
          >
            <RefreshCw size={16} />
          </button>
        </div>
      </header>

      {/* MAIN BODY GRID */}
      <div className="flex-1 grid grid-cols-12 overflow-hidden">
        
        {/* LEFT COLUMN: Controls, Terminal & AI Agent (5 Cols) */}
        <div className="col-span-12 lg:col-span-5 flex flex-col border-r border-slate-800/80 bg-slate-900/50 backdrop-blur-sm overflow-hidden">
          
          {/* ACTION BUTTONS GRID */}
          <div className="p-4 border-b border-slate-800/80 grid grid-cols-2 gap-2.5 bg-slate-900/60">
            
            <button 
              onClick={triggerFastProposal} 
              className="group relative p-3 rounded-xl bg-gradient-to-br from-emerald-900/40 to-slate-900 border border-emerald-500/30 hover:border-emerald-500/60 transition shadow-lg shadow-emerald-950/30 cursor-pointer text-left overflow-hidden"
            >
              <div className="absolute top-0 right-0 p-2 opacity-10 group-hover:opacity-20 transition text-emerald-400">
                <Activity size={40} />
              </div>
              <div className="flex items-center gap-2 text-emerald-400 font-bold text-xs mb-1">
                <Activity size={16} /> Propose Fast Tx
              </div>
              <p className="text-[11px] text-slate-400 leading-tight">Sub-ms pure JS schema validation</p>
            </button>

            <button 
              onClick={triggerConflictTest} 
              className="group relative p-3 rounded-xl bg-gradient-to-br from-rose-900/40 to-slate-900 border border-rose-500/30 hover:border-rose-500/60 transition shadow-lg shadow-rose-950/30 cursor-pointer text-left overflow-hidden"
            >
              <div className="absolute top-0 right-0 p-2 opacity-10 group-hover:opacity-20 transition text-rose-400">
                <ShieldAlert size={40} />
              </div>
              <div className="flex items-center gap-2 text-rose-400 font-bold text-xs mb-1">
                <ShieldAlert size={16} /> Test Schema Rule
              </div>
              <p className="text-[11px] text-slate-400 leading-tight">Intercept malformed/orphaned writes</p>
            </button>

            <button 
              onClick={triggerAiAudit} 
              className="col-span-2 group relative p-3 rounded-xl bg-gradient-to-r from-purple-900/40 via-violet-900/30 to-slate-900 border border-purple-500/30 hover:border-purple-500/60 transition shadow-lg shadow-purple-950/30 cursor-pointer text-left overflow-hidden flex items-center justify-between"
            >
              <div className="flex items-center gap-2.5">
                <div className="p-1.5 rounded-lg bg-purple-500/20 text-purple-400">
                  <ShieldCheck size={18} />
                </div>
                <div>
                  <div className="text-purple-300 font-bold text-xs">Trigger 3NF/BCNF Schema Audit</div>
                  <p className="text-[11px] text-slate-400 leading-tight">Scans mesh logs for normalization and key dependency anomalies</p>
                </div>
              </div>
              <Sparkles size={18} className="text-purple-400 animate-pulse" />
            </button>
          </div>

          {/* AI AGENT PROMPT BAR */}
          <form onSubmit={handleAiPrompt} className="p-4 border-b border-slate-800/80 bg-slate-950/80">
            <div className="flex items-center justify-between mb-2">
              <label className="text-xs font-bold text-purple-400 flex items-center gap-1.5">
                <Cpu size={14} /> AI Operations Planner
              </label>
              <span className="text-[10px] text-slate-500 font-mono uppercase">
                Model: {nodeInfo.modelName || 'Ollama'}
              </span>
            </div>

            <div className="relative flex items-center">
              <input
                type="text"
                value={nlInput}
                onChange={(e) => setNlInput(e.target.value)}
                placeholder={`Ask ${nodeInfo.modelName || 'AI'} (e.g. 'Add a book titled Distributed Systems for 59.99')`}
                disabled={isChatLoading}
                className="w-full bg-slate-900/90 border border-purple-500/30 focus:border-purple-400 rounded-xl px-3.5 py-2.5 pr-10 text-xs text-slate-100 placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-purple-500/20 transition shadow-inner font-mono"
              />
              <button
                type="submit"
                disabled={isChatLoading || !nlInput.trim()}
                className="absolute right-1.5 p-2 bg-gradient-to-r from-purple-600 to-violet-600 hover:from-purple-500 hover:to-violet-500 disabled:opacity-40 text-white rounded-lg text-xs font-semibold transition cursor-pointer shadow-md"
              >
                {isChatLoading ? <RefreshCw size={14} className="animate-spin" /> : <Send size={14} />}
              </button>
            </div>
          </form>

          {/* COLOR-CODED TERMINAL LOG VIEWER */}
          <div className="flex-1 flex flex-col bg-slate-950 overflow-hidden">
            
            {/* Terminal Header Filter Tabs */}
            <div className="px-4 py-2 border-b border-slate-800/80 bg-slate-900/70 flex items-center justify-between text-xs">
              <div className="flex items-center gap-1.5 font-mono text-slate-400">
                <Terminal size={14} className="text-cyan-400" />
                <span>Live Event Stream</span>
              </div>

              <div className="flex items-center gap-1 font-mono text-[11px]">
                {['ALL', 'P2P', 'DB', 'AI', 'ERRORS'].map(f => (
                  <button
                    key={f}
                    onClick={() => setLogFilter(f)}
                    className={`px-2 py-0.5 rounded transition cursor-pointer ${
                      logFilter === f ? 'bg-slate-700 text-white font-bold' : 'text-slate-500 hover:text-slate-300'
                    }`}
                  >
                    {f}
                  </button>
                ))}
              </div>
            </div>

            {/* Terminal Output Scroll Container */}
            <div className="flex-1 p-4 overflow-y-auto font-mono text-[11px] space-y-1.5 select-text">
              {filteredLogs.length === 0 ? (
                <div className="text-slate-600 italic py-4 text-center">No logs matching filter '{logFilter}'</div>
              ) : (
                filteredLogs.map((log, index) => (
                  <div key={index} className="flex gap-2 items-start leading-relaxed hover:bg-slate-900/40 p-1 rounded transition">
                    <span className="text-slate-600 shrink-0 select-none">[{log.time}]</span>
                    <span className={getLogColor(log.type)}>{log.text}</span>
                  </div>
                ))
              )}
            </div>

            {/* Offline Helper Banner when WS Disconnected */}
            {nodeInfo.wsStatus !== 'CONNECTED' && (
              <div className="p-3 bg-rose-950/80 border-t border-rose-800/80 text-rose-300 text-xs flex items-center justify-between">
                <div className="flex items-center gap-2">
                  <AlertCircle size={16} className="text-rose-400 shrink-0" />
                  <span>Node Server Offline on Port :{wsPort}</span>
                </div>
                <code className="bg-slate-900 px-2 py-0.5 rounded text-[11px] text-rose-200">node index.js</code>
              </div>
            )}

          </div>

        </div>

        {/* RIGHT COLUMN: Topology, Database & Mesh Log (7 Cols) */}
        <div className="col-span-12 lg:col-span-7 flex flex-col bg-slate-950 overflow-hidden">
          
          {/* TAB SELECTION BAR */}
          <div className="h-12 border-b border-slate-800/80 bg-slate-900/80 backdrop-blur px-6 flex items-center justify-between">
            <div className="flex items-center gap-2">
              <button
                onClick={() => setActiveTab('topology')}
                className={`px-3 py-1.5 rounded-xl text-xs font-bold transition flex items-center gap-1.5 cursor-pointer ${
                  activeTab === 'topology'
                    ? 'bg-gradient-to-r from-cyan-600/30 to-blue-600/30 text-cyan-300 border border-cyan-500/40'
                    : 'text-slate-400 hover:text-slate-200 hover:bg-slate-800/50'
                }`}
              >
                <Network size={14} /> Swarm Topology ({peers.length + 1})
              </button>

              <button
                onClick={() => setActiveTab('database')}
                className={`px-3 py-1.5 rounded-xl text-xs font-bold transition flex items-center gap-1.5 cursor-pointer ${
                  activeTab === 'database'
                    ? 'bg-gradient-to-r from-emerald-600/30 to-teal-600/30 text-emerald-300 border border-emerald-500/40'
                    : 'text-slate-400 hover:text-slate-200 hover:bg-slate-800/50'
                }`}
              >
                <Database size={14} /> Database ({dbState.items?.length || 0})
              </button>

              <button
                onClick={() => setActiveTab('meshlog')}
                className={`px-3 py-1.5 rounded-xl text-xs font-bold transition flex items-center gap-1.5 cursor-pointer ${
                  activeTab === 'meshlog'
                    ? 'bg-gradient-to-r from-purple-600/30 to-indigo-600/30 text-purple-300 border border-purple-500/40'
                    : 'text-slate-400 hover:text-slate-200 hover:bg-slate-800/50'
                }`}
              >
                <Clock size={14} /> Mesh Log ({meshLogs.length})
              </button>

              <button
                onClick={() => setActiveTab('chat')}
                className={`px-3 py-1.5 rounded-xl text-xs font-bold transition flex items-center gap-1.5 cursor-pointer relative ${
                  activeTab === 'chat'
                    ? 'bg-gradient-to-r from-violet-600/30 to-fuchsia-600/30 text-violet-300 border border-violet-500/40 shadow-lg shadow-violet-950/40'
                    : 'text-slate-400 hover:text-slate-200 hover:bg-slate-800/50'
                }`}
              >
                <MessageSquare size={14} /> Agent Chat
                {chatMessages.length > 1 && (
                  <span className="w-2 h-2 rounded-full bg-violet-400 animate-pulse" />
                )}
              </button>

              <button
                onClick={() => setActiveTab('activity')}
                className={`px-3 py-1.5 rounded-xl text-xs font-bold transition flex items-center gap-1.5 cursor-pointer ${
                  activeTab === 'activity'
                    ? 'bg-gradient-to-r from-cyan-600/30 to-sky-600/30 text-cyan-300 border border-cyan-500/40'
                    : 'text-slate-400 hover:text-slate-200 hover:bg-slate-800/50'
                }`}
              >
                <Gauge size={14} /> Agent Activity
                {pendingProposals.length > 0 && (
                  <span className="px-1.5 rounded-full bg-amber-500/20 border border-amber-400/40 text-amber-300 text-[10px]">{pendingProposals.length}</span>
                )}
                {activity.tasks.some(t => t.status === 'running') && (
                  <span className="w-2 h-2 rounded-full bg-cyan-400 animate-pulse" />
                )}
              </button>
            </div>

            {/* DB Quick Stats */}
            <div className="hidden sm:flex items-center gap-3 text-xs font-mono text-slate-400">
              <span>Items: <strong className="text-emerald-400">{dbState.items?.length || 0}</strong></span>
              <span>Logs: <strong className="text-purple-400">{meshLogs.length}</strong></span>
            </div>
          </div>

          {/* TAB 1: SWARM TOPOLOGY GRAPH */}
          {activeTab === 'topology' && (
            <div ref={graphContainerRef} className="flex-1 relative bg-slate-950 flex items-center justify-center overflow-hidden">
              <ForceGraph2D
                ref={graphRef}
                width={graphDimensions.width}
                height={graphDimensions.height}
                graphData={graphData}
                nodeLabel={node => `${node.label || node.id}\nPeer ID: ${node.peerId || 'Local'}`}
                nodeCanvasObjectMode={() => 'after'}
                nodeCanvasObject={(node, ctx, globalScale) => {
                  const fontSize = Math.max(12 / globalScale, 3);
                  ctx.font = `600 ${fontSize}px Inter, sans-serif`;
                  ctx.textAlign = 'center';
                  ctx.textBaseline = 'top';
                  ctx.fillStyle = '#e2e8f0';
                  ctx.fillText(node.label || node.id, node.x, node.y + Math.sqrt(node.val) * 9 + 2);
                }}
                nodeColor={node => node.group === 1 ? '#06b6d4' : '#10b981'}
                nodeRelSize={9}
                linkColor={() => '#334155'}
                linkWidth={2}
                linkDirectionalParticles={2}
                linkDirectionalParticleSpeed={0.005}
                backgroundColor="#030712"
                d3AlphaDecay={0.03}
                cooldownTicks={120}
                onEngineStop={() => graphRef.current?.zoomToFit(400, 90)}
              />
              
              {/* Connected nodes list */}
              <div className="absolute top-4 left-4 bg-slate-900/90 border border-slate-800 rounded-xl text-xs text-slate-300 backdrop-blur shadow-xl p-3 space-y-2 max-w-sm">
                <div className="text-[10px] uppercase tracking-wider text-slate-400 font-mono">Connected nodes</div>
                <div className="flex items-center gap-2">
                  <span className="w-2 h-2 rounded-full bg-cyan-400" />
                  <span className="font-bold text-slate-100">{nodeInfo.name}</span>
                  <span className="text-slate-500 font-mono text-[10px]">this node · {nodeInfo.modelName}</span>
                </div>
                {peers.length === 0 ? (
                  <div className="text-slate-500 text-[11px]">No peers connected</div>
                ) : peers.map((p, idx) => (
                  <div key={p.peerId || idx} className="flex items-start gap-2">
                    <span className={`w-2 h-2 mt-1 rounded-full ${p.connectedAt ? 'bg-emerald-400' : 'bg-slate-500'}`} />
                    <div className="min-w-0">
                      <div className="flex items-center gap-1.5">
                        <span className="font-bold text-slate-100">{peerLabel(p, idx)}</span>
                        <span className="text-[10px] text-slate-400">{p.connectedAt ? 'connected' : 'discovered'}</span>
                      </div>
                      <div className="text-slate-500 font-mono text-[10px] truncate">
                        {p.role ? `${p.role} · ` : ''}{p.model ? `${p.model} · ` : ''}{peerIp(p)} · {p.peerId?.slice(0, 12)}…
                      </div>
                      {p.perf && (
                        <div className="text-slate-400 font-mono text-[10px]">
                          CPU {p.perf.cpuPercent ?? '—'}% · RAM {p.perf.memPercent ?? '—'}% · {p.perf.processMemMb ?? '—'} MB
                        </div>
                      )}
                      {!p.name && <div className="text-[10px] text-slate-500">Name unknown — peer runs older code without name announcements</div>}
                    </div>
                  </div>
                ))}
              </div>

              {/* Overlay Badge */}
              <div className="absolute top-4 right-4 bg-slate-900/90 border border-slate-800 px-3.5 py-2 rounded-xl text-xs text-slate-300 backdrop-blur shadow-xl flex items-center gap-2">
                <div className="w-2.5 h-2.5 rounded-full bg-emerald-400 animate-ping" />
                <span>Live Swarm Topology ({peers.length + 1} Node{peers.length === 0 ? '' : 's'})</span>
              </div>
            </div>
          )}

          {/* TAB 2: REPLICATED SQLITE DATABASE TABLES */}
          {activeTab === 'database' && (
            <div className="flex-1 flex flex-col p-6 bg-slate-950 overflow-y-auto space-y-6">
              
              {/* Search & Filter Header */}
              <div className="flex items-center justify-between gap-4 bg-slate-900/80 p-3 rounded-2xl border border-slate-800/80">
                <div className="relative flex-1">
                  <Search size={16} className="absolute left-3 top-2.5 text-slate-500" />
                  <input
                    type="text"
                    placeholder="Search items by name or SKU..."
                    value={searchQuery}
                    onChange={e => setSearchQuery(e.target.value)}
                    className="w-full bg-slate-950 border border-slate-800 rounded-xl pl-9 pr-4 py-2 text-xs text-slate-100 placeholder-slate-500 focus:outline-none focus:border-cyan-500 font-mono"
                  />
                </div>
                <span className="text-xs text-slate-400 font-mono">Showing {filteredItems.length} of {dbState.items?.length || 0} items</span>
              </div>

              {/* Items Table */}
              <div className="space-y-2">
                <h3 className="text-xs font-bold text-emerald-400 flex items-center gap-2 font-mono uppercase tracking-wider">
                  <Database size={15} /> Items Table (`items`)
                </h3>
                <div className="overflow-x-auto border border-slate-800/80 rounded-2xl bg-slate-900/50 backdrop-blur">
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
                      {filteredItems.length === 0 ? (
                        <tr>
                          <td colSpan={5} className="p-4 text-center text-slate-500 italic">No items found</td>
                        </tr>
                      ) : (
                        filteredItems.map(item => (
                          <tr key={item.id} className="hover:bg-slate-800/40 transition">
                            <td className="p-3 font-semibold text-slate-400">{item.id}</td>
                            <td className="p-3 font-bold text-slate-100">{item.name}</td>
                            <td className="p-3 text-amber-400 font-bold">{item.sku}</td>
                            <td className="p-3 text-emerald-400 font-bold">${item.price}</td>
                            <td className="p-3 text-cyan-400">{item.category_id}</td>
                          </tr>
                        ))
                      )}
                    </tbody>
                  </table>
                </div>
              </div>

              {/* Categories Table */}
              <div className="space-y-2">
                <h3 className="text-xs font-bold text-cyan-400 flex items-center gap-2 font-mono uppercase tracking-wider">
                  <Layers size={15} /> Categories Table (`categories`)
                </h3>
                <div className="overflow-x-auto border border-slate-800/80 rounded-2xl bg-slate-900/50 backdrop-blur">
                  <table className="w-full text-left text-xs font-mono">
                    <thead className="bg-slate-900 text-slate-400 border-b border-slate-800">
                      <tr>
                        <th className="p-3">ID</th>
                        <th className="p-3">Category Name</th>
                        <th className="p-3">Description</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-800/50">
                      {(dbState.categories || []).map(cat => (
                        <tr key={cat.id} className="hover:bg-slate-800/40 transition">
                          <td className="p-3 font-semibold text-slate-400">{cat.id}</td>
                          <td className="p-3 font-bold text-cyan-300">{cat.name}</td>
                          <td className="p-3 text-slate-400">{cat.description || 'N/A'}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>

            </div>
          )}

          {/* TAB 3: MESH LOG & VECTOR CLOCKS */}
          {activeTab === 'meshlog' && (
            <div className="flex-1 flex flex-col p-6 bg-slate-950 overflow-y-auto space-y-4">
              <div className="flex items-center justify-between bg-slate-900/80 p-3 rounded-2xl border border-slate-800/80">
                <div>
                  <h3 className="text-xs font-bold text-purple-400 font-mono uppercase">Replicated Mesh Log (`_mesh_log`)</h3>
                  <p className="text-[11px] text-slate-400">Append-only sequence with causal vector-clock snapshots</p>
                </div>
                <button
                  onClick={fetchMeshLogs}
                  className="px-3 py-1 rounded-lg bg-slate-800 hover:bg-slate-700 text-xs font-mono text-slate-300 border border-slate-700 cursor-pointer"
                >
                  Refresh Log
                </button>
              </div>

              <div className="overflow-x-auto border border-slate-800/80 rounded-2xl bg-slate-900/50 backdrop-blur">
                <table className="w-full text-left text-xs font-mono">
                  <thead className="bg-slate-900 text-slate-400 border-b border-slate-800">
                    <tr>
                      <th className="p-3">Tx ID</th>
                      <th className="p-3">Op</th>
                      <th className="p-3">Table</th>
                      <th className="p-3">Origin Peer</th>
                      <th className="p-3">Vector Clock</th>
                      <th className="p-3">Applied At</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-800/50">
                    {meshLogs.length === 0 ? (
                      <tr>
                        <td colSpan={6} className="p-4 text-center text-slate-500 italic">No mesh log entries recorded yet.</td>
                      </tr>
                    ) : (
                      meshLogs.map(log => (
                        <tr key={log.id} className="hover:bg-slate-800/40 transition">
                          <td className="p-3 font-semibold text-slate-400" title={log.id}>
                            {log.id.slice(0, 16)}...
                          </td>
                          <td className="p-3 font-bold text-emerald-400">{log.operation}</td>
                          <td className="p-3 text-cyan-300">{log.table_name}</td>
                          <td className="p-3 text-slate-300" title={log.peer_id}>
                            {log.peer_id ? log.peer_id.slice(0, 12) + '...' : 'local'}
                          </td>
                          <td className="p-3 text-amber-300 font-mono text-[10px]">
                            {log.vector_clock}
                          </td>
                          <td className="p-3 text-slate-500 text-[10px]">
                            {log.applied_at}
                          </td>
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {/* TAB 4: AGENT COMMUNICATION SECTION */}
          {activeTab === 'chat' && (
            <div className="flex-1 flex flex-col bg-slate-950 overflow-hidden">
              
              {/* Chat Sub-Header with Mode Selector */}
              <div className="p-3 border-b border-slate-800/80 bg-slate-900/60 flex items-center justify-between gap-2 flex-wrap">
                <div className="flex items-center gap-2.5">
                  <div className="p-1.5 rounded-lg bg-violet-500/20 text-violet-400 border border-violet-500/30">
                    <Bot size={16} />
                  </div>
                  <div>
                    <div className="flex items-center gap-2">
                      <span className="text-xs font-bold text-slate-200">Coding Agent</span>
                      <span className="text-[10px] px-2 py-0.5 rounded-full bg-violet-950 border border-violet-500/30 text-violet-300 font-mono">
                        {nodeInfo.modelName || 'Ollama'}
                      </span>
                    </div>
                    <p className="text-[10px] text-slate-400">Node "{nodeInfo.name}" • {chatMode === 'fast' ? 'Ultra-fast CPU coding (<128 tokens)' : 'Deep architecture & planning'}</p>
                  </div>
                </div>

                <div className="flex items-center gap-2">
                  {/* Mode Selector */}
                  <div className="flex items-center bg-slate-900 border border-slate-800 rounded-lg p-0.5 text-xs font-mono">
                    <button
                      type="button"
                      onClick={() => setChatMode('fast')}
                      className={`px-2.5 py-1 rounded-md transition cursor-pointer flex items-center gap-1.5 ${
                        chatMode === 'fast'
                          ? 'bg-violet-600 text-white font-bold shadow-sm'
                          : 'text-slate-400 hover:text-slate-200'
                      }`}
                      title="Fast Mode: qwen2.5-coder:1.5b with 128-token limit"
                    >
                      <span>⚡</span> Fast (1.5B)
                    </button>
                    <button
                      type="button"
                      onClick={() => setChatMode('deep')}
                      className={`px-2.5 py-1 rounded-md transition cursor-pointer flex items-center gap-1.5 ${
                        chatMode === 'deep'
                          ? 'bg-purple-600 text-white font-bold shadow-sm'
                          : 'text-slate-400 hover:text-slate-200'
                      }`}
                      title="Deep Mode: gemma4:e2b with full architecture & planning context"
                    >
                      <span>🧠</span> Deep (Gemma)
                    </button>
                  </div>

                  <button
                    onClick={() => setChatMessages([{
                      id: `reset-${Date.now()}`,
                      sender: 'agent',
                      text: 'Chat history cleared. What can I code or plan for you in node "delta"?',
                      timestamp: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
                      operations: null
                    }])}
                    className="px-2.5 py-1 rounded-lg bg-slate-800/70 hover:bg-slate-800 text-[11px] text-slate-400 hover:text-rose-400 transition flex items-center gap-1 cursor-pointer border border-slate-700/60"
                    title="Clear conversation"
                  >
                    <Trash2 size={12} /> Clear
                  </button>
                </div>
              </div>

              {/* Multi-Model Warning Banner */}
              {loadedModels.length > 1 && (
                <div className="mx-4 mt-2.5 p-2.5 rounded-xl bg-amber-950/70 border border-amber-500/50 text-amber-200 text-xs flex items-center justify-between gap-3">
                  <div className="flex items-center gap-2">
                    <span className="text-base shrink-0">⚠️</span>
                    <span>
                      <strong>Warning:</strong> Multiple models loaded in Ollama ({loadedModels.join(', ')}). CPU inference will be throttled. Run <code className="bg-amber-900/60 px-1 py-0.5 rounded font-mono text-[11px]">ollama stop &lt;model&gt;</code> to free CPU memory.
                    </span>
                  </div>
                  <button
                    onClick={fetchAgentStatus}
                    className="px-2 py-1 bg-amber-900/50 hover:bg-amber-900 border border-amber-600/40 rounded text-[10px] font-mono shrink-0 cursor-pointer"
                  >
                    Refresh
                  </button>
                </div>
              )}

              {/* Quick Prompts Bar */}
              <div className="px-4 py-2 border-b border-slate-800/60 bg-slate-900/30 flex items-center gap-2 overflow-x-auto text-[11px]">
                <span className="text-slate-500 shrink-0 font-mono">Suggested:</span>
                {[
                  '👋 Hello Coding Agent',
                  '📦 What items are in the database?',
                  '➕ Add a Wireless Mouse for 49.99',
                  '💻 Show WebSocket API client code',
                  '📐 Show SQLite schema DDL',
                  '⚡ How does vector clock sync work?'
                ].map((prompt, idx) => (
                  <button
                    key={idx}
                    onClick={() => sendChatMessage(prompt.replace(/^[^\w\s]+/, '').trim())}
                    disabled={isChatLoading}
                    className="px-2.5 py-1 rounded-lg bg-slate-900 hover:bg-slate-800 border border-slate-800 hover:border-violet-500/40 text-slate-300 hover:text-violet-300 transition whitespace-nowrap cursor-pointer shrink-0"
                  >
                    {prompt}
                  </button>
                ))}
              </div>

              {/* Chat Messages Stream */}
              <div className="flex-1 p-4 overflow-y-auto space-y-4">
                {chatMessages.map(msg => (
                  <div
                    key={msg.id}
                    className={`flex ${msg.sender === 'user' ? 'justify-end' : 'justify-start'}`}
                  >
                    <div className={`max-w-[85%] rounded-2xl p-4 shadow-lg ${
                      msg.sender === 'user'
                        ? 'bg-gradient-to-r from-cyan-600 to-blue-600 text-white rounded-tr-sm shadow-cyan-950/40'
                        : msg.isError
                        ? 'bg-rose-950/60 border border-rose-800/60 text-rose-200 rounded-tl-sm'
                        : 'bg-slate-900/90 border border-slate-800/90 text-slate-100 rounded-tl-sm shadow-slate-950/60'
                    }`}>
                      {/* Message Meta */}
                      <div className="flex items-center justify-between gap-3 mb-1.5 text-[10px] opacity-75">
                        <span className="font-semibold flex items-center gap-1">
                          {msg.sender === 'user' ? <User size={12} /> : <Bot size={12} />}
                          {msg.sender === 'user' ? 'You' : `Agent (${nodeInfo.modelName || 'Ollama'})`}
                        </span>
                        <span>{msg.timestamp}</span>
                      </div>

                      {/* Message Content */}
                      <div className="text-xs leading-relaxed whitespace-pre-wrap">
                        {msg.text ? (
                          <>
                            {msg.text}
                            {msg.isStreaming && (
                              <span className="inline-block w-1.5 h-3.5 ml-1 bg-violet-400 animate-pulse align-middle rounded-sm" />
                            )}
                          </>
                        ) : (msg.isGenerating || msg.isStreaming) ? (
                          <div className="flex items-center gap-2 text-violet-300 py-0.5">
                            <RefreshCw size={13} className="animate-spin text-violet-400" />
                            <span className="italic">{msg.isGenerating ? 'Model is generating...' : 'Connecting to model...'}</span>
                          </div>
                        ) : null}
                      </div>

                      {/* Proposed Operations Card */}
                      {msg.operations && Array.isArray(msg.operations) && msg.operations.length > 0 && (
                        <div className="mt-3 p-3 rounded-xl bg-slate-950/90 border border-violet-500/30 space-y-2.5">
                          <div className="flex items-center justify-between">
                            <div className="flex items-center gap-1.5 text-xs font-bold text-violet-400">
                              <Sparkles size={14} /> Proposed Database Mutations ({msg.operations.length})
                            </div>
                            <span className="text-[10px] text-emerald-400 font-mono bg-emerald-950/80 px-2 py-0.5 rounded border border-emerald-500/30">
                              Schema Verified
                            </span>
                          </div>

                          <div className="space-y-1.5 max-h-48 overflow-y-auto">
                            {msg.operations.map((op, opIdx) => (
                              <div key={opIdx} className="p-2 rounded-lg bg-slate-900/90 border border-slate-800 text-[11px] font-mono">
                                <div className="flex items-center justify-between text-slate-300">
                                  <span className="font-bold text-emerald-400">{op.operation}</span>
                                  <span className="text-cyan-400">table: {op.table}</span>
                                </div>
                                <div className="text-slate-400 mt-1 text-[10px] break-all">
                                  {JSON.stringify(op.data)}
                                </div>
                              </div>
                            ))}
                          </div>

                          {/* Approval Actions */}
                          <div className="pt-2 border-t border-slate-800 flex items-center justify-between gap-2">
                            {msg.status === 'executed' ? (
                              <div className="flex items-center gap-1.5 text-emerald-400 font-semibold text-xs py-1">
                                <CheckCircle2 size={15} /> Executed & Gossiped to Swarm
                              </div>
                            ) : (
                              <>
                                <span className="text-[10px] text-slate-400">Requires your authorization to commit.</span>
                                <button
                                  onClick={() => executeApprovedPlan(msg.id, msg.operations)}
                                  disabled={executingPlanId === msg.id}
                                  className="px-3 py-1.5 rounded-lg bg-gradient-to-r from-emerald-600 to-teal-600 hover:from-emerald-500 hover:to-teal-500 text-white font-bold text-xs flex items-center gap-1.5 transition cursor-pointer shadow-md disabled:opacity-50"
                                >
                                  {executingPlanId === msg.id ? (
                                    <>
                                      <RefreshCw size={13} className="animate-spin" /> Committing...
                                    </>
                                  ) : (
                                    <>
                                      <CheckCircle2 size={13} /> Approve & Execute
                                    </>
                                  )}
                                </button>
                              </>
                            )}
                          </div>
                        </div>
                      )}
                    </div>
                  </div>
                ))}

                {/* Loading indicator */}
                {isChatLoading && (
                  <div className="flex justify-start">
                    <div className="bg-slate-900/90 border border-slate-800/90 rounded-2xl rounded-tl-sm p-3.5 flex items-center gap-2 text-xs text-slate-400">
                      <RefreshCw size={14} className="animate-spin text-violet-400" />
                      <span>Agent is thinking and analyzing database state...</span>
                    </div>
                  </div>
                )}

                <div ref={chatBottomRef} />
              </div>

              {/* Chat Input Bar */}
              <form
                onSubmit={(e) => { e.preventDefault(); sendChatMessage(); }}
                className="p-3 border-t border-slate-800/80 bg-slate-900/80 flex flex-col gap-1.5"
              >
                <div className="relative flex items-center">
                  <input
                    type="text"
                    value={chatInput}
                    onChange={(e) => setChatInput(e.target.value)}
                    placeholder="Message agent (e.g. 'hi', 'what items do we have?', 'add item mechanical keyboard for 89.99')..."
                    disabled={isChatLoading}
                    className="w-full bg-slate-950 border border-violet-500/30 focus:border-violet-400 rounded-xl px-4 py-2.5 pr-12 text-xs text-slate-100 placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-violet-500/20 transition font-mono"
                  />
                  <button
                    type="submit"
                    disabled={isChatLoading || !chatInput.trim()}
                    className="absolute right-1.5 p-2 bg-gradient-to-r from-violet-600 to-fuchsia-600 hover:from-violet-500 hover:to-fuchsia-500 disabled:opacity-40 text-white rounded-lg text-xs font-semibold transition cursor-pointer shadow-md"
                  >
                    {isChatLoading ? <RefreshCw size={14} className="animate-spin" /> : <Send size={14} />}
                  </button>
                </div>
                <div className="flex items-center justify-between text-[10px] text-slate-500 px-1 font-mono">
                  <span>Fast schema validator guarantees zero raw SQL and safe mutations</span>
                  <span>Enter to send</span>
                </div>
              </form>

            </div>
          )}

          {/* TAB 5: AGENT ACTIVITY — communications, task progress, performance */}
          {activeTab === 'activity' && (
            <AgentActivityPanel
              activity={activity}
              proposals={pendingProposals}
              onApprove={(id) => decideProposal(id, 'approve')}
              onReject={(id) => decideProposal(id, 'reject')}
              nodes={[
                {
                  id: nodeInfo.peerId,
                  name: nodeInfo.name,
                  local: true,
                  role: 'this laptop',
                  model: nodeInfo.modelName,
                  perf: activity.system ? { cpuPercent: activity.system.cpuPercent, memPercent: activity.system.memPercent } : null
                },
                ...peers.map((p, idx) => ({
                  id: p.peerId || idx,
                  name: peerLabel(p, idx),
                  role: p.role,
                  model: p.model,
                  ip: peerIp(p),
                  perf: p.perf || null
                }))
              ]}
            />
          )}

        </div>

      </div>

    </div>
  );
}
