import React, { useState, useEffect, useRef } from 'react';
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
  Sliders,
  Copy,
  ExternalLink
} from 'lucide-react';

export default function AgenticMeshDashboard() {
  const graphRef = useRef();
  const graphContainerRef = useRef(null);

  // Dynamic canvas sizing for ForceGraph2D
  const [graphDimensions, setGraphDimensions] = useState({ width: 600, height: 400 });

  // Port selector (default to 3001, but allows switching to 3002 Bravo or 3003 Charlie)
  const [wsPort, setWsPort] = useState('3001');
  const [nodeInfo, setNodeInfo] = useState({ name: 'Disconnected', peerId: 'N/A', wsStatus: 'CONNECTING' });
  const [peers, setPeers] = useState([]);
  const [dbState, setDbState] = useState({ categories: [], items: [], suppliers: [] });
  const [logs, setLogs] = useState([{ time: new Date().toLocaleTimeString(), text: 'System HUD Online. Connecting to mesh gateway...', type: 'system' }]);
  const [logFilter, setLogFilter] = useState('ALL');
  const [nlInput, setNlInput] = useState('');
  const [isProcessing, setIsProcessing] = useState(false);
  const [activeTab, setActiveTab] = useState('topology'); // 'topology' | 'database' | 'schema'
  const [searchQuery, setSearchQuery] = useState('');
  const [copiedPeerId, setCopiedPeerId] = useState(false);

  const getWsUrl = () => {
    const host = window.location.hostname || 'localhost';
    return `ws://${host}:${wsPort}/ws`;
  };

  const getApiUrl = (endpoint) => {
    const host = window.location.hostname || 'localhost';
    return `http://${host}:${wsPort}/api${endpoint}`;
  };

  const addLog = (text, type = 'system') => {
    setLogs(prev => [...prev.slice(-200), { time: new Date().toLocaleTimeString(), text, type }]);
  };

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

  const fetchDbState = async () => {
    try {
      const res = await fetch(getApiUrl('/db/state'));
      if (res.ok) {
        const data = await res.json();
        setDbState(data);
      }
    } catch (e) {}
  };

  const fetchPeers = async () => {
    try {
      const res = await fetch(getApiUrl('/peers'));
      if (res.ok) {
        const data = await res.json();
        setPeers(data);
      }
    } catch (e) {}
  };

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
        fetchDbState();
        fetchPeers();
      };

      socket.onmessage = (event) => {
        try {
          const msg = JSON.parse(event.data);
          
          if (msg.type === 'init') {
            setNodeInfo({ name: msg.nodeName, peerId: msg.peerId, wsStatus: 'CONNECTED' });
            if (msg.peers) setPeers(msg.peers);
            if (msg.dbState) setDbState(msg.dbState);
            addLog(`Mesh Node Active: ${msg.nodeName} (${msg.peerId?.slice(0, 12)}...)`, 'success');
          } else if (msg.type === 'log') {
            const { level, message, nodeName } = msg.data || {};
            let logType = 'system';
            if (level === 'P2P') logType = 'network';
            else if (level === 'DB') logType = 'success';
            else if (level === 'AI') logType = 'agent';
            else if (level === 'ERROR') logType = 'error';
            addLog(`[${nodeName || 'node'}] [${level || 'LOG'}] ${message}`, logType);
            if (level === 'DB') fetchDbState();
          } else if (msg.type === 'peer:joined' || msg.type === 'peer:left') {
            addLog(`Peer network event: ${msg.type}`, 'network');
            fetchPeers();
          } else if (msg.type === 'tx:committed' || msg.type === 'tx:replicated') {
            fetchDbState();
          }
        } catch (e) {}
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
  }, [wsPort]);

  // Topology Graph Data
  const graphData = {
    nodes: [
      { id: nodeInfo.name || 'Local Node', group: 1, val: 28, peerId: nodeInfo.peerId },
      ...peers.map((p, idx) => ({ 
        id: p.peerId ? `Node-${p.peerId.slice(0, 8)}` : `Peer-${idx + 1}`, 
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

  // Fast-path direct transaction
  const triggerFastProposal = async () => {
    addLog('Proposing Fast-Path Transaction (<1ms pure JS validation)...', 'system');
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
        addLog(`Fast-path write committed & gossiped! SKU: ${sku}`, 'success');
        fetchDbState();
      } else {
        addLog(`Fast-path validation error: ${data.errors?.join(', ')}`, 'error');
      }
    } catch (e) {
      addLog(`Error connecting to backend: ${e.message}. Ensure node is running on port ${wsPort}.`, 'error');
    }
  };

  // Test malformed payload rejection
  const triggerConflictTest = async () => {
    addLog('Executing Malformed Payload (Price -50 & Invalid FK 999)...', 'system');
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
      }
    } catch (e) {
      addLog(`Error connecting to backend: ${e.message}`, 'error');
    }
  };

  // AI Prompt handler with Gemma 4 E2B
  const handleAiPrompt = async (e) => {
    if (e) e.preventDefault();
    if (!nlInput.trim()) return;

    const userPrompt = nlInput.trim();
    setNlInput('');
    setIsProcessing(true);
    addLog(`User Prompt: "${userPrompt}"`, 'user');

    try {
      const res = await fetch(getApiUrl('/ask'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ prompt: userPrompt })
      });
      const data = await res.json();

      if (data.execResult && data.execResult.completed > 0) {
        addLog(`Gemma 4 E2B decomposed into ${data.plan.operations.length} tool operation(s). Executed & replicated!`, 'success');
        fetchDbState();
      } else if (data.error) {
        addLog(`AI execution note: ${data.error}`, 'error');
      }
    } catch (e) {
      addLog(`AI connection error: ${e.message}. Start backend with 'node index.js'`, 'error');
    } finally {
      setIsProcessing(false);
    }
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
    if (nodeInfo.peerId) {
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
          {['3001', '3002', '3003'].map(port => (
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
        </div>

        {/* Right Status Pill & Refresh */}
        <div className="flex items-center gap-4">
          <div className={`flex items-center gap-2 px-3 py-1.5 rounded-full border text-xs font-semibold ${
            nodeInfo.wsStatus === 'CONNECTED'
              ? 'bg-emerald-950/40 border-emerald-500/30 text-emerald-400 shadow-lg shadow-emerald-950/50'
              : 'bg-rose-950/40 border-rose-500/30 text-rose-400 animate-pulse'
          }`}>
            <Radio size={14} className={nodeInfo.wsStatus === 'CONNECTED' ? 'animate-ping' : ''} />
            <span>{nodeInfo.wsStatus}</span>
          </div>

          <button 
            onClick={() => { fetchDbState(); fetchPeers(); }} 
            title="Refresh Data" 
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
              <p className="text-[11px] text-slate-400 leading-tight">Sub-ms fast path JS schema check</p>
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
              <p className="text-[11px] text-slate-400 leading-tight">Verify rejection of bad payloads</p>
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
                  <div className="text-purple-300 font-bold text-xs">Trigger 3NF/BCNF Background AI Audit</div>
                  <p className="text-[11px] text-slate-400 leading-tight">Scans mesh logs for transitive & partial key dependencies</p>
                </div>
              </div>
              <Sparkles size={18} className="text-purple-400 animate-pulse" />
            </button>
          </div>

          {/* AI AGENT PROMPT BAR (Gemma 4 E2B) */}
          <form onSubmit={handleAiPrompt} className="p-4 border-b border-slate-800/80 bg-slate-950/80">
            <div className="flex items-center justify-between mb-2">
              <label className="text-xs font-bold text-purple-400 flex items-center gap-1.5">
                <Cpu size={14} /> Gemma 4 E2B AI Agent
              </label>
              <span className="text-[10px] text-slate-500 font-mono uppercase">Natural Language Tool Planner</span>
            </div>

            <div className="relative flex items-center">
              <input
                type="text"
                value={nlInput}
                onChange={(e) => setNlInput(e.target.value)}
                placeholder="Ask Gemma (e.g. 'Add a book titled Design Patterns priced at 49.99')"
                disabled={isProcessing}
                className="w-full bg-slate-900/90 border border-purple-500/30 focus:border-purple-400 rounded-xl px-3.5 py-2.5 pr-10 text-xs text-slate-100 placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-purple-500/20 transition shadow-inner font-mono"
              />
              <button
                type="submit"
                disabled={isProcessing || !nlInput.trim()}
                className="absolute right-1.5 p-2 bg-gradient-to-r from-purple-600 to-violet-600 hover:from-purple-500 hover:to-violet-500 disabled:opacity-40 text-white rounded-lg text-xs font-semibold transition cursor-pointer shadow-md"
              >
                {isProcessing ? <RefreshCw size={14} className="animate-spin" /> : <Send size={14} />}
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

        {/* RIGHT COLUMN: Swarm Topology & Replicated Database (7 Cols) */}
        <div className="col-span-12 lg:col-span-7 flex flex-col bg-slate-950 overflow-hidden">
          
          {/* TAB SELECTION BAR */}
          <div className="h-12 border-b border-slate-800/80 bg-slate-900/80 backdrop-blur px-6 flex items-center justify-between">
            <div className="flex items-center gap-2">
              <button
                onClick={() => setActiveTab('topology')}
                className={`px-4 py-2 rounded-xl text-xs font-bold transition flex items-center gap-2 cursor-pointer ${
                  activeTab === 'topology'
                    ? 'bg-gradient-to-r from-cyan-600/30 to-blue-600/30 text-cyan-300 border border-cyan-500/40'
                    : 'text-slate-400 hover:text-slate-200 hover:bg-slate-800/50'
                }`}
              >
                <Network size={15} /> Swarm Topology ({peers.length + 1})
              </button>

              <button
                onClick={() => setActiveTab('database')}
                className={`px-4 py-2 rounded-xl text-xs font-bold transition flex items-center gap-2 cursor-pointer ${
                  activeTab === 'database'
                    ? 'bg-gradient-to-r from-emerald-600/30 to-teal-600/30 text-emerald-300 border border-emerald-500/40'
                    : 'text-slate-400 hover:text-slate-200 hover:bg-slate-800/50'
                }`}
              >
                <Database size={15} /> Replicated SQLite Tables ({dbState.items?.length || 0})
              </button>
            </div>

            {/* DB Quick Stats */}
            <div className="hidden sm:flex items-center gap-4 text-xs font-mono text-slate-400">
              <span>Items: <strong className="text-emerald-400">{dbState.items?.length || 0}</strong></span>
              <span>Categories: <strong className="text-cyan-400">{dbState.categories?.length || 0}</strong></span>
              <span>Suppliers: <strong className="text-purple-400">{dbState.suppliers?.length || 0}</strong></span>
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
                nodeLabel={node => `${node.id}\nPeer ID: ${node.peerId || 'Local'}`}
                nodeColor={node => node.group === 1 ? '#06b6d4' : '#10b981'}
                nodeRelSize={9}
                linkColor={() => '#334155'}
                linkWidth={2}
                linkDirectionalParticles={2}
                linkDirectionalParticleSpeed={0.005}
                backgroundColor="#030712"
                d3AlphaDecay={0.03}
              />
              
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

        </div>

      </div>

    </div>
  );
}
