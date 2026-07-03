"use client";

import React, { useEffect, useRef, useState, useCallback } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import {
    ReactFlow,
    Background,
    Node,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';

// ---- Constants ----
const API_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8080';
const MAX_TASKS = 100;
const MAX_RETRIES = 5;
const RETRY_DELAY_MS = 3000;

// ---- Types ----
type TaskEvent = {
    type: string; // Pending, Processing, Completed, Failed, DLQ
    task_id: string;
    worker_id?: string;
}

type Task = {
    id: string;
    status: string;
}

type WorkerStats = {
    id: string;
    cpu: number;
    memory: number;
    status: string;
}

type BenchmarkResult = {
    name: string;
    tps: number;
    p50_latency_ms: number;
    p99_latency_ms: number;
}

type ColumnProps = {
    title: string;
    headerColor: string;
    color: string;
    tasks: Task[];
    onRetry?: () => void;
}

export default function Dashboard() {
    const [tasks, setTasks] = useState<Task[]>([]);
    const [workers, setWorkers] = useState<WorkerStats[]>([]);
    const [activeWorkerPulses, setActiveWorkerPulses] = useState<Record<string, boolean>>({});
    const [benchmarkData, setBenchmarkData] = useState<BenchmarkResult[]>([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);
    const retryCountRef = useRef(0);
    const eventSourceRef = useRef<EventSource | null>(null);

    const connectSSE = useCallback(() => {
        const eventSource = new EventSource(`${API_URL}/api/stream`);
        eventSourceRef.current = eventSource;

        eventSource.onopen = () => {
            retryCountRef.current = 0;
            setError(null);
            setLoading(false);
        };

        eventSource.onmessage = (e) => {
            const event: TaskEvent = JSON.parse(e.data);
            if (event.type === 'ping') return;

            setTasks(prev => {
                const existing = prev.find(t => t.id === event.task_id);
                if (existing) {
                    return prev.map(t => t.id === event.task_id ? { ...t, status: event.type } : t);
                } else {
                    // FIFO eviction: cap at MAX_TASKS
                    const next = [...prev, { id: event.task_id, status: event.type }];
                    if (next.length > MAX_TASKS) {
                        return next.slice(next.length - MAX_TASKS);
                    }
                    return next;
                }
            });

            if (event.worker_id) {
                setActiveWorkerPulses(prev => ({ ...prev, [event.worker_id!]: true }));
                setTimeout(() => {
                    setActiveWorkerPulses(prev => ({ ...prev, [event.worker_id!]: false }));
                }, 400);
            }
        };

        eventSource.onerror = () => {
            eventSource.close();
            eventSourceRef.current = null;

            if (retryCountRef.current < MAX_RETRIES) {
                retryCountRef.current += 1;
                setLoading(false);
                setError(`Connection lost. Retrying... (${retryCountRef.current}/${MAX_RETRIES})`);
                setTimeout(() => connectSSE(), RETRY_DELAY_MS);
            } else {
                setLoading(false);
                setError('Unable to connect after multiple attempts. Please check the server.');
            }
        };
    }, []);

    useEffect(() => {
        // Fetch initial workers
        fetch(`${API_URL}/api/workers`)
            .then(res => res.json())
            .then(data => setWorkers(data))
            .catch(console.error);

        // Connect SSE
        connectSSE();

        return () => {
            if (eventSourceRef.current) {
                eventSourceRef.current.close();
                eventSourceRef.current = null;
            }
        };
    }, [connectSSE]);

    // React Flow nodes
    const nodes: Node[] = workers.map((w, i) => ({
        id: w.id,
        position: { x: 50 + (i % 3) * 200, y: 50 + Math.floor(i / 3) * 150 },
        data: {
            label: (
                <div className={`p-4 rounded-xl shadow-xl border w-40 ${activeWorkerPulses[w.id] ? 'bg-indigo-600 border-indigo-400 drop-shadow-[0_0_15px_rgba(79,70,229,0.5)]' : 'bg-slate-800 border-slate-700'
                    } transition-all duration-300 text-slate-100 flex flex-col gap-1`}
                >
                    <div className="font-bold text-sm truncate">{w.id.replace('worker_node_1-', 'Worker ')}</div>
                    <div className="text-xs text-slate-400 flex justify-between">
                        <span>CPU</span>
                        <span className="font-mono text-indigo-300">{w.cpu.toFixed(0)}%</span>
                    </div>
                    <div className="text-xs text-slate-400 flex justify-between">
                        <span>RAM</span>
                        <span className="font-mono text-indigo-300">{w.memory.toFixed(0)}MB</span>
                    </div>
                </div>
            )
        },
        type: 'default',
        style: { backgroundColor: 'transparent', border: 'none', padding: 0 }
    }));

    const pending = tasks.filter(t => t.status === 'Pending').slice(-20); // Keep max 20 pending items visually for perf
    const processing = tasks.filter(t => t.status === 'Processing');
    const dlq = tasks.filter(t => t.status === 'DLQ');

    const handleRetryDLQ = async () => {
        try {
            await fetch(`${API_URL}/api/retry-dlq`, { method: 'POST' });
        } catch (e) { console.error(e) }
    };

    if (loading) {
        return (
            <div className="min-h-screen bg-slate-950 text-slate-100 p-8 font-sans flex items-center justify-center">
                <div className="text-center">
                    <div className="animate-pulse text-2xl font-semibold text-slate-300">Connecting...</div>
                    <p className="text-slate-500 mt-2 text-sm">Waiting for server response</p>
                </div>
            </div>
        );
    }

    if (error) {
        return (
            <div className="min-h-screen bg-slate-950 text-slate-100 p-8 font-sans flex items-center justify-center">
                <div className="text-center">
                    <div className="text-2xl font-semibold text-red-400">Connection Error</div>
                    <p className="text-slate-400 mt-2 text-sm">{error}</p>
                </div>
            </div>
        );
    }

    return (
        <div className="min-h-screen bg-slate-950 text-slate-100 p-8 font-sans selection:bg-indigo-500 selection:text-white">
            <div className="max-w-7xl mx-auto">
                <header className="mb-10 flex justify-between items-end">
                    <div>
                        <h1 className="text-4xl font-extrabold bg-clip-text text-transparent bg-gradient-to-r from-indigo-400 to-cyan-400 tracking-tight">
                            Buraq Dashboard
                        </h1>
                        <p className="text-slate-400 mt-2 text-sm font-medium">Enterprise Go Task Queue Visualizer</p>
                    </div>

                    <div className="flex gap-4">
                        {benchmarkData.length > 0 ? (
                            benchmarkData.map((b, i) => (
                                <div key={i} className="bg-slate-900 border border-slate-800 rounded-lg p-3 px-5 flex items-center gap-4">
                                    <div>
                                        <div className="text-xs text-slate-500 uppercase tracking-wider font-semibold">Throughput</div>
                                        <div className="text-xl font-bold text-white">{b.tps.toFixed(0)} <span className="text-sm font-normal text-slate-400">TPS</span></div>
                                    </div>
                                    <div className="w-px h-8 bg-slate-800 mx-2"></div>
                                    <div>
                                        <div className="text-xs text-slate-500 uppercase tracking-wider font-semibold">P99 Latency</div>
                                        <div className="text-xl font-bold text-cyan-400">{b.p99_latency_ms.toFixed(1)} <span className="text-sm font-normal text-slate-400">ms</span></div>
                                    </div>
                                </div>
                            ))
                        ) : (
                            <div className="bg-slate-900 border border-slate-800 rounded-lg p-3 px-5 flex items-center gap-4">
                                <div>
                                    <div className="text-xs text-slate-500 uppercase tracking-wider font-semibold">Throughput</div>
                                    <div className="text-xl font-bold text-slate-500">-- <span className="text-sm font-normal text-slate-600">TPS</span></div>
                                </div>
                                <div className="w-px h-8 bg-slate-800 mx-2"></div>
                                <div>
                                    <div className="text-xs text-slate-500 uppercase tracking-wider font-semibold">P99 Latency</div>
                                    <div className="text-xl font-bold text-slate-500">-- <span className="text-sm font-normal text-slate-600">ms</span></div>
                                </div>
                            </div>
                        )}
                    </div>
                </header>

                <div className="grid grid-cols-1 lg:grid-cols-4 gap-6 mb-8">
                    <Column title="Pending" color="border-slate-800" headerColor="text-slate-200" tasks={pending} />
                    <Column title="Processing" color="border-indigo-900/50" headerColor="text-indigo-400" tasks={processing} />
                    <Column title="Dead Letter Queue" color="border-red-900/40" headerColor="text-red-400" tasks={dlq} onRetry={handleRetryDLQ} />
                </div>

                <div className="grid gap-6">
                    <div className="h-[450px] bg-slate-900/80 backdrop-blur-sm rounded-2xl p-6 border border-slate-800/80 relative overflow-hidden">
                        <h2 className="text-xl font-semibold mb-2 text-white">Interactive Worker Map</h2>
                        <p className="text-xs text-slate-400 mb-4">Real-time resource utilization and pulse events</p>
                        <div className="absolute inset-0 top-[88px]">
                            <ReactFlow nodes={nodes} fitView colorMode="dark" minZoom={0.5} maxZoom={1.5} attributionPosition="bottom-right">
                                <Background color="#334155" gap={20} size={1} />
                            </ReactFlow>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
}

function Column({ title, headerColor, color, tasks, onRetry }: ColumnProps) {
    return (
        <div className={`bg-slate-900/50 rounded-2xl p-5 border ${color} flex flex-col h-[500px]`}>
            <div className="flex justify-between items-center mb-5 mt-1 border-b border-slate-800/80 pb-4">
                <h2 className={`text-lg font-semibold ${headerColor} flex items-center gap-2`}>
                    {title}
                    <span className="bg-slate-800 text-xs px-2 py-0.5 rounded-full text-slate-300 font-mono">{tasks.length}</span>
                </h2>
                {onRetry && (
                    <button
                        onClick={onRetry}
                        className="text-xs bg-indigo-600/20 text-indigo-300 hover:bg-indigo-600/40 hover:text-white border border-indigo-500/30 transition-all font-medium px-3 py-1.5 rounded-md"
                    >
                        Retry All
                    </button>
                )}
            </div>
            <div className="flex flex-col gap-3 overflow-y-auto pr-2 custom-scrollbar flex-1 relative">
                <AnimatePresence>
                    {tasks.length === 0 && (
                        <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} className="absolute inset-0 flex items-center justify-center text-slate-600 text-sm italic">
                            No tasks
                        </motion.div>
                    )}
                    {tasks.map((t) => (
                        <motion.div
                            key={t.id}
                            layout
                            initial={{ opacity: 0, scale: 0.95, y: -10 }}
                            animate={{ opacity: 1, scale: 1, y: 0 }}
                            exit={{ opacity: 0, scale: 0.95, transition: { duration: 0.2 } }}
                            className="bg-slate-800/80 hover:bg-slate-700/80 p-3.5 rounded-xl shadow-sm border border-slate-700/50 flex justify-between items-center transition-colors group"
                        >
                            <span className="font-mono text-sm text-slate-300 truncate mr-2" title={t.id}>{t.id}</span>
                            <span className="w-2 h-2 rounded-full bg-slate-600 group-hover:bg-indigo-400 transition-colors"></span>
                        </motion.div>
                    ))}
                </AnimatePresence>
            </div>
        </div>
    );
}
