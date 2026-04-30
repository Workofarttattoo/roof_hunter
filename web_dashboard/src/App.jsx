import React, { useState, useEffect, useMemo } from 'react';
import axios from 'axios';
import {
  Search, MapPin, Wind, AlertTriangle, Phone, Mail,
  ShieldCheck, MessageSquare, ShoppingBag, Zap, LayoutDashboard, Lock, ChevronRight,
  Calendar, Layers, CheckCircle2, XCircle, Clock, Crosshair,
  Video, FileText, ScanLine, Play, X, Quote, Plane, Eye, Sparkles, RefreshCw, Cloud,
  BarChart3, Ticket, LogIn, LogOut, UserPlus, CreditCard, User,
} from 'lucide-react';
import './App.css';

/** Same-origin `/api` when empty (Ingress splits / and /api). Local dev: `VITE_API_BASE=http://localhost:8000` */
const API_BASE = import.meta.env.VITE_API_BASE ?? '';
const AUTH_TOKEN_KEY = 'rh_auth_token';

function resolveAssetUrl(path) {
  if (!path) return undefined;
  if (path.startsWith('http')) return path;
  return `${API_BASE}${path}`;
}

function formatMoney(n) {
  return new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 }).format(n);
}

function defaultEstimate(lead) {
  const base = lead?.damage_score >= 40 ? 28500 : 16200;
  return {
    roofReplacement: base,
    gutters: 2400,
    skylightUpsell: 0,
    ventilation: 890,
  };
}

function bandLabel(band) {
  if (band === 'high') return 'High damage (≥40)';
  if (band === 'medium') return 'Medium (15–39)';
  return 'Low (<15)';
}

function App() {
  const [marketLeads, setMarketLeads] = useState([]);
  const [selectedState, setSelectedState] = useState('ALL');
  const [activeTab, setActiveTab] = useState('marketplace');
  const [searchQuery, setSearchQuery] = useState('');
  const [zipFilter, setZipFilter] = useState('');
  const [materialFilter, setMaterialFilter] = useState('ALL');
  const [isQualifying, setIsQualifying] = useState(null);
  const [selectedLead, setSelectedLead] = useState(null);
  const [showTranscript, setShowTranscript] = useState(false);
  const [provisioningNumber, setProvisioningNumber] = useState(false);
  const [workspaceLead, setWorkspaceLead] = useState(null);
  const [estimate, setEstimate] = useState(null);
  const [highlightDamage, setHighlightDamage] = useState(true);
  const [proAccount, setProAccount] = useState(false);
  const [flyoverNotes, setFlyoverNotes] = useState('');
  const [serverStats, setServerStats] = useState(null);
  const [awsSyncing, setAwsSyncing] = useState(false);
  const [authToken, setAuthToken] = useState(() => localStorage.getItem(AUTH_TOKEN_KEY) || '');
  const [sessionUser, setSessionUser] = useState(null);
  const [authModalOpen, setAuthModalOpen] = useState(false);
  const [authMode, setAuthMode] = useState('login');
  const [authEmail, setAuthEmail] = useState('');
  const [authUsername, setAuthUsername] = useState('');
  const [authPassword, setAuthPassword] = useState('');
  const [authError, setAuthError] = useState('');
  const [metricsTier, setMetricsTier] = useState(null);
  const [metricsStateFilter, setMetricsStateFilter] = useState('ALL');
  const [adminLeads, setAdminLeads] = useState([]);
  const [adminOps, setAdminOps] = useState(null);
  const [adminTickets, setAdminTickets] = useState([]);
  const [adminSubTab, setAdminSubTab] = useState('leads');
  const [ticketForm, setTicketForm] = useState({ title: '', body: '', lead_id: '', priority: 'normal' });
  const [squareLoading, setSquareLoading] = useState(null);

  const MIDDLEMAN_SETUP_FEE = 5.0;
  const MIDDLEMAN_MONTHLY_FEE = 15.0;

  const fetchLeads = () => {
    const params = new URLSearchParams();
    if (searchQuery) params.append('q', searchQuery);
    if (zipFilter) params.append('zip', zipFilter);
    if (materialFilter !== 'ALL') params.append('material', materialFilter);

    fetch(`${API_BASE}/api/leads/teasers?${params.toString()}`)
      .then((res) => res.json())
      .then((data) => {
        if (Array.isArray(data)) setMarketLeads(data);
      })
      .catch((err) => console.error('Marketplace fetch error:', err));
  };

  useEffect(() => {
    fetchLeads();
  }, [searchQuery, zipFilter, materialFilter]);

  const fetchStats = () => {
    fetch(`${API_BASE}/api/stats`)
      .then((res) => (res.ok ? res.json() : null))
      .then((data) => {
        if (data && typeof data.total_leads === 'number') setServerStats(data);
      })
      .catch(() => setServerStats(null));
  };

  useEffect(() => {
    fetchStats();
  }, []);

  useEffect(() => {
    if (!authToken) {
      setSessionUser(null);
      return;
    }
    fetch(`${API_BASE}/api/me`, { headers: { Authorization: `Bearer ${authToken}` } })
      .then((res) => (res.ok ? res.json() : Promise.reject(new Error('session'))))
      .then(setSessionUser)
      .catch(() => {
        setSessionUser(null);
        setAuthToken('');
        localStorage.removeItem(AUTH_TOKEN_KEY);
      });
  }, [authToken]);

  useEffect(() => {
    const q = metricsStateFilter === 'ALL' ? '' : `?state=${encodeURIComponent(metricsStateFilter)}`;
    fetch(`${API_BASE}/api/metrics/damage-tiers${q}`)
      .then((res) => (res.ok ? res.json() : null))
      .then((data) => setMetricsTier(data))
      .catch(() => setMetricsTier(null));
  }, [metricsStateFilter]);

  useEffect(() => {
    if (activeTab !== 'admin' || !authToken || sessionUser?.role !== 'admin') return undefined;
    const headers = { Authorization: `Bearer ${authToken}` };
    let cancelled = false;
    Promise.all([
      fetch(`${API_BASE}/api/admin/leads`, { headers }).then((r) => r.json()),
      fetch(`${API_BASE}/api/admin/operations-summary`, { headers }).then((r) => r.json()),
      fetch(`${API_BASE}/api/admin/tickets`, { headers }).then((r) => r.json()),
    ])
      .then(([leadsDoc, opsDoc, ticketsDoc]) => {
        if (cancelled) return;
        setAdminLeads(leadsDoc.leads || []);
        setAdminOps(opsDoc);
        setAdminTickets(ticketsDoc.tickets || []);
      })
      .catch(() => {
        if (!cancelled) {
          setAdminLeads([]);
          setAdminOps(null);
          setAdminTickets([]);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [activeTab, authToken, sessionUser?.role]);

  const handleAwsSync = async () => {
    setAwsSyncing(true);
    try {
      const headers = { 'Content-Type': 'application/json' };
      const key = import.meta.env.VITE_SYNC_API_KEY;
      if (key) headers['X-Roof-Hunter-Sync'] = key;
      const res = await fetch(`${API_BASE}/api/leads/sync-aws`, {
        method: 'POST',
        headers,
        body: JSON.stringify({}),
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        throw new Error(
          typeof data.detail === 'string' ? data.detail : JSON.stringify(data.detail) || res.statusText || 'Sync failed',
        );
      }
      alert(`AWS S3 sync: ${data.imported} imported, ${data.skipped} skipped.`);
      fetchStats();
      fetchLeads();
    } catch (err) {
      console.error(err);
      alert(err.message || 'AWS sync failed. Configure AWS_LEADS_BUCKET on the API.');
    } finally {
      setAwsSyncing(false);
    }
  };

  const persistAuth = (token) => {
    if (token) localStorage.setItem(AUTH_TOKEN_KEY, token);
    else localStorage.removeItem(AUTH_TOKEN_KEY);
    setAuthToken(token || '');
  };

  const handleAuthSubmit = async (e) => {
    e.preventDefault();
    setAuthError('');
    const path = authMode === 'register' ? '/api/auth/register' : '/api/auth/login';
    const body =
      authMode === 'register'
        ? { email: authEmail, username: authUsername, password: authPassword }
        : { email: authEmail, password: authPassword };
    try {
      const res = await fetch(`${API_BASE}${path}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        const msg = typeof data.detail === 'string' ? data.detail : JSON.stringify(data.detail || res.statusText);
        throw new Error(msg);
      }
      persistAuth(data.token);
      setSessionUser(data.user);
      setAuthModalOpen(false);
      setAuthPassword('');
    } catch (err) {
      setAuthError(err.message || 'Authentication failed');
    }
  };

  const handleLogout = () => {
    persistAuth('');
    setSessionUser(null);
  };

  const handleSquareCheckout = async (lead) => {
    if (!authToken) {
      setAuthMode('login');
      setAuthModalOpen(true);
      setAuthError('Sign in to pay with Square.');
      return;
    }
    const amount = Number(lead?.list_price_usd) > 0 ? Number(lead.list_price_usd) : 149;
    const desc =
      lead != null
        ? `Ridgeline unlock #${lead.id} · ${lead.zip_code || ''}`.slice(0, 256)
        : 'Ridgeline account credit';
    setSquareLoading(lead != null ? lead.id : 'acct');
    try {
      const res = await fetch(`${API_BASE}/api/payments/square/checkout-link`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${authToken}`,
        },
        body: JSON.stringify({
          amount_usd: amount,
          description: desc,
          buyer_email: sessionUser?.email,
        }),
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        const msg = typeof data.detail === 'string' ? data.detail : JSON.stringify(data.detail || res.statusText);
        throw new Error(msg);
      }
      if (data.url) window.open(data.url, '_blank', 'noopener,noreferrer');
      else alert('Square returned no URL — check SQUARE_ACCESS_TOKEN / SQUARE_LOCATION_ID on the API.');
    } catch (err) {
      alert(err.message || 'Square checkout failed');
    } finally {
      setSquareLoading(null);
    }
  };

  const submitAdminTicket = async (ev) => {
    ev.preventDefault();
    if (!authToken || sessionUser?.role !== 'admin') return;
    try {
      const res = await fetch(`${API_BASE}/api/admin/tickets`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${authToken}`,
        },
        body: JSON.stringify({
          title: ticketForm.title,
          body: ticketForm.body,
          lead_id: ticketForm.lead_id ? parseInt(ticketForm.lead_id, 10) : null,
          priority: ticketForm.priority,
        }),
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(typeof data.detail === 'string' ? data.detail : JSON.stringify(data.detail));
      setTicketForm({ title: '', body: '', lead_id: '', priority: 'normal' });
      const list = await fetch(`${API_BASE}/api/admin/tickets`, {
        headers: { Authorization: `Bearer ${authToken}` },
      }).then((r) => r.json());
      setAdminTickets(list.tickets || []);
      alert(`Ticket #${data.id} created. Slack/Discord notified if webhooks are set.`);
    } catch (err) {
      alert(err.message || 'Ticket failed');
    }
  };

  useEffect(() => {
    if (workspaceLead) setEstimate(defaultEstimate(workspaceLead));
    else setEstimate(null);
  }, [workspaceLead]);

  const handleQualify = async (leadId) => {
    setIsQualifying(leadId);
    try {
      await axios.post(`${API_BASE}/api/leads/qualify`, { lead_id: leadId });
      fetchLeads();
      fetchStats();
    } catch (err) {
      console.error('Qualify error:', err);
    } finally {
      setIsQualifying(null);
    }
  };

  const handleProvisionNumber = () => {
    setProvisioningNumber(true);
    setTimeout(() => {
      alert('Middleman Service: New local number provisioned. $5.00 setup fee applied to account.');
      setProvisioningNumber(false);
    }, 2000);
  };

  const groupedLeads = marketLeads.reduce((acc, lead) => {
    const state = lead.state || 'UNKNOWN';
    if (!acc[state]) acc[state] = [];
    acc[state].push(lead);
    return acc;
  }, {});

  const availableStates = Object.keys(groupedLeads).sort();
  const stateTabs = ['ALL', ...availableStates];

  const aggregateStats = useMemo(() => {
    const n = marketLeads.length;
    const high = marketLeads.filter((l) => (l.damage_score || 0) >= 40).length;
    const qualified = marketLeads.filter((l) => l.qualification_status === 'QUALIFIED').length;
    return { n, high, qualified };
  }, [marketLeads]);

  const estimateTotal = estimate
    ? estimate.roofReplacement + estimate.gutters + estimate.skylightUpsell + estimate.ventilation
    : 0;

  const getStatusIcon = (status) => {
    switch (status) {
      case 'QUALIFIED':
        return <CheckCircle2 size={12} className="status-icon-plus success" />;
      case 'REJECTED':
        return <XCircle size={12} className="status-icon-plus error" />;
      case 'NO_ANSWER':
        return <Clock size={12} className="status-icon-plus warning" />;
      default:
        return <Clock size={12} className="status-icon-plus pending" />;
    }
  };

  const openWorkspace = (lead, e) => {
    if (e) e.stopPropagation();
    setWorkspaceLead(lead);
  };

  const sendEstimateStub = () => {
    alert(
      `Estimate summary (${formatMoney(estimateTotal)}) queued for client email and SMS. Connect CRM webhook to deliver automatically.`,
    );
  };

  return (
    <div className="ridgeline-app ridgeline-pro">
      <header className="app-header glass">
        <div className="logo">
          <div className="logo-mark" aria-hidden>
            <ScanLine className="logo-icon" />
          </div>
          <div className="logo-text">
            <h1>Ridgeline</h1>
            <span className="beta-tag pro-tag">ridgeline.ai · Lead intelligence · Forensic imagery</span>
          </div>
        </div>
        <nav className="nav-tabs" aria-label="Primary">
          <button
            type="button"
            className={`nav-btn ${activeTab === 'marketplace' ? 'active' : ''}`}
            onClick={() => setActiveTab('marketplace')}
          >
            <ShoppingBag size={18} /> Lead marketplace
          </button>
          <button
            type="button"
            className={`nav-btn ${activeTab === 'command' ? 'active' : ''}`}
            onClick={() => setActiveTab('command')}
          >
            <LayoutDashboard size={18} /> Command center
          </button>
          <button
            type="button"
            className={`nav-btn ${activeTab === 'flyover' ? 'active' : ''}`}
            onClick={() => setActiveTab('flyover')}
          >
            <Plane size={18} /> Pro flyover
          </button>
          {sessionUser?.role === 'admin' && (
            <button
              type="button"
              className={`nav-btn ${activeTab === 'admin' ? 'active' : ''}`}
              onClick={() => setActiveTab('admin')}
            >
              <LayoutDashboard size={18} /> Ops / admin
            </button>
          )}
        </nav>
        <div className="header-account">
          <button
            type="button"
            className={`account-chip ${proAccount ? 'on' : ''}`}
            onClick={() => setProAccount((v) => !v)}
            title="Toggle demo Pro account (UAV on-demand)"
          >
            <ShieldCheck size={14} />
            {proAccount ? 'Pro account' : 'Preview as guest'}
          </button>
          {sessionUser ? (
            <>
              <span className="account-chip subtle" title={sessionUser.email}>
                <User size={14} /> {sessionUser.username}
              </span>
              <button type="button" className="account-chip" onClick={handleLogout}>
                <LogOut size={14} /> Sign out
              </button>
            </>
          ) : (
            <button
              type="button"
              className="account-chip primary-lite"
              onClick={() => {
                setAuthMode('login');
                setAuthModalOpen(true);
                setAuthError('');
              }}
            >
              <LogIn size={14} /> Sign in
            </button>
          )}
        </div>
      </header>

      <main className="main-content">
        {activeTab === 'marketplace' && (
          <section className="marketplace-section fade-in">
            <div className="info-hero glass-premium">
              <div className="info-hero-copy">
                <h2 className="info-hero-title">Sell roofs with evidence, not guesses</h2>
                <p className="info-hero-sub">
                  Search verified leads, review damage in paired imagery, send structured estimates, and show homeowners a
                  short flyover mockup plus optional full walkthrough before they buy.
                </p>
                <ul className="info-hero-list">
                  <li>
                    <Eye size={16} /> Photo-forward damage review with AI callouts
                  </li>
                  <li>
                    <FileText size={16} /> One-click estimate packages for email &amp; SMS
                  </li>
                  <li>
                    <Video size={16} /> 5s roof mockup reel + gated pre-purchase walkthrough
                  </li>
                  <li>
                    <Plane size={16} /> Account Pro: low-altitude UAV scans to surface issues early
                  </li>
                </ul>
              </div>
              <div className="info-hero-stats">
                <div className="stat-block">
                  <span className="stat-value">{serverStats != null ? serverStats.total_leads : '—'}</span>
                  <span className="stat-label">Total in database</span>
                </div>
                <div className="stat-block">
                  <span className="stat-value accent-warn">
                    {serverStats != null ? serverStats.high_severity : '—'}
                  </span>
                  <span className="stat-label">High-severity (≥40)</span>
                </div>
                <div className="stat-block">
                  <span className="stat-value accent-ok">{serverStats != null ? serverStats.qualified : '—'}</span>
                  <span className="stat-label">Qualified (AI)</span>
                </div>
                <div className="stat-block">
                  <span className="stat-value">{serverStats != null ? serverStats.from_aws_storms : '—'}</span>
                  <span className="stat-label">AWS S3 pipeline storms</span>
                </div>
              </div>
              {serverStats?.last_aws_sync && (
                <p className="hero-sync-meta">
                  <Cloud size={14} /> Last S3 ingest: {new Date(serverStats.last_aws_sync).toLocaleString()}
                  {serverStats.last_aws_import_count != null && ` · +${serverStats.last_aws_import_count} rows`}
                </p>
              )}
              <p className="hero-filter-meta">
                Showing <strong>{aggregateStats.n}</strong> leads in current filters (API returns up to 100).
              </p>

              <div className="damage-infographic glass-inset">
                <div className="info-graphics-head">
                  <BarChart3 size={24} className="tier-chart-icon" aria-hidden />
                  <div>
                    <h3 className="info-graphics-title">Damage mix · infographic</h3>
                    <p className="info-graphics-desc">
                      <strong>High</strong> ≥40 · <strong>Medium</strong> 15–39 · <strong>Low</strong> &lt;15 on the forensic
                      damage index. ZIP-tier multipliers apply at checkout (platinum / gold corridors).
                    </p>
                  </div>
                </div>
                {metricsTier?.summary ? (
                  <>
                    <div className="tier-bar-track" role="img" aria-label="Damage tier distribution">
                      {metricsTier.summary.pct_high > 0 && (
                        <div
                          className="tier-seg high"
                          style={{ flex: Math.max(metricsTier.summary.pct_high, 0.5) }}
                          title={`High: ${metricsTier.summary.high}`}
                        />
                      )}
                      {metricsTier.summary.pct_medium > 0 && (
                        <div className="tier-seg medium" style={{ flex: Math.max(metricsTier.summary.pct_medium, 0.5) }} />
                      )}
                      {metricsTier.summary.pct_low > 0 && (
                        <div className="tier-seg low" style={{ flex: Math.max(metricsTier.summary.pct_low, 0.5) }} />
                      )}
                    </div>
                    <ul className="tier-legend">
                      <li>
                        <span className="tier-dot high" /> High {metricsTier.summary.high}{' '}
                        <span className="pct">({metricsTier.summary.pct_high}%)</span>
                      </li>
                      <li>
                        <span className="tier-dot medium" /> Medium {metricsTier.summary.medium}{' '}
                        <span className="pct">({metricsTier.summary.pct_medium}%)</span>
                      </li>
                      <li>
                        <span className="tier-dot low" /> Low {metricsTier.summary.low}{' '}
                        <span className="pct">({metricsTier.summary.pct_low}%)</span>
                      </li>
                      <li className="tier-total">Total {metricsTier.summary.total} contacts</li>
                    </ul>
                  </>
                ) : (
                  <p className="muted small">Loading tier breakdown…</p>
                )}
                {metricsTier?.filter_state && metricsTier.filter_state !== 'ALL' && (
                  <p className="metrics-filter-tag">Chart filtered: <strong>{metricsTier.filter_state}</strong></p>
                )}
                {metricsTier?.by_state?.length > 0 && (
                  <div className="state-mix-mini">
                    <span className="muted small">State lens (chart only)</span>
                    <div className="state-pills">
                      <button
                        type="button"
                        className={metricsStateFilter === 'ALL' ? 'active' : ''}
                        onClick={() => setMetricsStateFilter('ALL')}
                      >
                        All
                      </button>
                      {metricsTier.by_state.slice(0, 14).map((s) => (
                        <button
                          key={s.state}
                          type="button"
                          className={metricsStateFilter === s.state ? 'active' : ''}
                          onClick={() => setMetricsStateFilter(s.state)}
                        >
                          {s.state}
                        </button>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            </div>

            <div className="section-header-block">
              <div className="title-block">
                <h2>
                  <ShoppingBag size={28} className="title-icon" /> National lead marketplace
                </h2>
                <p>Forensic-grade teasers. Open a lead workspace to compose estimates and preview client-facing video assets.</p>
              </div>

              <div className="search-filter-bar glass-premium">
                <div className="search-input-wrapper">
                  <Search className="search-icon" size={18} />
                  <input
                    type="search"
                    placeholder="Search city, state, ZIP, or neighborhood…"
                    value={searchQuery}
                    onChange={(e) => setSearchQuery(e.target.value)}
                    className="premium-search-input"
                    autoComplete="off"
                  />
                </div>

                <div className="filters-group">
                  <div className="filter-item">
                    <MapPin size={16} />
                    <input
                      type="text"
                      inputMode="numeric"
                      placeholder="ZIP"
                      value={zipFilter}
                      onChange={(e) => setZipFilter(e.target.value)}
                      className="filter-input-small"
                    />
                  </div>

                  <div className="filter-item">
                    <Layers size={16} />
                    <select
                      value={materialFilter}
                      onChange={(e) => setMaterialFilter(e.target.value)}
                      className="filter-select"
                    >
                      <option value="ALL">All materials</option>
                      <option value="Metal">Metal</option>
                      <option value="Asphalt">Asphalt</option>
                      <option value="Tile">Tile</option>
                    </select>
                  </div>
                </div>
              </div>

              <div className="state-selector-row">
                <div className="state-filters glass">
                  {stateTabs.map((s) => (
                    <button
                      key={s}
                      type="button"
                      className={`state-tab ${selectedState === s ? 'active' : ''}`}
                      onClick={() => {
                        setSelectedState(s);
                      }}
                    >
                      {s}
                    </button>
                  ))}
                </div>
              </div>
            </div>

            <div className="leads-grid">
              {Object.entries(groupedLeads)
                .filter(([state]) => selectedState === 'ALL' || selectedState === state)
                .map(([state, leads]) => (
                  <div key={state} className="state-group">
                    <h3 className="state-divider">
                      <span>{state}</span>
                      <span className="count-pill">{leads.length} available</span>
                    </h3>
                    <div className="cards-container">
                      {leads.map((lead) => (
                        <article
                          key={lead.id}
                          className={`lead-card glass-premium band-${lead.damage_band || 'low'} ${(lead.damage_score || 0) >= 40 ? 'severity-high' : ''}`}
                          role="button"
                          tabIndex={0}
                          onClick={() => openWorkspace(lead)}
                          onKeyDown={(e) => {
                            if (e.key === 'Enter' || e.key === ' ') {
                              e.preventDefault();
                              openWorkspace(lead);
                            }
                          }}
                        >
                          <div className="card-top-row">
                            <div className="badges-group">
                              <div className={`status-badge tier-${lead.zip_tier || 'standard'}`}>
                                {lead.zip_tier || 'standard'} · {formatMoney(lead.list_price_usd ?? 149)}
                              </div>
                              <div className={`damage-band-pill band-${lead.damage_band || 'low'}`}>
                                {bandLabel(lead.damage_band)}
                              </div>
                              <div
                                className={`qual-badge ${
                                  lead.qualification_status?.toLowerCase().replace('_', '-') || 'pending'
                                }`}
                              >
                                {getStatusIcon(lead.qualification_status)}
                                {lead.qualification_status || 'Pending AI call'}
                              </div>
                            </div>
                            <div className="storm-stat">
                              <Wind size={14} /> {(lead.magnitude || 0).toFixed(2)}″ hail
                            </div>
                          </div>

                          <div className="damage-viz">
                            <div className="damage-ring">
                              <svg viewBox="0 0 36 36" className="circular-chart">
                                <path
                                  className="circle-bg"
                                  d="M18 2.0845 a 15.9155 15.9155 0 0 1 0 31.831 a 15.9155 15.9155 0 0 1 0 -31.831"
                                />
                                <path
                                  className={`circle ${lead.damage_score >= 40 ? 'loss-color' : 'platinum-color'}`}
                                  strokeDasharray={`${lead.damage_score || 0}, 100`}
                                  d="M18 2.0845 a 15.9155 15.9155 0 0 1 0 31.831 a 15.9155 15.9155 0 0 1 0 -31.831"
                                />
                                <text x="18" y="20.35" className="percentage">
                                  {(lead.damage_score || 0).toFixed(0)}%
                                </text>
                              </svg>
                            </div>
                            <div className="viz-label">Model-estimated damage index</div>
                          </div>

                          {lead.image_url_before && (
                            <div className="forensic-imagery glass-inset">
                              <div className="imagery-grid">
                                <div className="img-container">
                                  <img src={resolveAssetUrl(lead.image_url_before)} alt="Pre-storm reference" onError={(e) => { e.target.style.display = 'none'; }} />
                                  <span className="img-label">Pre-storm</span>
                                </div>
                                <div className="img-container">
                                  <img src={resolveAssetUrl(lead.image_url_after)} alt="Post-storm with flags" onError={(e) => { e.target.style.display = 'none'; }} />
                                  <span className="img-label highlighted">Post-storm · AI flag</span>
                                </div>
                              </div>
                              {lead.image_findings && (
                                <div className="forensic-tag">
                                  <Crosshair size={10} /> {lead.image_findings}
                                </div>
                              )}
                            </div>
                          )}

                          {(lead.verification_images?.length > 0 ||
                            lead.verification_text?.length > 0 ||
                            lead.call_pitch_why) && (
                            <div className="verification-chain glass-inset">
                              <div className="verification-chain-head">
                                <Crosshair size={12} /> Verification ↔ dialer
                              </div>
                              {lead.call_pitch_why && (
                                <p className="verification-pitch">{lead.call_pitch_why}</p>
                              )}
                              {lead.verification_images?.map((v) => (
                                <div key={`${v.url}-${v.role}`} className="verification-item">
                                  {v.url ? (
                                    <img
                                      src={resolveAssetUrl(v.url)}
                                      alt={v.role || 'verification'}
                                      onError={(e) => {
                                        e.target.style.display = 'none';
                                      }}
                                    />
                                  ) : null}
                                  <div className="verification-copy">
                                    <span className="verification-role">{v.role || 'asset'}</span>
                                    {v.why && <p>{v.why}</p>}
                                    {v.call_hook && <p className="verification-hook">{v.call_hook}</p>}
                                  </div>
                                </div>
                              ))}
                              {lead.verification_text?.map((t) => (
                                <div key={`${t.type}-${(t.rationale || '').slice(0, 24)}`} className="verification-item text-only">
                                  <div className="verification-copy">
                                    <span className="verification-role">{t.type || 'text'}</span>
                                    {t.rationale && <p>{t.rationale}</p>}
                                    {t.call_hook && <p className="verification-hook">{t.call_hook}</p>}
                                  </div>
                                </div>
                              ))}
                            </div>
                          )}

                          <div className="lead-details">
                            <div className="address-teaser">
                              <Lock size={14} className="lock-icon" />
                              <span>
                                {lead.redacted_address}, {lead.city}
                              </span>
                            </div>

                            <div className="lead-traits">
                              <div className="trait">
                                <MapPin size={12} /> {lead.zip_code || lead.zipcode}
                              </div>
                              <div className="trait">
                                <Layers size={12} /> {lead.material_interest || 'Replace'}
                              </div>
                              <div className="trait">
                                <Calendar size={12} /> {lead.timeline || 'Within 6 months'}
                              </div>
                            </div>

                            {lead.call_log ? (
                              <div className="call-log-box glass-inset">
                                <div className="log-header">
                                  <Phone size={10} /> AI agent call log
                                </div>
                                <p>{lead.call_log}</p>
                              </div>
                            ) : (
                              <div className="proof-box glass-inset">
                                <MessageSquare size={12} className="quote-icon" />
                                <p>{lead.proof_msg || 'Catastrophic structural disruption verified via spectral imagery scan.'}</p>
                              </div>
                            )}
                          </div>

                          <div className="actions-row">
                            <button
                              type="button"
                              className="workspace-btn"
                              onClick={(e) => openWorkspace(lead, e)}
                            >
                              <Sparkles size={14} /> Workspace
                            </button>
                            {lead.qualification_status === 'PENDING' ? (
                              <button
                                type="button"
                                className="qualify-btn"
                                onClick={(e) => {
                                  e.stopPropagation();
                                  handleQualify(lead.id);
                                }}
                                disabled={isQualifying === lead.id}
                              >
                                {isQualifying === lead.id ? <span className="pulse-mini" /> : <Phone size={14} />}
                                AI qualify
                              </button>
                            ) : null}
                            <button
                              type="button"
                              className="purchase-btn-neon"
                              onClick={(e) => {
                                e.stopPropagation();
                                handleSquareCheckout(lead);
                              }}
                              disabled={squareLoading === lead.id}
                            >
                              {squareLoading === lead.id ? <span className="pulse-mini" /> : <ChevronRight size={20} />}
                            Unlock · {formatMoney(lead.list_price_usd ?? 149)}
                            </button>
                          </div>
                        </article>
                      ))}
                    </div>
                  </div>
                ))}

              {marketLeads.length === 0 && (
                <div className="no-results glass">
                  <AlertTriangle size={48} />
                  <h3>No leads match these filters.</h3>
                  <p>Widen search or clear ZIP to see national inventory.</p>
                </div>
              )}
            </div>
          </section>
        )}

        {activeTab === 'command' && (
          <div className="dashboard-command fade-in glass">
            <h2>Command center</h2>
            <p className="command-lead">
              Operations view for provisioning, infra, and middleman telephony. Marketplace buyers use the{' '}
              <strong>Workspace</strong> panel on each lead for estimates and video previews.
            </p>
            <div className="aws-sync-panel glass-premium">
              <div className="offer-text">
                <h3>
                  <Cloud size={20} /> AWS S3 lead ingest
                </h3>
                <p>
                  Pull new rows from <code>AWS_LEADS_BUCKET</code> / <code>AWS_LEADS_OBJECT_KEY</code> into the forensic DB.
                  Header stats and marketplace refresh after sync. Use IAM role or keys on the API service. In Kubernetes,{' '}
                  <code>k8s/sync-cronjob.yaml</code> runs a daily POST to this endpoint (06:00 UTC).
                </p>
                {serverStats?.last_aws_sync && (
                  <p className="muted small">
                    Last ingest: {new Date(serverStats.last_aws_sync).toLocaleString()} · DB total {serverStats.total_leads}
                  </p>
                )}
              </div>
              <button
                type="button"
                className="provision-btn"
                onClick={handleAwsSync}
                disabled={awsSyncing}
              >
                {awsSyncing ? (
                  <>
                    <RefreshCw size={18} className="spin" /> Syncing…
                  </>
                ) : (
                  <>
                    <RefreshCw size={18} /> Sync from S3
                  </>
                )}
              </button>
            </div>
            <div className="middleman-offer glass-premium">
              <div className="offer-text">
                <h3>
                  <Zap size={20} /> Resell local numbers
                </h3>
                <p>
                  Provision dedicated lines for your clients. <strong>${MIDDLEMAN_SETUP_FEE} setup</strong> + $
                  {MIDDLEMAN_MONTHLY_FEE}/mo maintenance.
                </p>
              </div>
              <button type="button" className="provision-btn" onClick={handleProvisionNumber} disabled={provisioningNumber}>
                {provisioningNumber ? 'Contacting carrier…' : 'Provision new line'}
              </button>
            </div>

            <div className="active-deployment-status glass-inset">
              <h3>
                <Layers size={16} /> Kubernetes readiness
              </h3>
              <p>
                Namespace isolation recommended for batch workers. Echo AI collocated: interference risk{' '}
                <strong>0%</strong> with dedicated workload identity.
              </p>
            </div>
          </div>
        )}

        {activeTab === 'admin' && sessionUser?.role !== 'admin' && (
          <section className="admin-gate fade-in glass">
            <Lock size={40} />
            <h2>Ops / admin</h2>
            <p>Sign in with an address listed in <code>ADMIN_EMAILS</code> on the API to view lead inventory, pilot targets, and tickets.</p>
            <button
              type="button"
              className="provision-btn"
              onClick={() => {
                setActiveTab('marketplace');
                setAuthMode('login');
                setAuthModalOpen(true);
              }}
            >
              <LogIn size={18} /> Sign in
            </button>
          </section>
        )}

        {activeTab === 'admin' && sessionUser?.role === 'admin' && (
          <section className="admin-portal fade-in glass">
            <div className="admin-portal-head">
              <h2>
                <LayoutDashboard size={28} /> Operations &amp; admin
              </h2>
              <p>Full ZIP visibility, tiered list pricing, pilot quotas (OK 3 / TX 12 verified visits/day), and Slack/Discord ticket fan-out.</p>
            </div>
            <div className="admin-subtabs">
              <button type="button" className={adminSubTab === 'leads' ? 'active' : ''} onClick={() => setAdminSubTab('leads')}>
                Lead database
              </button>
              <button type="button" className={adminSubTab === 'ops' ? 'active' : ''} onClick={() => setAdminSubTab('ops')}>
                Pilots &amp; targets
              </button>
              <button type="button" className={adminSubTab === 'tickets' ? 'active' : ''} onClick={() => setAdminSubTab('tickets')}>
                <Ticket size={16} /> Tickets
              </button>
            </div>

            {adminSubTab === 'leads' && (
              <div className="admin-table-wrap glass-inset">
                <p className="muted small">{adminLeads.length} rows (max 2000). Teaser marketplace still redacts street for buyers.</p>
                <div className="admin-table-scroll">
                  <table className="admin-table">
                    <thead>
                      <tr>
                        <th>ID</th>
                        <th>ZIP</th>
                        <th>City</th>
                        <th>ST</th>
                        <th>Band</th>
                        <th>Tier</th>
                        <th>List $</th>
                        <th>Status</th>
                        <th>Qualified</th>
                      </tr>
                    </thead>
                    <tbody>
                      {adminLeads.map((row) => (
                        <tr key={row.id}>
                          <td>{row.id}</td>
                          <td><strong>{row.zip_code}</strong></td>
                          <td>{row.city}</td>
                          <td>{row.state}</td>
                          <td>{row.damage_band}</td>
                          <td>{row.tier}</td>
                          <td>{formatMoney(row.price_usd)}</td>
                          <td>{row.status || '—'}</td>
                          <td>{row.qualification_status || '—'}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}

            {adminSubTab === 'ops' && adminOps && (
              <div className="admin-ops-grid">
                <div className="glass-inset pilot-markets">
                  <h3>Pilot program · first clients by metro</h3>
                  <ul className="pilot-list">
                    {(adminOps.pilot_markets || []).map((m) => (
                      <li key={m.market_key || m.id}>
                        <strong>{m.city}</strong>, {m.state} — {m.incentive_label}
                        <span className="slots">Slots {m.slots_used}/{m.slots_total}</span>
                      </li>
                    ))}
                  </ul>
                </div>
                <div className="glass-inset regional-targets">
                  <h3>Regional daily verified visit targets</h3>
                  <ul className="targets-list">
                    {(adminOps.regional_targets || []).map((t) => {
                      const tgt = t.target_verified_visits_per_day || 0;
                      const done = t.verified_visits_today ?? 0;
                      const pct = tgt > 0 ? Math.min(100, Math.round((done / tgt) * 100)) : 0;
                      return (
                        <li key={t.region_code}>
                          <div className="target-row">
                            <span>{t.label}</span>
                            <span className="target-num">
                              {done} / {tgt} today
                            </span>
                          </div>
                          <div className="progress-bar"><div className="progress-fill" style={{ width: `${pct}%` }} /></div>
                          {t.notes && <p className="muted small">{t.notes}</p>}
                        </li>
                      );
                    })}
                  </ul>
                </div>
              </div>
            )}

            {adminSubTab === 'ops' && !adminOps && <p className="muted">Loading operations summary…</p>}

            {adminSubTab === 'tickets' && (
              <div className="admin-tickets">
                <form className="ticket-form glass-inset" onSubmit={submitAdminTicket}>
                  <h3>Open ticket (Slack + Discord)</h3>
                  <p className="muted small">Set <code>SLACK_TICKETS_WEBHOOK</code> and <code>DISCORD_TICKETS_WEBHOOK</code> on the API.</p>
                  <label>
                    Title
                    <input
                      value={ticketForm.title}
                      onChange={(e) => setTicketForm({ ...ticketForm, title: e.target.value })}
                      required
                    />
                  </label>
                  <label>
                    Details
                    <textarea
                      value={ticketForm.body}
                      onChange={(e) => setTicketForm({ ...ticketForm, body: e.target.value })}
                      rows={4}
                      required
                    />
                  </label>
                  <label>
                    Lead ID (optional)
                    <input
                      value={ticketForm.lead_id}
                      onChange={(e) => setTicketForm({ ...ticketForm, lead_id: e.target.value })}
                      inputMode="numeric"
                    />
                  </label>
                  <label>
                    Priority
                    <select
                      value={ticketForm.priority}
                      onChange={(e) => setTicketForm({ ...ticketForm, priority: e.target.value })}
                    >
                      <option value="low">low</option>
                      <option value="normal">normal</option>
                      <option value="high">high</option>
                    </select>
                  </label>
                  <button type="submit" className="provision-btn">
                    <Ticket size={18} /> Create &amp; notify
                  </button>
                </form>
                <div className="ticket-list glass-inset">
                  <h3>Recent tickets</h3>
                  <ul>
                    {adminTickets.map((tk) => (
                      <li key={tk.id}>
                        <div className="ticket-row-title">
                          <strong>#{tk.id}</strong> {tk.title}{' '}
                          <span className={`ticket-status ${tk.status}`}>{tk.status}</span>
                        </div>
                        <p className="muted small">{tk.body?.slice(0, 160)}</p>
                      </li>
                    ))}
                  </ul>
                </div>
              </div>
            )}
          </section>
        )}

        {activeTab === 'flyover' && (
          <section className="flyover-section fade-in glass">
            <div className="flyover-header">
              <h2>
                <Plane size={28} /> Pro flyover · on-demand UAV
              </h2>
              <p>
                For <strong>accounts with Pro enabled</strong>, Ridgeline dispatches low-altitude passes to catch lift, soft
                metal, ridge wear, and penetration risks—often before the homeowner sees ground-level symptoms. Use outputs
                to prioritize outbound and tighten estimates.
              </p>
            </div>

            {!proAccount ? (
              <div className="flyover-gate glass-premium">
                <Lock size={36} />
                <h3>Pro account required</h3>
                <p>Toggle “Pro account” in the header to demo the request flow, or contact operations to enable flyover credits.</p>
              </div>
            ) : (
              <div className="flyover-pro glass-premium">
                <div className="flyover-benefits">
                  <h4>What you get</h4>
                  <ul>
                    <li>Close geometry capture (sub-structure angle) for sales decks</li>
                    <li>Orthomosaic + hotspot export to attach in workspace estimates</li>
                    <li>Optional rush slot for CAT corridors</li>
                  </ul>
                </div>
                <label className="flyover-field">
                  <span>Property or lead reference</span>
                  <textarea
                    value={flyoverNotes}
                    onChange={(e) => setFlyoverNotes(e.target.value)}
                    placeholder="e.g. Unlock lead #RK-2044 · 73112 · schedule before Friday AM"
                    rows={4}
                  />
                </label>
                <button
                  type="button"
                  className="purchase-btn-neon flyover-submit"
                  onClick={() =>
                    alert('Flyover request queued (demo). Wire to dispatch API + airspace check service.')
                  }
                >
                  <ScanLine size={18} /> Request close scan
                </button>
              </div>
            )}
          </section>
        )}
      </main>

      {/* Lead workspace (sales desk) */}
      {workspaceLead && estimate && (
        <div className="workspace-scrim" role="presentation" onClick={() => setWorkspaceLead(null)} />
      )}
      {workspaceLead && estimate && (
        <aside className="workspace-drawer glass-premium" aria-label="Lead workspace">
          <div className="workspace-drawer-head">
            <div>
              <p className="workspace-kicker">Lead workspace</p>
              <h3 className="workspace-title">
                {workspaceLead.redacted_address}, {workspaceLead.city}
              </h3>
              <p className="workspace-meta">
                ZIP {workspaceLead.zip_code || workspaceLead.zipcode} ·{' '}
                {(workspaceLead.magnitude || 0).toFixed(2)}″ event · Material: {workspaceLead.material_interest || '—'}
              </p>
            </div>
            <button type="button" className="icon-close" onClick={() => setWorkspaceLead(null)} aria-label="Close workspace">
              <X size={22} />
            </button>
          </div>

          <div className="workspace-scroll">
            <section className="workspace-section">
              <h4>
                <Eye size={16} /> Damage imagery
              </h4>
              <label className="toggle-damage">
                <input type="checkbox" checked={highlightDamage} onChange={(e) => setHighlightDamage(e.target.checked)} />
                Emphasize AI-flag overlays on thumbnails
              </label>
              {workspaceLead.image_url_before ? (
                <div className={`workspace-imagery ${highlightDamage ? 'damage-highlight' : ''}`}>
                  <div className="workspace-img-pair">
                    <figure>
                      <img src={resolveAssetUrl(workspaceLead.image_url_before)} alt="Before" onError={(e) => { e.target.style.display = 'none'; }} />
                      <figcaption>Reference</figcaption>
                    </figure>
                    <figure>
                      <img src={resolveAssetUrl(workspaceLead.image_url_after)} alt="After" onError={(e) => { e.target.style.display = 'none'; }} />
                      <figcaption>Damaged / flagged</figcaption>
                    </figure>
                  </div>
                  {workspaceLead.image_findings && (
                    <p className="workspace-findings">
                      <Crosshair size={14} /> {workspaceLead.image_findings}
                    </p>
                  )}
                </div>
              ) : (
                <p className="muted">No imagery bundle on this teaser—unlock full lead for satellite & UAV stills.</p>
              )}
            </section>

            {(workspaceLead.verification_images?.length > 0 ||
              workspaceLead.verification_text?.length > 0 ||
              workspaceLead.call_pitch_why) && (
              <section className="workspace-section">
                <h4>
                  <Crosshair size={16} /> Verification trail (why this lead / what to say)
                </h4>
                {workspaceLead.call_pitch_why && (
                  <p className="workspace-findings">{workspaceLead.call_pitch_why}</p>
                )}
                <div className="verification-workspace-grid">
                  {workspaceLead.verification_images?.map((v) => (
                    <div key={`ws-${v.url}-${v.role}`} className="verification-workspace-card">
                      {v.url ? (
                        <img src={resolveAssetUrl(v.url)} alt={v.role || ''} onError={(e) => { e.target.style.display = 'none'; }} />
                      ) : null}
                      <div>
                        <strong>{v.role}</strong>
                        {v.why && <p className="muted small">{v.why}</p>}
                        {v.call_hook && <p className="workspace-findings">{v.call_hook}</p>}
                      </div>
                    </div>
                  ))}
                  {workspaceLead.verification_text?.map((t) => (
                    <div key={`ws-t-${t.type}`} className="verification-workspace-card text-only">
                      <div>
                        <strong>{t.type}</strong>
                        {t.rationale && <p className="muted small">{t.rationale}</p>}
                        {t.call_hook && <p className="workspace-findings">{t.call_hook}</p>}
                      </div>
                    </div>
                  ))}
                </div>
              </section>
            )}

            <section className="workspace-section">
              <h4>
                <Video size={16} /> Client video · mockup & walkthrough
              </h4>
              <p className="muted small">
                Five-second flyover mockups position the replacement roof and upsells (ridge vent, skylights, metal accent).
                Full walkthrough is watermarked until the homeowner purchases the visual package—reduces tire-kickers.
              </p>
              <div className="video-cards">
                <div className="video-card">
                  <div className="video-thumb">
                    <Play size={32} className="play-ic" />
                    <span className="video-badge">~5 sec</span>
                  </div>
                  <div className="video-card-body">
                    <strong>Teaser flyover mockup</strong>
                    <span className="muted small">Low-res shareable clip · branded end card</span>
                  </div>
                </div>
                <div className="video-card locked">
                  <div className="video-thumb">
                    <Lock size={28} />
                    <span className="video-badge">Pre-purchase</span>
                  </div>
                  <div className="video-card-body">
                    <strong>Full walkthrough preview</strong>
                    <span className="muted small">Gated until visual package sold · HD export after payment</span>
                  </div>
                </div>
              </div>
            </section>

            <section className="workspace-section">
              <h4>
                <Quote size={16} /> Estimate package
              </h4>
              <div className="estimate-grid">
                <label>
                  Roof replacement
                  <input
                    type="number"
                    step="100"
                    value={estimate.roofReplacement}
                    onChange={(e) =>
                      setEstimate({ ...estimate, roofReplacement: Number(e.target.value) || 0 })
                    }
                  />
                </label>
                <label>
                  Gutters &amp; edge
                  <input
                    type="number"
                    step="50"
                    value={estimate.gutters}
                    onChange={(e) => setEstimate({ ...estimate, gutters: Number(e.target.value) || 0 })}
                  />
                </label>
                <label>
                  Skylight upsell
                  <input
                    type="number"
                    step="50"
                    value={estimate.skylightUpsell}
                    onChange={(e) => setEstimate({ ...estimate, skylightUpsell: Number(e.target.value) || 0 })}
                  />
                </label>
                <label>
                  Ventilation package
                  <input
                    type="number"
                    step="25"
                    value={estimate.ventilation}
                    onChange={(e) => setEstimate({ ...estimate, ventilation: Number(e.target.value) || 0 })}
                  />
                </label>
              </div>
              <div className="estimate-total">
                <span>Client-facing total</span>
                <strong>{formatMoney(estimateTotal)}</strong>
              </div>
            </section>
          </div>

          <div className="workspace-actions">
            <button
              type="button"
              className="secondary-btn"
              onClick={() => {
                setSelectedLead(workspaceLead);
                setShowTranscript(true);
              }}
            >
              <MessageSquare size={16} /> Sample transcript
            </button>
            <button type="button" className="secondary-btn" onClick={sendEstimateStub}>
              <Mail size={16} /> Email / SMS estimate
            </button>
            <button type="button" className="primary-btn" onClick={() => alert('Schedule outbound (demo): hook calendar API.')}>
              <Phone size={16} /> Call homeowner
            </button>
            <button
              type="button"
              className="cta-line"
              onClick={() => handleSquareCheckout(workspaceLead)}
              disabled={squareLoading === workspaceLead?.id}
            >
              {squareLoading === workspaceLead?.id ? 'Opening Square…' : `Unlock full dossier · ${formatMoney(workspaceLead.list_price_usd ?? 149)}`}
            </button>
          </div>
        </aside>
      )}

      {showTranscript && selectedLead && (
        <div className="modal-overlay" role="dialog" aria-modal="true" onClick={() => setShowTranscript(false)}>
          <div className="modal-content glass-premium" onClick={(e) => e.stopPropagation()}>
            <div className="modal-header">
              <h3>
                <MessageSquare size={18} /> Call transcript: {selectedLead.redacted_address}
              </h3>
              <button type="button" className="close-btn" onClick={() => setShowTranscript(false)} aria-label="Close">
                ×
              </button>
            </div>
            <div className="transcript-body glass-inset">
              <div className="speaker agent">Sarah: “Hi, I’m calling because your property was flagged after the recent hail corridor.”</div>
              <div className="speaker lead">Homeowner: “I didn’t notice anything from the yard.”</div>
              <div className="speaker agent">
                Sarah: “That’s common—our imagery picked up granule loss and ridge bruising on the south slope. We can book a no-cost look.”
              </div>
              <div className="status-marker success">Outcome: inspection booked</div>
            </div>
          </div>
        </div>
      )}

      {authModalOpen && (
        <div
          className="modal-overlay"
          role="dialog"
          aria-modal="true"
          aria-labelledby="auth-modal-title"
          onClick={() => setAuthModalOpen(false)}
        >
          <div className="modal-content glass-premium auth-modal" onClick={(e) => e.stopPropagation()}>
            <div className="modal-header">
              <h3 id="auth-modal-title">
                {authMode === 'register' ? <UserPlus size={18} /> : <LogIn size={18} />}{' '}
                {authMode === 'register' ? 'Create client account' : 'Sign in'}
              </h3>
              <button type="button" className="close-btn" onClick={() => setAuthModalOpen(false)} aria-label="Close">
                ×
              </button>
            </div>
            <form className="auth-form" onSubmit={handleAuthSubmit}>
              {authError && <div className="auth-error">{authError}</div>}
              <label>
                Email
                <input
                  type="email"
                  autoComplete="email"
                  value={authEmail}
                  onChange={(e) => setAuthEmail(e.target.value)}
                  required
                />
              </label>
              {authMode === 'register' && (
                <label>
                  Username
                  <input
                    type="text"
                    autoComplete="username"
                    value={authUsername}
                    onChange={(e) => setAuthUsername(e.target.value)}
                    required
                    minLength={2}
                  />
                </label>
              )}
              <label>
                Password
                <input
                  type="password"
                  autoComplete={authMode === 'register' ? 'new-password' : 'current-password'}
                  value={authPassword}
                  onChange={(e) => setAuthPassword(e.target.value)}
                  required
                  minLength={8}
                />
              </label>
              <button type="submit" className="provision-btn wide">
                {authMode === 'register' ? 'Register' : 'Sign in'}
              </button>
              <p className="auth-switch muted small">
                {authMode === 'register' ? (
                  <>
                    Already have an account?{' '}
                    <button type="button" className="link-btn" onClick={() => { setAuthMode('login'); setAuthError(''); }}>
                      Sign in
                    </button>
                  </>
                ) : (
                  <>
                    New buyer?{' '}
                    <button type="button" className="link-btn" onClick={() => { setAuthMode('register'); setAuthError(''); }}>
                      Create account
                    </button>
                  </>
                )}
              </p>
              <p className="muted small">
                <CreditCard size={14} /> Payments run through Square after login (sandbox or production per{' '}
                <code>SQUARE_ENV</code>).
              </p>
            </form>
          </div>
        </div>
      )}

      <footer className="footer-bar glass">
        <div className="system-status">
          <span className="pulse" /> Live: forensic lead graph + API {API_BASE}
        </div>
        <div className="user-info">
          <ShieldCheck size={14} /> ridgeline.ai · Encrypted gateway
        </div>
      </footer>
    </div>
  );
}

export default App;
