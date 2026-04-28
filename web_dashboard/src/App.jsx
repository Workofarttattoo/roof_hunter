import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { 
  Search, MapPin, Wind, TrendingUp, AlertTriangle, Phone, Mail, User, 
  ShieldCheck, MessageSquare, ShoppingBag, Zap, LayoutDashboard, Lock, ChevronRight,
  Filter, Home, Calendar, Layers, CheckCircle2, XCircle, Clock
} from 'lucide-react';
import './App.css';

const API_BASE = "https://fusvj-2600-8801-3302-a800-109b-ac94-745-66ee.run.pinggy-free.link";

function App() {
  const [marketLeads, setMarketLeads] = useState([]);
  const [selectedState, setSelectedState] = useState('ALL');
  const [activeTab, setActiveTab] = useState('marketplace');
  const [searchQuery, setSearchQuery] = useState('');
  const [zipFilter, setZipFilter] = useState('');
  const [materialFilter, setMaterialFilter] = useState('ALL');
  const [isQualifying, setIsQualifying] = useState(null);

  const fetchLeads = () => {
    const params = new URLSearchParams();
    if (searchQuery) params.append('q', searchQuery);
    if (zipFilter) params.append('zip', zipFilter);
    if (materialFilter !== 'ALL') params.append('material', materialFilter);

    fetch(`${API_BASE}/api/leads/teasers?${params.toString()}`)
      .then(res => res.json())
      .then(data => {
        if (Array.isArray(data)) {
          setMarketLeads(data);
        }
      })
      .catch(err => console.error("Marketplace fetch error:", err));
  };

  useEffect(() => {
    fetchLeads();
  }, [searchQuery, zipFilter, materialFilter]);

  const handleQualify = async (leadId) => {
    setIsQualifying(leadId);
    try {
      await axios.post(`${API_BASE}/api/leads/qualify`, { lead_id: leadId });
      fetchLeads(); // Refresh data
    } catch (err) {
      console.error("Qualify error:", err);
    } finally {
      setIsQualifying(null);
    }
  };

  const groupedLeads = marketLeads.reduce((acc, lead) => {
    const state = lead.state || 'UNKNOWN';
    if (!acc[state]) acc[state] = [];
    acc[state].push(lead);
    return acc;
  }, {});

  const availableStates = Object.keys(groupedLeads).sort();
  const stateTabs = ['ALL', ...availableStates];

  const getStatusIcon = (status) => {
    switch (status) {
      case 'QUALIFIED': return <CheckCircle2 size={12} className="status-icon-plus success" />;
      case 'REJECTED': return <XCircle size={12} className="status-icon-plus error" />;
      case 'NO_ANSWER': return <Clock size={12} className="status-icon-plus warning" />;
      default: return <Clock size={12} className="status-icon-plus pending" />;
    }
  };

  return (
    <div className="roof-hunter-app">
      <header className="app-header glass">
        <div className="logo">
          <Zap className="logo-icon neon-flicker" />
          <div className="logo-text">
            <h1>ROOF HUNTER</h1>
            <span className="beta-tag">CAT-5 FORENSICS</span>
          </div>
        </div>
        <nav className="nav-tabs">
          <button 
            className={`nav-btn ${activeTab === 'dashboard' ? 'active' : ''}`}
            onClick={() => setActiveTab('dashboard')}
          >
            <LayoutDashboard size={18} /> COMMAND CENTER
          </button>
          <button 
            className={`nav-btn ${activeTab === 'marketplace' ? 'active' : ''}`}
            onClick={() => setActiveTab('marketplace')}
          >
            <ShoppingBag size={18} /> LEAD MARKETPLACE
          </button>
        </nav>
      </header>

      <main className="main-content">
        {activeTab === 'marketplace' ? (
          <section className="marketplace-section fade-in">
            <div className="section-header-block">
              <div className="title-block">
                <h2><ShoppingBag size={32} className="title-icon" /> National Lead Marketplace</h2>
                <p>Browse verified forensic leads. Unlock addresses for full homeowner intelligence.</p>
              </div>
              
              <div className="search-filter-bar glass-premium">
                <div className="search-input-wrapper">
                  <Search className="search-icon" size={18} />
                  <input 
                    type="text" 
                    placeholder="Search by City, State, or Zip..." 
                    value={searchQuery}
                    onChange={(e) => setSearchQuery(e.target.value)}
                    className="premium-search-input"
                  />
                </div>
                
                <div className="filters-group">
                  <div className="filter-item">
                    <MapPin size={16} />
                    <input 
                      type="text" 
                      placeholder="Zip Code" 
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
                      <option value="ALL">All Materials</option>
                      <option value="Metal">Metal</option>
                      <option value="Asphalt">Asphalt</option>
                      <option value="Tile">Tile</option>
                    </select>
                  </div>
                </div>
              </div>

              <div className="state-selector-row">
                <div className="state-filters glass">
                  {stateTabs.map(s => (
                    <button 
                      key={s} 
                      className={`state-tab ${selectedState === s ? 'active' : ''}`}
                      onClick={() => setSelectedState(s)}
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
                      <span className="count-pill">{leads.length} AVAILABLE</span>
                    </h3>
                    <div className="cards-container">
                      {leads.map(lead => (
                        <div key={lead.id} className="lead-card glass-premium">
                          <div className="card-top-row">
                            <div className="badges-group">
                              <div className={`status-badge ${lead.damage_score >= 40 ? 'total-loss' : 'platinum'}`}>
                                {lead.damage_score >= 40 ? 'TOTAL LOSS' : 'PLATINUM'}
                              </div>
                              <div className={`qual-badge ${lead.qualification_status?.toLowerCase().replace('_', '-') || 'pending'}`}>
                                {getStatusIcon(lead.qualification_status)}
                                {lead.qualification_status || 'PENDING AI CALL'}
                              </div>
                            </div>
                            <div className="storm-stat">
                              <Wind size={14} /> {lead.magnitude.toFixed(2)}"
                            </div>
                          </div>
                          
                          <div className="damage-viz">
                            <div className="damage-ring">
                              <svg viewBox="0 0 36 36" className="circular-chart">
                                <path className="circle-bg" d="M18 2.0845 a 15.9155 15.9155 0 0 1 0 31.831 a 15.9155 15.9155 0 0 1 0 -31.831" />
                                <path 
                                  className={`circle ${lead.damage_score >= 40 ? 'loss-color' : 'platinum-color'}`} 
                                  strokeDasharray={`${lead.damage_score}, 100`} 
                                  d="M18 2.0845 a 15.9155 15.9155 0 0 1 0 31.831 a 15.9155 15.9155 0 0 1 0 -31.831" 
                                />
                                <text x="18" y="20.35" className="percentage">{lead.damage_score.toFixed(0)}%</text>
                              </svg>
                            </div>
                            <div className="viz-label">AI VERIFIED DAMAGE</div>
                          </div>

                          <div className="lead-details">
                            <div className="address-teaser">
                              <Lock size={14} className="lock-icon" />
                              <span>{lead.redacted_address}, {lead.city}</span>
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
                                  <Phone size={10} /> AI AGENT CALL LOG
                                </div>
                                <p>{lead.call_log}</p>
                              </div>
                            ) : (
                              <div className="proof-box glass-inset">
                                <MessageSquare size={12} className="quote-icon" />
                                <p>{lead.proof_msg || "Catastrophic structural disruption verified via spectral imagery scan."}</p>
                              </div>
                            )}
                          </div>

                          <div className="actions-row">
                            {lead.qualification_status === 'PENDING' ? (
                              <button 
                                className="qualify-btn" 
                                onClick={() => handleQualify(lead.id)}
                                disabled={isQualifying === lead.id}
                              >
                                {isQualifying === lead.id ? (
                                  <span className="pulse-mini"></span>
                                ) : (
                                  <Phone size={14} />
                                )}
                                TRIGGER AI COLD CALL
                              </button>
                            ) : null}
                            <button className="purchase-btn-neon">
                              <ChevronRight size={20} /> UNLOCK LEAD $149
                            </button>
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                ))}
              
              {marketLeads.length === 0 && (
                <div className="no-results glass">
                  <AlertTriangle size={48} />
                  <h3>No leads found matching your search filters.</h3>
                  <p>Try broadening your search or adjusting the state selection.</p>
                </div>
              )}
            </div>
          </section>
        ) : (
          <div className="dashboard-placeholder glass">
            <h2>COMMAND CENTER</h2>
            <p>Select the Marketplace to view high-value forensic leads.</p>
          </div>
        )}
      </main>

      <footer className="footer-bar glass">
        <div className="system-status">
          <span className="pulse"></span> System Live: National Forensic Intelligence Active
        </div>
        <div className="user-info">
          <ShieldCheck size={14} /> SECURE GATEWAY ENABLED
        </div>
      </footer>
    </div>
  );
}

export default App;
