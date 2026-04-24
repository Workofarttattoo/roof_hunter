import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { Search, MapPin, Wind, Heart, TrendingUp, AlertTriangle, Phone, Mail, User, ShieldCheck, MessageSquare, Briefcase } from 'lucide-react';

const API_BASE = "http://127.0.0.1:8000";
const GOOGLE_MAPS_KEY = "AIzaSyDkaKopClqI80_60jnbwzGcnUWG7MF8nFg";

function App() {
  const [storms, setStorms] = useState([]);
  const [stats, setStats] = useState({ total_events_tracked: 0, states_affected: 0, total_leads: 0, total_contacts: 0 });
  const [loading, setLoading] = useState(false);
  const [deepScanResults, setDeepScanResults] = useState({});
  const [selectedProperty, setSelectedProperty] = useState(null);
  const [propertyData, setPropertyData] = useState(null);
  const [mapInitialized, setMapInitialized] = useState(false);
  const [corridorRisk, setCorridorRisk] = useState([]);
  
  // Filters
  const [stateFilter, setStateFilter] = useState('');
  const [minIncome, setMinIncome] = useState('');
  const [minHail, setMinHail] = useState('');

  const fetchStorms = async () => {
    setLoading(true);
    try {
      const params = { limit: 100 };
      if (stateFilter) params.state = stateFilter;
      if (minIncome) params.min_income = minIncome;
      if (minHail) params.min_hail = minHail;
      
      const res = await axios.get(`${API_BASE}/api/leads`, { params });
      setStorms(res.data.data || []);
    } catch (e) {
      console.error('Leads fetch error:', e);
      setStorms([]);
    }
    setLoading(false);
  };

  const triggerDeepScan = async (leadId) => {
    try {
      const res = await axios.post(`${API_BASE}/api/leads/deep-scan`, { lead_id: leadId });
      setDeepScanResults(prev => ({ ...prev, [leadId]: res.data.damage_assessment || res.data.message }));
    } catch (e) {
      console.error('Deep scan error:', e);
    }
  };

  const fetchStats = async () => {
    try {
      const res = await axios.get(`${API_BASE}/api/stats`);
      setStats(res.data);
    } catch (e) {
      console.error(e);
    }
  };

  const fetchCorridorRisk = async () => {
    try {
      const res = await axios.get(`${API_BASE}/api/corridor-forecast`);
      setCorridorRisk(res.data.sites || []);
    } catch (e) {
      console.error(e);
    }
  };

  const fetchPropertyValuation = async (address) => {
    try {
      const res = await axios.get(`${API_BASE}/api/property-data`, { params: { address } });
      if (res.data.status === 'success') {
        setPropertyData(res.data.data.value);
      }
    } catch (e) {
      console.error('RentCast fetch error:', e);
    }
  };

  useEffect(() => {
    fetchStats();
    fetchStorms();
    fetchCorridorRisk();

    // Map initialization logic from snippet
    const setupMap = async () => {
      await customElements.whenDefined('gmp-map');
      const placePicker = document.querySelector('gmpx-place-picker');
      const map = document.querySelector('gmp-map');
      const marker = document.querySelector('gmp-advanced-marker');
      
      if (!placePicker || !map || !marker) return;

      placePicker.addEventListener('gmpx-placechange', () => {
        const place = placePicker.value;
        if (!place || !place.location) {
            setSelectedProperty(null);
            setPropertyData(null);
            return;
        }

        setSelectedProperty({
          name: place.displayName,
          address: place.formattedAddress,
          lat: place.location.lat,
          lng: place.location.lng
        });
        
        fetchPropertyValuation(place.formattedAddress);

        if (place.viewport) {
           map.innerMap.fitBounds(place.viewport);
        } else {
           map.center = place.location;
           map.zoom = 17;
        }
        marker.position = place.location;
      });
      setMapInitialized(true);
    };

    setupMap();
  }, []);

  return (
    <div className="dashboard-container">
      <header className="header">
        <div className="logo-container">
          <h1>ECH0-ROOF</h1>
          <p>Socioeconomic Intelligence & Autonomous Lead Routing</p>
        </div>
        <div className="stats-container">
          <div className="stat-pill">
            <AlertTriangle size={16} color="#38bdf8" />
            <span>Tracking <strong>{stats.total_events_tracked.toLocaleString()}</strong> Storm Events</span>
          </div>
          <div className="stat-pill">
            <MapPin size={16} color="#818cf8" />
            <span>Across <strong>{stats.states_affected}</strong> States</span>
          </div>
          <div className="stat-pill">
            <User size={16} color="#4ade80" />
            <span><strong>{(stats.total_leads || 0).toLocaleString()}</strong> Verified Leads</span>
          </div>
        </div>
      </header>

      <gmpx-api-loader 
        key={GOOGLE_MAPS_KEY} 
        solution-channel="GMP_GE_mapsandplacesautocomplete_v2">
      </gmpx-api-loader>

      <section className="inspection-chamber">
        <div className="inspection-header">
           <h2><Search size={20} color="#38bdf8"/> Deep-Target Forensic Inspection</h2>
           {selectedProperty && (
             <button className="search-btn" style={{height: '36px'}} onClick={() => triggerDeepScan('manual')}>
               Scan Property
             </button>
           )}
        </div>
        <div className="map-viewport">
          <gmp-map center="36.92,-97.41" zoom="12" map-id="DEMO_MAP_ID">
            <div slot="control-block-start-inline-start" className="place-picker-container">
              <gmpx-place-picker placeholder="Enter property address for manual deep scan..."></gmpx-place-picker>
            </div>
            <gmp-advanced-marker></gmp-advanced-marker>
          </gmp-map>
        </div>
        {selectedProperty && (
          <div className="inspection-stats">
             <div className="inspected-property-card">
               <span className="property-name">{selectedProperty.name}</span>
               <span className="property-addr">{selectedProperty.address}</span>
             </div>
             {propertyData && (
                <div className="stat-pill" style={{borderColor: '#4ade80'}}>
                   <strong>VALUE:</strong> ${propertyData.value?.toLocaleString() || 'N/A'}
                </div>
             )}
             <div className="stat-pill">
               <strong>LAT:</strong> {selectedProperty.lat?.toFixed(4)}
             </div>
             <div className="stat-pill">
               <strong>LNG:</strong> {selectedProperty.lng?.toFixed(4)}
             </div>
          </div>
        )}
      </section>

      {corridorRisk.length > 0 && (
         <div className="threat-banner" style={{marginBottom: '2rem', background: 'rgba(56, 189, 248, 0.1)', color: '#38bdf8'}}>
            <AlertTriangle size={18} />
            <strong>STORM CORRIDOR ALERT:</strong> {corridorRisk.length} Priority Target Zones Detected in KS/OK Path
         </div>
      )}

      <section className="search-bar">
        <div className="filter-group">
          <label>State Abbreviation</label>
          <input 
            type="text" 
            placeholder="e.g. TX, FL, OK" 
            value={stateFilter} 
            onChange={(e) => setStateFilter(e.target.value)}
          />
        </div>
        <div className="filter-group">
          <label>Min Household Income ($)</label>
          <input 
            type="number" 
            placeholder="e.g. 75000" 
            value={minIncome} 
            onChange={(e) => setMinIncome(e.target.value)}
          />
        </div>
        <div className="filter-group">
          <label>Min Hail / Wind Magnitude</label>
          <input 
            type="number" 
            placeholder="e.g. 1.5" 
            step="0.5"
            value={minHail} 
            onChange={(e) => setMinHail(e.target.value)}
          />
        </div>
        <button className="search-btn" onClick={fetchStorms}>
          Analyze Datalake
        </button>
      </section>

      <h2 className="results-header">High-Confidence Homeowner Leads ({storms.length} Found)</h2>
      
      {loading ? (
        <div className="loader">Running GEOBIA Physics Algorithms...</div>
      ) : (
        <div className="grid-container">
          {storms.map((storm) => (
            <div className={`lead-card tier-${storm.tier?.toLowerCase()}`} key={storm.id}>
              {storm.tier && <div className="tier-badge">{storm.tier} LEAD</div>}
              {(storm.deep_scan_result || deepScanResults[storm.id]) && (
                <div className="deep-scan-overlay">
                  <TrendingUp size={12} /> {deepScanResults[storm.id] || storm.deep_scan_result}
                </div>
              )}
              {storm.image_url && (
                <div className="satellite-preview">
                  <img src={storm.image_url} alt="Roof Evidence" />
                  {storm.street_view_url && (
                    <div className="street-view-pip">
                        <img src={storm.street_view_url} alt="Street View" />
                    </div>
                  )}
                  <div className="image-overlay">XGBoost Damage Confirmed</div>
                </div>
              )}
              
              <div className="card-header">
                <span className="zip-badge">{storm.zipcode || "ZONE"}</span>
                <span className="date-text">{storm.event_date}</span>
              </div>
              
              <h3 className="location-title">{storm.city}, {storm.state}</h3>
              
              {storm.homeowner_name && (
                <div className="contact-info-panel">
                  <div className="lead-owner"><User size={16}/> {storm.homeowner_name}</div>
                  <div className="lead-contact"><Phone size={14}/> {storm.phone_number}</div>
                  <div className="lead-contact"><Mail size={14}/> {storm.email}</div>
                  <div className="lead-contact"><Briefcase size={14}/> {storm.insurance_company || 'Unknown Carrier'}</div>
                  
                  <div className="lead-status-row">
                    <span className={`status-pill status-${storm.status?.toLowerCase().replace(' ', '-')}`}>
                        {storm.status}
                    </span>
                    <div className="action-icons">
                        <button className="sms-btn" onClick={() => triggerDeepScan(storm.id)} title="Run Deep Scan Forensics">
                            <TrendingUp size={14} />
                        </button>
                        <button className="sms-btn" title="Send SMS">
                            <MessageSquare size={14} />
                        </button>
                    </div>
                  </div>
                </div>
              )}

              <div className="threat-banner">
                <Wind size={18} />
                {storm.magnitude}" {storm.event_type}
              </div>
              
              <div className="proof-container">
                <ShieldCheck size={14} color="#4ade80" />
                <span className="proof-text">{storm.proof_msg || "Socio-Economic Filter Active"}</span>
              </div>

              <div className="stats-grid">
                <div className="stat-item">
                  <span className="stat-label">Med. Home Value</span>
                  <span className="stat-value">${storm.median_home_value?.toLocaleString() || 'N/A'}</span>
                </div>
                <div className="stat-item">
                  <span className="stat-label">Med. Income</span>
                  <span className="stat-value">${storm.median_household_income?.toLocaleString() || 'N/A'}</span>
                </div>
              </div>
              
              <button className="action-btn">Generate Sales Packet</button>
            </div>
          ))}
          {storms.length === 0 && !loading && (
            <div className="no-leads">
                 <p>No high-value verified damage zones found. Adjust filters to broaden search.</p>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

export default App;
