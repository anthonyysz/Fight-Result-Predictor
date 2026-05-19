import React, { useState } from "react";
import "../style/stats.css";

const DEFAULT_LOCAL_API_BASE_URL = "http://127.0.0.1:8000";

const normalizeApiBaseUrl = (value) => {
  if (!value) {
    return DEFAULT_LOCAL_API_BASE_URL;
  }

  const trimmed = value.trim().replace(/\/+$/, "");
  return trimmed || DEFAULT_LOCAL_API_BASE_URL;
};

const API_BASE_URL = normalizeApiBaseUrl(process.env.REACT_APP_API_BASE_URL);
const CHART_URL = `${API_BASE_URL}/stats/average-return-chart`;

const Stats = () => {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  return (
    <div className="stats-screen w-full">
      <div className="stats-container mx-auto">
        <div className="stats-heading-row">
          <div>
            <p className="stats-greeting-text">Historical results</p>
            <h1 className="stats-title-text">Average Return</h1>
          </div>

          <p className="stats-bio-text">
            Average Model Return Rate per Fight Card<br></br>
            (This will look more readable as more fights happen)
          </p>
        </div>

        <div className="stats-chart-shell">
          {loading && !error ? (
            <div className="stats-status-message">Loading return chart...</div>
          ) : null}

          {error ? (
            <div className="stats-status-message">{error}</div>
          ) : null}

          <img
            className={loading || error ? "stats-chart-image is-hidden" : "stats-chart-image"}
            src={CHART_URL}
            alt="Line chart showing average model return by fight date"
            onLoad={() => {
              setLoading(false);
              setError("");
            }}
            onError={() => {
              setLoading(false);
              setError("Unable to load the return chart.");
            }}
          />
        </div>
      </div>
    </div>
  );
};

export default Stats;
