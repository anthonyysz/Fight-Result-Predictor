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
const AVERAGE_RETURN_CHART_URL = `${API_BASE_URL}/stats/average-return-chart`;
const TOP_EVENTS_CHART_URL = `${API_BASE_URL}/stats/top-betting-events-chart`;

const Stats = () => {
  const [averageReturnLoading, setAverageReturnLoading] = useState(true);
  const [averageReturnError, setAverageReturnError] = useState("");
  const [topEventsLoading, setTopEventsLoading] = useState(true);
  const [topEventsError, setTopEventsError] = useState("");

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
          {averageReturnLoading && !averageReturnError ? (
            <div className="stats-status-message">Loading return chart...</div>
          ) : null}

          {averageReturnError ? (
            <div className="stats-status-message">{averageReturnError}</div>
          ) : null}

          <img
            className={
              averageReturnLoading || averageReturnError
                ? "stats-chart-image is-hidden"
                : "stats-chart-image"
            }
            src={AVERAGE_RETURN_CHART_URL}
            alt="Line chart showing average model return by fight date"
            onLoad={() => {
              setAverageReturnLoading(false);
              setAverageReturnError("");
            }}
            onError={() => {
              setAverageReturnLoading(false);
              setAverageReturnError("Unable to load the return chart.");
            }}
          />
        </div>

        <div className="stats-chart-shell stats-table-chart-shell">
          {topEventsLoading && !topEventsError ? (
            <div className="stats-status-message">Loading top betting events...</div>
          ) : null}

          {topEventsError ? (
            <div className="stats-status-message">{topEventsError}</div>
          ) : null}

          <img
            className={
              topEventsLoading || topEventsError
                ? "stats-chart-image is-hidden"
                : "stats-chart-image"
            }
            src={TOP_EVENTS_CHART_URL}
            alt="Table chart showing top five betting events by average return"
            onLoad={() => {
              setTopEventsLoading(false);
              setTopEventsError("");
            }}
            onError={() => {
              setTopEventsLoading(false);
              setTopEventsError("Unable to load the top betting events chart.");
            }}
          />
        </div>
      </div>
    </div>
  );
};

export default Stats;
