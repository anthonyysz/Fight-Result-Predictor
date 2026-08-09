import React, { useState } from "react";
import "../style/stats.css";

const AVERAGE_RETURN_CHART_URL = "/generated/average-return-chart.png";
const SUGGESTED_BET_CONFIDENCE_CHART_URL = "/generated/suggested-bet-confidence-chart.png";
const TOP_EVENTS_CHART_URL = "/generated/top-betting-events-chart.png";

const ChartPanel = ({ url, alt, loadingText, errorText, tableLayout = false }) => {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  return (
    <div className={tableLayout ? "stats-chart-shell stats-table-chart-shell" : "stats-chart-shell"}>
      {loading && !error ? <div className="stats-status-message">{loadingText}</div> : null}

      {error ? <div className="stats-status-message">{error}</div> : null}

      <img
        className={loading || error ? "stats-chart-image is-hidden" : "stats-chart-image"}
        src={url}
        alt={alt}
        onLoad={() => {
          setLoading(false);
          setError("");
        }}
        onError={() => {
          setLoading(false);
          setError(errorText);
        }}
      />
    </div>
  );
};

const Stats = () => {
  return (
    <div className="stats-screen w-full">
      <div className="stats-container mx-auto">
        <div className="stats-heading-row">
          <div>
            <p className="stats-greeting-text">Historical results</p>
            <h1 className="stats-title-text">Return Progression</h1>
          </div>

          <p className="stats-bio-text">
            Cumulative average model return rate by fight card<br />
            (This will look more readable as more fights happen)
          </p>
        </div>

        <ChartPanel
          url={AVERAGE_RETURN_CHART_URL}
          alt="Line chart showing cumulative average model return by fight date"
          loadingText="Loading return chart..."
          errorText="Unable to load the return chart."
        />

        <ChartPanel
          url={SUGGESTED_BET_CONFIDENCE_CHART_URL}
          alt="Scatter plot showing suggested bet confidence by odds and bet result"
          loadingText="Loading suggested bet confidence chart..."
          errorText="Unable to load the suggested bet confidence chart."
        />

        <ChartPanel
          url={TOP_EVENTS_CHART_URL}
          alt="Table chart showing top five betting events by average return"
          loadingText="Loading top betting events..."
          errorText="Unable to load the top betting events chart."
          tableLayout
        />
      </div>
    </div>
  );
};

export default Stats;
