import React, { useEffect, useState } from "react";
import "../style/home.css";

const API_BASE_URL = process.env.REACT_APP_API_BASE_URL || "http://127.0.0.1:8000";

const formatOdds = (odds) => {
  const numericOdds = Number(odds);
  return numericOdds > 0 ? `+${numericOdds}` : `${numericOdds}`;
};

const formatConfidence = (confidence) => {
  return `${Math.round(Number(confidence) * 100)}%`;
};

const buildRowKey = (row) => {
  return `${row.fight_date}-${row.red_fighter}-${row.blue_fighter}-${row.weight_class}`;
};

const Home = () => {
  const [fightRows, setFightRows] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    let cancelled = false;

    const loadPredictions = async () => {
      try {
        const response = await fetch(`${API_BASE_URL}/predictions/upcoming`);

        if (!response.ok) {
          throw new Error(`Request failed with status ${response.status}`);
        }

        const data = await response.json();

        if (!cancelled) {
          setFightRows(Array.isArray(data.rows) ? data.rows : []);
          setError("");
        }
      } catch (err) {
        if (!cancelled) {
          setFightRows([]);
          setError(err instanceof Error ? err.message : "Failed to load predictions.");
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    };

    loadPredictions();

    return () => {
      cancelled = true;
    };
  }, []);

  const renderStatusRow = (message) => {
    return (
      <div className="fight-table-row" key={message}>
        <div className="fight-table-item fighter-item">
          <div className="fighter-row">
            <span className="corner-label red-corner">Status</span>
            <span className="fighter-name">{message}</span>
          </div>
        </div>

        <div className="fight-table-item odds-item">
          <span>--</span>
          <span>--</span>
        </div>

        <div className="fight-table-item data-item" data-label="Weight">
          --
        </div>

        <div className="fight-table-item data-item" data-label="Winner?">
          --
        </div>

        <div className="fight-table-item data-item" data-label="Confidence">
          --
        </div>

        <div className="fight-table-item data-item" data-label="Pick/Pass">
          --
        </div>
      </div>
    );
  };

  const renderTableRows = () => {
    if (loading) {
      return [renderStatusRow("Loading upcoming predictions...")];
    }

    if (error) {
      return [renderStatusRow(`Unable to load predictions: ${error}`)];
    }

    if (fightRows.length === 0) {
      return [renderStatusRow("No upcoming predictions are available yet.")];
    }

    return fightRows.map((row) => (
      <div className="fight-table-row" key={buildRowKey(row)}>
        <div className="fight-table-item fighter-item">
          <div className="fighter-row">
            <span className="corner-label red-corner">Red Corner</span>
            <span className="fighter-name">{row.red_fighter}</span>
          </div>
          <div className="fighter-row">
            <span className="corner-label blue-corner">Blue Corner</span>
            <span className="fighter-name">{row.blue_fighter}</span>
          </div>
        </div>

        <div className="fight-table-item odds-item">
          <span>{formatOdds(row.red_odds)}</span>
          <span>{formatOdds(row.blue_odds)}</span>
        </div>

        <div className="fight-table-item data-item" data-label="Weight">
          {row.weight_class}
        </div>

        <div className="fight-table-item data-item" data-label="Winner?">
          {row.predicted_winner}
        </div>

        <div className="fight-table-item data-item" data-label="Confidence">
          {formatConfidence(row.confidence)}
        </div>

        <div className="fight-table-item data-item" data-label="Pick/Pass">
          {row.recommended_bet}
        </div>
      </div>
    ));
  };

  return (
    <div className="home-screen w-full">
      <div className="home-container mx-auto">
        <div className="home-heading-row">
          <div>
            <p className="home-greeting-text">Upcoming predictions</p>
            <h1 className="home-title-text">Fight Card</h1>
          </div>

          <p className="home-bio-text">
            Live rows from the upcoming predictions table.
          </p>
        </div>

        <div className="fight-table-shell w-full">
          <div className="fight-table-header">
            <div className="fight-table-header-item">Fighter</div>
            <div className="fight-table-header-item">Odds</div>
            <div className="fight-table-header-item">Weight</div>
            <div className="fight-table-header-item">Winner?</div>
            <div className="fight-table-header-item">Confidence</div>
            <div className="fight-table-header-item">Pick/Pass</div>
          </div>

          <div className="fight-table-body">{renderTableRows()}</div>
        </div>
      </div>
    </div>
  );
};

export default Home;
