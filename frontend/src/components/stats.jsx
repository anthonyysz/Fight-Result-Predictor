import React, { useState } from "react";
import "../style/stats.css";

const AVERAGE_RETURN_CHART_URL = "/generated/average-return-chart.png";
const TOP_EVENTS_CHART_URL = "/generated/top-betting-events-chart.png";

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
            <h1 className="stats-title-text">Model Performance</h1>
          </div>

          <p className="stats-bio-text">
            Track how the model has performed so far, then join the list to get
            the next fight card picks delivered before the weekend.
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

        <section className="stats-cta-shell" aria-labelledby="email-cta-title">
          <div className="stats-cta-copy">
            <p className="stats-greeting-text">Weekly picks by email</p>
            <h2 className="stats-cta-title" id="email-cta-title">
              Want the full upcoming fight table?
            </h2>
            <p className="stats-cta-text">
              The public site now highlights long-term performance. Upcoming
              picks and the full fight card table will be sent through an email
              list once the subscription flow is ready.
            </p>
          </div>

          <div className="stats-cta-actions">
            <button className="stats-cta-button" type="button" disabled>
              Email signup coming soon
            </button>
            <p className="stats-cta-note">
              Until the email list goes live, this section is a placeholder for
              the future subscription experience.
            </p>
          </div>
        </section>
      </div>
    </div>
  );
};

export default Stats;
