import React from "react";
import "../style/home.css";

const Home = () => {
  return (
    <div className="home-screen w-full">
      <div className="home-container mx-auto">
        <div className="home-heading-row">
          <div>
            <p className="home-greeting-text">Fight week reports</p>
            <h1 className="home-title-text">Local UFC picks for the next card</h1>
          </div>

          <p className="home-bio-text">
            Generate the current report from the local backend whenever you
            want fresh odds, predictions, and pick/pass recommendations.
          </p>
        </div>

        <section className="home-signup-shell" aria-labelledby="local-report-title">
          <div className="home-signup-copy">
            <p className="home-greeting-text">Local only</p>
            <h2 className="home-signup-title" id="local-report-title">
              Reports stay on this machine
            </h2>
            <p className="home-signup-text">
              The backend writes the latest CSV, HTML, and text reports under
              the generated data folder. Nothing leaves this machine.
            </p>
            <p className="home-signup-status">
              Use the local scripts or admin API to refresh fight-week output.
            </p>
          </div>

          <div className="home-signup-form">
            <p className="home-signup-text">
              Generated report files are saved in
              backend/data/generated/reports, with archived copies kept for
              past runs.
            </p>
          </div>
        </section>
      </div>
    </div>
  );
};

export default Home;
