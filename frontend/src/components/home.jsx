import React from "react";
import "../style/home.css";

const fightRows = [
  1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
];

const Home = () => {
  return (
    <div className="home-screen w-full">
      <div className="home-container mx-auto">
        <div className="home-heading-row">
          <div>
            <p className="home-greeting-text">Upcoming predictions</p>
            <h1 className="home-title-text">Fight Card</h1>
          </div>

          <p className="home-bio-text">
            
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

          <div className="fight-table-body">
            {fightRows.map((row) => (
              <div className="fight-table-row" key={row}>
                <div className="fight-table-item fighter-item">
                  <div className="fighter-row">
                    <span className="corner-label red-corner">Red Corner</span>
                    <span className="fighter-name">Fighter Name</span>
                  </div>
                  <div className="fighter-row">
                    <span className="corner-label blue-corner">Blue Corner</span>
                    <span className="fighter-name">Fighter Name</span>
                  </div>
                </div>

                <div className="fight-table-item odds-item">
                  <span>- ---</span>
                  <span>- ---</span>
                </div>

                <div className="fight-table-item data-item" data-label="Weight">
                  Weight Class
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
            ))}
          </div>
        </div>
      </div>
    </div>
  );
};

export default Home;
