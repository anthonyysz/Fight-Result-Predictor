import React from "react";
import "../style/about.css";

const About = () => {
  return (
    <div className="about-screen w-full">
      <div className="about-columns">
        <div className="about-grid-item-top">
          <div className="about-title-div">
            <p className="about-subtext">About</p>
            <h1 className="about-title">Fight Result Predictor</h1>
          </div>
          <div className="about-subtitle-div">
            An AI-backed approach to consistent, long-term gain
          </div>
        </div>

        <div className="about-grid-item-bottom">
          <div className="about-text-div">
            While betting on sports has always seemed a bit taboo, it never felt out of place in combat sports. It actually feels like the heart
            and soul of the fight game. Big money promoters and bigger money superstars have always flaunted their status as the top-dog and 
            dared somebody to come take them down. Since the best are often fighting the best, and since the fights are
            often promoted as "you never know what can happen", that intrigue has grasped the public for generations. With sportsbooks more
            popular than ever in the age of placing a bet from your phone or computer, I feel like it's time to level the playing field. <br></br><br></br>

            Sports books have advertised everywhere they're legally allowed to, and that's not by accident. They are making more money than ever
            at the cost of their user. These online sports books and casinos are bigger than ever because they're so convenient to use. As a fan 
            of combat sports, I want to use AI technology to take a little bit back from them. <br></br><br></br>
            I have employed:
            <ul>
              <li>- UFC fight data dating back to 2010</li>
              <li>- The most popular, reliable machine learning classifiers of today</li>
              <li>- Different models fine-tuned for each weight class to reach weight-specific accuracy</li>
            </ul>
            <br></br>
            Through backtesting, I was able to tune the model parameters to see between an 8% and 40% increase in money when following the model's
            suggested bets. At the time of launch on April 29th, 2026, I don't have enough data to show you how it performs in real time, but that will 
            be implemented as soon as the data is available.
          </div>
        </div>
      </div>
    </div>
  );
};

export default About;
