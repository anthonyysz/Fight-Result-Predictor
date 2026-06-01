import React from "react";
import "../style/home.css";

const MAILCHIMP_SIGNUP_ACTION = (process.env.REACT_APP_MAILCHIMP_SIGNUP_ACTION || "").trim();

const Home = () => {
  const signupIsReady = MAILCHIMP_SIGNUP_ACTION.length > 0;

  return (
    <div className="home-screen w-full">
      <div className="home-container mx-auto">
        <div className="home-heading-row">
          <div>
            <p className="home-greeting-text">Fight week emails</p>
            <h1 className="home-title-text">UFC picks, twice on fight weeks</h1>
          </div>

          <p className="home-bio-text">
            Early reads go out Monday at noon Eastern. Updated picks go out
            Friday at noon Eastern when there is a weekend card.
          </p>
        </div>

        <section className="home-signup-shell" aria-labelledby="mailing-list-title">
          <div className="home-signup-copy">
            <p className="home-greeting-text">Mailing list</p>
            <h2 className="home-signup-title" id="mailing-list-title">
              Get the fight card report
            </h2>
            <p className="home-signup-text">
              The report includes every modeled matchup, odds, predicted winner,
              confidence, expected value, and pick/pass recommendation.
            </p>
          </div>

          {signupIsReady ? (
            <form
              className="home-signup-form"
              action={MAILCHIMP_SIGNUP_ACTION}
              method="post"
              target="_blank"
              noValidate
            >
              <label className="home-signup-label" htmlFor="mce-EMAIL">
                Email address
              </label>
              <div className="home-signup-controls">
                <input
                  className="home-signup-input"
                  id="mce-EMAIL"
                  name="EMAIL"
                  type="email"
                  autoComplete="email"
                  required
                />
                <button className="home-signup-button" type="submit">
                  Subscribe
                </button>
              </div>
            </form>
          ) : (
            <div className="home-signup-form">
              <p className="home-signup-text">
                Email signup will appear here after the Mailchimp embedded form
                URL is added to the frontend environment.
              </p>
            </div>
          )}
        </section>
      </div>
    </div>
  );
};

export default Home;
