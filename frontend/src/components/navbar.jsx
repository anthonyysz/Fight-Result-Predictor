import React from "react";
import { Link, useLocation } from "react-router-dom";
import "../style/navbar.css";

const Navbar = () => {
  const location = useLocation();
  const isAboutPage = location.pathname === "/" || location.pathname === "/about";
//Setting up our navbar with routing
  return (
    <div className="navbar w-full">
      <div className="navbar-content w-full">
        <Link to="/" className="navbar-title">
          Fight Result Predictor
        </Link>
        <ul className="navbar-buttons">
          <li className="navbar-item">
            <Link
              to="/stats"
              className={location.pathname === "/stats" ? "navbar-link active-link" : "navbar-link"}
            >
              Stats
            </Link>
          </li>
          <li className="navbar-item">
            <Link
              to="/"
              className={
                isAboutPage ? "navbar-link active-link" : "navbar-link"
              }
            >
              About
            </Link>
          </li>
        </ul>
      </div>
    </div>
  );
};

export default Navbar;
