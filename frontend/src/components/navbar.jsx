import React from "react";
import { Link, useLocation } from "react-router-dom";
import "../style/navbar.css";

const Navbar = () => {
  const location = useLocation();

  return (
    <div className="navbar w-full">
      <div className="navbar-content w-full">
        <Link to="/" className="navbar-title">
          Fight Result Predictor
        </Link>

        <ul className="navbar-buttons">
          <li className="navbar-item">
            <Link
              to="/"
              className={location.pathname === "/" ? "navbar-link active-link" : "navbar-link"}
            >
              Predictor
            </Link>
          </li>
          <li className="navbar-item">
            <Link
              to="/about"
              className={
                location.pathname === "/about" ? "navbar-link active-link" : "navbar-link"
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
