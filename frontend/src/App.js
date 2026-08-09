import Navbar from "./components/navbar";
import About from "./components/about";
import Stats from "./components/stats";
import { Route, Routes } from "react-router-dom";

function AboutPage() {
  return (
    <>
      <Navbar />
      <About />
    </>
  );
}

function StatsPage() {
  return (
    <>
      <Navbar />
      <Stats />
    </>
  );
}

function App() {
  return (
    <Routes>
      <Route path="/" element={<AboutPage />} />
      <Route path="/about" element={<AboutPage />} />
      <Route path="/stats" element={<StatsPage />} />
    </Routes>
  );
}

export default App;
