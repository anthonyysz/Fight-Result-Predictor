import Home from "./components/home";
import Navbar from "./components/navbar";
import About from "./components/about";
import { Route, Routes } from "react-router-dom";

function MainPage() {
  return (
    <>
      <Navbar />
      <Home />
    </>
  );
}

function AboutPage() {
  return (
    <>
      <Navbar />
      <About />
    </>
  );
}

function App() {
  return (
    <Routes>
      <Route path="/" element={<MainPage />} />
      <Route path="/about" element={<AboutPage />} />
    </Routes>
  );
}

export default App;
