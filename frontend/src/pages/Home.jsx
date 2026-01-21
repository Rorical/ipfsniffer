import Logo from '../components/Logo';
import SearchBar from '../components/SearchBar';
import './Home.css';

function Home() {
  return (
    <div className="home">
      <div className="home-logo">
        <Logo size="large" />
        <h1>IPFS Search</h1>
      </div>
      <SearchBar autoFocus />
      <div className="home-footer">
        <p>Search across the InterPlanetary File System</p>
      </div>
    </div>
  );
}

export default Home;
