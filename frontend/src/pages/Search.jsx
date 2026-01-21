import { useState, useEffect } from 'react';
import { useSearchParams } from 'react-router-dom';
import Logo from '../components/Logo';
import SearchBar from '../components/SearchBar';
import SearchResults from '../components/SearchResults';
import { search } from '../services/api';
import './Search.css';

function Search() {
  const [searchParams] = useSearchParams();
  const query = searchParams.get('q') || '';
  const [results, setResults] = useState([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [page, setPage] = useState(1);
  const limit = 10;

  useEffect(() => {
    if (!query) return;

    const fetchResults = async () => {
      setLoading(true);
      setError(null);

      try {
        const data = await search(query, { page, limit });
        setResults(data.hits || []);
        setTotal(data.total?.value || 0);
      } catch (err) {
        setError(err.message);
      } finally {
        setLoading(false);
      }
    };

    fetchResults();
  }, [query, page]);

  const handlePageChange = (newPage) => {
    setPage(newPage);
    window.scrollTo(0, 0);
  };

  return (
    <div className="search-page">
      <div className="search-page-header">
        <a href="/" className="search-page-logo">
          <Logo size="small" />
          <span>IPFS Search</span>
        </a>
        <div className="search-page-bar">
          <SearchBar initialQuery={query} />
        </div>
      </div>
      <SearchResults
        results={results}
        loading={loading}
        error={error}
        total={total}
        query={query}
        page={page}
        limit={limit}
        onPageChange={handlePageChange}
      />
    </div>
  );
}

export default Search;
