import SearchResult from './SearchResult';
import './SearchResults.css';

function SearchResults({ results, loading, error, total, query, page, limit, onPageChange }) {
  if (loading) {
    return (
      <div className="search-results">
        <div className="loading">Loading results...</div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="search-results">
        <div className="error">
          <p>Error: {error}</p>
          <p>Please check your connection and try again.</p>
        </div>
      </div>
    );
  }

  const totalPages = Math.ceil(total / limit);

  return (
    <div className="search-results">
      <div className="results-stats">
        About {total} results ({(page * limit - limit + 1).toLocaleString()} - {Math.min(page * limit, total).toLocaleString()})
      </div>

      {results.length === 0 ? (
        <div className="no-results">
          <p>No results found for "{query}"</p>
          <p>Try different keywords or check your spelling.</p>
        </div>
      ) : (
        <>
          <div className="results-list">
            {results.map((result) => (
              <SearchResult key={result.id} result={result} />
            ))}
          </div>

          {totalPages > 1 && (
            <div className="pagination">
              <button
                className="page-button"
                disabled={page <= 1}
                onClick={() => onPageChange(page - 1)}
              >
                Previous
              </button>
              <span className="page-info">
                Page {page} of {totalPages}
              </span>
              <button
                className="page-button"
                disabled={page >= totalPages}
                onClick={() => onPageChange(page + 1)}
              >
                Next
              </button>
            </div>
          )}
        </>
      )}
    </div>
  );
}

export default SearchResults;
