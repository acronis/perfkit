package sql

import (
	"context"
	"fmt"

	"github.com/acronis/perfkit/db"
)

// sqlRecommendationsSource implements database recommendations functionality
type sqlRecommendationsSource struct {
	q querier // Interface for executing database queries
	d dialect // Interface for dialect-specific operations
}

// newSQLRecommendationsSource creates a new recommendations source
// Parameters:
//   - q: querier interface for database operations
//   - d: dialect interface for database-specific functionality
//
// Returns:
//   - db.RecommendationsSource implementation
func newSQLRecommendationsSource(q querier, d dialect) db.RecommendationsSource {
	return &sqlRecommendationsSource{q: q, d: d}
}

// Recommendations returns database configuration recommendations
// Uses both dialect-specific recommendations and dynamic checks
// Implementation details:
//   - Gets base recommendations from dialect interface
//   - For PostgreSQL: Checks replication status and WAL settings
//   - Returns nil if no recommendations are available
//
// Returns:
//   - []db.Recommendation: List of recommended settings
//   - error: Any error that occurred during the check
func (s *sqlRecommendationsSource) Recommendations() ([]db.Recommendation, error) {
	// Get dialect-specific recommendations using dialect interface
	var recommendations = s.d.recommendations()

	if len(recommendations) == 0 {
		return nil, nil
	}

	// Handle dialect-specific dynamic recommendations
	switch s.d.name() {
	case db.POSTGRES:
		var rowNum uint64

		// Use querier interface to check replication status
		var row = s.q.queryRowContext(
			context.Background(),
			"SELECT COUNT(*) FROM pg_stat_replication;",
		)

		if err := row.Scan(&rowNum); err != nil {
			return nil, fmt.Errorf(
				"db: cannot get rows count in table '%s': %v",
				"pg_stat_replication",
				err,
			)
		}

		// Add WAL level recommendation based on replication status
		var r = db.Recommendation{
			Setting:       "wal_level",
			Meaning:       "amount of information written to WAL",
			ExpectedValue: "replica",
		}
		if rowNum == 0 {
			r.ExpectedValue = "minimal"
		}
		recommendations = append(recommendations, r)

		// Add max_wal_senders recommendation for non-replicated setups
		if rowNum == 0 {
			r = db.Recommendation{
				Setting:       "max_wal_senders",
				Meaning:       "max num of simultaneous replication connections",
				ExpectedValue: "0",
			}
			recommendations = append(recommendations, r)
		}
	default:
		// Other dialects use only static recommendations from dialect.recommendations()
	}

	return recommendations, nil
}
