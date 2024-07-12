package sql

import (
	"context"
	"fmt"

	"github.com/acronis/perfkit/db"
)

type sqlRecommendationsSource struct {
	q querier
	d dialect
}

func newSQLRecommendationsSource(q querier, d dialect) db.RecommendationsSource {
	return &sqlRecommendationsSource{q: q, d: d}
}

// Recommendations prints DB recommendations to stdout
func (s *sqlRecommendationsSource) Recommendations() ([]db.Recommendation, error) {
	var recommendations = s.d.recommendations()

	if len(recommendations) == 0 {
		return nil, nil
	}

	switch s.d.name() {
	case db.POSTGRES:
		var rowNum uint64

		var row = s.q.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM pg_stat_replication;")

		if err := row.Scan(&rowNum); err != nil {
			return nil, fmt.Errorf("db: cannot get rows count in table '%s': %v", "pg_stat_replication", err)
		}

		var r = db.Recommendation{Setting: "wal_level", Meaning: "amount of information written to WAL", ExpectedValue: "replica"}
		if rowNum == 0 {
			r.ExpectedValue = "minimal"
		}
		recommendations = append(recommendations, r)

		if rowNum == 0 {
			r = db.Recommendation{Setting: "max_wal_senders", Meaning: "max num of simultaneous replication connections", ExpectedValue: "0"}
			recommendations = append(recommendations, r)
		}
	default:
	}

	return recommendations, nil
}
