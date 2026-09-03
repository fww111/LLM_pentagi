package providers

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"pentagentx/pkg/database"
)

// dbAccessor gives access to the raw DB handle behind a sqlc Querier.
type dbAccessor interface {
	DB() database.DBTX
}

// multiAgentQueries returns a MultiAgentQueries bound to the flow provider's DB.
func (fp *flowProvider) multiAgentQueries() (*database.MultiAgentQueries, error) {
	accessor, ok := fp.db.(dbAccessor)
	if !ok {
		return nil, fmt.Errorf("database querier does not expose raw DB access")
	}
	return database.NewMultiAgentQueries(accessor.DB()), nil
}

func decodeStringList(raw json.RawMessage) []string {
	if len(raw) == 0 {
		return nil
	}
	var values []string
	if err := json.Unmarshal(raw, &values); err != nil {
		return nil
	}
	return values
}

func rawJSONToString(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	return string(raw)
}

func nullString(ns sql.NullString) string {
	if !ns.Valid {
		return ""
	}
	return ns.String
}
