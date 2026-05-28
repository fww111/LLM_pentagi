package database

// DB returns the underlying DBTX (e.g. *sql.DB) used by this Queries instance.
func (q *Queries) DB() DBTX {
	return q.db
}
