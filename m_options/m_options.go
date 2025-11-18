package m_options

import (
	"database/sql"
	"fmt"

	"github.com/rsmrtk/smartlg/logger"
)

type Options struct {
	Log *logger.Logger
	DB  *sql.DB
}

func (o Options) IsValid() error {
	if o == (Options{}) {
		return fmt.Errorf("options is empty")
	}
	if o.Log == nil {
		return fmt.Errorf("log is nil")
	}
	if o.DB == nil {
		return fmt.Errorf("db is nil")
	}
	return nil
}
