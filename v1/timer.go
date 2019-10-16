package machinery

import (
	"github.com/piaobeizu/cron"
	"github.com/piaobeizu/machinery/v1/tasks"
)

var CronList = make(map[*tasks.Signature][]cron.Cron)
