// db/job_summary.go

package db

import (
	"context"
	"encoding/json"
	"gokafka-raw/internal/model"
	"strconv"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

func InsertJobSummary(ctx context.Context, pool *pgxpool.Pool, msg model.JobSummaryMessage, logger *zap.SugaredLogger) error {
	var outputMap map[string]any
	if len(msg.Output) > 0 {
		if err := json.Unmarshal(msg.Output, &outputMap); err != nil {
			logger.Errorw("failed to unmarshal job output", "error", err)
			return err
		}
	}
	if outputMap == nil {
		outputMap = map[string]any{}
	}

	outputMap = sanitizeNulls(outputMap).(map[string]any)

	totalOutput := toNumeric(outputMap["total_output"])
	goodOutput := toNumeric(outputMap["good_output"])
	rejectOutput := toNumeric(outputMap["reject_output"])
	machineRejectOutput := toNumeric(outputMap["machine_reject_output"])

	// model_name has its own column — fall back to product_name if the
	// job payload doesn't send model_name explicitly. CONFIRM: is
	// product_name (商品名 from the HMI) actually the right value for
	// this column, or are model_name and product_name different concepts
	// in your process (e.g. model_name = machine/process recipe vs
	// product_name = the specific filled item)?
	modelName, _ := outputMap["model_name"].(string)
	if modelName == "" {
		modelName, _ = outputMap["product_name"].(string)
	}

	// Strip everything with a dedicated column — what's left (operator,
	// product_code, ink_name, ink_lot, reject breakdown, room_temp,
	// ink_temp, filling_code) is exactly what meta is for.
	delete(outputMap, "total_output")
	delete(outputMap, "good_output")
	delete(outputMap, "reject_output")
	delete(outputMap, "model_name")
	delete(outputMap, "machine_reject_output")

	metaJSON, err := json.Marshal(outputMap)
	if err != nil {
		logger.Errorw("failed to marshal job meta", "error", err)
		return err
	}

	_, err = pool.Exec(ctx, `
    INSERT INTO analytics.job_summary
        (tenant_id, device_id, lot_id, job_ref, model_name,
         started_at, ended_at,
         total_output, good_output, reject_output, machine_reject_output,
         status, meta)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'pending',$12)
    ON CONFLICT (tenant_id, job_ref) DO UPDATE SET
        total_output           = EXCLUDED.total_output,
        good_output            = EXCLUDED.good_output,
        reject_output          = EXCLUDED.reject_output,
        machine_reject_output  = EXCLUDED.machine_reject_output,
        model_name    = COALESCE(NULLIF(EXCLUDED.model_name, ''), analytics.job_summary.model_name),
        meta          = EXCLUDED.meta,
        status        = 'corrected',
        posted_at     = now()
`,
		msg.TenantID, msg.DeviceID, nullUUID(msg.LotID), msg.JobRef, modelName,
		msg.StartedAt, msg.EndedAt, totalOutput, goodOutput, rejectOutput, machineRejectOutput, string(metaJSON),
	)
	if err != nil {
		logger.Errorw("failed to insert job summary", "error", err)
		return err
	}
	return nil
}

func toNumeric(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case json.Number:
		f, _ := n.Float64()
		return f
	case string:
		f, _ := strconv.ParseFloat(n, 64)
		return f
	default:
		return 0
	}
}
