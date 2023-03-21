package postgres

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/project-flogo/services/flow-state/store/metadata"
	"strconv"
	"strings"

	"github.com/project-flogo/flow/state"
	task2 "github.com/project-flogo/services/flow-state/store/task"
)

const (
	STEP_INSERT     = "INSERT INTO steps (flowinstanceid, stepid, taskname, status, starttime, endtime, stepdata) VALUES ($1,$2,$3,$4,$5,$6,$7);"
	SNAPSHOT_INSERT = "INSERT INTO snapshopt (flowinstanceid, hostid, stepid, starttime, endtime, stepdata) VALUES ($1,$2,$3,$4,$5,$6);"

	FlowState_UPSERT_RERUN = "INSERT INTO flowstate (flowInstanceId, userId, appName,appVersion, flowName, hostId, flowInput, flowOutput, rerunCount, startTime, endTime, status) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) ON CONFLICT (flowinstanceid) DO UPDATE SET hostId = EXCLUDED.hostId, flowName = EXCLUDED.flowName, userId = EXCLUDED.userId, status = EXCLUDED.status, flowInput = EXCLUDED.flowInput, flowOutput = EXCLUDED.flowOutput, rerunCount = EXCLUDED.rerunCount,  starttime=EXCLUDED.starttime,endtime= EXCLUDED.endtime;"

	UpdateFlowState = "UPDATE flowstate set endtime=$1, status=$2, flowInput=$3, flowOutput=$4, executiontime=ROUND( ((EXTRACT(EPOCH FROM ($1 - starttime)))*1000) :: numeric , 3) where flowinstanceid = $5;"

	UpsertSteps = "INSERT INTO steps (flowinstanceid, stepid, taskname, status, starttime, endtime, stepdata, subflowid, flowname, rerun) VALUES($1,$2,$3,$4,$5,$6,$7, $8, $9, $10) ON CONFLICT (flowinstanceid, stepid) DO UPDATE SET status = EXCLUDED.status, starttime=EXCLUDED.starttime,endtime= EXCLUDED.endtime,stepdata=EXCLUDED.stepdata;\n"
	DeleteSteps = "DELETE from steps where flowinstanceid = $1 and CAST(stepid as INTEGER) >= $2"

	UpsertAppState = "INSERT INTO appstate (userId, appName, persistenceenabled) VALUES($1,$2,$3) ON CONFLICT (userId, appName) DO UPDATE SET persistenceenabled = EXCLUDED.persistenceenabled ;\n"
)

type StatefulDB struct {
	db *sql.DB
}

func (s *StatefulDB) InsertFlowState(flowState *state.FlowState) (results *ResultSet, err error) {
	var flowInputs, flowOutputs []byte
	if flowState.FlowInputs != nil {
		flowInputs, _ = json.Marshal(flowState.FlowInputs)
	}
	if flowState.FlowOutputs != nil {
		flowOutputs, _ = json.Marshal(flowState.FlowOutputs)
	}
	inputArgs := []interface{}{flowState.FlowInstanceId, flowState.UserId, flowState.AppName, flowState.AppVersion, flowState.FlowName, flowState.HostId, flowInputs, flowOutputs, flowState.RerunCount, flowState.StartTime, flowState.EndTime, flowState.FlowStats}
	return s.insert(FlowState_UPSERT_RERUN, inputArgs)
}

func (s *StatefulDB) InsertAppState(appStatedata *metadata.Metadata) (results *ResultSet, err error) {
	inputArgs := []interface{}{appStatedata.Username, appStatedata.AppName, appStatedata.PersistEnabled}
	return s.insert(UpsertAppState, inputArgs)
}

func (s *StatefulDB) UpdateFlowState(flowState *state.FlowState) (results *ResultSet, err error) {
	var flowInputs, flowOutputs []byte
	if flowState.FlowInputs != nil {
		flowInputs, _ = json.Marshal(flowState.FlowInputs)
	}
	if flowState.FlowOutputs != nil {
		flowOutputs, _ = json.Marshal(flowState.FlowOutputs)
	}
	inputArgs := []interface{}{flowState.EndTime, flowState.FlowStats, flowInputs, flowOutputs, flowState.FlowInstanceId}
	return s.insert(UpdateFlowState, inputArgs)
}

func (s *StatefulDB) InsertSteps(step *state.Step) (results *ResultSet, err error) {
	//flowInstanceId := step.FlowInstId
	//hostID := step.HostId
	stepId := step.Id
	tasks, err := task2.StepToTask(step)
	var status, taskName, subflowid, flowname string
	rerun := step.Rerun

	if len(tasks) > 0 {
		status = string(tasks[0].Status)
		taskName = string(tasks[0].Id)
		subflowid = strconv.Itoa(tasks[0].SubflowId)
		flowname = tasks[0].Flowname
		if strings.Contains(flowname, ":") {
			flowname = flowname[strings.LastIndex(flowname, ":")+1:]
		}
	} else {
		return nil, fmt.Errorf("No Tasks Found")
	}

	//startTime := step.
	b, err := json.Marshal(step)
	if err != nil {
		return nil, err
	}
	stepData := decodeBytes(b)
	inputArgs := []interface{}{step.FlowId, stepId, taskName, status, step.StartTime, step.EndTime, stepData, subflowid, flowname, rerun}
	return s.insert(UpsertSteps, inputArgs)
}
func (s *StatefulDB) DeleteSteps(flowId string, stepId string) (results *ResultSet, err error) {
	intStepId, err := strconv.Atoi(stepId)
	if err != nil {
		return nil, fmt.Errorf("Error while converting stepid to Int: ", err)
	}
	inputArgs := []interface{}{flowId, intStepId}
	return s.delete(DeleteSteps, inputArgs)
}

func (s *StatefulDB) insert(insertSql string, inputArgs []interface{}) (results *ResultSet, err error) {

	// log.Debug("prepared insert [%s]", prepared)
	stmt, err := s.getStepStatement(insertSql)
	if err != nil {
		logCache.Errorf("Failed to prepare statement: %s", err)
		return nil, err
	}

	logCache.Debug("----------- DB Stats in Insert activity -----------")
	rows, err := stmt.Query(inputArgs...)
	if err != nil {
		logCache.Errorf("Executing prepared query got error: %s", err)
		// stmt.Close()
		return nil, err
	}
	if rows == nil {
		return nil, nil
	}
	defer rows.Close()
	return UnmarshalRows(rows)
}

func (s *StatefulDB) update(insertSql string, inputArgs []interface{}) (results *ResultSet, err error) {

	// log.Debug("prepared insert [%s]", prepared)
	stmt, err := s.getStepStatement(insertSql)
	if err != nil {
		logCache.Errorf("Failed to prepare statement: %s", err)
		return nil, err
	}

	logCache.Debug("----------- DB Stats in Insert activity -----------")
	rows, err := stmt.Query(inputArgs...)
	if err != nil {
		logCache.Errorf("Executing prepared query got error: %s", err)
		// stmt.Close()
		return nil, err
	}
	if rows == nil {
		return nil, nil
	}
	defer rows.Close()
	return UnmarshalRows(rows)
}

func (s *StatefulDB) query(querySql string, inputArgs []interface{}) (results *ResultSet, err error) {
	// log.Debug("prepared insert [%s]", prepared)
	stmt, err := s.getStepStatement(querySql)
	if err != nil {
		logCache.Errorf("Failed to prepare statement: %s", err)
		return nil, err
	}

	logCache.Debug("----------- DB Stats in query activity -----------")
	rows, err := stmt.Query(inputArgs...)
	if err != nil {
		logCache.Errorf("Executing prepared query got error: %s", err)
		// stmt.Close()
		return nil, err
	}
	if rows == nil {
		return nil, nil
	}
	defer rows.Close()
	return UnmarshalRows(rows)
}

func (s *StatefulDB) delete(deleteSql string, inputArgs []interface{}) (results *ResultSet, err error) {

	// log.Debug("prepared insert [%s]", prepared)
	stmt, err := s.getStepStatement(deleteSql)
	if err != nil {
		logCache.Errorf("Failed to prepare statement: %s", err)
		return nil, err
	}

	logCache.Debug("----------- DB Stats in Delete activity -----------")
	rows, err := stmt.Query(inputArgs...)
	if err != nil {
		logCache.Errorf("Executing prepared query got error: %s", err)
		// stmt.Close()
		return nil, err
	}
	if rows == nil {
		return nil, nil
	}
	defer rows.Close()
	return UnmarshalRows(rows)
}

// GetStatement
func (s *StatefulDB) getStepStatement(prepared string) (stmt *sql.Stmt, err error) {
	preparedQueryCacheMutex.Lock()
	defer preparedQueryCacheMutex.Unlock()
	stmt, ok := preparedQueryCache[prepared]
	if !ok {
		stmt, err = s.db.Prepare(prepared)
		if err != nil {
			return nil, err
		}
		preparedQueryCache[prepared] = stmt
	}
	return stmt, nil
}

func decodeBytes(blob []byte) []byte {
	decodedBlob, err := base64.StdEncoding.DecodeString(string(blob))
	if err != nil {
		return blob
	}
	return decodedBlob
}
