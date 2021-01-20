package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"
)

type KSQLAPI interface {
	Query(ctx context.Context, statement string) (*RowResult, error)
	CreateStream(ctx context.Context, statement string, streamProperties map[string]string) (*CreateStreamResult, error)
	Exec(ctx context.Context, statement string, streamProperties map[string]string) error
	Describe(ctx context.Context, statement string, streamProperties map[string]string) (*DescribeResult, error)
}

var (
	StreamPropertiesOffsetEarliest = map[string]string{"ksql.streams.auto.offset.reset": "earliest"}
	pathQuery                      = "/query"
	pathExec                       = "/ksql"
	QueryWithNoResults             = errors.New("query with no results")
)

type QueryRequest struct {
	KSQL string `json:"ksql"`
}

type ExecRequest struct {
	KSQL             string            `json:"ksql"`
	StreamProperties map[string]string `json:"streamsProperties"`
}

type ResultStatus string

const (
	ResultStatusQueued     ResultStatus = "QUEUED"
	ResultStatusParsing    ResultStatus = "PARSING"
	ResultStatusExecuting  ResultStatus = "EXECUTING"
	ResultStatusTerminated ResultStatus = "TERMINATED"
	ResultStatusSuccess    ResultStatus = "SUCCESS"
	ResultStatusError      ResultStatus = "ERROR"
)

type CreateStreamResult struct {
	CurrentStatus struct {
		StatementTest string `json:"statementTest"`
		CommandId     string `json:"commandId"`
		CommandStatus struct {
			Status  ResultStatus `json:"status"`
			Message string       `json:"message"`
		}
	} `json:"currentStatus"`
}

type DescribeResult struct {
	StatementText     string `json:"statementText"`
	SourceDescription struct {
		Name         string `json:"name"`
		WriteQueries []struct {
			Id string `json:"id"`
		} `json:"writeQueries"`
		Type        string `json:"type"`
		Key         string `json:"key"`
		Timestamp   string `json:"timestamp"`
		KafkaTopic  string `json:"topic"`
		Extended    bool   `json:"extended"`
		Statistics  string `json:"statistics"`
		ErrorStats  string `json:"errorStats"`
		Replication int    `json:"replication"`
		Partitions  int    `json:"partitions"`
	} `json:"sourceDescription"`
}

type RowResult struct {
	Row struct {
		Columns []string `json:"columns"`
	} `json:"row"`
	ErrorMessage *string `json:"errorMessage"`
}

type ErrorResult struct {
	Message string `json:"message"`
}

func NewKSQLAPI(host string) (KSQLAPI, error) {
	logfile, err := os.Create(fmt.Sprintf("httpcalls-%d-%d-%d.log", time.Now().Hour(), time.Now().Minute(), time.Now().Second()))
	if err != nil {
		fmt.Println("could not create logs file")
		return nil, err
	}

	return &ksqlAPI{
		host:       host,
		httpClient: http.DefaultClient,
		logger:     log.New(logfile, "", log.LstdFlags),
	}, nil
}

type ksqlAPI struct {
	host       string
	httpClient *http.Client
	logger     *log.Logger
}

func (k ksqlAPI) Query(ctx context.Context, statement string) (*RowResult, error) {
	path := fmt.Sprintf("%s%s", k.host, pathQuery)
	if statement == "" {
		return nil, errors.New("empty statement")
	}

	queryRequest := QueryRequest{KSQL: statement}
	marshal, err := json.Marshal(queryRequest)
	if err != nil {
		return nil, err
	}

	resultBytes, err := k.postToAPI(path, marshal)
	if err != nil {
		return nil, err
	}
	result := make([]RowResult, 0)
	err = json.Unmarshal(resultBytes, &result)
	if err != nil {
		return nil, err
	}

	for _, rowResult := range result {
		if rowResult.ErrorMessage != nil {
			return nil, errors.New(*rowResult.ErrorMessage)
		}
		if rowResult.Row.Columns != nil {
			if len(rowResult.Row.Columns) > 0 {
				return &rowResult, nil
			}
		}
	}
	return nil, QueryWithNoResults
}

func (k ksqlAPI) postToAPI(path string, body []byte) ([]byte, error) {
	post, err := k.httpClient.Post(path, "application/json", bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}
	defer func() {
		post.Body.Close()
	}()
	all, err := ioutil.ReadAll(post.Body)
	k.logger.Println(string(all))
	if post.StatusCode > 299 {
		res := ErrorResult{}
		_ = json.Unmarshal(all, &res)
		return nil, errors.New("invalid http status, please check http logs file: " + res.Message)
	}
	return all, nil
}

func (k ksqlAPI) CreateStream(ctx context.Context, statement string, streamProperties map[string]string) (*CreateStreamResult, error) {
	path := fmt.Sprintf("%s%s", k.host, pathExec)
	if statement == "" {
		return nil, errors.New("empty statement")
	}

	execRequest := ExecRequest{KSQL: statement, StreamProperties: streamProperties}
	marshal, err := json.Marshal(execRequest)
	if err != nil {
		return nil, err
	}
	resultBytes, err := k.postToAPI(path, marshal)
	if err != nil {
		return nil, err
	}

	result := make([]CreateStreamResult, 0)
	err = json.Unmarshal(resultBytes, &result)
	if err != nil {
		return nil, err
	}
	for _, streamResult := range result {
		return &streamResult, nil
	}
	return nil, QueryWithNoResults
}

func (k ksqlAPI) Exec(ctx context.Context, statement string, streamProperties map[string]string) error {
	path := fmt.Sprintf("%s%s", k.host, pathExec)
	if statement == "" {
		return errors.New("empty statement")
	}

	execRequest := ExecRequest{KSQL: statement, StreamProperties: streamProperties}
	marshal, err := json.Marshal(execRequest)
	if err != nil {
		return err
	}

	if isDryRun(ctx) {
		fmt.Printf("[dry-run] %s\n", string(marshal))
		return nil
	}
	_, err = k.postToAPI(path, marshal)
	if err != nil {
		return err
	}
	return nil
}

func (k ksqlAPI) Describe(ctx context.Context, statement string, streamProperties map[string]string) (*DescribeResult, error) {
	path := fmt.Sprintf("%s%s", k.host, pathExec)
	if statement == "" {
		return nil, errors.New("empty statement")
	}

	execRequest := ExecRequest{KSQL: statement}
	marshal, err := json.Marshal(execRequest)
	if err != nil {
		return nil, err
	}
	resultBytes, err := k.postToAPI(path, marshal)
	if err != nil {
		return nil, err
	}

	result := make([]DescribeResult, 0)
	err = json.Unmarshal(resultBytes, &result)
	if err != nil {
		return nil, err
	}
	for _, describeResult := range result {
		return &describeResult, nil
	}
	return nil, QueryWithNoResults
}
