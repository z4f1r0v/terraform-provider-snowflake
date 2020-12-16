package resources

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"strings"
)

type fileFormatID struct {
	DatabaseName   string
	SchemaName     string
	FileFormatName string
}

// String() takes in a stageID object and returns a pipe-delimited string:
// DatabaseName|SchemaName|StageName
func (si *fileFormatID) String() (string, error) {
	var buf bytes.Buffer
	csvWriter := csv.NewWriter(&buf)
	csvWriter.Comma = stageIDDelimiter
	dataIdentifiers := [][]string{{si.DatabaseName, si.SchemaName, si.FileFormatName}}
	err := csvWriter.WriteAll(dataIdentifiers)
	if err != nil {
		return "", err
	}
	strStageID := strings.TrimSpace(buf.String())
	return strStageID, nil
}

func fileFormatIDFromString(stringID string) (*fileFormatID, error) {
	reader := csv.NewReader(strings.NewReader(stringID))
	reader.Comma = '|'
	lines, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("Not CSV compatible")
	}

	if len(lines) != 1 {
		return nil, fmt.Errorf("1 line per stage")
	}
	if len(lines[0]) != 3 {
		return nil, fmt.Errorf("3 fields allowed")
	}

	fileFormat := &fileFormatID{
		DatabaseName:   lines[0][0],
		SchemaName:     lines[0][1],
		FileFormatName: lines[0][2],
	}
	return fileFormat, nil
}
