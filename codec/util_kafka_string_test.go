package codec

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestReadFindCoordinatorErrorMessage(t *testing.T) {
	bytes := testHex2Bytes(t, "0000000000")
	msg, idx := readFindCoordinatorErrorMessage(bytes, 0)
	assert.Nil(t, msg)
	assert.Equal(t, 1, idx)
}
