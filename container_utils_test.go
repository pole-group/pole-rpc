// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pole_rpc

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewSet(t *testing.T) {
	s := NewSet()

	arr := []interface{}{"1", "2", "3"}

	s.AddAll(arr...)

	assert.True(t, s.Size() == len(arr))
}
