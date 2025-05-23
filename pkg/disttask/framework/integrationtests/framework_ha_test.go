// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package integrationtests

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/disttask/framework/testutil"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
)

func submitTaskAndCheckSuccessForHA(ctx context.Context, t *testing.T, taskKey string, testContext *testutil.TestContext) {
	submitTaskAndCheckSuccess(ctx, t, taskKey, "", testContext, map[proto.Step]int{
		proto.StepOne: 10,
		proto.StepTwo: 5,
	})
}

func TestHANodeRandomShutdown(t *testing.T) {
	c := testutil.NewDXFContextWithRandomNodes(t, 4, 15)
	registerExampleTask(t, c.MockCtrl, testutil.GetMockHATestSchedulerExt(c.MockCtrl), c.TestContext, nil)

	// we keep [1, 10] nodes running, as we only have 10 subtask at stepOne
	keepCount := int(math.Min(float64(c.NodeCount()-1), float64(c.Rand.Intn(10)+1)))
	nodeNeedDown := c.GetRandNodeIDs(c.NodeCount() - keepCount)
	t.Logf("started %d nodes, and we keep %d nodes, nodes that need shutdown: %v", c.NodeCount(), keepCount, nodeNeedDown)
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockTiDBShutdown",
		func(e taskexecutor.TaskExecutor, execID string, _ *proto.TaskBase) {
			if _, ok := nodeNeedDown[execID]; ok {
				c.AsyncShutdown(execID)
				e.Cancel()
			}
		},
	)
	submitTaskAndCheckSuccessForHA(c.Ctx, t, "😊", c.TestContext)
}

func TestHARandomShutdownInDifferentStep(t *testing.T) {
	c := testutil.NewDXFContextWithRandomNodes(t, 6, 15)

	registerExampleTask(t, c.MockCtrl, testutil.GetMockHATestSchedulerExt(c.MockCtrl), c.TestContext, nil)
	// they might overlap, but will leave at least 2 nodes running
	nodeNeedDownAtStepOne := c.GetRandNodeIDs(c.NodeCount()/2 - 1)
	nodeNeedDownAtStepTwo := c.GetRandNodeIDs(c.NodeCount()/2 - 1)
	t.Logf("started %d nodes, shutdown nodes at step 1: %v, shutdown nodes at step 2: %v",
		c.NodeCount(), nodeNeedDownAtStepOne, nodeNeedDownAtStepTwo)
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockTiDBShutdown",
		func(e taskexecutor.TaskExecutor, execID string, task *proto.TaskBase) {
			var targetNodes map[string]struct{}
			switch task.Step {
			case proto.StepOne:
				targetNodes = nodeNeedDownAtStepOne
			case proto.StepTwo:
				targetNodes = nodeNeedDownAtStepTwo
			default:
				return
			}
			if _, ok := targetNodes[execID]; ok {
				c.AsyncShutdown(execID)
				e.Cancel()
			}
		},
	)
	submitTaskAndCheckSuccessForHA(c.Ctx, t, "😊", c.TestContext)
}

func TestHAMultipleOwner(t *testing.T) {
	c := testutil.NewDXFContextWithRandomNodes(t, 4, 8)
	prevCount := c.NodeCount()
	additionalOwnerCnt := c.Rand.Intn(2) + 1
	for i := range additionalOwnerCnt {
		c.ScaleOutBy(fmt.Sprintf("tidb-%d", i), true)
	}
	require.Equal(t, prevCount+additionalOwnerCnt, c.NodeCount())

	registerExampleTask(t, c.MockCtrl, testutil.GetMockHATestSchedulerExt(c.MockCtrl), c.TestContext, nil)
	var wg util.WaitGroupWrapper
	for i := range 10 {
		taskKey := fmt.Sprintf("key%d", i)
		wg.Run(func() {
			submitTaskAndCheckSuccessForHA(c.Ctx, t, taskKey, c.TestContext)
		})
	}
	wg.Wait()
}

// TODO add a case of real network partition, each owner should see different set of live nodes
