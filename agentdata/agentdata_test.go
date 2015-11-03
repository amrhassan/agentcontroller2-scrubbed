package agentdata_test
import (
	"testing"
	"github.com/Jumpscale/agentcontroller2/agentdata"
	"github.com/stretchr/testify/assert"
	"github.com/Jumpscale/agentcontroller2/core"
)

// Note: These tests don't validate the thread-safety aspect of the implementation

func TestAgentData(t *testing.T) {

	d := agentdata.NewAgentData()

	assert.Empty(t, d.ConnectedAgents())

	id := core.AgentID{GID: 0, NID: 42}
	id2 := core.AgentID{GID: 0, NID: 23}

	assert.Nil(t, d.GetRoles(id))
	assert.Nil(t, d.GetRoles(id2))

	dummyRoles := []core.AgentRole {"dummy", "slave"}

	d.SetRoles(core.AgentID{GID: 0, NID: 42}, dummyRoles)

	assert.Equal(t, d.GetRoles(id), dummyRoles)
	assert.Equal(t, d.ConnectedAgents(), []core.AgentID{id})

	dummyRoles2 := []core.AgentRole {"node", "super"}

	d.SetRoles(core.AgentID{GID: 0, NID: 23}, dummyRoles2)

	assert.Equal(t, d.GetRoles(id2), dummyRoles2)
	assert.Equal(t, d.GetRoles(id), dummyRoles)
	assert.Contains(t, d.ConnectedAgents(), id)
	assert.Contains(t, d.ConnectedAgents(), id2)

	assert.True(t, d.HasRole(id, "slave"))
	assert.True(t, d.HasRole(id, "dummy"))
	assert.False(t, d.HasRole(id, "super"))
	assert.True(t, d.HasRole(id2, "super"))
	assert.True(t, d.HasRole(id2, "node"))
	assert.False(t, d.HasRole(id2, "slave"))

	assert.False(t, d.HasRole(core.AgentID{GID: 1, NID: 42}, "node"))
}
