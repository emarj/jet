package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	"github.com/go-jet/jet/v2/qrm"
	"github.com/stretchr/testify/require"
)

func TestNestedDoubleJoin(t *testing.T) {
	schema := `CREATE TABLE entity (
		id INTEGER PRIMARY KEY,
		name TEXT NOT NULL
	);
	CREATE TABLE account (
		id INTEGER PRIMARY KEY,
		name TEXT NOT NULL,
		owner_id INTEGER NOT NULL
	);
	CREATE TABLE operation (
		id INTEGER PRIMARY KEY,
		from_id INTEGER NOT NULL,
		to_id INTEGER NOT NULL,
		amount INTEGER NOT NULL
	);
	
	INSERT INTO entity (id,name) VALUES (1,"user1"),(2,"user2");
	INSERT INTO account (id,name,owner_id) VALUES (1,"acc1",1),(2,"acc2",2),(3,"acc3",2);
	INSERT INTO operation (from_id,to_id,amount) VALUES (1,2,100);
	INSERT INTO operation (from_id,to_id,amount) VALUES (2,3,50);`

	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)

	_, err = db.Exec(schema)
	require.NoError(t, err)

	/* From := j.Account.AS("from")
	To := j.Account.AS("to")

	FromEntity := j.Entity.AS("from.entity")
	ToEntity := j.Entity.AS("to.entity")

	query := jet.SELECT(
		j.Operation.AllColumns,
		From.AllColumns,
		To.AllColumns,
		FromEntity.AllColumns,
		ToEntity.AllColumns,
	).FROM(
		j.Operation.INNER_JOIN(
			From,
			From.ID.EQ(j.Operation.FromID),
		).INNER_JOIN(
			To,
			To.ID.EQ(j.Operation.ToID),
		).INNER_JOIN(
			FromEntity,
			FromEntity.ID.EQ(From.OwnerID),
		).INNER_JOIN(
			ToEntity,
			ToEntity.ID.EQ(To.OwnerID),
		),
	).WHERE(
		FromEntity.ID.EQ(jet.Int(int64(eID))).OR(ToEntity.ID.EQ(jet.Int(int64(eID)))),
	) */

	query := `SELECT operation.id AS "operation.id",
	operation.from_id AS "operation.from_id",
	operation.to_id AS "operation.to_id",
	operation.amount AS "operation.amount",
	'from'.id AS "from.id",
	'from'.name AS "from.name",
	'from'.owner_id AS "from.owner_id",
	'to'.id AS "to.id",
	'to'.name AS "to.name",
	'to'.owner_id AS "to.owner_id",
	'from.entity'.id AS "from.entity.id",
	'from.entity'.name AS "from.entity.name",
	'to.entity'.id AS "to.entity.id",
	'to.entity'.name AS "to.entity.name"
FROM operation
	INNER JOIN account AS 'from' ON ('from'.id = operation.from_id)
	INNER JOIN account AS 'to' ON ('to'.id = operation.to_id)
	INNER JOIN entity AS 'from.entity' ON ('from.entity'.id = 'from'.owner_id)
	INNER JOIN entity AS 'to.entity' ON ('to.entity'.id = 'to'.owner_id)
WHERE ('from.entity'.id = :id) OR ('to.entity'.id = :id);`

	type Entity struct {
		ID   int    `json:"id" sql:"primary_key"`
		Name string `json:"name"`
	}

	type Account struct {
		ID    int    `json:"id" sql:"primary_key"`
		Name  string `json:"name"`
		Owner Entity `json:"owner"`
	}
	type Operation struct {
		ID     int     `json:"id" sql:"primary_key"`
		From   Account `json:"from" alias:"from"`
		To     Account `json:"to" alias:"to"`
		Amount int     `json:"amount"`
	}

	args := []interface{}{sql.Named("id", 2)}

	var ops []Operation
	qrm.Query(context.Background(), db, query, args, &ops)
	require.NoError(t, err)

	require.Equal(t, len(ops), 2)
	require.NotNil(t, ops[0].From.Owner)
	require.Equal(t, ops[0].From.Owner.ID, 1)
	require.Equal(t, ops[0].From.Owner.Name, "user1")

	gotJSON, err := json.Marshal(ops)
	require.NoError(t, err)

	expectedJSON := `
	[
        {
                "id": 1,
                "from": {
                        "id": 1,
                        "name": "acc1",
                        "owner": {
                                "id": 1,
                                "name": "user1"
                        }
                },
                "to": {
                        "id": 2,
                        "name": "acc2",
                        "owner": {
                                "id": 2,
                                "name": "user2"
                        }
                },
                "amount": 100
        },
        {
                "id": 2,
                "from": {
                        "id": 2,
                        "name": "acc2",
                        "owner": {
                                "id": 2,
                                "name": "user2"
                        }
                },
                "to": {
                        "id": 3,
                        "name": "acc3",
                        "owner": {
                                "id": 2,
                                "name": "user2"
                        }
                },
                "amount": 50
        }
]
`

	require.JSONEq(t, expectedJSON, string(gotJSON))

}
