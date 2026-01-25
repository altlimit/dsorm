package dsorm_test

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/altlimit/dsorm"
)

// Global test DB instance for convenience, or strictly local?
// Let's use a global one initialized in TestMain for simplicity, mimicking previous behavior
var testDB *dsorm.Client

// TestMain setups the environment for tests
func TestMain(m *testing.M) {
	flag.Parse()

	// Setup Emulator
	os.Setenv("DATASTORE_EMULATOR_HOST", "localhost:8081")
	os.Setenv("DATASTORE_PROJECT_ID", "app-test")
	// Setup Encryption Key
	os.Setenv("DATASTORE_ENCRYPTION_KEY", "12345678901234567890123456789012") // 32 bytes

	// Initialize DB
	ctx := context.Background()
	var err error
	testDB, err = dsorm.New(ctx)
	if err != nil {
		panic(err)
	}

	code := m.Run()
	os.Exit(code)
}

// ------------------------------------------------------------------
// Test Models
// ------------------------------------------------------------------

type LifecycleModel struct {
	dsorm.Base
	ID     int64 `model:"id"`
	Value  string
	Events []string `datastore:"-"`
}

func (m *LifecycleModel) BeforeSave(ctx context.Context, model dsorm.Model) error {
	m.Events = append(m.Events, "BeforeSave")
	return nil
}

func (m *LifecycleModel) AfterSave(ctx context.Context) error {
	m.Events = append(m.Events, "AfterSave")
	return nil
}

func (m *LifecycleModel) OnLoad(ctx context.Context) error {
	m.Events = append(m.Events, "OnLoad")
	return nil
}

type KeyMappingModel struct {
	dsorm.Base
	ID     string         `model:"id"`
	Parent *datastore.Key `model:"parent"`
	NS     string         `model:"ns"`
}

type EncryptionModel struct {
	dsorm.Base
	ID        int64  `model:"id"`
	Secret    string `marshal:"secret" encrypt:"" datastore:"-"`
	AltSecret string `marshal:"alt_secret" encrypt:"MTIzNDU2Nzg5MDEyMzQ1Ng==" datastore:"-"` // 1234567890123456 base64 encoded
}

type JSONModel struct {
	dsorm.Base
	ID   int64             `model:"id"`
	Data map[string]string `marshal:"data" datastore:"-"`
}

type DatastoreTagModel struct {
	dsorm.Base
	ID         int64  `model:"id"`
	Ignored    string `datastore:"-"`
	Renamed    string `datastore:"custom_name"`
	NotIndexed string `datastore:",noindex"`
	Indexed    string // Indexed by default
}

// ------------------------------------------------------------------
// Tests
// ------------------------------------------------------------------

func TestModelLifecycle(t *testing.T) {
	ctx := context.Background()

	m := &LifecycleModel{Value: "lifecycle"}
	m.Init(ctx, m)

	if !m.IsNew() {
		t.Error("IsNew() should be true for new model")
	}

	// Put triggers BeforeSave and AfterSave
	if err := testDB.Put(ctx, m); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	if len(m.Events) != 2 {
		t.Errorf("Expected 2 events (BeforeSave, AfterSave), got %v", m.Events)
	} else {
		if m.Events[0] != "BeforeSave" {
			t.Errorf("Expected event 0 to be BeforeSave, got %s", m.Events[0])
		}
		if m.Events[1] != "AfterSave" {
			t.Errorf("Expected event 1 to be AfterSave, got %s", m.Events[1])
		}
	}

	m.IsNew()

	fetched := &LifecycleModel{
		ID: m.ID,
	}
	if err := testDB.Get(ctx, fetched); err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if fetched.IsNew() {
		t.Error("IsNew() should be false for fetched model")
	}

	if len(fetched.Events) != 1 {
		t.Errorf("Expected 1 event (OnLoad), got %v", fetched.Events)
	} else {
		if fetched.Events[0] != "OnLoad" {
			t.Errorf("Expected event 0 to be OnLoad, got %s", fetched.Events[0])
		}
	}
}

func TestKeyMapping(t *testing.T) {
	ctx := context.Background()
	parentKey := datastore.NameKey("Parent", "parent-id", nil)
	parentKey.Namespace = "ns-custom"

	m := &KeyMappingModel{
		ID:     "custom-id",
		Parent: parentKey,
		NS:     "ns-custom",
	}

	key := testDB.Key(m)
	if key.Name != "custom-id" {
		t.Errorf("Expected Name 'custom-id', got '%s'", key.Name)
	}
	if key.Namespace != "ns-custom" {
		t.Errorf("Expected Namespace 'ns-custom', got '%s'", key.Namespace)
	}
	if key.Parent == nil || key.Parent.Name != "parent-id" {
		t.Errorf("Expected Parent 'parent-id', got %v", key.Parent)
	}

	// Save and Verify
	if err := testDB.Put(ctx, m); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Load back
	fetched := &KeyMappingModel{
		ID:     "custom-id",
		Parent: parentKey,
		NS:     "ns-custom",
	}

	if err := testDB.Get(ctx, fetched); err != nil {
		t.Fatalf("Get failed: %v", err)
	}
}

func TestPropertyMarshaling(t *testing.T) {
	ctx := context.Background()

	// Test JSON
	data := map[string]string{"foo": "bar"}
	jm := &JSONModel{Data: data}
	jm.Key = datastore.NameKey("JSONModel", "json-1", nil)

	if err := testDB.Put(ctx, jm); err != nil {
		t.Fatalf("Put JSONModel failed: %v", err)
	}

	fetchedJM := &JSONModel{}
	fetchedJM.Key = jm.Key
	if err := testDB.Get(ctx, fetchedJM); err != nil {
		t.Fatalf("Get JSONModel failed: %v", err)
	}

	if fetchedJM.Data["foo"] != "bar" {
		t.Errorf("JSON Marshaling failed. Expected 'bar', got '%s'", fetchedJM.Data["foo"])
	}

	// Test Encryption
	em := &EncryptionModel{
		Secret:    "super-secret-value",
		AltSecret: "another-secret",
	}
	em.Key = datastore.NameKey("EncryptionModel", "enc-1", nil)

	if err := testDB.Put(ctx, em); err != nil {
		t.Fatalf("Put EncryptionModel failed: %v", err)
	}

	// Verify encryption in Datastore (raw check)
	rawClient := testDB.RawClient()
	var rawProps datastore.PropertyList
	if err := rawClient.Get(ctx, em.Key, &rawProps); err != nil {
		t.Fatalf("Raw Get failed: %v", err)
	}

	// Find the properties
	var secretProp, altSecretProp datastore.Property
	for _, p := range rawProps {
		if p.Name == "secret" {
			secretProp = p
		}
		if p.Name == "alt_secret" {
			altSecretProp = p
		}
	}

	// They should NOT be the plain text
	if val, ok := secretProp.Value.(string); !ok || val == "\"super-secret-value\"" {
		t.Errorf("Secret stored in plain text or invalid type: %v", secretProp.Value)
	}
	if val, ok := altSecretProp.Value.(string); !ok || val == "\"another-secret\"" {
		t.Errorf("AltSecret stored in plain text via custom key or invalid type: %v", altSecretProp.Value)
	}

	// Verify Decrypt on Load
	fetchedEM := &EncryptionModel{}
	fetchedEM.Key = em.Key
	if err := testDB.Get(ctx, fetchedEM); err != nil {
		t.Fatalf("Get EncryptionModel failed: %v", err)
	}

	if fetchedEM.Secret != "super-secret-value" {
		t.Errorf("Secret decryption failed. Got '%s'", fetchedEM.Secret)
	}
	if fetchedEM.AltSecret != "another-secret" {
		t.Errorf("AltSecret decryption failed. Got '%s'", fetchedEM.AltSecret)
	}
}

func TestDatastoreTags(t *testing.T) {
	ctx := context.Background()
	m := &DatastoreTagModel{
		Ignored:    "should-not-save",
		Renamed:    "renamed-value",
		NotIndexed: "hidden",
		Indexed:    "visible",
	}
	// Use a new random key to avoid collision with previous runs
	m.Key = datastore.NameKey("DatastoreTagModel", fmt.Sprintf("dtag-%d", time.Now().UnixNano()), nil)

	if err := testDB.Put(ctx, m); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Verify via Raw Client
	rawClient := testDB.RawClient()
	var rawProps datastore.PropertyList
	if err := rawClient.Get(ctx, m.Key, &rawProps); err != nil {
		t.Fatalf("Raw Get failed: %v", err)
	}

	foundRenamed := false
	foundIndexed := false
	foundNotIndexed := false

	for _, p := range rawProps {
		if p.Name == "Ignored" {
			t.Error("Found field 'Ignored' which should have been ignored")
		}
		if p.Name == "custom_name" {
			foundRenamed = true
			if p.Value.(string) != "renamed-value" {
				t.Errorf("Renamed value mismatch. Got %v", p.Value)
			}
		}
		if p.Name == "NotIndexed" {
			foundNotIndexed = true
			if !p.NoIndex {
				t.Error("Field 'NotIndexed' should be NoIndex=true")
			}
		}
		if p.Name == "Indexed" {
			foundIndexed = true
			if p.NoIndex {
				t.Error("Field 'Indexed' should be NoIndex=false")
			}
		}
	}

	if !foundRenamed {
		t.Error("Did not find 'custom_name' property")
	}
	if !foundNotIndexed {
		t.Error("Did not find 'NotIndexed' property")
	}
	if !foundIndexed {
		t.Error("Did not find 'Indexed' property")
	}

	// Verify Query behavior
	// Querying on unindexed field should return nothing
	q := datastore.NewQuery("DatastoreTagModel").FilterField("NotIndexed", "=", "hidden")
	results, _, err := dsorm.Query[*DatastoreTagModel](ctx, testDB, q, "", 0)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("Query on unindexed field returned %d results, expected 0", len(results))
	}

	// Querying on indexed field should find it
	q2 := datastore.NewQuery("DatastoreTagModel").FilterField("Indexed", "=", "visible")
	results2, _, err := dsorm.Query[*DatastoreTagModel](ctx, testDB, q2, "", 0)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	// Note: We might get multiple results if previous tests didn't clean up.
	// But we used a unique ID.
	if len(results2) < 1 {
		t.Errorf("Query on indexed field returned %d results, expected >= 1", len(results2))
	}
}

func TestDBOperations(t *testing.T) {
	ctx := context.Background()

	// PutMulti
	var models []*LifecycleModel
	for i := 0; i < 5; i++ {
		m := &LifecycleModel{Value: fmt.Sprintf("val-%d", i)}
		m.Key = datastore.NameKey("LifecycleModel", fmt.Sprintf("multi-%d", i), nil)
		models = append(models, m)
	}

	if err := testDB.PutMulti(ctx, models); err != nil {
		t.Fatalf("PutMulti failed: %v", err)
	}

	// GetMulti
	var fetchedModels []*LifecycleModel
	var keys []*datastore.Key
	for _, m := range models {
		fetchedModels = append(fetchedModels, &LifecycleModel{})
		keys = append(keys, m.Key)
	}

	if err := testDB.GetMulti(ctx, keys, fetchedModels); err != nil {
		t.Fatalf("GetMulti failed: %v", err)
	}

	for i, m := range fetchedModels {
		if m.Value != fmt.Sprintf("val-%d", i) {
			t.Errorf("GetMulti index %d mismatch. Got %s", i, m.Value)
		}
	}

	// Query
	q := datastore.NewQuery("LifecycleModel").Order("Value")
	results, _, err := dsorm.Query[*LifecycleModel](ctx, testDB, q, "", 0)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// We have at least 5 from above + 1 from Lifecycle test. Sort order might pick them up.
	if len(results) < 5 {
		t.Errorf("Query returned %d results, expected at least 5", len(results))
	}
	for i, r := range results {
		if r == nil {
			t.Errorf("Result %d is nil", i)
		}
	}

	// Delete
	if err := testDB.Delete(ctx, models[0]); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if err := testDB.Get(ctx, models[0]); err != datastore.ErrNoSuchEntity {
		t.Errorf("Expected ErrNoSuchEntity after delete, got %v", err)
	}

	// DeleteMulti
	if err := testDB.DeleteMulti(ctx, models[1:]); err != nil {
		t.Fatalf("DeleteMulti failed: %v", err)
	}
}

func TestTransactions(t *testing.T) {
	ctx := context.Background()
	m := &LifecycleModel{ID: 555, Value: "initial"}
	// ID=555 -> Key(LifecycleModel, 555)

	if err := testDB.Put(ctx, m); err != nil {
		t.Fatal(err)
	}

	// Successful TX
	_, err := testDB.Transact(ctx, func(tx *dsorm.Transaction) error {
		fetched := &LifecycleModel{ID: 555}
		// Auto-init and Get
		if err := tx.Get(fetched); err != nil {
			return err
		}
		fetched.Value = "updated-in-tx"
		// Auto-init and Put
		return tx.Put(fetched)
	})
	if err != nil {
		t.Fatalf("Transact failed: %v", err)
	}

	if err := testDB.Get(ctx, m); err != nil {
		t.Fatal(err)
	}
	if m.Value != "updated-in-tx" {
		t.Errorf("Value not updated. Got %s", m.Value)
	}

	// Failed TX (Rollback)
	_, err = testDB.Transact(ctx, func(tx *dsorm.Transaction) error {
		fetched := &LifecycleModel{ID: 555}
		if err := tx.Get(fetched); err != nil {
			return err
		}
		fetched.Value = "rollback-this"
		if err := tx.Put(fetched); err != nil {
			return err
		}
		return fmt.Errorf("intentional-error")
	})

	if err == nil || err.Error() != "intentional-error" {
		t.Errorf("Expected intentional-error, got %v", err)
	}

	if err := testDB.Get(ctx, m); err != nil {
		t.Fatal(err)
	}
	if m.Value != "updated-in-tx" {
		t.Errorf("Value should not ensure updated. Got %s", m.Value)
	}
}
