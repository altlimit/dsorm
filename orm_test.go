package dsorm_test

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/altlimit/dsorm"
	"github.com/altlimit/dsorm/ds/local"
)

// Global test DB instance for convenience, or strictly local?
// Let's use a global one initialized in TestMain for simplicity, mimicking previous behavior
var testClients map[string]*dsorm.Client

func runAllStores(t *testing.T, f func(*testing.T, *dsorm.Client)) {
	for name, db := range testClients {
		t.Run(name, func(t *testing.T) {
			f(t, db)
		})
	}
}

// TestMain setups the environment for tests
func TestMain(m *testing.M) {
	flag.Parse()

	// Setup Emulator
	if os.Getenv("DATASTORE_EMULATOR_HOST") == "" {
		os.Setenv("DATASTORE_EMULATOR_HOST", "localhost:8081")
	}
	os.Setenv("DATASTORE_PROJECT_ID", "app-test")
	// Setup Encryption Key
	os.Setenv("DATASTORE_ENCRYPTION_KEY", "12345678901234567890123456789012") // 32 bytes

	// Initialize DB
	ctx := context.Background()
	testDB, err := dsorm.New(ctx)
	if err != nil {
		panic(err)
	}

	tempDir, err := os.MkdirTemp("", "dsorm_test_*")
	if err != nil {
		panic(err)
	}

	localStore := local.NewStore(tempDir)
	localClient, err := dsorm.New(ctx, dsorm.WithStore(localStore))
	if err != nil {
		panic(err)
	}

	testClients = map[string]*dsorm.Client{
		"CloudStore": testDB,
		"LocalStore": localClient,
	}

	code := m.Run()
	os.RemoveAll(tempDir)
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

func (m *LifecycleModel) AfterSave(ctx context.Context, old dsorm.Model) error {
	m.Events = append(m.Events, "AfterSave")
	if old == nil {
		m.Events = append(m.Events, "OldIsNil")
	} else {
		if oldM, ok := old.(*LifecycleModel); ok {
			m.Events = append(m.Events, fmt.Sprintf("OldValue=%s", oldM.Value))
		}
	}
	return nil
}

func (m *LifecycleModel) BeforeDelete(ctx context.Context) error {
	m.Events = append(m.Events, "BeforeDelete")
	return nil
}

func (m *LifecycleModel) AfterDelete(ctx context.Context) error {
	m.Events = append(m.Events, "AfterDelete")
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
	Secret    string `marshal:"secret,encrypt" datastore:"-"`
	AltSecret string `marshal:"alt_secret,encrypt" datastore:"-"` // encrypted with default key
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

type DirectEncryptModel struct {
	dsorm.Base
	ID   int64  `model:"id"`
	Data string `marshal:"data,encrypt" datastore:"-"`
}

// ------------------------------------------------------------------
// Tests
// ------------------------------------------------------------------

func TestModelLifecycle(t *testing.T) {
	runAllStores(t, testModelLifecycle)
}

func testModelLifecycle(t *testing.T, testDB *dsorm.Client) {
	ctx := context.Background()

	m := &LifecycleModel{Value: "lifecycle"}
	// m.Init(ctx, m) // Managed by ORM now

	if !m.IsNew() {
		t.Error("IsNew() should be true for new model")
	}

	// Put triggers BeforeSave and AfterSave
	if err := testDB.Put(ctx, m); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	if len(m.Events) != 3 { // BeforeSave, AfterSave, OldIsNil
		t.Errorf("Expected 3 events for new save, got %v", m.Events)
	} else {
		if m.Events[0] != "BeforeSave" {
			t.Errorf("Expected event 0 to be BeforeSave, got %s", m.Events[0])
		}
		if m.Events[1] != "AfterSave" {
			t.Errorf("Expected event 1 to be AfterSave, got %s", m.Events[1])
		}
		if m.Events[2] != "OldIsNil" {
			t.Errorf("Expected event 2 to be OldIsNil, got %s", m.Events[2])
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

	// Update triggers AfterSave with Old Value
	fetched.Events = nil // reset events
	fetched.Value = "updated-lifecycle"
	if err := testDB.Put(ctx, fetched); err != nil {
		t.Fatalf("Put update failed: %v", err)
	}

	// Events: BeforeSave, AfterSave, OldValue=lifecycle
	if len(fetched.Events) != 3 {
		t.Errorf("Expected 3 events for update, got %v", fetched.Events)
	} else {
		foundOldVal := false
		for _, e := range fetched.Events {
			if e == "OldValue=lifecycle" {
				foundOldVal = true
			}
		}
		if !foundOldVal {
			t.Error("Expected OldValue=lifecycle event for update")
		}
	}

	// Delete triggers Delete Hooks
	fetched.Events = nil
	if err := testDB.Delete(ctx, fetched); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	// We can't check fetched.Events easily because Delete doesn't reload the struct?
	// Actually Delete accepts interface{}, so we passed the pointer.
	// The methods modify the slice on that pointer. So we should see events.
	if len(fetched.Events) != 2 {
		t.Errorf("Expected 2 delete events, got %v", fetched.Events)
	} else {
		if fetched.Events[0] != "BeforeDelete" {
			t.Errorf("Expected BeforeDelete, got %s", fetched.Events[0])
		}
		if fetched.Events[1] != "AfterDelete" {
			t.Errorf("Expected AfterDelete, got %s", fetched.Events[1])
		}
	}
}

func TestKeyMapping(t *testing.T) {
	runAllStores(t, testKeyMapping)
}

func testKeyMapping(t *testing.T, testDB *dsorm.Client) {
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
	runAllStores(t, testPropertyMarshaling)
}

func testPropertyMarshaling(t *testing.T, testDB *dsorm.Client) {
	ctx := context.Background()

	// Test JSON
	data := map[string]string{"foo": "bar"}
	jm := &JSONModel{
		ID:   int64(10),
		Data: data,
	}
	// jm.Key = datastore.NameKey("JSONModel", "json-1", nil) // Use ID
	// Mapping: ID int64 `model:"id"`. If we want string NameKey, we should change model or use LoadKey?
	// The model has ID int64. So it will generate IDKey.
	// But test used NameKey "json-1".
	// Let's assume we can change the test logic to use ID=10 (already set).
	// Or we change ID field type? No let's stick to int64 for this model struct for test.

	if err := testDB.Put(ctx, jm); err != nil {
		t.Fatalf("Put JSONModel failed: %v", err)
	}

	fetchedJM := &JSONModel{}
	fetchedJM.ID = jm.ID
	if err := testDB.Get(ctx, fetchedJM); err != nil {
		t.Fatalf("Get JSONModel failed: %v", err)
	}

	if fetchedJM.Data["foo"] != "bar" {
		t.Errorf("JSON Marshaling failed. Expected 'bar', got '%s'", fetchedJM.Data["foo"])
	}

	// Test Encryption
	encCtx := context.Background()
	secret := []byte("different-secret-32-bytes-long!!") // 32 bytes, different from TestMain

	storeOpts := dsorm.WithStore(testDB.InternalClient().Store)
	encDB, err := dsorm.New(encCtx, dsorm.WithEncryptionKey(secret), storeOpts)
	if err != nil {
		t.Fatalf("New DB with enc key failed: %v", err)
	}

	em := &EncryptionModel{
		ID:        int64(100),
		Secret:    "super-secret-value",
		AltSecret: "another-secret",
	}
	// em.Key = datastore.NameKey ... we use ID=100.

	if err := encDB.Put(encCtx, em); err != nil {
		t.Fatalf("Put EncryptionModel failed: %v", err)
	}

	// Verify encryption in Datastore (raw check)
	store := encDB.InternalClient().Store
	// Need key
	key := encDB.Key(em)
	var rawProps datastore.PropertyList
	if err := store.Get(encCtx, key, &rawProps); err != nil {
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
	fetchedEM.ID = em.ID
	if err := encDB.Get(encCtx, fetchedEM); err != nil {
		t.Fatalf("Get EncryptionModel failed: %v", err)
	}

	if fetchedEM.Secret != "super-secret-value" {
		t.Errorf("Secret decryption failed. Got '%s'", fetchedEM.Secret)
	}
	if fetchedEM.AltSecret != "another-secret" {
		t.Errorf("AltSecret decryption failed. Got '%s'", fetchedEM.AltSecret)
	}

	// Test Priority: Context Key vs Env Key
	// Context key was set to "different-secret-32-bytes-long!!"
	// Env key is "12345678901234567890123456789012"
	// If we accept Env key (testDB), decryption should fail (or return garbage/error).
	fetchedWithEnv := &EncryptionModel{}
	fetchedWithEnv.ID = em.ID
	if err := testDB.Get(ctx, fetchedWithEnv); err == nil {
		// It might not error if garbage looks like string, but it shouldn't match original.
		// Usually AES decryption without correct key/iv will produce random bytes, likely failing JSON unmarshal or just being wrong.
		// Or error on pad check.
		// encryption.Decrypt might error.
		if fetchedWithEnv.Secret == "super-secret-value" {
			t.Error("Decrypted successfully with Env key but should have used Context key for encryption!")
		}
	} else {
		// Error is expected/possible
		t.Logf("Expected error decrypting with wrong key: %v", err)
	}

	// Test DirectEncryptModel (new tag behavior)
	dem := &DirectEncryptModel{
		ID:   500,
		Data: "sensitive-data",
	}
	if err := encDB.Put(encCtx, dem); err != nil {
		t.Fatalf("Put DirectEncryptModel failed: %v", err)
	}

	fetchedDEM := &DirectEncryptModel{ID: 500}
	if err := encDB.Get(encCtx, fetchedDEM); err != nil {
		t.Fatalf("Get DirectEncryptModel failed: %v", err)
	}
	if fetchedDEM.Data != "sensitive-data" {
		t.Errorf("DirectEncryptModel data mismatch. Got '%s'", fetchedDEM.Data)
	}

	// Verify it is indeed encrypted in raw
	keyDEM := encDB.Key(dem)
	var rawPropsDEM datastore.PropertyList
	if err := store.Get(encCtx, keyDEM, &rawPropsDEM); err != nil {
		t.Fatalf("Raw Get DEM failed: %v", err)
	}
	foundData := false
	for _, p := range rawPropsDEM {
		if p.Name == "data" {
			foundData = true
			if p.Value.(string) == "\"sensitive-data\"" {
				t.Error("DirectEncryptModel data stored in plain text")
			}
			if !p.NoIndex {
				t.Error("DirectEncryptModel data should be NoIndex")
			}
		}
	}
	if !foundData {
		t.Error("DirectEncryptModel data property not found")
	}
}

func TestDatastoreTags(t *testing.T) {
	runAllStores(t, testDatastoreTags)
}

func testDatastoreTags(t *testing.T, testDB *dsorm.Client) {
	ctx := context.Background()
	m := &DatastoreTagModel{
		Ignored:    "should-not-save",
		Renamed:    "renamed-value",
		NotIndexed: "hidden",
		Indexed:    "visible",
	}
	// Use a new random key to avoid collision with previous runs
	// m.Key = ... use ID
	m.ID = time.Now().UnixNano()

	if err := testDB.Put(ctx, m); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Verify via Raw Client
	store := testDB.InternalClient().Store
	// Need key - use db.Key to generate it as we rely on ID
	key := testDB.Key(m)
	var rawProps datastore.PropertyList
	if err := store.Get(ctx, key, &rawProps); err != nil {
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
	q := dsorm.NewQuery("DatastoreTagModel").FilterField("NotIndexed", "=", "hidden")
	results, _, err := dsorm.Query[*DatastoreTagModel](ctx, testDB, q, "")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("Query on unindexed field returned %d results, expected 0", len(results))
	}

	// Querying on indexed field should find it
	q2 := dsorm.NewQuery("DatastoreTagModel").FilterField("Indexed", "=", "visible")
	results2, _, err := dsorm.Query[*DatastoreTagModel](ctx, testDB, q2, "")
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
	runAllStores(t, testDBOperations)
}

func testDBOperations(t *testing.T, testDB *dsorm.Client) {
	ctx := context.Background()

	// PutMulti
	var models []*LifecycleModel
	for i := 0; i < 5; i++ {
		m := &LifecycleModel{
			ID:    int64(i + 1000), // Explicit ID
			Value: fmt.Sprintf("val-%d", i),
		}
		// m.Key = ...
		models = append(models, m)
	}

	if err := testDB.PutMulti(ctx, models); err != nil {
		t.Fatalf("PutMulti failed: %v", err)
	}

	// GetMulti
	var fetchedModels []*LifecycleModel
	for _, m := range models {
		newM := &LifecycleModel{}
		// newM.ID = m.Key.ID // m.Key is method now, returning *Key.
		// Wait, m (LifecycleModel) has Base embedded. m.Key() returns *Key.
		// BUT we haven't Loaded m yet, so m.Key() might be nil if it wasn't set by Put?
		// Put calls LoadKey on the struct if it implements KeyLoader. Base does.
		// So m.Key() should be populated after Put.
		if m.Key() == nil {
			t.Fatalf("Model key is nil after Put")
		}
		newM.ID = m.Key().ID
		fetchedModels = append(fetchedModels, newM)
	}

	if err := testDB.GetMulti(ctx, fetchedModels); err != nil {
		t.Fatalf("GetMulti failed: %v", err)
	}

	for i, m := range fetchedModels {
		if m.Value != fmt.Sprintf("val-%d", i) {
			t.Errorf("GetMulti index %d mismatch. Got %s", i, m.Value)
		}
	}

	// Query
	q := dsorm.NewQuery("LifecycleModel").Order("Value")
	results, _, err := dsorm.Query[*LifecycleModel](ctx, testDB, q, "")
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
	runAllStores(t, testTransactions)
}

func testTransactions(t *testing.T, testDB *dsorm.Client) {
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

func TestTransactionCRUD(t *testing.T) {
	runAllStores(t, testTransactionCRUD)
}

func testTransactionCRUD(t *testing.T, testDB *dsorm.Client) {
	ctx := context.Background()
	baseID := time.Now().UnixNano()

	// --- Transaction PutMulti + GetMulti ---
	t.Run("PutMulti_GetMulti", func(t *testing.T) {
		m1 := &LifecycleModel{ID: baseID + 1, Value: "tx-put-1"}
		m2 := &LifecycleModel{ID: baseID + 2, Value: "tx-put-2"}

		_, err := testDB.Transact(ctx, func(tx *dsorm.Transaction) error {
			return tx.PutMulti([]*LifecycleModel{m1, m2})
		})
		if err != nil {
			t.Fatalf("Transact PutMulti failed: %v", err)
		}

		// Verify via GetMulti in a new transaction
		_, err = testDB.Transact(ctx, func(tx *dsorm.Transaction) error {
			fetched := []*LifecycleModel{
				{ID: baseID + 1},
				{ID: baseID + 2},
			}
			if err := tx.GetMulti(fetched); err != nil {
				return err
			}
			if fetched[0].Value != "tx-put-1" {
				t.Errorf("Expected tx-put-1, got %s", fetched[0].Value)
			}
			if fetched[1].Value != "tx-put-2" {
				t.Errorf("Expected tx-put-2, got %s", fetched[1].Value)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("Transact GetMulti failed: %v", err)
		}
	})

	// --- Transaction Delete ---
	t.Run("Delete", func(t *testing.T) {
		m := &LifecycleModel{ID: baseID + 10, Value: "to-delete"}
		if err := testDB.Put(ctx, m); err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		_, err := testDB.Transact(ctx, func(tx *dsorm.Transaction) error {
			return tx.Delete(&LifecycleModel{ID: baseID + 10})
		})
		if err != nil {
			t.Fatalf("Transact Delete failed: %v", err)
		}

		// Verify deleted
		check := &LifecycleModel{ID: baseID + 10}
		if err := testDB.Get(ctx, check); err != datastore.ErrNoSuchEntity {
			t.Errorf("Expected ErrNoSuchEntity, got %v", err)
		}
	})

	// --- Transaction DeleteMulti ---
	t.Run("DeleteMulti", func(t *testing.T) {
		m1 := &LifecycleModel{ID: baseID + 20, Value: "del-1"}
		m2 := &LifecycleModel{ID: baseID + 21, Value: "del-2"}
		if err := testDB.PutMulti(ctx, []*LifecycleModel{m1, m2}); err != nil {
			t.Fatalf("PutMulti failed: %v", err)
		}

		_, err := testDB.Transact(ctx, func(tx *dsorm.Transaction) error {
			return tx.DeleteMulti([]*LifecycleModel{
				{ID: baseID + 20},
				{ID: baseID + 21},
			})
		})
		if err != nil {
			t.Fatalf("Transact DeleteMulti failed: %v", err)
		}

		// Verify all deleted
		check1 := &LifecycleModel{ID: baseID + 20}
		check2 := &LifecycleModel{ID: baseID + 21}
		if err := testDB.Get(ctx, check1); err != datastore.ErrNoSuchEntity {
			t.Errorf("Expected ErrNoSuchEntity for ID %d, got %v", baseID+20, err)
		}
		if err := testDB.Get(ctx, check2); err != datastore.ErrNoSuchEntity {
			t.Errorf("Expected ErrNoSuchEntity for ID %d, got %v", baseID+21, err)
		}
	})
}

type ParentModel struct {
	dsorm.Base
	ID string `model:"id"`
}

type ChildModel struct {
	dsorm.Base
	ID     string       `model:"id"`
	Parent *ParentModel `model:"parent" datastore:"-"`
}

func TestStructParent(t *testing.T) {
	runAllStores(t, testStructParent)
}

func testStructParent(t *testing.T, testDB *dsorm.Client) {
	ctx := context.Background()

	parent := &ParentModel{ID: "parent-1"}
	child := &ChildModel{
		ID:     "child-1",
		Parent: parent,
	}

	// Verify Key Generation
	key := testDB.Key(child)
	if key.Parent == nil {
		t.Fatal("Child key should have a parent")
	}
	if key.Parent.Name != "parent-1" {
		t.Errorf("Expected parent key name 'parent-1', got '%s'", key.Parent.Name)
	}
	if key.Parent.Kind != "ParentModel" {
		t.Errorf("Expected parent key kind 'ParentModel', got '%s'", key.Parent.Kind)
	}

	// Save
	if err := testDB.Put(ctx, child); err != nil {
		t.Fatalf("Put child failed: %v", err)
	}

	// Load
	// Wait, if I just do &ChildModel{ID: "child-1"}, the key doesn't know the parent!
	// Datastore keys differ if parent is different.
	// So to GET, we must supply the parent key info somehow if we are constructing the key from the struct.
	// The testDB.Key(fetched) needs to know the parent to form the correct key.
	// In this new model, we naturally put the parent struct in.

	// To Get, we usually pass a struct. The struct must allow Key() to generate the full key.
	// So we need to populate the Parent in the struct we are fetching into, OR we need to use a key query.
	// Let's populate the parent in the struct we use for lookup.
	lookup := &ChildModel{
		ID:     "child-1",
		Parent: &ParentModel{ID: "parent-1"},
	}

	if err := testDB.Get(ctx, lookup); err != nil {
		t.Fatalf("Get child failed: %v", err)
	}

	// Verify loaded parent
	// The Get() should populate lookup.Parent's key fields from the loaded Key (which is redundant but checks logic)
	// Actually, Get() calls Load() which calls LoadKey().
	// LoadKey() should populate the parent struct fields from the Key's parent.

	// Better test: check if LoadKey worked on a fresh struct if we manually load it?
	// But Get() writes to the struct we passed.

	if lookup.Parent == nil {
		t.Error("Loaded struct should have Parent field populated")
	} else {
		// Parent ID should be set from the key
		if lookup.Parent.ID != "parent-1" {
			t.Errorf("Loaded parent ID expected 'parent-1', got '%s'", lookup.Parent.ID)
		}
	}
}

func TestGetMultiGeneric(t *testing.T) {
	runAllStores(t, testGetMultiGeneric)
}

func testGetMultiGeneric(t *testing.T, testDB *dsorm.Client) {
	ctx := context.Background()

	// 1. Test with []string using KeyMappingModel (String ID)
	var strModels []*KeyMappingModel
	for i := 0; i < 3; i++ {
		m := &KeyMappingModel{ID: fmt.Sprintf("gm-%d", i)}
		strModels = append(strModels, m)
	}
	if err := testDB.PutMulti(ctx, strModels); err != nil {
		t.Fatalf("Setup PutMulti String failed: %v", err)
	}

	strIDs := []string{"gm-0", "gm-1", "gm-2"}
	resStr, err := dsorm.GetMulti[*KeyMappingModel](ctx, testDB, strIDs)
	if err != nil {
		t.Fatalf("GetMulti string ids failed: %v", err)
	}
	if len(resStr) != 3 {
		t.Errorf("Expected 3 results, got %d", len(resStr))
	} else {
		for i, r := range resStr {
			if r == nil {
				t.Fatalf("String Result %d is nil", i)
			}
			// Note: GetMulti constructs keys with default options.
			// NameKey("KeyMappingModel", "gm-0", nil).
			// But our items were saved with Namespace="ns-test" (via NS field mapping to Key.Namespace)!
			// Wait! KeyMappingModel's ID field is mapped to "id", NS to "ns".
			// db.Key(m) uses both.
			// When GetMulti constructs keys from []string, it simply does NameKey(Kind, id, nil).
			// It generally *cannot* know the namespace unless encoded in the ID or passed separately?
			// The current GetMulti implementation does NOT handle namespaces for primitive ID slices.
			// So looking them up by just ID will fail if they have a namespace!
			// We should ensure NS is empty for this test if we want simple lookup.
		}
	}

	// Retry String Test with NO Namespace to simplify
	var simpleStrModels []*KeyMappingModel
	for i := 0; i < 3; i++ {
		m := &KeyMappingModel{ID: fmt.Sprintf("simple-%d", i)} // NS empty
		simpleStrModels = append(simpleStrModels, m)
	}
	if err := testDB.PutMulti(ctx, simpleStrModels); err != nil {
		t.Fatalf("Setup PutMulti Simple String failed: %v", err)
	}

	simpleIDs := []string{"simple-0", "simple-1", "simple-2"}
	resSimple, err := dsorm.GetMulti[*KeyMappingModel](ctx, testDB, simpleIDs)
	if err != nil {
		t.Fatalf("GetMulti simple string ids failed: %v", err)
	}
	if len(resSimple) != 3 {
		t.Errorf("Expected 3 simple results, got %d", len(resSimple))
	} else if resSimple[0] == nil {
		t.Error("Simple Result 0 is nil")
	}

	// 2. Test with []int64 using LifecycleModel (Int ID)
	var intModels []*LifecycleModel
	for i := 0; i < 3; i++ {
		m := &LifecycleModel{ID: int64(200 + i), Value: fmt.Sprintf("val-%d", i)}
		intModels = append(intModels, m)
	}
	if err := testDB.PutMulti(ctx, intModels); err != nil {
		t.Fatalf("Setup PutMulti Int failed: %v", err)
	}

	intIDs := []int64{200, 201, 202}
	resInt, err := dsorm.GetMulti[*LifecycleModel](ctx, testDB, intIDs)
	if err != nil {
		t.Fatalf("GetMulti int ids failed: %v", err)
	}
	if len(resInt) != 3 {
		t.Errorf("Expected 3 int results, got %d", len(resInt))
	} else if resInt[0] == nil {
		t.Error("Int Result 0 is nil")
	}

	// 3. Test with []*datastore.Key
	var keys []*datastore.Key
	for _, m := range intModels {
		keys = append(keys, m.Key())
	}
	resKeys, err := dsorm.GetMulti[*LifecycleModel](ctx, testDB, keys)
	if err != nil {
		t.Fatalf("GetMulti keys failed: %v", err)
	}
	if len(resKeys) != 3 {
		t.Errorf("Expected 3 results from keys, got %d", len(resKeys))
	}

	// 4. Test with Slice of Structs
	resStructs, err := dsorm.GetMulti[*LifecycleModel](ctx, testDB, intModels)
	if err != nil {
		t.Fatalf("GetMulti structs failed: %v", err)
	}
	if len(resStructs) != 3 {
		t.Errorf("Expected 3 results from structs, got %d", len(resStructs))
	}
}

func TestTransactionPendingKey(t *testing.T) {
	runAllStores(t, testTransactionPendingKey)
}

func testTransactionPendingKey(t *testing.T, testDB *dsorm.Client) {
	if strings.Contains(t.Name(), "LocalStore") {
		t.Skip("LocalStore cannot populate datastore.Commit private keys for PendingKey resolution")
	}

	ctx := context.Background()
	m := &LifecycleModel{Value: "pending-key"}
	// ID is 0, so incomplete key.

	// Run transaction
	_, err := testDB.Transact(ctx, func(tx *dsorm.Transaction) error {
		return tx.Put(m)
	})
	if err != nil {
		t.Fatalf("Transact failed: %v", err)
	}

	// After fix, this should be populated.
	// Currently, we expect this to likely be 0.
	if m.ID == 0 {
		t.Error("ID is 0, expected it to be populated from PendingKey")
	}
}

func TestSlicePropertyQuery(t *testing.T) {
	runAllStores(t, testSlicePropertyQuery)
}

func testSlicePropertyQuery(t *testing.T, testDB *dsorm.Client) {
	ctx := context.Background()

	type User struct {
		dsorm.Base
		ID      int64     `datastore:"-" model:"id"`
		Name    string    `datastore:"name"`
		Created time.Time `datastore:"created" model:"created"`
		Tags    []string  `datastore:"tag"`
	}

	user := &User{
		Name: "Alice",
		Tags: []string{"tag1", "tag2"},
	}

	if err := testDB.Put(ctx, user); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Query for tag1 — should find the user
	q := dsorm.NewQuery("User").FilterField("tag", "=", "tag1")
	users, _, err := dsorm.Query[*User](ctx, testDB, q, "")
	if err != nil {
		t.Fatalf("Query tag1 failed: %v", err)
	}
	if len(users) < 1 {
		t.Errorf("Expected at least 1 result for tag1, got %d", len(users))
	}

	// Query for tag2 — should also find the user
	q2 := dsorm.NewQuery("User").FilterField("tag", "=", "tag2")
	users2, _, err := dsorm.Query[*User](ctx, testDB, q2, "")
	if err != nil {
		t.Fatalf("Query tag2 failed: %v", err)
	}
	if len(users2) < 1 {
		t.Errorf("Expected at least 1 result for tag2, got %d", len(users2))
	}

	// Verify the returned user has both tags
	if len(users) > 0 {
		if users[0].Name != "Alice" {
			t.Errorf("Expected name 'Alice', got '%s'", users[0].Name)
		}
		if len(users[0].Tags) != 2 {
			t.Errorf("Expected 2 tags, got %d: %v", len(users[0].Tags), users[0].Tags)
		}
	}
}

// ------------------------------------------------------------------
// Comprehensive Query Tests
// ------------------------------------------------------------------

type QueryModel struct {
	dsorm.Base
	ID    int64  `model:"id"`
	Group string `datastore:"group"` // scoping field to isolate test runs
	Label string `datastore:"label"`
	Score int    `datastore:"score"`
}

func TestQueryFeatures(t *testing.T) {
	runAllStores(t, testQueryFeatures)
}

func testQueryFeatures(t *testing.T, testDB *dsorm.Client) {
	ctx := context.Background()

	// Unique group per test run to isolate from leftover data
	group := fmt.Sprintf("grp-%d", time.Now().UnixNano())

	// Seed 5 models with distinct scores and labels
	baseID := time.Now().UnixNano()
	seeds := []struct {
		label string
		score int
	}{
		{"alpha", 10},
		{"bravo", 20},
		{"charlie", 30},
		{"delta", 40},
		{"echo", 50},
	}
	for i, s := range seeds {
		m := &QueryModel{ID: baseID + int64(i), Group: group, Label: s.label, Score: s.score}
		if err := testDB.Put(ctx, m); err != nil {
			t.Fatalf("Seed Put %d failed: %v", i, err)
		}
	}

	// Helper: new query scoped to our group
	newQ := func() *dsorm.QueryBuilder {
		return dsorm.NewQuery("QueryModel").FilterField("group", "=", group)
	}

	// --- Limit ---
	t.Run("Limit", func(t *testing.T) {
		q := newQ().Limit(2)
		results, _, err := dsorm.Query[*QueryModel](ctx, testDB, q, "")
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if len(results) != 2 {
			t.Errorf("Expected 2 results, got %d", len(results))
		}
	})

	// --- Offset ---
	t.Run("Offset", func(t *testing.T) {
		q := newQ().Order("score").Limit(2).Offset(2)
		results, _, err := dsorm.Query[*QueryModel](ctx, testDB, q, "")
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if len(results) != 2 {
			t.Errorf("Expected 2 results, got %d", len(results))
		}
		if len(results) >= 1 && results[0].Score != 30 {
			t.Errorf("Expected first result to have score 30, got %d", results[0].Score)
		}
	})

	// --- Order Ascending ---
	t.Run("OrderAsc", func(t *testing.T) {
		q := newQ().Order("score")
		results, _, err := dsorm.Query[*QueryModel](ctx, testDB, q, "")
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if len(results) != 5 {
			t.Fatalf("Expected 5 results, got %d", len(results))
		}
		for i := 1; i < len(results); i++ {
			if results[i].Score < results[i-1].Score {
				t.Errorf("Results not sorted ascending: score[%d]=%d < score[%d]=%d",
					i, results[i].Score, i-1, results[i-1].Score)
			}
		}
	})

	// --- Order Descending ---
	t.Run("OrderDesc", func(t *testing.T) {
		q := newQ().Order("-score")
		results, _, err := dsorm.Query[*QueryModel](ctx, testDB, q, "")
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if len(results) != 5 {
			t.Fatalf("Expected 5 results, got %d", len(results))
		}
		for i := 1; i < len(results); i++ {
			if results[i].Score > results[i-1].Score {
				t.Errorf("Results not sorted descending: score[%d]=%d > score[%d]=%d",
					i, results[i].Score, i-1, results[i-1].Score)
			}
		}
	})

	// --- Cursor / Pagination ---
	t.Run("Cursor", func(t *testing.T) {
		q := newQ().Order("score").Limit(3)
		page1, cursor, err := dsorm.Query[*QueryModel](ctx, testDB, q, "")
		if err != nil {
			t.Fatalf("Page 1 failed: %v", err)
		}
		if len(page1) != 3 {
			t.Fatalf("Page 1: expected 3 results, got %d", len(page1))
		}
		if cursor == "" {
			t.Fatal("Expected non-empty cursor after page 1")
		}

		// Page 2
		page2, _, err := dsorm.Query[*QueryModel](ctx, testDB, q, cursor)
		if err != nil {
			t.Fatalf("Page 2 failed: %v", err)
		}
		if len(page2) != 2 {
			t.Errorf("Page 2: expected 2 results, got %d", len(page2))
		}

		// Ensure no overlap between pages
		page1IDs := make(map[int64]bool)
		for _, r := range page1 {
			page1IDs[r.ID] = true
		}
		for _, r := range page2 {
			if page1IDs[r.ID] {
				t.Errorf("Page 2 result ID %d was already in page 1", r.ID)
			}
		}
	})

	// --- Inequality: Greater Than ---
	t.Run("InequalityGT", func(t *testing.T) {
		q := newQ().FilterField("score", ">", 30)
		results, _, err := dsorm.Query[*QueryModel](ctx, testDB, q, "")
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if len(results) < 2 {
			t.Errorf("Expected at least 2 results (score 40, 50), got %d", len(results))
		}
		for _, r := range results {
			if r.Score <= 30 {
				t.Errorf("Unexpected result with score %d (should be > 30)", r.Score)
			}
		}
	})

	// --- Inequality: Greater Than or Equal ---
	t.Run("InequalityGTE", func(t *testing.T) {
		q := newQ().FilterField("score", ">=", 30)
		results, _, err := dsorm.Query[*QueryModel](ctx, testDB, q, "")
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if len(results) < 3 {
			t.Errorf("Expected at least 3 results (score 30, 40, 50), got %d", len(results))
		}
		for _, r := range results {
			if r.Score < 30 {
				t.Errorf("Unexpected result with score %d (should be >= 30)", r.Score)
			}
		}
	})

	// --- Inequality: Less Than ---
	t.Run("InequalityLT", func(t *testing.T) {
		q := newQ().FilterField("score", "<", 30)
		results, _, err := dsorm.Query[*QueryModel](ctx, testDB, q, "")
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if len(results) < 2 {
			t.Errorf("Expected at least 2 results (score 10, 20), got %d", len(results))
		}
		for _, r := range results {
			if r.Score >= 30 {
				t.Errorf("Unexpected result with score %d (should be < 30)", r.Score)
			}
		}
	})

	// --- Inequality: Less Than or Equal ---
	t.Run("InequalityLTE", func(t *testing.T) {
		q := newQ().FilterField("score", "<=", 30)
		results, _, err := dsorm.Query[*QueryModel](ctx, testDB, q, "")
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if len(results) < 3 {
			t.Errorf("Expected at least 3 results (score 10, 20, 30), got %d", len(results))
		}
		for _, r := range results {
			if r.Score > 30 {
				t.Errorf("Unexpected result with score %d (should be <= 30)", r.Score)
			}
		}
	})

	// --- Multiple Filters ---
	t.Run("MultipleFilters", func(t *testing.T) {
		q := newQ().
			FilterField("label", "=", "charlie")
		results, _, err := dsorm.Query[*QueryModel](ctx, testDB, q, "")
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if len(results) != 1 {
			t.Errorf("Expected 1 result, got %d", len(results))
		}
		for _, r := range results {
			if r.Label != "charlie" || r.Score != 30 {
				t.Errorf("Unexpected result: label=%q score=%d", r.Label, r.Score)
			}
		}
	})

	// --- Order + Filter combined ---
	t.Run("OrderWithFilter", func(t *testing.T) {
		q := newQ().
			FilterField("score", ">=", 20).
			Order("score").
			Limit(3)
		results, _, err := dsorm.Query[*QueryModel](ctx, testDB, q, "")
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if len(results) != 3 {
			t.Errorf("Expected 3, got %d", len(results))
		}
		if len(results) >= 3 {
			if results[0].Score != 20 || results[1].Score != 30 || results[2].Score != 40 {
				t.Errorf("Expected scores [20,30,40], got [%d,%d,%d]",
					results[0].Score, results[1].Score, results[2].Score)
			}
		}
	})
}

// --- Ancestor Query Test ---
func TestAncestorQuery(t *testing.T) {
	runAllStores(t, testAncestorQuery)
}

func testAncestorQuery(t *testing.T, testDB *dsorm.Client) {
	ctx := context.Background()

	parent := &ParentModel{ID: fmt.Sprintf("qparent-%d", time.Now().UnixNano())}
	child1 := &ChildModel{ID: "qchild-1", Parent: parent}
	child2 := &ChildModel{ID: "qchild-2", Parent: parent}

	if err := testDB.Put(ctx, child1); err != nil {
		t.Fatalf("Put child1 failed: %v", err)
	}
	if err := testDB.Put(ctx, child2); err != nil {
		t.Fatalf("Put child2 failed: %v", err)
	}

	// Also save a child under a different parent
	other := &ParentModel{ID: "qother-parent"}
	otherChild := &ChildModel{ID: "qchild-other", Parent: other}
	if err := testDB.Put(ctx, otherChild); err != nil {
		t.Fatalf("Put otherChild failed: %v", err)
	}

	parentKey := testDB.Key(parent)
	q := dsorm.NewQuery("ChildModel").Ancestor(parentKey)
	results, _, err := dsorm.Query[*ChildModel](ctx, testDB, q, "")
	if err != nil {
		t.Fatalf("Ancestor query failed: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 children under parent, got %d", len(results))
	}
	for _, r := range results {
		if r.ID != "qchild-1" && r.ID != "qchild-2" {
			t.Errorf("Unexpected child ID: %s", r.ID)
		}
	}
}

// --- Namespace Query Test ---
func TestNamespaceQuery(t *testing.T) {
	runAllStores(t, testNamespaceQuery)
}

type NSQueryModel struct {
	dsorm.Base
	ID    string `model:"id"`
	NS    string `model:"ns"`
	Value string `datastore:"value"`
}

func testNamespaceQuery(t *testing.T, testDB *dsorm.Client) {
	ctx := context.Background()

	ns := fmt.Sprintf("ns-query-%d", time.Now().UnixNano())

	// Use unique IDs per run to avoid collision
	m1 := &NSQueryModel{ID: fmt.Sprintf("ns-1-%d", time.Now().UnixNano()), NS: ns, Value: "hello"}
	m2 := &NSQueryModel{ID: fmt.Sprintf("ns-2-%d", time.Now().UnixNano()), NS: ns, Value: "world"}

	if err := testDB.Put(ctx, m1); err != nil {
		t.Fatalf("Put m1 failed: %v", err)
	}
	if err := testDB.Put(ctx, m2); err != nil {
		t.Fatalf("Put m2 failed: %v", err)
	}

	// Query with filter in the custom namespace
	q := dsorm.NewQuery("NSQueryModel").
		Namespace(ns).
		FilterField("value", "=", "hello")
	results, _, err := dsorm.Query[*NSQueryModel](ctx, testDB, q, "")
	if err != nil {
		t.Fatalf("Namespace query failed: %v", err)
	}
	if len(results) < 1 {
		t.Errorf("Expected at least 1 result in namespace %q, got %d", ns, len(results))
	}
	for _, r := range results {
		if r == nil {
			continue
		}
		if r.Value != "hello" {
			t.Errorf("Unexpected value %q in namespace query", r.Value)
		}
	}

	// Query all in namespace
	q2 := dsorm.NewQuery("NSQueryModel").Namespace(ns)
	allResults, _, err := dsorm.Query[*NSQueryModel](ctx, testDB, q2, "")
	if err != nil {
		t.Fatalf("Namespace all query failed: %v", err)
	}
	if len(allResults) != 2 {
		t.Errorf("Expected 2 results in namespace %q, got %d", ns, len(allResults))
	}
}
