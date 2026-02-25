import re

with open("orm_test.go", "r") as f:
    text = f.read()

# 1. Imports
text = re.sub(
    r'"cloud\.google\.com/go/datastore"',
    '"cloud.google.com/go/datastore"\n\t"github.com/altlimit/dsorm/ds"\n\t"github.com/ostafen/clover/v2"',
    text
)

# 2. testDB -> testClients
text = re.sub(
    r'var testDB \*dsorm\.Client',
    '''var testClients map[string]*dsorm.Client

func runAllStores(t *testing.T, f func(*testing.T, *dsorm.Client)) {
for name, db := range testClients {
t.Run(name, func(t *testing.T) {
f(t, db)
})
}
}''',
    text
)

# 3. TestMain
text = re.sub(
    r'(\s*ctx := context\.Background\(\)\s*var err error\s*testDB, err = dsorm\.New\(ctx\)\s*if err != nil \{\s*panic\(err\)\s*\})',
    '''
ctx := context.Background()
testDB, err := dsorm.New(ctx)
if err != nil {
panic(err)
}

cloverDB, err := clover.Open("")
if err != nil {
panic(err)
}
localStore := ds.NewLocalStore(cloverDB)
localClient, err := dsorm.New(ctx, dsorm.WithStore(localStore, localStore.(ds.Queryer), localStore.(ds.Transactioner)))
if err != nil {
panic(err)
}

testClients = map[string]*dsorm.Client{
"CloudStore": testDB,
"LocalStore": localClient,
}''',
    text
)

# 4. Turn test funcs into subtests and replace global testDB with local testDB parameter
test_funcs = re.findall(r'func (Test[A-Z]\w*)\(t \*testing\.T\) \{', text)
for tf in test_funcs:
    new_tf = tf[0].lower() + tf[1:]
    
    # Replace declaration
    text = re.sub(
        r'func ' + tf + r'\(t \*testing\.T\) \{',
        f'func {tf}(t *testing.T) {{\n\trunAllStores(t, {new_tf})\n}}\n\nfunc {new_tf}(t *testing.T, testDB *dsorm.Client) {{',
        text
    )

with open("orm_test.go", "w") as f:
    f.write(text)
