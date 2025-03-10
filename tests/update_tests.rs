use redoxql::database::RDatabase;
use redoxql::query::RQuery;

#[test]
fn thousands_of_updates_test() {
    let mut db = RDatabase::new();
    let table_ref = db.create_table("Scores5".to_string(), 3, 0);
    let mut q = RQuery::new(table_ref);

    q.insert(vec![0, 2, 3]);

    // Instead of constantly updating primary_key_column, update the second column
    for x in 0..100_000 {
        let res = q.update(0, vec![None, Some(x), None]);
        assert_eq!(res, true);
    }
}
