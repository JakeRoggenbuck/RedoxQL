use redoxql::database::RDatabase;
use redoxql::query::RQuery;

#[test]
fn thousands_of_updates_test() {
    let mut db = RDatabase::new();
    let t = db.create_table(String::from("Grades"), 3, 0);
    let mut q = RQuery::new(t);

    q.insert(vec![0, 2, 3]);

    for x in 0..1_000_000 {
        let res = q.update(0, vec![Some(x), None, None]);
        assert!(res);
    }
}
