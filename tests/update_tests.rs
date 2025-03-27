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
        assert!(res);
    }
}

#[test]
fn thousands_of_ops_test() {
    let mut db = RDatabase::new();
    let table_ref = db.create_table("Scores6".to_string(), 3, 0);
    let mut q = RQuery::new(table_ref);

    q.insert(vec![0, 2, 3]);

    // Instead of constantly updating primary_key_column, update the second column
    for x in 0..100_000 {
        q.insert(vec![x, 2, 3]);
    }

    for x in 0..100_000 {
        let res = q.update(x, vec![None, Some(x), Some(4)]);
        assert!(res);
    }

    for x in 0..100_000 {
        let res = q.select(x, 0, vec![1, 1, 1]).unwrap()[0]
            .clone()
            .unwrap()
            .columns;
        assert_eq!(res, vec![Some(x), Some(x), Some(4)]);
    }
}
