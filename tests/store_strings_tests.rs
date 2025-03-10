use log::debug;
use redoxql::database::RDatabase;
use redoxql::query::RQuery;
use redoxql::utils::{decode_string_from_ints, encode_str_to_ints};

#[test]
fn store_strings() {
    let mut db = RDatabase::new();
    let table_ref = db.create_table("Books".to_string(), 64, 0);
    let mut q = RQuery::new(table_ref);

    let name = "Cracking the Coding Interview";

    let a = encode_str_to_ints(name);
    debug!("{:?}", a);

    let b = decode_string_from_ints(a.clone());
    debug!("{:?}", b);

    // Insert string into db
    let mut str = vec![0; 64 - a.len()];
    str.extend(a);
    q.insert(str);

    let v = q.select(0, 0, vec![1; 64]);
    let mut vals = vec![];
    for x in &v.unwrap()[0].as_mut().unwrap().columns {
        if let Some(d) = x {
            vals.push(*d);
        }
    }

    assert_eq!(
        decode_string_from_ints(vals),
        "Cracking the Coding Interview"
    );
}
