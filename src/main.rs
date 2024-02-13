use bcs::serialize_into;
use scylla::query::Query;
use scylla::{IntoTypedRows, Session, SessionBuilder};
use rand::SeedableRng;
use rand::Rng;

fn get_upper_bound_option(key_prefix: &[u8]) -> Option<Vec<u8>> {
    let len = key_prefix.len();
    for i in (0..len).rev() {
        let val = key_prefix[i];
        if val < u8::MAX {
            let mut upper_bound = key_prefix[0..i + 1].to_vec();
            upper_bound[i] += 1;
            return Some(upper_bound);
        }
    }
    None
}

async fn find_keys_by_prefix(session: &Session, key_prefix: Vec<u8>) -> Vec<Vec<u8>> {
    let mut keys = Vec::new();
    let len = key_prefix.len();
    let mut paging_state = None;
    loop {
        let result = match get_upper_bound_option(&key_prefix) {
            None => {
                let values = (key_prefix.clone(),);
                let query = "SELECT k FROM kv.pairs WHERE dummy = 0 AND k >= ? ALLOW FILTERING";
                session.query_paged(query, values, paging_state).await.unwrap()
            }
            Some(upper_bound) => {
                let values = (key_prefix.clone(), upper_bound);
                let query =
                    "SELECT k FROM kv.pairs WHERE dummy = 0 AND k >= ? AND k < ? ALLOW FILTERING";
                session.query_paged(query, values, paging_state).await.unwrap()
            }
        };
        if let Some(rows) = result.rows {
            for row in rows.into_typed::<(Vec<u8>,)>() {
                let key = row.unwrap();
                let short_key = key.0[len..].to_vec();
                keys.push(short_key);
            }
        }
        if result.paging_state.is_none() {
            return keys;
        }
        paging_state = result.paging_state;
    }
}

async fn write_batch(session: &Session, n_blk: usize) {
    let mut rng = rand::rngs::StdRng::seed_from_u64(2);
    let mut l_insert_query = Vec::new();
    let mut l_insert_values = Vec::new();
    let mut l_delete_query = Vec::new();
    let mut l_delete_values = Vec::new();
    let n = 1000;
    let mut pos = 0;
    let mut n_insert = 0;
    let mut n_delete = 0;
    for _ in 0..n_blk {
        let mut batch_insert_query =
            scylla::statement::batch::Batch::new(scylla::frame::request::batch::BatchType::Logged);
        let mut batch_insert_values = Vec::new();
        let mut batch_delete_query =
            scylla::statement::batch::Batch::new(scylla::frame::request::batch::BatchType::Logged);
        let mut batch_delete_values = Vec::new();
        for _i in 0..n {
            let mut key = vec![0];
            serialize_into(&mut key, &(pos as usize)).unwrap();
            pos += 1;
            let value = key.clone();
            let query = "INSERT INTO kv.pairs (dummy, k, v) VALUES (0, ?, ?)";
            let values = vec![key.clone(), value];
            let query = Query::new(query);
            batch_insert_values.push(values);
            batch_insert_query.append_statement(query);
            n_insert += 1;
            let delete = rng.gen::<bool>();
            if delete {
                let values = vec![key];
                let query = "DELETE FROM kv.pairs WHERE dummy = 0 AND k = ?";
                let query = Query::new(query);
                batch_delete_values.push(values);
                batch_delete_query.append_statement(query);
                n_delete += 1;
            }
        }
        l_insert_query.push(batch_insert_query);
        l_insert_values.push(batch_insert_values);
        l_delete_query.push(batch_delete_query);
        l_delete_values.push(batch_delete_values);
    }
    println!("n_insert={} n_delete={}", n_insert, n_delete);
    println!("The l_insert_* and l_delete_* built");
    for (queries, values) in l_insert_query.iter().zip(l_insert_values.iter()) {
        session.batch(&queries, values).await.unwrap();
    }
    println!("The insert have been processed");
    for (queries, values) in l_delete_query.iter().zip(l_delete_values.iter()) {
        session.batch(&queries, values).await.unwrap();
    }
    println!("The delete have been processed");
}

async fn create_test_session() -> Session {
    let session_builder = SessionBuilder::new().known_node("localhost:9042");
    let session = session_builder.build().await.unwrap();
    session
        .query(
            "CREATE KEYSPACE IF NOT EXISTS kv WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }",
            &[],
        )
        .await.unwrap();
    session
        .query("DROP TABLE IF EXISTS kv.pairs;", &[])
        .await
        .unwrap();
    session
        .query(
            "CREATE TABLE IF NOT EXISTS kv.pairs (dummy int, k blob, v blob, primary key (dummy, k))",
            &[],
        )
        .await.unwrap();
    session
}

async fn treat_one_case(n_blk: usize) {
    println!("treat_one_case n_blk={}", n_blk);
    let session = create_test_session().await;

    write_batch(&session, n_blk).await;

    let key_prefix = vec![0];
    let keys = find_keys_by_prefix(&session, key_prefix.clone()).await;
    println!("key_prefix={:?} |keys|={}", key_prefix.clone(), keys.len());

}



#[tokio::main]
async fn main() {
    treat_one_case(18).await;
    treat_one_case(22).await;
}
