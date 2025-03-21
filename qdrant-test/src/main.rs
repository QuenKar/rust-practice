use qdrant_client::{
    qdrant::{
        Condition, CreateCollectionBuilder, DeletePointsBuilder, Distance, Filter, PointStruct,
        PointsIdsList, QueryPointsBuilder, UpsertPointsBuilder, VectorParamsBuilder,
    },
    Qdrant,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Qdrant::from_url("http://21.91.250.232:6334").build()?;

    client
        .create_collection(
            CreateCollectionBuilder::new("test_collection")
                .vectors_config(VectorParamsBuilder::new(4, Distance::Dot)),
        )
        .await?;

    let points = vec![
        PointStruct::new(1, vec![0.05, 0.61, 0.76, 0.74], [("city", "Berlin".into())]),
        PointStruct::new(2, vec![0.19, 0.81, 0.75, 0.11], [("city", "London".into())]),
        PointStruct::new(3, vec![0.36, 0.55, 0.47, 0.94], [("city", "Moscow".into())]),
        // ..truncated
    ];

    // Upsert points
    let response = client
        .upsert_points(UpsertPointsBuilder::new("test_collection", points).wait(true))
        .await?;
    dbg!(response);

    // Search points
    let search_result = client
        .query(QueryPointsBuilder::new("test_collection").query(vec![0.2, 0.1, 0.9, 0.7]))
        .await?;

    dbg!(search_result);

    // Search points with filter
    let search_result = client
        .query(
            QueryPointsBuilder::new("test_collection")
                .query(vec![0.2, 0.1, 0.9, 0.7])
                .filter(Filter::must([Condition::matches(
                    "city",
                    "London".to_string(),
                )]))
                .with_payload(true),
        )
        .await?;

    dbg!(search_result);

    // delete points by id 2
    client
        .delete_points(
            DeletePointsBuilder::new("test_collection")
                .points(PointsIdsList {
                    ids: vec![2.into()],
                })
                .wait(true),
        )
        .await?;

    // query all points
    let all_points = client
        .query(QueryPointsBuilder::new("test_collection"))
        .await?;

    dbg!(all_points);

    // Delete collection
    client.delete_collection("test_collection").await?;

    println!("Finish test!");

    Ok(())
}
