use std::result;
use std::sync::Arc;

use aws_config::load_from_env;
use aws_sdk_s3::types::ByteStream;
use aws_sdk_s3::Client;
use futures::stream::FuturesUnordered;
use futures::{StreamExt, TryStreamExt};
use htsget_search::htsget::search::Search;
use htsget_search::htsget::vcf_search::VcfSearch;
use htsget_search::htsget::Format::Vcf;
use htsget_search::htsget::Query;
use htsget_search::storage::aws::AwsS3Storage;
use htsget_search::storage::{BytesPosition, BytesRange};
use htsget_search::RegexResolver;
use lambda_runtime::{Error, LambdaEvent};
use noodles::tabix::Index;
use noodles::{bgzf, tabix, vcf};
use noodles_vcf::record::{AlternateBases, Position, ReferenceBases};
use noodles_vcf::Header;
use serde::{Deserialize, Serialize};
use tokio::select;

pub type Result<T> = result::Result<T, Error>;

/// A beacon sequence query request, see:
/// http://docs.genomebeacons.org/variant-queries/
#[derive(Debug, Deserialize)]
pub struct SequenceQueryRequest {
    vcf_bucket: String,
    // This is assumed to be GZ compressed, i.e. ending in ".vcf.gz"
    vcf_key: String,
    vcf_index_bucket: String,
    // This is assumed to end in ".vcf.gz.tbi"
    vcf_index_key: String,
    reference_name: String,
    start: u32,
    reference_bases: String,
    alternate_bases: String,
}

/// The beacon response, indicating whether the sequence was found.
#[derive(Debug, Serialize)]
pub struct SequenceQueryResponse {
    found: bool,
}

/// Handles the SequenceQueryRequest lambda event.
pub async fn beacon_handler(
    event: LambdaEvent<SequenceQueryRequest>,
) -> Result<SequenceQueryResponse> {
    let vcf_index_id = verify_key(&event.payload.vcf_index_key, ".vcf.gz.tbi")?;
    let vcf_id = verify_key(&event.payload.vcf_key, ".vcf.gz")?;

    let client = Client::new(&load_from_env().await);

    let index = get_index(
        &client,
        &event.payload.vcf_index_bucket,
        &event.payload.vcf_index_key,
    )
    .await?;

    let vcf_search = vcf_searcher(client.clone(), event.payload.vcf_bucket.clone());
    let header = vcf_search.get_header(&vcf_id, &Vcf, &index).await?;

    let byte_ranges = vcf_search
        .get_byte_ranges_for_reference_name(
            event.payload.reference_name,
            &index,
            &header,
            Query::new(&vcf_index_id, Vcf).with_start(event.payload.start),
        )
        .await?;

    let mut blocks = FuturesUnordered::new();
    for range in BytesPosition::merge_all(byte_ranges)
        .iter()
        .map(BytesRange::from)
    {
        let client_owned = client.clone();
        let bucket = event.payload.vcf_bucket.clone();
        let key = event.payload.vcf_key.clone();
        blocks.push(tokio::spawn(async move {
            client_owned
                .get_object()
                .bucket(bucket)
                .key(key)
                .range(String::from(&range))
                .send()
                .await
        }));
    }

    loop {
        select! {
            Some(next) = blocks.next() => {
                if beacon_sequence_query(
                    next??.body,
                    &header,
                    event.payload.start,
                    &event.payload.reference_bases,
                    &event.payload.alternate_bases,
                )
                .await?
                {
                    return Ok(SequenceQueryResponse { found: true });
                }
            },
            else => break
        }
    }

    Ok(SequenceQueryResponse { found: false })
}

/// Verify that the index key ends with the suffix.
fn verify_key(key: &str, suffix: &str) -> Result<String> {
    if let Some(value) = key.strip_suffix(suffix) {
        Ok(value.to_string())
    } else {
        Err(Error::from(format!("Invalid key: {}", key)))
    }
}

/// Get the index from the bucket and key.
async fn get_index(client: &Client, bucket: &str, key: &str) -> Result<Index> {
    let response = client.get_object().bucket(bucket).key(key).send().await?;

    Ok(tabix::AsyncReader::new(response.body.into_async_read())
        .read_index()
        .await?)
}

/// Create the vcf search struct.
fn vcf_searcher(client: Client, bucket: String) -> VcfSearch<AwsS3Storage> {
    let storage = AwsS3Storage::new(client, bucket, RegexResolver::default());
    VcfSearch::new(Arc::new(storage))
}

/// Perform the beacon sequence query search.
async fn beacon_sequence_query(
    byte_stream: ByteStream,
    header: &Header,
    start: u32,
    reference_bases: &str,
    alternate_bases: &str,
) -> Result<bool> {
    let mut vcf_blocks =
        vcf::AsyncReader::new(bgzf::AsyncReader::new(byte_stream.into_async_read()));
    let mut records = vcf_blocks.records(header);

    while let Some(record) = records.try_next().await? {
        let start = Position::from(usize::try_from(start)?);
        let pos = record.position();

        if pos > start {
            return Ok(false);
        }

        if pos == start
            && record.reference_bases() == &reference_bases.parse::<ReferenceBases>()?
            && record.alternate_bases() == &alternate_bases.parse::<AlternateBases>()?
        {
            return Ok(true);
        }
    }

    Ok(false)
}

#[cfg(test)]
mod tests {
    use lambda_runtime::Context;

    use super::*;

    // some variants across out 10 germline samples
    // chr1    1135738 G       C       1111110100
    // chr1    1137118 G       C       1111110100
    // chr1    1140653 A       G       1101110110
    // chr1    1163041 C       T       1111110010
    // chr1    1163334 C       G       1111110010
    // chr1    1168009 GGGGCGGAGGGCCGAGCGGGGCCAGCAGACGGGTGA    G       1111110010
    // chr1    1175206 T       C       1000111111
    // chr1    1182018 A       G       1000111111
    // chr1    1184277 G       C       1000111111
    // chr1    1220751 T       C       0010111111
    // chr1    1236037 C       T       1100011111

    #[tokio::test]
    async fn test_where_variant_should_be_found() {
        let r = beacon_handler(LambdaEvent::new(
            SequenceQueryRequest {
                vcf_bucket: "umccr-10g-data-dev".to_string(),
                vcf_key: "HG00174/HG00174.hard-filtered.vcf.gz".to_string(),
                vcf_index_bucket: "umccr-10g-data-dev".to_string(),
                vcf_index_key: "HG00174/HG00174.hard-filtered.vcf.gz.tbi".to_string(),
                // chr1    1220751 T       C       0010111111
                reference_name: "chr1".to_string(),
                start: 1220751,
                reference_bases: "T".to_string(),
                alternate_bases: "C".to_string(),
            },
            Context::default(),
        ))
        .await;

        assert!(r.unwrap().found, "Expected variant was not found");
    }

    #[tokio::test]
    async fn test_where_variant_should_not_be_found() {
        let r = beacon_handler(LambdaEvent::new(
            SequenceQueryRequest {
                vcf_bucket: "umccr-10g-data-dev".to_string(),
                vcf_key: "HG00096/HG00096.hard-filtered.vcf.gz".to_string(),
                vcf_index_bucket: "umccr-10g-data-dev".to_string(),
                vcf_index_key: "HG00096/HG00096.hard-filtered.vcf.gz.tbi".to_string(),
                // chr1    1220751 T       C       0010111111
                reference_name: "chr1".to_string(),
                start: 1220751,
                reference_bases: "T".to_string(),
                alternate_bases: "C".to_string(),
            },
            Context::default(),
        ))
        .await;

        assert!(!r.unwrap().found, "Unexpected variant was found");
    }
}
