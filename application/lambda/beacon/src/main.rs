use std::collections::hash_map::DefaultHasher;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use lambda_runtime::{handler_fn, Context, Error};
use log::LevelFilter;
use serde::{Deserialize, Serialize};
use simple_logger::SimpleLogger;
use aws_sdk_s3::Client;
use futures::stream::FuturesUnordered;
use futures::{StreamExt, TryStreamExt};
use htsget_search::htsget::Format;
use htsget_search::htsget::search::{BgzfSearch, Search};
use htsget_search::RegexResolver;
use htsget_search::storage::{BytesPosition, BytesRange};
use htsget_search::StorageType::AwsS3Storage;
use noodles::core::Position;
use noodles::csi::{BinningIndex, BinningIndexReferenceSequence};
use noodles::vcf::header::Header;
use noodles_vcf::AsyncReader;
use tokio::select;
use tokio::time::{sleep, Duration};

// See http://docs.genomebeacons.org/variant-queries/

// the 'beacon' query we can make
#[derive(Deserialize)]
struct Request {
    // the VCF to query
    vcf_bucket: String,
    vcf_key: String,
    vcf_index_bucket: String,
    vcf_index_key: String,

    // the beacon details
    // this is only set up for beacon sequence queries.. we need to think
    // about a better model for dynamic/range etc queries
    reference_name: String,
    start: u32,
    reference_bases: String,
    alternate_bases: String,
}

// the 'beacon' result
#[derive(Serialize)]
#[derive(Debug)]
struct Response {
    found: bool,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    // required to enable CloudWatch error logging by the runtime
    // can be replaced with any other method of initializing `log`
    SimpleLogger::new().with_utc_timestamps().with_level(LevelFilter::Debug).init().unwrap();

    // create a mechanism to invoke the handler assuming invocation inside lambda
    let func = handler_fn(beacon_handler);

    // and run it
    lambda_runtime::run(func).await?;

    Ok(())
}

fn to_digits(mut v: u64) -> Vec<u8> {
    let mut digits: Vec<u8> = Vec::with_capacity(20);

    while v > 0 {
        let n = (v % 10) as u8;
        v /= 10;
        digits.push(n);
    }
    digits
}

pub(crate) async fn beacon_handler(event: Request, _ctx: Context) -> Result<Response, Error> {
    let client = Client::new(&aws_config::load_from_env().await);

    let response = client
        .get_object()
        .bucket(&event.vcf_index_bucket)
        .key(&event.vcf_index_key)
        .send()
        .await?;

    let index = noodles::tabix::AsyncReader::new(response.body.into_async_read()).read_index().await?;
    let query = htsget_search::htsget::Query::new(event.vcf_key, Format::Vcf).with_start(event.start);
    let storage = htsget_search::storage::aws::AwsS3Storage::new(client, event.vcf_index_bucket, RegexResolver::default());
    let vcf_search = htsget_search::htsget::vcf_search::VcfSearch::new(Arc::new(storage));
    let byte_ranges = vcf_search.get_byte_ranges_for_reference_name(event.reference_name, &index, &Header::default(), query).await?;

    let mut blocks = FuturesUnordered::new();
    for range in BytesPosition::merge_all(byte_ranges).iter().map(BytesRange::from) {
        blocks.push(tokio::spawn(async move {
            client.get_object().bucket(&event.vcf_bucket).key(&event.vcf_key).range(String::from(range)).send().await
        }));
    };

    let body = blocks.next().await.unwrap().unwrap().unwrap().body;
    let mut vcf_block = AsyncReader::new(body.into_async_read());
    let mut records = vcf_block.records(&header);
    while let Some(record) = records.try_next().await? {
        if record.reference_bases() == event.reference_bases.parse()? && record.alternate_bases() == event.alternate_bases.parse()? {
            return Ok(Response { found: true });
        }
    }

    // loop {
    //     select! {
    //         Some(next) = blocks.next() => {
    //             let a = next;
    //         },
    //         else => break
    //     }
    // }

    // htsget_search::htsget::
    // index.header().
    // println!("{:?}", index);

    // do some tabix parsing so we can jump direct to the right spot in the VCF
    // I don't know if there is some clever way to hook up the tabix to stream the S3 index
    // or if we literally download to the lambda and process locally..

    /*let tabix_src = format!("{}.tbi", src);
    let index = tabix::r#async::read(tabix_src).await?;

    let reader = File::open(src).await.map(bgzf::AsyncReader::new)?;
    let line_comment_prefix = char::from(index.header().line_comment_prefix());

    let mut lines = reader.lines();

    while let Some(line) = lines.next_line().await? {
        if !line.starts_with(line_comment_prefix) {
            break;
        }

        println!("{}", line);
    } */

    // do some bioinformatics


    // simulate variable runtime - different keys = different pauses (to be removed obviously)
    // let mut s = DefaultHasher::new();
    // event.vcf_key.hash(&mut s);
    // let h = s.finish();
    //
    // sleep(Duration::from_secs(h % 10)).await;
    //
    // // prepare the response
    // // do some random result generation (to be removed and replaced with REAL bioinformatics)
    // let digits = to_digits(event.start);
    // let left_digit = digits.last().copied().unwrap();
    // let left_digit_str = vec![left_digit + 48];

    let resp = Response {
        found: false
    };

    // return `Response` (it will be serialized to JSON automatically by the runtime)
    Ok(resp)
}

fn index_positions(index: &noodles::tabix::Index) -> Vec<u64> {
    let mut positions = HashSet::new();

    positions.extend(
        index
          .reference_sequences()
          .iter()
          .flat_map(|ref_seq| ref_seq.bins())
          .flat_map(|bin| bin.chunks())
          .flat_map(|chunk| [chunk.start().compressed(), chunk.end().compressed()]),
    );
    positions.extend(
        index
          .reference_sequences()
          .iter()
          .filter_map(|ref_seq| ref_seq.metadata())
          .flat_map(|metadata| {
              [
                  metadata.start_position().compressed(),
                  metadata.end_position().compressed(),
              ]
          }),
    );

    positions.remove(&0);
    let mut positions: Vec<u64> = positions.into_iter().collect();
    positions.sort_unstable();
    positions
}

#[cfg(test)]
mod tests {
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
        let r = beacon_handler(Request {
            vcf_bucket: "umccr-10g-data-dev".to_string(),
            vcf_key: "HG00174/HG00174.hard-filtered.vcf.gz".to_string(),
            vcf_index_bucket: "umccr-10g-data-dev".to_string(),
            vcf_index_key: "HG00174/HG00174.hard-filtered.vcf.gz.tbi".to_string(),
            // chr1    1220751 T       C       0010111111
            reference_name: "chr1".to_string(),
            start: 1220751,
            reference_bases: "T".to_string(),
            alternate_bases: "C".to_string(),
        }, Context::default()).await;

        assert!(r.unwrap().found, "Expected variant was not found");
    }

    #[tokio::test]
    async fn test_where_variant_should_not_be_found() {
        let r = beacon_handler(Request {
            vcf_bucket: "umccr-10g-data-dev".to_string(),
            vcf_key: "HG00096/HG00096.hard-filtered.vcf.gz".to_string(),
            vcf_index_bucket: "umccr-10g-data-dev".to_string(),
            vcf_index_key: "HG00096/HG00096.hard-filtered.vcf.gz.tbi".to_string(),
            // chr1    1220751 T       C       0010111111
            reference_name: "chr1".to_string(),
            start: 1220751,
            reference_bases: "T".to_string(),
            alternate_bases: "C".to_string(),
        }, Context::default()).await;

        assert!(!r.unwrap().found, "Unexpected variant was found");
    }

    // this test should be deleted once the real bioinformatics is in
    // this one just tests our stupid 'random' logic
    #[tokio::test]
    async fn test_random_logic_with_leading_nine() {
        let r = beacon_handler(Request {
            vcf_bucket: "umccr-10g-data-dev".to_string(),
            vcf_key: "HG00096/HG00096.hard-filtered.vcf.gz".to_string(),
            vcf_index_bucket: "umccr-10g-data-dev".to_string(),
            vcf_index_key: "HG00096/HG00096.hard-filtered.vcf.gz.tbi".to_string(),
            reference_name: "chrX".to_string(),
            start: 9220751,
            reference_bases: "A".to_string(),
            alternate_bases: "A".to_string(),
        }, Context::default()).await;

        assert!(r.unwrap().found, "Leading 9 in start meant that this path should match");
    }

}
